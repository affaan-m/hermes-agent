"""Private durable admitted-ID claims. Importing this module opens no store.

Root explicitly initializes a new store. Runtime opens mode=rw only. A claim
survives expiry/restart and burns on ambiguity; it is not a delivery receipt.
"""
import asyncio
import hashlib
import json
import logging
import os
from pathlib import Path
import queue
import sqlite3
import stat
import threading
import time

NAME = "delivery_claims.sqlite3"
MAX_PAGES = 32768
APPLICATION_ID = 0x48444331
QUEUE_LIMIT = 8


_ERROR_CODES = frozenset({
    "registry_unsafe_root", "registry_unsafe_file", "registry_invalid_limit",
    "registry_invalid_journal", "registry_invalid_settings", "registry_replaced",
    "registry_initialize_failed", "registry_invalid_identity", "registry_invalid_schema",
    "registry_unavailable", "registry_busy", "registry_closed", "registry_claim_failed",
    "registry_queue_full", "registry_claim_timeout", "registry_cleanup_pending",
    "registry_full", "registry_corrupt",
})
_logger = logging.getLogger(__name__)
_diagnostic_clock = time.monotonic
_diagnostic_lock = threading.Lock()
_diagnostic_state = dict(failures=0, warnings=0, suppressed=0, last_code=None, next_warning_at=0.0)
_MAX_COUNTER = (1 << 63) - 1


def _safe_code(value):
    return value if type(value) is str and value in _ERROR_CODES else "registry_unavailable"


class RegistryError(RuntimeError):
    """Fixed diagnostic code only, with no identity, path or SQLite text."""

    def __init__(self, code="registry_unavailable"):
        self.code = _safe_code(code)
        super().__init__(self.code)


def report_registry_failure(error):
    """Internal only: one process-wide warning per minute, fixed bounded state."""
    code = _safe_code(getattr(error, "code", None))
    if code == "registry_invalid_identity":
        return  # Invalid input is consent/identity denial, not broken storage.
    with _diagnostic_lock:
        state = _diagnostic_state
        state["failures"] = min(_MAX_COUNTER, state["failures"] + 1)
        state["last_code"] = code
        now = _diagnostic_clock()
        if now < state["next_warning_at"]:
            state["suppressed"] = min(_MAX_COUNTER, state["suppressed"] + 1)
            return
        total, suppressed = state["failures"], state["suppressed"]
        state["warnings"] = min(_MAX_COUNTER, state["warnings"] + 1)
        state["suppressed"] = 0
        state["next_warning_at"] = now + 60.0
    try:
        _logger.warning("Delivery claim storage unavailable: code=%s failures_total=%d "
                        "suppressed_since_warning=%d; scoped delivery held", code, total, suppressed)
    except Exception:
        pass  # Diagnostics cannot change the existing fail-closed reply policy.


def registry_failure_stats():
    """Fixed-code/numeric process counters only; no per-identity cache or payload."""
    with _diagnostic_lock:
        return {key: _diagnostic_state[key] for key in ("failures", "warnings", "suppressed", "last_code")}


def _sqlite_failure_code(error):
    code = getattr(error, "sqlite_errorcode", None)
    if type(code) is not int:
        return "registry_claim_failed"
    return {sqlite3.SQLITE_BUSY: "registry_busy", sqlite3.SQLITE_LOCKED: "registry_busy",
            sqlite3.SQLITE_FULL: "registry_full", sqlite3.SQLITE_CORRUPT: "registry_corrupt",
            sqlite3.SQLITE_NOTADB: "registry_corrupt"}.get(code & 0xff, "registry_claim_failed")


def _root(value):
    try:
        path = Path(value)
        info = path.lstat()
        if (not path.is_absolute() or path.resolve(strict=True) != path
                or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o700):
            raise ValueError
        return path
    except (OSError, ValueError, TypeError):
        raise RegistryError("registry_unsafe_root") from None


def _file(path):
    try:
        info = path.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1):
            raise ValueError
        return info.st_dev, info.st_ino
    except (OSError, ValueError):
        raise RegistryError("registry_unsafe_file") from None


def _sidecars(path):
    for suffix in ("-journal", "-wal", "-shm"):
        side = path.with_name(path.name + suffix)
        if side.is_symlink() or side.exists():
            _file(side)


def _connect(path):
    return sqlite3.connect(path.as_uri() + "?mode=rw", uri=True, timeout=0.25,
                           isolation_level=None, check_same_thread=False)


def _settings(conn, pages):
    if type(pages) is not int or not 4 <= pages <= MAX_PAGES:
        raise RegistryError("registry_invalid_limit")
    if conn.execute("PRAGMA journal_mode=DELETE").fetchone()[0] != "delete":
        raise RegistryError("registry_invalid_journal")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=250")
    if (conn.execute("PRAGMA synchronous").fetchone()[0] != 2
            or conn.execute("PRAGMA busy_timeout").fetchone()[0] != 250
            or conn.execute(f"PRAGMA max_page_count={pages}").fetchone()[0] != pages
            or conn.execute("PRAGMA page_size").fetchone()[0] != 4096):
        raise RegistryError("registry_invalid_settings")


def initialize_registry(root, *, max_pages=MAX_PAGES):
    """Root-only explicit first initialization; never overwrite/recreate a store."""
    root = _root(root)
    if type(max_pages) is not int or not 4 <= max_pages <= MAX_PAGES:
        raise RegistryError("registry_invalid_limit")
    path = root / NAME
    conn = None
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
        os.close(fd)
        identity = _file(path)
        _sidecars(path)
        conn = _connect(path)
        conn.execute("PRAGMA page_size=4096")
        _settings(conn, max_pages)
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("CREATE TABLE claims (digest BLOB PRIMARY KEY CHECK(length(digest)=32), "
                     "kind TEXT NOT NULL CHECK(kind IN ('request','dispatch')), "
                     "claimed_at INTEGER NOT NULL) WITHOUT ROWID")
        conn.execute("CREATE TABLE registry_meta (singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
                     "max_pages INTEGER NOT NULL)")
        conn.execute("INSERT INTO registry_meta VALUES (1, ?)", (max_pages,))
        conn.execute(f"PRAGMA application_id={APPLICATION_ID}")
        conn.execute("PRAGMA user_version=1")
        conn.execute("COMMIT")
        if _file(path) != identity:
            raise RegistryError("registry_replaced")
        return {"schema_version": 1, "max_pages": max_pages, "page_size": 4096}
    except (OSError, sqlite3.Error):
        # Preserve any partial initialization for explicit root reconciliation.
        raise RegistryError("registry_initialize_failed") from None
    finally:
        if conn is not None:
            conn.close()


def _digest(kind, identity):
    if kind not in ("request", "dispatch") or type(kind) is not str:
        raise RegistryError("registry_invalid_identity")
    if type(identity) is not tuple or not 1 <= len(identity) <= 8:
        raise RegistryError("registry_invalid_identity")
    try:
        for i, value in enumerate(identity):
            if value is None and kind == "request" and i == 5:
                continue
            if (type(value) is not str or not value.strip()
                    or len(value.encode("utf-8")) > 1024):
                raise ValueError
        encoded = json.dumps([1, kind, identity], ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    except (ValueError, UnicodeError):
        raise RegistryError("registry_invalid_identity") from None
    return hashlib.sha256(encoded).digest()


class Registry:
    """One trusted runtime-root store; serialized bounded claims and one I/O thread."""

    def __init__(self, root):
        self.root = _root(root)
        self.path = self.root / NAME
        self._identity = _file(self.path)
        _sidecars(self.path)
        self._conn = None
        self._lock = threading.Lock()
        self._worker_lock = threading.Lock()
        self._jobs = queue.Queue(maxsize=QUEUE_LIMIT)
        self._stop = threading.Event()
        self._worker = None
        self._closed = False
        try:
            self._conn = _connect(self.path)
            conn = self._conn
            if (conn.execute("PRAGMA application_id").fetchone()[0] != APPLICATION_ID
                    or conn.execute("PRAGMA user_version").fetchone()[0] != 1):
                raise RegistryError("registry_invalid_schema")
            schema = conn.execute("SELECT type,name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()
            if schema != [("table", "claims"), ("table", "registry_meta")]:
                raise RegistryError("registry_invalid_schema")
            columns = conn.execute("PRAGMA table_info(claims)").fetchall()
            if [(r[1], r[2], r[3], r[5]) for r in columns] != [
                    ("digest", "BLOB", 1, 1), ("kind", "TEXT", 1, 0), ("claimed_at", "INTEGER", 1, 0)]:
                raise RegistryError("registry_invalid_schema")
            rows = conn.execute("SELECT singleton,max_pages FROM registry_meta").fetchall()
            if len(rows) != 1 or rows[0][0] != 1:
                raise RegistryError("registry_invalid_schema")
            _settings(conn, rows[0][1])
            self._check_file()
        except (sqlite3.Error, RegistryError):
            if self._conn is not None:
                self._conn.close()
            raise RegistryError("registry_unavailable") from None

    def _check_file(self):
        if _root(self.root) != self.root or _file(self.path) != self._identity:
            raise RegistryError("registry_replaced")
        _sidecars(self.path)

    def _claim_digest(self, kind, digest):
        if not self._lock.acquire(timeout=0.25):
            raise RegistryError("registry_busy")
        try:
            if self._closed:
                raise RegistryError("registry_closed")
            self._check_file()
            inserted = self._conn.execute(
                "INSERT OR IGNORE INTO claims(digest,kind,claimed_at) VALUES (?,?,?)",
                (digest, kind, int(time.time())),
            ).rowcount == 1
            self._check_file()
            return inserted
        except sqlite3.Error as error:
            raise RegistryError(_sqlite_failure_code(error)) from None
        finally:
            self._lock.release()

    def claim(self, kind, identity):
        if self._stop.is_set():
            raise RegistryError("registry_closed")
        return self._claim_digest(kind, _digest(kind, identity))

    @staticmethod
    def _deliver(future, value, failed):
        if not future.done():
            if failed:
                future.set_exception(RegistryError(failed))
            else:
                future.set_result(value)

    def _work(self):
        while not self._stop.is_set() or not self._jobs.empty():
            try:
                kind, digest, loop, future = self._jobs.get(timeout=0.05)
            except queue.Empty:
                continue
            try:
                value = self._claim_digest(kind, digest)
                failed = False
            except RegistryError as error:
                value, failed = False, _safe_code(error.code)
            try:
                loop.call_soon_threadsafe(self._deliver, future, value, failed)
            except RuntimeError:
                pass  # A stopped caller cannot gain authority; any claim stays burned.
            finally:
                self._jobs.task_done()

    async def aclaim(self, kind, identity):
        digest = _digest(kind, identity)
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        with self._worker_lock:
            if self._stop.is_set() or self._closed:
                raise RegistryError("registry_closed")
            if self._worker is None:
                self._worker = threading.Thread(target=self._work, name="delivery-claims", daemon=True)
                self._worker.start()
            try:
                self._jobs.put_nowait((kind, digest, loop, future))
            except queue.Full:
                raise RegistryError("registry_queue_full") from None
        try:
            return await asyncio.wait_for(future, timeout=1.0)
        except asyncio.TimeoutError:
            raise RegistryError("registry_claim_timeout") from None

    def count(self):
        with self._lock:
            self._check_file()
            return self._conn.execute("SELECT count(*) FROM claims").fetchone()[0]

    def close(self):
        with self._worker_lock:
            self._stop.set()
        if self._worker is not None:
            self._worker.join(timeout=2.5)
            if self._worker.is_alive():
                raise RegistryError("registry_cleanup_pending")
        with self._lock:
            if not self._closed:
                self._conn.close()
                self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
