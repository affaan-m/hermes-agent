"""Protected host binding for audience-scoped readonly table context.

No defaults grant an audience or open a book. The isolated child is a bounded
readonly implementation detail, not a model tool or an outbound capability.
"""
from datetime import datetime
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import stat
import selectors
import threading
import subprocess
import sys
import time
import types
import uuid

POLICY_PATH = "/Library/Application Support/ItoDesk/table-context-policy.json"
_POLICY_UID = 0
MAX_POLICY_BYTES = 128 * 1024
MAX_CODE_BYTES = 1024 * 1024
MAX_SNAPSHOT_BYTES = 8 * 1024 * 1024
# Eight fresh-child samples with50rows/84realpairs: max89.6ms. Reserve
# headroom for CPU/I/O variation plus parent validation/projection. Live I/O
# acceptance remains separate; these constants cannot be selected by a model.
READ_SECONDS = 0.350
TOTAL_READ_SECONDS = 0.500
CLEANUP_SECONDS = 0.200
_UNREAPED = {}
_CHILD_LOCK = threading.Lock()
_CODE_NAMES = ("schema", "store", "matching", "nonpricing_actions", "workflow", "planner")
_IDENTITY_FIELDS = ("profile_id", "user_id", "platform", "workspace_id", "channel_id", "thread_id")
_READ_TOOLS = frozenset(("inventory_show", "match_query", "price_query"))


def _protected_bytes(path, limit):
    """Open a bounded regular file through root-owned, non-symlink directories."""
    if type(path) is not str or not path.startswith("/"):
        raise ValueError("absolute protected path required")
    parts = path.split("/")[1:]
    if not parts or any(part in ("", ".", "..") for part in parts):
        raise ValueError("canonical protected path required")
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for index, part in enumerate(parts):
            last = index == len(parts) - 1
            flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
            if not last:
                flags |= os.O_DIRECTORY
            child = os.open(part, flags, dir_fd=fd)
            os.close(fd)
            fd = child
            info = os.fstat(fd)
            if info.st_uid not in (0, _POLICY_UID) or info.st_mode & 0o022:
                raise ValueError("unprotected source")
            if last and (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > limit):
                raise ValueError("invalid protected file")
        chunks = []
        size = 0
        while True:
            block = os.read(fd, min(65536, limit + 1 - size))
            if not block:
                break
            chunks.append(block)
            size += len(block)
            if size > limit:
                raise ValueError("protected file too large")
        after = os.fstat(fd)
        if (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns) != (
                after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise ValueError("protected file changed")
        return b"".join(chunks)
    finally:
        os.close(fd)


def _unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate policy key")
        value[key] = item
    return value


def _expiry(stamp):
    if type(stamp) is not str or len(stamp) > 64:
        raise ValueError("invalid enrollment expiry")
    value = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    if value.utcoffset() is None:
        raise ValueError("timezone required")
    return value.timestamp()


def _policy():
    raw = _protected_bytes(POLICY_PATH, MAX_POLICY_BYTES)
    policy = json.loads(raw, object_pairs_hook=_unique_object)
    if type(policy) is not dict or type(policy.get("schema_version")) is not int or policy["schema_version"] != 1:
        raise ValueError("unsupported policy")
    if policy.get("enabled") is False:
        return None, hashlib.sha256(raw).hexdigest()
    if (policy.get("enabled") is not True
            or set(policy) != {"schema_version", "enabled", "db_path", "code_sources", "enrollments"}):
        raise ValueError("invalid policy")
    db_path = policy["db_path"]
    if type(db_path) is not str or not db_path.startswith("/") or any(part in ("", ".", "..") for part in db_path.split("/")[1:]):
        raise ValueError("invalid canonical book path")
    sources = policy["code_sources"]
    if type(sources) is not dict or set(sources) != set(_CODE_NAMES):
        raise ValueError("exact source manifest required")
    for entry in sources.values():
        if (type(entry) is not dict or set(entry) != {"path", "sha256"}
                or type(entry["path"]) is not str or not entry["path"].startswith("/")
                or type(entry["sha256"]) is not str or len(entry["sha256"]) != 64
                or any(c not in "0123456789abcdef" for c in entry["sha256"])):
            raise ValueError("invalid source pin")
    entries = policy["enrollments"]
    if type(entries) is not list or len(entries) > 1024:
        raise ValueError("invalid enrollments")
    audiences = {}
    for entry in entries:
        keys = {"operation", "audience", "scope", "mode", "expires_at"}
        if type(entry) is not dict or entry.get("mode") not in ("company", "operator"):
            raise ValueError("explicit context mode required")
        if entry["mode"] == "company":
            keys.add("company_id")
        if (set(entry) != keys or entry["operation"] != "desk_table_context"
                or type(entry["audience"]) is not dict
                or set(entry["audience"]) != set(_IDENTITY_FIELDS[:-1])):
            raise ValueError("invalid enrollment")
        if entry["mode"] == "company" and (type(entry["company_id"]) is not str or not 0 < len(entry["company_id"]) <= 128):
            raise ValueError("exact company required")
        audience = tuple(entry["audience"][key] for key in _IDENTITY_FIELDS[:-1])
        if any(type(item) is not str or not 0 < len(item) <= 256 for item in audience) or audience[2] != "slack":
            raise ValueError("invalid exact audience")
        scope = entry["scope"]
        if type(scope) is not dict:
            raise ValueError("explicit thread scope required")
        if scope.get("kind") == "channel_threads" and set(scope) == {"kind"}:
            thread = None
        elif (scope.get("kind") == "conversation" and set(scope) == {"kind", "thread_id"}
                and type(scope["thread_id"]) is str and 0 < len(scope["thread_id"]) <= 256):
            thread = scope["thread_id"]
        else:
            raise ValueError("invalid thread scope")
        previous = audiences.setdefault(audience, set())
        if previous and (thread is None or None in previous or thread in previous):
            raise ValueError("ambiguous audience enrollment")
        previous.add(thread)
        _expiry(entry["expires_at"])
    return policy, hashlib.sha256(raw).hexdigest()


@contextmanager
def _load_code(policy, names):
    """Execute full pinned modules in an empty-path package, without __init__."""
    name = "_desk_table_pinned_" + uuid.uuid4().hex
    package = types.ModuleType(name)
    package.__path__ = []
    sys.modules[name] = package
    loaded = {}
    try:
        for short in names:
            entry = policy["code_sources"][short]
            raw = _protected_bytes(entry["path"], MAX_CODE_BYTES)
            if hashlib.sha256(raw).hexdigest() != entry["sha256"]:
                raise ValueError("source pin mismatch")
            module = types.ModuleType(name + "." + short)
            module.__file__ = entry["path"]
            module.__package__ = name
            sys.modules[module.__name__] = module
            loaded[short] = module
            exec(compile(raw, entry["path"], "exec"), module.__dict__)
        yield loaded
    finally:
        for module in loaded.values():
            sys.modules.pop(module.__name__, None)
        sys.modules.pop(name, None)


def _active_request(request):
    host = sys.modules.get("gateway.inventory_context")
    return bool(host is not None and getattr(host, "ABI_VERSION", None) == "inventory_request_v1"
                and type(request) is host.InventoryRequest
                and host.capture_inventory_request() is request and request.validate() is True)


def _command():
    return [sys.executable, "-I", "-S", str(Path(__file__).resolve()), "--snapshot"]


def _cleanup_child(child):
    try:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=CLEANUP_SECONDS)
    except BaseException as error:
        try:
            unresolved = child.poll() is None
        except BaseException:
            unresolved = True
        if unresolved:
            # Preserve ownership for every failure, including kill/poll errors,
            # not only wait timeouts. Never create replacement children here.
            with _CHILD_LOCK:
                _UNREAPED[child.pid] = child
        if not isinstance(error, Exception):
            raise
        raise RuntimeError("owned child cleanup pending" if unresolved else "child cleanup failed") from None


def _bounded_snapshot(policy_digest):
    with _CHILD_LOCK:
        for pid, previous in list(_UNREAPED.items()):
            if previous.poll() is not None:
                del _UNREAPED[pid]
        if _UNREAPED:
            raise RuntimeError("owned child cleanup pending")
    child = subprocess.Popen(_command(), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.DEVNULL, cwd="/", close_fds=True,
                             env={"PATH": "/usr/bin:/bin", "HOME": "/var/empty", "PYTHONDONTWRITEBYTECODE": "1"})
    selector = None
    primary_error = None
    try:
        selector = selectors.DefaultSelector()
        deadline = time.monotonic() + READ_SECONDS
        child.stdin.write(policy_digest.encode("ascii"))
        child.stdin.close()
        selector.register(child.stdout, selectors.EVENT_READ)
        chunks = []
        size = 0
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("snapshot deadline exceeded")
            for key, _ in selector.select(remaining):
                block = os.read(key.fileobj.fileno(), min(65536, MAX_SNAPSHOT_BYTES + 1 - size))
                if not block:
                    selector.unregister(key.fileobj)
                    continue
                size += len(block)
                if size > MAX_SNAPSHOT_BYTES:
                    raise ValueError("snapshot too large")
                chunks.append(block)
        child.wait(timeout=max(0, deadline - time.monotonic()))
        if child.returncode != 0:
            raise ValueError("snapshot unavailable")
        return json.loads(b"".join(chunks))
    except BaseException as error:
        primary_error = error
        raise
    finally:
        cleanup_error = None
        try:
            if selector is not None:
                selector.close()
        except BaseException as error:
            cleanup_error = error
        try:
            _cleanup_child(child)
        except BaseException as error:
            cleanup_error = cleanup_error or error
        for pipe in (child.stdin, child.stdout):
            if pipe is not None:
                try:
                    pipe.close()
                except OSError:
                    pass
        # Preserve an already-raised interruption/primary failure. Cleanup
        # still records unresolved child ownership and fences replacements.
        if primary_error is None and cleanup_error is not None:
            raise cleanup_error


class _HostReader:
    def __init__(self, policy, digest):
        self.policy = policy
        self.digest = digest
        with _load_code(policy, ("workflow", "planner")) as modules:
            self.scope_type = modules["planner"].ContextScope
            self.reader = modules["planner"].RequestContextReader(snapshot_reader=self.snapshot, resolve_scope=self.scope, max_read_seconds=TOTAL_READ_SECONDS)

    def scope(self, request):
        if not _active_request(request):
            return None
        current, digest = _policy()
        if current is None or digest != self.digest or not _active_request(request):
            return None
        identity = request.identity
        for entry in current["enrollments"]:
            scope = entry["scope"]
            if (tuple(entry["audience"][key] for key in _IDENTITY_FIELDS[:-1]) == identity[:5]
                    and (scope["kind"] == "channel_threads" or scope["thread_id"] == identity[5])
                    and _expiry(entry["expires_at"]) > time.time()):
                if entry["mode"] == "company":
                    return self.scope_type("company", entry["company_id"])
                return self.scope_type("operator")
        return None

    def snapshot(self):
        host = sys.modules.get("gateway.inventory_context")
        request = host.capture_inventory_request() if host is not None else None
        if self.scope(request) is None:
            raise ValueError("scope unavailable")
        return _bounded_snapshot(self.digest)

    def __call__(self, request):
        return self.reader(request)


def bind_host_context(agent):
    """Called after actual agent initialization; disabled CLI has no file I/O."""
    if (getattr(agent, "platform", None) != "slack"
            or not getattr(agent, "_user_id", None) or not getattr(agent, "_chat_id", None)
            or not _READ_TOOLS.intersection(getattr(agent, "valid_tool_names", ()))):
        return False
    try:
        policy, digest = _policy()
        if policy is None:
            return False
        reader = _HostReader(policy, digest)
        from agent.desk_table_context import bind_context_reader
        bind_context_reader(agent, reader, max_read_seconds=TOTAL_READ_SECONDS)
        return True
    except Exception:
        # Missing/invalid policy or code pins never break ordinary startup and
        # never expose policy contents, paths or internal exception diagnostics.
        return False


def _child_main():
    expected = sys.stdin.buffer.read(65)
    if len(expected) != 64:
        return 2
    policy, digest = _policy()
    if policy is None or expected.decode("ascii") != digest:
        return 2
    with _load_code(policy, ("schema", "store", "matching", "nonpricing_actions")) as modules:
        store = modules["store"].DeskStore(policy["db_path"], readonly=True)
        try:
            snapshot = modules["nonpricing_actions"].export_nonpricing_snapshot(store)
        finally:
            store.close()
    encoded = json.dumps(snapshot, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("utf-8")
    if len(encoded) > MAX_SNAPSHOT_BYTES:
        return 2
    sys.stdout.buffer.write(encoded)
    return 0


if __name__ == "__main__":
    try:
        result = _child_main() if sys.argv[1:] == ["--snapshot"] else 2
    except Exception:
        result = 2
    raise SystemExit(result)
