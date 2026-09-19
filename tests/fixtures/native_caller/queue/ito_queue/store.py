"""SQLite store for the task queue.

Default location: /Volumes/Agent-Runtime/state/queue/queue.sqlite3. The
volume is external so the root SSD never fills; if it is not mounted the
store refuses to open rather than silently creating a second queue under
/Volumes on the root disk. ITO_QUEUE_DIR overrides the directory (tests,
the Pro, the Air).
"""

from __future__ import annotations

import contextlib
import json
import os
import pathlib
import sqlite3
import uuid
from typing import Iterable

from .schema import OWNERS, Task, ValidationError, now_iso

DEFAULT_VOLUME = pathlib.Path("/Volumes/Agent-Runtime")
DEFAULT_DIR = DEFAULT_VOLUME / "state" / "queue"
DB_NAME = "queue.sqlite3"

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    area TEXT NOT NULL,
    kind TEXT NOT NULL,
    owner TEXT NOT NULL,
    state TEXT NOT NULL,
    source_ref TEXT NOT NULL DEFAULT '',
    detail TEXT NOT NULL DEFAULT '',
    deps TEXT NOT NULL DEFAULT '[]',
    acceptance TEXT NOT NULL DEFAULT '{"type": "none"}',
    receipts TEXT NOT NULL DEFAULT '[]',
    blocked_on TEXT NOT NULL DEFAULT '',
    priority INTEGER NOT NULL DEFAULT 50,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    claimed_by TEXT NOT NULL DEFAULT '',
    last_check_at TEXT NOT NULL DEFAULT '',
    last_check_ok INTEGER,
    last_check_detail TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS tasks_state ON tasks(state);
CREATE INDEX IF NOT EXISTS tasks_area ON tasks(area);
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    task_id TEXT NOT NULL,
    event TEXT NOT NULL,
    actor TEXT NOT NULL DEFAULT '',
    detail TEXT NOT NULL DEFAULT ''
);
"""


class StoreError(RuntimeError):
    pass


def default_dir() -> pathlib.Path:
    override = os.environ.get("ITO_QUEUE_DIR")
    if override:
        return pathlib.Path(override).expanduser()
    if not DEFAULT_VOLUME.is_dir() or not os.path.ismount(DEFAULT_VOLUME):
        raise StoreError(
            f"{DEFAULT_VOLUME} is not mounted; refusing to create a queue on the root disk. "
            "Set ITO_QUEUE_DIR to use another directory."
        )
    return DEFAULT_DIR


def _row_to_task(row: sqlite3.Row) -> Task:
    d = dict(row)
    d["deps"] = json.loads(d["deps"] or "[]")
    d["acceptance"] = json.loads(d["acceptance"] or "{}")
    d["receipts"] = json.loads(d["receipts"] or "[]")
    ok = d.get("last_check_ok")
    d["last_check_ok"] = None if ok is None else bool(ok)
    return Task.from_dict(d)


class Store:
    def __init__(self, directory: pathlib.Path | str | None = None):
        self.dir = pathlib.Path(directory).expanduser() if directory else default_dir()
        self.dir.mkdir(parents=True, exist_ok=True)
        self.path = self.dir / DB_NAME
        self.conn = sqlite3.connect(str(self.path), timeout=1, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        # Stop all older writers before activating this additive schema.
        # In particular, old positional INSERTs are incompatible with new columns.
        with self._transaction():
            for statement in SCHEMA.split(";"):
                if statement.strip():
                    self.conn.execute(statement)
            columns = {r["name"] for r in self.conn.execute("PRAGMA table_info(tasks)")}
            if "revision" not in columns:
                self.conn.execute("ALTER TABLE tasks ADD COLUMN revision INTEGER NOT NULL DEFAULT 0")
            if "claim_token" not in columns:
                self.conn.execute("ALTER TABLE tasks ADD COLUMN claim_token TEXT NOT NULL DEFAULT ''")
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass

    def close(self) -> None:
        self.conn.close()

    # -- writes -------------------------------------------------------------

    def _event(self, task_id: str, event: str, actor: str = "", detail: str = "") -> None:
        self.conn.execute(
            "INSERT INTO events(ts, task_id, event, actor, detail) VALUES (?,?,?,?,?)",
            (now_iso(), task_id, event, actor, detail[:2000]),
        )

    @contextlib.contextmanager
    def _transaction(self):
        """Serialize decision, row write and audit write, including schema changes."""
        try:
            self.conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            raise StoreError("queue busy or unavailable; no transition performed") from exc
        try:
            yield
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise

    def _write(self, task: Task) -> None:
        task.validate()
        d = task.to_dict()
        for name in ("deps", "acceptance", "receipts"):
            d[name] = json.dumps(d[name])
        d["last_check_ok"] = None if task.last_check_ok is None else int(task.last_check_ok)
        # Names come from the dataclass, never external SQL identifiers.
        names = list(d)
        columns = ",".join(names)
        values = ",".join(":" + n for n in names)
        updates = ",".join(n + "=excluded." + n for n in names if n not in ("id", "created_at"))
        self.conn.execute(f"INSERT INTO tasks ({columns}) VALUES ({values}) "
                          f"ON CONFLICT(id) DO UPDATE SET {updates}", d)

    @staticmethod
    def _new_task(task: Task) -> None:
        task.validate()
        if (task.state not in ("open", "blocked") or task.claimed_by or task.claim_token
                or task.revision != 0 or task.last_check_at or task.last_check_ok is not None):
            raise StoreError("new tasks must be open/blocked without claims or check results")

    def add(self, task: Task, actor: str = "", replace: bool = False) -> Task:
        if replace:
            raise StoreError("whole-row replacement disabled; use reviewed revision-bound transitions")
        with self._transaction():
            self._new_task(task)
            if self.get(task.id):
                raise StoreError(f"{task.id} already exists")
            for dep in task.deps:
                if not self.get(dep):
                    raise StoreError(f"{task.id}: unknown dep {dep}")
            self._write(task)
            self._event(task.id, "added", actor, task.title)
        return task

    def import_tasks(self, tasks: Iterable[Task], actor: str = "", replace: bool = False) -> tuple[int, int]:
        """Insert new open/blocked rows atomically; preserve every existing row."""
        if replace:
            raise StoreError("whole-row replacement disabled; use reviewed revision-bound transitions")
        pending = list(tasks)
        added = skipped = 0
        with self._transaction():
            known = {r["id"] for r in self.conn.execute("SELECT id FROM tasks")}
            ids = {t.id for t in pending}
            if len(ids) != len(pending):
                raise StoreError("duplicate task ids in import")
            for t in pending:
                if t.id in known:
                    skipped += 1
                    continue
                self._new_task(t)
                if any(dep not in known and dep not in ids for dep in t.deps):
                    raise StoreError(f"{t.id}: unknown dependency")
                self._write(t)
                self._event(t.id, "added", actor, t.title)
                added += 1
        return added, skipped

    def _required(self, task_id: str, expected_revision: int | None = None) -> Task:
        task = self.get(task_id)
        if not task:
            raise StoreError(f"no task {task_id}")
        if expected_revision is not None and (type(expected_revision) is not int
                                              or task.revision != expected_revision):
            raise StoreError("stale task revision")
        return task

    @staticmethod
    def _revision(expected_revision: int | None) -> None:
        if type(expected_revision) is not int or expected_revision < 0:
            raise StoreError("expected revision required; read the current task first")

    @staticmethod
    def _actor(task: Task, actor: str) -> None:
        # These labels enforce protocol consistency, not authentication.
        if actor not in OWNERS or actor not in (task.owner, "affaan"):
            raise StoreError("actor must be current owner or reviewed operator affaan")

    def _deps(self, task: Task) -> None:
        if self.unmet_deps(task):
            raise StoreError(f"{task.id} has unmet dependencies")

    def _aws_hold(self, task_id: str, *, finishing: bool = False) -> None:
        # Persisted opt-in, visible to every new Store instance. Never a local flag.
        tables = {r[0] for r in self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('aws_attempts','aws_enrollments')")}
        if "aws_attempts" in tables and self.conn.execute(
                "SELECT 1 FROM aws_attempts WHERE task_id=? AND state NOT IN ('accepted','retry_authorized') LIMIT 1", (task_id,)).fetchone():
            raise StoreError("unresolved AWS effects forbid canonical transition")
        if finishing and "aws_enrollments" in tables and self.conn.execute(
                "SELECT 1 FROM aws_enrollments WHERE task_id=? AND state='active'", (task_id,)).fetchone():
            raise StoreError("AWS enrollment requires independent canonical acceptance")

    def _save(self, task: Task, event: str, actor: str, detail: str = "") -> Task:
        self._aws_hold(task.id, finishing=task.state == "done")
        task.revision += 1
        task.updated_at = now_iso()
        self._write(task)
        self._event(task.id, event, actor, detail)
        return task

    @staticmethod
    def _clear_claim(task: Task) -> None:
        task.claimed_by = ""
        task.claim_token = ""
        task.last_check_at = ""
        task.last_check_ok = None
        task.last_check_detail = ""

    def claim(self, task_id: str, actor: str) -> Task:
        with self._transaction():
            task = self._required(task_id)
            if actor not in OWNERS or actor != task.owner:
                raise StoreError("claim requires the assigned owner")
            if task.kind == "human" and actor != "affaan":
                raise StoreError("human tasks require owner and actor affaan")
            if task.state != "open" or task.claimed_by or task.claim_token:
                raise StoreError("claim requires an open unclaimed task")
            self._deps(task)
            active = self.conn.execute(
                "SELECT id FROM tasks WHERE state='claimed' AND (owner=? OR claimed_by=?) LIMIT 1",
                (actor, actor),
            ).fetchone()
            if active:
                raise StoreError("owner already has an active claim")
            task.state = "claimed"
            task.claimed_by = actor
            task.claim_token = uuid.uuid4().hex
            return self._save(task, "claimed", actor)

    def done(self, task_id: str, actor: str, receipt: str = "", *,
             expected_revision: int | None = None, reviewed: bool = False) -> Task:
        """Explicit reviewed acceptance. A label/flag is not proof of approval."""
        self._revision(expected_revision)
        if reviewed is not True or not isinstance(receipt, str) or not receipt.strip():
            raise StoreError("manual completion requires explicit review and a review receipt")
        with self._transaction():
            task = self._required(task_id, expected_revision)
            self._actor(task, actor)
            if task.state != "claimed" or task.claimed_by != task.owner:
                raise StoreError("completion requires a current owner claim")
            if task.kind == "human" and (actor != "affaan" or task.owner != "affaan"):
                raise StoreError("human completion requires affaan")
            self._deps(task)
            task.receipts.append(receipt.strip())
            task.state = "done"
            task.blocked_on = ""
            return self._save(task, "done", actor, "explicit reviewed acceptance: " + receipt.strip())

    def reassign(self, task_id: str, actor: str, owner: str, *,
                 expected_revision: int | None = None, reason: str = "") -> Task:
        self._revision(expected_revision)
        if owner not in OWNERS or not reason.strip():
            raise StoreError("reassign requires a known owner and a reviewed release reason")
        with self._transaction():
            task = self._required(task_id, expected_revision)
            self._actor(task, actor)
            if task.state not in ("open", "claimed", "blocked"):
                raise StoreError("cannot reassign a closed task")
            if task.kind == "human" and owner != "affaan":
                raise StoreError("human tasks must be assigned to affaan")
            previous = task.owner
            task.owner = owner
            if task.state == "claimed":
                task.state = "open"
            self._clear_claim(task)
            return self._save(task, "reassigned", actor, f"{previous} -> {owner}: {reason}")

    def _state_change(self, task_id, actor, state, allowed, reason, expected_revision):
        self._revision(expected_revision)
        with self._transaction():
            task = self._required(task_id, expected_revision)
            self._actor(task, actor)
            if task.state not in allowed:
                raise StoreError("invalid prior state for transition")
            task.state = state
            task.blocked_on = reason if state == "blocked" else ""
            self._clear_claim(task)
            return self._save(task, state, actor, reason)

    def block(self, task_id: str, actor: str, reason: str, *, expected_revision=None) -> Task:
        if not reason.strip():
            raise ValidationError("block needs a reason")
        return self._state_change(task_id, actor, "blocked", ("open", "claimed"), reason.strip(), expected_revision)

    def unblock(self, task_id: str, actor: str, *, expected_revision=None) -> Task:
        return self._state_change(task_id, actor, "open", ("blocked",), "unblocked", expected_revision)

    def drop(self, task_id: str, actor: str, reason: str, *, expected_revision=None) -> Task:
        if not reason.strip():
            raise ValidationError("drop needs a reason")
        return self._state_change(task_id, actor, "dropped", ("open", "claimed", "blocked"), reason.strip(), expected_revision)

    def record_check(self, *args, **kwargs) -> Task:
        raise StoreError("unbound check writes disabled; use finish_check with the current claim")

    def finish_check(self, task_id: str, *, expected_revision: int, claim_token: str,
                     ok: bool, detail: str, actor: str = "queue-render") -> Task:
        """Commit check and optional completion as one CAS transaction, never execute a check here."""
        self._revision(expected_revision)
        if actor != "queue-render" or type(ok) is not bool:
            raise StoreError("invalid automatic checker result")
        with self._transaction():
            task = self._required(task_id, expected_revision)
            if (task.state != "claimed" or task.kind == "human" or task.owner == "affaan"
                    or task.claimed_by != task.owner or not claim_token
                    or task.claim_token != claim_token or task.acceptance.type != "receipt"):
                raise StoreError("ineligible or stale automatic claim")
            self._deps(task)
            task.last_check_at = now_iso()
            task.last_check_ok = ok
            task.last_check_detail = detail[:500]
            self._event(task_id, "check-pass" if ok else "check-fail", actor, detail)
            if ok:
                task.receipts.append(f"acceptance passed {now_iso()}: {detail}")
                task.state = "done"
                return self._save(task, "done", actor, detail)
            # A failed poll does not invalidate the producer's claim-bound proof.
            self._write(task)
            return task

    # -- reads --------------------------------------------------------------

    def get(self, task_id: str) -> Task | None:
        row = self.conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        return _row_to_task(row) if row else None

    def list(self, state: str | None = None, area: str | None = None,
             owner: str | None = None, include_closed: bool = False) -> list[Task]:
        clauses, params = [], []
        if state:
            clauses.append("state=?")
            params.append(state)
        elif not include_closed:
            clauses.append("state NOT IN ('done','dropped')")
        if area:
            clauses.append("area=?")
            params.append(area.upper())
        if owner:
            clauses.append("owner=?")
            params.append(owner)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = self.conn.execute(
            f"SELECT * FROM tasks{where} ORDER BY area, priority, created_at, id", params
        ).fetchall()
        return [_row_to_task(r) for r in rows]

    def unmet_deps(self, task: Task) -> list[str]:
        unmet = []
        for dep in task.deps:
            d = self.get(dep)
            if d is None or d.state != "done":
                unmet.append(dep)
        return unmet

    def events(self, task_id: str | None = None, limit: int = 50) -> list[dict]:
        if task_id:
            rows = self.conn.execute(
                "SELECT * FROM events WHERE task_id=? ORDER BY seq DESC LIMIT ?", (task_id, limit)
            ).fetchall()
        else:
            rows = self.conn.execute("SELECT * FROM events ORDER BY seq DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def counts(self) -> dict[str, int]:
        rows = self.conn.execute("SELECT state, COUNT(*) AS n FROM tasks GROUP BY state").fetchall()
        return {r["state"]: r["n"] for r in rows}
