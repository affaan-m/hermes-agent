"""Canonical ordering for one bounded synthetic context trial.

This module supplies no authentication transport, cloud SDK, process launcher or
live initialization. Actor labels are protocol checks, never authentication.
Use only with a trusted injected authenticator and an explicitly supplied Store.
"""
from __future__ import annotations

import functools
import contextlib
import hashlib
import json
import math
import re
import sqlite3
import time
import uuid
from .aws_receipts import CanonicalAWS, ReceiptError, encoded, sha, fingerprint
from .store import StoreError

TRIAL_LIMITS = {
    "input_max_bytes": 1048576, "result_max_bytes": 8192, "row_limit": 20,
    "request_ttl_s": 120, "cli_timeout_s": 30, "max_fetches": 2,
    "late_evidence_s": 60, "artifact_max_bytes": 16777216, "retention_s": 86400,
}
_SPEC = {"schema_version", "request_id", "operation", "input_sha256",
         "input_bytes", "cli_source_manifest_sha256", "audience_grant_sha256",
         "projection", "deadline_at", "result_max_bytes"}
_SCOPE = {"scope_id", "request_id", "sequence", "grant_epoch", "operation",
          "target", "projection", "not_after", "revoked"}
_AUTH = {"principal", "kind", "invocation_id", "executor", "expires_at"}
_EXECUTOR = {"task_arn", "task_definition_arn", "launch_token"}
_TYPED = {"schema", "operation", "task_id", "request_spec_sha256", "timeout"}
_AUDIENCE = {"kind", "profile_id", "canonical_request_id", "source_request_id",
             "authority_epoch", "expires_at", "company_id", "operation", "projection"}
_AUDIENCE_VARIANTS = {
    "slack": {"workspace_id", "bot_user_id", "user_id", "channel_id", "thread_id", "message_id"},
    "dashboard": {"application_id", "tenant_id", "subject_id", "session_id", "session_epoch", "session_generation"},
}
_NATIVE_REQUEST_LIMIT = 1024
_CONTROL_RESERVE = 4096
_SCHEMA = (
    """CREATE TABLE IF NOT EXISTS aws_context_scopes (
      scope_id TEXT PRIMARY KEY, request_id TEXT NOT NULL, sequence INTEGER NOT NULL,
      epoch INTEGER NOT NULL, version INTEGER NOT NULL, body TEXT NOT NULL,
      grant_sha TEXT NOT NULL, history TEXT NOT NULL, current_context_attempt_id TEXT,
      schema_version INTEGER NOT NULL DEFAULT 1 CHECK(schema_version=1))""",
    """CREATE TABLE IF NOT EXISTS aws_context_bindings (
      attempt_id TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE, scope_id TEXT NOT NULL,
      head_version INTEGER NOT NULL, head_sha TEXT NOT NULL, body TEXT NOT NULL,
      manifest TEXT, manifest_sha TEXT, expected TEXT, observation TEXT,
      schema_version INTEGER NOT NULL DEFAULT 1 CHECK(schema_version=1))""",
    """CREATE TABLE IF NOT EXISTS aws_context_actions (
      attempt_id TEXT NOT NULL, phase TEXT NOT NULL, reservation_id TEXT NOT NULL,
      invocation_id TEXT NOT NULL, actor TEXT NOT NULL, expires_at REAL NOT NULL,
      body TEXT NOT NULL, schema_version INTEGER NOT NULL DEFAULT 1 CHECK(schema_version=1),
      PRIMARY KEY(attempt_id,phase,reservation_id))""",
    """CREATE UNIQUE INDEX IF NOT EXISTS aws_context_once
      ON aws_context_actions(attempt_id,phase)
      WHERE phase IN ('activation','start','delivery')""",
    """CREATE TABLE IF NOT EXISTS aws_context_native_requests (
      source_key TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE, scope_id TEXT NOT NULL,
      descriptor TEXT NOT NULL, host_principal TEXT NOT NULL, host_invocation TEXT NOT NULL,
      issuer TEXT NOT NULL, expired INTEGER NOT NULL DEFAULT 0 CHECK(expired IN (0,1)))""",
    """CREATE TABLE IF NOT EXISTS aws_context_retention_control (
      singleton INTEGER PRIMARY KEY CHECK(singleton=1), high_water REAL NOT NULL,
      cleanup_generation INTEGER NOT NULL, checkpointed_generation INTEGER NOT NULL,
      pending INTEGER NOT NULL CHECK(pending IN (0,1)))""",
)


class ArtifactError(StoreError):
    def __init__(self, code, *, uncertain=False):
        self.code = code
        self.uncertain = uncertain
        super().__init__(code)


class _ExpiredAudience(Exception):
    """Only raised before phase writes; its terminal marker must commit."""


def _deny(code):
    raise ArtifactError(code)


def _guard(fn):
    @functools.wraps(fn)
    def run(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except ArtifactError:
            raise
        except Exception:
            raise ArtifactError("operation_unknown", uncertain=True) from None
    return run


def _number(value):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        _deny("invalid_number")
    return value


def _integer(value, low, high):
    if type(value) is not int or not low <= value <= high:
        _deny("invalid_integer")
    return value


def _text(value, limit=512):
    if type(value) is not str or not 0 < len(value) <= limit or any(ord(x) < 32 for x in value):
        _deny("invalid_text")
    return value


def _hex(value, n=64):
    if type(value) is not str or re.fullmatch("[0-9a-f]{" + str(n) + "}", value) is None:
        _deny("invalid_digest")
    return value


def _fields(value, keys):
    if type(value) is not dict or set(value) != set(keys):
        _deny("invalid_fields")


def _copy(value, limit=65536):
    try:
        data = encoded(value)
        if len(data) > limit:
            _deny("input_too_large")
        return json.loads(data)
    except ArtifactError:
        raise
    except Exception:
        _deny("invalid_json")


def _parse(data, limit):
    if type(data) is not bytes or not 0 < len(data) <= limit:
        _deny("input_too_large")
    def pairs(items):
        out = {}
        for k, v in items:
            if k in out:
                _deny("duplicate_field")
            out[k] = v
        return out
    def finite(text):
        return _number(float(text)) if not text.startswith("-") else _finite_signed(text)
    def constant(_):
        _deny("invalid_number")
    try:
        obj = json.loads(data.decode("utf-8"), object_pairs_hook=pairs,
                         parse_float=finite, parse_constant=constant)
        if type(obj) is not dict:
            _deny("invalid_json")
        encoded(obj)  # Reject nonfinite values and unencodable nested content.
        return obj
    except ArtifactError:
        raise
    except Exception:
        _deny("invalid_json")


def _finite_signed(text):
    value = float(text)
    if not math.isfinite(value):
        _deny("invalid_number")
    return value


def _executor(value):
    _fields(value, _EXECUTOR)
    _text(value["task_arn"])
    _text(value["task_definition_arn"])
    _hex(value["launch_token"], 32)


def _projection(value, limits):
    _fields(value, {"company_id", "evaluation_time", "limit"})
    if type(value["company_id"]) is not str or re.fullmatch("[A-Za-z0-9_.:-]{1,128}", value["company_id"]) is None:
        _deny("invalid_company")
    _number(value["evaluation_time"])
    _integer(value["limit"], 1, limits["row_limit"])


class CanonicalContextActions:
    def __init__(self, canonical: CanonicalAWS, *, authenticate=None, clock=None,
                 scope_controllers=(), host_principals=(), limits=None, enabled=False,
                 resolve_audience=None, monotonic_clock=None):
        self.canonical = canonical
        self.store = canonical.store
        self.authenticate = authenticate
        self.clock = clock
        self.scope_controllers = frozenset(scope_controllers)
        self.host_principals = frozenset(host_principals)
        self.limits = _copy(limits) if limits is not None else None
        self.enabled = enabled is True
        self._initialized = False
        self._acknowledged_bindings = set()
        self.resolve_audience = resolve_audience
        self.monotonic_clock = monotonic_clock or time.monotonic
        self._native = {}
        self._issuer = uuid.uuid4().hex
        self._cleanup_verified = False
        self._last_effective = _number(clock()) if callable(clock) else 0
        self._last_monotonic = _number(self.monotonic_clock())

    def _secure_delete(self):
        self.store.conn.execute("PRAGMA main.secure_delete=ON")
        if self.store.conn.execute("PRAGMA main.secure_delete").fetchone()[0] != 1:
            _deny("secure_delete_unavailable")

    @contextlib.contextmanager
    def _transaction(self):
        expired = False
        try:
            self._secure_delete()
            with self.store._transaction():
                try:
                    yield
                except _ExpiredAudience:
                    # The signal is used only by preflight, before action writes.
                    # Store commits the irreversible expiry marker on normal exit.
                    expired = True
        except ArtifactError:
            raise
        except BaseException:
            # SQL/commit uncertainty can roll back the durable cleanup latch
            # after a native association was irreversibly released. Only a
            # fresh controller reconciliation may restore local admission.
            self._cleanup_verified = False
            raise
        if expired:
            _deny("audience_expired")

    def _effective_time(self):
        wall, mono = _number(self.clock()), _number(self.monotonic_clock())
        if mono < self._last_monotonic:
            _deny("monotonic_clock_regressed")
        row = self.store.conn.execute(
            "SELECT high_water FROM aws_context_retention_control WHERE singleton=1").fetchone()
        if row is None:
            _deny("retention_control_missing")
        now = max(wall, max(self._last_effective, row[0]) + mono - self._last_monotonic)
        # This pair deliberately survives denied or rolled-back transactions.
        self._last_effective, self._last_monotonic = now, mono
        if self.store.conn.in_transaction:
            self.store.conn.execute(
                "UPDATE aws_context_retention_control SET high_water=MAX(high_water,?) WHERE singleton=1", (now,))
        return now

    def _cleanup_pending(self):
        self.store.conn.execute("""UPDATE aws_context_retention_control SET
            cleanup_generation=cleanup_generation+1,pending=1 WHERE singleton=1""")
        self._cleanup_verified = False

    def _retention_ready(self):
        row = self.store.conn.execute(
            "SELECT pending FROM aws_context_retention_control WHERE singleton=1").fetchone()
        if not self._cleanup_verified or row is None or row[0]:
            _deny("retention_reconciliation_required")

    def _descriptor(self, caller, handle, now):
        if not callable(self.resolve_audience):
            _deny("audience_resolver_unavailable")
        try:
            value = _copy(self.resolve_audience(caller, handle, now), 8192)
        except Exception:
            _deny("audience_unavailable")
        if type(value) is not dict or type(value.get("kind")) is not str or value["kind"] not in _AUDIENCE_VARIANTS:
            _deny("audience_unavailable")
        _fields(value, _AUDIENCE | _AUDIENCE_VARIANTS[value["kind"]])
        for key, item in value.items():
            if key in {"projection", "authority_epoch", "session_epoch", "expires_at"}:
                continue
            if key == "thread_id" and item is None:
                continue
            _text(item, 128)
        _hex(value["canonical_request_id"], 32)
        _integer(value["authority_epoch"], 1, 2**53)
        if value["kind"] == "dashboard":
            _integer(value["session_epoch"], 1, 2**53)
        _number(value["expires_at"])
        _projection(value["projection"], self.limits)
        if (value["operation"] != "nonpricing.context"
                or value["company_id"] != value["projection"]["company_id"]
                or now >= value["expires_at"]):
            _deny("audience_unavailable")
        return value

    def _expire_native(self, request_id):
        # Retired native objects may retain an entire request payload graph.
        # Never restore this association if the following SQL later fails.
        self._cleanup_verified = False
        self._native.pop(request_id, None)
        cursor = self.store.conn.execute(
            "UPDATE aws_context_native_requests SET expired=1 WHERE request_id=? AND expired=0", (request_id,))
        if cursor.rowcount:
            self._cleanup_pending()
        raise _ExpiredAudience()

    def _check_audience(self, scope, ctx, now, *, caller=None, handle=None, ready=True):
        if "audience" not in scope:
            return
        if caller is not None:
            current_caller = self._auth(caller, {ctx["kind"]})
            if encoded(current_caller) != encoded(ctx) or now >= current_caller["expires_at"]:
                _deny("caller_changed")
        desc = scope["audience"]
        row = self.store.conn.execute(
            "SELECT * FROM aws_context_native_requests WHERE request_id=?", (scope["request_id"],)).fetchone()
        if row is None or row["scope_id"] != scope["scope_id"] or row["descriptor"] != encoded(desc).decode():
            _deny("native_request_missing")
        if row["expired"] or now >= min(scope["not_after"], desc["expires_at"]):
            self._expire_native(scope["request_id"])
        pair = self._native.get(scope["request_id"])
        if pair is None or row["issuer"] != self._issuer:
            _deny("native_association_lost")
        original_caller, original_handle = pair
        if ctx["kind"] == "host" and (caller is not original_caller
                or (handle is not None and handle is not original_handle)):
            _deny("wrong_native_request")
        host = self._auth(original_caller, {"host"})
        if now >= host["expires_at"]:
            self._expire_native(scope["request_id"])
        if host["principal"] != row["host_principal"] or host["invocation_id"] != row["host_invocation"]:
            _deny("native_host_changed")
        current = self._descriptor(original_caller, original_handle, now)
        if encoded(current) != encoded(desc):
            _deny("audience_changed")
        if ready:
            self._retention_ready()

    def _ready(self, *, schema=True):
        if (not self.enabled or not callable(self.authenticate) or not callable(self.clock)
                or self.limits is None or not self.scope_controllers or not self.host_principals):
            _deny("unconfigured")
        _fields(self.limits, TRIAL_LIMITS)
        for name, ceiling in TRIAL_LIMITS.items():
            _integer(self.limits[name], 1, ceiling)
        if schema and not self._initialized:
            _deny("uninitialized")

    def _auth(self, caller, kinds):
        self._ready()
        try:
            ctx = _copy(self.authenticate(caller), 4096)
        except Exception:
            _deny("unauthenticated")
        _fields(ctx, _AUTH)
        _text(ctx["principal"])
        _hex(ctx["invocation_id"], 32)
        _number(ctx["expires_at"])
        if ctx["kind"] not in kinds:
            _deny("wrong_caller")
        if ctx["kind"] == "controller":
            if ctx["principal"] not in self.scope_controllers or ctx["executor"] is not None:
                _deny("wrong_controller")
        elif ctx["kind"] == "host":
            if ctx["principal"] not in self.host_principals or ctx["executor"] is not None:
                _deny("wrong_host")
        elif ctx["kind"] == "worker":
            _executor(ctx["executor"])
        else:
            _deny("wrong_caller")
        return ctx

    def _now(self, ctx):
        now = self._effective_time() if callable(self.resolve_audience) else _number(self.clock())
        if now >= ctx["expires_at"]:
            _deny("caller_expired")
        return now

    def _event(self, envelope, phase, principal, value):
        self.canonical._event(envelope, "context-" + phase, principal, sha(encoded(value)))
        self.store.conn.execute("UPDATE aws_attempts SET version=version+1 WHERE attempt_id=?",
                                (envelope["attempt_id"],))

    def _current_attempt(self, envelope, *, current=True):
        try:
            r = self.canonical._attempt(envelope)
            if current:
                self.canonical._current(r, envelope)
        except ReceiptError:
            _deny("canonical_changed")
        if current and (r["withdrawn"] or r["state"] in ("accepted", "retry_authorized")):
            _deny("attempt_withdrawn_or_closed")
        return r

    def _binding(self, envelope, ctx, now, *, current=True, active=True, caller=None):
        r = self._current_attempt(envelope, current=current)
        row = self.store.conn.execute("SELECT * FROM aws_context_bindings WHERE attempt_id=?",
                                      (envelope["attempt_id"],)).fetchone()
        if row is None:
            _deny("binding_missing")
        b = dict(row)
        body = json.loads(b["body"])
        if encoded(body.get("limits")) != encoded(self.limits):
            _deny("bound_policy_changed")
        if encoded(body["envelope"]) != encoded(envelope):
            _deny("binding_conflict")
        head = self.store.conn.execute("SELECT * FROM aws_context_scopes WHERE scope_id=?",
                                       (b["scope_id"],)).fetchone()
        if head is None:
            _deny("scope_missing")
        scope = json.loads(head["body"])
        if current:
            self._check_audience(scope, ctx, now, caller=caller)
            if "audience" in scope and now >= body["spec"]["deadline_at"]:
                self._expire_native(b["request_id"])
            if (head["request_id"] != b["request_id"] or head["version"] != b["head_version"]
                    or head["grant_sha"] != b["head_sha"]
                    or encoded(scope) != encoded(body["scope"])
                    or scope["revoked"] or now >= scope["not_after"]
                    or now >= body["spec"]["deadline_at"]):
                _deny("scope_changed_or_expired")
        if active and not b["manifest"]:
            _deny("activation_missing")
        if ctx["kind"] == "worker":
            if not r["executor"] or encoded(ctx["executor"]) != encoded(json.loads(r["executor"])):
                _deny("wrong_executor")
        if active:
            expected = json.loads(b["expected"])
            if not r["executor"] or encoded(expected["executor"]) != encoded(json.loads(r["executor"])):
                _deny("executor_binding_changed")
        return r, b, body, dict(head)

    def _action(self, attempt, phase):
        return self.store.conn.execute(
            "SELECT * FROM aws_context_actions WHERE attempt_id=? AND phase=?",
            (attempt, phase)).fetchone()

    def _budget(self, extra):
        # Logical private-ledger budget; physical/WAL and transport cleanup are
        # root deployment responsibilities, not claimed by this source port.
        used = 0
        for table, cols in (
            ("aws_context_scopes", ("body", "history")),
            ("aws_context_bindings", ("body", "manifest", "expected", "observation")),
            ("aws_context_actions", ("body",)),
            ("aws_context_native_requests", ("source_key", "request_id", "scope_id", "descriptor", "host_principal", "host_invocation", "issuer")),
            ("aws_context_retention_control", ("high_water", "cleanup_generation", "checkpointed_generation", "pending")),
        ):
            expr = "+".join("COALESCE(LENGTH(" + c + "),0)" for c in cols)
            used += self.store.conn.execute("SELECT COALESCE(SUM(" + expr + "),0) FROM " + table).fetchone()[0]
        bindings = self.store.conn.execute("SELECT COUNT(*) FROM aws_context_bindings").fetchone()[0]
        used += (bindings + 1) * _CONTROL_RESERVE
        if used + extra > self.limits["artifact_max_bytes"]:
            _deny("ledger_budget_exceeded")

    def _insert_action(self, envelope, phase, reservation, ctx, expires, body, *, control=False):
        data = encoded(body).decode()
        if not control:
            self._budget(len(data.encode()))
        self.store.conn.execute(
            "INSERT INTO aws_context_actions(attempt_id,phase,reservation_id,invocation_id,actor,expires_at,body) VALUES(?,?,?,?,?,?,?)",
            (envelope["attempt_id"], phase, reservation, ctx["invocation_id"],
             ctx["principal"], expires, data))
        self._event(envelope, phase, ctx["principal"], body)

    @_guard
    def initialize_for_candidate(self):
        """Explicit additive schema only. Never chooses/opens a Store path."""
        self._ready(schema=False)
        with self._transaction():
            for sql in _SCHEMA:
                self.store.conn.execute(sql)
            for sql in _SCHEMA:
                kind = "index" if "UNIQUE INDEX" in sql else "table"
                name = sql.split("EXISTS ", 1)[1].split()[0]
                actual = self.store.conn.execute(
                    "SELECT sql FROM sqlite_master WHERE type=? AND name=?", (kind, name)).fetchone()
                normalize = lambda s: "".join(s.replace("IF NOT EXISTS ", "").lower().split())
                if actual is None or normalize(actual[0]) != normalize(sql):
                    _deny("schema_mismatch")
        with self._transaction():
            self.store.conn.execute("""INSERT OR IGNORE INTO aws_context_retention_control
                (singleton,high_water,cleanup_generation,checkpointed_generation,pending) VALUES(1,?,0,0,1)""",
                (self._last_effective,))
        self._initialized = True

    @_guard
    def set_audience_scope(self, caller, request_spec, *, host_caller, audience_handle,
                           expected_scope_version=0):
        ctx = self._auth(caller, {"controller"})
        host = self._auth(host_caller, {"host"})
        spec = _copy(request_spec, 8192)
        _fields(spec, _SCOPE - {"target"})
        _text(spec["scope_id"], 128)
        _hex(spec["request_id"], 32)
        for key in ("sequence", "grant_epoch"):
            _integer(spec[key], 1, 2**53)
        _integer(expected_scope_version, 0, 2**53)
        _projection(spec["projection"], self.limits)
        _number(spec["not_after"])
        if spec["revoked"] is not False or spec["operation"] != "nonpricing.context":
            _deny("unsupported_scope")
        with self._transaction():
            now = self._effective_time()
            ctx = self._auth(caller, {"controller"})
            host = self._auth(host_caller, {"host"})
            if now >= min(ctx["expires_at"], host["expires_at"]):
                _deny("caller_expired")
            known = self.store.conn.execute("SELECT * FROM aws_context_native_requests WHERE request_id=?",
                                            (spec["request_id"],)).fetchone()
            if known is not None and (known["expired"] or now >= json.loads(known["descriptor"])["expires_at"]):
                self._expire_native(spec["request_id"])
            self._retention_ready()
            desc = self._descriptor(host_caller, audience_handle, now)
            if (spec["request_id"] != desc["canonical_request_id"]
                    or encoded(spec["projection"]) != encoded(desc["projection"])
                    or not now < spec["not_after"] <= min(desc["expires_at"], now + self.limits["request_ttl_s"])):
                _deny("audience_scope_mismatch")
            identity = [desc[k] for k in (("kind", "profile_id", "workspace_id", "source_request_id", "operation")
                if desc["kind"] == "slack" else ("kind", "profile_id", "application_id", "tenant_id",
                "session_generation", "session_id", "source_request_id", "operation"))]
            source_key = sha(encoded(identity))
            spec["audience"] = desc
            spec["target"] = (dict(profile=desc["profile_id"], platform="slack", workspace=desc["workspace_id"],
                channel=desc["channel_id"], thread=desc["thread_id"]) if desc["kind"] == "slack" else
                dict(profile=desc["profile_id"], platform="dashboard", workspace=desc["tenant_id"],
                     channel=desc["application_id"], thread=desc["session_id"]))
            digest = sha(encoded(spec))
            old = self.store.conn.execute("SELECT * FROM aws_context_scopes WHERE scope_id=?", (spec["scope_id"],)).fetchone()
            existing = self.store.conn.execute("SELECT * FROM aws_context_native_requests WHERE source_key=? OR request_id=?",
                (source_key, spec["request_id"])).fetchall()
            if existing:
                pair = self._native.get(spec["request_id"])
                if (len(existing) != 1 or existing[0]["source_key"] != source_key
                        or existing[0]["request_id"] != spec["request_id"] or existing[0]["issuer"] != self._issuer
                        or existing[0]["expired"] or pair is None or pair[0] is not host_caller or pair[1] is not audience_handle
                        or old is None or old["version"] != expected_scope_version or old["body"] != encoded(spec).decode()):
                    _deny("native_request_replayed")
                self._check_audience(spec, host, now, caller=host_caller, handle=audience_handle)
                return {"scope_version": old["version"], "grant_sha256": digest}
            if self.store.conn.execute("SELECT COUNT(*) FROM aws_context_native_requests").fetchone()[0] >= _NATIVE_REQUEST_LIMIT:
                _deny("native_request_ledger_full")
            if old is None:
                if expected_scope_version or spec["sequence"] != 1 or spec["grant_epoch"] != 1:
                    _deny("scope_cas")
                version, history = 1, []
            else:
                previous = json.loads(old["body"])
                history = json.loads(old["history"])
                if (old["version"] != expected_scope_version or "audience" not in previous
                        or spec["request_id"] in history or spec["sequence"] != old["sequence"] + 1
                        or spec["grant_epoch"] != old["epoch"] + 1):
                    _deny("scope_cas")
                version = old["version"] + 1
            if len(history) >= 128:
                _deny("scope_history_full")
            history.append(spec["request_id"])
            self._budget(len(encoded(spec)) + len(encoded(desc)) + len(encoded(history)) + 2048)
            if old is not None:
                self._native.pop(old["request_id"], None)
                self.store.conn.execute("UPDATE aws_context_native_requests SET expired=1 WHERE request_id=?", (old["request_id"],))
                if old["current_context_attempt_id"] is not None:
                    self._cleanup_pending()
            self.store.conn.execute("""INSERT INTO aws_context_native_requests
                (source_key,request_id,scope_id,descriptor,host_principal,host_invocation,issuer)
                VALUES(?,?,?,?,?,?,?)""", (source_key,spec["request_id"],spec["scope_id"],encoded(desc).decode(),
                host["principal"],host["invocation_id"],self._issuer))
            self.store.conn.execute("""INSERT INTO aws_context_scopes
                (scope_id,request_id,sequence,epoch,version,body,grant_sha,history) VALUES(?,?,?,?,?,?,?,?)
                ON CONFLICT(scope_id) DO UPDATE SET request_id=excluded.request_id,sequence=excluded.sequence,
                epoch=excluded.epoch,version=excluded.version,body=excluded.body,grant_sha=excluded.grant_sha,
                history=excluded.history,current_context_attempt_id=NULL""", (spec["scope_id"],spec["request_id"],
                spec["sequence"],spec["grant_epoch"],version,encoded(spec).decode(),digest,encoded(history).decode()))
        # A failed/unknown commit cannot reconstruct this process-local authority.
        self._native[spec["request_id"]] = (host_caller, audience_handle)
        return {"scope_version": version, "grant_sha256": digest}

    @_guard
    def revoke_audience_scope(self, caller, scope_id, *, expected_scope_version):
        ctx = self._auth(caller, {"controller"})
        _text(scope_id, 128)
        _integer(expected_scope_version, 1, 2**53)
        with self._transaction():
            ctx = self._auth(caller, {"controller"})
            self._now(ctx)
            old = self.store.conn.execute("SELECT * FROM aws_context_scopes WHERE scope_id=?", (scope_id,)).fetchone()
            if old is None or old["version"] != expected_scope_version:
                _deny("scope_cas")
            spec = json.loads(old["body"])
            if "audience" not in spec or spec["revoked"]:
                _deny("external_scope_required")
            spec.update(revoked=True, grant_epoch=spec["grant_epoch"]+1)
            digest, version = sha(encoded(spec)), old["version"]+1
            self.store.conn.execute("UPDATE aws_context_scopes SET body=?,grant_sha=?,epoch=?,version=?,current_context_attempt_id=NULL WHERE scope_id=?",
                (encoded(spec).decode(),digest,spec["grant_epoch"],version,scope_id))
            self._native.pop(spec["request_id"], None)
            self.store.conn.execute("UPDATE aws_context_native_requests SET expired=1 WHERE request_id=?", (spec["request_id"],))
            self._cleanup_pending()
        return {"scope_version": version, "grant_sha256": digest}

    @_guard
    def set_scope(self, caller, scope_spec, expected_scope_version=0):
        ctx = self._auth(caller, {"controller"})
        spec = _copy(scope_spec)
        _fields(spec, _SCOPE)
        _text(spec["scope_id"], 128)
        _hex(spec["request_id"], 32)
        _integer(spec["sequence"], 1, 2**53)
        _integer(spec["grant_epoch"], 1, 2**53)
        _integer(expected_scope_version, 0, 2**53)
        if spec["operation"] != "nonpricing.context" or type(spec["revoked"]) is not bool:
            _deny("unsupported_scope")
        _fields(spec["target"], {"profile", "platform", "workspace", "channel", "thread"})
        for key in ("profile", "platform", "workspace", "channel"):
            _text(spec["target"][key], 128)
        if spec["target"]["thread"] is not None:
            _text(spec["target"]["thread"], 128)
        if spec["target"]["platform"] != "internal":
            _deny("synthetic_internal_scope_only")
        _projection(spec["projection"], self.limits)
        _number(spec["not_after"])
        digest = sha(encoded(spec))
        with self._transaction():
            now = self._now(ctx)
            old = self.store.conn.execute("SELECT * FROM aws_context_scopes WHERE scope_id=?",
                                          (spec["scope_id"],)).fetchone()
            if old is None:
                if expected_scope_version != 0 or spec["sequence"] != 1 or spec["grant_epoch"] != 1 or spec["revoked"]:
                    _deny("scope_cas")
                history = []
                version = 1
            else:
                if old["version"] != expected_scope_version:
                    _deny("scope_cas")
                previous = json.loads(old["body"])
                if "audience" in previous:
                    _deny("external_scope_downgrade")
                history = json.loads(old["history"])
                version = old["version"] + 1
                if spec["request_id"] == old["request_id"]:
                    wanted = dict(previous, revoked=True, grant_epoch=previous["grant_epoch"] + 1)
                    if previous["revoked"] or encoded(spec) != encoded(wanted):
                        _deny("retired_or_rebound_request")
                else:
                    if (spec["request_id"] in history or spec["revoked"]
                            or spec["sequence"] != old["sequence"] + 1
                            or spec["grant_epoch"] != old["epoch"] + 1):
                        _deny("retired_or_rebound_request")
            if not spec["revoked"] and not now < spec["not_after"] <= now + self.limits["request_ttl_s"]:
                _deny("scope_expiry")
            if spec["request_id"] not in history:
                if len(history) >= 128:
                    _deny("scope_history_full")
                history.append(spec["request_id"])
            if not spec["revoked"]:
                self._budget(len(encoded(spec)) + len(encoded(history)))
            self.store.conn.execute(
                """INSERT INTO aws_context_scopes(scope_id,request_id,sequence,epoch,version,body,grant_sha,history)
                VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(scope_id) DO UPDATE SET
                request_id=excluded.request_id,sequence=excluded.sequence,epoch=excluded.epoch,
                version=excluded.version,body=excluded.body,grant_sha=excluded.grant_sha,
                history=excluded.history,current_context_attempt_id=NULL""",
                (spec["scope_id"], spec["request_id"], spec["sequence"], spec["grant_epoch"],
                 version, encoded(spec).decode(), digest, encoded(history).decode()))
            affected = self.store.conn.execute(
                "SELECT a.envelope FROM aws_attempts a JOIN aws_context_bindings b ON a.attempt_id=b.attempt_id WHERE b.scope_id=?",
                (spec["scope_id"],)).fetchall()
            for row in affected:
                self._event(json.loads(row["envelope"]), "scope-changed", ctx["principal"], {"grant_sha256": digest})
        return {"scope_version": version, "grant_sha256": digest}

    @_guard
    def bind_request(self, caller, envelope, typed_payload_bytes, request_spec_bytes, *, expected_scope_version):
        ctx = self._auth(caller, {"host"})
        env = _copy(envelope)
        payload = _parse(typed_payload_bytes, 8192)
        spec = _parse(request_spec_bytes, 8192)
        _fields(payload, _TYPED)
        _fields(spec, _SPEC)
        if (type(payload["schema"]) is not int or payload["schema"] != 1
                or payload["operation"] != "nonpricing.context" or payload["task_id"] != env["task_id"]
                or sha(encoded(payload)) != env["spec_sha256"]
                or payload["request_spec_sha256"] != sha(encoded(spec))):
            _deny("preapproval_commitment_mismatch")
        _integer(payload["timeout"], 1, self.limits["cli_timeout_s"])
        if type(spec["schema_version"]) is not int or spec["schema_version"] != 1 or spec["operation"] != "nonpricing.context":
            _deny("unsupported_operation")
        _hex(spec["request_id"], 32)
        for key in ("input_sha256", "cli_source_manifest_sha256", "audience_grant_sha256"):
            _hex(spec[key])
        _integer(spec["input_bytes"], 1, self.limits["input_max_bytes"])
        _integer(spec["result_max_bytes"], 1, self.limits["result_max_bytes"])
        _projection(spec["projection"], self.limits)
        _number(spec["deadline_at"])
        _integer(expected_scope_version, 1, 2**53)
        with self._transaction():
            now = self._now(ctx)
            r = self._current_attempt(env)
            try:
                self.canonical._dispatcher(ctx["principal"], env)
            except ReceiptError:
                _deny("wrong_dispatcher")
            if r["admitted"] or r["state"] != "approved_effects_unknown":
                _deny("binding_must_precede_admission")
            heads = self.store.conn.execute(
                "SELECT * FROM aws_context_scopes WHERE request_id=?", (spec["request_id"],)).fetchall()
            if len(heads) != 1:
                _deny("scope_missing_or_ambiguous")
            head = dict(heads[0])
            scope = json.loads(head["body"])
            self._check_audience(scope, ctx, now, caller=caller)
            if (head["version"] != expected_scope_version or head["grant_sha"] != spec["audience_grant_sha256"]
                    or scope["revoked"] or encoded(scope["projection"]) != encoded(spec["projection"])
                    or not now < spec["deadline_at"] <= scope["not_after"]
                    or spec["deadline_at"] > now + self.limits["request_ttl_s"]):
                _deny("scope_commitment_mismatch")
            body = {"envelope": env, "payload": payload, "spec": spec, "scope": scope,
                    "limits": _copy(self.limits),
                    "late_evidence_deadline": spec["deadline_at"] + self.limits["late_evidence_s"]}
            old = self.store.conn.execute("SELECT * FROM aws_context_bindings WHERE attempt_id=?",
                                          (env["attempt_id"],)).fetchone()
            if old is not None:
                if old["body"] != encoded(body).decode():
                    _deny("binding_conflict")
                return {"status": "observed", "request_id": spec["request_id"]}
            self._budget(len(encoded(body)))
            self.store.conn.execute(
                "INSERT INTO aws_context_bindings(attempt_id,request_id,scope_id,head_version,head_sha,body) VALUES(?,?,?,?,?,?)",
                (env["attempt_id"], spec["request_id"], scope["scope_id"], head["version"],
                 head["grant_sha"], encoded(body).decode()))
            self._event(env, "bound", ctx["principal"], {"request_id": spec["request_id"]})
        # An exception/unknown commit does not reconstruct this local acknowledgement.
        self._acknowledged_bindings.add(env["attempt_id"])
        return {"status": "bound", "request_id": spec["request_id"]}

    def dispatcher_port(self, caller):
        self._auth(caller, {"host"})
        return _DispatcherPort(self, caller)

    @_guard
    def _admit(self, caller, envelope, actor, *, request_sha256):
        ctx = self._auth(caller, {"host"})
        env = _copy(envelope)
        _hex(request_sha256)
        if ctx["principal"] != actor or env["attempt_id"] not in self._acknowledged_bindings:
            _deny("binding_ack_required")
        # Do not call CanonicalAWS.admit in a second/nested transaction. Preserve
        # its exact predicates and SQL here, with the additional context fence.
        with self._transaction():
            now = self._now(ctx)
            r, _, _, _ = self._binding(env, ctx, now, active=False, caller=caller)
            try:
                self.canonical._dispatcher(actor, env)
            except ReceiptError:
                _deny("wrong_dispatcher")
            if r["withdrawn"] or r["admitted"] or r["state"] != "approved_effects_unknown":
                _deny("admission_spent_or_withdrawn")
            limit = self.store.conn.execute("SELECT max_inflight FROM aws_limits WHERE singleton=1").fetchone()[0]
            active = self.store.conn.execute(
                "SELECT COUNT(*) FROM aws_attempts WHERE admitted=1 AND state NOT IN ('accepted','retry_authorized')").fetchone()[0]
            if active >= min(limit, 1):
                _deny("capacity_held")
            self.store.conn.execute(
                "UPDATE aws_attempts SET admitted=1,request_sha256=?,state='admitted_effects_unknown',version=version+1 WHERE attempt_id=?",
                (request_sha256, env["attempt_id"]))
            self.canonical._event(env, "admitted", actor)
        self._acknowledged_bindings.discard(env["attempt_id"])
        return env

    @_guard
    def activate(self, caller, envelope, executor, binding_manifest_bytes):
        ctx = self._auth(caller, {"host"})
        env = _copy(envelope)
        executor = _copy(executor)
        _executor(executor)
        manifest = _parse(binding_manifest_bytes, 8192)
        _fields(manifest, {"request_id", "request_spec_sha256", "canonical_sha256", "executor", "grant"})
        with self._transaction():
            now = self._now(ctx)
            r, b, body, head = self._binding(env, ctx, now, active=False, caller=caller)
            if ctx["principal"] != env["dispatcher_id"]:
                _deny("wrong_dispatcher")
            if not r["admitted"] or not r["executor"] or encoded(json.loads(r["executor"])) != encoded(executor):
                _deny("registered_executor_required")
            if r["state"] != "admitted_effects_unknown":
                _deny("wrong_phase")
            expected_manifest = {
                "request_id": b["request_id"], "request_spec_sha256": sha(encoded(body["spec"])),
                "canonical_sha256": sha(encoded(env)), "executor": executor,
                "grant": {"scope_id": b["scope_id"], "request_id": b["request_id"],
                          "sequence": head["sequence"], "grant_epoch": head["epoch"],
                          "grant_sha256": head["grant_sha"]}}
            if encoded(manifest) != encoded(expected_manifest):
                _deny("manifest_conflict")
            digest = sha(encoded(manifest))
            if b["manifest"]:
                if b["manifest_sha"] != digest:
                    _deny("manifest_conflict")
                return json.loads(b["expected"])
            expected = {"request_id": b["request_id"], "request_spec_sha256": manifest["request_spec_sha256"],
                        "manifest_sha256": digest, "canonical_sha256": sha(encoded(env)),
                        "spec_sha256": env["spec_sha256"], "input_sha256": body["spec"]["input_sha256"],
                        "source_sha256": body["spec"]["cli_source_manifest_sha256"], "executor": executor}
            self._budget(len(encoded(manifest)) + len(encoded(expected)))
            self.store.conn.execute("UPDATE aws_context_bindings SET manifest=?,manifest_sha=?,expected=? WHERE attempt_id=?",
                                    (encoded(manifest).decode(), digest, encoded(expected).decode(), env["attempt_id"]))
            self._insert_action(env, "activation", "activation", ctx,
                                min(body["spec"]["deadline_at"], ctx["expires_at"]), {"manifest_sha256": digest})
        return expected

    def _worker_phase(self, env, ctx, now, request_id, invocation_id, expected_manifest, *, caller=None):
        _hex(request_id, 32)
        _hex(invocation_id, 32)
        _hex(expected_manifest)
        if ctx["invocation_id"] != invocation_id:
            _deny("wrong_invocation")
        r, b, body, head = self._binding(env, ctx, now, caller=caller)
        if (b["request_id"] != request_id or b["manifest_sha"] != expected_manifest
                or r["state"] != "admitted_effects_unknown"):
            _deny("wrong_phase_or_binding")
        return r, b, body, head

    @_guard
    def authorize_fetch(self, caller, envelope, request_id, invocation_id, expected_manifest, *, reservation_id):
        ctx = self._auth(caller, {"worker"})
        env = _copy(envelope)
        _hex(reservation_id, 32)
        with self._transaction():
            now = self._now(ctx)
            _, b, body, _ = self._worker_phase(env, ctx, now, request_id, invocation_id, expected_manifest, caller=caller)
            old = self.store.conn.execute(
                "SELECT * FROM aws_context_actions WHERE attempt_id=? AND phase='fetch' AND reservation_id=?",
                (env["attempt_id"], reservation_id)).fetchone()
            if old is not None:
                return False
            count = self.store.conn.execute(
                "SELECT COUNT(*) FROM aws_context_actions WHERE attempt_id=? AND phase='fetch'",
                (env["attempt_id"],)).fetchone()[0]
            if count >= body["limits"]["max_fetches"]:
                _deny("fetch_budget_exhausted")
            self._insert_action(env, "fetch", reservation_id, ctx,
                min(body["spec"]["deadline_at"], ctx["expires_at"], now + min(body["payload"]["timeout"], body["limits"]["cli_timeout_s"])),
                {"manifest_sha256": b["manifest_sha"], "input_sha256": body["spec"]["input_sha256"]})
        return True

    def _fetch_progress(self, attempt_id, fetch, input_bytes):
        rows = self.store.conn.execute("SELECT * FROM aws_context_actions WHERE attempt_id=? AND phase='fetch_chunk'",
                                       (attempt_id,)).fetchall()
        if len(rows) > 64:
            _deny("chunk_ledger_invalid")
        selected = []
        for row in rows:
            data = json.loads(row["body"])
            if data["fetch_reservation_id"] == fetch["reservation_id"]:
                if row["actor"] != fetch["actor"] or row["invocation_id"] != fetch["invocation_id"]:
                    _deny("chunk_ledger_invalid")
                selected.append(data)
        offset = 0
        for data in sorted(selected, key=lambda v: v["offset"]):
            if data["offset"] != offset or data["length"] != min(32768, input_bytes-offset):
                _deny("chunk_ledger_invalid")
            offset += data["length"]
        return offset

    @_guard
    def authorize_fetch_chunk(self, caller, envelope, request_id, invocation_id, expected_manifest,
                              *, reservation_id, offset, length):
        ctx = self._auth(caller, {"worker"})
        env = _copy(envelope)
        _hex(reservation_id, 32)
        _integer(offset, 0, self.limits["input_max_bytes"]-1)
        _integer(length, 1, 32768)
        with self._transaction():
            now = self._now(ctx)
            _, b, body, _ = self._worker_phase(env, ctx, now, request_id, invocation_id, expected_manifest, caller=caller)
            fetch = self.store.conn.execute("""SELECT * FROM aws_context_actions
                WHERE attempt_id=? AND phase='fetch' AND reservation_id=?""", (env["attempt_id"],reservation_id)).fetchone()
            if (fetch is None or fetch["actor"] != ctx["principal"] or fetch["invocation_id"] != invocation_id
                    or now >= fetch["expires_at"] or json.loads(fetch["body"])["manifest_sha256"] != b["manifest_sha"]):
                _deny("fetch_authority_required")
            if self._action(env["attempt_id"], "start") is not None:
                _deny("fetch_after_start")
            size = body["spec"]["input_bytes"]
            progress = self._fetch_progress(env["attempt_id"], fetch, size)
            if offset < progress:
                return False
            if offset != progress or offset >= size or length != min(32768, size-offset):
                _deny("chunk_order_or_size")
            chunk_id = sha(encoded([reservation_id,offset]))
            self._insert_action(env, "fetch_chunk", chunk_id, ctx, fetch["expires_at"],
                {"fetch_reservation_id":reservation_id,"offset":offset,"length":length,"manifest_sha256":b["manifest_sha"]})
        return True

    @_guard
    def authorize_start(self, caller, envelope, invocation_id, expected_manifest):
        ctx = self._auth(caller, {"worker"})
        env = _copy(envelope)
        with self._transaction():
            now = self._now(ctx)
            b0 = self.store.conn.execute("SELECT request_id FROM aws_context_bindings WHERE attempt_id=?",
                                        (env["attempt_id"],)).fetchone()
            if b0 is None:
                _deny("binding_missing")
            _, b, body, _ = self._worker_phase(env, ctx, now, b0["request_id"], invocation_id, expected_manifest, caller=caller)
            if self._action(env["attempt_id"], "start") is not None:
                return False
            fetched = self.store.conn.execute(
                "SELECT 1 FROM aws_context_actions WHERE attempt_id=? AND phase='fetch' AND invocation_id=? AND actor=?",
                (env["attempt_id"], invocation_id, ctx["principal"])).fetchone()
            if fetched is None:
                _deny("input_fetch_required")
            if "audience" in body["scope"]:
                fetches = self.store.conn.execute("""SELECT * FROM aws_context_actions WHERE
                    attempt_id=? AND phase='fetch' AND invocation_id=? AND actor=?""",
                    (env["attempt_id"],invocation_id,ctx["principal"])).fetchall()
                if not any(now < fetch["expires_at"] and self._fetch_progress(env["attempt_id"],fetch,body["spec"]["input_bytes"])
                           == body["spec"]["input_bytes"] for fetch in fetches):
                    _deny("complete_input_fetch_required")
            self._insert_action(env, "start", "start", ctx,
                min(body["spec"]["deadline_at"], ctx["expires_at"], now + min(body["payload"]["timeout"], body["limits"]["cli_timeout_s"])),
                {"manifest_sha256": b["manifest_sha"]})
        return True

    @_guard
    def record_result_observation(self, caller, envelope, binding, stdout_bytes):
        # The exchange supplies its independently authenticated worker subject,
        # not a claimed worker ID from artifact metadata.
        ctx = self._auth(caller, {"worker"})
        env = _copy(envelope)
        expected = _copy(binding, 8192)
        context = _parse(stdout_bytes, self.limits["result_max_bytes"])
        stdout_digest, context_digest = sha(stdout_bytes), sha(encoded(context))
        with self._transaction():
            now = self._now(ctx)
            r, b, body, _ = self._binding(env, ctx, now, current=False)
            if encoded(expected) != encoded(json.loads(b["expected"])):
                _deny("result_binding_conflict")
            if len(stdout_bytes) > body["spec"]["result_max_bytes"]:
                _deny("result_too_large")
            start = self._action(env["attempt_id"], "start")
            if (start is None or start["invocation_id"] != ctx["invocation_id"]
                    or start["actor"] != ctx["principal"]
                    or json.loads(start["body"])["manifest_sha256"] != b["manifest_sha"]):
                _deny("winning_start_required")
            if now >= body["late_evidence_deadline"]:
                _deny("late_evidence_expired")
            observation = {"stdout_sha256": stdout_digest, "context_sha256": context_digest,
                           "stdout_bytes": len(stdout_bytes), "manifest_sha256": b["manifest_sha"],
                           "invocation_id": ctx["invocation_id"], "actor": ctx["principal"],
                           "observed_at": now}
            if b["observation"]:
                old = json.loads(b["observation"])
                if encoded({k: v for k, v in old.items() if k != "observed_at"}) != encoded(
                        {k: v for k, v in observation.items() if k != "observed_at"}):
                    _deny("immutable_result_conflict")
                return False
            if r["state"] == "accepted":
                _deny("attempt_closed")
            # Fixed hash-only observation is reserved at binding admission.
            self.store.conn.execute("UPDATE aws_context_bindings SET observation=? WHERE attempt_id=?",
                                    (encoded(observation).decode(), env["attempt_id"]))
            if r["state"] == "retry_authorized":
                # New late evidence invalidates effects-absent reconciliation.
                # Withdrawal remains set; this never restores action authority.
                self.store.conn.execute("UPDATE aws_attempts SET state='admitted_effects_unknown' WHERE attempt_id=?",
                                        (env["attempt_id"],))
            self._event(env, "result-observed", ctx["principal"], observation)
        return True

    def context_store(self, caller):
        self._auth(caller, {"host"})
        return _ContextStore(self, caller)

    @_guard
    def _commit_context_once(self, caller, binding, stdout_sha256, decoded_context):
        ctx = self._auth(caller, {"host"})
        expected = _copy(binding, 8192)
        _hex(stdout_sha256)
        context = _copy(decoded_context, self.limits["result_max_bytes"])
        with self._transaction():
            now = self._now(ctx)
            request_id = expected.get("request_id")
            _hex(request_id, 32)
            row = self.store.conn.execute("SELECT body FROM aws_context_bindings WHERE request_id=?",
                                          (request_id,)).fetchone()
            if row is None:
                _deny("binding_missing")
            env = json.loads(row["body"])["envelope"]
            r, b, body, head = self._binding(env, ctx, now, caller=caller)
            if encoded(expected) != encoded(json.loads(b["expected"])):
                _deny("result_binding_conflict")
            if self._action(env["attempt_id"], "delivery") is not None:
                return False
            start = self._action(env["attempt_id"], "start")
            if start is None or not b["observation"] or not r["terminal_sha256"] or not r["terminal_result"]:
                _deny("result_lineage_missing")
            observation = json.loads(b["observation"])
            result = json.loads(r["terminal_result"])
            if (r["state"] != "execution_succeeded_pending_review" or not self.canonical._success(result)
                    or result["stdout_sha256"] != stdout_sha256
                    or observation["observed_at"] >= body["spec"]["deadline_at"]
                    or observation["stdout_sha256"] != stdout_sha256
                    or result["stdout_bytes"] != observation["stdout_bytes"]
                    or observation["stdout_bytes"] > body["spec"]["result_max_bytes"]
                    or observation["context_sha256"] != sha(encoded(context))
                    or observation["manifest_sha256"] != b["manifest_sha"]
                    or observation["invocation_id"] != start["invocation_id"]
                    or observation["actor"] != start["actor"]):
                _deny("result_evidence_mismatch")
            # Closed projection schema is checked by the exact frozen consumer.
            # This method is injected only behind that trusted host validator.
            accepted_task = self.store.get(env["task_id"]).to_dict()
            accepted_task.update(state="done", blocked_on="", revision=accepted_task["revision"]+1)
            accepted_task["receipts"] = accepted_task["receipts"] + [
                "independent AWS acceptance terminal sha256=" + r["terminal_sha256"]]
            for key in ("last_check_at", "last_check_ok", "last_check_detail", "updated_at"):
                accepted_task.pop(key)
            data = {"binding": expected, "context": context, "stdout_sha256": stdout_sha256,
                    "context_sha256": sha(encoded(context)), "scope_version": head["version"],
                    "accepted_task_sha256": sha(encoded(accepted_task)),
                    "created_at": now, "read_expires_at": min(body["spec"]["deadline_at"], ctx["expires_at"]),
                    "purge_deadline": min(body["spec"]["deadline_at"], now + body["limits"]["retention_s"])}
            self._insert_action(env, "delivery", "delivery", ctx,
                                min(body["spec"]["deadline_at"], ctx["expires_at"]), data)
            self.store.conn.execute("UPDATE aws_context_scopes SET current_context_attempt_id=? WHERE scope_id=?",
                                    (env["attempt_id"], b["scope_id"]))
        return True

    @_guard
    def read_current_context(self, caller, scope_id, *, audience_handle):
        ctx = self._auth(caller, {"host"})
        _text(scope_id, 128)
        with self._transaction():
            now = self._now(ctx)
            head = self.store.conn.execute("SELECT * FROM aws_context_scopes WHERE scope_id=?", (scope_id,)).fetchone()
            if head is None:
                _deny("scope_missing")
            scope = json.loads(head["body"])
            if "audience" not in scope:
                _deny("external_scope_required")
            self._check_audience(scope, ctx, now, caller=caller, handle=audience_handle)
            if scope["revoked"] or head["current_context_attempt_id"] is None:
                _deny("current_context_missing")
            b = self.store.conn.execute("SELECT * FROM aws_context_bindings WHERE attempt_id=?",
                                        (head["current_context_attempt_id"],)).fetchone()
            if (b is None or b["request_id"] != head["request_id"] or b["head_version"] != head["version"]
                    or b["head_sha"] != head["grant_sha"]):
                _deny("context_lineage_changed")
            body = json.loads(b["body"])
            env = body["envelope"]
            r = self._current_attempt(env, current=False)
            if r["withdrawn"] or r["state"] not in {"execution_succeeded_pending_review", "accepted"}:
                _deny("context_lineage_changed")
            delivery = self._action(env["attempt_id"], "delivery")
            if delivery is None or self._action(env["attempt_id"], "context_purge") is not None:
                _deny("context_purged_or_missing")
            data = json.loads(delivery["body"])
            if r["state"] == "accepted":
                task = self.store.get(env["task_id"])
                enrollment = self.store.conn.execute("SELECT state FROM aws_enrollments WHERE task_id=?", (env["task_id"],)).fetchone()
                if (task is None or task.state != "done" or task.kind != "deterministic"
                        or task.owner != env["claim_owner"] or task.claimed_by != task.owner
                        or task.claim_token != env["claim_token"] or task.revision != env["claim_revision"]+1
                        or fingerprint(task) != data.get("accepted_task_sha256")
                        or enrollment is None or enrollment[0] != "accepted"
                        or self.store.unmet_deps(task) or self.canonical._other_unknown(env)):
                    _deny("accepted_task_changed")
            else:
                try:
                    self.canonical._current(r, env)
                except ReceiptError:
                    _deny("canonical_changed")
            if now >= min(data["read_expires_at"], data["purge_deadline"], body["spec"]["deadline_at"]):
                self._expire_native(scope["request_id"])
            observation = json.loads(b["observation"] or "null")
            result = json.loads(r["terminal_result"] or "null")
            if ("context" not in data or sha(encoded(data["context"])) != data["context_sha256"]
                    or encoded(data["binding"]) != b["expected"].encode()
                    or not observation or not result or not self.canonical._success(result)
                    or data["stdout_sha256"] != result["stdout_sha256"]
                    or observation["context_sha256"] != data["context_sha256"]
                    or data["scope_version"] != head["version"]):
                _deny("context_lineage_changed")
            return _copy(data["context"], self.limits["result_max_bytes"])

    def _retire_native_requests(self, now):
        changed = False
        rows = self.store.conn.execute("SELECT * FROM aws_context_native_requests").fetchall()
        if len(rows) > _NATIVE_REQUEST_LIMIT:
            _deny("native_request_ledger_invalid")
        for row in rows:
            desc = json.loads(row["descriptor"])
            pair = self._native.get(row["request_id"])
            invalid = row["expired"] or row["issuer"] != self._issuer or pair is None or now >= desc["expires_at"]
            if not invalid:
                try:
                    host = self._auth(pair[0], {"host"})
                    invalid = (now >= host["expires_at"] or host["principal"] != row["host_principal"]
                        or host["invocation_id"] != row["host_invocation"]
                        or encoded(self._descriptor(pair[0],pair[1],now)) != row["descriptor"].encode())
                except ArtifactError:
                    invalid = True
            if invalid:
                self._native.pop(row["request_id"], None)
                if not row["expired"]:
                    self.store.conn.execute("UPDATE aws_context_native_requests SET expired=1 WHERE request_id=?", (row["request_id"],))
                    changed = True
        return changed

    def _context_is_retired(self, binding, data, now):
        body = json.loads(binding["body"])
        scope = body["scope"]
        deadline = min(body["spec"]["deadline_at"], data.get("purge_deadline", body["spec"]["deadline_at"]),
                       data.get("read_expires_at", body["spec"]["deadline_at"]))
        if now >= deadline:
            return True
        head = self.store.conn.execute("SELECT * FROM aws_context_scopes WHERE scope_id=?", (binding["scope_id"],)).fetchone()
        if (head is None or head["request_id"] != binding["request_id"] or head["version"] != binding["head_version"]
                or head["grant_sha"] != binding["head_sha"] or json.loads(head["body"])["revoked"]):
            return True
        r = self._current_attempt(body["envelope"], current=False)
        if r["withdrawn"] or r["state"] == "retry_authorized":
            return True
        if "audience" in scope:
            native = self.store.conn.execute("SELECT expired,issuer FROM aws_context_native_requests WHERE request_id=?",
                                             (binding["request_id"],)).fetchone()
            return native is None or native["expired"] or native["issuer"] != self._issuer
        return False

    def _checkpoint(self):
        return tuple(self.store.conn.execute("PRAGMA main.wal_checkpoint(TRUNCATE)").fetchone())

    @_guard
    def purge_expired_contexts(self, caller):
        ctx = self._auth(caller, {"controller"})
        self._cleanup_verified = False
        purged = 0
        with self._transaction():
            ctx = self._auth(caller, {"controller"})
            now = self._effective_time()
            if now >= ctx["expires_at"]:
                _deny("caller_expired")
            self._retire_native_requests(now)
            rows = self.store.conn.execute("""SELECT a.* FROM aws_context_actions a
                WHERE a.phase='delivery' AND NOT EXISTS (SELECT 1 FROM aws_context_actions p
                WHERE p.attempt_id=a.attempt_id AND p.phase='context_purge')""").fetchall()
            for delivery in rows:
                data = json.loads(delivery["body"])
                binding = self.store.conn.execute("SELECT * FROM aws_context_bindings WHERE attempt_id=?",
                                                 (delivery["attempt_id"],)).fetchone()
                if binding is None:
                    _deny("context_lineage_missing")
                if "context" not in data:
                    _deny("purge_tombstone_missing")
                if not self._context_is_retired(binding, data, now):
                    continue
                original_sha = sha(delivery["body"].encode())
                del data["context"]
                clean = encoded(data).decode()
                body = json.loads(binding["body"])
                if "audience" in body["scope"]:
                    self._native.pop(binding["request_id"], None)
                    self.store.conn.execute("UPDATE aws_context_native_requests SET expired=1 WHERE request_id=?",
                                            (binding["request_id"],))
                deadline = data.get("purge_deadline", body["spec"]["deadline_at"])
                self._insert_action(body["envelope"], "context_purge", "context_purge", ctx, deadline,
                    {"delivery_sha256":original_sha,"sanitized_sha256":sha(clean.encode()),"purge_deadline":deadline}, control=True)
                self.store.conn.execute("UPDATE aws_context_actions SET body=? WHERE attempt_id=? AND phase='delivery'",
                                        (clean,delivery["attempt_id"]))
                self.store.conn.execute("UPDATE aws_context_scopes SET current_context_attempt_id=NULL WHERE current_context_attempt_id=?",
                                        (delivery["attempt_id"],))
                purged += 1
            # Every reconciliation owns a new generation, including a retry
            # with zero payloads. An older checkpoint cannot clear its failure.
            self._cleanup_pending()
            generation = self.store.conn.execute("SELECT cleanup_generation FROM aws_context_retention_control WHERE singleton=1").fetchone()[0]
        try:
            checkpoint = self._checkpoint()
            complete = type(checkpoint) is tuple and checkpoint == (0,0,0)
            busy = bool(checkpoint[0]) if type(checkpoint) is tuple and len(checkpoint) == 3 else True
        except Exception:
            complete, busy = False, True
        if complete:
            with self._transaction():
                ctx = self._auth(caller, {"controller"})
                if self._effective_time() >= ctx["expires_at"]:
                    _deny("caller_expired")
                cursor = self.store.conn.execute("""UPDATE aws_context_retention_control SET
                    pending=0,checkpointed_generation=? WHERE singleton=1 AND cleanup_generation=?""", (generation,generation))
                complete = cursor.rowcount == 1
            self._cleanup_verified = complete
        return {"status":"checkpoint_complete" if complete else "cleanup_incomplete", "payloads_purged":purged,
                "cleanup_generation":generation,"checkpoint_busy":busy,"physical_readback_required":True}

    @_guard
    def _authorize_stop(self, caller, envelope, actor, executor):
        ctx = self._auth(caller, {"host"})
        env, executor = _copy(envelope), _copy(executor)
        if ctx["principal"] != actor:
            _deny("wrong_dispatcher")
        # Preserve the pinned core stop predicates while checking the new
        # authenticated caller's expiry only after acquiring that same lock.
        with self._transaction():
            self._now(ctx)
            r = self._current_attempt(env, current=False)
            try:
                self.canonical._dispatcher(actor, env)
            except ReceiptError:
                _deny("wrong_dispatcher")
            if (not r["withdrawn"] or not r["fence_authorized"] or r["stop_consumed"]
                    or not r["executor"]
                    or encoded(json.loads(r["executor"])) != encoded(executor)
                    or r["state"] in ("accepted", "retry_authorized")):
                _deny("stop_authority_required")
            self.store.conn.execute(
                "UPDATE aws_attempts SET stop_consumed=1,version=version+1 WHERE attempt_id=?",
                (env["attempt_id"],))
            self.canonical._event(env, "stop-authorized", actor)
        return True

    @_guard
    def observe_action(self, caller, envelope, reservation_id):
        ctx = self._auth(caller, {"controller", "host", "worker"})
        env = _copy(envelope)
        _text(reservation_id, 128)
        with self._transaction():
            now = self._now(ctx)
            r = self._current_attempt(env, current=False)
            if ctx["kind"] == "worker" and (not r["executor"] or encoded(ctx["executor"]) != encoded(json.loads(r["executor"]))):
                _deny("wrong_executor")
            row = self.store.conn.execute(
                "SELECT * FROM aws_context_actions WHERE attempt_id=? AND reservation_id=?",
                (env["attempt_id"], reservation_id)).fetchone()
            if row is None:
                return {"status": "not_observed"}
            evidence_sha = sha(row["body"].encode())
            if row["phase"] == "delivery":
                tombstone = self._action(env["attempt_id"], "context_purge")
                if tombstone is not None:
                    proof = json.loads(tombstone["body"])
                    if proof["sanitized_sha256"] != evidence_sha or "context" in json.loads(row["body"]):
                        _deny("purge_evidence_mismatch")
                    evidence_sha = _hex(proof["delivery_sha256"])
            return {"status": "observed", "phase": row["phase"], "reservation_id": row["reservation_id"],
                    "invocation_id": row["invocation_id"], "expires_at": row["expires_at"],
                    "evidence_sha256": evidence_sha}


class _DispatcherPort:
    def __init__(self, actions, caller):
        self.actions, self.caller = actions, caller

    def admit(self, envelope, actor, *, request_sha256=None):
        return self.actions._admit(self.caller, envelope, actor, request_sha256=request_sha256)

    def record_launch(self, envelope, actor, executor):
        ctx = self.actions._auth(self.caller, {"host"})
        if ctx["principal"] != actor:
            _deny("wrong_dispatcher")
        self.actions._now(ctx)
        return self.actions.canonical.record_launch(_copy(envelope), actor, _copy(executor))

    def authorize_stop(self, envelope, actor, executor):
        return self.actions._authorize_stop(self.caller, envelope, actor, executor)


class _ContextStore:
    def __init__(self, actions, caller):
        self.actions, self.caller = actions, caller

    def commit_context_once(self, binding, stdout_sha256, decoded_context):
        try:
            return self.actions._commit_context_once(self.caller, binding, stdout_sha256, decoded_context)
        except ArtifactError as exc:
            if exc.uncertain:
                raise
            return False
