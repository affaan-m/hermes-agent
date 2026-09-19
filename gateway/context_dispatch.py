"""Disabled-by-default, original-owner context dispatch composition.

All ports are protected host injection, never model arguments. In particular,
approval uses the existing independent approval authority, and reconciliation
is observation-only: this module never accepts an attempt or authorizes retry.
A clean completed result leaves durable capacity pending independent review.
Root may accept only after observing clean closure outside this lifetime.

Transport/provider ports must enforce their supplied deadline. This synchronous
owner cannot preempt arbitrary host code; it checks deadlines before and after
calls and provides no thread pool or generic retry backend. Artifact cleanup
must be independently implemented by cleanup_resources; closing an artifact
store alone does not establish physical deletion. No SDK identity is supplied
by this module.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import hashlib
import json
import math
import threading


_PROCESS_SLOT = threading.Lock()
_PENDING = "execution_succeeded_pending_review"


@dataclass(frozen=True)
class OwnedResources:
    store: object
    canonical: object
    actions: object
    audience: object
    authority: object
    artifacts: object


@dataclass(frozen=True)
class DispatchPorts:
    create_owner: object
    approve: object
    launch_policy: object
    make_launch_guard: object
    observe_launch: object
    router_factory: object
    exchange_factory: object
    results_factory: object
    verify_broker: object
    observe_executor: object
    open_transport: object
    corroborate_terminal: object
    reconcile: object
    cleanup_resources: object


@dataclass(frozen=True)
class DispatchOutcome:
    state: str
    reason: str
    attempt_id: str | None = None
    context_result: object = None
    closure_receipt: object = None


class _Held(Exception):
    pass


def _number(value):
    if type(value) not in (int, float):
        raise _Held()
    try:
        value = float(value)
    except (OverflowError, ValueError):
        raise _Held() from None
    if not math.isfinite(value):
        raise _Held()
    return value


def _encoded(value, limit=8192):
    data = json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode()
    if len(data) > limit:
        raise _Held()
    return data


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _hex(value, length=64):
    if type(value) is not str or len(value) != length or any(c not in "0123456789abcdef" for c in value):
        raise _Held()
    return value


class BoundedInbox:
    """Four raw frames; callbacks enqueue only after owner activation.

    The lock protects short bounded operations, never waits for queue capacity.
    No queue insertion time is used as authenticated broker evidence.
    """
    def __init__(self, *, monotonic_clock):
        self._owner = threading.current_thread()
        self._clock = monotonic_clock
        self._condition = threading.Condition()
        self._queue = deque()
        self._open = False
        self._closed = False
        self._fault = False

    def _owned(self):
        if threading.current_thread() is not self._owner:
            raise _Held()

    def open(self):
        self._owned()
        with self._condition:
            if self._closed or self._open or self._fault:
                raise _Held()
            self._open = True

    def offer(self, raw):
        with self._condition:
            if not self._open or self._closed or self._fault:
                return False
            if type(raw) is not bytes or not 0 < len(raw) <= 90 * 1024 or len(self._queue) >= 4:
                self._fault = True
                self._queue.clear()
                self._condition.notify_all()
                return False
            self._queue.append(raw)
            self._condition.notify()
            return True

    def take_until(self, monotonic_deadline):
        self._owned()
        with self._condition:
            if not self._queue and self._open and not self._fault:
                remaining = _number(monotonic_deadline) - _number(self._clock())
                if remaining > 0:
                    self._condition.wait(min(remaining, 0.05))
            if self._fault or not self._open:
                return None
            return self._queue.popleft() if self._queue else None

    def faulted(self):
        with self._condition:
            return self._fault

    def close(self):
        self._owned()
        with self._condition:
            self._open = False
            self._closed = True
            self._queue.clear()
            self._condition.notify_all()


class _Prepared:
    """Identity only. All authority remains in the original issuer registry."""
    __slots__ = ()


class NativeContextDispatcher:
    def __init__(self, *, ports, host_caller, controller_caller, enabled=False,
                 clock, monotonic_clock):
        self._ports = ports
        self._host = host_caller
        self._controller = controller_caller
        self._enabled = enabled is True
        self._clock = clock
        self._mono = monotonic_clock
        self._owner = threading.current_thread()
        self._last = None
        self._effective = None
        self._clock_fault = False
        self._prepared = {}

    def _tick(self, deadline=None):
        if threading.current_thread() is not self._owner or self._clock_fault:
            raise _Held()
        try:
            now = (_number(self._clock()), _number(self._mono()))
            if self._last is not None and any(a < b for a, b in zip(now, self._last)):
                raise _Held()
            self._effective = (now[0] if self._last is None else
                               max(now[0], self._effective + (now[1] - self._last[1])))
            self._last = now
        except Exception:
            self._clock_fault = True
            raise _Held() from None
        if deadline is not None and self._effective >= deadline:
            raise _Held()
        return self._effective, now[1]

    def _call(self, deadline, function, *args, **kwargs):
        self._tick(deadline)
        result = function(*args, **kwargs)
        self._tick(deadline)
        return result

    def _controller_descriptor(self, owner):
        """Read-only closure fence, usable after the owned Store is closed.

        These are the pinned canonical authenticator's closed fields. This
        does not authorize an action or renew the original controller lease.
        """
        value = owner.authority(self._controller)
        if (type(value) is not dict
                or set(value) != {"principal", "kind", "invocation_id", "executor", "expires_at"}
                or value["kind"] != "controller" or value["executor"] is not None
                or type(value["principal"]) is not str
                or value["principal"] not in owner.actions.scope_controllers):
            raise _Held()
        _hex(value["invocation_id"], 32)
        if self._tick()[0] >= _number(value["expires_at"]):
            raise _Held()
        return _encoded(value, 4096)

    def run(self, *, enrollment, authority_epoch, scope_spec, task_id,
            input_bytes, cli_source_manifest_sha256, timeout=30,
            expected_scope_version=0):
        if not self._enabled:
            return DispatchOutcome("disabled", "disabled")
        if threading.current_thread() is not self._owner:
            return DispatchOutcome("rejected", "owner_mismatch")
        if not _PROCESS_SLOT.acquire(blocking=False):
            return DispatchOutcome("busy", "capacity_held")
        record = {"resources": None, "scope_cm": None, "scope_entered": False,
                  "scope": None, "version": None, "envelope": None,
                  "approval_started": False, "transport": None, "exchange": None,
                  "binding": None, "executor": None, "inbox": None,
                  "initial_checkpoint": False, "grant_started": False,
                  "controller_descriptor": None,
                  "context": None, "used": False, "launch_called": False,
                  "deadline": None, "cleanup_deadline": None, "token": None}
        outcome = DispatchOutcome("held", "preparation_held")
        clean = False
        try:
            now, _ = self._tick()
            if type(self._ports) is not DispatchPorts:
                raise _Held()
            if type(input_bytes) is not bytes or not 0 < len(input_bytes) <= 1024 * 1024:
                raise _Held()
            if type(timeout) is not int or not 1 <= timeout <= 30:
                raise _Held()
            source_hash = _hex(cli_source_manifest_sha256)
            scope = json.loads(_encoded(scope_spec))
            deadline = _number(scope["not_after"])
            if not now < deadline <= now + 120:
                raise _Held()
            record.update(scope=scope, deadline=deadline, cleanup_deadline=deadline + 60)
            owner = self._ports.create_owner()
            record["resources"] = owner
            if (type(owner) is not OwnedResources or owner.canonical.store is not owner.store
                    or owner.actions.store is not owner.store or owner.actions.canonical is not owner.canonical
                    or owner.actions.resolve_audience != owner.audience.resolve_context_audience
                    or owner.actions.authenticate is not owner.authority):
                raise _Held()
            owner.canonical.initialize_for_candidate()
            owner.actions.initialize_for_candidate()
            initial = owner.actions.purge_expired_contexts(self._controller)
            if initial.get("status") != "checkpoint_complete":
                raise _Held()
            record["initial_checkpoint"] = True
            self._tick(deadline)
            record["controller_descriptor"] = self._controller_descriptor(owner)
            owner.audience.bind(authority_epoch=authority_epoch, enrollment=json.loads(_encoded(enrollment)))
            cm = owner.audience.request_scope(self._host, canonical_request_id=scope["request_id"],
                                              approved_projection=scope["projection"])
            record["scope_cm"] = cm
            cm.__enter__()
            record["scope_entered"] = True
            handle = owner.audience.capture_context_audience(self._host)
            if handle is None:
                raise _Held()
            record["handle"] = handle
            record["grant_started"] = True
            grant = owner.actions.set_audience_scope(self._controller, scope,
                host_caller=self._host, audience_handle=handle, expected_scope_version=expected_scope_version)
            record["version"] = grant["scope_version"]
            spec = dict(schema_version=1, request_id=scope["request_id"], operation="nonpricing.context",
                input_sha256=_sha(input_bytes), input_bytes=len(input_bytes),
                cli_source_manifest_sha256=source_hash, audience_grant_sha256=grant["grant_sha256"],
                projection=scope["projection"], deadline_at=deadline, result_max_bytes=8192)
            spec_bytes = _encoded(spec)
            payload = dict(schema=1, operation="nonpricing.context", task_id=task_id,
                           request_spec_sha256=_sha(spec_bytes), timeout=timeout)
            typed_bytes = _encoded(payload)
            record["approval_started"] = True
            envelope = self._call(deadline, self._ports.approve, owner, typed_bytes, spec_bytes,
                                  deadline_at=deadline)
            envelope = json.loads(_encoded(envelope))
            record["envelope"] = envelope
            if envelope["task_id"] != task_id or envelope["spec_sha256"] != _sha(typed_bytes):
                raise _Held()
            _hex(envelope["attempt_id"], 32)
            record.update(spec=spec, spec_bytes=spec_bytes, payload=payload, typed_bytes=typed_bytes,
                          input=input_bytes, grant=grant)
            token = _Prepared()
            record["token"] = token
            self._prepared[token] = record
            outcome = self.dispatch_context(token)
        except Exception:
            outcome = DispatchOutcome("unknown" if record["launch_called"] else "held",
                                      "dispatch_unknown" if record["launch_called"] else "preparation_held",
                                      self._attempt_id(record))
        finally:
            clean = self._close(record)
            token = record.get("token")
            if token is not None:
                self._prepared.pop(token, None)
            context = record.get("context") if clean else None
            if clean:
                _PROCESS_SLOT.release()
            # Drop retained input and opaque native graphs even when capacity is held.
            attempt_id = self._attempt_id(record)
            receipt = None
            if clean and context is not None:
                receipt = {"schema": 1, "attempt_id": attempt_id,
                           "canonical_sha256": _sha(_encoded(record["envelope"])),
                           "cleanup_complete": True, "canonical_state": _PENDING,
                           "durable_capacity_released": False}
            record.clear()
        if clean and context is not None:
            return DispatchOutcome("completed", "clean_context_pending_review", attempt_id, context, receipt)
        if not clean:
            return DispatchOutcome("unknown" if outcome.state == "unknown" else "held",
                                   "closure_unresolved", attempt_id)
        return outcome

    @staticmethod
    def _attempt_id(record):
        envelope = record.get("envelope")
        value = envelope.get("attempt_id") if type(envelope) is dict else None
        return value if type(value) is str and len(value) == 32 and all(c in "0123456789abcdef" for c in value) else None

    def dispatch_context(self, prepared_request):
        if (type(prepared_request) is not _Prepared or threading.current_thread() is not self._owner
                or prepared_request not in self._prepared):
            return DispatchOutcome("rejected", "prepared_request_invalid")
        r = self._prepared[prepared_request]
        if r["used"] or not r["scope_entered"]:
            return DispatchOutcome("rejected", "prepared_request_spent", self._attempt_id(r))
        r["used"] = True
        o, p, env, deadline = r["resources"], self._ports, r["envelope"], r["deadline"]
        now, _ = self._tick(deadline)
        if o.audience.resolve_context_audience(self._host, r["handle"], now) is None:
            raise _Held()
        # Protected source-pinned policy API has no public digest accessor.
        # Bind its exact configuration to the existing independent approval.
        if _sha(_encoded(p.launch_policy._value)) != env["launch_policy_sha256"]:
            raise _Held()
        bound = o.actions.bind_request(self._host, env, r["typed_bytes"], r["spec_bytes"],
                                      expected_scope_version=r["version"])
        if bound != {"status": "bound", "request_id": r["spec"]["request_id"]}:
            raise _Held()
        coordinates = dict(attempt_id=env["attempt_id"], request_id=r["spec"]["request_id"],
            request_spec_sha256=_sha(r["spec_bytes"]), spec_sha256=env["spec_sha256"],
            input_sha256=r["spec"]["input_sha256"], source_sha256=r["spec"]["cli_source_manifest_sha256"],
            canonical_sha256=_sha(_encoded(env)), launch_token=env["launch_token"])
        launch_request = p.launch_policy.request(coordinates)
        if launch_request["taskDefinition"] != env["task_definition_arn"]:
            raise _Held()
        actor = env["dispatcher_id"]  # Actual port independently authenticates this exact host.
        dispatcher = o.actions.dispatcher_port(self._host)
        dispatcher.admit(env, actor, request_sha256=_sha(_encoded(launch_request, 32768)))
        router = p.router_factory(attempt_id=env["attempt_id"], request_id=r["spec"]["request_id"],
                                  clock=self._clock, monotonic_clock=self._mono)
        inbox = BoundedInbox(monotonic_clock=self._mono)
        r["inbox"] = inbox
        startup_deadline = min(deadline, self._tick(deadline)[0] + 30)
        r["transport"] = self._call(startup_deadline, p.open_transport, inbox.offer, coordinates,
                                     deadline_at=startup_deadline)
        guard = self._call(startup_deadline, p.make_launch_guard, coordinates)
        self._tick(startup_deadline)
        r["launch_called"] = True
        response = self._call(startup_deadline, guard.run_task, **launch_request)
        observed = self._call(startup_deadline, p.observe_launch, response, coordinates,
                              deadline_at=startup_deadline)
        if type(observed) is not dict or set(observed) != {"task", "task_definition", "role", "network"}:
            raise _Held()
        executor = p.launch_policy.observe(observed["task"], task_definition=observed["task_definition"],
            role=observed["role"], network=observed["network"], coordinates=coordinates)
        r["executor"] = executor
        dispatcher.record_launch(env, actor, executor)
        manifest = dict(request_id=r["spec"]["request_id"], request_spec_sha256=_sha(r["spec_bytes"]),
            canonical_sha256=coordinates["canonical_sha256"], executor=executor,
            grant=dict(scope_id=r["scope"]["scope_id"], request_id=r["spec"]["request_id"],
                sequence=r["scope"]["sequence"], grant_epoch=r["scope"]["grant_epoch"],
                grant_sha256=r["grant"]["grant_sha256"]))
        binding = o.actions.activate(self._host, env, executor, _encoded(manifest))
        r["binding"] = binding
        created = self._tick(startup_deadline)[0]
        if o.artifacts.publish(binding, "input", r["input"], created_at=created, expires_at=created + 600) is not True:
            raise _Held()
        digest = observed["task"]["containers"][0]["imageDigest"]
        if type(digest) is not str or not digest.startswith("sha256:"):
            raise _Held()
        exchange = p.exchange_factory(actions=o.actions, authority=o.authority, artifacts=o.artifacts,
            verify_broker=p.verify_broker, observe_executor=p.observe_executor, envelope=env,
            binding=binding, request_spec=r["spec"], typed_payload=r["payload"],
            role_id=observed["role"]["RoleId"], task_role_arn=observed["task_definition"]["taskRoleArn"],
            image_sha256=_hex(digest[7:]), created_at=created, clock=self._clock)
        r["exchange"] = exchange
        self._tick(startup_deadline)
        router.install(exchange)
        self._tick(startup_deadline)
        inbox.open()
        consumer = p.results_factory(exchange=o.artifacts, context_store=o.actions.context_store(self._host))
        transport = r["transport"]
        def check_ingress():
            self._tick(r["cleanup_deadline"])
            if inbox.faulted():
                raise _Held()

        def guarded_send(topic, data):
            check_ingress()
            transport.send(topic, data)
            check_ingress()

        processed = 0
        while True:
            now, mono = self._tick(r["cleanup_deadline"])
            poll_deadline = min(r["cleanup_deadline"], now + 0.05)
            self._call(r["cleanup_deadline"], transport.poll, deadline_at=poll_deadline)
            check_ingress()
            raw = inbox.take_until(mono + 0.05)
            check_ingress()
            if raw is not None:
                processed += 1
                if processed > 256:
                    raise _Held()
                opaque = transport.decode_delivery(raw)
                check_ingress()
                router.handle(opaque, guarded_send)
                check_ingress()
            summary = self._call(r["cleanup_deadline"], p.corroborate_terminal, o, exchange, binding,
                                 deadline_at=r["cleanup_deadline"])
            check_ingress()
            if summary is not None:
                committed = consumer.consume(binding, summary, request_spec=r["spec"])
                check_ingress()
                if (committed.get("status") != "context_committed"
                        or committed.get("context_committed") is not True):
                    raise _Held()
                self._tick(deadline)
                context = o.actions.read_current_context(self._host, r["scope"]["scope_id"],
                                                         audience_handle=r["handle"])
                check_ingress()
                self._tick(deadline)
                r["context"] = json.loads(_encoded(context))
                return DispatchOutcome("held", "cleanup_pending", env["attempt_id"])

    def _close(self, r):
        """Best-effort owner cleanup; no acceptance, retry or ledger deletion."""
        clean = r["initial_checkpoint"] and not (r["grant_started"] and r["version"] is None)
        o, transport = r["resources"], r["transport"]
        if r["inbox"] is not None:
            try:
                r["inbox"].close()
                if r["inbox"].faulted():
                    clean = False
            except Exception:
                clean = False
        if r["exchange"] is not None:
            try:
                r["exchange"].disconnect()
            except Exception:
                clean = False
        if o is None:
            return False  # Factory may have created resources before losing its acknowledgment.
        if r["version"] is not None:
            try:
                revoked = o.actions.revoke_audience_scope(self._controller, r["scope"]["scope_id"],
                                                         expected_scope_version=r["version"])
                r["version"] = revoked["scope_version"]
                if o.actions.purge_expired_contexts(self._controller).get("status") != "checkpoint_complete":
                    clean = False
            except Exception:
                clean = False
        if transport is not None:
            try:
                if transport.close() is not True:
                    clean = False
            except Exception:
                clean = False
        try:
            if self._ports.cleanup_resources(o, transport, r["binding"], deadline_at=r["cleanup_deadline"]) is not True:
                clean = False
        except Exception:
            clean = False
        env = r["envelope"]
        if env is not None:
            try:
                before = o.canonical._attempt(env)
                observed = self._ports.reconcile(o, env, r["executor"], deadline_at=r["cleanup_deadline"])
                after = o.canonical._attempt(env)
                if (type(observed) is not dict or set(observed) != {"resolved"}
                        or observed["resolved"] is not True or before != after
                        or after["state"] != _PENDING or not after["admitted"]):
                    clean = False
            except Exception:
                clean = False
        elif r["approval_started"]:
            clean = False
        try:
            self._tick(r["cleanup_deadline"])
        except Exception:
            clean = False
        if r["scope_entered"]:
            try:
                r["scope_cm"].__exit__(None, None, None)
            except Exception:
                clean = False
            r["scope_entered"] = False
        try:
            o.store.close()
        except Exception:
            clean = False
        try:
            self._tick(r["cleanup_deadline"])
            if (r["controller_descriptor"] is None
                    or self._controller_descriptor(o) != r["controller_descriptor"]):
                clean = False
            self._tick(r["cleanup_deadline"])
        except Exception:
            clean = False
        return clean
