"""Fixed outward failure text; diagnostic detail belongs in server logs.

Use structured execution state instead of guessing failures from prose. This
module keeps application imports lazy and consumes the shared audience policy.
"""
from collections.abc import Mapping
from typing import Any


SAFE_FAILURE_TEXT = "Sorry, I couldn't complete that request. Please try again."


class DeliveryPolicyDenied(Exception):
    """Terminal authorization/class denial, never a transport retry signal."""


class DeliveryNotConfirmed(Exception):
    """Delivery was filtered or only partly sent; do not mirror or duplicate."""


def policy_denial():
    return {"success": False, "delivered": False, "error_kind": "policy_denied",
            "error": "delivery_not_authorized"}


def is_policy_denial(result) -> bool:
    def value(key):
        return result.get(key) if isinstance(result, Mapping) else getattr(result, key, None)
    return (value("error_kind") == "policy_denied"
            or value("error") in {"delivery_not_authorized", "audience_policy_suppressed"})


def is_terminal_delivery_failure(result) -> bool:
    kind = result.get("error_kind") if isinstance(result, Mapping) else getattr(result, "error_kind", None)
    return is_policy_denial(result) or kind in {
        "private_delivery_failed", "delivery_unknown", "delivery_not_confirmed",
    }


def terminal_failure_result(result):
    if is_policy_denial(result):
        return policy_denial()
    kind = result.get("error_kind") if isinstance(result, Mapping) else getattr(result, "error_kind", None)
    if kind in {"delivery_unknown", "delivery_not_confirmed"}:
        return {"success": False, "delivered": False, "error_kind": kind, "error": kind}
    return {"success": False, "delivered": False, "error_kind": "private_delivery_failed",
            "error": "private_delivery_failed"}


def requires_safe_failure(result: Mapping[str, Any]) -> bool:
    """An earlier streamed fragment is not a completed result in these states."""
    return bool(result.get("failed") or result.get("partial") or result.get("error")
                or result.get("interrupted") or result.get("completed") is False)


def normalize_agent_response(result: Mapping[str, Any], response: str | None) -> str:
    """Preserve successful answers and intentional silence, never error bodies."""
    if result.get("interrupted"):
        # The runtime can put exception/continuation diagnostics into a
        # nonempty interrupted final response. Only empty interruption is
        # intentional silence; arbitrary interruption prose is not trusted.
        return SAFE_FAILURE_TEXT if response else ""
    if requires_safe_failure(result):
        return SAFE_FAILURE_TEXT
    return response or SAFE_FAILURE_TEXT


def prepare_outbound_text(content: str, output_class=None) -> str:
    """Prepare classified text before formatting, blocks or direct delivery.

    Execution failures must be classified by their producer from structured
    state. Secret redaction is defense in depth, not a diagnostic classifier.
    """
    from gateway.message_audience import OutputClass
    if output_class is OutputClass.SAFE_ERROR:
        return SAFE_FAILURE_TEXT
    from agent.redact import redact_sensitive_text, _redact_url_userinfo
    return _redact_url_userinfo(redact_sensitive_text(str(content or ""), force=True))


def channel_policy_inputs(config, platform, workspace_id, chat_id):
    """Adapt only supplied trusted configuration; never read env or user files."""
    from gateway.message_audience import Audience, ChannelIdentity, ChannelPolicy

    identity = ChannelIdentity(str(getattr(platform, "value", platform)),
                               str(workspace_id or ""), str(chat_id or ""))
    extra = getattr(config, "extra", None)
    entries = extra.get("message_audience", []) if isinstance(extra, Mapping) else []
    policies = {}
    if not isinstance(entries, list):
        return identity, policies
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        try:
            key = ChannelIdentity(entry["platform"], entry["workspace_id"], entry["channel_id"])
            policy = ChannelPolicy(Audience(entry["audience"]),
                                   entry.get("desk_voice", False),
                                   entry.get("operator_messages_are_requests", False))
            policies[key] = policy
        except (KeyError, TypeError, ValueError):
            continue  # Malformed supplied policies confer no extra privilege.
    return identity, policies


def destination_output_allowed(adapter, chat_id, output_class, metadata=None, *, workspace_id=None):
    """Class gate for an already-authorized transport call, resolved per target.

    Authorization to initiate a turn/delivery belongs at intake or the trusted
    cron/tool dispatcher. Calling this does not grant participation permission.
    No event metadata, private-message shape or inherited audience is trusted.
    """
    from gateway.message_audience import Action, ParticipationDecision, ParticipationSignals, decide_participation, output_allowed

    extra = getattr(adapter.config, "extra", {}) or {}
    workspace = workspace_id
    binding = getattr(adapter, "_bind_delivery_metadata", None)
    if callable(binding):
        routed = dict(metadata or {})
        if workspace_id is not None:
            if routed.get("slack_team_id") not in (None, workspace_id):
                return False
            routed["slack_team_id"] = workspace_id
        try:
            routed = binding(chat_id, routed)
        except DeliveryPolicyDenied:
            return False
        workspace = routed.get("slack_team_id", "")
        # A live transport with unresolved client identity stays external.
        identity, policies = channel_policy_inputs(adapter.config, adapter.platform, workspace, chat_id)
        audience = decide_participation(identity, ParticipationSignals(), policies).audience
        return output_allowed(ParticipationDecision(Action.RESPOND, audience, "bound_transport"), output_class)
    metadata_team_id = getattr(adapter, "_metadata_team_id", None)
    if workspace is None and callable(metadata_team_id):
        workspace = metadata_team_id(metadata)
    if not workspace:
        workspace = (getattr(adapter, "_channel_team", {}) or {}).get(str(chat_id))
    if not workspace and isinstance(extra, Mapping):
        workspace = extra.get("workspace_id") or extra.get("scope_id")
    identity, policies = channel_policy_inputs(adapter.config, adapter.platform, workspace, chat_id)
    # This is an audience-only transport gate. Do not fabricate operator
    # consent to authorize scheduled work; authorized_delivery handles that.
    audience = decide_participation(identity, ParticipationSignals(), policies).audience
    decision = ParticipationDecision(Action.RESPOND, audience, "authorized_transport_class_check")
    return output_allowed(decision, output_class)


# Capabilities are process-local objects registered by trusted intake/dispatch.
# No event/job/model field can reconstruct an object identity. Only admitted
# identities are claimed durably; pending provenance and live scopes expire.
# A durable claim is not a provider delivery/exactly-once guarantee.
import contextvars
import functools
import math
import sys
import threading
import time
import weakref
from contextlib import contextmanager, ExitStack
from dataclasses import dataclass, field
from types import SimpleNamespace

_CAPABILITY_TTL = 300.0
_DISPATCH_TTL = 3600.0
_MAX_REGISTRY = 4096
_MAX_PENDING = 4096
_clock = time.monotonic
_now = time.time
_lock = threading.RLock()
_tickets = {}
_pending_tickets = weakref.WeakKeyDictionary()
_scopes = {}
_pending = contextvars.ContextVar("hermes_pending_delivery", default=None)
_current = contextvars.ContextVar("hermes_delivery_scope", default=None)
_producer = contextvars.ContextVar("hermes_delivery_producer", default=None)
_attempt = contextvars.ContextVar("hermes_delivery_attempt", default=None)
_batch = contextvars.ContextVar("hermes_delivery_batch", default=None)
_MISSING = object()
_THREAD_KEYS = ("thread_id", "thread_ts", "message_thread_id", "direct_messages_topic_id")


class _Opaque:
    __slots__ = ("__weakref__",)


@dataclass(eq=False)
class _Scope:
    ticket: object
    kind: str
    identity: tuple
    expires: float
    adapter: object = None
    revoked: bool = False
    suppress_final: bool = False
    ledger: dict = field(default_factory=dict)


def _deny():
    raise DeliveryPolicyDenied("delivery_not_authorized")


def _text(value):
    return value if isinstance(value, str) and value.strip() else None


def _platform(value):
    return str(getattr(value, "value", value))


def _thread(value):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int)) or not str(value).strip():
        _deny()
    return str(value)


def _source_identity(source):
    identity = (getattr(source, "profile", None) or "default",
                getattr(source, "user_id", None), _platform(getattr(source, "platform", "")),
                getattr(source, "scope_id", None), str(getattr(source, "chat_id", "") or ""),
                _thread(getattr(source, "thread_id", None)))
    if not all(_text(value) for value in identity[:5]):
        _deny()
    return identity


def _register(kind, identity, event=None):
    if kind != "request":
        _deny()
    from gateway.delivery_registry import _digest, RegistryError
    try:
        _digest(kind, identity)  # Bound pending identity bytes before retaining them.
    except RegistryError:
        _deny()
    try:
        event_ref = weakref.ref(event) if event is not None else lambda: None
    except TypeError:
        _deny()  # Never retain an un-weakrefable provider payload as fallback.
    with _lock:
        _sweep()
        if len(_pending_tickets) >= _MAX_PENDING:
            _deny()
        token = _Opaque()
        _pending_tickets[token] = dict(kind=kind, identity=identity, event=event_ref,
            expires=_clock() + _CAPABILITY_TTL, event_was_none=event is None, used=False, revoked=False)
        return token


def _sweep():
    """Bounded live maps only; durable replay claims are never expired here."""
    now = _clock()
    for token, record in list(_pending_tickets.items()):
        if now >= record["expires"]:
            _pending_tickets.pop(token, None)
    for token, record in list(_tickets.items()):
        if now >= record["expires"]:
            _revoke(_scopes.get(token))
            _tickets.pop(token, None)


def _get_registry(registry):
    from gateway.delivery_registry import Registry
    if not isinstance(registry, Registry):
        _deny()
    return registry


def _valid_scope():
    scope = _current.get()
    with _lock:
        if (not isinstance(scope, _Scope) or _scopes.get(scope.ticket) is not scope
                or scope.revoked or _clock() >= scope.expires):
            return None
        return scope


def _revoke(scope):
    if scope is not None:
        with _lock:
            scope.revoked = True
            _scopes.pop(scope.ticket, None)
            _tickets.pop(scope.ticket, None)


def reject_pending_request():
    """Terminal gateway rejection; intake return alone is not rejection."""
    with _lock:
        token = _pending.get()
        if isinstance(token, _Opaque):
            _pending_tickets.pop(token, None)


@contextmanager
def fresh_request_intake():
    """New adapter ingress cannot borrow an inherited pending carrier on denial."""
    marker = _pending.set(None)
    try:
        yield
    finally:
        _pending.reset(marker)


@contextmanager
def intake_request(source, request_id, *, event=None):
    """Trusted adapter intake only; pending authority is unusable before auth."""
    if not _text(request_id):
        _deny()
    token = _register("request", (*_source_identity(source), request_id), event)
    del event  # The suspended contextmanager must not retain provider payloads.
    marker = _pending.set(token)
    try:
        yield token
    finally:
        _pending.reset(marker)
        # Base.handle_message schedules a child task before returning. Its
        # copied pending ticket remains usable once, within the original TTL.


def isolated_delivery_turn(function):
    """No inherited active grant; retain only the opaque pending intake ticket."""
    @functools.wraps(function)
    async def wrapped(*args, **kwargs):
        scope_marker = _current.set(None)
        attempt_marker = _attempt.set(None)
        producer_marker = _producer.set(None)
        batch_marker = _batch.set(None)
        scope = None
        try:
            result = await function(*args, **kwargs)
            scope = _current.get()
            return None if scope is not None and scope.suppress_final else result
        finally:
            _revoke(scope or _current.get())
            reject_pending_request()
            _batch.reset(batch_marker)
            _producer.reset(producer_marker)
            _attempt.reset(attempt_marker)
            _current.reset(scope_marker)
    return wrapped


def _reserve_request(adapter, source, request_id, event):
    identity = _source_identity(source)
    token = _pending.get()
    if not isinstance(token, _Opaque):
        _deny()
    with _lock:
        _sweep()
        record = _pending_tickets.get(token)
        if (record is None or record["kind"] != "request" or record["used"]
                or record["revoked"] or _clock() >= record["expires"]
                or record["identity"][:6] != identity or _valid_scope() is not None):
            _deny()
        if record["event"]() is not event or (event is None and not record["event_was_none"]):
            _deny()
        if request_id != record["identity"][6]:
            if request_id not in (None, "") or event is None:
                _deny()
        if _platform(getattr(adapter, "platform", "")) != identity[2]:
            _deny()
        binding = getattr(adapter, "_bind_delivery_metadata", None)
        if not callable(binding):
            _deny()  # No proven selected-client binding for this intake producer.
        metadata = binding(identity[4], {"slack_team_id": identity[3]})
        if metadata.get("slack_team_id") != identity[3]:
            _deny()
        if len(_tickets) >= _MAX_REGISTRY:
            _deny()
        record["used"] = True
        return token, record, identity


def _activate_reserved(adapter, token, record, identity):
    with _lock:
        _sweep()
        if (_pending_tickets.get(token) is not record or _clock() >= record["expires"]
                or len(_tickets) >= _MAX_REGISTRY):
            _deny()
        _pending_tickets.pop(token, None)
        scope = _Scope(token, "request", identity, record["expires"], adapter)
        _tickets[token] = dict(kind="request", identity=record["identity"],
            expires=record["expires"], used=True, revoked=False)
        _scopes[token] = scope
        _current.set(scope)
        _producer.set("send_message")
        return scope


def activate_request(adapter, source, request_id, *, event=None, registry=None):
    """Synchronous trusted caller; gateway uses the asynchronous I/O variant."""
    from gateway.delivery_registry import RegistryError, report_registry_failure
    token, record, identity = _reserve_request(adapter, source, request_id, event)
    try:
        if not _get_registry(registry).claim("request", record["identity"]):
            _deny()
        return _activate_reserved(adapter, token, record, identity)
    except RegistryError as error:
        report_registry_failure(error)
        _deny()
    finally:
        with _lock:
            _pending_tickets.pop(token, None)


async def activate_request_async(adapter, source, request_id, *, event=None, registry=None):
    """Claim I/O off-loop; reservation/activation ContextVars stay on this task."""
    from gateway.delivery_registry import RegistryError, report_registry_failure
    token, record, identity = _reserve_request(adapter, source, request_id, event)
    try:
        if not await _get_registry(registry).aclaim("request", record["identity"]):
            _deny()
        return _activate_reserved(adapter, token, record, identity)
    except RegistryError as error:
        report_registry_failure(error)
        _deny()
    finally:
        with _lock:
            _pending_tickets.pop(token, None)


def mint_dispatch(job_id, execution_id, profile_id, *, registry=None):
    """Built-in tick only, after a backend execution claim was created."""
    identity = (job_id, execution_id, profile_id)
    if not all(_text(value) for value in identity):
        _deny()
    from gateway.delivery_registry import RegistryError, report_registry_failure
    expires = _clock() + _DISPATCH_TTL
    with _lock:
        _sweep()
        if len(_tickets) >= _MAX_REGISTRY:
            _deny()
    try:
        if not _get_registry(registry).claim("dispatch", identity):
            _deny()
    except RegistryError as error:
        report_registry_failure(error)
        _deny()
    with _lock:
        _sweep()
        if _clock() >= expires or len(_tickets) >= _MAX_REGISTRY:
            _deny()
        token = _Opaque()
        _tickets[token] = dict(kind="dispatch", identity=identity, expires=expires,
            used=False, revoked=False)
        return token


def revoke_dispatch(ticket):
    """Invalidate an ambiguous/failed dispatcher submission without replay reuse."""
    with _lock:
        if not isinstance(ticket, _Opaque):
            _deny()
        record = _tickets.get(ticket)
        if record is None:
            return  # Idempotent cleanup of a swept/revoked opaque ticket.
        if record["kind"] != "dispatch":
            _deny()
        record["revoked"] = True
        _revoke(_scopes.get(ticket))
        _tickets.pop(ticket, None)


@contextmanager
def dispatch_execution(ticket, job_id, execution_id, profile_id):
    identity = (job_id, execution_id, profile_id)
    with _lock:
        if not isinstance(ticket, _Opaque):
            _deny()
        record = _tickets.get(ticket)
        if (record is None or record["kind"] != "dispatch" or record["identity"] != identity
                or record["used"] or record["revoked"] or _clock() >= record["expires"]):
            _deny()
        scope = _Scope(ticket, "dispatch", identity, record["expires"])
        record["used"] = True
        _scopes[ticket] = scope
    marker = _current.set(scope)
    producer_marker = _producer.set("cron.tool")
    attempt_marker = _attempt.set(None)
    try:
        yield scope
    finally:
        _revoke(scope)
        _attempt.reset(attempt_marker)
        _producer.reset(producer_marker)
        _current.reset(marker)


def dispatch_matches(job_id, execution_id=None):
    scope = _valid_scope()
    return (scope is not None and scope.kind == "dispatch" and scope.identity[0] == job_id
            and (execution_id is None or scope.identity[1] == execution_id))


@contextmanager
def producer_context(producer, *, job_id=None, execution_id=None):
    if (not _text(producer) or _valid_scope() is None
            or (job_id is not None and not dispatch_matches(job_id, execution_id))
            or (execution_id is not None and job_id is None)):
        _deny()
    marker = _producer.set(producer)
    try:
        yield
    finally:
        _producer.reset(marker)


def active_request_adapter(platform, chat_id):
    scope = _valid_scope()
    if (scope is not None and scope.kind == "request" and scope.identity[2] == _platform(platform)
            and scope.identity[4] == str(chat_id)):
        return scope.adapter
    return None


def _operations(operations):
    if (not isinstance(operations, (tuple, list, frozenset, set)) or not operations
            or any(value not in ("text", "media") for value in operations)):
        _deny()
    return frozenset(operations)


def _resolve_thread(metadata, thread_id=_MISSING, *, inherited=_MISSING):
    values = [_thread(metadata[key]) for key in _THREAD_KEYS if key in metadata]
    if thread_id is not _MISSING:
        values.append(_thread(thread_id))
    if values and any(value != values[0] for value in values):
        _deny()
    value = values[0] if values else (None if inherited is _MISSING else inherited)
    if inherited is not _MISSING and value != inherited:
        _deny()
    return value


def delivery_thread(metadata):
    """Resolve strict route identity without adding transport-specific aliases."""
    if metadata is not None and not isinstance(metadata, Mapping):
        _deny()
    return _resolve_thread(metadata or {})


def authorized_delivery(config, platform, workspace_id, chat_id, *, producer=None,
                        operations=("text",), thread_id=None) -> bool:
    """Evaluate current scoped authority; legacy channel-wide grants never apply."""
    scope = _valid_scope()
    if scope is None:
        return False
    stage = _producer.get()
    if producer is not None and producer != stage:
        return False
    try:
        required = _operations(operations)
        exact_thread = _thread(thread_id)
    except DeliveryPolicyDenied:
        return False
    platform_name = _platform(platform)
    channel = str(chat_id) if chat_id is not None and not isinstance(chat_id, bool) else ""
    if not all(_text(value) for value in (platform_name, workspace_id, channel, stage)):
        return False
    if scope.kind == "request":
        return (stage == "send_message" and
                (platform_name, workspace_id, str(chat_id), exact_thread) ==
                (scope.identity[2], scope.identity[3], scope.identity[4], scope.identity[5]))
    extra = getattr(config, "extra", {}) or {}
    entries = extra.get("scoped_delivery_grants", []) if isinstance(extra, Mapping) else []
    if not isinstance(entries, list):
        return False
    job_id, execution_id, profile_id = scope.identity
    expected = dict(producer=stage, job_id=job_id, profile_id=profile_id,
                    platform=platform_name, workspace_id=workspace_id,
                    channel_id=str(chat_id), thread_id=exact_thread)
    for entry in entries:
        if not isinstance(entry, Mapping) or any(key not in entry or entry[key] != value for key, value in expected.items()):
            continue
        expiry = entry.get("expires_at")
        if (isinstance(expiry, bool) or not isinstance(expiry, (int, float))
                or not math.isfinite(expiry) or expiry <= _now()
                or not _text(entry.get("requester_id")) or not _text(entry.get("approval_id"))
                or ("execution_id" in entry and entry["execution_id"] != execution_id)):
            continue
        allowed = entry.get("operations")
        if (not isinstance(allowed, list) or allowed not in (["text"], ["text", "media"])
                or not required.issubset(allowed)):
            continue
        return True
    return False


def check_delivery(config, platform, chat_id, *, metadata=None, adapter=None, output_class=None,
                   producer=None, operations=("text",), thread_id=_MISSING):
    """Preflight exact current capability before side effects; does not claim a slot."""
    from gateway.message_audience import OutputClass
    scope = _valid_scope()
    if scope is None:
        _deny()
    if metadata is not None and not isinstance(metadata, Mapping):
        _deny()
    routed = dict(metadata or {})
    required = _operations(operations)
    inherited = _MISSING
    if adapter is not None:
        if _platform(getattr(adapter, "platform", "")) != _platform(platform):
            _deny()
        config = getattr(adapter, "config", None)
        if config is None:
            _deny()
    if scope.kind == "request":
        if (scope.identity[2] != _platform(platform) or scope.identity[4] != str(chat_id)
                or (adapter is not None and adapter is not scope.adapter)):
            _deny()
        adapter = scope.adapter
        config = adapter.config
        inherited = scope.identity[5]
        if routed.get("slack_team_id", scope.identity[3]) != scope.identity[3]:
            _deny()
        routed.setdefault("slack_team_id", scope.identity[3])
    exact_thread = _resolve_thread(routed, thread_id, inherited=inherited)
    if exact_thread is not None and not any(key in routed for key in _THREAD_KEYS):
        routed.setdefault("thread_id", exact_thread)
    kind = routed.get("_hermes_output_class", OutputClass.FINAL) if output_class is None else output_class
    target = adapter or SimpleNamespace(config=config, platform=platform)
    binding = getattr(target, "_bind_delivery_metadata", None)
    if callable(binding):
        routed = binding(chat_id, routed)
        workspace = routed.get("slack_team_id", "")
    else:
        extra = getattr(config, "extra", {}) or {}
        workspace = extra.get("workspace_id") or extra.get("scope_id")
        if routed.get("scope_id") not in (None, "", workspace):
            _deny()
        if _platform(platform) == "slack":
            if routed.get("slack_team_id") not in (None, "", workspace):
                _deny()
            if workspace:
                routed["slack_team_id"] = workspace
    if (_resolve_thread(routed, inherited=exact_thread) != exact_thread
            or not isinstance(kind, OutputClass)
            or not authorized_delivery(config, platform, workspace, chat_id, producer=producer,
                                       operations=required, thread_id=exact_thread)
            or not destination_output_allowed(target, chat_id, kind, routed, workspace_id=workspace)):
        _deny()
    routed["_hermes_output_class"] = kind
    return routed


@dataclass(eq=False)
class _AttemptRecord:
    scope: object
    key: tuple
    operations: frozenset
    metadata: dict
    state: str = "pending"
    owner: object = None
    uncertain: bool = False


class _AttemptHandle:
    def __init__(self, record, owner, metadata=None):
        self._record = record
        self._owner = owner
        self._outcome = "unknown"
        self._finished = False
        self.metadata = dict(record.metadata if metadata is None else metadata)

    def finish(self, result):
        record = self._record
        if (_valid_scope() is not record.scope or record.scope.ledger.get(record.key) is not record
                or record.state != "pending" or (self._owner and record.owner is not self)):
            raise DeliveryNotConfirmed("delivery_not_confirmed")
        def value(key):
            return result.get(key) if isinstance(result, Mapping) else getattr(result, key, None)
        if (value("success") is True and value("delivered") is not False
                and not value("error") and not value("error_kind")):
            self._outcome = "confirmed"
        elif result == "" or (not value("error") and not value("error_kind")
                              and value("delivered") is False and
                              (value("suppressed") is True or value("silent") is True)):
            self._outcome = "suppressed"
        else:
            self._outcome = "unknown"
        self._finished = True
        if self._outcome == "unknown":
            record.uncertain = True
        # Only the owner commits. Nested success never confirms the outer
        # text/media batch; nested uncertainty remains sticky for the owner.


def active_delivery_attempt():
    """Whether scoped provenance is present and requires the strict boundary.

    This is not authorization. Expired, revoked or forged inherited context
    must enter delivery_attempt and fail, never become an unscoped fallback.
    A valid turn without an owned attempt retains ordinary reply/stream flow.
    """
    return (_attempt.get() is not None
            or (_current.get() is not None and _valid_scope() is None))


@contextmanager
def delivery_attempt(config, platform, chat_id, *, metadata=None, adapter=None,
                     producer=None, operations=("text",), thread_id=_MISSING, output_class=None):
    routed = check_delivery(config, platform, chat_id, metadata=metadata, adapter=adapter,
        producer=producer, operations=operations, thread_id=thread_id, output_class=output_class)
    scope = _valid_scope()
    if scope is None:
        _deny()
    stage = producer or _producer.get()
    workspace = routed.get("slack_team_id") or routed.get("scope_id")
    if not workspace:
        extra = getattr(config, "extra", {}) or {}
        workspace = extra.get("workspace_id") or extra.get("scope_id")
    key = (stage, _platform(platform), workspace, str(chat_id), _resolve_thread(routed))
    rights = _operations(operations)
    active = _attempt.get()
    with _lock:
        if active is not None:
            if (not isinstance(active, _AttemptRecord)
                    or scope.ledger.get(active.key) is not active or active.scope is not scope
                    or active.key != key or not rights.issubset(active.operations)):
                _deny()
            if active.uncertain or active.state != "pending":
                raise DeliveryNotConfirmed("delivery_not_confirmed")
            handle = _AttemptHandle(active, False, routed)
            owner = False
        else:
            if key in scope.ledger:
                raise DeliveryNotConfirmed("delivery_not_confirmed")
            if len(scope.ledger) >= _MAX_REGISTRY:
                _deny()
            record = _AttemptRecord(scope, key, rights, routed)
            scope.ledger[key] = record
            handle = _AttemptHandle(record, True)
            record.owner = handle
            owner = True
    marker = _attempt.set(handle._record)
    completed = False
    try:
        yield handle
        completed = True
    finally:
        if owner:
            with _lock:
                state = (handle._outcome if completed and _valid_scope() is scope
                         and not handle._record.uncertain else "unknown")
                handle._record.state = state
                if state != "confirmed":
                    scope.suppress_final = True
        elif not completed or not handle._finished:
            handle._record.uncertain = True
        _attempt.reset(marker)


def delivery_batch(function):
    """Own per-target attempt lifetimes without widening dispatcher authority."""
    @functools.wraps(function)
    def wrapped(*args, **kwargs):
        stack = ExitStack()
        marker = _batch.set(stack)
        try:
            return function(*args, **kwargs)
        finally:
            try:
                stack.__exit__(*sys.exc_info())
            finally:
                _batch.reset(marker)
    return wrapped


def begin_batch_attempt(config, platform, chat_id, **kwargs):
    stack = _batch.get()
    if stack is None:
        _deny()
    stack.close()
    return stack.enter_context(delivery_attempt(config, platform, chat_id, **kwargs))
