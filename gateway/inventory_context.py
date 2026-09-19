"""Process-local authority for one admitted foreground inventory read.

Only authenticated Slack intake and the gateway admission/worker seams issue
these capabilities. Session state, environment, model arguments and serialized
metadata cannot reconstruct them. Delivery pinning is separate from read
authority because the adapter sends the final response after the agent returns.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
import threading
import time
import weakref
import re

ABI_VERSION = "inventory_request_v1"
ROUTE_ABI_VERSION = "inventory_delivery_route_v1"
_TTL_SECONDS = 300.0
_MAX_POSTED_MESSAGES = 128
_LOCK = threading.RLock()
_INTAKES = weakref.WeakKeyDictionary()
_CURRENT = ContextVar("inventory_admitted_request", default=None)
_DELIVERY = ContextVar("inventory_intake_delivery", default=None)


class _Receipt:
    __slots__ = ("__weakref__",)


class InventoryDeliveryDenied(ValueError):
    """A pinned destination denial is terminal, never a formatting retry."""


class InventoryDeliveryUnconfirmed(InventoryDeliveryDenied):
    """An attempted SDK post has no trustworthy acknowledgment; never retry."""


@dataclass
class _Intake:
    adapter: object
    raw: dict
    workspace: str
    client: object
    bot_user_id: str
    snapshot: tuple
    expires: float
    routing: tuple
    session_thread: object
    delivery_thread: object
    question_claim: object = None
    consumed: bool = False
    post_attempts: int = 0
    posted_messages: set = field(default_factory=set)


@dataclass
class _Lease:
    receipt: object
    intake: _Intake
    event: object
    active: bool = True
    worker_entered: bool = False
    worker_active: bool = False
    worker: object = None
    request: object = None


@dataclass(frozen=True)
class InventoryRequest:
    identity: tuple
    _lease: object = field(repr=False, compare=False)

    @property
    def delivery_identity(self):
        """Attest actual output destination without redefining request ABI v1."""
        with _LOCK:
            if not self.validate():
                return None
            return (*self.identity[:5], self._lease.intake.delivery_thread)

    def validate(self):
        """Check immediately before reading and again before releasing output."""
        with _LOCK:
            lease = self._lease
            owns_execution = bool(
                _CURRENT.get() is lease and lease.request is self
                and lease.active and lease.worker_active
                and lease.worker is threading.current_thread()
            )
            if not owns_execution:
                return False
            if _DELIVERY.get() is not lease.receipt or not _normalized_matches(lease):
                lease.active = False
                lease.worker_active = False
                return False
            return True


def _string(value):
    return value if isinstance(value, str) and value and value == value.strip() else None


def _snapshot(raw):
    if type(raw) is not dict:
        return None
    if raw.get("type") not in {"message", "app_mention"}:
        return None
    if any(raw.get(key) for key in ("bot_id", "bot_profile", "subtype", "internal", "synthetic", "hidden")):
        return None
    user, channel, message = (_string(raw.get(key)) for key in ("user", "channel", "ts"))
    thread = _string(raw.get("thread_ts") or message)
    text = raw.get("text")
    if not all((user, channel, message, thread)) or not isinstance(text, str):
        return None
    return (user, channel, message, thread, text, raw.get("channel_type"))


def _record(receipt):
    if type(receipt) is not _Receipt:
        return None
    return _INTAKES.get(receipt)


def _routing(adapter):
    extra = getattr(getattr(adapter, "config", None), "extra", None)
    if not isinstance(extra, dict) or extra.get("reply_broadcast", False) is not False:
        return None
    threaded = extra.get("reply_in_thread", True)
    if type(threaded) is not bool:
        return None
    dm = extra.get("dm_top_level_threads_as_sessions")
    if dm is not None and type(dm) not in (str, bool, int):
        return None
    # Match SlackAdapter._dm_top_level_threads_as_sessions, including default.
    dm_sessions = True if dm is None else str(dm).strip().lower() in {"1", "true", "yes", "on"}
    return (threaded, dm_sessions)


def _threads(raw, snapshot, routing):
    message = snapshot[2]
    raw_thread = raw.get("thread_ts")
    if raw_thread and _string(raw_thread) is None:
        return None
    genuine = raw_thread if raw_thread and raw_thread != message else None
    threaded, dm_sessions = routing
    if snapshot[5] == "im":
        session = raw_thread or (message if dm_sessions else None)
    else:
        session = genuine or (message if threaded else None)
    delivery = genuine or (message if threaded else None)
    return session, delivery


def _intake_valid(intake):
    extra = getattr(getattr(intake.adapter, "config", None), "extra", None) if intake is not None else None
    return bool(
        intake is not None and time.monotonic() < intake.expires
        and isinstance(extra, dict)
        and extra.get("reply_broadcast", False) is False
        and _routing(intake.adapter) == intake.routing
        and _threads(intake.raw, intake.snapshot, intake.routing) == (intake.session_thread, intake.delivery_thread)
        and _snapshot(intake.raw) == intake.snapshot
        and getattr(intake.adapter, "_team_clients", {}).get(intake.workspace) is intake.client
        and getattr(intake.adapter, "_team_bot_user_ids", {}).get(intake.workspace) == intake.bot_user_id
        and (intake.question_claim is None or _question_valid(intake.question_claim,intake.raw,intake.workspace,intake.bot_user_id))
    )


def issue_intake(adapter, event, *, workspace, client, bot_user_id):
    """Issue only from an authenticated callback with its selected client."""
    workspace, bot_user_id = _string(workspace), _string(bot_user_id)
    snapshot = _snapshot(event)
    if not workspace or not bot_user_id or client is None or snapshot is None:
        return None
    user, _channel, _message, _thread, text, channel_type = snapshot
    if user == bot_user_id:
        return None
    if (getattr(adapter, "_team_clients", {}).get(workspace) is not client
            or getattr(adapter, "_team_bot_user_ids", {}).get(workspace) != bot_user_id):
        return None
    question_claim = None
    if channel_type != "im" and f"<@{bot_user_id}>" not in text:
        question_claim = _claim_question(event,workspace,bot_user_id)
        if question_claim is None:
            return None
    routing = _routing(adapter)
    threads = _threads(event, snapshot, routing) if routing is not None else None
    if threads is None:
        return None
    receipt = _Receipt()
    with _LOCK:
        candidate = _Intake(adapter, event, workspace, client, bot_user_id,
                            snapshot, time.monotonic() + _TTL_SECONDS, routing, *threads, question_claim=question_claim)
        if not _intake_valid(candidate):
            return None
        _INTAKES[receipt] = candidate
    return receipt


def intake_workspace(receipt, adapter, event):
    with _LOCK:
        intake = _record(receipt)
        if _intake_valid(intake) and intake.adapter is adapter and intake.raw is event:
            return intake.workspace
    return None


def _normalized_matches(lease):
    intake, event = lease.intake, lease.event
    if not _intake_valid(intake):
        return False
    if (getattr(event, "internal", False) or getattr(event, "synthetic", False)
            or getattr(event, "_hermes_startup_restore_replay", False)):
        return False
    metadata = getattr(event, "metadata", None)
    if not isinstance(metadata, dict) or metadata.get("_inventory_intake") is not lease.receipt:
        return False
    if getattr(event, "raw_message", None) is not intake.raw:
        return False
    source = getattr(event, "source", None)
    user, channel, message, thread, _text, _channel_type = intake.snapshot
    return bool(
        getattr(getattr(source, "platform", None), "value", None) == "slack"
        and getattr(source, "user_id", None) == user
        and getattr(source, "chat_id", None) == channel
        and getattr(source, "thread_id", None) == intake.session_thread
        and getattr(source, "scope_id", intake.workspace) == intake.workspace
        and getattr(event, "message_id", None) == message
        and getattr(intake.adapter, "_channel_team", {}).get(channel) == intake.workspace
    )


def clear_inherited():
    """Clear inherited read and delivery state without revoking the parent."""
    _CURRENT.set(None)
    _DELIVERY.set(None)


@contextmanager
def admitted_request(event, adapter, profile):
    """Consume actual intake once, after normal gateway authorization passed."""
    lease = None
    metadata = getattr(event, "metadata", None)
    receipt = metadata.get("_inventory_intake") if isinstance(metadata, dict) else None
    with _LOCK:
        intake = _record(receipt)
        if intake is not None and not intake.consumed:
            intake.consumed = True
            candidate = _Lease(receipt, intake, event)
            if (intake.adapter is adapter and _string(profile) and _normalized_matches(candidate)
                    and (intake.question_claim is None or intake.question_claim.authorization.profile_id==profile)):
                lease = candidate
                user, channel, _message, thread, _text, _channel_type = intake.snapshot
                lease.request = InventoryRequest((profile, user, "slack", intake.workspace, channel, thread), lease)
    if lease is not None:
        # A queued base-adapter drain task can inherit the previous turn's
        # context. Gateway entry clears that state; admission pins THIS intake.
        # Keep this task-local delivery pin after read authority is revoked:
        # the calling base adapter sends the final response after we return.
        # Resetting here would restore a stale predecessor or lose the route.
        _DELIVERY.set(receipt)
    token = _CURRENT.set(lease)
    try:
        yield None
    finally:
        if lease is not None:
            with _LOCK:
                lease.active = False
                lease.worker_active = False
        _CURRENT.reset(token)


@contextmanager
def foreground_worker():
    """Bind once around the gateway's real synchronous run_conversation."""
    lease = _CURRENT.get()
    allowed = False
    with _LOCK:
        if lease is not None and lease.active and not lease.worker_entered and _normalized_matches(lease):
            lease.worker_entered = True
            lease.worker_active = True
            lease.worker = threading.current_thread()
            allowed = True
    token = _CURRENT.set(lease if allowed else None)
    try:
        yield None
    finally:
        if allowed:
            with _LOCK:
                lease.worker_active = False
        _CURRENT.reset(token)


def capture_inventory_request():
    lease = _CURRENT.get()
    request = lease.request if lease is not None else None
    return request if request is not None and request.validate() else None


@contextmanager
def intake_delivery(receipt):
    """Pin output routing; copies may deliver after the callback/turn returns."""
    with _LOCK:
        valid = _record(receipt) is not None
    token = _DELIVERY.set(receipt if valid else None)
    try:
        yield None
    finally:
        _DELIVERY.reset(token)


def has_intake_delivery():
    return _DELIVERY.get() is not None


def delivery_client(adapter, chat_id, *, team_id=None):
    """Return None only without pinned intake; invalid pinned routes raise."""
    receipt = _DELIVERY.get()
    if receipt is None:
        return None
    with _LOCK:
        intake = _record(receipt)
        if (_intake_valid(intake) and intake.adapter is adapter
                and chat_id == intake.snapshot[1]
                and (team_id in (None, "") or team_id == intake.workspace)):
            return intake.client
    raise InventoryDeliveryDenied("Inventory intake delivery is unavailable")


def validate_delivery_thread(adapter, thread_id):
    """Validate the resolved SDK route; normalize only this intake's own key."""
    receipt = _DELIVERY.get()
    if receipt is None:
        return thread_id
    with _LOCK:
        intake = _record(receipt)
        if not _intake_valid(intake) or intake.adapter is not adapter:
            raise InventoryDeliveryDenied("Inventory intake delivery is unavailable")
        if thread_id == intake.delivery_thread:
            return thread_id
        # Flat DM follow-on sends can carry the known synthetic session key
        # without reply_to. It is not authority to open a different thread.
        if (intake.delivery_thread is None and intake.session_thread == intake.snapshot[2]
                and thread_id == intake.session_thread):
            return None
    raise InventoryDeliveryDenied("Inventory intake delivery is unavailable")


def validate_delivery_metadata(adapter, reply_to, metadata):
    """Check supplied keys before the adapter can discard a synthetic key."""
    if _DELIVERY.get() is None:
        return
    with _LOCK:
        intake = _record(_DELIVERY.get())
        if not _intake_valid(intake) or intake.adapter is not adapter:
            raise InventoryDeliveryDenied("Inventory intake delivery is unavailable")
        if metadata is not None and not isinstance(metadata, dict):
            raise InventoryDeliveryDenied("Inventory intake delivery is unavailable")
        md = metadata or {}
        values = [md[key] for key in ("thread_id", "thread_ts") if md.get(key) is not None]
        if (any(_string(value) is None for value in values)
                or len(set(values)) > 1
                or any(value not in (intake.session_thread, intake.delivery_thread) for value in values)
                or (reply_to is not None and reply_to not in (intake.snapshot[2], intake.snapshot[3]))):
            raise InventoryDeliveryDenied("Inventory intake delivery is unavailable")


def reserve_delivery(adapter, chat_id, thread_id, *, team_id=None):
    """Bound post attempts atomically; never refund unknown SDK outcomes."""
    if _DELIVERY.get() is None:
        return
    with _LOCK:
        delivery_client(adapter, chat_id, team_id=team_id)
        validate_delivery_thread(adapter, thread_id)
        intake = _record(_DELIVERY.get())
        if intake.post_attempts >= _MAX_POSTED_MESSAGES:
            raise InventoryDeliveryDenied("Inventory intake delivery is unavailable")
        intake.post_attempts += 1


def record_delivery(adapter, chat_id, message_id, thread_id, *, team_id=None):
    """Record only an acknowledged public post; unknown results grant no edit."""
    if _DELIVERY.get() is None:
        return
    with _LOCK:
        delivery_client(adapter, chat_id, team_id=team_id)
        actual = validate_delivery_thread(adapter, thread_id)
        intake = _record(_DELIVERY.get())
        if type(message_id) is not str or re.fullmatch(r"[0-9]{1,16}\.[0-9]{1,9}", message_id) is None:
            return False
        if len(intake.posted_messages) >= _MAX_POSTED_MESSAGES:
            raise InventoryDeliveryDenied("Inventory intake delivery is unavailable")
        if (message_id, actual) in intake.posted_messages:
            raise InventoryDeliveryUnconfirmed("Slack response acknowledgment is unavailable")
        intake.posted_messages.add((message_id, actual))
        return True


def validate_edit(adapter, chat_id, message_id, *, team_id=None):
    if _DELIVERY.get() is None:
        return
    with _LOCK:
        delivery_client(adapter, chat_id, team_id=team_id)
        intake = _record(_DELIVERY.get())
        if (message_id, intake.delivery_thread) not in intake.posted_messages:
            raise InventoryDeliveryDenied("Inventory intake delivery is unavailable")


def validate_private_recipient(adapter, user_id):
    if _DELIVERY.get() is None:
        return
    with _LOCK:
        intake = _record(_DELIVERY.get())
        if _intake_valid(intake) and intake.adapter is adapter and user_id == intake.snapshot[0]:
            return
    raise InventoryDeliveryDenied("Inventory intake delivery is unavailable")


# Additive request-evidence ABI; no change to inventory_request_v1.
EVIDENCE_ABI_VERSION = "inventory_source_evidence_v2"
_EVIDENCE_HANDLES = weakref.WeakKeyDictionary()


@dataclass(frozen=True)
class InventorySourceEvidence:
    identity: tuple
    bot_user_id: str
    event_id: str
    message_id: str
    reply_to_message_id: object
    reference_kind: str
    text: str
    observed: str


@dataclass(frozen=True, eq=False)
class InventoryEvidenceHandle:
    _request: object = field(repr=False, compare=False)

    def read(self):
        """Revalidate the owning admitted worker before exposing original fields.

        Slack thread_ts is a thread root, never proof of an arbitrary reply's
        parent. A caller must also match a durable, confirmed outgoing question.
        """
        from datetime import datetime, timezone
        import hashlib
        import json
        with _LOCK:
            request = _EVIDENCE_HANDLES.get(self)
            if request is not self._request or type(request) is not InventoryRequest or not request.validate():
                return None
            intake = request._lease.intake
            user, channel, message, _thread, text, _channel_type = intake.snapshot
            if (not re.fullmatch(r"[0-9]{1,16}\.[0-9]{1,9}",message)
                    or len(text.encode("utf-8")) > 65536):
                return None
            raw_thread = intake.raw.get("thread_ts")
            root = raw_thread if raw_thread and raw_thread != message else None
            if root is not None and re.fullmatch(r"[0-9]{1,16}\.[0-9]{1,9}",root) is None:
                return None
            try:
                observed = datetime.fromtimestamp(float(message),timezone.utc).isoformat().replace("+00:00","Z")
            except (ValueError,OverflowError,OSError):
                return None
            digest = hashlib.sha256(json.dumps([*request.identity[:5],message],separators=(",",":")).encode()).hexdigest()
            evidence = InventorySourceEvidence(request.identity,intake.bot_user_id,"slack-"+digest,message,root,
                                               "thread_root" if root is not None else "none",text,observed)
            return evidence if request.validate() else None


def capture_inventory_evidence():
    """Return an opaque, worker-bound handle; tool arguments cannot issue one."""
    with _LOCK:
        request = capture_inventory_request()
        if type(request) is not InventoryRequest or not request.validate():
            return None
        handle = InventoryEvidenceHandle(request)
        _EVIDENCE_HANDLES[handle] = request
        return handle


ACTIVE_QUESTION_ABI_VERSION = "inventory_active_question_v1"
_QUESTION_RESOLVER = None
_QUESTION_GENERATION = 0


@dataclass(frozen=True)
class ActiveQuestionAuthorization:
    """Trusted host result of a read-only protected grant/question lookup."""
    profile_id: str
    question_id: str
    expires_at: float
    revalidate: object = field(repr=False, compare=False)


@dataclass(frozen=True)
class _QuestionClaim:
    authorization: ActiveQuestionAuthorization
    identity: tuple
    generation: int


def bind_active_question_resolver(resolver):
    """Trusted startup only. Rebinding revokes claims made by the prior resolver."""
    global _QUESTION_RESOLVER, _QUESTION_GENERATION
    if resolver is not None and not callable(resolver):
        raise ValueError("invalid question resolver")
    with _LOCK:
        _QUESTION_RESOLVER = resolver
        _QUESTION_GENERATION += 1


def _question_identity(raw, workspace, bot_user_id):
    snapshot = _snapshot(raw)
    if snapshot is None:
        return None
    user,channel,message,_thread,_text,_kind = snapshot
    root = raw.get("thread_ts")
    if (not root or root==message or not isinstance(root,str)
            or re.fullmatch(r"[0-9]{1,16}\.[0-9]{1,9}",root) is None):
        return None
    return (workspace,bot_user_id,channel,user,root,message)


def _question_valid(claim, raw, workspace, bot_user_id):
    import math
    try:
        if (type(claim) is not _QuestionClaim or claim.generation != _QUESTION_GENERATION
                or _QUESTION_RESOLVER is None
                or claim.identity != _question_identity(raw,workspace,bot_user_id)):
            return False
        resolver, generation = _QUESTION_RESOLVER, _QUESTION_GENERATION
        auth = claim.authorization
        return bool(type(auth) is ActiveQuestionAuthorization
                    and _string(auth.profile_id) and _string(auth.question_id)
                    and type(auth.expires_at) in (int,float) and math.isfinite(auth.expires_at)
                    and time.time() < auth.expires_at and callable(auth.revalidate)
                    and auth.revalidate() is True
                    and resolver is _QUESTION_RESOLVER
                    and generation == _QUESTION_GENERATION
                    and time.time() < auth.expires_at)
    except Exception:
        return False


def _claim_question(raw,workspace,bot_user_id):
    identity = _question_identity(raw,workspace,bot_user_id)
    with _LOCK:
        resolver, generation = _QUESTION_RESOLVER, _QUESTION_GENERATION
    if identity is None or resolver is None:
        return None
    try:
        authorization = resolver(identity)
        with _LOCK:
            if resolver is not _QUESTION_RESOLVER or generation != _QUESTION_GENERATION:
                return None
            claim = _QuestionClaim(authorization,identity,generation)
            return claim if _question_valid(claim,raw,workspace,bot_user_id) else None
    except Exception:
        return None


def active_question_reply(receipt,adapter,raw):
    """Same-event read-only gate; it never records a reply or issues authority."""
    with _LOCK:
        intake = _record(receipt)
        return bool(intake is not None and intake.adapter is adapter and intake.raw is raw
                    and intake.question_claim is not None and _intake_valid(intake))
