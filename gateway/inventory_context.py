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
_CONTEXT_ATTESTATIONS = weakref.WeakKeyDictionary()


class _ContextAttestation:
    __slots__ = ("__weakref__",)


def capture_inventory_context_attestation():
    """Opaque original-message attestation for the current foreground read."""
    with _LOCK:
        request = capture_inventory_request()
        if request is None:
            return None
        handle = _ContextAttestation()
        # Request dataclass equality ignores its lease; never use it as a key.
        # A retained attestation must not retain the native request payload.
        _CONTEXT_ATTESTATIONS[handle] = weakref.ref(request)
        return handle if resolve_inventory_context_attestation(handle) is not None else None


def resolve_inventory_context_attestation(handle):
    """Revalidate exact registry membership and original owner before release."""
    with _LOCK:
        if type(handle) is not _ContextAttestation:
            return None
        reference = _CONTEXT_ATTESTATIONS.get(handle)
        request = reference() if reference is not None else None
        if request is None or capture_inventory_request() is not request or not request.validate():
            return None
        delivery = request.delivery_identity
        if delivery is None:
            return None
        value = {"identity": request.identity, "delivery_identity": delivery,
                 "message_id": request._lease.intake.snapshot[2],
                 "bot_user_id": request._lease.intake.bot_user_id}
        return value if request.validate() else None


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
    )


def issue_intake(adapter, event, *, workspace, client, bot_user_id):
    """Issue only from an authenticated callback with its selected client."""
    workspace, bot_user_id = _string(workspace), _string(bot_user_id)
    snapshot = _snapshot(event)
    if not workspace or not bot_user_id or client is None or snapshot is None:
        return None
    user, _channel, _message, _thread, text, channel_type = snapshot
    if user == bot_user_id or (channel_type != "im" and f"<@{bot_user_id}>" not in text):
        return None
    if (getattr(adapter, "_team_clients", {}).get(workspace) is not client
            or getattr(adapter, "_team_bot_user_ids", {}).get(workspace) != bot_user_id):
        return None
    routing = _routing(adapter)
    threads = _threads(event, snapshot, routing) if routing is not None else None
    if threads is None:
        return None
    receipt = _Receipt()
    with _LOCK:
        candidate = _Intake(adapter, event, workspace, client, bot_user_id,
                            snapshot, time.monotonic() + _TTL_SECONDS, routing, *threads)
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
            if intake.adapter is adapter and _string(profile) and _normalized_matches(candidate):
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

def delivery_denial_reason(adapter, chat_id, *, team_id=None):
    """Short reason a pinned intake denies this destination, for log lines.

    None when there is no pinned intake or the destination would pass. The
    vocabulary is fixed and carries no user text, tokens or provider
    payloads. Motivation (2026-09-22): a desk turn ran 356.7 s against the
    300 s lease and the refusal logged only the canned denial text, so the
    expired lease was invisible until MAIN reconstructed it by hand.
    """
    receipt = _DELIVERY.get()
    if receipt is None:
        return None
    with _LOCK:
        intake = _record(receipt)
        if not _intake_valid(intake):
            if intake is not None and time.monotonic() >= intake.expires:
                return "intake lease expired"
            return "intake no longer valid"
        if intake.adapter is not adapter:
            return "intake adapter mismatch"
        if chat_id != intake.snapshot[1]:
            return "intake channel mismatch"
        if team_id not in (None, "") and team_id != intake.workspace:
            return "intake workspace mismatch"
        if intake.post_attempts >= _MAX_POSTED_MESSAGES:
            return "intake post budget exhausted"
    return None


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
