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

ABI_VERSION = "inventory_request_v1"
_TTL_SECONDS = 300.0
_LOCK = threading.RLock()
_INTAKES = weakref.WeakKeyDictionary()
_CURRENT = ContextVar("inventory_admitted_request", default=None)
_DELIVERY = ContextVar("inventory_intake_delivery", default=None)


class _Receipt:
    __slots__ = ("__weakref__",)


@dataclass
class _Intake:
    adapter: object
    raw: dict
    workspace: str
    client: object
    bot_user_id: str
    snapshot: tuple
    expires: float
    consumed: bool = False


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


def _intake_valid(intake):
    extra = getattr(getattr(intake.adapter, "config", None), "extra", None) if intake is not None else None
    return bool(
        intake is not None and time.monotonic() < intake.expires
        and isinstance(extra, dict)
        and extra.get("reply_broadcast", False) is False
        and extra.get("reply_in_thread", True) is True
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
    receipt = _Receipt()
    with _LOCK:
        candidate = _Intake(adapter, event, workspace, client, bot_user_id,
                            snapshot, time.monotonic() + _TTL_SECONDS)
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
        and getattr(source, "thread_id", None) == thread
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
    raise ValueError("Inventory intake delivery is unavailable")
