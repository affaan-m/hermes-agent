"""Default-disabled audience association for one original Slack request.

Trusted native composition supplies enrollment and an independently authenticated
opaque host caller. This module has no model-facing grant endpoint, transport,
Store, SDK, configuration lookup or installed-runtime initialization.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import hashlib
import json
import math
import re
import threading
import time
import weakref

from .inventory_context import (capture_inventory_context_attestation,
                                resolve_inventory_context_attestation)

_ENROLLMENT = {"profile_id", "workspace_id", "bot_user_id", "user_id", "channel_id",
               "thread_id", "message_id", "company_id", "projection", "expires_at"}
_MAX_REQUESTS = 128
_MAX_LIFETIME = 120.0


def _number(value):
    try:
        finite = type(value) in (int, float) and math.isfinite(value)
    except OverflowError:
        finite = False
    if not finite or value < 0:
        raise ValueError("invalid audience number")
    return value


def _text(value):
    if type(value) is not str or not 0 < len(value) <= 128 or value != value.strip() or any(ord(c) < 32 for c in value):
        raise ValueError("invalid audience identity")
    return value


def _projection(value):
    if type(value) is not dict or set(value) != {"company_id", "evaluation_time", "limit"}:
        raise ValueError("invalid audience projection")
    if type(value["company_id"]) is not str or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value["company_id"]) is None:
        raise ValueError("invalid audience company")
    if type(value["limit"]) is not int or not 1 <= value["limit"] <= 20:
        raise ValueError("invalid audience row limit")
    return dict(company_id=value["company_id"], evaluation_time=_number(value["evaluation_time"]), limit=value["limit"])


def _copy(value):
    return json.loads(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False))


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


class _AudienceHandle:
    __slots__ = ("__weakref__",)


@dataclass
class _Scope:
    host: object
    attestation: object
    original: dict
    descriptor: dict
    thread: object
    mono_deadline: float
    active: bool = True
    handle: object = None


class SlackContextAudience:
    """A trusted owner's empty-by-default, exact-message enrollment manager."""
    def __init__(self, *, clock=None, monotonic_clock=None):
        self._clock = clock or time.time
        self._monotonic = monotonic_clock or time.monotonic
        self._owner = threading.current_thread()
        self._lock = threading.RLock()
        self._epoch = 0
        self._enrollment = None
        self._enrollment_deadline = 0.0
        self._last_mono = _number(self._monotonic())
        self._current = ContextVar("slack_context_audience", default=None)
        self._handles = weakref.WeakKeyDictionary()
        self._active = {}
        self._seen = {}

    def _times(self):
        wall, mono = _number(self._clock()), _number(self._monotonic())
        if mono < self._last_mono:
            raise ValueError("audience monotonic clock regressed")
        self._last_mono = mono
        return wall, mono

    def _new_epoch(self, epoch):
        if threading.current_thread() is not self._owner:
            raise ValueError("audience enrollment requires its trusted owner")
        if type(epoch) is not int or not self._epoch < epoch <= 2**53:
            raise ValueError("audience epoch must increase")

    def _retire(self, scope):
        scope.active = False
        scope.host = None
        scope.attestation = None
        if scope.handle is not None:
            handle = scope.handle()
            if handle is not None:
                self._handles.pop(handle, None)
        scope.handle = None
        self._active.pop(id(scope), None)

    def _retire_all(self):
        for scope in list(self._active.values()):
            self._retire(scope)

    def bind(self, *, authority_epoch, enrollment):
        with self._lock:
            self._new_epoch(authority_epoch)
            if type(enrollment) is not dict or set(enrollment) != _ENROLLMENT:
                raise ValueError("closed exact-message enrollment required")
            value = {}
            for key in _ENROLLMENT - {"projection", "expires_at"}:
                value[key] = None if key == "thread_id" and enrollment[key] is None else _text(enrollment[key])
            value["projection"] = _projection(enrollment["projection"])
            value["expires_at"] = _number(enrollment["expires_at"])
            wall, mono = self._times()
            if (value["company_id"] != value["projection"]["company_id"]
                    or not wall < value["expires_at"] <= wall + _MAX_LIFETIME):
                raise ValueError("invalid audience enrollment scope")
            self._retire_all()
            self._epoch = authority_epoch
            self._enrollment = value
            self._enrollment_deadline = mono + value["expires_at"] - wall

    def disable(self, *, authority_epoch):
        with self._lock:
            self._new_epoch(authority_epoch)
            self._retire_all()
            self._epoch = authority_epoch
            self._enrollment = None

    def _matches(self, original, enrollment):
        if type(original) is not dict or set(original) != {"identity", "delivery_identity", "message_id", "bot_user_id"}:
            return False
        identity, delivery = original["identity"], original["delivery_identity"]
        return (type(identity) is tuple and len(identity) == 6 and type(delivery) is tuple and len(delivery) == 6
            and identity[:5] == delivery[:5]
            and delivery == (enrollment["profile_id"], enrollment["user_id"], "slack", enrollment["workspace_id"],
                             enrollment["channel_id"], enrollment["thread_id"])
            and original["message_id"] == enrollment["message_id"] and original["bot_user_id"] == enrollment["bot_user_id"])

    @contextmanager
    def request_scope(self, host_caller, *, canonical_request_id, approved_projection):
        scope = None
        with self._lock:
            outer = self._current.get()
            if (outer is None and host_caller is not None and self._enrollment is not None
                    and threading.current_thread() is self._owner):
                try:
                    try:
                        wall, mono = self._times()
                    except (ValueError, TypeError):
                        self._retire_all()
                        self._enrollment = None
                        raise
                    if wall >= self._enrollment["expires_at"] or mono >= self._enrollment_deadline:
                        self._retire_all()
                        self._enrollment = None
                        raise ValueError("audience enrollment expired")
                    projection = _projection(approved_projection)
                    valid_id = type(canonical_request_id) is str and re.fullmatch(r"[0-9a-f]{32}", canonical_request_id)
                    attestation = capture_inventory_context_attestation()
                    original = resolve_inventory_context_attestation(attestation)
                    enrollment = self._enrollment
                    if (valid_id and projection == enrollment["projection"] and self._matches(original, enrollment)
                            and wall < enrollment["expires_at"] and mono < self._enrollment_deadline):
                        source_id = "slack:" + _digest([enrollment[k] for k in
                            ("profile_id", "workspace_id", "channel_id", "message_id")])
                        if source_id not in self._seen and canonical_request_id not in self._seen.values() and len(self._seen) < _MAX_REQUESTS:
                            descriptor = {"kind":"slack", "canonical_request_id":canonical_request_id,
                                "source_request_id":source_id, "authority_epoch":self._epoch, "operation":"nonpricing.context",
                                **_copy(enrollment)}
                            descriptor["expires_at"] = min(enrollment["expires_at"], wall + _MAX_LIFETIME)
                            scope = _Scope(host_caller, attestation, original, descriptor, threading.current_thread(),
                                           min(self._enrollment_deadline, mono + _MAX_LIFETIME))
                            self._active[id(scope)] = scope
                            self._seen[source_id] = canonical_request_id
                except (ValueError, TypeError):
                    scope = None
            token = self._current.set(scope)
        try:
            yield None
        finally:
            with self._lock:
                if scope is not None:
                    self._retire(scope)
                self._current.reset(token)

    def _valid(self, scope, host, now):
        if (scope is None or not scope.active or scope.host is not host
                or scope.thread is not threading.current_thread() or self._current.get() is not scope):
            return False
        try:
            wall, mono = self._times()
            now = _number(now)
            if (self._enrollment is None or self._epoch != scope.descriptor["authority_epoch"]
                    or now >= scope.descriptor["expires_at"] or wall >= scope.descriptor["expires_at"]
                    or mono >= scope.mono_deadline
                    or resolve_inventory_context_attestation(scope.attestation) != scope.original):
                self._retire(scope)
                return False
        except (ValueError, TypeError):
            self._retire(scope)
            return False
        return True

    def capture_context_audience(self, host_caller):
        with self._lock:
            scope = self._current.get()
            if not self._valid(scope, host_caller, self._clock()):
                return None
            handle = scope.handle() if scope.handle is not None else None
            if handle is None:
                handle = _AudienceHandle()
                scope.handle = weakref.ref(handle)
                self._handles[handle] = scope
            return handle

    def resolve_context_audience(self, host_caller, handle, now):
        with self._lock:
            if type(handle) is not _AudienceHandle:
                return None
            scope = self._handles.get(handle)
            if not self._valid(scope, host_caller, now):
                return None
            descriptor = _copy(scope.descriptor)
            return descriptor if self._valid(scope, host_caller, now) else None
