"""Scoped synchronous cloud-context tool; inert without trusted caller ports.

This module imports no application, configuration or provider modules. A caller
must supply the actual registry and the same lock used to publish agent.tools.
The factory prepares an invocation, not a launch; only the original foreground
thread and Context may spend it. Public content approval stays with that factory.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass
import json
import math
import threading
from weakref import WeakKeyDictionary, ref

TOOL_NAME = 'ito_cloud_context'
_TOOLSET = 'ito-scoped-context'
_SUPPORTED_MODES = frozenset({'chat_completions', 'anthropic_messages', 'codex_responses'})
_MAX_PUBLIC_BYTES = 8192
_SCHEMA = {
    'name': TOOL_NAME,
    'description': 'Retrieve the approved inventory and matching context for this request.',
    'parameters': {'type': 'object', 'properties': {}, 'additionalProperties': False},
}
_HELD = '{"status":"held"}'
_CURRENT = ContextVar('ito_context_tool_binding', default=None)
_WITNESS = ContextVar('ito_context_tool_origin', default=None)
_STATE_LOCK = threading.RLock()
_BINDINGS = {}
_REGISTRATIONS = WeakKeyDictionary()


@dataclass(frozen=True)
class PreparedContextCall:
    """Trusted, single-use ports; never construct from model/event arguments.

    invoke closes over the reviewed dispatcher.run graph. validate checks current
    native authority. project_result explicitly approves a closed public mapping
    for this exact conversation; it must not return the raw dispatcher outcome.
    All callbacks must be bounded; Python cannot preempt a blocked trusted port.
    """
    invoke: object
    validate: object
    project_result: object
    expires_at: float
    clock: object
    monotonic_clock: object


class ContextToolBindingError(RuntimeError):
    """Fixed safe overlap error; the attempted conversation must not run."""


class _Held(Exception):
    pass


class _Binding:
    def __init__(self, agent, registry, snapshot_lock):
        self.agent = agent
        self.registry = registry
        self.snapshot_lock = snapshot_lock
        self.thread = threading.current_thread()
        self.witness = object()
        self.token = None
        self.prepared = None
        self.ready = False
        self.closed = False
        self.spent = False
        self.invalid = False
        self.wall_start = self.wall_last = None
        self.mono_start = self.mono_last = None

    def origin(self):
        if threading.current_thread() is not self.thread or _CURRENT.get() is not self:
            return False
        if _WITNESS.get() is not self.witness or self.token is None:
            return False
        try:
            # A copied Context contains the same values but cannot reset this
            # token. Rearm only after a successful original-Context reset.
            _WITNESS.reset(self.token)
            self.token = _WITNESS.set(self.witness)
        except (ValueError, RuntimeError):
            return False
        return True

    def time_valid(self):
        try:
            prepared = self.prepared
            if prepared is None or self.invalid:
                return False
            wall = _number(prepared.clock())
            mono = _number(prepared.monotonic_clock())
            if self.wall_start is None:
                self.wall_start = self.wall_last = wall
                self.mono_start = self.mono_last = mono
            if wall < self.wall_last or mono < self.mono_last:
                raise _Held()
            self.wall_last, self.mono_last = wall, mono
            effective = max(wall, _number(self.wall_start + mono - self.mono_start))
            if effective >= _number(prepared.expires_at):
                raise _Held()
            return True
        except Exception:
            self.invalid = True
            return False

    def mode_valid(self):
        if self.closed or self.agent is None or self.invalid:
            return False
        if getattr(self.agent, 'api_mode', None) not in _SUPPORTED_MODES:
            self.invalid = True
            return False
        return True

    def valid(self):
        if not self.mode_valid() or not self.origin() or not self.time_valid():
            return False
        with _STATE_LOCK:
            if _BINDINGS.get(id(self.agent)) is not self:
                return False
        if not _registration_valid(self.registry):
            return False
        try:
            return (self.prepared.validate() is True and self.mode_valid()
                and self.origin() and self.time_valid() and self.mode_valid())
        except Exception:
            return False


def _number(value):
    if type(value) not in (int, float):
        raise _Held()
    value = float(value)
    if not math.isfinite(value):
        raise _Held()
    return value


def _never_available():
    return False


def _entry_matches(entry, registration):
    return (
        entry is registration[0]
        and entry.name == TOOL_NAME and entry.toolset == _TOOLSET
        and entry.schema == _SCHEMA and entry.handler is registration[1]
        and entry.check_fn is _never_available and entry.is_async is False
        and entry.requires_env == [] and entry.dynamic_schema_overrides is None
        and entry.max_result_size_chars == _MAX_PUBLIC_BYTES
        and entry.description == _SCHEMA['description'] and entry.emoji == ''
    )


def _registration_valid(registry):
    try:
        with _STATE_LOCK, registry._lock:
            registration = _REGISTRATIONS.get(registry)
            return registration is not None and _entry_matches(registry._tools.get(TOOL_NAME), registration)
    except Exception:
        return False


def _ensure_registration(registry):
    """Pin one exact entry. Never overwrite, recover or adopt a collision."""
    try:
        with _STATE_LOCK, registry._lock:
            registration = _REGISTRATIONS.get(registry)
            existing = registry._tools.get(TOOL_NAME)
            if registration is not None:
                return _entry_matches(existing, registration)
            if existing is not None:
                return False
            registry_ref = ref(registry)
            def handler(args, **kwargs):
                # Registry kwargs (task IDs, logging details) supply no authority.
                return _dispatch(registry_ref(), args)
            registry.register(TOOL_NAME, _TOOLSET, deepcopy(_SCHEMA), handler,
                check_fn=_never_available, is_async=False,
                max_result_size_chars=_MAX_PUBLIC_BYTES)
            entry = registry._tools.get(TOOL_NAME)
            registration = (entry, handler)
            if not _entry_matches(entry, registration):
                return False
            _REGISTRATIONS[registry] = registration
            return True
    except Exception:
        return False


def _clean_outcome(outcome):
    if getattr(outcome, 'state', None) != 'completed':
        return False
    receipt = getattr(outcome, 'closure_receipt', None)
    return (type(receipt) is dict and receipt.get('cleanup_complete') is True
        and receipt.get('durable_capacity_released') is False
        and receipt.get('canonical_state') == 'execution_succeeded_pending_review')


def _public_result(value):
    if type(value) is not dict or set(value) != {'status', 'context'}:
        raise _Held()
    if value['status'] != 'completed' or type(value['context']) is not str:
        raise _Held()
    # Bound before serializing to avoid constructing a large duplicate buffer.
    if len(value['context']) > _MAX_PUBLIC_BYTES:
        raise _Held()
    result = json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
    if len(result.encode('utf-8')) > _MAX_PUBLIC_BYTES:
        raise _Held()
    return result


def _dispatch(registry, args):
    """No exceptions, receipt internals or authority objects cross this boundary."""
    try:
        binding = _CURRENT.get()
        if type(args) is not dict or args or binding is None:
            return _HELD
        if binding.registry is not registry or binding.closed or binding.spent or not binding.ready:
            return _HELD
        # Native validation is a trusted callback, so reserve the invocation
        # before running it. Wrong physical/context origins do not spend it.
        if not binding.origin():
            return _HELD
        binding.spent = True
        if not binding.valid():
            return _HELD
        outcome = binding.prepared.invoke()
        if not binding.valid() or not _clean_outcome(outcome):
            return _HELD
        public = binding.prepared.project_result(outcome)
        if not binding.valid() or not _clean_outcome(outcome):
            return _HELD
        result = _public_result(public)
        return result if binding.valid() else _HELD
    except Exception:
        return _HELD


def project_context_tool(agent, definitions, names, *, registry=None):
    """Publish under the caller's snapshot lock, with positive registry ownership.

    Without a supplied registry or an exact live binding, preserve metadata: the
    name alone does not prove it is ours. A foreign replacement owns its current
    schema, including a byte-identical one. Native validation/invocation/content
    projection ports are never called here.
    """
    projected = list(definitions)
    projected_names = set(names)
    with _STATE_LOCK:
        binding = _BINDINGS.get(id(agent))
        live = binding is not None and binding.agent is agent and not binding.closed
        selected = registry if registry is not None else (binding.registry if live else None)
        if selected is None:
            return projected, projected_names
        registration = _REGISTRATIONS.get(selected)
        if registration is None:
            return projected, projected_names
        with selected._lock:
            entry = selected._tools.get(TOOL_NAME)
            ours_or_absent = entry is registration[0] or entry is None
            foreign_schema = deepcopy(entry.schema) if not ours_or_absent else None
        def keep(definition):
            if not (isinstance(definition, dict)
                    and isinstance(definition.get('function'), dict)
                    and definition['function'].get('name') == TOOL_NAME):
                return True
            if ours_or_absent:
                return False
            function = definition['function']
            # Metadata follows the current foreign owner when identical. Only
            # an identifiable stale owned definition may otherwise be removed.
            return function == foreign_schema or function != _SCHEMA
        projected = [definition for definition in projected if keep(definition)]
        has_named_definition = any(isinstance(definition, dict)
            and isinstance(definition.get('function'), dict)
            and definition['function'].get('name') == TOOL_NAME for definition in projected)
        if not has_named_definition:
            projected_names.discard(TOOL_NAME)
        if (live and binding.registry is selected and binding.ready
                and binding.prepared is not None and binding.mode_valid()
                and binding.time_valid() and binding.mode_valid() and _registration_valid(selected)):
            projected.append({'type': 'function', 'function': deepcopy(_SCHEMA)})
            projected_names.add(TOOL_NAME)
    return projected, projected_names


@contextmanager
def bind_context_tool(agent, *, factory=None, registry, snapshot_lock):
    """Attach at most one prepared capability to an original foreground turn.

    Overlap raises a fixed safe error before its body or factory can execute. The same lock
    must serialize every refresh/publication and this scope's close. A cached
    agent's unrelated tool changes survive closing and subsequent turns.
    """
    with snapshot_lock, _STATE_LOCK:
        if id(agent) in _BINDINGS or _CURRENT.get() is not None:
            raise ContextToolBindingError('Cloud context binding already active')
    if not callable(factory) or getattr(agent, 'api_mode', None) not in _SUPPORTED_MODES:
        yield False
        return
    binding = None
    current_token = None
    with snapshot_lock:
        with _STATE_LOCK:
            if id(agent) in _BINDINGS or _CURRENT.get() is not None:
                raise ContextToolBindingError('Cloud context binding already active')
            if _ensure_registration(registry):
                binding = _Binding(agent, registry, snapshot_lock)
                _BINDINGS[id(agent)] = binding
    if binding is None:
        yield False
        return
    try:
        current_token = _CURRENT.set(binding)
        binding.token = _WITNESS.set(binding.witness)
        try:
            prepared = factory(agent=agent)
            if type(prepared) is not PreparedContextCall or not all(callable(port) for port in (
                    prepared.invoke, prepared.validate, prepared.project_result,
                    prepared.clock, prepared.monotonic_clock)):
                raise _Held()
            _number(prepared.expires_at)
            binding.prepared = prepared
            if not binding.valid():
                raise _Held()
            binding.ready = True
            with snapshot_lock:
                agent.tools, agent.valid_tool_names = project_context_tool(agent, agent.tools, agent.valid_tool_names, registry=registry)
        except Exception:
            binding.invalid = True
            binding.prepared = None
        yield binding.prepared is not None and not binding.invalid
    finally:
        cleanup_failed = False
        # Retire authority first even if publication/lock acquisition fails.
        with _STATE_LOCK:
            binding.closed = True
            binding.ready = False
            binding.prepared = None
            if _BINDINGS.get(id(agent)) is binding:
                del _BINDINGS[id(agent)]
        try:
            with snapshot_lock:
                agent.tools, agent.valid_tool_names = project_context_tool(
                    agent, agent.tools, agent.valid_tool_names, registry=registry)
        except Exception:
            cleanup_failed = True
        finally:
            try:
                if binding.token is not None:
                    _WITNESS.reset(binding.token)
            except (ValueError, RuntimeError):
                cleanup_failed = True
            try:
                if current_token is not None:
                    _CURRENT.reset(current_token)
            except (ValueError, RuntimeError):
                cleanup_failed = True
            finally:
                # Closed copied Contexts may outlive this turn. They keep only
                # an inert shell, never cached agent/registry/authority graphs.
                binding.agent = None
                binding.registry = None
                binding.thread = None
                binding.snapshot_lock = None
                binding.witness = None
                binding.token = None
        if cleanup_failed:
            raise ContextToolBindingError('Cloud context cleanup failed') from None
