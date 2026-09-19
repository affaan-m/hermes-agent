"""Opt-in host composition. Profile configuration selects trusted operator code.

The provider supplies authenticated services, never model-visible arguments.
No provider import, credential resolution, owner creation or SDK call occurs
with the profile flag off. Enabling without all services fails startup closed.
"""
from dataclasses import dataclass
from copy import deepcopy
import hashlib
import importlib
import json
import time

from gateway.host_adapters import Budget, ExistingApproval, HostError, document, number


@dataclass(frozen=True)
class ApprovedContext:
    request_sha256: str
    result_sha256: str
    context: str
    expires_at: float


@dataclass(frozen=True)
class HostServices:
    session: object
    policy: object
    mqtt_config: dict
    enrollment_lookup: object
    approval_resolver: object
    receipt_reader: object
    create_owner: object
    approve_result: object
    audit_receipt: object
    typed_reviewer: object = None


class TypedReviewer(ExistingApproval):
    """Send immutable proposal bytes to an independently authenticated reviewer.

    The exchange must finish durably recording approval before returning. The
    inherited check then reads that approval independently by both exact hashes.
    This process never calls canonical.approve or canonical.accept.
    """
    def __init__(self, *, exchange, **kwargs):
        super().__init__(**kwargs)
        self.exchange = exchange
        self._proposed = False

    def __call__(self, owner, typed, spec, *, deadline_at):
        budget = Budget(deadline_at, self.clock, self.mono)
        budget.remaining()
        if self._proposed or type(typed) is not bytes or type(spec) is not bytes:
            raise HostError('typed_review_unavailable')
        self._proposed = True
        document(typed, 8192)
        document(spec, 8192)
        self.exchange(typed=typed, spec=spec, deadline_at=deadline_at)
        budget.remaining()
        return super().__call__(owner, typed, spec, deadline_at=deadline_at)


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    ensure_ascii=True, allow_nan=False).encode()).hexdigest()


def build_host_factory(services, *, profile, clock=time.time,
                       monotonic=time.monotonic, modules=None):
    """Compose the reviewed reviewer, V2 observer and terminal cleanup graph.

    The enrollment service is called with a fresh native message/delivery
    attestation. Result approval must explicitly bind that request and the exact
    result digest. An audit write is required before public release. It does not
    accept the canonical attempt or release durable capacity.
    """
    from gateway import cloud_caller
    from gateway.inventory_context import (
        capture_inventory_context_attestation, resolve_inventory_context_attestation,
    )
    if type(services) is not HostServices or type(profile) is not str or not profile:
        raise HostError('host_services_unavailable')
    for name in ('enrollment_lookup', 'approval_resolver', 'receipt_reader',
                 'create_owner', 'approve_result', 'audit_receipt', 'typed_reviewer'):
        if not callable(getattr(services, name)):
            raise HostError('host_services_unavailable')

    if services.session is None or services.policy is None:
        raise HostError('host_services_unavailable')
    if type(services.mqtt_config) is not dict or set(services.mqtt_config) != cloud_caller._MQTT_KEYS:
        raise HostError('host_services_unavailable')
    selected_modules = modules if modules is not None else cloud_caller._default_modules()

    def factory(*, agent):
        attestation = capture_inventory_context_attestation()
        original = resolve_inventory_context_attestation(attestation)
        if original is None or original['identity'][0] != profile:
            raise HostError('request_unavailable')
        request_hash = _hash(original)
        enrollment = services.enrollment_lookup(deepcopy(original))
        keys = {'host_caller', 'controller_caller', 'run_kwargs', 'expires_at'}
        if type(enrollment) is not dict or set(enrollment) != keys:
            raise HostError('request_unavailable')
        # Freeze payloads independently of the lookup's mutable backing data.
        request = dict(enrollment, run_kwargs=deepcopy(enrollment['run_kwargs']))

        def validate(_agent, candidate):
            current = resolve_inventory_context_attestation(attestation)
            if current is None or current != original:
                return False
            fresh = services.enrollment_lookup(deepcopy(current))
            return (type(fresh) is dict and set(fresh) == keys
                    and fresh['host_caller'] is request['host_caller']
                    and fresh['controller_caller'] is request['controller_caller']
                    and fresh['run_kwargs'] == request['run_kwargs']
                    and fresh['expires_at'] == request['expires_at']
                    and candidate is request)

        def project(_agent, candidate, context):
            if not validate(None, candidate):
                raise HostError('result_unavailable')
            result_hash = _hash(context)
            approval = services.approve_result(request_sha256=request_hash,
                result_sha256=result_hash, context=deepcopy(context),
                deadline_at=request["expires_at"])
            if (type(approval) is not ApprovedContext
                    or approval.request_sha256 != request_hash
                    or approval.result_sha256 != result_hash
                    or number(approval.expires_at) <= number(clock())
                    or type(approval.context) is not str or not approval.context
                    or len(json.dumps({'status': 'completed', 'context': approval.context},
                                      ensure_ascii=True).encode()) > 8192
                    or _hash(context) != result_hash or not validate(None, candidate)):
                raise HostError('result_approval_unavailable')
            services.audit_receipt({
                'schema': 'caller-activation/1', 'request_sha256': request_hash,
                'result_sha256': result_hash, 'state': 'execution_succeeded_pending_review',
                'cleanup_complete': True, 'durable_capacity_released': False,
            }, deadline_at=request['expires_at'])
            if not validate(None, candidate) or number(approval.expires_at) <= number(clock()):
                raise HostError('result_approval_unavailable')
            return approval.context

        composed = cloud_caller.build_context_tool_factory(
            session=services.session, policy=services.policy,
            mqtt_config=services.mqtt_config, approval_resolver=services.approval_resolver,
            receipt_reader=services.receipt_reader, create_owner=services.create_owner,
            resolve_request=lambda _agent: request, validate_request=validate,
            project_context=project, modules=selected_modules, clock=clock, monotonic=monotonic,
            approval_factory=lambda **kwargs: TypedReviewer(exchange=services.typed_reviewer, **kwargs))
        return composed(agent=agent)
    return factory


def configure_context_tool(config, *, host_services=None, legacy_factory=None):
    """Called only on explicit profile opt-in by GatewayRunner.

    The provider is installed operator Python code (module:function), with the
    same trust as the host process. YAML supplies no enrollment/approval data.
    It receives only the selected profile home, never the agent or model args.
    """
    if getattr(config, 'caller_host_enabled', False) is not True:
        return legacy_factory
    if legacy_factory is not None or getattr(config, 'multiplex_profiles', False):
        raise HostError('caller_host_configuration_conflict')
    from hermes_cli.config import get_hermes_home
    home = get_hermes_home()
    if host_services is None:
        provider = getattr(config, 'caller_host_provider', '')
        if type(provider) is not str or provider.count(':') != 1:
            raise HostError('caller_host_provider_required')
        module, name = provider.split(':')
        if not module or not all(part.isidentifier() for part in module.split('.')) or not name.isidentifier():
            raise HostError('caller_host_provider_required')
        try:
            host_services = getattr(importlib.import_module(module), name)(profile_home=home)
        except Exception:
            raise HostError('caller_host_provider_unavailable') from None
    # Same profile spelling used by admitted_request in gateway.run.
    from hermes_cli.profiles import get_active_profile_name
    return build_host_factory(host_services, profile=get_active_profile_name() or "default")
