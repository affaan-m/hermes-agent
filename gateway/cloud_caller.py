"""Compose the reviewed native caller with its real host adapters.

Default disabled. Importing this module has no SDK, provider, filesystem or
network effect. The host assembles one factory per process by calling
build_context_tool_factory(...) with every protected port and passes the
result as GatewayRunner(context_tool_factory=...). Omitting the call leaves
the gateway exactly as before; there is no profile or environment activation.

Reviewed bytes this composition binds (SHA-256):
  gateway/context_tool.py      c19576b0 (caller helper; manifest a6cf953e,
                               forward f90aecb9, inverse f457aadc, code review
                               e2b17e1f, security review 52e0d24d)
  gateway/context_dispatch.py  576c72bb (dispatcher candidate)
  gateway/host_adapters.py     63f0e3aa (host adapters packet)

Authority rules inherited from those packets: the model argument schema is
empty; enrollment, approval, destinations and receipts come only from the
operator-supplied ports below; denial or an unknown outcome never falls back
to durable capacity and never retries. Missing ports fail closed at build
time, so a partially wired host cannot construct a factory at all.
"""
from __future__ import annotations

import time
from types import SimpleNamespace

from gateway.context_tool import PreparedContextCall
from gateway.context_dispatch import DispatchPorts, NativeContextDispatcher
from gateway.host_adapters import BotoCalls, HostBundle, HostError, prepared_factory

_MQTT_KEYS = frozenset({"endpoint", "cert_path", "key_path", "ca_path"})


def _default_modules():
    """Resolve the reviewed dispatcher dependency classes from the installed
    canonical/exchange packages (ito-desk). Lazy: importing this module never
    requires them; only an actual composition build does."""
    from infra.aws.exchange.controller import ActivationRouter, ExchangeController
    from infra.aws.runner.context_launch import RunTaskGuard
    from infra.aws.runner.context_results import ContextResults

    return SimpleNamespace(
        DispatchPorts=DispatchPorts,
        RunTaskGuard=RunTaskGuard,
        ActivationRouter=ActivationRouter,
        ExchangeController=ExchangeController,
        ContextResults=ContextResults,
    )


def _require_callable(name, value):
    if not callable(value):
        raise HostError("port_missing:" + name)
    return value


def build_context_tool_factory(
    *,
    session,
    policy,
    mqtt_config,
    approval_resolver,
    receipt_reader,
    create_owner,
    resolve_request,
    validate_request,
    project_context,
    modules=None,
    clock=time.time,
    monotonic=time.monotonic,
):
    """Return the exact factory(agent=...) for GatewayRunner, or raise.

    Every port is protected operator code, never model-derived:
      session: an already authenticated boto3 session with nonblocking
        credentials (BotoCalls never constructs a default session).
      policy: a reviewed ContextLaunchPolicy built from verified deployed V2
        values (task definition ARN, immutable image digest, task role ID,
        subnet, security group). Never model- or worker-supplied.
      mqtt_config: exactly {endpoint, cert_path, key_path, ca_path} for the
        per-attempt X509 transport; client identity and topics are fixed in
        gateway.host_adapters.
      approval_resolver / receipt_reader: the independently authenticated
        external approval lookup and canonical receipt read ports. This
        composition never calls canonical.approve or canonical.accept.
      create_owner: opens the one existing canonical owner graph (Store,
        CanonicalAWS, actions, audience, authority, artifacts). Called at most
        once, during invoke, on the original physical foreground thread.
      resolve_request / validate_request / project_context: the protected
        original-request binding; see inventory_request_resolver for the
        reviewed intake-anchored implementation.
      modules: test-only override for the dispatcher dependency namespace;
        production leaves it None so the installed pinned classes are used.

    Transport, owner graph and provider clients are constructed only inside
    invoke, on the calling thread, so deadline and owner checks in the
    adapters apply to the thread that actually runs the attempt.
    """
    _require_callable("resolve_request", resolve_request)
    _require_callable("validate_request", validate_request)
    _require_callable("project_context", project_context)
    _require_callable("create_owner", create_owner)
    _require_callable("approval_resolver", approval_resolver)
    _require_callable("receipt_reader", receipt_reader)
    _require_callable("clock", clock)
    _require_callable("monotonic", monotonic)
    if session is None or policy is None:
        raise HostError("port_missing:session_or_policy")
    if type(mqtt_config) is not dict or set(mqtt_config) != _MQTT_KEYS:
        raise HostError("port_missing:mqtt_config")
    mods = modules if modules is not None else _default_modules()

    def make_bundle(request):
        provider_call = BotoCalls(session=session, clock=clock, monotonic=monotonic)
        return HostBundle(
            modules=mods,
            create_owner=create_owner,
            policy=policy,
            provider_call=provider_call,
            mqtt_config=mqtt_config,
            approval_resolver=approval_resolver,
            receipt_reader=receipt_reader,
            request_deadline=request["expires_at"],
            clock=clock,
            monotonic=monotonic,
        )

    return prepared_factory(
        prepared_class=PreparedContextCall,
        dispatcher_class=NativeContextDispatcher,
        resolve_request=resolve_request,
        make_bundle=make_bundle,
        validate_request=validate_request,
        project_context=project_context,
        clock=clock,
        monotonic=monotonic,
    )


def inventory_request_resolver(enrollment_lookup):
    """Bind the factory to the current authenticated original request.

    Returns the (resolve_request, validate_request) pair for
    build_context_tool_factory. The admitted foreground intake issued by
    gateway.inventory_context is the only request authority anchor: a turn
    without a live admitted request, a copied context, or a different thread
    resolves to HostError('request_unavailable') and the tool stays
    unpublished for that turn.

    enrollment_lookup(identity) is operator code. It receives the admitted
    request's identity tuple (profile, user, platform, workspace, channel,
    thread) and returns exactly
    {host_caller, controller_caller, run_kwargs, expires_at} for an enrolled
    nonpricing context task, or None when the conversation is not enrolled.
    run_kwargs must be the exact NativeContextDispatcher.run argument mapping
    (enrollment, scope, canonical task, immutable CLI-source manifest hash,
    bounded input); this resolver passes it through beyond the shape check in
    prepared_factory. The lookup must be deterministic for a live enrollment
    and must stop returning it once revoked: validate_request re-runs the
    same lookup against the currently admitted request and refuses when the
    enrollment changed or disappeared, so revocation between preparation and
    invocation holds. Handles are identity-compared, payloads
    equality-compared, and nothing is retained across turns.
    """
    _require_callable("enrollment_lookup", enrollment_lookup)

    def resolve_request(agent):
        from gateway.inventory_context import capture_inventory_request

        admitted = capture_inventory_request()
        if admitted is None:
            return None
        enrollment = enrollment_lookup(admitted.identity)
        if type(enrollment) is not dict:
            return None
        return {
            "host_caller": enrollment.get("host_caller"),
            "controller_caller": enrollment.get("controller_caller"),
            "run_kwargs": enrollment.get("run_kwargs"),
            "expires_at": enrollment.get("expires_at"),
        }

    def validate_request(agent, request):
        from gateway.inventory_context import capture_inventory_request

        current = capture_inventory_request()
        if current is None:
            return False
        enrollment = enrollment_lookup(current.identity)
        if type(enrollment) is not dict:
            return False
        return (
            enrollment.get("expires_at") == request["expires_at"]
            and enrollment.get("run_kwargs") == request["run_kwargs"]
            and enrollment.get("host_caller") is request["host_caller"]
            and enrollment.get("controller_caller") is request["controller_caller"]
        )

    return resolve_request, validate_request
