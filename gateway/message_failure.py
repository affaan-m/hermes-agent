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
    return is_policy_denial(result) or kind == "private_delivery_failed"


def terminal_failure_result(result):
    if is_policy_denial(result):
        return policy_denial()
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


def check_delivery(config, platform, chat_id, *, metadata=None, adapter=None, output_class=None):
    """Authorize a dispatch from trusted config, never model/event flags.

    Returns bound routing metadata or raises a terminal denial. Call before
    topic creation, persistence, attachments and alternate transport attempts.
    """
    from types import SimpleNamespace
    from gateway.message_audience import OutputClass
    routed = dict(metadata or {})
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
            raise DeliveryPolicyDenied("delivery_not_authorized")
        if str(getattr(platform, "value", platform)) == "slack":
            requested = routed.get("slack_team_id")
            if requested and requested != workspace:
                raise DeliveryPolicyDenied("delivery_not_authorized")
            if workspace:
                routed["slack_team_id"] = workspace
    if (not isinstance(kind, OutputClass)
            or not authorized_delivery(config, platform, workspace, chat_id)
            or not destination_output_allowed(target, chat_id, kind, routed, workspace_id=workspace)):
        raise DeliveryPolicyDenied("delivery_not_authorized")
    routed["_hermes_output_class"] = kind
    return routed


def authorized_delivery(config, platform, workspace_id, chat_id) -> bool:
    """Require an explicit trusted, exact-destination scheduled delivery grant."""
    from gateway.message_audience import ParticipationSignals, decide_participation

    identity, policies = channel_policy_inputs(config, platform, workspace_id, chat_id)
    extra = getattr(config, "extra", {}) or {}
    entries = extra.get("authorized_deliveries", []) if isinstance(extra, Mapping) else []
    granted = isinstance(entries, list) and any(
        isinstance(entry, Mapping) and entry.get("operator_requested") is True
        and (entry.get("platform"), entry.get("workspace_id"), entry.get("channel_id"))
        == (identity.platform, identity.workspace_id, identity.channel_id)
        for entry in entries
    )
    return decide_participation(identity, ParticipationSignals(
        synthetic_internal=True, operator_requested=bool(granted),
    ), policies).allow_model
