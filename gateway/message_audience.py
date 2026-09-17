"""Pure audience and participation policy for messaging boundaries.

Callers supply authenticated adapter facts and a trusted, profile-scoped policy
map. Message text, display names, model output and arbitrary event metadata are
not authority. In particular, synthetic ``MessageEvent.internal`` and historical
thread participation confer neither audience trust nor consent.

Evaluate before model/context/media work. Carry the decision to every send/edit;
re-evaluate for a different destination. Output classes describe already-cleaned
content, not a sanitizer: consumers must classify after all prefixes/formatting.
There is no I/O, configuration lookup, provider import or model call here.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields
from enum import Enum


class Audience(str, Enum):
    EXTERNAL = "external"
    INTERNAL = "internal"
    PRIVATE_OPERATOR = "private_operator"


class Action(str, Enum):
    RESPOND = "respond"
    MUTE = "mute"
    DEFER = "defer"


class OutputClass(str, Enum):
    FINAL = "final"
    SAFE_ERROR = "safe_error"
    OPERATIONAL = "operational"
    PROGRESS = "progress"
    REASONING = "reasoning"
    RUNTIME_INTERNALS = "runtime_internals"
    SECRET = "secret"
    RAW_PATH = "raw_path"


@dataclass(frozen=True)
class ChannelIdentity:
    platform: str
    workspace_id: str
    channel_id: str


@dataclass(frozen=True)
class ChannelPolicy:
    audience: Audience = Audience.EXTERNAL
    desk_voice: bool = False
    operator_messages_are_requests: bool = False


@dataclass(frozen=True)
class ParticipationSignals:
    addressed_to_agent: bool = False
    addressed_to_other_human: bool = False
    explicit_command: bool = False  # Recognized, bot-directed control/command.
    reply_to_agent: bool = False  # Actual direct reply author, not thread root.
    provider_requested: bool = False  # A request to this agent, not to another human.
    operator_requested: bool = False  # Trusted request scoped to this destination.
    sender_is_operator: bool = False
    sender_is_bot: bool = False
    direct_message: bool = False  # Actual one-to-one inbound human DM, never MPIM.
    substantive_text: bool = False
    open_question: bool = False
    thread_participation: bool = False  # Context only; never permission.
    has_attachments: bool = False
    attachment_burst_pending: bool = False
    synthetic_internal: bool = False  # Synthetic delivery, not audience trust.


_EXTERNAL_OUTPUTS = frozenset({OutputClass.FINAL, OutputClass.SAFE_ERROR})
_INTERNAL_OUTPUTS = _EXTERNAL_OUTPUTS | {OutputClass.OPERATIONAL, OutputClass.PROGRESS}


@dataclass(frozen=True)
class ParticipationDecision:
    action: Action
    audience: Audience
    reason: str

    @property
    def allow_model(self) -> bool:
        return self.action is Action.RESPOND

    @property
    def allowed_output_classes(self) -> frozenset[OutputClass]:
        if self.action is not Action.RESPOND:
            return frozenset()
        if isinstance(self.audience, Audience) and self.audience in (Audience.INTERNAL, Audience.PRIVATE_OPERATOR):
            return _INTERNAL_OUTPUTS
        return _EXTERNAL_OUTPUTS


def _valid_identity(identity: ChannelIdentity) -> bool:
    return (isinstance(identity, ChannelIdentity)
            and all(type(value) is str and value and value.strip() == value
                    for value in (identity.platform, identity.workspace_id, identity.channel_id)))


def _policy_for(identity: ChannelIdentity, policies: Mapping | None) -> ChannelPolicy:
    # An incomplete identity must not match even an explicitly malformed key.
    if not _valid_identity(identity) or not isinstance(policies, Mapping):
        return ChannelPolicy()
    policy = policies.get(identity)
    if (not isinstance(policy, ChannelPolicy) or not isinstance(policy.audience, Audience)
            or type(policy.desk_voice) is not bool
            or type(policy.operator_messages_are_requests) is not bool):
        return ChannelPolicy()
    return policy


def decide_participation(
    identity: ChannelIdentity,
    signals: ParticipationSignals,
    policies: Mapping[ChannelIdentity, ChannelPolicy] | None = None,
) -> ParticipationDecision:
    """Return a deterministic decision; missing audience defaults external-safe."""
    policy = _policy_for(identity, policies)
    audience = policy.audience
    if (not isinstance(signals, ParticipationSignals)
            or any(type(getattr(signals, field.name)) is not bool for field in fields(ParticipationSignals))):
        return ParticipationDecision(Action.MUTE, audience, "invalid_signals")
    if signals.synthetic_internal and not _valid_identity(identity):
        return ParticipationDecision(Action.MUTE, audience, "synthetic_requires_exact_target")
    if signals.synthetic_internal and not signals.operator_requested:
        return ParticipationDecision(Action.MUTE, audience, "synthetic_requires_operator_request")
    if signals.sender_is_bot and not signals.operator_requested:
        return ParticipationDecision(Action.MUTE, audience, "bot_without_operator_request")

    explicit_agent = signals.addressed_to_agent or signals.explicit_command or signals.reply_to_agent
    if signals.addressed_to_other_human and not (explicit_agent or signals.operator_requested):
        return ParticipationDecision(Action.MUTE, audience, "addressed_to_other_human")

    if signals.operator_requested:
        reason = "operator_requested"
    elif explicit_agent:
        reason = "explicit_agent_request"
    elif signals.provider_requested:
        reason = "provider_requested"
    elif signals.direct_message and (signals.substantive_text or signals.has_attachments):
        reason = "one_to_one_request"
    elif policy.desk_voice and signals.open_question and not signals.sender_is_operator:
        reason = "desk_voice_open_question"
    elif (audience in (Audience.INTERNAL, Audience.PRIVATE_OPERATOR)
          and policy.operator_messages_are_requests and signals.sender_is_operator
          and signals.substantive_text):
        reason = "internal_operator_request"
    else:
        return ParticipationDecision(Action.MUTE, audience, "no_request")

    # Approval/stop controls must still reach the runtime's inline command path.
    if signals.attachment_burst_pending and not signals.explicit_command:
        return ParticipationDecision(Action.DEFER, audience, "attachment_burst_pending")
    return ParticipationDecision(Action.RESPOND, audience, reason)


def output_allowed(decision: ParticipationDecision, output_class: OutputClass) -> bool:
    """Class gate, not content inspection; raw reasoning/secrets/paths never pass."""
    return (isinstance(decision, ParticipationDecision)
            and isinstance(output_class, OutputClass)
            and output_class in decision.allowed_output_classes)
