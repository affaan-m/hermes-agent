"""Pure synthetic audience/participation contract, runnable without gateway imports."""
from __future__ import annotations

import dataclasses
import importlib.util
from pathlib import Path
import sys
import unittest


class AudiencePolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # gateway.__init__ imports config/runtime; load only the owned pure module.
        path = Path(__file__).resolve().parents[2] / "gateway" / "message_audience.py"
        name = "_audience_policy_under_test"
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        try:
            spec.loader.exec_module(module)
        except BaseException:
            sys.modules.pop(name, None)
            raise
        cls.p = module

    @classmethod
    def tearDownClass(cls):
        sys.modules.pop("_audience_policy_under_test", None)

    def setUp(self):
        p = self.p
        self.channel = p.ChannelIdentity("synthetic-chat", "workspace-a", "channel-a")
        self.other_workspace = p.ChannelIdentity("synthetic-chat", "workspace-b", "channel-a")
        self.internal = {self.channel: p.ChannelPolicy(audience=p.Audience.INTERNAL)}

    def decide(self, policies=None, identity=None, **signals):
        return self.p.decide_participation(identity or self.channel,
                                          self.p.ParticipationSignals(**signals), policies)

    def test_unknown_audience_explicit_request_is_external_safe(self):
        d = self.decide(addressed_to_agent=True)
        self.assertEqual(d.audience, self.p.Audience.EXTERNAL)
        self.assertTrue(d.allow_model)
        self.assertTrue(self.p.output_allowed(d, self.p.OutputClass.FINAL))
        self.assertTrue(self.p.output_allowed(d, self.p.OutputClass.SAFE_ERROR))
        for kind in (self.p.OutputClass.OPERATIONAL, self.p.OutputClass.PROGRESS,
                     self.p.OutputClass.REASONING, self.p.OutputClass.RUNTIME_INTERNALS):
            with self.subTest(kind=kind):
                self.assertFalse(self.p.output_allowed(d, kind))

    def test_internal_policy_requires_exact_platform_workspace_channel(self):
        for identity in (self.other_workspace,
                         self.p.ChannelIdentity("other-platform", "workspace-a", "channel-a"),
                         self.p.ChannelIdentity("synthetic-chat", "workspace-a", "channel-b"),
                         self.p.ChannelIdentity("synthetic-chat", "", "channel-a")):
            with self.subTest(identity=identity):
                d = self.decide(self.internal, identity, addressed_to_agent=True)
                self.assertEqual(d.audience, self.p.Audience.EXTERNAL)
                self.assertFalse(self.p.output_allowed(d, self.p.OutputClass.PROGRESS))

    def test_even_matching_incomplete_identity_cannot_grant_internal_trust(self):
        for identity in (None, self.p.ChannelIdentity("synthetic-chat", "", "channel-a"),
                         self.p.ChannelIdentity("synthetic-chat", None, "channel-a"),
                         self.p.ChannelIdentity("synthetic-chat", "workspace-a", ""),
                         self.p.ChannelIdentity(" synthetic-chat", "workspace-a", "channel-a")):
            with self.subTest(identity=identity):
                policies = {identity: self.p.ChannelPolicy(audience=self.p.Audience.INTERNAL)}
                d = self.p.decide_participation(identity,
                    self.p.ParticipationSignals(addressed_to_agent=True), policies)
                self.assertEqual(d.audience, self.p.Audience.EXTERNAL)
                self.assertFalse(self.p.output_allowed(d, self.p.OutputClass.PROGRESS))

    def test_synthetic_deliveries_require_scoped_operator_request(self):
        policies = {self.channel: self.p.ChannelPolicy(desk_voice=True,
            audience=self.p.Audience.INTERNAL, operator_messages_are_requests=True)}
        for signals in ({"addressed_to_agent": True}, {"explicit_command": True},
                        {"reply_to_agent": True}, {"provider_requested": True},
                        {"open_question": True},
                        {"sender_is_operator": True, "substantive_text": True}):
            with self.subTest(signals=signals):
                self.assertFalse(self.decide(policies, synthetic_internal=True, **signals).allow_model)
        d = self.decide(policies, synthetic_internal=True, operator_requested=True)
        self.assertTrue(d.allow_model)

    def test_synthetic_operator_request_requires_complete_target_identity(self):
        signals = self.p.ParticipationSignals(synthetic_internal=True, operator_requested=True)
        for identity in (None, self.p.ChannelIdentity("synthetic-chat", "", "channel-a"),
                         self.p.ChannelIdentity("synthetic-chat", "workspace-a", "")):
            with self.subTest(identity=identity):
                d = self.p.decide_participation(identity, signals)
                self.assertEqual(d.action, self.p.Action.MUTE)
                self.assertFalse(d.allow_model)
        d = self.p.decide_participation(self.channel, signals)
        self.assertTrue(d.allow_model)
        self.assertEqual(d.audience, self.p.Audience.EXTERNAL)

    def test_synthetic_internal_and_thread_history_do_not_grant_consent(self):
        for policies in (None, self.internal):
            for signals in ({"synthetic_internal": True}, {"thread_participation": True},
                            {"synthetic_internal": True, "thread_participation": True},
                            {"has_attachments": True, "thread_participation": True}):
                with self.subTest(policies=policies, signals=signals):
                    d = self.decide(policies, **signals)
                    self.assertEqual(d.action, self.p.Action.MUTE)
                    self.assertFalse(d.allow_model)
                    self.assertEqual(d.allowed_output_classes, frozenset())

    def test_other_human_question_stays_muted_in_participating_thread(self):
        for policies in (None, self.internal,
                         {self.channel: self.p.ChannelPolicy(desk_voice=True)}):
            d = self.decide(policies, addressed_to_other_human=True, open_question=True,
                            thread_participation=True, provider_requested=True,
                            substantive_text=True, has_attachments=True)
            self.assertEqual(d.action, self.p.Action.MUTE)
            self.assertFalse(d.allow_model)

    def test_explicit_agent_and_operator_positive_controls(self):
        for signal in ("addressed_to_agent", "explicit_command", "reply_to_agent",
                       "provider_requested", "operator_requested"):
            with self.subTest(signal=signal):
                d = self.decide(**{signal: True})
                self.assertEqual(d.action, self.p.Action.RESPOND)
                self.assertTrue(d.allow_model)

    def test_addressing_agent_and_human_together_preserves_agent_request(self):
        for signal in ("addressed_to_agent", "explicit_command", "reply_to_agent", "operator_requested"):
            with self.subTest(signal=signal):
                d = self.decide(addressed_to_other_human=True, **{signal: True})
                self.assertTrue(d.allow_model)

    def test_bot_mentions_cannot_start_a_loop_without_operator_request(self):
        for signal in ("addressed_to_agent", "explicit_command", "reply_to_agent", "provider_requested"):
            with self.subTest(signal=signal):
                d = self.decide(sender_is_bot=True, **{signal: True})
                self.assertFalse(d.allow_model)
        self.assertTrue(self.decide(sender_is_bot=True, operator_requested=True).allow_model)

    def test_pending_attachment_burst_defers_only_an_authorized_response(self):
        d = self.decide(addressed_to_agent=True, has_attachments=True,
                        attachment_burst_pending=True)
        self.assertEqual(d.action, self.p.Action.DEFER)
        self.assertFalse(d.allow_model)
        self.assertEqual(d.allowed_output_classes, frozenset())
        done = self.decide(addressed_to_agent=True, has_attachments=True)
        self.assertTrue(done.allow_model)
        alone = self.decide(has_attachments=True, attachment_burst_pending=True)
        self.assertEqual(alone.action, self.p.Action.MUTE)

    def test_control_command_is_not_deferred_by_attachment_burst(self):
        d = self.decide(explicit_command=True, attachment_burst_pending=True)
        self.assertEqual(d.action, self.p.Action.RESPOND)
        self.assertTrue(d.allow_model)

    def test_one_to_one_dm_participation_does_not_make_audience_internal(self):
        for content in ({"substantive_text": True}, {"has_attachments": True}):
            with self.subTest(content=content):
                d = self.decide(direct_message=True, **content)
                self.assertTrue(d.allow_model)
                self.assertEqual(d.audience, self.p.Audience.EXTERNAL)
        self.assertFalse(self.decide(direct_message=True).allow_model)
        self.assertFalse(self.decide(direct_message=True, substantive_text=True,
                                     synthetic_internal=True).allow_model)

    def test_shared_group_attachments_are_not_dm_requests(self):
        self.assertFalse(self.decide(has_attachments=True, substantive_text=True,
                                     direct_message=False).allow_model)

    def test_desk_voice_allows_open_provider_questions_not_statements_or_principals(self):
        policies = {self.channel: self.p.ChannelPolicy(desk_voice=True)}
        self.assertTrue(self.decide(policies, open_question=True).allow_model)
        self.assertFalse(self.decide(policies, substantive_text=True).allow_model)
        self.assertFalse(self.decide(policies, open_question=True, sender_is_operator=True).allow_model)
        self.assertFalse(self.decide(None, open_question=True).allow_model)

    def test_internal_operator_auto_policy_must_be_explicit_and_cannot_cross_scope(self):
        policy = self.p.ChannelPolicy(audience=self.p.Audience.INTERNAL,
                                      operator_messages_are_requests=True)
        policies = {self.channel: policy}
        self.assertTrue(self.decide(policies, sender_is_operator=True, substantive_text=True).allow_model)
        for kwargs in ({"sender_is_operator": True}, {"substantive_text": True},
                       {"sender_is_operator": True, "substantive_text": True,
                        "addressed_to_other_human": True}):
            self.assertFalse(self.decide(policies, **kwargs).allow_model)
        self.assertFalse(self.decide(policies, self.other_workspace, sender_is_operator=True,
                                     substantive_text=True).allow_model)
        external = {self.channel: dataclasses.replace(policy, audience=self.p.Audience.EXTERNAL)}
        self.assertFalse(self.decide(external, sender_is_operator=True, substantive_text=True).allow_model)

    def test_internal_and_private_operator_allow_concise_operations(self):
        for audience in (self.p.Audience.INTERNAL, self.p.Audience.PRIVATE_OPERATOR):
            policies = {self.channel: self.p.ChannelPolicy(audience=audience)}
            d = self.decide(policies, operator_requested=True)
            self.assertEqual(d.audience, audience)
            for kind in (self.p.OutputClass.FINAL, self.p.OutputClass.SAFE_ERROR,
                         self.p.OutputClass.OPERATIONAL, self.p.OutputClass.PROGRESS):
                with self.subTest(audience=audience, kind=kind):
                    self.assertTrue(self.p.output_allowed(d, kind))

    def test_secrets_raw_paths_reasoning_and_raw_runtime_never_allowed(self):
        for audience in self.p.Audience:
            d = self.decide({self.channel: self.p.ChannelPolicy(audience=audience)},
                            operator_requested=True)
            for kind in (self.p.OutputClass.SECRET, self.p.OutputClass.RAW_PATH,
                         self.p.OutputClass.REASONING, self.p.OutputClass.RUNTIME_INTERNALS):
                with self.subTest(audience=audience, kind=kind):
                    self.assertFalse(self.p.output_allowed(d, kind))

    def test_mute_and_defer_block_every_output_class(self):
        for d in (self.decide(), self.decide(addressed_to_agent=True, attachment_burst_pending=True)):
            for kind in self.p.OutputClass:
                with self.subTest(action=d.action, kind=kind):
                    self.assertFalse(self.p.output_allowed(d, kind))
        self.assertFalse(self.p.output_allowed(self.decide(addressed_to_agent=True), "final"))

    def test_invalid_decision_audience_cannot_promote_outputs(self):
        d = self.p.ParticipationDecision(self.p.Action.RESPOND, "internal", "untyped")
        self.assertFalse(self.p.output_allowed(d, self.p.OutputClass.PROGRESS))
        self.assertFalse(self.p.output_allowed(None, self.p.OutputClass.FINAL))

    def test_signal_booleans_are_never_coerced(self):
        for value in ("false", 1, [], None):
            for field in dataclasses.fields(self.p.ParticipationSignals):
                with self.subTest(field=field.name, value=value):
                    signals = {"addressed_to_agent": True, field.name: value}
                    self.assertFalse(self.decide(**signals).allow_model)

    def test_invalid_policy_values_cannot_elevate_or_trigger(self):
        for policy in ("internal", {"audience": "internal"},
                       self.p.ChannelPolicy(audience="internal"),
                       self.p.ChannelPolicy(audience=self.p.Audience.INTERNAL, desk_voice="true")):
            with self.subTest(policy=policy):
                d = self.decide({self.channel: policy}, addressed_to_agent=True)
                self.assertEqual(d.audience, self.p.Audience.EXTERNAL)
                self.assertFalse(self.p.output_allowed(d, self.p.OutputClass.PROGRESS))
                self.assertFalse(self.decide({self.channel: policy}, open_question=True).allow_model)

    def test_inputs_immutable_and_new_invocation_cannot_reuse_internal_trust(self):
        policy = self.p.ChannelPolicy(audience=self.p.Audience.INTERNAL)
        policies = {self.channel: policy}
        signals = self.p.ParticipationSignals(addressed_to_agent=True)
        d = self.p.decide_participation(self.channel, signals, policies)
        self.assertEqual(policies, {self.channel: policy})
        with self.assertRaises(dataclasses.FrozenInstanceError):
            signals.addressed_to_agent = False
        with self.assertRaises(dataclasses.FrozenInstanceError):
            d.action = self.p.Action.RESPOND
        self.assertEqual(self.decide(addressed_to_agent=True).audience, self.p.Audience.EXTERNAL)


if __name__ == "__main__":
    unittest.main(verbosity=2)
