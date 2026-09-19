"""Closed native audience composition; no real Slack or provider activity.

Ported from the native-slack candidate (original pin 5e2921d8): test bodies
are byte-identical; only module/fixture resolution changed. The canonical
boundary case uses the hash-verified vendored queue package under
tests/fixtures/native_caller when the private capture tree is absent.
"""
import contextvars
import copy
import hashlib
import pathlib
import sys
import threading
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import test_slack_context_attestation as fixture

# Hard load: the audience module must exist in this repo; a failure here is a
# product gap, not a skip.
context_audience = fixture._load(
    "native_audience_candidate.context_audience",
    fixture.ROOT / "gateway" / "context_audience.py",
)


class AudienceTests(fixture.NativeFixture):
    def setUp(self):
        super().setUp()
        self.assertIsNotNone(context_audience, "native Slack audience helper is not implemented")
        self.manager = context_audience.SlackContextAudience(clock=lambda: self.now, monotonic_clock=lambda: self.mono)
        self.host = object()
        self.projection = dict(company_id="company-a", evaluation_time=self.now, limit=20)
        self.enrollment = dict(profile_id="profile-a",workspace_id="workspace-a",bot_user_id="bot-a",user_id="user-a",
            channel_id="channel-a",thread_id="100.1",message_id="100.2",company_id="company-a",
            projection=copy.deepcopy(self.projection),expires_at=self.now+120)

    def bind(self, epoch=1, **changes):
        self.manager.bind(authority_epoch=epoch, enrollment=dict(self.enrollment, **changes))

    def scope(self, host=None, request="1"*32, projection=None):
        return self.manager.request_scope(host or self.host, canonical_request_id=request,
            approved_projection=self.projection if projection is None else projection)

    def capture(self, host=None):
        return self.manager.capture_context_audience(host or self.host)

    def resolve(self, handle, host=None):
        return self.manager.resolve_context_audience(host or self.host,handle,self.now)

    def test_disabled_helper_does_not_create_authority(self):
        with self.foreground(), self.scope():
            self.assertIsNone(self.capture())

    def test_current_mentioned_request_has_exact_closed_descriptor(self):
        self.bind()
        with self.foreground(), self.scope():
            value = self.resolve(self.capture())
            self.assertEqual(set(value), {"kind","profile_id","canonical_request_id","source_request_id",
                "authority_epoch","expires_at","company_id","operation","projection","workspace_id",
                "bot_user_id","user_id","channel_id","thread_id","message_id"})
            self.assertEqual(value["kind"], "slack"); self.assertEqual(value["canonical_request_id"],"1"*32)
            self.assertEqual(value["message_id"],"100.2"); self.assertEqual(value["thread_id"],"100.1")
            self.assertEqual(value["projection"],self.projection)
            self.assertEqual(value["operation"],"nonpricing.context")

    def test_unmentioned_conversation_stays_silent_when_enrolled(self):
        self.bind()
        with self.foreground(raw=dict(self.raw,text="hello colleague")), self.scope():
            self.assertIsNone(self.capture())

    def test_wrong_exact_original_message_is_not_channel_grant(self):
        self.bind(message_id="999.1")
        with self.foreground(), self.scope():
            self.assertIsNone(self.capture())

    def test_wrong_workspace_bot_user_thread_profile_enrollment_denies(self):
        for field in ("workspace_id","bot_user_id","user_id","thread_id","profile_id"):
            with self.subTest(field=field):
                self.manager = context_audience.SlackContextAudience(clock=lambda:self.now,monotonic_clock=lambda:self.mono)
                self.bind(**{field:"wrong"})
                with self.foreground(), self.scope():
                    self.assertIsNone(self.capture())

    def test_projection_expansion_or_company_substitution_denies(self):
        self.bind()
        with self.foreground(), self.scope(projection=dict(self.projection,limit=19)):
            self.assertIsNone(self.capture())

    def test_reenrollment_invalidates_existing_scope_and_same_epoch_refused(self):
        self.bind()
        with self.foreground(), self.scope():
            handle=self.capture();self.assertIsNotNone(handle)
            with self.assertRaises(ValueError): self.bind()
            self.bind(epoch=2)
            self.assertIsNone(self.resolve(handle))

    def test_disable_is_terminal_for_original_handle(self):
        self.bind()
        with self.foreground(), self.scope():
            handle=self.capture();self.manager.disable(authority_epoch=2)
            self.assertIsNone(self.resolve(handle));self.assertIsNone(self.capture())

    def test_same_principal_looking_other_host_object_cannot_resolve(self):
        self.bind()
        with self.foreground(), self.scope():
            handle=self.capture();self.assertIsNone(self.resolve(handle,object()))
            self.assertIsNotNone(self.resolve(handle))

    def test_forged_handle_does_not_resolve(self):
        self.bind()
        with self.foreground(), self.scope():
            handle=self.capture();self.assertIsNone(self.resolve(object()))
            self.assertIsNone(self.resolve(type(handle)()))

    def test_copied_context_other_thread_is_not_original_host(self):
        self.bind()
        with self.foreground(), self.scope():
            handle=self.capture();copied=contextvars.copy_context();results=[]
            thread=threading.Thread(target=lambda:results.append(copied.run(self.resolve,handle)))
            thread.start();thread.join(1);self.assertFalse(thread.is_alive())
            self.assertEqual(results,[None]);self.assertIsNotNone(self.resolve(handle))

    def test_expired_or_ended_request_cannot_be_replayed(self):
        self.bind()
        with self.foreground(), self.scope():
            handle=self.capture();copied=contextvars.copy_context();self.mono+=121
            self.assertIsNone(self.resolve(handle))
        self.now-=100;self.mono+=1
        self.assertIsNone(copied.run(self.resolve,handle))

    def test_second_scope_cannot_relabel_same_physical_request(self):
        self.bind()
        with self.foreground():
            with self.scope(): self.assertIsNotNone(self.capture())
            with self.scope(request="2"*32): self.assertIsNone(self.capture())

    def test_midstream_selected_client_change_holds_next_resolution(self):
        self.bind()
        with self.foreground(), self.scope():
            handle=self.capture();self.assertIsNotNone(self.resolve(handle))
            self.adapter._team_clients["workspace-a"]=object()
            self.assertIsNone(self.resolve(handle))

    def test_flat_dm_is_usable_with_explicit_none_output_thread(self):
        self.adapter.config.extra["reply_in_thread"] = False
        raw = dict(self.raw, channel_type="im", text="inventory please"); raw.pop("thread_ts")
        self.bind(thread_id=None)
        with self.foreground(raw=raw), self.scope():
            self.assertIsNone(self.resolve(self.capture())["thread_id"])

    def test_nested_scope_cannot_create_second_authority(self):
        self.bind()
        with self.foreground(), self.scope():
            handle = self.capture()
            with self.scope(request="2"*32):
                self.assertIsNone(self.capture()); self.assertIsNone(self.resolve(handle))
            self.assertEqual(self.resolve(handle)["canonical_request_id"],"1"*32)

    def test_clock_rollback_cannot_restore_expired_scope(self):
        self.bind()
        with self.foreground(), self.scope():
            handle = self.capture(); self.now += 121
            self.assertIsNone(self.resolve(handle)); self.now -= 121
            self.assertIsNone(self.resolve(handle)); self.assertIsNone(self.capture())

    def test_terminal_scope_releases_registered_host_and_attestation(self):
        self.bind()
        with self.foreground():
            with self.scope():
                handle = self.capture(); record = self.manager._handles[handle]
                self.assertIs(record.host,self.host)
            self.assertIsNone(record.host); self.assertIsNone(record.attestation)
            self.assertEqual(len(self.manager._handles),0); self.assertEqual(len(self.manager._active),0)
            self.assertEqual(len(self.manager._seen),1)

    def test_rebind_preserves_spent_original_request(self):
        self.bind()
        with self.foreground():
            with self.scope(): self.assertIsNotNone(self.capture())
            self.bind(epoch=2)
            with self.scope(request="2"*32): self.assertIsNone(self.capture())

    def test_actual_frozen_canonical_helper_accepts_only_current_native_association(self):
        repository = fixture.ROOT.parent / "canonical-main-integration" / "repo"
        if not (repository / "queue" / "ito_queue" / "aws_artifacts.py").exists():
            repository = fixture.ROOT / "tests" / "fixtures" / "native_caller"
        source = repository / "queue" / "ito_queue" / "aws_artifacts.py"
        self.assertEqual(hashlib.sha256(source.read_bytes()).hexdigest(),
                         "623983aefd36c911948a0ca806cf1d1998c04154aa9559f489e288ecb21d8f0b")
        sys.path.insert(0,str(repository/"queue"));self.addCleanup(sys.path.remove,str(repository/"queue"))
        from ito_queue.store import Store
        from ito_queue.aws_receipts import CanonicalAWS
        from ito_queue.aws_artifacts import CanonicalContextActions, ArtifactError, TRIAL_LIMITS
        self.bind()
        controller = object()
        identities = {controller:dict(principal="controller",kind="controller",invocation_id="c"*32,executor=None,expires_at=self.now+120),
            self.host:dict(principal="host",kind="host",invocation_id="d"*32,executor=None,expires_at=self.now+120)}
        with tempfile.TemporaryDirectory(prefix="native-canonical-") as directory:
            store = Store(pathlib.Path(directory))
            try:
                canonical = CanonicalAWS(store,reviewers={"reviewer"},dispatchers={"host"},max_inflight=1)
                canonical.initialize_for_candidate()
                actions = CanonicalContextActions(canonical,authenticate=identities.get,clock=lambda:self.now,
                    monotonic_clock=lambda:self.mono,resolve_audience=self.manager.resolve_context_audience,
                    scope_controllers={"controller"},host_principals={"host"},limits=TRIAL_LIMITS,enabled=True)
                actions.initialize_for_candidate()
                self.assertEqual(actions.purge_expired_contexts(controller)["status"],"checkpoint_complete")
                spec=dict(scope_id="native-scope",request_id="1"*32,sequence=1,grant_epoch=1,operation="nonpricing.context",
                    projection=self.projection,not_after=self.now+120,revoked=False)
                with self.foreground(), self.scope():
                    handle=self.capture()
                    accepted=actions.set_audience_scope(controller,spec,host_caller=self.host,audience_handle=handle)
                    self.assertEqual(accepted["scope_version"],1)
                    other=object();identities[other]=dict(identities[self.host])
                    with self.assertRaises(ArtifactError):
                        actions.set_audience_scope(controller,spec,host_caller=other,audience_handle=handle,expected_scope_version=1)
                with self.assertRaises(ArtifactError):
                    actions.set_audience_scope(controller,spec,host_caller=self.host,audience_handle=handle,expected_scope_version=1)
            finally:
                store.close()

    def test_fresh_genuine_foreground_cannot_move_manager_to_other_owner_thread(self):
        self.bind(); results=[]; errors=[]
        def other_owner():
            try:
                with self.foreground(), self.scope():
                    results.append(self.capture())
            except Exception as exc:
                errors.append(type(exc).__name__)
        thread=threading.Thread(target=other_owner)
        thread.start();thread.join(1)
        self.assertFalse(thread.is_alive());self.assertEqual(errors,[])
        self.assertEqual(results,[None])
        with self.foreground(), self.scope():
            self.assertIsNotNone(self.capture())

    def test_observed_enrollment_expiry_before_first_scope_is_terminal(self):
        self.bind();self.now+=121;self.mono+=1
        with self.foreground(), self.scope():
            self.assertIsNone(self.capture())
        self.now-=121;self.mono+=1
        with self.foreground(), self.scope():
            self.assertIsNone(self.capture())


    def test_huge_projection_number_bind_has_fixed_value_error(self):
        bad = dict(self.projection, evaluation_time=10**1000)
        with self.assertRaises(ValueError):
            self.bind(projection=bad)

    def test_huge_projection_number_scope_denies_without_escaping(self):
        self.bind()
        with self.foreground(), self.scope(projection=dict(self.projection, evaluation_time=10**1000)):
            self.assertIsNone(self.capture())

    def test_huge_preentry_clock_clears_enrollment_terminally(self):
        self.bind(); original = self.now; self.now = 10**1000
        with self.foreground(), self.scope():
            self.assertIsNone(self.capture())
        self.now = original
        with self.foreground(), self.scope():
            self.assertIsNone(self.capture())

    def test_huge_clock_retires_current_handle_without_escaping(self):
        self.bind()
        with self.foreground(), self.scope():
            handle = self.capture(); original = self.now; self.now = 10**1000
            self.assertIsNone(self.resolve(handle))
            self.now = original
            self.assertIsNone(self.resolve(handle))


if __name__ == "__main__":
    unittest.main()
