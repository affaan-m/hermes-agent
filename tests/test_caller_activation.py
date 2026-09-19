"""Real intake/registry composition with a synthetic dispatcher, no provider calls."""
import contextlib
from dataclasses import FrozenInstanceError, replace
import hashlib
import json
import threading
import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from gateway import caller_activation as activation, cloud_caller, inventory_context
from gateway.context_dispatch import DispatchOutcome, DispatchPorts
from gateway.context_tool import TOOL_NAME, bind_context_tool
from gateway.host_adapters import ArtifactCleanup, HostBundle, HostError
from tools.registry import ToolRegistry


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    ensure_ascii=True, allow_nan=False).encode()).hexdigest()


class ActivationTests(unittest.TestCase):
    def setUp(self):
        inventory_context.clear_inherited()
        self.addCleanup(inventory_context.clear_inherited)
        self.client = object()
        self.adapter = SimpleNamespace(config=SimpleNamespace(extra={}),
            _team_clients={'workspace-a': self.client},
            _team_bot_user_ids={'workspace-a': 'bot-a'},
            _channel_team={'channel-a': 'workspace-a'})
        self.raw = dict(type='message', user='user-a', channel='channel-a', ts='100.2',
                        thread_ts='100.1', text='<@bot-a> inventory please', channel_type='channel')
        self.agent = SimpleNamespace(api_mode='chat_completions', tools=[], valid_tool_names=set())
        self.registry = ToolRegistry()
        self.lock = threading.RLock()
        self.enrollment = dict(host_caller=object(), controller_caller=object(),
                               run_kwargs={'task_id': 'job-a'}, expires_at=1020.0)
        self.enrolled = True
        self.lookups, self.approvals, self.receipts, self.dispatches = [], [], [], []
        self.context = {'inventory': ['synthetic capacity'], 'private': 'not public'}
        self.outcome = DispatchOutcome('completed', 'clean_context_pending_review',
            context_result=self.context, closure_receipt={
                'cleanup_complete': True, 'durable_capacity_released': False,
                'canonical_state': 'execution_succeeded_pending_review'})
        self.services = activation.HostServices(session=object(), policy=object(),
            mqtt_config=dict(endpoint='synthetic.invalid', cert_path='/synthetic/cert',
                             key_path='/synthetic/key', ca_path='/synthetic/ca'),
            enrollment_lookup=self.lookup, approval_resolver=lambda **kwargs: None,
            receipt_reader=lambda *args, **kwargs: None,
            create_owner=lambda: self.fail('synthetic dispatcher must not open owner'),
            approve_result=self.approve, audit_receipt=self.audit,
            typed_reviewer=lambda **kwargs: None)
        self.modules = SimpleNamespace(DispatchPorts=DispatchPorts, RunTaskGuard=object,
            ActivationRouter=object, ExchangeController=object, ContextResults=object)
        testcase = self

        class SyntheticDispatcher:
            def __init__(self, **kwargs):
                testcase.dispatches.append(kwargs)
            def run(self, **kwargs):
                testcase.assertEqual(kwargs, testcase.enrollment['run_kwargs'])
                return testcase.outcome

        patcher = patch.object(cloud_caller, 'NativeContextDispatcher', SyntheticDispatcher)
        patcher.start()
        self.addCleanup(patcher.stop)

    def lookup(self, attested):
        self.lookups.append(attested)
        return dict(self.enrollment) if self.enrolled else None

    def audit(self, receipt, *, deadline_at):
        self.assertEqual(deadline_at, self.enrollment['expires_at'])
        self.receipts.append(receipt)

    def approve(self, **kwargs):
        self.assertEqual(kwargs['deadline_at'], self.enrollment['expires_at'])
        self.approvals.append(kwargs)
        return activation.ApprovedContext(kwargs['request_sha256'], kwargs['result_sha256'],
                                          'approved inventory context', 1010.0)

    def factory(self, **kwargs):
        return activation.build_host_factory(kwargs.pop('services', self.services),
            profile=kwargs.pop('profile', 'profile-a'), clock=lambda: 1000.0,
            monotonic=lambda: 10.0, modules=self.modules, **kwargs)

    @contextlib.contextmanager
    def foreground(self, raw=None):
        raw = self.raw if raw is None else raw
        receipt = inventory_context.issue_intake(self.adapter, raw, workspace='workspace-a',
                                                 client=self.client, bot_user_id='bot-a')
        event = SimpleNamespace(raw_message=raw, message_id=raw['ts'],
            metadata={'_inventory_intake': receipt}, source=SimpleNamespace(
                platform=SimpleNamespace(value='slack'), user_id=raw['user'],
                chat_id=raw['channel'], thread_id=raw.get('thread_ts') or raw['ts'], scope_id='workspace-a'))
        with inventory_context.admitted_request(event, self.adapter, 'profile-a'), inventory_context.foreground_worker():
            yield event

    @contextlib.contextmanager
    def bound(self, factory=None):
        with bind_context_tool(self.agent, factory=factory or self.factory(),
                               registry=self.registry, snapshot_lock=self.lock) as ready:
            yield ready

    def call(self, args=None):
        return json.loads(self.registry.get_entry(TOOL_NAME).handler({} if args is None else args))

    def test_synthetic_call_audits_exact_request_and_result_then_retires_tool(self):
        with self.foreground(), self.bound() as ready:
            self.assertTrue(ready)
            self.assertIn(TOOL_NAME, self.agent.valid_tool_names)
            self.assertEqual(self.dispatches, [])
            self.assertEqual(self.call(), {'status': 'completed', 'context': 'approved inventory context'})
            self.assertEqual(self.call(), {'status': 'held'})
        self.assertNotIn(TOOL_NAME, self.agent.valid_tool_names)
        self.assertEqual(self.agent.tools, [])
        self.assertEqual(len(self.dispatches), 1)
        ports = self.dispatches[0]['ports']
        self.assertIsInstance(ports, DispatchPorts)
        self.assertIsInstance(ports.cleanup_resources, ArtifactCleanup)
        self.assertIsInstance(ports.observe_executor.__self__, HostBundle)
        self.assertIs(ports.observe_executor.__self__, ports.corroborate_terminal.__self__)
        attested = self.lookups[0]
        self.assertEqual(set(attested), {'identity', 'delivery_identity', 'message_id', 'bot_user_id'})
        self.assertEqual(attested['message_id'], '100.2')
        self.assertEqual(attested['delivery_identity'][-1], '100.1')
        self.assertEqual(self.receipts, [{
            'schema': 'caller-activation/1', 'request_sha256': digest(attested),
            'result_sha256': digest(self.context), 'state': 'execution_succeeded_pending_review',
            'cleanup_complete': True, 'durable_capacity_released': False}])
        self.assertNotIn('private', repr(self.receipts))

    def test_result_requires_exact_typed_request_and_result_approval(self):
        for invalid in ('untyped', 'request', 'result', 'expired', 'empty'):
            with self.subTest(invalid=invalid):
                def deny(**kwargs):
                    approval = self.approve(**kwargs)
                    if invalid == 'untyped':
                        return vars(approval)
                    return replace(approval, **{
                        'request': {'request_sha256': '0' * 64},
                        'result': {'result_sha256': '0' * 64},
                        'expired': {'expires_at': 1000.0},
                        'empty': {'context': ''}}[invalid])
                services = replace(self.services, approve_result=deny)
                with self.foreground(), self.bound(self.factory(services=services)) as ready:
                    self.assertTrue(ready)
                    self.assertEqual(self.call(), {'status': 'held'})
                self.assertEqual(self.receipts, [])
                self.assertNotIn(TOOL_NAME, self.agent.valid_tool_names)

    def test_revocation_after_prepare_prevents_dispatch_and_audit(self):
        with self.foreground(), self.bound() as ready:
            self.assertTrue(ready)
            self.enrolled = False
            self.assertEqual(self.call(), {'status': 'held'})
        self.assertEqual(self.dispatches, [])
        self.assertEqual(self.receipts, [])

    def test_cross_message_same_thread_cannot_reuse_prepared_authority(self):
        with self.foreground():
            prepared = self.factory()(agent=self.agent)
        with self.foreground(dict(self.raw, ts='100.3')):
            self.assertFalse(prepared.validate())
            with self.assertRaises(HostError):
                prepared.invoke()
        self.assertEqual(self.dispatches, [])
        self.assertEqual(self.receipts, [])

    def test_agent_attributes_cannot_supply_request_authority(self):
        self.agent.identity = ('profile-a', 'user-a', 'slack', 'workspace-a', 'channel-a', '100.1')
        self.agent.context_tool_factory = self.factory()
        self.agent.enrollment = self.enrollment
        self.agent.approved = True
        with self.bound() as ready:
            self.assertFalse(ready)
            self.assertEqual(self.call(), {'status': 'held'})
        self.assertEqual(self.lookups, [])
        self.assertEqual(self.dispatches, [])

    def test_model_arguments_never_become_authority(self):
        with self.foreground(), self.bound() as ready:
            self.assertTrue(ready)
            self.assertEqual(self.call({'approved': True}), {'status': 'held'})
            self.assertEqual(self.dispatches, [])
        self.assertEqual(self.receipts, [])

    def test_wrong_profile_remains_unpublished(self):
        with self.foreground(), self.bound(self.factory(profile='different-profile')) as ready:
            self.assertFalse(ready)
        self.assertEqual(self.lookups, [])
        self.assertEqual(self.dispatches, [])

    def test_unclean_outcome_never_reaches_result_approval_or_audit(self):
        self.outcome = replace(self.outcome, closure_receipt={
            'cleanup_complete': False, 'durable_capacity_released': False,
            'canonical_state': 'execution_succeeded_pending_review'})
        with self.foreground(), self.bound() as ready:
            self.assertTrue(ready)
            self.assertEqual(self.call(), {'status': 'held'})
        self.assertEqual(self.approvals, [])
        self.assertEqual(self.receipts, [])

    def test_audit_failure_holds_and_retires_capability(self):
        def unavailable(_receipt, *, deadline_at):
            raise RuntimeError('private sink details')
        with self.foreground(), self.bound(self.factory(services=replace(
                self.services, audit_receipt=unavailable))) as ready:
            self.assertTrue(ready)
            self.assertEqual(self.call(), {'status': 'held'})
        self.assertNotIn(TOOL_NAME, self.agent.valid_tool_names)

    def test_flagged_on_gateway_factory_executes_synthetic_audited_call(self):
        from gateway.config import GatewayConfig
        from gateway.run import GatewayRunner
        original_build = activation.build_host_factory
        def deterministic_build(services, *, profile):
            return original_build(services, profile=profile, clock=lambda: 1000.0,
                                  monotonic=lambda: 10.0, modules=self.modules)
        with tempfile.TemporaryDirectory() as directory, patch.dict(
                'os.environ', {'HERMES_HOME': directory}), patch(
                'hermes_cli.profiles.get_active_profile_name', return_value='profile-a'), patch.object(
                activation, 'build_host_factory', side_effect=deterministic_build):
            runner = GatewayRunner(GatewayConfig(caller_host_enabled=True,
                sessions_dir=Path(directory) / 'sessions'), caller_host_services=self.services)
            self.assertTrue(callable(runner._context_tool_factory))
            self.assertEqual(self.dispatches, [])
            with self.foreground(), self.bound(runner._context_tool_factory) as ready:
                self.assertTrue(ready)
                self.assertEqual(self.call(), {'status': 'completed', 'context': 'approved inventory context'})
            self.assertEqual(len(self.receipts), 1)
            self.assertEqual(self.receipts[0]['schema'], 'caller-activation/1')
            self.assertTrue(self.receipts[0]['cleanup_complete'])
            self.assertFalse(self.receipts[0]['durable_capacity_released'])
            self.assertNotIn(TOOL_NAME, self.agent.valid_tool_names)

    def test_missing_static_ports_fail_when_building_factory(self):
        for field in ('session', 'policy', 'mqtt_config', 'enrollment_lookup',
                      'approval_resolver', 'receipt_reader', 'create_owner',
                      'approve_result', 'audit_receipt', 'typed_reviewer'):
            with self.subTest(field=field), self.assertRaises(HostError):
                self.factory(services=replace(self.services, **{field: None}))
        for mqtt in ({}, dict(self.services.mqtt_config, unexpected='forbidden')):
            with self.subTest(mqtt=mqtt), self.assertRaises(HostError):
                self.factory(services=replace(self.services, mqtt_config=mqtt))
        self.assertEqual(self.dispatches, [])
        self.assertEqual(self.lookups, [])

    def test_services_and_approval_are_frozen(self):
        with self.assertRaises(FrozenInstanceError):
            self.services.approve_result = lambda **kwargs: True
        with self.assertRaises(FrozenInstanceError):
            activation.ApprovedContext('a', 'b', 'context', 1020).context = 'changed'


class TypedReviewerTests(unittest.TestCase):
    def setUp(self):
        self.typed, self.spec = b'{"task_id":"job-a"}', b'{"scope":"synthetic"}'
        self.envelope = dict(task_id='job-a', dispatcher_id='dispatcher-a', claim_owner='worker-a',
                             spec_sha256=hashlib.sha256(self.typed).hexdigest())
        self.events = []
        self.now = 1000.0
        self.owner = SimpleNamespace(canonical=SimpleNamespace(reviewers={'reviewer-a'},
            _attempt=lambda env: {'state': 'approved_effects_unknown', 'admitted': 0}))

    def exchange(self, **kwargs):
        self.events.append(('exchange', kwargs))

    def resolve(self, **kwargs):
        self.events.append(('resolve', kwargs))
        return dict(principal='reviewer-a', kind='reviewer', expires_at=1010.0,
                    request_spec_sha256=hashlib.sha256(self.spec).hexdigest(), envelope=self.envelope)

    def reviewer(self, exchange=None):
        return activation.TypedReviewer(exchange=exchange or self.exchange, resolve=self.resolve,
                                         clock=lambda: self.now, monotonic=lambda: 10.0)

    def test_typed_exchange_precedes_independent_exact_hash_lookup(self):
        reviewer = self.reviewer()
        result = reviewer(self.owner, self.typed, self.spec, deadline_at=1020.0)
        self.assertEqual(result, self.envelope)
        self.assertEqual(self.events, [
            ('exchange', dict(typed=self.typed, spec=self.spec, deadline_at=1020.0)),
            ('resolve', dict(spec_sha256=hashlib.sha256(self.typed).hexdigest(),
                            request_spec_sha256=hashlib.sha256(self.spec).hexdigest(), deadline_at=1020.0))])
        with self.assertRaises(HostError):
            reviewer(self.owner, self.typed, self.spec, deadline_at=1020.0)
        self.assertEqual(len(self.events), 2)

    def test_ambiguous_exchange_exception_is_never_retried(self):
        def unknown(**kwargs):
            self.events.append(('exchange', kwargs))
            raise TimeoutError('synthetic unknown outcome')
        reviewer = self.reviewer(unknown)
        with self.assertRaises((HostError, TimeoutError)):
            reviewer(self.owner, self.typed, self.spec, deadline_at=1020.0)
        with self.assertRaises(HostError):
            reviewer(self.owner, self.typed, self.spec, deadline_at=1020.0)
        self.assertEqual([event[0] for event in self.events], ['exchange'])

    def test_exchange_over_deadline_cannot_read_or_retry_approval(self):
        def late(**kwargs):
            self.events.append(('exchange', kwargs))
            self.now = 1020.0
        reviewer = self.reviewer(late)
        with self.assertRaises(HostError):
            reviewer(self.owner, self.typed, self.spec, deadline_at=1020.0)
        with self.assertRaises(HostError):
            reviewer(self.owner, self.typed, self.spec, deadline_at=1030.0)
        self.assertEqual([event[0] for event in self.events], ['exchange'])

    def test_mutable_proposal_or_invalid_document_never_reaches_reviewer(self):
        for typed in (bytearray(self.typed), b'not-json', b'[]'):
            with self.subTest(typed=typed), self.assertRaises(HostError):
                self.reviewer()(self.owner, typed, self.spec, deadline_at=1020.0)
        self.assertEqual(self.events, [])
