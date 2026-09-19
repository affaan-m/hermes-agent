"""Offline helper coverage; full captured registry only, never discovery/startup."""
import contextvars
import gc
import importlib.util
import json
from pathlib import Path
import sys
import threading
import types
import unittest
import weakref

ROOT = Path(__file__).resolve().parents[1]
MAP = ROOT.parent / 'native-caller-map' / 'source'


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def load_context_tool():
    return _load('context_tool_under_test', ROOT / 'source/gateway/context_tool.py')


def load_registry():
    return _load('captured_registry_under_test', MAP / 'tools/registry.py')


def clean_outcome(context='B300 inventory summary'):
    return types.SimpleNamespace(state='completed', reason='clean_context_pending_review',
        context_result=context, closure_receipt={'cleanup_complete': True,
        'durable_capacity_released': False, 'canonical_state': 'execution_succeeded_pending_review'})


class ContextToolTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = load_context_tool()
        cls.registry_mod = load_registry()

    def setUp(self):
        self.registry = self.registry_mod.ToolRegistry()
        self.lock = threading.RLock()
        self.agent = types.SimpleNamespace(api_mode='chat_completions', tools=[
            {'type': 'function', 'function': {'name': 'ordinary', 'parameters': {}}}],
            valid_tool_names={'ordinary'})
        self.wall = 100.0
        self.mono = 200.0
        self.calls = []
        self.factories = []
        self.valid = True
        self.outcome = clean_outcome()
        self.projection = lambda outcome: {'status': 'completed', 'context': outcome.context_result}

    def prepared(self, **overrides):
        args = dict(invoke=self.invoke, validate=lambda: self.valid,
            project_result=lambda outcome: self.projection(outcome), expires_at=110.0,
            clock=lambda: self.wall, monotonic_clock=lambda: self.mono)
        args.update(overrides)
        return self.mod.PreparedContextCall(**args)

    def invoke(self):
        self.calls.append('invoke')
        return self.outcome

    def factory(self, *, agent):
        self.assertIs(agent, self.agent)
        self.factories.append('prepare')
        return self.prepared()

    def bind(self, factory=None):
        return self.mod.bind_context_tool(self.agent, factory=factory or self.factory,
            registry=self.registry, snapshot_lock=self.lock)

    def dispatch(self, args=None):
        return json.loads(self.registry.dispatch(self.mod.TOOL_NAME, {} if args is None else args))

    def test_default_inert_and_unsupported_modes_do_not_prepare(self):
        with self.mod.bind_context_tool(self.agent, registry=self.registry, snapshot_lock=self.lock):
            self.assertNotIn(self.mod.TOOL_NAME, self.agent.valid_tool_names)
        for mode in ('codex_app_server', 'unknown', None):
            self.agent.api_mode = mode
            with self.bind():
                self.assertNotIn(self.mod.TOOL_NAME, self.agent.valid_tool_names)
        self.assertEqual(self.factories, [])

    def test_completed_once_and_registry_always_hidden(self):
        with self.bind():
            self.assertIn(self.mod.TOOL_NAME, self.agent.valid_tool_names)
            entry = self.registry.get_entry(self.mod.TOOL_NAME)
            self.assertFalse(entry.check_fn())
            self.assertEqual(self.registry.get_definitions({self.mod.TOOL_NAME}), [])
            self.assertEqual(self.dispatch(), {'status': 'completed', 'context': 'B300 inventory summary'})
            self.assertEqual(self.dispatch(), {'status': 'held'})
        self.assertEqual(self.calls, ['invoke'])
        self.assertEqual(self.agent.valid_tool_names, {'ordinary'})
        self.assertEqual(self.dispatch(), {'status': 'held'})

    def test_overlap_rejects_second_factory_preserving_first(self):
        with self.bind():
            with self.assertRaises(self.mod.ContextToolBindingError):
                with self.bind():
                    self.fail('overlapping body executed')
            self.assertEqual(self.factories, ['prepare'])
            self.assertEqual(self.dispatch()['status'], 'completed')
            self.assertIn(self.mod.TOOL_NAME, self.agent.valid_tool_names)
        self.assertEqual(self.calls, ['invoke'])

    def test_overlap_with_default_or_unsupported_binding_never_enters_body(self):
        with self.bind():
            for mode in ('chat_completions', 'codex_app_server'):
                self.agent.api_mode = mode
                with self.assertRaises(self.mod.ContextToolBindingError):
                    with self.mod.bind_context_tool(self.agent, registry=self.registry, snapshot_lock=self.lock):
                        self.fail('default overlapping conversation entered')
            self.agent.api_mode = 'chat_completions'
            self.assertEqual(self.dispatch()['status'], 'completed')

    def test_cross_thread_overlap_rejects_before_factory(self):
        results = []
        with self.bind():
            def attempt():
                try:
                    with self.bind():
                        results.append('body')
                except self.mod.ContextToolBindingError:
                    results.append('rejected')
            thread = threading.Thread(target=attempt)
            thread.start(); thread.join(timeout=2)
            self.assertFalse(thread.is_alive())
            self.assertEqual(results, ['rejected'])
            self.assertEqual(self.factories, ['prepare'])
            self.assertEqual(self.dispatch()['status'], 'completed')

    def test_preparation_reentry_cannot_invoke_or_publish(self):
        results = []
        def validate():
            if not results:
                results.append(self.dispatch())
                with self.lock:
                    definitions, names = self.mod.project_context_tool(self.agent, [], set(), registry=self.registry)
                self.assertNotIn(self.mod.TOOL_NAME, names)
            return True
        with self.bind(lambda **kw: self.prepared(validate=validate)):
            self.assertEqual(results, [{'status': 'held'}])
            self.assertEqual(self.calls, [])
            self.assertEqual(self.dispatch()['status'], 'completed')

    def test_registry_identity_and_closed_copy_cannot_reuse_binding(self):
        other = self.registry_mod.ToolRegistry()
        with self.mod.bind_context_tool(self.agent, factory=self.factory, registry=other, snapshot_lock=self.lock):
            pass
        with self.bind():
            copied = contextvars.copy_context()
            self.assertEqual(json.loads(other.dispatch(self.mod.TOOL_NAME, {})), {'status': 'held'})
            self.assertEqual(self.calls, [])
        self.assertEqual(copied.run(self.dispatch), {'status': 'held'})
        self.assertEqual(self.calls, [])

    def test_body_exception_cleans_only_owned_schema(self):
        with self.assertRaisesRegex(RuntimeError, 'ordinary failure'):
            with self.bind():
                raise RuntimeError('ordinary failure')
        self.assertEqual(self.agent.valid_tool_names, {'ordinary'})
        with self.bind():
            self.assertEqual(self.dispatch()['status'], 'completed')

    def test_same_thread_copied_context_denied_original_survives(self):
        with self.bind():
            copied = contextvars.copy_context()
            self.assertEqual(copied.run(self.dispatch), {'status': 'held'})
            self.assertEqual(self.calls, [])
            self.assertEqual(self.dispatch()['status'], 'completed')

    def test_other_physical_thread_with_copied_context_denied(self):
        with self.bind():
            results = []
            copied = contextvars.copy_context()
            thread = threading.Thread(target=lambda: results.append(copied.run(self.dispatch)))
            thread.start(); thread.join(timeout=2)
            self.assertFalse(thread.is_alive())
            self.assertEqual(results, [{'status': 'held'}])
            self.assertEqual(self.calls, [])
            self.assertEqual(self.dispatch()['status'], 'completed')

    def test_reentrant_invocation_denied(self):
        seen = []
        def invoke():
            self.calls.append('invoke')
            seen.append(self.dispatch())
            return self.outcome
        with self.bind(lambda **kw: self.prepared(invoke=invoke)):
            self.assertEqual(self.dispatch()['status'], 'completed')
        self.assertEqual(seen, [{'status': 'held'}])
        self.assertEqual(self.calls, ['invoke'])

    def test_untrusted_arguments_denied_without_consuming_request(self):
        with self.bind():
            self.assertEqual(self.dispatch({'approved': True}), {'status': 'held'})
            self.assertEqual(self.calls, [])
            self.assertEqual(self.dispatch()['status'], 'completed')

    def test_expired_and_revoked_before_invoke(self):
        with self.bind():
            self.wall = 111
            self.assertEqual(self.dispatch(), {'status': 'held'})
        self.assertEqual(self.calls, [])
        self.wall = 100
        self.valid = False
        with self.bind():
            self.assertEqual(self.dispatch(), {'status': 'held'})
        self.assertEqual(self.calls, [])

    def test_monotonic_advancement_expiry_and_rollback_denied(self):
        with self.bind():
            self.mono = 211
            self.assertEqual(self.dispatch(), {'status': 'held'})
        self.mono = 200
        with self.bind():
            self.mono = 199
            self.assertEqual(self.dispatch(), {'status': 'held'})
        self.assertEqual(self.calls, [])

    def test_nonfinite_clocks_or_expiry_do_not_expose(self):
        for kwargs in ({'expires_at': float('inf')}, {'clock': lambda: float('nan')},
                {'monotonic_clock': lambda: float('inf')}):
            with self.bind(lambda **kw: self.prepared(**kwargs)):
                self.assertNotIn(self.mod.TOOL_NAME, self.agent.valid_tool_names)
        self.assertEqual(self.calls, [])

    def test_late_expiry_after_invoke_withholds_context(self):
        def invoke():
            self.calls.append('invoke'); self.mono = 211
            return self.outcome
        with self.bind(lambda **kw: self.prepared(invoke=invoke)):
            self.assertEqual(self.dispatch(), {'status': 'held'})
        self.assertEqual(self.calls, ['invoke'])

    def test_projection_revocation_withholds_context(self):
        def project(outcome):
            self.valid = False
            return {'status': 'completed', 'context': 'must not escape'}
        self.projection = project
        with self.bind():
            self.assertEqual(self.dispatch(), {'status': 'held'})

    def test_unknown_denied_dirty_closure_never_projected_or_retried(self):
        outcomes = [types.SimpleNamespace(state=state) for state in ('unknown', 'held', 'rejected')]
        for field, value in (('cleanup_complete', False), ('durable_capacity_released', True)):
            out = clean_outcome(); out.closure_receipt[field] = value; outcomes.append(out)
        for outcome in outcomes:
            self.outcome = outcome
            self.projection = lambda outcome: self.fail('unsafe outcome projected')
            with self.bind():
                self.assertEqual(self.dispatch(), {'status': 'held'})
                self.assertEqual(self.dispatch(), {'status': 'held'})
        self.assertEqual(len(self.calls), len(outcomes))

    def test_exceptions_and_unapproved_projection_never_leak(self):
        def fail():
            raise RuntimeError('PRIVATE credential-shaped SECRET')
        with self.bind(lambda **kw: self.prepared(invoke=fail)):
            self.assertEqual(self.dispatch(), {'status': 'held'})
        for projection in ({'status': 'completed', 'context': 'x', 'closure_receipt': 'SECRET'},
                {'status': 'completed', 'context': 'x' * 20000},
                {'status': 'completed', 'context': {'private': 'SECRET'}}, None):
            self.projection = lambda outcome, value=projection: value
            with self.bind():
                self.assertEqual(self.dispatch(), {'status': 'held'})

    def test_registration_collision_preserves_foreign_entry(self):
        handler = lambda args, **kw: 'foreign'
        self.registry.register(self.mod.TOOL_NAME, 'foreign', {'name': self.mod.TOOL_NAME}, handler)
        entry = self.registry.get_entry(self.mod.TOOL_NAME)
        with self.bind():
            self.assertNotIn(self.mod.TOOL_NAME, self.agent.valid_tool_names)
        self.assertIs(self.registry.get_entry(self.mod.TOOL_NAME), entry)
        self.assertEqual(self.factories, [])

    def test_entry_mutation_invalidates_dispatch_and_publication(self):
        with self.bind():
            entry = self.registry.get_entry(self.mod.TOOL_NAME)
            entry.check_fn = lambda: True
            self.assertEqual(self.dispatch(), {'status': 'held'})
            with self.lock:
                definitions, names = self.mod.project_context_tool(self.agent, self.agent.tools, self.agent.valid_tool_names, registry=self.registry)
            self.assertNotIn(self.mod.TOOL_NAME, names)
        self.assertEqual(self.calls, [])

    def test_refresh_removes_stale_closed_schema_preserves_other_tools(self):
        with self.bind():
            stale = list(self.agent.tools)
            self.agent.tools.append({'type': 'function', 'function': {'name': 'new_tool'}})
            self.agent.valid_tool_names.add('new_tool')
        self.assertEqual(self.agent.valid_tool_names, {'ordinary', 'new_tool'})
        with self.lock:
            definitions, names = self.mod.project_context_tool(self.agent, stale, {'ordinary', self.mod.TOOL_NAME}, registry=self.registry)
        self.assertEqual(names, {'ordinary'})
        self.assertEqual([d['function']['name'] for d in definitions], ['ordinary'])

    def test_mode_change_while_bound_removes_publication(self):
        with self.bind():
            self.agent.api_mode = 'codex_app_server'
            with self.lock:
                definitions, names = self.mod.project_context_tool(self.agent, self.agent.tools, self.agent.valid_tool_names, registry=self.registry)
            self.assertNotIn(self.mod.TOOL_NAME, names)
            self.assertFalse(any(d['function']['name'] == self.mod.TOOL_NAME for d in definitions))

    def test_mode_change_while_bound_denies_invoke_before_action(self):
        with self.bind():
            self.agent.api_mode = 'unsupported-mode'
            self.assertEqual(self.dispatch(), {'status': 'held'})
        self.assertEqual(self.calls, [])

    def test_validation_callback_mode_change_denies_before_invoke(self):
        validations = []
        def validate():
            validations.append('validate')
            if len(validations) > 1:
                self.agent.api_mode = 'codex_app_server'
            return True
        with self.bind(lambda **kw: self.prepared(validate=validate)):
            self.assertEqual(self.dispatch(), {'status': 'held'})
        self.assertEqual(self.calls, [])

    def test_absent_owned_entry_strips_stale_definition_without_recreation(self):
        with self.bind():
            stale = list(self.agent.tools)
        self.registry.deregister(self.mod.TOOL_NAME)
        with self.lock:
            definitions, names = self.mod.project_context_tool(self.agent, stale,
                {'ordinary', self.mod.TOOL_NAME}, registry=self.registry)
        self.assertEqual(names, {'ordinary'})
        self.assertFalse(any(d['function']['name'] == self.mod.TOOL_NAME for d in definitions))
        with self.bind():
            self.assertNotIn(self.mod.TOOL_NAME, self.agent.valid_tool_names)
        self.assertEqual(self.factories, ['prepare'])

    def test_missing_registry_proof_preserves_unowned_metadata(self):
        foreign = {'type': 'function', 'function': {'name': self.mod.TOOL_NAME, 'description': 'unowned'}}
        with self.lock:
            definitions, names = self.mod.project_context_tool(self.agent, [foreign], {self.mod.TOOL_NAME})
        self.assertEqual(definitions, [foreign])
        self.assertEqual(names, {self.mod.TOOL_NAME})

    def test_identical_foreign_replacement_keeps_metadata_but_denies_binding(self):
        with self.bind():
            owned_schema = dict(self.registry.get_entry(self.mod.TOOL_NAME).schema)
            self.registry.deregister(self.mod.TOOL_NAME)
            handler = lambda args, **kwargs: 'foreign'
            self.registry.register(self.mod.TOOL_NAME, 'foreign', owned_schema, handler)
            with self.lock:
                definitions, names = self.mod.project_context_tool(self.agent, self.agent.tools,
                    self.agent.valid_tool_names, registry=self.registry)
            self.assertIn(self.mod.TOOL_NAME, names)
            self.assertTrue(any(d['function'] == owned_schema for d in definitions))
            # The retired owned handler cannot claim this foreign registry entry.
            self.assertEqual(json.loads(self.mod._dispatch(self.registry, {})), {'status': 'held'})
        self.assertEqual(self.calls, [])
        self.assertIs(self.registry.get_entry(self.mod.TOOL_NAME).handler, handler)

    def test_closed_copied_context_releases_agent_and_registry_graphs(self):
        class Agent:
            pass
        agent = Agent()
        agent.api_mode = 'chat_completions'
        agent.tools = []
        agent.valid_tool_names = set()
        self.agent = agent
        registry = self.registry
        agent_ref, registry_ref = weakref.ref(agent), weakref.ref(registry)
        with self.bind():
            copied = contextvars.copy_context()
        del agent, registry, self.agent, self.registry
        gc.collect()
        self.assertIsNone(agent_ref(), 'closed copied Context retained cached agent')
        self.assertIsNone(registry_ref(), 'closed copied Context retained registry')
        self.assertIsNotNone(copied)  # Keep the closed copied Context alive through assertions.

    def test_cleanup_projection_exception_still_resets_and_clears_binding(self):
        def exercise():
            original = self.mod.project_context_tool
            captured = []
            def fail_closing(agent, definitions, names, **kwargs):
                binding = self.mod._CURRENT.get()
                if binding is not None and binding.closed:
                    raise RuntimeError('synthetic close failure')
                return original(agent, definitions, names, **kwargs)
            self.mod.project_context_tool = fail_closing
            try:
                with self.assertRaisesRegex(self.mod.ContextToolBindingError, '^Cloud context cleanup failed$'):
                    with self.bind():
                        captured.append(self.mod._CURRENT.get())
            finally:
                self.mod.project_context_tool = original
            binding = captured[0]
            self.assertIsNone(self.mod._CURRENT.get(), 'cleanup exception left current binding')
            self.assertIsNone(self.mod._WITNESS.get(), 'cleanup exception left original-context witness')
            self.assertIsNone(binding.prepared)
            self.assertIsNone(binding.agent)
            self.assertIsNone(binding.registry)
            with self.bind():
                self.assertEqual(self.dispatch()['status'], 'completed')
        contextvars.Context().run(exercise)

    def test_cleanup_setter_exception_still_retires_authority_and_allows_reuse(self):
        class Agent:
            def __setattr__(self, name, value):
                if name == 'tools' and getattr(self, 'fail_tools', False):
                    raise RuntimeError('PRIVATE synthetic setter error')
                object.__setattr__(self, name, value)
        agent = Agent()
        agent.api_mode = 'chat_completions'
        agent.tools = []
        agent.valid_tool_names = set()
        self.agent = agent
        with self.assertRaisesRegex(self.mod.ContextToolBindingError, '^Cloud context cleanup failed$'):
            with self.bind():
                binding = self.mod._CURRENT.get()
                agent.fail_tools = True
        self.assertIsNone(self.mod._CURRENT.get())
        self.assertIsNone(self.mod._WITNESS.get())
        self.assertIsNone(binding.prepared)
        self.assertIsNone(binding.agent)
        self.assertIsNone(binding.registry)
        self.assertEqual(self.dispatch(), {'status': 'held'})
        agent.fail_tools = False
        with self.bind():
            self.assertEqual(self.dispatch()['status'], 'completed')

    def test_foreign_same_name_projection_survives_refused_collision(self):
        schema = {'name': self.mod.TOOL_NAME, 'description': 'Foreign existing tool', 'parameters': {}}
        handler = lambda args, **kw: 'foreign result'
        self.registry.register(self.mod.TOOL_NAME, 'foreign', schema, handler)
        foreign_definition = {'type': 'function', 'function': schema}
        self.agent.tools.append(foreign_definition)
        self.agent.valid_tool_names.add(self.mod.TOOL_NAME)
        with self.bind():
            self.assertEqual(self.factories, [])
            with self.lock:
                definitions, names = self.mod.project_context_tool(self.agent, self.agent.tools, self.agent.valid_tool_names, registry=self.registry)
            self.assertIn(self.mod.TOOL_NAME, names, 'foreign tool removed after refused collision')
            self.assertIn(foreign_definition, definitions)
        self.assertIs(self.registry.get_entry(self.mod.TOOL_NAME).handler, handler)

    def test_factory_exception_clears_reservation_cached_agent_reusable(self):
        def fail(**kw):
            raise RuntimeError('PRIVATE')
        with self.bind(fail):
            self.assertNotIn(self.mod.TOOL_NAME, self.agent.valid_tool_names)
        with self.bind():
            self.assertEqual(self.dispatch()['status'], 'completed')


if __name__ == '__main__':
    unittest.main()
