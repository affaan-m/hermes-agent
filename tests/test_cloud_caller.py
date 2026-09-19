"""Composition wiring tests for gateway/cloud_caller.py.

Hermetic: all operator ports and provider/SDK surfaces are fakes. Proves the
assembly contract (fail-closed port checks, exact reviewed class identity,
no provider work before invoke, per-invoke bundle construction on the calling
thread) and the intake-anchored resolver semantics (unenrolled turns hold,
revocation between prepare and invoke refuses, no cross-turn retention).
"""
import sys
import types
import unittest
from unittest import mock

from gateway import cloud_caller
from gateway.cloud_caller import HostError
from gateway.context_dispatch import NativeContextDispatcher
from gateway.context_tool import PreparedContextCall


def _ports(**overrides):
    ports = dict(
        session=object(),
        policy=types.SimpleNamespace(_value={"task_role_id": "AROA" + "A" * 17}),
        mqtt_config=dict(
            endpoint="example-ats.iot.us-east-1.amazonaws.com",
            cert_path="/synthetic/cert",
            key_path="/synthetic/key",
            ca_path="/synthetic/ca",
        ),
        approval_resolver=lambda **kw: None,
        receipt_reader=lambda key, **kw: None,
        create_owner=lambda: None,
        resolve_request=lambda agent: None,
        validate_request=lambda agent, request: False,
        project_context=lambda agent, request, context: "",
        modules=types.SimpleNamespace(),
    )
    ports.update(overrides)
    return ports


class BuildTests(unittest.TestCase):
    def test_missing_ports_fail_closed(self):
        for key in (
            "resolve_request", "validate_request", "project_context",
            "create_owner", "approval_resolver", "receipt_reader",
        ):
            with self.subTest(port=key):
                with self.assertRaises(HostError):
                    cloud_caller.build_context_tool_factory(**_ports(**{key: None}))

    def test_missing_session_policy_or_mqtt_fail_closed(self):
        for patch in (
            {"session": None},
            {"policy": None},
            {"mqtt_config": None},
            {"mqtt_config": {"endpoint": "x"}},
            {"mqtt_config": dict(endpoint="e", cert_path="c", key_path="k", ca_path="a", extra="nope")},
        ):
            with self.subTest(patch=sorted(patch)):
                with self.assertRaises(HostError):
                    cloud_caller.build_context_tool_factory(**_ports(**patch))

    def test_default_modules_resolve_lazily_or_fail_closed(self):
        # The fork alone does not ship infra.aws; composition without an
        # injected module namespace must fail, never silently substitute.
        # Where the ito-desk package is installed (production venv), the
        # defaults must resolve to the reviewed classes instead.
        try:
            mods = cloud_caller._default_modules()
        except ImportError:
            with self.assertRaises(ImportError):
                cloud_caller.build_context_tool_factory(**_ports(modules=None))
            return
        for name in ("DispatchPorts", "RunTaskGuard", "ActivationRouter", "ExchangeController", "ContextResults"):
            self.assertTrue(hasattr(mods, name), name)

    def test_factory_binds_exact_reviewed_classes(self):
        self.assertIs(cloud_caller.NativeContextDispatcher, NativeContextDispatcher)
        self.assertIs(cloud_caller.PreparedContextCall, PreparedContextCall)
        factory = cloud_caller.build_context_tool_factory(**_ports())
        self.assertTrue(callable(factory))


class FactoryFlowTests(unittest.TestCase):
    def setUp(self):
        self.bundle = mock.Mock(name="HostBundleInstance")
        self.boto = mock.Mock(name="BotoCallsInstance")
        self.bundle_patcher = mock.patch.object(cloud_caller, "HostBundle")
        self.boto_patcher = mock.patch.object(cloud_caller, "BotoCalls")
        self.dispatcher_patcher = mock.patch.object(cloud_caller, "NativeContextDispatcher")
        self.bundle_cls = self.bundle_patcher.start()
        self.boto_cls = self.boto_patcher.start()
        self.dispatcher_cls = self.dispatcher_patcher.start()
        self.bundle_cls.return_value = self.bundle
        self.boto_cls.return_value = self.boto
        self.addCleanup(self.bundle_patcher.stop)
        self.addCleanup(self.boto_patcher.stop)
        self.addCleanup(self.dispatcher_patcher.stop)
        self.policy = types.SimpleNamespace(_value={"task_role_id": "AROA" + "A" * 17})
        self.host_caller = object()
        self.controller_caller = object()
        self.request = dict(
            host_caller=self.host_caller,
            controller_caller=self.controller_caller,
            run_kwargs={"task_id": "job"},
            expires_at=1020.0,
        )

    def build(self, **overrides):
        ports = _ports(
            policy=self.policy,
            resolve_request=lambda agent: self.request,
            validate_request=lambda agent, request: True,
            project_context=lambda agent, request, context: "approved context",
        )
        ports.update({"clock": lambda: 1000.0, "monotonic": lambda: 10.0})
        ports.update(overrides)
        return cloud_caller.build_context_tool_factory(**ports)

    def _completed_outcome(self):
        return types.SimpleNamespace(
            state="completed",
            context_result={"private": "must project"},
            closure_receipt={"cleanup_complete": True, "durable_capacity_released": False},
        )

    def test_prepare_is_inert_then_invoke_constructs_once_on_thread(self):
        factory = self.build()
        prepared = factory(agent=object())
        self.assertIsInstance(prepared, PreparedContextCall)
        self.assertEqual(self.boto_cls.call_count, 0)
        self.assertEqual(self.bundle_cls.call_count, 0)
        self.assertEqual(self.dispatcher_cls.call_count, 0)
        outcome = self._completed_outcome()
        self.dispatcher_cls.return_value.run.return_value = outcome
        result = prepared.invoke()
        self.assertIs(result, outcome)
        # Bundle and provider client exist only after invoke, on this thread.
        self.assertEqual(self.boto_cls.call_count, 1)
        self.assertEqual(self.bundle_cls.call_count, 1)
        bundle_kwargs = self.bundle_cls.call_args.kwargs
        self.assertEqual(bundle_kwargs["request_deadline"], 1020.0)
        self.assertIs(bundle_kwargs["policy"], self.policy)
        self.assertIs(bundle_kwargs["provider_call"], self.boto)
        dispatcher_kwargs = self.dispatcher_cls.call_args.kwargs
        self.assertIs(dispatcher_kwargs["host_caller"], self.host_caller)
        self.assertIs(dispatcher_kwargs["controller_caller"], self.controller_caller)
        self.assertIs(dispatcher_kwargs["enabled"], True)
        self.dispatcher_cls.return_value.run.assert_called_once_with(task_id="job")
        with self.assertRaises(HostError):
            prepared.invoke()

    def test_projection_reaches_model_only_after_clean_closure(self):
        factory = self.build()
        prepared = factory(agent=object())
        outcome = self._completed_outcome()
        self.dispatcher_cls.return_value.run.return_value = outcome
        prepared.invoke()
        self.assertEqual(
            prepared.project_result(outcome),
            {"status": "completed", "context": "approved context"},
        )

    def test_unresolved_request_holds_without_construction(self):
        factory = self.build(resolve_request=lambda agent: None)
        with self.assertRaises(HostError):
            factory(agent=object())
        self.assertEqual(self.bundle_cls.call_count, 0)
        self.assertEqual(self.dispatcher_cls.call_count, 0)


class _FakeInventory(types.ModuleType):
    def __init__(self):
        super().__init__("gateway.inventory_context")
        self.current = None

    def capture_inventory_request(self):
        request = self.current
        return request if request is not None and request.validate() else None


class _Admitted:
    def __init__(self, identity):
        self.identity = identity
        self.live = True

    def validate(self):
        return self.live


class InventoryResolverTests(unittest.TestCase):
    def setUp(self):
        self.fake_inventory = _FakeInventory()
        self.gateway_pkg = types.ModuleType("gateway")
        patchers = [
            mock.patch.dict(sys.modules, {
                "gateway": self.gateway_pkg,
                "gateway.inventory_context": self.fake_inventory,
            }),
        ]
        for patcher in patchers:
            patcher.start()
            self.addCleanup(patcher.stop)
        self.identity = ("profile-a", "user-a", "slack", "workspace-a", "channel-a", "100.1")
        self.host_caller = object()
        self.controller_caller = object()
        self.run_kwargs = {"task_id": "job"}
        self.enrollment = {
            "host_caller": self.host_caller,
            "controller_caller": self.controller_caller,
            "run_kwargs": self.run_kwargs,
            "expires_at": 1020.0,
        }
        self.enrolled = True
        self.resolve, self.validate = cloud_caller.inventory_request_resolver(
            lambda identity: dict(self.enrollment) if self.enrolled else None
        )

    def test_no_admitted_request_resolves_to_none(self):
        self.assertIsNone(self.resolve(object()))

    def test_unenrolled_admitted_request_resolves_to_none(self):
        self.fake_inventory.current = _Admitted(self.identity)
        self.enrolled = False
        self.assertIsNone(self.resolve(object()))

    def test_enrolled_request_resolves_and_validates(self):
        self.fake_inventory.current = _Admitted(self.identity)
        request = self.resolve(object())
        self.assertEqual(
            set(request), {"host_caller", "controller_caller", "run_kwargs", "expires_at"}
        )
        self.assertIs(request["host_caller"], self.host_caller)
        self.assertTrue(self.validate(object(), request))

    def test_revocation_between_prepare_and_invoke_refuses(self):
        self.fake_inventory.current = _Admitted(self.identity)
        request = self.resolve(object())
        self.enrolled = False
        self.assertFalse(self.validate(object(), request))

    def test_expired_intake_refuses(self):
        admitted = _Admitted(self.identity)
        self.fake_inventory.current = admitted
        request = self.resolve(object())
        admitted.live = False
        self.assertFalse(self.validate(object(), request))

    def test_nothing_retained_across_turns(self):
        self.fake_inventory.current = _Admitted(self.identity)
        self.resolve(object())
        self.fake_inventory.current = None
        self.assertIsNone(self.resolve(object()))


if __name__ == "__main__":
    unittest.main()
