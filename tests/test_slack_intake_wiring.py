"""Intake wiring tests: the reviewed Slack intake bridge on current sources.

Exercises the real seams added by the p12-slack-intake composition with a
fake Bolt authorization and adapter state: _inventory_callback_receipt issues
an intake receipt only from an authenticated addressed callback, the receipt
travels through event metadata into admitted_request, and
capture_inventory_request returns the exact six-tuple on the original thread
only. Unauthenticated/mismatched callbacks, unaddressed traffic, copied
contexts and post-admission revocation all yield None. No real Slack SDK,
network or gateway startup.
"""
import contextlib
import importlib.util
import contextvars
import pathlib
import sys
import threading
import types
import unittest
from unittest.mock import MagicMock, patch

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _ensure_slack_stubs():
    """Minimal slack module stubs so the real adapter imports offline."""
    if "slack_bolt" in sys.modules and hasattr(sys.modules["slack_bolt"], "__file__"):
        return
    slack_bolt = MagicMock()
    slack_bolt.async_app.AsyncApp = MagicMock
    slack_bolt.adapter.socket_mode.async_handler.AsyncSocketModeHandler = MagicMock
    slack_sdk = MagicMock()
    slack_sdk.web.async_client.AsyncWebClient = MagicMock
    for name, mod in [
        ("slack_bolt", slack_bolt),
        ("slack_bolt.async_app", slack_bolt.async_app),
        ("slack_bolt.adapter", slack_bolt.adapter),
        ("slack_bolt.adapter.socket_mode", slack_bolt.adapter.socket_mode),
        ("slack_bolt.adapter.socket_mode.async_handler", slack_bolt.adapter.socket_mode.async_handler),
        ("slack_sdk", slack_sdk),
        ("slack_sdk.web", slack_sdk.web),
        ("slack_sdk.web.async_client", slack_sdk.web.async_client),
    ]:
        sys.modules.setdefault(name, mod)
    sys.modules.setdefault("aiohttp", MagicMock())


_ensure_slack_stubs()


class AuthorizeResult:
    """Stub matching the slack_bolt AuthorizeResult surface the wiring reads."""

    def __init__(self, team_id, bot_user_id):
        self.team_id = team_id
        self.bot_user_id = bot_user_id


_authz_pkg = types.ModuleType("slack_bolt.authorization")
_authz_mod = types.ModuleType("slack_bolt.authorization.authorize_result")
_authz_mod.AuthorizeResult = AuthorizeResult
sys.modules.setdefault("slack_bolt.authorization", _authz_pkg)
sys.modules.setdefault("slack_bolt.authorization.authorize_result", _authz_mod)

from gateway import inventory_context as native  # noqa: E402
from gateway.config import Platform, PlatformConfig  # noqa: E402

# Loaded by path: under pytest the bare 'plugins' package can resolve to a
# different installation; the file under test must come from this checkout.
# Its directory sits on sys.path during the load so the module-level
# 'from block_kit import render_blocks' fallback resolves.
_slack_dir = str(ROOT / "plugins" / "platforms" / "slack")
_adapter_spec = importlib.util.spec_from_file_location(
    "slack_adapter_under_test", ROOT / "plugins" / "platforms" / "slack" / "adapter.py"
)
slack_adapter = importlib.util.module_from_spec(_adapter_spec)
sys.modules[_adapter_spec.name] = slack_adapter
sys.path.insert(0, _slack_dir)
try:
    _adapter_spec.loader.exec_module(slack_adapter)
finally:
    sys.path.remove(_slack_dir)
SlackAdapter = slack_adapter.SlackAdapter


class IntakeWiringTests(unittest.TestCase):
    def setUp(self):
        self.mono = [100.0]
        config = PlatformConfig(enabled=True, token="xoxb-fake-token")
        clock = patch.object(native.time, "monotonic", lambda: self.mono[0])
        clock.start()
        self.addCleanup(clock.stop)
        native.clear_inherited()
        self.addCleanup(native.clear_inherited)
        self.adapter = SlackAdapter.__new__(SlackAdapter)
        self.adapter.config = config
        self.client = object()
        self.adapter._team_clients = {"workspace-a": self.client}
        self.adapter._team_bot_user_ids = {"workspace-a": "bot-a"}
        self.adapter._channel_team = {"channel-a": "workspace-a"}
        self.raw = dict(
            type="message", user="user-a", channel="channel-a", ts="100.2",
            thread_ts="100.1", text="<@bot-a> inventory please", channel_type="channel",
        )
        self.context = {
            "authorize_result": AuthorizeResult("workspace-a", "bot-a"),
            "team_id": "workspace-a",
        }

    def _event_with(self, receipt):
        return types.SimpleNamespace(
            raw_message=self.raw, message_id=self.raw["ts"],
            metadata={"_inventory_intake": receipt},
            source=types.SimpleNamespace(
                platform=types.SimpleNamespace(value="slack"), user_id=self.raw["user"],
                chat_id=self.raw["channel"], thread_id=self.raw["thread_ts"],
                scope_id="workspace-a",
            ),
        )

    @contextlib.contextmanager
    def _foreground(self, receipt):
        event = self._event_with(receipt)
        with native.admitted_request(event, self.adapter, "ito"), native.foreground_worker():
            yield event

    def test_authenticated_addressed_callback_admits_exact_six_tuple(self):
        receipt = self.adapter._inventory_callback_receipt(self.raw, self.context)
        self.assertIsNotNone(receipt)
        with self._foreground(receipt):
            request = native.capture_inventory_request()
            self.assertIsNotNone(request)
            self.assertEqual(
                request.identity,
                ("ito", "user-a", "slack", "workspace-a", "channel-a", "100.1"),
            )
            self.assertEqual(request.identity[2], "slack")
        # Admission ended with the turn: nothing survives exit.
        self.assertIsNone(native.capture_inventory_request())

    def test_unauthenticated_or_mismatched_callback_issues_nothing(self):
        self.assertIsNone(self.adapter._inventory_callback_receipt(self.raw, {}))
        wrong_type = {"authorize_result": object(), "team_id": "workspace-a"}
        self.assertIsNone(self.adapter._inventory_callback_receipt(self.raw, wrong_type))
        mismatch = {
            "authorize_result": AuthorizeResult("workspace-a", "bot-a"),
            "team_id": "workspace-b",
        }
        self.assertIsNone(self.adapter._inventory_callback_receipt(self.raw, mismatch))

    def test_unaddressed_traffic_never_issues(self):
        raw = dict(self.raw, text="hello other human")
        self.assertIsNone(self.adapter._inventory_callback_receipt(raw, self.context))
        dm_from_bot = dict(self.raw, user="bot-a", channel_type="im")
        self.assertIsNone(self.adapter._inventory_callback_receipt(dm_from_bot, self.context))

    def test_missing_receipt_means_no_request(self):
        with self._foreground(None):
            self.assertIsNone(native.capture_inventory_request())

    def test_copied_context_cannot_invoke_on_another_thread(self):
        receipt = self.adapter._inventory_callback_receipt(self.raw, self.context)
        with self._foreground(receipt):
            self.assertIsNotNone(native.capture_inventory_request())
            copied = contextvars.copy_context()
            results = []
            thread = threading.Thread(
                target=lambda: results.append(copied.run(native.capture_inventory_request))
            )
            thread.start()
            thread.join(1)
            self.assertFalse(thread.is_alive())
            self.assertEqual(results, [None])

    def test_revocation_between_preparation_and_invocation_refuses(self):
        receipt = self.adapter._inventory_callback_receipt(self.raw, self.context)
        with self._foreground(receipt):
            self.assertIsNotNone(native.capture_inventory_request())
            copied = contextvars.copy_context()
        # The turn ended: admission revoked, copied context cannot reopen it.
        self.assertIsNone(native.capture_inventory_request())
        self.assertIsNone(copied.run(native.capture_inventory_request))


if __name__ == "__main__":
    unittest.main()
