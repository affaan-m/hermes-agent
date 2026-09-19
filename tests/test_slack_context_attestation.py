"""Actual captured inventory lease with synthetic authenticated intake inputs.

Ported from the native-slack candidate (original pin f29208ec): test bodies
are byte-identical; only module resolution changed. The module under test is
loaded from this repo under a stub package so the sibling audience module's
relative import resolves without importing the real gateway package.
"""
import contextlib
import contextvars
import importlib.util
import pathlib
import sys
import threading
import types
import unittest
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_pkg = types.ModuleType("native_audience_candidate")
_pkg.__path__ = []
sys.modules["native_audience_candidate"] = _pkg
native = _load(
    "native_audience_candidate.inventory_context", ROOT / "gateway" / "inventory_context.py"
)


class NativeFixture(unittest.TestCase):
    def setUp(self):
        self.now, self.mono = 1700000000.0, 100.0
        clock = patch.object(native.time, "monotonic", lambda: self.mono)
        clock.start(); self.addCleanup(clock.stop)
        native.clear_inherited(); self.addCleanup(native.clear_inherited)
        self.client = object()
        self.adapter = types.SimpleNamespace(config=types.SimpleNamespace(extra={}),
            _team_clients={"workspace-a": self.client}, _team_bot_user_ids={"workspace-a": "bot-a"},
            _channel_team={"channel-a": "workspace-a"})
        self.raw = dict(type="message", user="user-a", channel="channel-a", ts="100.2",
                        thread_ts="100.1", text="<@bot-a> inventory please", channel_type="channel")

    @contextlib.contextmanager
    def foreground(self, *, profile="profile-a", raw=None):
        raw = self.raw if raw is None else raw
        receipt = native.issue_intake(self.adapter, raw, workspace="workspace-a", client=self.client, bot_user_id="bot-a")
        message = raw["ts"]
        threaded = self.adapter.config.extra.get("reply_in_thread", True)
        session = raw.get("thread_ts") or (message if threaded or raw.get("channel_type") == "im" else None)
        event = types.SimpleNamespace(raw_message=raw, message_id=message, metadata={"_inventory_intake": receipt},
            source=types.SimpleNamespace(platform=types.SimpleNamespace(value="slack"), user_id=raw["user"],
                chat_id=raw["channel"], thread_id=session, scope_id="workspace-a"))
        with native.admitted_request(event, self.adapter, profile), native.foreground_worker():
            yield event

    def capture(self):
        return native.capture_inventory_context_attestation()

    def resolve(self, handle):
        return native.resolve_inventory_context_attestation(handle)


class AttestationTests(NativeFixture):
    def test_original_message_and_actual_reply_thread_are_distinct(self):
        with self.foreground():
            handle = self.capture(); self.assertIsNotNone(handle)
            value = self.resolve(handle)
            self.assertEqual(set(value), {"identity", "delivery_identity", "message_id", "bot_user_id"})
            self.assertEqual(value["message_id"], "100.2")
            self.assertEqual(value["identity"][-1], "100.1")
            self.assertEqual(value["delivery_identity"][-1], "100.1")
            self.assertEqual(value["bot_user_id"], "bot-a")
            self.assertNotIn(self.raw["text"], repr(value))

    def test_direct_message_flat_output_keeps_actual_none_thread(self):
        self.adapter.config.extra["reply_in_thread"] = False
        raw = dict(self.raw, channel_type="im", text="inventory please"); raw.pop("thread_ts")
        with self.foreground(raw=raw):
            value = self.resolve(self.capture())
            self.assertEqual(value["identity"][-1], raw["ts"])
            self.assertIsNone(value["delivery_identity"][-1])

    def test_unmentioned_human_conversation_does_not_issue(self):
        with self.foreground(raw=dict(self.raw, text="hello other human")):
            self.assertIsNone(self.capture())

    def test_forged_handle_and_public_looking_read_do_not_authorize(self):
        with self.foreground():
            handle = self.capture()
            self.assertIsNone(self.resolve(object()))
            self.assertIsNone(self.resolve(types.SimpleNamespace(read=lambda: self.resolve(handle))))
            self.assertIsNone(self.resolve(type(handle)()))

    def test_copied_context_cannot_move_to_another_physical_thread(self):
        with self.foreground():
            handle = self.capture(); copied = contextvars.copy_context(); results = []
            thread = threading.Thread(target=lambda: results.append(copied.run(self.resolve, handle)))
            thread.start(); thread.join(1)
            self.assertFalse(thread.is_alive()); self.assertEqual(results, [None])
            self.assertIsNotNone(self.resolve(handle))

    def test_copied_context_cannot_reopen_finished_foreground(self):
        with self.foreground():
            handle = self.capture(); copied = contextvars.copy_context()
        self.assertIsNone(copied.run(self.resolve, handle))

    def test_selected_client_replacement_invalidates_attestation(self):
        with self.foreground():
            handle = self.capture(); self.adapter._team_clients["workspace-a"] = object()
            self.assertIsNone(self.resolve(handle))

    def test_same_channel_other_workspace_cannot_borrow_attestation(self):
        with self.foreground():
            handle = self.capture(); self.adapter._channel_team["channel-a"] = "workspace-b"
            self.assertIsNone(self.resolve(handle))

    def test_routing_or_bot_replacement_invalidates_attestation(self):
        with self.foreground():
            handle = self.capture(); self.adapter.config.extra["reply_in_thread"] = False
            self.assertIsNone(self.resolve(handle))

    def test_original_event_mutation_invalidates_attestation(self):
        with self.foreground():
            handle = self.capture(); self.raw["ts"] = "100.3"
            self.assertIsNone(self.resolve(handle))

    def test_intake_expiry_invalidates_attestation(self):
        with self.foreground():
            handle = self.capture(); self.mono += 301
            self.assertIsNone(self.resolve(handle))

    def test_old_handle_cannot_follow_a_new_native_request(self):
        with self.foreground():
            old = self.capture()
        with self.foreground(raw=dict(self.raw, ts="100.3")):
            self.assertIsNotNone(self.capture()); self.assertIsNone(self.resolve(old))


if __name__ == "__main__":
    unittest.main()
