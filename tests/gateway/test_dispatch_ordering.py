"""Per-conversation serial dispatch and the concurrent-conversation cap.

Covers gateway/dispatch.py plus the base adapter and runner seams that use it:

* gateway.per_conversation_serial: three messages into one busy conversation
  run as three separate turns, in arrival order, never overlapping, with no
  text merge and no interrupt.
* gateway.max_concurrent_conversations: N conversations never exceed the cap
  and are admitted in FIFO order.
* one ``dispatch`` log line per turn carrying running=k/cap.
"""

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway import dispatch as dispatch_mod
from gateway.config import GatewayConfig
from gateway.dispatch import ConversationDispatcher, configure_dispatcher, reset_dispatcher
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    Platform,
    PlatformConfig,
    SendResult,
    SessionSource,
)


@pytest.fixture(autouse=True)
def _fresh_dispatcher():
    reset_dispatcher()
    yield
    reset_dispatcher()


class _FakeAdapter(BasePlatformAdapter):
    """Minimal adapter: no typing indicator, no network, records sends."""

    def __init__(self):
        cfg = PlatformConfig(enabled=True, token="test")
        cfg.typing_indicator = False
        super().__init__(cfg, Platform.SLACK)
        self.sent = []

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        return True

    async def disconnect(self) -> None:
        self._mark_disconnected()

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        self.sent.append(content)
        return SendResult(success=True, message_id="out-1")

    async def get_chat_info(self, chat_id):
        return {"id": chat_id, "type": "channel"}


def _event(text, message_id, chat_id="C1", thread_id="T1"):
    source = SessionSource(
        platform=Platform.SLACK,
        chat_id=chat_id,
        chat_type="channel",
        user_id="U1",
        thread_id=thread_id,
    )
    return MessageEvent(
        text=text,
        message_type=MessageType.TEXT,
        source=source,
        message_id=message_id,
    )


class _RecordingHandler:
    """Fake gateway handler: records order and overlap, releases on demand."""

    def __init__(self, hold=0.05):
        self.calls = []            # message ids in the order the handler started
        self.in_flight = 0
        self.peak_in_flight = 0
        self.in_flight_by_session = {}
        self.peak_by_session = {}
        self.hold = hold

    async def __call__(self, event):
        key = f"{event.source.chat_id}:{event.source.thread_id}"
        self.calls.append(event.message_id)
        self.in_flight += 1
        self.peak_in_flight = max(self.peak_in_flight, self.in_flight)
        n = self.in_flight_by_session.get(key, 0) + 1
        self.in_flight_by_session[key] = n
        self.peak_by_session[key] = max(self.peak_by_session.get(key, 0), n)
        try:
            await asyncio.sleep(self.hold)
        finally:
            self.in_flight -= 1
            self.in_flight_by_session[key] -= 1
        return None


async def _settle(adapter, timeout=3.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.02)
        if not adapter._background_tasks and not adapter._active_sessions:
            return
    raise AssertionError(
        f"adapter did not settle: tasks={len(adapter._background_tasks)} "
        f"active={list(adapter._active_sessions)}"
    )


# --------------------------------------------------------------------------
# ConversationDispatcher
# --------------------------------------------------------------------------


class TestConversationDispatcher:
    @pytest.mark.asyncio
    async def test_cap_bounds_running_and_admits_fifo(self):
        d = ConversationDispatcher(max_concurrent_conversations=2)
        started = []
        release = {}

        async def turn(name):
            slot = await d.acquire(name, platform="test", message_id=name)
            started.append(name)
            await release[name].wait()
            d.release(slot)

        for n in ("a", "b", "c", "d", "e"):
            release[n] = asyncio.Event()
        tasks = [asyncio.create_task(turn(n)) for n in ("a", "b", "c", "d", "e")]
        await asyncio.sleep(0.01)
        assert started == ["a", "b"]
        assert d.running == 2 and d.queued == 3

        release["a"].set()
        await asyncio.sleep(0.01)
        assert started == ["a", "b", "c"]
        assert d.running == 2

        release["b"].set()
        release["c"].set()
        await asyncio.sleep(0.01)
        assert started == ["a", "b", "c", "d", "e"]
        for n in ("d", "e"):
            release[n].set()
        await asyncio.gather(*tasks)
        assert d.running == 0
        assert d.peak_running == 2

    @pytest.mark.asyncio
    async def test_unbounded_never_waits(self):
        d = ConversationDispatcher()
        slots = [await d.acquire(f"s{i}") for i in range(10)]
        assert d.running == 10 and d.queued == 0
        for s in slots:
            d.release(s)
        assert d.running == 0

    @pytest.mark.asyncio
    async def test_cancelled_waiter_leaves_queue(self):
        d = ConversationDispatcher(max_concurrent_conversations=1)
        held = await d.acquire("first")
        waiter = asyncio.create_task(d.acquire("second"))
        await asyncio.sleep(0.01)
        assert d.queued == 1
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert d.queued == 0
        d.release(held)
        assert d.running == 0

    def test_coerce_cap(self):
        assert dispatch_mod.coerce_max_concurrent_conversations(None) is None
        assert dispatch_mod.coerce_max_concurrent_conversations(0) is None
        assert dispatch_mod.coerce_max_concurrent_conversations(-3) is None
        assert dispatch_mod.coerce_max_concurrent_conversations(True) is None
        assert dispatch_mod.coerce_max_concurrent_conversations("4") == 4
        assert dispatch_mod.coerce_max_concurrent_conversations("nope") is None


# --------------------------------------------------------------------------
# Config keys
# --------------------------------------------------------------------------


class TestConfigKeys:
    def test_nested_gateway_keys(self):
        cfg = GatewayConfig.from_dict(
            {"gateway": {"per_conversation_serial": True, "max_concurrent_conversations": 3}}
        )
        assert cfg.per_conversation_serial is True
        assert cfg.max_concurrent_conversations == 3
        d = cfg.to_dict()
        assert d["per_conversation_serial"] is True
        assert d["max_concurrent_conversations"] == 3

    def test_defaults(self):
        cfg = GatewayConfig.from_dict({})
        assert cfg.per_conversation_serial is False
        assert cfg.max_concurrent_conversations is None

    def test_top_level_wins_and_bad_values_disable(self):
        cfg = GatewayConfig.from_dict(
            {
                "per_conversation_serial": "yes",
                "max_concurrent_conversations": "x",
                "gateway": {"per_conversation_serial": False, "max_concurrent_conversations": 9},
            }
        )
        assert cfg.per_conversation_serial is True
        assert cfg.max_concurrent_conversations is None


# --------------------------------------------------------------------------
# Base adapter: per-conversation serial ordering
# --------------------------------------------------------------------------


class TestPerConversationSerial:
    @pytest.mark.asyncio
    async def test_three_messages_one_thread_run_in_order_without_overlap(self):
        configure_dispatcher(per_conversation_serial=True)
        adapter = _FakeAdapter()
        handler = _RecordingHandler(hold=0.05)
        adapter.set_message_handler(handler)

        for mid in ("1726.001", "1726.002", "1726.003"):
            await adapter.handle_message(_event(f"msg {mid}", mid))
            # all three land within the first turn's window
        await _settle(adapter)

        assert handler.calls == ["1726.001", "1726.002", "1726.003"]
        assert handler.peak_by_session["C1:T1"] == 1
        # No text merge: each turn saw exactly its own message.
        assert len(handler.calls) == 3

    @pytest.mark.asyncio
    async def test_serial_off_keeps_legacy_merge(self):
        configure_dispatcher(per_conversation_serial=False)
        adapter = _FakeAdapter()
        seen = []

        async def handler(event):
            seen.append(event.text)
            await asyncio.sleep(0.05)
            return None

        adapter.set_message_handler(handler)
        await adapter.handle_message(_event("one", "1"))
        await adapter.handle_message(_event("two", "2"))
        await adapter.handle_message(_event("three", "3"))
        await _settle(adapter)
        # Legacy behaviour: follow-ups merge into one pending turn.
        assert seen == ["one", "two\nthree"]

    @pytest.mark.asyncio
    async def test_serial_queue_survives_more_than_two_followups(self):
        configure_dispatcher(per_conversation_serial=True)
        adapter = _FakeAdapter()
        handler = _RecordingHandler(hold=0.02)
        adapter.set_message_handler(handler)
        ids = [f"m{i}" for i in range(6)]
        for mid in ids:
            await adapter.handle_message(_event(mid, mid))
        await _settle(adapter)
        assert handler.calls == ids

    @pytest.mark.asyncio
    async def test_dispatch_log_line_per_turn(self, caplog):
        configure_dispatcher(per_conversation_serial=True, max_concurrent_conversations=4)
        adapter = _FakeAdapter()
        adapter.set_message_handler(_RecordingHandler(hold=0.01))
        with caplog.at_level(logging.INFO, logger="gateway.dispatch"):
            for mid in ("a", "b"):
                await adapter.handle_message(_event(mid, mid))
            await _settle(adapter)
        starts = [r.getMessage() for r in caplog.records if r.getMessage().startswith("dispatch seq=")]
        assert len(starts) == 2
        assert "msg_id=a" in starts[0] and "msg_id=b" in starts[1]
        assert all("running=1/4" in line for line in starts)


# --------------------------------------------------------------------------
# Base adapter: bounded concurrent conversations
# --------------------------------------------------------------------------


class TestMaxConcurrentConversations:
    @pytest.mark.asyncio
    async def test_cap_holds_across_sessions(self):
        configure_dispatcher(max_concurrent_conversations=2)
        adapter = _FakeAdapter()
        handler = _RecordingHandler(hold=0.05)
        adapter.set_message_handler(handler)

        for i in range(6):
            await adapter.handle_message(_event(f"hi {i}", f"m{i}", chat_id=f"C{i}", thread_id=None))
        await _settle(adapter, timeout=5.0)

        assert handler.peak_in_flight == 2
        assert sorted(handler.calls) == [f"m{i}" for i in range(6)]
        # Admission is FIFO by arrival.
        assert handler.calls == [f"m{i}" for i in range(6)]
        assert adapter._dispatcher.running == 0

    @pytest.mark.asyncio
    async def test_cap_and_serial_together(self):
        configure_dispatcher(per_conversation_serial=True, max_concurrent_conversations=1)
        adapter = _FakeAdapter()
        handler = _RecordingHandler(hold=0.02)
        adapter.set_message_handler(handler)
        await adapter.handle_message(_event("a1", "a1", chat_id="A", thread_id=None))
        await adapter.handle_message(_event("b1", "b1", chat_id="B", thread_id=None))
        await adapter.handle_message(_event("a2", "a2", chat_id="A", thread_id=None))
        await _settle(adapter, timeout=5.0)
        assert handler.peak_in_flight == 1
        assert handler.calls.index("a1") < handler.calls.index("a2")
        assert set(handler.calls) == {"a1", "b1", "a2"}

    @pytest.mark.asyncio
    async def test_slot_released_when_handler_raises(self):
        configure_dispatcher(max_concurrent_conversations=1)
        adapter = _FakeAdapter()

        async def boom(event):
            raise RuntimeError("handler failed")

        adapter.set_message_handler(boom)
        await adapter.handle_message(_event("x", "x", chat_id="X", thread_id=None))
        await _settle(adapter)
        assert adapter._dispatcher.running == 0


# --------------------------------------------------------------------------
# Runner busy handler: serial mode never interrupts
# --------------------------------------------------------------------------


def _make_runner():
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    runner._running_agents = {}
    runner._running_agents_ts = {}
    runner._busy_ack_ts = {}
    runner._draining = False
    runner._busy_input_mode = "interrupt"
    runner._busy_text_mode = "interrupt"
    runner.adapters = {}
    runner.config = MagicMock()
    runner.config.group_sessions_per_user = True
    runner.config.thread_sessions_per_user = False
    runner.session_store = None
    runner.hooks = MagicMock()
    runner.hooks.emit = AsyncMock()
    runner._is_user_authorized = lambda _source: True
    return runner


class TestRunnerSerialBusyPath:
    @pytest.mark.asyncio
    async def test_serial_mode_queues_each_message_and_never_interrupts(self):
        configure_dispatcher(per_conversation_serial=True)
        runner = _make_runner()
        adapter = _FakeAdapter()
        runner.adapters[Platform.SLACK] = adapter
        agent = MagicMock()
        key = "slack:C1:thread:T1"
        runner._running_agents[key] = agent

        assert await runner._handle_active_session_busy_message(_event("two", "2"), key) is True
        assert await runner._handle_active_session_busy_message(_event("three", "3"), key) is True

        agent.interrupt.assert_not_called()
        agent.steer.assert_not_called()
        assert adapter.sent == []  # no busy ack in serial mode
        assert adapter._pending_messages[key].message_id == "2"
        assert [e.message_id for e in runner._queued_events[key]] == ["3"]
        assert runner._queue_depth(key, adapter=adapter) == 2

    @pytest.mark.asyncio
    async def test_serial_off_still_interrupts(self):
        configure_dispatcher(per_conversation_serial=False)
        runner = _make_runner()
        adapter = _FakeAdapter()
        runner.adapters[Platform.SLACK] = adapter
        agent = MagicMock()
        agent.get_activity_summary.return_value = {}
        key = "slack:C1:thread:T1"
        runner._running_agents[key] = agent
        runner._agent_has_active_subagents = lambda _a: False
        runner._session_has_compression_in_flight = lambda _k: False
        await runner._handle_active_session_busy_message(_event("two", "2"), key)
        agent.interrupt.assert_called_once()
