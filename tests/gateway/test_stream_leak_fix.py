"""Regression tests for the quiet-channel queued-follow-up stream leak (0.21.3 port).

Port of tests/gateway/test_stream_leak_fix.py from the runtime branch
(affaan-m/hermes-agent#30). On 2026-09-18 the desk answered an operator's own
posts inside a quiet (counterparty-facing) Slack channel when a follow-up
arrived mid-turn: the queued-lane delivery sent ``first_response`` directly to
``source.chat_id``, bypassing the ``_operator_unaddressed_in_quiet`` reroute
the completed-turn path applies. At 0.21.3 the delivery lives in
``GatewayRunner._run_agent_deliver_first_response`` (gateway/run_turn.py);
these tests drive ``_run_agent`` with a queued follow-up and assert the
delivery target of the interrupted turn's first response.
"""

import importlib
import sys
import types
from types import SimpleNamespace

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent, MessageType
from gateway.session import SessionSource

QUIET_CHAT = "C0BQUIET"
HOME_CHAT = "C0BHOME"
SESSION_KEY = f"agent:main:slack:group:{QUIET_CHAT}"
OPERATOR_ID = "U0BOPERATOR"
COUNTERPARTY_ID = "U0BSUPPLIER"


class CaptureAdapter(BasePlatformAdapter):
    def __init__(self):
        super().__init__(PlatformConfig(enabled=True, token="***"), Platform.SLACK)
        self.sent = []
        self.typing = []
        self.fail_chat_ids = set()

    async def connect(self) -> bool:
        return True

    async def disconnect(self) -> None:
        return None

    async def send(self, chat_id, content, reply_to=None, metadata=None) -> SendResult:
        if chat_id in self.fail_chat_ids:
            raise RuntimeError(f"simulated send failure to {chat_id}")
        self.sent.append(
            {
                "chat_id": chat_id,
                "content": content,
                "reply_to": reply_to,
                "metadata": metadata,
            }
        )
        return SendResult(success=True, message_id=f"sent-{len(self.sent)}")

    async def send_typing(self, chat_id, metadata=None) -> None:
        self.typing.append({"chat_id": chat_id, "metadata": metadata})

    async def stop_typing(self, chat_id) -> None:
        return None

    async def get_chat_info(self, chat_id: str):
        return {"id": chat_id}


class StubAgent:
    calls = []

    def __init__(self, **kwargs):
        self.tools = []
        self.tool_progress_callback = kwargs.get("tool_progress_callback")

    def run_conversation(self, message, conversation_history=None, task_id=None, **kwargs):
        type(self).calls.append(message)
        return {
            "final_response": f"done-{len(type(self).calls)}",
            "messages": [],
            "api_calls": 1,
        }


class StubStreamConsumer:
    """Records creation; delivers nothing, confirms nothing."""

    instances = []

    def __init__(self, **kwargs):
        self.final_response_sent = False
        self.already_sent = False
        self.message_id = None
        self._turn_split_delivery = False
        type(self).instances.append(self)

    async def run(self):
        return None

    def finish(self):
        return None

    def on_delta(self, text):
        return None

    def on_commentary(self, text):
        return None

    def on_segment_break(self):
        return None


def _make_runner(adapter):
    gateway_run = importlib.import_module("gateway.run")
    runner = object.__new__(gateway_run.GatewayRunner)
    runner.adapters = {adapter.platform: adapter}
    runner._voice_mode = {}
    runner._prefill_messages = []
    runner._ephemeral_system_prompt = ""
    runner._reasoning_config = None
    runner._provider_routing = {}
    runner._fallback_model = None
    runner._session_db = None
    runner._running_agents = {}
    runner._session_run_generation = {}
    runner.hooks = SimpleNamespace(loaded_hooks=False)
    runner.config = SimpleNamespace(
        thread_sessions_per_user=False,
        group_sessions_per_user=False,
        stt_enabled=False,
        get_home_channel=lambda platform: SimpleNamespace(chat_id=HOME_CHAT),
    )
    runner._model = "openai/gpt-4.1-mini"
    runner._base_url = None
    runner._decide_image_input_mode = lambda **_kw: "native"
    return runner


def _install_stubs(monkeypatch, tmp_path):
    StubAgent.calls = []
    StubStreamConsumer.instances = []

    fake_dotenv = types.ModuleType("dotenv")
    fake_dotenv.load_dotenv = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "dotenv", fake_dotenv)

    fake_run_agent = types.ModuleType("run_agent")
    fake_run_agent.AIAgent = StubAgent
    monkeypatch.setitem(sys.modules, "run_agent", fake_run_agent)

    gateway_run = importlib.import_module("gateway.run")
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run, "_resolve_runtime_agent_kwargs", lambda: {"api_key": "***"})
    monkeypatch.setattr(
        gateway_run,
        "_load_gateway_config",
        lambda: {"display": {"quiet_channels": [QUIET_CHAT]}},
    )

    import gateway.stream_consumer as stream_consumer_mod

    monkeypatch.setattr(stream_consumer_mod, "GatewayStreamConsumer", StubStreamConsumer)
    return gateway_run


def _source(user_id):
    return SessionSource(
        platform=Platform.SLACK,
        chat_id=QUIET_CHAT,
        chat_type="group",
        user_id=user_id,
    )


def _event(text, source, metadata=None):
    return MessageEvent(
        text=text,
        message_type=MessageType.TEXT,
        source=source,
        message_id=f"m-{text[:6]}",
        metadata=metadata or {},
    )


async def _run_interrupted_turn(monkeypatch, tmp_path, *, user_id, event_metadata=None):
    """First turn in the quiet channel completes with a follow-up queued, so
    the queued-follow-up branch delivers the first turn's response."""
    gateway_run = _install_stubs(monkeypatch, tmp_path)
    monkeypatch.setenv("SLACK_ALLOWED_USERS", OPERATOR_ID)

    adapter = CaptureAdapter()
    runner = _make_runner(adapter)

    source = _source(user_id)
    event = _event("operator first", source, metadata=event_metadata)
    followup_source = _source(user_id)
    adapter._pending_messages[SESSION_KEY] = _event("operator followup", followup_source)

    result = await runner._run_agent(
        message="operator first",
        context_prompt="",
        history=[],
        source=source,
        session_id="sess-stream-leak",
        session_key=SESSION_KEY,
        event=event,
    )
    return adapter, result


@pytest.mark.asyncio
async def test_operator_unaddressed_quiet_channel_reroutes_first_response_home(
    monkeypatch, tmp_path
):
    """Operator post (not addressed to the bot) in a quiet channel, followed
    by a second post mid-turn: no send to the quiet channel, one send to the
    platform home channel with the bookkeeping prefix."""
    adapter, result = await _run_interrupted_turn(monkeypatch, tmp_path, user_id=OPERATOR_ID)

    quiet_sends = [s for s in adapter.sent if s["chat_id"] == QUIET_CHAT]
    home_sends = [s for s in adapter.sent if s["chat_id"] == HOME_CHAT]
    assert quiet_sends == [], f"leaked into quiet channel: {quiet_sends}"
    assert len(home_sends) == 1
    assert "operator bookkeeping" in home_sends[0]["content"]
    assert "done-1" in home_sends[0]["content"]
    assert QUIET_CHAT in home_sends[0]["content"]
    # The queued follow-up itself still ran.
    assert result["final_response"] == "done-2"
    assert len(StubAgent.calls) == 2


@pytest.mark.asyncio
async def test_failed_home_send_drops_in_channel_copy(monkeypatch, tmp_path, caplog):
    """Fail closed: when the predicate is true and the home-channel send
    raises, nothing may go to the quiet channel; a warning names the
    session key."""
    import logging

    gateway_run = _install_stubs(monkeypatch, tmp_path)
    monkeypatch.setenv("SLACK_ALLOWED_USERS", OPERATOR_ID)

    adapter = CaptureAdapter()
    adapter.fail_chat_ids.add(HOME_CHAT)
    runner = _make_runner(adapter)

    source = _source(OPERATOR_ID)
    event = _event("operator first", source)
    adapter._pending_messages[SESSION_KEY] = _event("operator followup", _source(OPERATOR_ID))

    with caplog.at_level(logging.WARNING):
        result = await runner._run_agent(
            message="operator first",
            context_prompt="",
            history=[],
            source=source,
            session_id="sess-stream-leak-failclosed",
            session_key=SESSION_KEY,
            event=event,
        )

    quiet_sends = [s for s in adapter.sent if s["chat_id"] == QUIET_CHAT]
    assert quiet_sends == [], f"leaked into quiet channel: {quiet_sends}"
    assert [s for s in adapter.sent if s["chat_id"] == HOME_CHAT] == []
    assert any(
        "Home-channel reroute failed" in r.getMessage() and SESSION_KEY in r.getMessage()
        for r in caplog.records
    )
    # The queued follow-up itself still ran.
    assert result["final_response"] == "done-2"
    assert len(StubAgent.calls) == 2


@pytest.mark.asyncio
async def test_missing_home_channel_drops_in_channel_copy(monkeypatch, tmp_path, caplog):
    """Fail closed: with no home channel configured, the reroute drops the
    response instead of delivering it into the quiet channel."""
    import logging

    gateway_run = _install_stubs(monkeypatch, tmp_path)
    monkeypatch.setenv("SLACK_ALLOWED_USERS", OPERATOR_ID)

    adapter = CaptureAdapter()
    runner = _make_runner(adapter)
    runner.config.get_home_channel = lambda platform: None

    source = _source(OPERATOR_ID)
    event = _event("operator first", source)
    adapter._pending_messages[SESSION_KEY] = _event("operator followup", _source(OPERATOR_ID))

    with caplog.at_level(logging.WARNING):
        result = await runner._run_agent(
            message="operator first",
            context_prompt="",
            history=[],
            source=source,
            session_id="sess-stream-leak-nohome",
            session_key=SESSION_KEY,
            event=event,
        )

    assert adapter.sent == [], f"nothing may be sent, got: {adapter.sent}"
    assert any(
        "No home channel configured" in r.getMessage() and SESSION_KEY in r.getMessage()
        for r in caplog.records
    )
    assert result["final_response"] == "done-2"


@pytest.mark.asyncio
async def test_counterparty_post_keeps_in_channel_delivery(monkeypatch, tmp_path):
    """Counterparty post in the same quiet channel, same interruption: the
    first response is delivered in-channel as before."""
    adapter, result = await _run_interrupted_turn(
        monkeypatch, tmp_path, user_id=COUNTERPARTY_ID
    )

    quiet_sends = [s for s in adapter.sent if s["chat_id"] == QUIET_CHAT]
    home_sends = [s for s in adapter.sent if s["chat_id"] == HOME_CHAT]
    assert len(quiet_sends) == 1
    assert quiet_sends[0]["content"] == "done-1"
    assert home_sends == []
    assert result["final_response"] == "done-2"


@pytest.mark.asyncio
async def test_bot_addressed_operator_post_keeps_in_channel_delivery(
    monkeypatch, tmp_path
):
    """Operator post addressed to the bot in a quiet channel keeps normal
    in-channel delivery (the reroute only covers unaddressed bookkeeping)."""
    adapter, result = await _run_interrupted_turn(
        monkeypatch,
        tmp_path,
        user_id=OPERATOR_ID,
        event_metadata={"addressed_bot": True},
    )

    quiet_sends = [s for s in adapter.sent if s["chat_id"] == QUIET_CHAT]
    home_sends = [s for s in adapter.sent if s["chat_id"] == HOME_CHAT]
    assert len(quiet_sends) == 1
    assert quiet_sends[0]["content"] == "done-1"
    assert home_sends == []
    assert result["final_response"] == "done-2"


@pytest.mark.asyncio
async def test_suppress_streaming_blocks_interim_consumer(monkeypatch, tmp_path):
    """suppress_streaming (operator unaddressed in a quiet channel) must hold
    back the interim-message consumer too: with streaming off and interim
    messages enabled, no stream consumer may be created at all."""
    gateway_run = _install_stubs(monkeypatch, tmp_path)
    monkeypatch.setenv("SLACK_ALLOWED_USERS", OPERATOR_ID)

    adapter = CaptureAdapter()
    runner = _make_runner(adapter)
    runner.config.streaming = SimpleNamespace(
        enabled=False, transport="off", cursor="", edit_interval=1.0,
        buffer_threshold=1, fresh_final_after_seconds=0,
    )

    source = _source(OPERATOR_ID)
    result = await runner._run_agent(
        message="operator first",
        context_prompt="",
        history=[],
        source=source,
        session_id="sess-stream-leak-interim",
        session_key=SESSION_KEY,
        event=_event("operator first", source),
        suppress_streaming=True,
    )

    assert result["final_response"] == "done-1"
    assert StubStreamConsumer.instances == []


@pytest.mark.asyncio
async def test_interim_consumer_created_without_suppression(monkeypatch, tmp_path):
    """Control: the same setup without suppress_streaming does create the
    interim consumer, so the previous test can actually fail."""
    gateway_run = _install_stubs(monkeypatch, tmp_path)
    monkeypatch.setenv("SLACK_ALLOWED_USERS", OPERATOR_ID)

    adapter = CaptureAdapter()
    runner = _make_runner(adapter)
    runner.config.streaming = SimpleNamespace(
        enabled=False, transport="off", cursor="", edit_interval=1.0,
        buffer_threshold=1, fresh_final_after_seconds=0,
    )

    source = _source(OPERATOR_ID)
    result = await runner._run_agent(
        message="operator first",
        context_prompt="",
        history=[],
        source=source,
        session_id="sess-stream-leak-interim-control",
        session_key=SESSION_KEY,
        event=_event("operator first", source),
    )

    assert result["final_response"] == "done-1"
    assert len(StubStreamConsumer.instances) == 1
