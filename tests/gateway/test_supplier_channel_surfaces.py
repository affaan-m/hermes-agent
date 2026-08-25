"""Supplier-desk channel surfaces: chat-scoped Slack authorization and
display.quiet_channels suppression.

Contract:
* SLACK_GROUP_ALLOWED_CHATS authorizes any member posting in a designated
  channel (mirrors TELEGRAM_GROUP_ALLOWED_CHATS) so counterparties can talk
  to the desk without being individually allowlisted.
* display.quiet_channels mutes the working display surface (reasoning
  prepend, progress bubbles, status notices) while final answers still land.
"""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import gateway.run as gateway_run
from gateway.authz_mixin import GatewayAuthorizationMixin
from gateway.config import GatewayConfig, Platform
from gateway.platforms.base import MessageEvent
from gateway.run import _quiet_channel_ids
from gateway.session import SessionEntry, SessionSource


def _slack_group_source(chat_id="C_SUPPLIER", user_id="U_SUPPLIER"):
    return SessionSource(
        platform=Platform.SLACK,
        chat_id=chat_id,
        chat_type="group",
        user_id=user_id,
    )


def _runner():
    runner = gateway_run.GatewayRunner(GatewayConfig())
    runner.pairing_store = None
    runner.pairing_stores = {}
    return runner


class TestSlackGroupAllowedChats:
    def test_listed_channel_authorizes_any_member(self, monkeypatch):
        monkeypatch.setenv("SLACK_GROUP_ALLOWED_CHATS", "C_SUPPLIER")
        assert _runner()._is_user_authorized(_slack_group_source()) is True

    def test_unlisted_channel_stays_denied(self, monkeypatch):
        monkeypatch.setenv("SLACK_GROUP_ALLOWED_CHATS", "C_SUPPLIER")
        monkeypatch.delenv("SLACK_ALLOWED_USERS", raising=False)
        assert _runner()._is_user_authorized(
            _slack_group_source(chat_id="C_OTHER")
        ) is False

    def test_wildcard_authorizes_any_channel(self, monkeypatch):
        monkeypatch.setenv("SLACK_GROUP_ALLOWED_CHATS", "*")
        assert _runner()._is_user_authorized(
            _slack_group_source(chat_id="C_ANY")
        ) is True

    def test_dm_not_covered_by_chat_allowlist(self, monkeypatch):
        monkeypatch.setenv("SLACK_GROUP_ALLOWED_CHATS", "*")
        monkeypatch.delenv("SLACK_ALLOWED_USERS", raising=False)
        source = SessionSource(
            platform=Platform.SLACK, chat_id="D123", chat_type="dm", user_id="U_X",
        )
        assert _runner()._is_user_authorized(source) is False


class TestQuietChannelIds:
    def test_parses_list(self):
        assert _quiet_channel_ids({"display": {"quiet_channels": ["C1", " C2 "]}}) == {"C1", "C2"}

    def test_missing_or_malformed_is_empty(self):
        assert _quiet_channel_ids({}) == set()
        assert _quiet_channel_ids(None) == set()
        assert _quiet_channel_ids({"display": {"quiet_channels": "oops"}}) == set()


# ---------------------------------------------------------------------------
# Reasoning prepend suppression through the real epilogue
# ---------------------------------------------------------------------------


def _source(chat_id="-1001"):
    return SessionSource(
        platform=Platform.TELEGRAM,
        chat_id=chat_id,
        chat_type="group",
        user_id="12345",
    )


def _event(source):
    return MessageEvent(text="question", source=source, message_id="msg-1")


def _epilogue_runner(monkeypatch, tmp_path):
    runner = gateway_run.GatewayRunner(GatewayConfig())
    runner.adapters = {}
    runner._running_agents = {}
    runner._running_agents_ts = {}
    runner._pending_messages = {}
    runner._pending_approvals = {}
    runner._is_user_authorized = lambda _source: True
    runner._set_session_env = lambda _context: None
    runner._handle_active_session_busy_message = AsyncMock(return_value=False)
    runner._session_db = MagicMock()
    runner._recover_telegram_topic_thread_id = lambda _source: None
    runner._cache_session_source = lambda _key, _source: None
    runner._is_session_run_current = lambda _key, _gen: True
    runner._reply_anchor_for_event = lambda _event: None
    runner._get_guild_id = lambda _event: None
    runner._should_send_voice_reply = lambda *_a, **_kw: False
    runner.hooks = MagicMock()
    runner.hooks.emit = AsyncMock()
    runner.session_store = MagicMock()
    runner.session_store.get_or_create_session.return_value = SessionEntry(
        session_key="agent:main:telegram:group:-1001:12345",
        session_id="sess-quiet",
        created_at=datetime.now(),
        updated_at=datetime.now(),
        platform=Platform.TELEGRAM,
        chat_type="group",
    )
    runner.session_store.load_transcript.return_value = []
    runner.session_store.append_to_transcript = MagicMock()
    runner.session_store.update_session = MagicMock()
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(
        gateway_run, "_resolve_runtime_agent_kwargs", lambda: {"api_key": "fake"}
    )
    monkeypatch.setattr(
        "agent.model_metadata.get_model_context_length",
        lambda *_args, **_kwargs: 100_000,
    )
    runner._run_agent = AsyncMock(return_value={
        "final_response": "The answer is 42.",
        "messages": [
            {"role": "user", "content": "question"},
            {"role": "assistant", "content": "The answer is 42."},
        ],
        "tools": [],
        "history_offset": 0,
        "last_prompt_tokens": 0,
        "api_calls": 1,
        "failed": False,
        "last_reasoning": "Let me think through the pricing math carefully.",
    })
    return runner


@pytest.mark.asyncio
async def test_reasoning_prepend_appears_in_normal_channel(monkeypatch, tmp_path):
    (tmp_path / "config.yaml").write_text("display:\n  show_reasoning: true\n")
    runner = _epilogue_runner(monkeypatch, tmp_path)

    response = await runner._handle_message_with_agent(
        _event(_source()), _source(), "agent:main:telegram:group:-1001:12345", 1
    )

    assert "Reasoning:" in response
    assert "The answer is 42." in response


@pytest.mark.asyncio
async def test_reasoning_prepend_suppressed_in_quiet_channel(monkeypatch, tmp_path):
    (tmp_path / "config.yaml").write_text(
        "display:\n  show_reasoning: true\n  quiet_channels:\n    - '-1001'\n"
    )
    runner = _epilogue_runner(monkeypatch, tmp_path)

    response = await runner._handle_message_with_agent(
        _event(_source()), _source(), "agent:main:telegram:group:-1001:12345", 1
    )

    assert "Reasoning:" not in response
    assert response == "The answer is 42."
