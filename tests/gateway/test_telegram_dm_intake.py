"""Tests for the Telegram DM intake path (dm_policy=allowlist).

A DM from a non-allowlisted user is never answered, but exactly one
operator notice per sender per day is posted to the configured ops chat
and topic.  The operator-only /allow_dm <user_id> command appends the id
to the runtime allowlist file the adapter reads, admitting that sender
without a restart.
"""
import asyncio
import json
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import MessageType  # noqa: F401  (parity with sibling tests)


def _make_adapter(tmp_path, monkeypatch, *, allowed_env="111,222", extra_overrides=None,
                  home_channel=None):
    from plugins.platforms.telegram.adapter import TelegramAdapter

    monkeypatch.setenv("TELEGRAM_ALLOWED_USERS", allowed_env)

    extra = {
        "dm_intake_allowlist_file": str(tmp_path / "telegram_dm_allowlist.json"),
        "dm_intake_notice_file": str(tmp_path / "telegram_dm_intake_notices.json"),
        "dm_intake_notice": {"chat_id": "-1003795075025", "topic_id": "8072"},
    }
    if extra_overrides:
        extra.update(extra_overrides)

    adapter = object.__new__(TelegramAdapter)
    adapter.platform = Platform.TELEGRAM
    adapter.config = PlatformConfig(
        enabled=True, token="fake-token", extra=extra, home_channel=home_channel,
    )
    adapter._bot = AsyncMock()
    adapter._bot.id = 999
    adapter._bot.username = "test_bot"
    adapter._message_handler = AsyncMock()
    adapter._pending_text_batches = {}
    adapter._pending_text_batch_tasks = {}
    adapter._text_batch_delay_seconds = 0.01
    adapter._text_batch_split_delay_seconds = 0.01
    adapter._mention_patterns = adapter._compile_mention_patterns()
    adapter._forum_lock = asyncio.Lock()
    adapter._forum_command_registered = set()
    adapter._active_sessions = {}
    adapter._pending_messages = {}
    adapter._dm_intake_notices = None
    return adapter


def _make_dm(text="hello", *, from_user_id=555, username="supplier_jane"):
    return SimpleNamespace(
        message_id=42,
        text=text,
        caption=None,
        entities=[],
        caption_entities=[],
        message_thread_id=None,
        is_topic_message=False,
        chat=SimpleNamespace(id=from_user_id, type="private", title=None, is_forum=False),
        from_user=SimpleNamespace(
            id=from_user_id, username=username, full_name="Supplier Jane",
            first_name="Supplier",
        ),
        reply_to_message=None,
        date=None,
        location=None,
        photo=None,
        video=None,
        audio=None,
        voice=None,
        document=None,
        sticker=None,
        media_group_id=None,
    )


def _update(msg):
    return SimpleNamespace(update_id=1, message=msg, effective_message=None)


def _sends_to(adapter, chat_id):
    return [
        c for c in adapter._bot.send_message.await_args_list
        if str(c.kwargs.get("chat_id")) == str(chat_id)
    ]


@pytest.mark.asyncio
async def test_unknown_dm_produces_one_notice_and_no_reply(tmp_path, monkeypatch):
    """A blocked DM posts one operator notice; the sender gets nothing."""
    adapter = _make_adapter(tmp_path, monkeypatch)

    await adapter._handle_text_message(_update(_make_dm()), SimpleNamespace())

    notices = _sends_to(adapter, "-1003795075025")
    assert len(notices) == 1
    kwargs = notices[0].kwargs
    assert kwargs["message_thread_id"] == 8072
    body = kwargs["text"]
    assert "user_id: 555" in body
    assert "username: supplier_jane" in body
    assert "hello" in body
    assert "/allow_dm 555" in body
    # No reply to the sender's DM chat (chat id 555).
    assert _sends_to(adapter, "555") == []


@pytest.mark.asyncio
async def test_notice_preview_truncated_at_200_chars(tmp_path, monkeypatch):
    adapter = _make_adapter(tmp_path, monkeypatch)

    await adapter._handle_text_message(
        _update(_make_dm(text="x" * 500)), SimpleNamespace()
    )

    body = _sends_to(adapter, "-1003795075025")[0].kwargs["text"]
    preview_line = next(l for l in body.splitlines() if l.startswith("text: "))
    assert len(preview_line) == len("text: ") + 200


@pytest.mark.asyncio
async def test_second_dm_same_day_produces_nothing(tmp_path, monkeypatch):
    """The per-sender per-day dedupe suppresses the repeat notice."""
    adapter = _make_adapter(tmp_path, monkeypatch)

    await adapter._handle_text_message(_update(_make_dm()), SimpleNamespace())
    await adapter._handle_text_message(_update(_make_dm(text="are you there?")), SimpleNamespace())

    assert len(_sends_to(adapter, "-1003795075025")) == 1
    # Dedupe state persisted for crash/restart safety.
    state = json.loads((tmp_path / "telegram_dm_intake_notices.json").read_text())
    assert set(state) == {"555"}


@pytest.mark.asyncio
async def test_allow_dm_from_operator_admits_user(tmp_path, monkeypatch):
    """/allow_dm from an allowlisted operator admits the new sender."""
    adapter = _make_adapter(tmp_path, monkeypatch)

    # Operator 111 issues the command from their own DM with the bot.
    cmd = _make_dm(text="/allow_dm 555", from_user_id=111)
    cmd.chat = SimpleNamespace(id=111, type="private", title=None, is_forum=False)
    await adapter._handle_command(_update(cmd), SimpleNamespace())

    allowlist = json.loads((tmp_path / "telegram_dm_allowlist.json").read_text())
    assert "555" in allowlist
    assert "555" in os.environ["TELEGRAM_ALLOWED_USERS"].split(",")

    # The operator got a confirmation, not silence.
    replies = _sends_to(adapter, "111")
    assert len(replies) == 1
    assert "added 555" in replies[0].kwargs["text"]

    # The admitted user's next DM passes the prefilter: no notice, event built.
    adapter._bot.send_message.reset_mock()
    built = []
    original_build = adapter._build_message_event

    def track_build(*a, **kw):
        built.append(True)
        return original_build(*a, **kw)

    adapter._build_message_event = track_build
    adapter._enqueue_text_event = lambda event: None

    await adapter._handle_text_message(_update(_make_dm()), SimpleNamespace())

    assert built, "admitted user's DM should reach event building"
    assert _sends_to(adapter, "-1003795075025") == []


@pytest.mark.asyncio
async def test_allow_dm_from_non_operator_ignored(tmp_path, monkeypatch):
    """/allow_dm from a non-operator appends nothing and answers nothing."""
    adapter = _make_adapter(tmp_path, monkeypatch)

    # User 555 is not in TELEGRAM_ALLOWED_USERS. Put them in an allowed
    # group context so the message prefilter itself would pass (the chat is
    # authorized) and only the operator check can stop the command.
    monkeypatch.setenv("TELEGRAM_GROUP_ALLOWED_CHATS", "-100777")
    msg = _make_dm(text="/allow_dm 555", from_user_id=555)
    msg.chat = SimpleNamespace(id=-100777, type="supergroup", title="G", is_forum=False)

    await adapter._handle_command(_update(msg), SimpleNamespace())

    assert not (tmp_path / "telegram_dm_allowlist.json").exists()
    assert "555" not in os.environ["TELEGRAM_ALLOWED_USERS"].split(",")
    assert adapter._bot.send_message.await_args_list == []


@pytest.mark.asyncio
async def test_allow_dm_usage_error_replies_to_operator(tmp_path, monkeypatch):
    adapter = _make_adapter(tmp_path, monkeypatch)

    cmd = _make_dm(text="/allow_dm", from_user_id=222)
    cmd.chat = SimpleNamespace(id=222, type="private", title=None, is_forum=False)
    await adapter._handle_command(_update(cmd), SimpleNamespace())

    replies = _sends_to(adapter, "222")
    assert len(replies) == 1
    assert "Usage: /allow_dm" in replies[0].kwargs["text"]
    assert not (tmp_path / "telegram_dm_allowlist.json").exists()


@pytest.mark.asyncio
async def test_runtime_allowlist_merged_into_env_at_init(tmp_path, monkeypatch):
    """A gateway restart re-admits /allow_dm users via the persisted file."""
    (tmp_path / "telegram_dm_allowlist.json").write_text(json.dumps(["555"]))
    from plugins.platforms.telegram.adapter import TelegramAdapter

    monkeypatch.setenv("TELEGRAM_ALLOWED_USERS", "111")
    adapter = object.__new__(TelegramAdapter)
    adapter.config = PlatformConfig(
        enabled=True,
        token="fake-token",
        extra={"dm_intake_allowlist_file": str(tmp_path / "telegram_dm_allowlist.json")},
    )
    adapter._merge_runtime_dm_allowlist_into_env()

    assert os.environ["TELEGRAM_ALLOWED_USERS"] == "111,555"


@pytest.mark.asyncio
async def test_blocked_group_message_posts_no_notice(tmp_path, monkeypatch):
    """Group rejections stay log-only; the notice is DM-only."""
    adapter = _make_adapter(
        tmp_path, monkeypatch, extra_overrides={"allow_from": ["999"]},
    )
    msg = _make_dm(from_user_id=555)
    msg.chat = SimpleNamespace(id=-100888, type="supergroup", title="G", is_forum=False)

    await adapter._handle_text_message(_update(msg), SimpleNamespace())

    assert adapter._bot.send_message.await_args_list == []
