"""Never-silent ack: directly-addressed turns must never end in total silence.

Covers the three pieces of the contract:

* ``gateway.run._never_silent_ack_response`` — the intentional-silence marker
  (NO_REPLY/[SILENT]) is replaced with a minimal visible ack when the user
  directly addressed the bot, and keeps suppressing outbound text for
  passive free-response ingestion.
* ``gateway.run._event_addressed_bot`` / ``_never_silent_ack_enabled`` —
  direct-address resolution and the config opt-out.
* Slack adapter inbound marking — ``metadata["addressed_bot"]`` is set on
  1:1 DMs, @mentions, and bot-thread replies, and left False for passive
  free-response channel traffic.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.run import (
    _NEVER_SILENT_ACK_TEXT,
    _event_addressed_bot,
    _never_silent_ack_enabled,
    _never_silent_ack_response,
)


def _event(metadata=None, internal=False):
    return SimpleNamespace(metadata=metadata or {}, internal=internal)


def _source(chat_type="group"):
    return SimpleNamespace(chat_type=chat_type)


class TestEventAddressedBot:
    def test_metadata_true_marks_direct_address(self):
        assert _event_addressed_bot(_event({"addressed_bot": True}), _source()) is True

    def test_metadata_false_marks_passive(self):
        assert _event_addressed_bot(_event({"addressed_bot": False}), _source()) is False

    def test_unmarked_dm_counts_as_direct_address(self):
        assert _event_addressed_bot(_event(), _source("dm")) is True

    def test_unmarked_group_does_not_count(self):
        # Conservative fallback: platforms that never mark the event keep
        # intentional silence available in group channels.
        assert _event_addressed_bot(_event(), _source("group")) is False

    def test_missing_metadata_attr_is_safe(self):
        assert _event_addressed_bot(SimpleNamespace(), _source("group")) is False


class TestNeverSilentAckEnabled:
    def test_default_is_enabled(self):
        assert _never_silent_ack_enabled({}) is True
        assert _never_silent_ack_enabled(None) is True
        assert _never_silent_ack_enabled({"gateway": {}}) is True

    def test_config_opt_out(self):
        assert _never_silent_ack_enabled({"gateway": {"never_silent_ack": False}}) is False

    @pytest.mark.parametrize("raw", ["false", "0", "no", "off", "False", " OFF "])
    def test_string_opt_outs(self, raw):
        assert _never_silent_ack_enabled({"gateway": {"never_silent_ack": raw}}) is False

    @pytest.mark.parametrize("raw", ["true", "1", "yes", "on"])
    def test_string_opt_ins(self, raw):
        assert _never_silent_ack_enabled({"gateway": {"never_silent_ack": raw}}) is True

    def test_malformed_config_stays_enabled(self):
        assert _never_silent_ack_enabled({"gateway": None}) is True


class TestNeverSilentAckResponse:
    def test_direct_address_gets_visible_ack(self):
        out = _never_silent_ack_response(
            _event({"addressed_bot": True}), _source(), {},
        )
        assert out == _NEVER_SILENT_ACK_TEXT
        assert out.strip()  # never empty for a direct address

    def test_passive_free_response_stays_silent(self):
        assert _never_silent_ack_response(
            _event({"addressed_bot": False}), _source(), {},
        ) == ""

    def test_config_opt_out_restores_legacy_silence(self):
        cfg = {"gateway": {"never_silent_ack": False}}
        assert _never_silent_ack_response(
            _event({"addressed_bot": True}), _source(), cfg,
        ) == ""

    def test_internal_synthetic_event_never_acked(self):
        # Background-process notifications and other synthetic turns are not
        # user addresses; an ack would surface as unprompted channel noise.
        assert _never_silent_ack_response(
            _event({"addressed_bot": True}, internal=True), _source(), {},
        ) == ""


# ---------------------------------------------------------------------------
# Slack adapter inbound marking
# ---------------------------------------------------------------------------


def _make_slack_adapter():
    from gateway.config import PlatformConfig
    from plugins.platforms.slack.adapter import SlackAdapter

    config = PlatformConfig(enabled=True, token="***")
    adapter = SlackAdapter(config)
    adapter._app = MagicMock()
    adapter._app.client = AsyncMock()
    adapter._app.client.users_info = AsyncMock(
        return_value={
            "user": {
                "is_bot": False,
                "profile": {"display_name": "Test User"},
                "real_name": "Test User",
            }
        }
    )
    adapter._bot_user_id = "U_BOT"
    adapter._team_bot_user_ids = {"T_TEAM": "U_BOT"}
    adapter._running = True
    adapter.handle_message = AsyncMock()
    store = MagicMock()
    store.get_or_create_session.return_value = SimpleNamespace(session_id="s1")
    adapter.set_session_store(store)
    return adapter


@pytest.mark.asyncio
async def test_slack_mention_marks_addressed_bot():
    adapter = _make_slack_adapter()
    await adapter._handle_slack_message(
        {
            "text": "<@U_BOT> ping",
            "channel": "C_CHAN",
            "channel_type": "channel",
            "ts": "171.111",
            "user": "U_USER",
            "team_id": "T_TEAM",
        }
    )

    msg_event = adapter.handle_message.await_args.args[0]
    assert msg_event.metadata["addressed_bot"] is True


@pytest.mark.asyncio
async def test_slack_one_to_one_dm_marks_addressed_bot():
    adapter = _make_slack_adapter()
    await adapter._handle_slack_message(
        {
            "text": "hello",
            "channel": "D123",
            "channel_type": "im",
            "ts": "171.222",
            "user": "U_USER",
            "team_id": "T_TEAM",
        }
    )

    msg_event = adapter.handle_message.await_args.args[0]
    assert msg_event.metadata["addressed_bot"] is True


@pytest.mark.asyncio
async def test_slack_bot_thread_reply_marks_addressed_bot():
    adapter = _make_slack_adapter()
    adapter._bot_message_ts.add("171.000")  # bot's earlier reply in this thread
    await adapter._handle_slack_message(
        {
            "text": "following up",
            "channel": "C_CHAN",
            "channel_type": "channel",
            "ts": "171.333",
            "thread_ts": "171.000",
            "user": "U_USER",
            "team_id": "T_TEAM",
        }
    )

    msg_event = adapter.handle_message.await_args.args[0]
    assert msg_event.metadata["addressed_bot"] is True


@pytest.mark.asyncio
async def test_slack_free_response_passive_message_not_marked():
    adapter = _make_slack_adapter()
    adapter.config.extra["free_response_channels"] = ["C_CHAN"]
    await adapter._handle_slack_message(
        {
            "text": "unaddressed channel chatter",
            "channel": "C_CHAN",
            "channel_type": "channel",
            "ts": "171.444",
            "user": "U_USER",
            "team_id": "T_TEAM",
        }
    )

    msg_event = adapter.handle_message.await_args.args[0]
    assert msg_event.metadata["addressed_bot"] is False
