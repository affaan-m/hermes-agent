"""Tests for one-shot DM re-resolution in the Slack adapter send path.

Slack Connect churn can recreate a DM: every chat.postMessage to the old
D-id then fails channel_not_found forever (observed 2026-09-19 22:44 ET,
MPDM D0BV2ENS5AS). The adapter re-resolves once through conversations.open
from the user id(s) inbound traffic taught it, retries the post once,
caches the dead-to-fresh mapping for the process lifetime, and fails
closed otherwise. C- and G-channels are never touched.
"""

import logging
from types import SimpleNamespace

import pytest
from slack_sdk.errors import SlackApiError

from gateway.config import PlatformConfig
from plugins.platforms.slack.adapter import SlackAdapter

OLD_DM = "D0BOLD"
NEW_DM = "D0BNEW"
USER_IDS = {"U0BPARAM", "U0BHARBOR"}


class FakeSlackClient:
    def __init__(self, *, fail_channels=None, open_channel=NEW_DM, open_error=None):
        self.posts = []
        self.opens = []
        self.fail_channels = fail_channels or {}
        self.open_channel = open_channel
        self.open_error = open_error

    async def chat_postMessage(self, **kwargs):
        self.posts.append(kwargs)
        code = self.fail_channels.get(kwargs["channel"])
        if code:
            raise SlackApiError("rejected", {"ok": False, "error": code})
        return {"ok": True, "ts": f"111.{len(self.posts)}", "channel": kwargs["channel"]}

    async def conversations_open(self, users=None):
        self.opens.append(users)
        if self.open_error is not None:
            raise self.open_error
        return {"ok": True, "channel": {"id": self.open_channel}}


def _make_adapter(client):
    adapter = SlackAdapter(PlatformConfig(enabled=True, token="xoxb-fake-token"))
    adapter._app = SimpleNamespace()  # truthy: "connected"
    adapter._bot_user_id = "U0BBOT"
    adapter._get_client = lambda chat_id, team_id=None: client
    return adapter


@pytest.mark.asyncio
async def test_rejected_dm_reresolves_and_delivers(caplog):
    """channel_not_found on a D-channel with known user ids: one
    conversations.open, one retry to the fresh channel, delivery succeeds,
    mapping cached, log line carries ids only."""
    client = FakeSlackClient(fail_channels={OLD_DM: "channel_not_found"})
    adapter = _make_adapter(client)
    adapter._dm_channel_users[OLD_DM] = set(USER_IDS)

    with caplog.at_level(logging.INFO):
        result = await adapter.send(OLD_DM, "hello")

    assert result.success is True
    assert [p["channel"] for p in client.posts] == [OLD_DM, NEW_DM]
    assert client.opens == ["U0BHARBOR,U0BPARAM"]  # sorted, comma-joined
    assert adapter._dm_reresolved[OLD_DM] == NEW_DM
    assert any(
        f"re-resolved DM {OLD_DM} -> {NEW_DM}" in r.getMessage() for r in caplog.records
    )


@pytest.mark.asyncio
async def test_reresolved_mapping_applies_to_later_sends():
    """A second send to the dead D-id goes straight to the fresh channel:
    no failed post, no second conversations.open."""
    client = FakeSlackClient(fail_channels={OLD_DM: "channel_not_found"})
    adapter = _make_adapter(client)
    adapter._dm_channel_users[OLD_DM] = set(USER_IDS)

    first = await adapter.send(OLD_DM, "one")
    assert first.success is True
    posts_before = len(client.posts)

    second = await adapter.send(OLD_DM, "two")
    assert second.success is True
    assert [p["channel"] for p in client.posts[posts_before:]] == [NEW_DM]
    assert len(client.opens) == 1


@pytest.mark.asyncio
async def test_rejection_without_user_ids_stays_failure():
    """No known user ids for the DM: the send stays a classified failure,
    no conversations.open, no retry."""
    client = FakeSlackClient(fail_channels={OLD_DM: "channel_not_found"})
    adapter = _make_adapter(client)

    result = await adapter.send(OLD_DM, "hello")

    assert result.success is False
    assert result.error_kind == "not_found"
    assert [p["channel"] for p in client.posts] == [OLD_DM]
    assert client.opens == []


@pytest.mark.asyncio
async def test_open_failure_keeps_classified_failure():
    """conversations.open itself failing keeps today's fail-closed
    behaviour: one post, no retry, classified failure."""
    client = FakeSlackClient(
        fail_channels={OLD_DM: "channel_not_found"},
        open_error=SlackApiError("no users", {"ok": False, "error": "users_not_found"}),
    )
    adapter = _make_adapter(client)
    adapter._dm_channel_users[OLD_DM] = set(USER_IDS)

    result = await adapter.send(OLD_DM, "hello")

    assert result.success is False
    assert result.error_kind == "not_found"
    assert [p["channel"] for p in client.posts] == [OLD_DM]
    assert client.opens == ["U0BHARBOR,U0BPARAM"]


@pytest.mark.asyncio
async def test_non_dm_channel_never_reresolved():
    """channel_not_found on a C-channel is never re-resolved, even with
    DM user knowledge present: one post, classified failure, no open."""
    client = FakeSlackClient(fail_channels={"C0BCHAN": "channel_not_found"})
    adapter = _make_adapter(client)
    adapter._dm_channel_users[OLD_DM] = set(USER_IDS)

    result = await adapter.send("C0BCHAN", "hello")

    assert result.success is False
    assert result.error_kind == "not_found"
    assert [p["channel"] for p in client.posts] == ["C0BCHAN"]
    assert client.opens == []


@pytest.mark.asyncio
async def test_other_error_codes_stay_failures():
    """A different Slack error on a D-channel (not_in_channel) is not a
    re-resolution candidate."""
    client = FakeSlackClient(fail_channels={OLD_DM: "not_in_channel"})
    adapter = _make_adapter(client)
    adapter._dm_channel_users[OLD_DM] = set(USER_IDS)

    result = await adapter.send(OLD_DM, "hello")

    assert result.success is False
    assert [p["channel"] for p in client.posts] == [OLD_DM]
    assert client.opens == []


def test_note_dm_channel_user_learns_human_ids_only():
    """Inbound bookkeeping learns D-channel human ids and skips the bot
    itself and non-DM channels."""
    adapter = _make_adapter(FakeSlackClient())
    adapter._note_dm_channel_user(OLD_DM, "U0BPARAM")
    adapter._note_dm_channel_user(OLD_DM, "U0BBOT")  # the bot itself
    adapter._note_dm_channel_user("C0BCHAN", "U0BPARAM")  # not a DM
    adapter._note_dm_channel_user(OLD_DM, "")
    assert adapter._dm_channel_users == {OLD_DM: {"U0BPARAM"}}
