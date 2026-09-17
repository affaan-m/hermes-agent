"""Offline actual-adapter boundaries with synthetic destinations/transports."""
import asyncio
import inspect
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.message_audience import OutputClass
from gateway.message_failure import SAFE_FAILURE_TEXT, authorized_delivery
from gateway.platforms.base import BasePlatformAdapter, SendResult
from plugins.platforms.slack.adapter import SlackAdapter, _standalone_send


class Adapter(BasePlatformAdapter):
    def __init__(self, audience="external", platform=Platform.SLACK):
        config = PlatformConfig(enabled=True, extra={
            "workspace_id": "TONE", "message_audience": [
                {"platform": platform.value, "workspace_id": "TONE", "channel_id": "CSAME", "audience": audience},
            ],
        })
        super().__init__(config, platform)
        self.calls = []

    async def connect(self, **kwargs): return True
    async def disconnect(self): pass
    async def get_chat_info(self, chat_id): return {"id": chat_id}

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        self.calls.append((chat_id, content, metadata))
        return SendResult(success=True, message_id="sent")

    async def edit_message(self, chat_id, message_id, content, *, finalize=False, metadata=None):
        self.calls.append((chat_id, content, metadata, finalize))
        return SendResult(success=True, message_id=message_id)


@pytest.mark.parametrize("method", ["send", "edit_message"])
def test_real_subclass_overrides_apply_class_gate_and_preserve_signature(method):
    async def scenario():
        adapter = Adapter()
        kwargs = {"chat_id": "CSAME", "content": "synthetic diagnostic", "metadata": {
            "thread_id": "thread", "_hermes_output_class": OutputClass.PROGRESS,
        }}
        if method == "edit_message": kwargs.update(message_id="sent", finalize=True)
        result = await getattr(adapter, method)(**kwargs)
        assert not result.success and result.raw_response["suppressed"]
        assert adapter.calls == []
        kwargs["metadata"]["_hermes_output_class"] = OutputClass.SAFE_ERROR
        result = await getattr(adapter, method)(**kwargs)
        assert result.success
        assert adapter.calls[0][1] == SAFE_FAILURE_TEXT
        assert adapter.calls[0][2] == {"thread_id": "thread"}
        assert "_hermes_output_class" in kwargs["metadata"]  # Caller mapping preserved.
        if method == "edit_message":
            assert adapter.calls[0][3] is True
            assert "finalize" in inspect.signature(adapter.edit_message).parameters
            assert "metadata" in inspect.signature(adapter.edit_message).parameters
    asyncio.run(scenario())


@pytest.mark.parametrize("platform", [Platform.SLACK, Platform.TELEGRAM])
def test_final_answers_preserved_and_internal_progress_is_target_specific(platform):
    async def scenario():
        adapter = Adapter("private_operator", platform)
        result = await adapter.send("CSAME", "Supplier needs 20 TB of disk capacity.")
        assert result.success
        progress = {"_hermes_output_class": OutputClass.PROGRESS}
        assert (await adapter.send("CSAME", "Working on the requested check.", metadata=progress)).success
        assert not (await adapter.send("COTHER", "synthetic progress", metadata=progress)).success
        adapter._channel_team = {"CSAME": "TTWO"}
        assert not (await adapter.send("CSAME", "synthetic progress", metadata=progress)).success
        for kind in (OutputClass.REASONING, OutputClass.SECRET, OutputClass.RAW_PATH, OutputClass.RUNTIME_INTERNALS):
            assert not (await adapter.send("CSAME", "synthetic hidden details", metadata={"_hermes_output_class": kind})).success
        assert len(adapter.calls) == 2
    asyncio.run(scenario())


def slack_adapter():
    adapter = SlackAdapter(PlatformConfig(enabled=True, extra={"workspace_id": "TONE"}))
    adapter._bot_user_id = "UBOT"
    adapter._team_bot_user_ids = {"TONE": "UBOT"}
    adapter._primary_team_id = "TONE"  # Synthetic authenticated primary workspace.
    adapter._resolve_user_name = AsyncMock(return_value="Synthetic User")
    adapter._fetch_thread_context = AsyncMock(return_value="")
    adapter._fetch_thread_parent_text = AsyncMock(return_value="")
    adapter._reactions_enabled = lambda: False
    adapter.handle_message = AsyncMock()
    return adapter


@pytest.mark.parametrize("text,attachments", [
    ("<@UHUMAN> can you confirm capacity?", False),
    ("Any update?", False),
    ("", True),
])
def test_actual_slack_intake_ignores_human_thread_and_screenshot_traffic(text, attachments):
    async def scenario():
        adapter = slack_adapter()
        adapter._bot_message_ts.add("thread")
        adapter._mentioned_threads.add("thread")
        adapter._has_active_session_for_thread = lambda **kwargs: True
        event = {"channel": "CSAME", "channel_type": "channel", "team": "TONE", "ts": "2", "thread_ts": "thread", "user": "UHUMAN", "text": text}
        if attachments: event["files"] = [{"id": "FSCREEN", "mimetype": "image/png", "url_private": "https://example.invalid/synthetic.png"}]
        await adapter._handle_slack_message(event)
        adapter.handle_message.assert_not_called()
        adapter._resolve_user_name.assert_not_called()
        adapter._fetch_thread_context.assert_not_called()
    asyncio.run(scenario())


@pytest.mark.parametrize("text,channel_type", [("<@UBOT> confirm capacity", "channel"), ("/help", "channel"), ("Please help", "im")])
def test_actual_slack_explicit_requests_and_human_dm_continue(text, channel_type):
    async def scenario():
        adapter = slack_adapter()
        await adapter._handle_slack_message({"channel": "CSAME", "channel_type": channel_type, "team": "TONE", "ts": "2", "user": "UHUMAN", "text": text})
        adapter.handle_message.assert_awaited_once()
        event = adapter.handle_message.call_args.args[0]
        assert event._audience_decision.allow_model
        assert event.source.scope_id == "TONE"
        assert event.metadata["addressed_bot"]
    asyncio.run(scenario())


def test_scheduled_authorization_requires_complete_exact_trusted_destination():
    config = PlatformConfig(enabled=True, extra={"authorized_deliveries": [
        {"platform": "slack", "workspace_id": "TONE", "channel_id": "CSAME", "operator_requested": True},
    ]})
    assert authorized_delivery(config, "slack", "TONE", "CSAME")
    assert not authorized_delivery(config, "slack", "TTWO", "CSAME")
    assert not authorized_delivery(config, "slack", "", "CSAME")
    assert not authorized_delivery(config, "slack", "TONE", "COTHER")
    config.extra["authorized_deliveries"][0]["operator_requested"] = False
    assert not authorized_delivery(config, "slack", "TONE", "CSAME")


def test_standalone_slack_denies_before_token_lookup_or_network():
    result = asyncio.run(_standalone_send(PlatformConfig(enabled=True), "CSAME", "Synthetic scheduled text"))
    assert result["success"] is False and result["error"] == "delivery_not_authorized"


@pytest.mark.parametrize("method", ["send", "edit_message"])
def test_slack_real_transport_renders_safe_text_and_blocks_together(method):
    async def scenario():
        adapter = slack_adapter()
        client = SimpleNamespace(chat_postMessage=AsyncMock(return_value={"ts": "sent"}),
                                 chat_update=AsyncMock(return_value={"ok": True}))
        adapter._app = SimpleNamespace(client=client)
        adapter._team_clients = {"TONE": client}
        adapter._maybe_blocks = lambda text: [{"type": "section", "text": {"type": "mrkdwn", "text": text}}]
        adapter.stop_typing = AsyncMock()
        metadata = {"thread_id": "thread", "_hermes_output_class": OutputClass.SAFE_ERROR}
        kwargs = dict(chat_id="CSAME", content="synthetic private diagnostic", metadata=metadata)
        if method == "edit_message": kwargs.update(message_id="sent", finalize=True)
        result = await getattr(adapter, method)(**kwargs)
        assert result.success
        call = (client.chat_postMessage if method == "send" else client.chat_update).call_args.kwargs
        assert call["text"] == SAFE_FAILURE_TEXT
        assert call["blocks"][0]["text"]["text"] == SAFE_FAILURE_TEXT
        if method == "send": assert call["thread_ts"] == "thread"
        assert metadata["_hermes_output_class"] is OutputClass.SAFE_ERROR
    asyncio.run(scenario())


def test_slack_edit_clears_previous_blocks_when_replacing_with_plain_safe_error():
    async def scenario():
        adapter = slack_adapter()
        client = SimpleNamespace(chat_update=AsyncMock(return_value={"ok": True}))
        adapter._app = SimpleNamespace(client=client)
        adapter._team_clients = {"TONE": client}
        adapter._maybe_blocks = lambda text: None
        adapter.stop_typing = AsyncMock()
        result = await adapter.edit_message("CSAME", "sent", "synthetic diagnostic",
            finalize=True, metadata={"_hermes_output_class": OutputClass.SAFE_ERROR})
        assert result.success
        assert client.chat_update.call_args.kwargs["blocks"] == []
        assert client.chat_update.call_args.kwargs["text"] == SAFE_FAILURE_TEXT
    asyncio.run(scenario())


def test_slack_ephemeral_failure_does_not_fall_back_to_public_channel():
    async def scenario():
        adapter = slack_adapter()
        client = SimpleNamespace(chat_postMessage=AsyncMock())
        adapter._app = SimpleNamespace(client=client)
        adapter._team_clients = {"TONE": client}
        adapter._pop_slash_context = lambda chat_id, metadata=None: {"response_url": "https://example.invalid/fake"}
        adapter._send_slash_ephemeral = AsyncMock(return_value=SendResult(success=False, error="synthetic failure"))
        result = await adapter.send("CSAME", "synthetic diagnostic", metadata={"_hermes_output_class": OutputClass.SAFE_ERROR})
        assert not result.success
        client.chat_postMessage.assert_not_called()
        assert adapter._send_slash_ephemeral.call_args.args[1] == SAFE_FAILURE_TEXT
    asyncio.run(scenario())


def test_slack_forwarded_mention_is_content_not_participation_authorization():
    async def scenario():
        adapter = slack_adapter()
        await adapter._handle_slack_message({"channel": "CSAME", "channel_type": "channel", "team": "TONE", "ts": "2", "user": "UHUMAN", "text": "Shared reference", "attachments": [{"text": "<@UBOT> please inspect this"}]})
        adapter.handle_message.assert_not_called()
        adapter._resolve_user_name.assert_not_called()
    asyncio.run(scenario())


@pytest.mark.parametrize("text", ["> <@UBOT> please help", "`<@UBOT>`", "/stop@OTHERBOT", "!stop@OTHERBOT"])
def test_slack_quotes_code_and_other_bot_commands_do_not_authorize(text):
    async def scenario():
        adapter = slack_adapter()
        await adapter._handle_slack_message({"channel": "CSAME", "channel_type": "channel", "team": "TONE", "ts": "2", "user": "UHUMAN", "text": text})
        adapter.handle_message.assert_not_called()
    asyncio.run(scenario())


def test_direct_send_uses_existing_secret_redactor_before_rendering():
    async def scenario():
        adapter = Adapter()
        secret = "synthetic-value-for-boundary-test"
        assert (await adapter.send("CSAME", "password=" + secret)).success
        assert secret not in adapter.calls[0][1]
    asyncio.run(scenario())


@pytest.mark.parametrize("kind,text", [(OutputClass.FINAL, "Capacity is 20 TB."), (OutputClass.SAFE_ERROR, "synthetic runtime error"), (OutputClass.PROGRESS, "synthetic progress"), (OutputClass.FINAL, "https://fixture:synthetic-password@example.invalid/")])
def test_authorized_standalone_uses_shared_content_and_class_boundary(monkeypatch, kind, text):
    import sys
    import gateway.platforms.base as base
    calls = []
    class Response:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def json(self): return {"ok": True, "ts": "sent"}
    class Session(Response):
        def __init__(self, **kwargs): pass
        def post(self, url, **kwargs):
            calls.append(kwargs["json"])
            return Response()
    monkeypatch.setitem(sys.modules, "aiohttp", SimpleNamespace(ClientSession=Session, ClientTimeout=lambda **kw: None))
    monkeypatch.setattr(base, "resolve_proxy_url", lambda: None)
    monkeypatch.setattr(base, "proxy_kwargs_for_aiohttp", lambda proxy: ({}, {}))
    config = PlatformConfig(enabled=True, token="synthetic-test-token", extra={"workspace_id": "TONE", "authorized_deliveries": [
        {"platform": "slack", "workspace_id": "TONE", "channel_id": "CSAME", "operator_requested": True}]})
    result = asyncio.run(_standalone_send(config, "CSAME", text, thread_id="thread", output_class=kind))
    if kind is OutputClass.PROGRESS:
        assert not result["success"] and calls == []
    else:
        assert result["success"]
        expected = SAFE_FAILURE_TEXT if kind is OutputClass.SAFE_ERROR else text
        if "fixture:synthetic-password@" in text:
            assert "synthetic-password" not in calls[0]["text"]
            assert calls[0]["text"].startswith("https://fixture:")
            assert calls[0]["text"].endswith("@example.invalid/")
            expected = calls[0]["text"]  # Existing Slack formatting may style the mask.
        assert calls == [{"channel": "CSAME", "text": expected, "mrkdwn": True, "thread_ts": "thread"}]


@pytest.mark.parametrize("method", ["send", "edit_message"])
def test_http_userinfo_password_is_removed_at_text_boundary(method):
    async def scenario():
        adapter = Adapter()
        kwargs = {"chat_id": "CSAME", "content": "https://fixture:synthetic-password@example.invalid/"}
        if method == "edit_message": kwargs["message_id"] = "sent"
        assert (await getattr(adapter, method)(**kwargs)).success
        assert adapter.calls[0][1] == "https://fixture:***@example.invalid/"
    asyncio.run(scenario())


@pytest.mark.parametrize("audience,allowed", [("external", False), ("private_operator", True)])
@pytest.mark.parametrize("kind", [OutputClass.FINAL, OutputClass.SAFE_ERROR])
def test_slack_execution_approval_is_private_operator_output(audience, allowed, kind):
    async def scenario():
        adapter = slack_adapter()
        adapter.config.extra["message_audience"] = [{"platform": "slack", "workspace_id": "TONE", "channel_id": "CSAME", "audience": audience}]
        client = SimpleNamespace(chat_postMessage=AsyncMock(return_value={"ts": "sent"}))
        adapter._app = SimpleNamespace(client=client)
        adapter._team_clients = {"TONE": client}
        adapter.stop_typing = AsyncMock()
        result = await adapter.send_exec_approval("CSAME", "echo synthetic", "synthetic-session", metadata={"thread_id": "thread", "_hermes_output_class": kind})
        assert result.success is allowed
        if allowed:
            assert client.chat_postMessage.call_args.kwargs["thread_ts"] == "thread"
        else:
            assert result.error == "audience_policy_suppressed"
            client.chat_postMessage.assert_not_called()
    asyncio.run(scenario())


def test_slack_media_caption_is_redacted_before_file_transport():
    async def scenario():
        adapter = slack_adapter()
        adapter._upload_file = AsyncMock(return_value=SendResult(success=True))
        secret = "synthetic-caption-password"
        result = await adapter.send_image_file("CSAME", "/synthetic/image.png", caption="password=" + secret)
        assert result.success
        assert secret not in adapter._upload_file.call_args.args[2]
        adapter._upload_file.reset_mock()
        result = await adapter.send_image_file("CSAME", "/synthetic/image.png", caption="synthetic progress", metadata={"_hermes_output_class": OutputClass.PROGRESS})
        assert not result.success
        adapter._upload_file.assert_not_called()
    asyncio.run(scenario())
