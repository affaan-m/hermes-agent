"""Bounded offline dispatcher boundaries; actual adapters, fake I/O only."""
import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.delivery import DeliveryRouter, DeliveryTarget
from gateway.message_audience import OutputClass
from gateway.message_failure import SAFE_FAILURE_TEXT
from gateway.platforms.base import SendResult, _thread_metadata_for_source
from plugins.platforms.slack.adapter import SlackAdapter
from tools import send_message_tool as tool


def config(platform="slack", grant=True):
    return PlatformConfig(enabled=True, token="synthetic-token", extra={
        "workspace_id": "TA", "message_audience": [
            {"platform": platform, "workspace_id": "TA", "channel_id": "CSAME", "audience": "internal"}],
        "authorized_deliveries": ([{"platform": platform, "workspace_id": "TA", "channel_id": "CSAME", "operator_requested": True}] if grant else []),
    })


def slack():
    adapter = SlackAdapter(config())
    clients = {team: SimpleNamespace(chat_postMessage=AsyncMock(return_value={"ts": "sent"}), chat_update=AsyncMock(return_value={"ok": True})) for team in ("TA", "TB")}
    adapter._team_clients = clients
    adapter._app = SimpleNamespace(client=clients["TA"])
    adapter._channel_team = {"CSAME": "TA"}
    adapter.stop_typing = AsyncMock()
    return adapter, clients


@pytest.mark.parametrize("method", ["send", "edit_message"])
@pytest.mark.parametrize("kind,allowed", [(OutputClass.PROGRESS, False), (OutputClass.FINAL, True)])
def test_same_channel_workspace_metadata_binds_both_policy_and_actual_transport(method, kind, allowed):
    async def scenario():
        adapter, clients = slack()
        metadata = {"slack_team_id": "TB", "thread_id": "thread", "_hermes_output_class": kind,
                    "audience": "internal", "operator_requested": True}
        kwargs = dict(chat_id="CSAME", content="Capacity is 20 TB.", metadata=metadata)
        if method == "edit_message": kwargs.update(message_id="sent", finalize=True)
        result = await getattr(adapter, method)(**kwargs)
        assert result.success is allowed
        clients["TA"].chat_postMessage.assert_not_called()
        clients["TA"].chat_update.assert_not_called()
        transport = clients["TB"].chat_postMessage if method == "send" else clients["TB"].chat_update
        assert transport.await_count == int(allowed)
        assert metadata["slack_team_id"] == "TB"
    asyncio.run(scenario())


@pytest.mark.parametrize("method", ["send", "edit_message"])
def test_unknown_explicit_workspace_cannot_fall_back_to_internal_client(method):
    async def scenario():
        adapter, clients = slack()
        kwargs = dict(chat_id="CSAME", content="Supplier final", metadata={"slack_team_id": "TUNKNOWN"})
        if method == "edit_message": kwargs["message_id"] = "sent"
        assert not (await getattr(adapter, method)(**kwargs)).success
        clients["TA"].chat_postMessage.assert_not_called()
        clients["TA"].chat_update.assert_not_called()
    asyncio.run(scenario())


def test_source_scope_is_preserved_even_without_thread():
    source = SimpleNamespace(platform=Platform.SLACK, scope_id="TB", thread_id=None)
    assert _thread_metadata_for_source(source)["slack_team_id"] == "TB"


def test_weixin_denied_before_early_direct_sender(monkeypatch):
    sender = AsyncMock(return_value={"success": True})
    monkeypatch.setattr(tool, "_send_weixin", sender)
    result = asyncio.run(tool._send_to_platform(Platform.WEIXIN, config("weixin", False), "CSAME", "Synthetic", media_files=[("/synthetic/file", False)]))
    assert result["success"] is False and result["error_kind"] == "policy_denied"
    sender.assert_not_called()


def test_authorized_non_slack_standalone_final_preserved_and_redacted(monkeypatch):
    sender = AsyncMock(return_value={"success": True})
    monkeypatch.setattr(tool, "_send_weixin", sender)
    result = asyncio.run(tool._send_to_platform(Platform.WEIXIN, config("weixin"), "CSAME", "Capacity is 20 TB. https://fixture:synthetic-private-password@example.invalid/"))
    assert result["success"]
    assert "Capacity is 20 TB." in sender.call_args.args[2]
    assert "synthetic-private-password" not in sender.call_args.args[2]


def router(adapter):
    value = object.__new__(DeliveryRouter)
    value.adapters = {adapter.platform: adapter}
    value._save_full_output = lambda *args: pytest.fail("denial reached output persistence")
    value._filter_silence_narration_enabled = lambda: False
    return value


def test_router_denies_before_persistence_or_topic_creation():
    adapter = SimpleNamespace(platform=Platform.TELEGRAM, config=config("telegram", False), ensure_dm_topic=AsyncMock(), send=AsyncMock())
    result = asyncio.run(router(adapter)._deliver_to_platform(DeliveryTarget(Platform.TELEGRAM, "CSAME", "new topic"), "X" * 6000, {}))
    assert result["error_kind"] == "policy_denied"
    adapter.ensure_dm_topic.assert_not_called()
    adapter.send.assert_not_called()


def test_router_truncation_footer_has_no_saved_path():
    adapter = SimpleNamespace(platform=Platform.TELEGRAM, config=config("telegram"), send=AsyncMock(return_value=SendResult(success=True)))
    value = router(adapter)
    value._save_full_output = lambda *args: Path("/synthetic/private/output.txt")
    result = asyncio.run(value._deliver_to_platform(DeliveryTarget(Platform.TELEGRAM, "CSAME"), "Capacity " * 800, {}))
    assert result.success
    text = adapter.send.call_args.args[1]
    assert "truncated" in text and "/synthetic" not in text and "output.txt" not in text


def test_router_preserves_structured_policy_denial():
    denial = SendResult(success=False, error="audience_policy_suppressed", raw_response={"suppressed": True})
    adapter = SimpleNamespace(platform=Platform.SLACK, config=config(), send=AsyncMock(return_value=denial))
    result = asyncio.run(router(adapter)._deliver_to_platform(DeliveryTarget(Platform.SLACK, "CSAME"), "Supplier answer", {}))
    assert result["error_kind"] == "policy_denied" and not result["success"]


@pytest.mark.parametrize("helper", ["_send_via_adapter", "_registry_standalone_send"])
def test_independent_dispatch_helpers_cannot_bypass_authorization(helper):
    result = asyncio.run(getattr(tool, helper)(Platform.SLACK if helper == "_send_via_adapter" else "slack", config(grant=False), "CSAME", "Synthetic"))
    assert result["error_kind"] == "policy_denied" and not result["success"]


def test_model_tool_flags_cannot_authorize_resolved_destination(monkeypatch):
    import gateway.config as gateway_config
    monkeypatch.setattr(gateway_config, "load_gateway_config", lambda: SimpleNamespace(platforms={Platform.SLACK: config(grant=False)}))
    monkeypatch.setattr(tool, "_maybe_skip_cron_duplicate_send", lambda *args: None)
    monkeypatch.setattr(tool, "_parse_target_ref", lambda *args: ("CSAME", None, True))
    sender = AsyncMock()
    monkeypatch.setattr(tool, "_send_to_platform", sender)
    import json
    result = json.loads(tool._handle_send({"target": "slack:CSAME", "message": "Synthetic", "operator_requested": True, "internal": True, "metadata": {"audience": "internal"}}))
    assert result["error_kind"] == "policy_denied"
    sender.assert_not_called()


def test_direct_safe_failure_cannot_forward_supplied_media(monkeypatch):
    sender = AsyncMock(return_value={"success": True})
    monkeypatch.setattr(tool, "_send_weixin", sender)
    result = asyncio.run(tool._send_to_platform(Platform.WEIXIN, config("weixin"), "CSAME", "synthetic diagnostic", media_files=[("/synthetic/private.png", False)], output_class=OutputClass.SAFE_ERROR))
    assert result["success"]
    assert sender.call_args.args[2] == SAFE_FAILURE_TEXT
    assert sender.call_args.kwargs["media_files"] == []


def test_standalone_scope_cannot_override_configured_credential_workspace():
    from gateway.message_failure import check_delivery, DeliveryPolicyDenied
    cfg = config()
    cfg.extra["workspace_id"] = "TB"
    cfg.extra["authorized_deliveries"].append({"platform": "slack", "workspace_id": "TB", "channel_id": "CSAME", "operator_requested": True})
    with pytest.raises(DeliveryPolicyDenied):
        check_delivery(cfg, "slack", "CSAME", metadata={"scope_id": "TA"}, output_class=OutputClass.OPERATIONAL)


@pytest.mark.parametrize("intake", ["message", "slash"])
def test_actual_intake_scope_survives_cache_change_through_runner_metadata(intake):
    import ast
    async def scenario():
        adapter, clients = slack()
        adapter._bot_user_id = "UBOT"
        adapter._team_bot_user_ids = {"TA": "UBOT", "TB": "UBOT"}
        adapter._resolve_user_name = AsyncMock(return_value="Synthetic")
        adapter._reactions_enabled = lambda: False
        adapter.handle_message = AsyncMock()
        if intake == "message":
            await adapter._handle_slack_message({"channel": "CSAME", "channel_type": "channel", "team": "TA", "user": "UHUMAN", "ts": "2", "text": "<@UBOT> Capacity?"})
        else:
            await adapter._handle_slash_command({"command": "/help", "channel_id": "CSAME", "team_id": "TA", "user_id": "UHUMAN"})
        source = adapter.handle_message.call_args.args[0].source
        assert source.scope_id == "TA"
        adapter._channel_team["CSAME"] = "TB"
        path = Path(__file__).resolve().parents[2] / "gateway/run.py"
        node = next(n for n in ast.walk(ast.parse(path.read_text())) if isinstance(n, ast.FunctionDef) and n.name == "_thread_metadata_for_source")
        namespace = {"Platform": Platform}
        module = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), node], type_ignores=[])
        exec(compile(ast.fix_missing_locations(module), str(path), "exec"), namespace)
        runner = SimpleNamespace(_thread_metadata_for_target=lambda *a, **kw: {"thread_id": "2"})
        metadata = namespace[node.name](runner, source)
        metadata["_hermes_output_class"] = OutputClass.PROGRESS
        assert (await adapter.send("CSAME", "Working on the requested check.", metadata=metadata)).success
        clients["TA"].chat_postMessage.assert_awaited_once()
        clients["TB"].chat_postMessage.assert_not_called()
    asyncio.run(scenario())


def test_router_safe_error_is_rendered_before_silence_filter():
    adapter = SimpleNamespace(platform=Platform.TELEGRAM, config=config("telegram"), send=AsyncMock(return_value=SendResult(success=True)))
    value = router(adapter)
    value._filter_silence_narration_enabled = lambda: True
    result = asyncio.run(value._deliver_to_platform(DeliveryTarget(Platform.TELEGRAM, "CSAME"), ".", {"_hermes_output_class": OutputClass.SAFE_ERROR}))
    assert result.success
    assert adapter.send.call_args.args[1] == SAFE_FAILURE_TEXT


def test_assistant_thread_seed_uses_its_supplied_workspace():
    adapter, clients = slack()
    adapter._session_store = object()
    captured = {}
    class Captured(Exception): pass
    def build_source(**kwargs):
        captured.update(kwargs)
        raise Captured()
    adapter.build_source = build_source
    with pytest.raises(Captured):
        adapter._seed_assistant_thread_session({"channel_id": "CSAME", "thread_ts": "thread", "user_id": "UHUMAN", "team_id": "TB"})
    assert captured["scope_id"] == "TB"


def test_native_slash_ephemeral_context_is_bound_to_user_and_workspace():
    from plugins.platforms.slack.adapter import _slash_user_id
    async def scenario():
        adapter, clients = slack()
        adapter.handle_message = AsyncMock()
        for team in ("TA", "TB"):
            await adapter._handle_slash_command({"command": "/help", "channel_id": "CSAME", "team_id": team,
                "user_id": "UHUMAN", "response_url": "https://example.invalid/" + team})
        adapter._send_slash_ephemeral = AsyncMock(return_value=SendResult(success=True))
        token = _slash_user_id.set("UHUMAN")
        try:
            for team in ("TA", "TB"):
                assert (await adapter.send("CSAME", "Synthetic reply", metadata={"slack_team_id": team})).success
                assert adapter._send_slash_ephemeral.call_args.args[0]["response_url"].endswith("/" + team)
        finally:
            _slash_user_id.reset(token)
        assert adapter._send_slash_ephemeral.await_count == 2
        clients["TA"].chat_postMessage.assert_not_called()
        clients["TB"].chat_postMessage.assert_not_called()
    asyncio.run(scenario())


@pytest.mark.parametrize("error,retryable", [("response_url POST returned 400", False), ("synthetic connection reset", True), (ValueError("synthetic formatter failure"), True)])
def test_real_base_retry_cannot_promote_failed_private_reply_to_public(error, retryable, monkeypatch):
    from plugins.platforms.slack.adapter import _slash_user_id
    async def scenario():
        adapter, clients = slack()
        adapter.handle_message = AsyncMock()
        await adapter._handle_slash_command({"command": "/help", "channel_id": "CSAME", "team_id": "TA",
            "user_id": "UHUMAN", "response_url": "https://example.invalid/private"})
        adapter._send_slash_ephemeral = AsyncMock(return_value=SendResult(success=False, error=error, retryable=retryable))
        if isinstance(error, Exception):
            adapter._send_slash_ephemeral.side_effect = error
        monkeypatch.setattr(asyncio, "sleep", AsyncMock())
        token = _slash_user_id.set("UHUMAN")
        try:
            result = await adapter._send_with_retry("CSAME", "Synthetic private reply", metadata={"slack_team_id": "TA"})
        finally:
            _slash_user_id.reset(token)
        assert not result.success
        adapter._send_slash_ephemeral.assert_awaited_once()
        clients["TA"].chat_postMessage.assert_not_called()
        clients["TB"].chat_postMessage.assert_not_called()
    asyncio.run(scenario())


@pytest.mark.parametrize("method", ["_fetch_thread_context", "_fetch_thread_parent_text"])
def test_context_fetch_uses_supplied_workspace_not_mutable_channel_cache(method):
    async def scenario():
        adapter, clients = slack()
        for client in clients.values():
            client.conversations_replies = AsyncMock(return_value={"messages": []})
        adapter._channel_team["CSAME"] = "TB"
        kwargs = {"team_id": "TA"}
        if method == "_fetch_thread_context":
            kwargs["current_ts"] = "current"
        await getattr(adapter, method)("CSAME", "thread", **kwargs)
        clients["TA"].conversations_replies.assert_awaited_once()
        clients["TB"].conversations_replies.assert_not_called()
    asyncio.run(scenario())


@pytest.mark.parametrize("state", ["expired", "consumed"])
def test_native_private_invocation_cannot_fall_through_after_context_loss(state):
    async def scenario():
        adapter, clients = slack()
        adapter._send_slash_ephemeral = AsyncMock(return_value=SendResult(success=True))
        async def handle(event):
            if state == "expired":
                for ctx in adapter._slash_command_contexts.values():
                    ctx["ts"] -= adapter._SLASH_CTX_TTL + 1
            else:
                assert (await adapter.send("CSAME", "First private reply", metadata={"slack_team_id": "TA"})).success
            result = await adapter._send_with_retry("CSAME", "Delayed private reply", metadata={"slack_team_id": "TA"})
            assert not result.success and result.error_kind == "private_delivery_failed"
            clients["TA"].chat_postMessage.assert_not_called()
            # A separate, exact destination does not inherit private intent.
            assert (await adapter.send("COTHER", "Authorized final", metadata={"slack_team_id": "TB"})).success
            clients["TB"].chat_postMessage.assert_awaited_once()
        adapter.handle_message = handle
        await adapter._handle_slash_command({"command": "/help", "channel_id": "CSAME", "team_id": "TA",
            "user_id": "UHUMAN", "response_url": "https://example.invalid/private"})
    asyncio.run(scenario())
