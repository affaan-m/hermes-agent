"""Execute real scheduler delivery functions with isolated configuration/I/O.

AST extraction avoids importing cron startup, providers, hooks or installers.
This is bounded source composition coverage, not installed runtime coverage.
"""
import ast
import asyncio
import concurrent.futures
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
import gateway.config as gateway_config
import agent.async_utils as async_utils
from gateway.config import Platform, PlatformConfig
from gateway.message_failure import SAFE_FAILURE_TEXT
from gateway.platforms.base import SendResult
from tools import send_message_tool as tool


def functions(namespace):
    source = Path(__file__).resolve().parents[2] / "cron/scheduler.py"
    wanted = {"_deliver_result", "_summarize_cron_failure_for_delivery", "_send_media_via_adapter"}
    nodes = [n for n in ast.parse(source.read_text()).body if isinstance(n, ast.FunctionDef) and n.name in wanted]
    module = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), *nodes], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(source), "exec"), namespace)
    return namespace


def setup(monkeypatch, *, granted=True, denied_transport=False, mirror=False):
    pconfig = PlatformConfig(enabled=True, extra={"workspace_id": "TA", "authorized_deliveries": [
        {"platform": "telegram", "workspace_id": "TA", "channel_id": "CSAME", "operator_requested": True}] if granted else []})
    config = SimpleNamespace(platforms={Platform.TELEGRAM: pconfig})
    monkeypatch.setattr(gateway_config, "load_gateway_config", lambda: config)
    standalone = AsyncMock(return_value={"success": True})
    monkeypatch.setattr(tool, "_send_to_platform", standalone)
    class Future:
        def __init__(self, coro): self.coro = coro
        def result(self, timeout=None): return asyncio.run(self.coro)
    monkeypatch.setattr(async_utils, "safe_schedule_threadsafe", lambda coro, loop: Future(coro))
    result = SendResult(success=False, error="audience_policy_suppressed") if denied_transport else SendResult(success=True)
    adapter = SimpleNamespace(platform=Platform.TELEGRAM, config=pconfig, send=AsyncMock(return_value=result), send_image_file=AsyncMock(return_value=SendResult(success=True)))
    mirror_call, thread_call = Mock(), Mock(return_value=None)
    namespace = dict(asyncio=asyncio, concurrent=concurrent, logger=logging.getLogger(__name__),
        load_config=lambda: {"cron": {"wrap_response": True}},
        _resolve_delivery_targets=lambda job: job["targets"], _resolve_origin=lambda job: job.get("origin", {}),
        _cron_mirror_delivery_enabled=lambda *a: mirror, _target_matches_origin=lambda *a: mirror,
        _maybe_mirror_cron_delivery=mirror_call, _open_continuable_cron_thread=thread_call,
        _confirm_adapter_delivery=lambda result: bool(result and result.success))
    functions(namespace)
    job = {"id": "synthetic-private-job-id", "name": "synthetic-private-job-name", "targets": [{"platform": "telegram", "chat_id": "CSAME"}]}
    return namespace, job, adapter, standalone, mirror_call, thread_call


def test_failure_renderer_never_returns_job_ids_control_guidance_or_raw_error():
    ns = functions({"logger": logging.getLogger(__name__)})
    text = ns["_summarize_cron_failure_for_delivery"]({"id": "synthetic-private-id"}, "OSError synthetic /private/path")
    assert text == SAFE_FAILURE_TEXT


@pytest.mark.parametrize("content", ["Capacity is 20 TB.", "MEDIA:/synthetic/file.png"])
def test_denied_target_has_no_thread_text_media_standalone_or_mirror(monkeypatch, content):
    ns, job, adapter, standalone, mirror, thread = setup(monkeypatch, granted=False, mirror=True)
    error = ns["_deliver_result"](job, content, {Platform.TELEGRAM: adapter}, SimpleNamespace(is_running=lambda: True))
    assert error
    adapter.send.assert_not_called(); adapter.send_image_file.assert_not_called()
    standalone.assert_not_called(); mirror.assert_not_called(); thread.assert_not_called()


def test_live_policy_denial_is_terminal_without_standalone_or_mirror(monkeypatch):
    ns, job, adapter, standalone, mirror, thread = setup(monkeypatch, denied_transport=True)
    error = ns["_deliver_result"](job, "Capacity is 20 TB.", {Platform.TELEGRAM: adapter}, SimpleNamespace(is_running=lambda: True))
    assert error
    adapter.send.assert_awaited_once()
    standalone.assert_not_called(); mirror.assert_not_called()


def test_authorized_supplier_final_has_no_scheduler_wrapper(monkeypatch):
    ns, job, adapter, standalone, mirror, thread = setup(monkeypatch)
    error = ns["_deliver_result"](job, "Capacity is 20 TB.")
    assert error is None
    assert standalone.call_args.args[3] == "Capacity is 20 TB."


def test_safe_failure_class_survives_scheduler_composition(monkeypatch):
    from gateway.message_audience import OutputClass
    ns, job, adapter, standalone, mirror, thread = setup(monkeypatch)
    error = ns["_deliver_result"](job, "synthetic raw error MEDIA:/synthetic/file.png", output_class=OutputClass.SAFE_ERROR)
    assert error is None
    assert standalone.call_args.args[3] == SAFE_FAILURE_TEXT
    assert standalone.call_args.kwargs["media_files"] == []
    assert standalone.call_args.kwargs["output_class"] is OutputClass.SAFE_ERROR


def test_denied_fanout_target_does_not_block_independently_authorized_target(monkeypatch):
    ns, job, adapter, standalone, mirror, thread = setup(monkeypatch)
    job["targets"].insert(0, {"platform": "telegram", "chat_id": "COTHER", "operator_requested": True})
    error = ns["_deliver_result"](job, "Supplier capacity is 20 TB.")
    assert error
    standalone.assert_awaited_once()
    assert standalone.call_args.args[2] == "CSAME"


def test_media_helper_policy_denial_stops_following_attachments(monkeypatch, tmp_path):
    from gateway.message_failure import DeliveryPolicyDenied
    ns, job, adapter, standalone, mirror, thread = setup(monkeypatch)
    ns.update(_IMAGE_EXTS={".png"}, _VIDEO_EXTS={".mp4"})
    adapter.send_image_file.return_value = SendResult(success=False, error="audience_policy_suppressed")
    file = tmp_path / "synthetic.png"
    file.write_bytes(b"synthetic media")
    with pytest.raises(DeliveryPolicyDenied):
        ns["_send_media_via_adapter"](adapter, "CSAME", [(str(file), False), (str(file), False)], {}, object(), job, Platform.TELEGRAM)
    adapter.send_image_file.assert_awaited_once()


def test_filtered_delivery_is_not_mirrored_or_retried(monkeypatch):
    ns, job, adapter, standalone, mirror, thread = setup(monkeypatch, mirror=True)
    adapter.send.return_value = {"success": True, "delivered": False, "filtered": "silence_narration"}
    assert ns["_deliver_result"](job, "Capacity", {Platform.TELEGRAM: adapter}, SimpleNamespace(is_running=lambda: True))
    standalone.assert_not_called(); mirror.assert_not_called()


def test_media_transport_failure_does_not_claim_success(monkeypatch, tmp_path):
    ns, job, adapter, standalone, mirror, thread = setup(monkeypatch)
    ns.update(_IMAGE_EXTS={".png"}, _VIDEO_EXTS={".mp4"})
    adapter.send_image_file.return_value = SendResult(success=False, error="synthetic transport failure")
    file = tmp_path / "synthetic.png"
    file.write_bytes(b"synthetic media")
    with pytest.raises(Exception, match="delivery_not_confirmed"):
        ns["_send_media_via_adapter"](adapter, "CSAME", [(str(file), False)], {}, object(), job, Platform.TELEGRAM)
