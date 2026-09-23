"""Regression tests for SLACK-MEDIA-FAIL-DROPS-TEXT (2026-09-22).

At 16:30:37 the desk's reply to a Slack DM carried two MEDIA: attachments.
The pinned inventory-intake lease (300 s TTL) had expired during the
356.7 s turn, so the pinned-route preflight denied the text post and both
uploads; the log showed only the canned denial text and the user saw
nothing. These tests pin: the refusal log names the reason and the path,
a failed text post is logged, and a failed attachment never takes the
text down (the text goes out, then the failed attachments are reported).
"""

import asyncio
import json
import logging
import os
import time
from types import SimpleNamespace

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    SendResult,
)
from gateway.session import SessionSource, build_session_key

import gateway.inventory_context as ic


# ---------------------------------------------------------------------------
# delivery_denial_reason
# ---------------------------------------------------------------------------

def _seed_intake(adapter=None, *, expired=False, post_attempts=0):
    """One valid-by-construction intake, using the module's own invariants."""
    adapter = adapter or SimpleNamespace(
        config=SimpleNamespace(extra={}),
        _team_clients={},
        _team_bot_user_ids={},
    )
    client = object()
    adapter._team_clients["T1"] = client
    adapter._team_bot_user_ids["T1"] = "UBOT"
    raw = {
        "type": "message",
        "user": "U1",
        "channel": "D1",
        "ts": "1000.000001",
        "text": "hi",
        "channel_type": "im",
    }
    snapshot = ic._snapshot(raw)
    routing = ic._routing(adapter)
    session_thread, delivery_thread = ic._threads(raw, snapshot, routing)
    expires = time.monotonic() - 1 if expired else time.monotonic() + 300
    intake = ic._Intake(
        adapter, raw, "T1", client, "UBOT", snapshot, expires, routing,
        session_thread, delivery_thread, post_attempts=post_attempts,
    )
    receipt = ic._Receipt()
    ic._INTAKES[receipt] = intake
    token = ic._DELIVERY.set(receipt)
    return adapter, receipt, token


def _clear_intake(receipt, token):
    ic._DELIVERY.reset(token)
    ic._INTAKES.pop(receipt, None)


def test_denial_reason_none_without_intake():
    token = ic._DELIVERY.set(None)
    try:
        assert ic.delivery_denial_reason(object(), "D1") is None
    finally:
        ic._DELIVERY.reset(token)


def test_denial_reason_expired_lease():
    adapter, receipt, token = _seed_intake(expired=True)
    try:
        assert ic.delivery_denial_reason(adapter, "D1") == "intake lease expired"
    finally:
        _clear_intake(receipt, token)


def test_denial_reason_route_mismatches_and_pass():
    adapter, receipt, token = _seed_intake()
    try:
        assert ic.delivery_denial_reason(adapter, "D1") is None
        assert ic.delivery_denial_reason(adapter, "D2") == "intake channel mismatch"
        other = SimpleNamespace(config=SimpleNamespace(extra={}))
        assert ic.delivery_denial_reason(other, "D1") == "intake adapter mismatch"
        assert (
            ic.delivery_denial_reason(adapter, "D1", team_id="T2")
            == "intake workspace mismatch"
        )
    finally:
        _clear_intake(receipt, token)


def test_denial_reason_post_budget_exhausted():
    adapter, receipt, token = _seed_intake(post_attempts=ic._MAX_POSTED_MESSAGES)
    try:
        assert (
            ic.delivery_denial_reason(adapter, "D1")
            == "intake post budget exhausted"
        )
    finally:
        _clear_intake(receipt, token)


# ---------------------------------------------------------------------------
# Slack adapter: the refusal log names the reason and the path
# ---------------------------------------------------------------------------

def _slack_adapter():
    from plugins.platforms.slack.adapter import SlackAdapter

    adapter = SlackAdapter(PlatformConfig(enabled=True, token="xoxb-fake-token"))
    adapter._app = SimpleNamespace()
    return adapter


def test_document_denial_logs_reason_with_path(monkeypatch, caplog):
    from gateway.inventory_context import InventoryDeliveryDenied

    adapter = _slack_adapter()
    monkeypatch.setattr(ic, "has_intake_delivery", lambda: True)
    monkeypatch.setattr(
        ic, "delivery_denial_reason", lambda *a, **k: "intake lease expired"
    )

    def _deny(*_a, **_k):
        raise InventoryDeliveryDenied("Inventory intake delivery is unavailable")

    monkeypatch.setattr(adapter, "_resolve_thread_ts", _deny)

    with caplog.at_level(logging.WARNING):
        result = asyncio.run(
            adapter.send_document("D1", "/tmp/artifact-STRIKE.pdf")
        )

    assert result.success is False
    messages = [r.getMessage() for r in caplog.records]
    assert any(
        "Refused document upload /tmp/artifact-STRIKE.pdf" in m
        and "intake lease expired" in m
        for m in messages
    ), messages


def test_voice_and_video_denial_log_reason_with_path(monkeypatch, caplog):
    from gateway.inventory_context import InventoryDeliveryDenied

    adapter = _slack_adapter()
    monkeypatch.setattr(ic, "has_intake_delivery", lambda: True)
    monkeypatch.setattr(
        ic, "delivery_denial_reason", lambda *a, **k: "intake post budget exhausted"
    )

    def _deny(*_a, **_k):
        raise InventoryDeliveryDenied("Inventory intake delivery is unavailable")

    monkeypatch.setattr(adapter, "_resolve_thread_ts", _deny)

    async def _run():
        voice = await adapter.send_voice("D1", "/tmp/note.ogg")
        video = await adapter.send_video("D1", "/tmp/clip.mp4")
        return voice, video

    with caplog.at_level(logging.WARNING):
        voice, video = asyncio.run(_run())

    assert voice.success is False and video.success is False
    messages = [r.getMessage() for r in caplog.records]
    assert any("Refused audio upload /tmp/note.ogg" in m for m in messages)
    assert any("Refused video upload /tmp/clip.mp4" in m for m in messages)
    assert all("intake post budget exhausted" in m for m in messages if "Refused" in m)


# ---------------------------------------------------------------------------
# base.py flow: text first, failed attachments reported, never dropped
# ---------------------------------------------------------------------------

class FlowStubAdapter(BasePlatformAdapter):
    def __init__(self, *, text_send_ok=True):
        super().__init__(PlatformConfig(enabled=True, token="fake"), Platform.DISCORD)
        self.sent = []
        self.documents = []
        self.text_send_ok = text_send_ok

    async def connect(self, *, is_reconnect: bool = False):
        return True

    async def disconnect(self):
        pass

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        self.sent.append({"chat_id": chat_id, "content": content})
        if self.text_send_ok or "Couldn't deliver" in str(content):
            return SendResult(success=True, message_id=f"m{len(self.sent)}")
        return SendResult(
            success=False,
            error="This response cannot be delivered to the selected conversation.",
        )

    async def send_typing(self, chat_id, metadata=None):
        pass

    async def get_chat_info(self, chat_id):
        return {"id": chat_id}

    async def send_document(self, chat_id, file_path, caption=None, file_name=None,
                            reply_to=None, metadata=None):
        self.documents.append(file_path)
        return SendResult(
            success=False,
            error="This response cannot be delivered to the selected conversation.",
        )


def _flow_event():
    return MessageEvent(
        text="show me the order",
        source=SessionSource(
            platform=Platform.DISCORD, chat_id="c1", chat_type="dm", user_id="u1"
        ),
        message_id="m1",
    )


@pytest.mark.asyncio
async def test_failed_attachment_never_drops_text_and_is_reported(tmp_path, caplog):
    attachment = tmp_path / "STRIKE-ORDER.pdf"
    attachment.write_bytes(b"%PDF-fake")
    adapter = FlowStubAdapter()

    async def handler(event):
        return f"Here is the order summary.\nMEDIA:{attachment}"

    adapter.set_message_handler(handler)
    event = _flow_event()

    with caplog.at_level(logging.WARNING):
        await adapter._process_message_background(event, build_session_key(event.source))

    contents = [s["content"] for s in adapter.sent]
    assert any("Here is the order summary." in c for c in contents), contents
    notes = [c for c in contents if "Couldn't deliver" in c]
    assert len(notes) == 1, contents
    assert "1 attachment(s)" in notes[0]
    assert "STRIKE-ORDER.pdf" in notes[0]
    assert adapter.documents == [str(attachment)]
    messages = [r.getMessage() for r in caplog.records]
    assert any("Failed to send media (.pdf)" in m for m in messages)


@pytest.mark.asyncio
async def test_failed_text_send_is_logged(tmp_path, caplog):
    adapter = FlowStubAdapter(text_send_ok=False)

    async def handler(event):
        return "The answer."

    adapter.set_message_handler(handler)
    event = _flow_event()

    with caplog.at_level(logging.WARNING):
        await adapter._process_message_background(event, build_session_key(event.source))

    messages = [r.getMessage() for r in caplog.records]
    assert any(
        "Final response text delivery to c1 failed" in m
        and "cannot be delivered" in m
        for m in messages
    ), messages
