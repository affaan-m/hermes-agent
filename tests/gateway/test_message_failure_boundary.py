"""Synthetic regressions for runtime failures at the actual message handler."""
import asyncio

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.message_failure import SAFE_FAILURE_TEXT, normalize_agent_response, requires_safe_failure
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, ProcessingOutcome, SendResult
from gateway.session import SessionSource, build_session_key


class RecordingAdapter(BasePlatformAdapter):
    def __init__(self, fail_send=False):
        config = PlatformConfig(enabled=True)
        config.typing_indicator = False
        super().__init__(config, Platform.SLACK)
        self.sent = []
        self.outcomes = []
        self.fail_send = fail_send

    async def connect(self, **kwargs):
        return True

    async def disconnect(self):
        pass

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        self.sent.append((chat_id, content, metadata))
        if self.fail_send:
            raise OSError("synthetic-notification-failure")
        return SendResult(success=True, message_id="synthetic-sent")

    async def get_chat_info(self, chat_id):
        return {"id": chat_id}

    async def on_processing_complete(self, event, outcome):
        self.outcomes.append(outcome)


@pytest.mark.parametrize("fail_send", [False, True])
def test_actual_handler_exception_is_safe_threaded_and_cleans_up(fail_send, caplog):
    async def scenario():
        adapter = RecordingAdapter(fail_send)
        async def fail(event):
            raise RuntimeError("synthetic-continuation-error /synthetic/private/run.json")
        adapter.set_message_handler(fail)
        event = MessageEvent(text="@bot please help", message_id="synthetic-message", source=SessionSource(
            platform=Platform.SLACK, chat_id="synthetic-supplier", chat_type="group",
            thread_id="synthetic-thread",
        ))
        key = build_session_key(event.source)
        # Match handle_message's ownership registration. Cleanup intentionally
        # cannot remove another task's session guard.
        adapter._session_tasks[key] = asyncio.current_task()
        await adapter._process_message_background(event, key)
        assert len(adapter.sent) == 1  # Failed notification must not recurse.
        chat_id, content, metadata = adapter.sent[0]
        assert chat_id == "synthetic-supplier"
        assert metadata["thread_id"] == "synthetic-thread"
        assert content == SAFE_FAILURE_TEXT
        assert "RuntimeError" not in content and "/reset" not in content
        assert "synthetic-continuation-error" not in content
        assert adapter.outcomes == [ProcessingOutcome.FAILURE]
        assert key not in adapter._active_sessions
        assert key not in adapter._session_tasks
    asyncio.run(scenario())
    assert "synthetic-continuation-error" in caplog.text  # Detail stays server-side.


@pytest.mark.parametrize("result", [
    {"failed": True, "error": "synthetic backend continuation"},
    {"partial": True, "api_calls": 0, "error": "synthetic private path"},
    {"partial": True, "api_calls": 1},
    {"error": "synthetic provider envelope"},
    {"completed": False},
])
@pytest.mark.parametrize("response", ["", "synthetic partial runtime diagnostic"])
def test_structured_failure_never_reuses_error_or_partial_diagnostics(result, response):
    assert normalize_agent_response(result, response) == SAFE_FAILURE_TEXT
    assert requires_safe_failure({**result, "already_sent": True})


def test_positive_answer_and_intentional_interrupt_are_preserved():
    answer = "The supplier offered two servers at the quoted rate."
    assert normalize_agent_response({"completed": True}, answer) == answer
    assert normalize_agent_response({"interrupted": True, "partial": True}, "") == ""
    assert normalize_agent_response({"interrupted": True, "completed": False},
                                    "synthetic continuation diagnostic") == SAFE_FAILURE_TEXT
    assert normalize_agent_response({"api_calls": 0}, "") == SAFE_FAILURE_TEXT
