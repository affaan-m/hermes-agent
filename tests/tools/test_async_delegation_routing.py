"""Tests for async-delegation routing metadata durability.

Verifies that background delegations dispatched from bg_* sessions (e.g. the
/background slash command) carry platform/chat_id/chat_type/thread_id so the
gateway can route their completions after a restart, even when the session_key
is not parseable by _parse_session_key.
"""

import json
import queue
import threading
import time

import pytest

from tools import async_delegation as ad
from tools.process_registry import process_registry


@pytest.fixture(autouse=True)
def _clean_state():
    ad._reset_for_tests()
    while not process_registry.completion_queue.empty():
        process_registry.completion_queue.get_nowait()
    yield
    ad._reset_for_tests()
    while not process_registry.completion_queue.empty():
        process_registry.completion_queue.get_nowait()


def _drain_one(timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not process_registry.completion_queue.empty():
            return process_registry.completion_queue.get_nowait()
        time.sleep(0.02)
    return None


# ---------------------------------------------------------------------------
# Routing metadata stored in the record and forwarded to the completion event
# ---------------------------------------------------------------------------

def test_dispatch_stores_routing_metadata():
    """dispatch_async_delegation stores routing metadata in the record."""

    def runner():
        return {"status": "completed", "summary": "ok"}

    res = ad.dispatch_async_delegation(
        goal="test",
        context=None,
        toolsets=None,
        role="leaf",
        model="m",
        session_key="bg_123456_abcdef",
        routing_platform="telegram",
        routing_chat_id="-1001234567890",
        routing_chat_type="group",
        routing_thread_id="12345",
        runner=runner,
        max_async_children=3,
    )
    assert res["status"] == "dispatched"

    evt = _drain_one()
    assert evt is not None
    assert evt["type"] == "async_delegation"
    assert evt["session_key"] == "bg_123456_abcdef"
    assert evt["platform"] == "telegram"
    assert evt["chat_id"] == "-1001234567890"
    assert evt["chat_type"] == "group"
    assert evt["thread_id"] == "12345"


def test_dispatch_batch_stores_routing_metadata():
    """dispatch_async_delegation_batch stores routing metadata in the record."""

    def runner():
        return {
            "results": [
                {"status": "completed", "summary": "done: a"},
                {"status": "completed", "summary": "done: b"},
            ],
            "total_duration_seconds": 0.5,
        }

    res = ad.dispatch_async_delegation_batch(
        goals=["a", "b"],
        context=None,
        toolsets=None,
        role="leaf",
        model="m",
        session_key="bg_123456_abcdef",
        routing_platform="telegram",
        routing_chat_id="-1001234567890",
        routing_chat_type="group",
        routing_thread_id="12345",
        runner=runner,
        max_async_children=3,
    )
    assert res["status"] == "dispatched"

    evt = _drain_one()
    assert evt is not None
    assert evt["type"] == "async_delegation"
    assert evt["session_key"] == "bg_123456_abcdef"
    assert evt["platform"] == "telegram"
    assert evt["chat_id"] == "-1001234567890"
    assert evt["chat_type"] == "group"
    assert evt["thread_id"] == "12345"
    assert evt["is_batch"] is True
    assert len(evt["results"]) == 2


def test_routing_metadata_optional_and_defaults_empty():
    """Routing metadata is optional; empty strings are the default."""

    def runner():
        return {"status": "completed", "summary": "ok"}

    res = ad.dispatch_async_delegation(
        goal="test",
        context=None,
        toolsets=None,
        role="leaf",
        model="m",
        session_key="agent:main:telegram:dm:12345:678",
        runner=runner,
        max_async_children=3,
    )
    assert res["status"] == "dispatched"

    evt = _drain_one()
    assert evt is not None
    assert evt["session_key"] == "agent:main:telegram:dm:12345:678"
    # No routing metadata captured — gateway will parse session_key instead.
    assert evt.get("platform", "") == ""
    assert evt.get("chat_id", "") == ""
    assert evt.get("chat_type", "") == ""


# ---------------------------------------------------------------------------
# Gateway integration: _enrich_async_delegation_routing skips pre-enriched events
# ---------------------------------------------------------------------------

def test_gateway_enrich_skips_pre_enriched_event():
    """_enrich_async_delegation_routing returns early when platform is set."""
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    evt = {
        "type": "async_delegation",
        "delegation_id": "deleg_x1",
        "session_key": "bg_123456_abcdef",
        "platform": "telegram",
        "chat_id": "-1001234567890",
        "chat_type": "group",
        "thread_id": "12345",
    }
    runner._enrich_async_delegation_routing(evt)
    # Values must be unchanged — not overwritten by _parse_session_key.
    assert evt["platform"] == "telegram"
    assert evt["chat_id"] == "-1001234567890"
    assert evt["chat_type"] == "group"
    assert evt["thread_id"] == "12345"


def test_gateway_builds_routable_source_from_bg_event():
    """_build_process_event_source can route a bg_* event when routing
    metadata is present, even though _parse_session_key returns None."""
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    evt = {
        "type": "async_delegation",
        "delegation_id": "deleg_x1",
        "session_key": "bg_123456_abcdef",
        "platform": "telegram",
        "chat_id": "-1001234567890",
        "chat_type": "group",
        "thread_id": "12345",
    }
    src = runner._build_process_event_source(evt)
    assert src is not None
    assert src.platform.value == "telegram"
    assert src.chat_id == "-1001234567890"
    assert src.chat_type == "group"
    assert src.thread_id == "12345"


def test_gateway_drops_unroutable_bg_event_without_metadata():
    """A bg_* event without routing metadata is dropped (regression guard)."""
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    evt = {
        "type": "async_delegation",
        "delegation_id": "deleg_x1",
        "session_key": "bg_123456_abcdef",
        # No platform/chat_id/chat_type — cannot route.
    }
    src = runner._build_process_event_source(evt)
    assert src is None


# ---------------------------------------------------------------------------
# delegate_task captures routing metadata from the parent agent
# ---------------------------------------------------------------------------

def test_delegate_task_captures_routing_metadata_for_bg_session(monkeypatch):
    """When get_current_session_key() returns empty and the parent agent's
    session_id is a bg_* task id, delegate_task captures the parent's routing
    metadata so the completion event can be routed after a gateway restart."""
    from unittest.mock import MagicMock
    import tools.delegate_tool as dt

    parent = MagicMock()
    parent._delegate_depth = 0
    parent.session_id = "bg_123456_abcdef"
    parent.platform = "telegram"
    parent._chat_id = "-1001234567890"
    parent._chat_type = "group"
    parent._thread_id = "12345"
    parent._interrupt_requested = False
    parent._active_children = []
    parent._active_children_lock = None

    fake_child = MagicMock()
    fake_child._delegate_role = "leaf"
    fake_child._subagent_id = "s1"

    def fast_child(task_index, goal, child=None, parent_agent=None, **kw):
        return {
            "task_index": 0, "status": "completed", "summary": f"done: {goal}",
            "api_calls": 1, "duration_seconds": 0.1, "model": "m",
            "exit_reason": "completed",
        }

    creds = {
        "model": "m", "provider": None, "base_url": None, "api_key": None,
        "api_mode": None, "command": None, "args": None,
    }
    monkeypatch.setattr(dt, "_build_child_agent", lambda **kw: fake_child)
    monkeypatch.setattr(dt, "_run_single_child", fast_child)
    monkeypatch.setattr(dt, "_resolve_delegation_credentials", lambda *a, **k: creds)

    out = dt.delegate_task(
        goal="test bg routing",
        context=None,
        background=True,
        parent_agent=parent,
    )
    parsed = json.loads(out)
    assert parsed["status"] == "dispatched"

    evt = _drain_one()
    assert evt is not None
    assert evt["type"] == "async_delegation"
    assert evt["session_key"] == "bg_123456_abcdef"
    assert evt["platform"] == "telegram"
    assert evt["chat_id"] == "-1001234567890"
    assert evt["chat_type"] == "group"
    assert evt["thread_id"] == "12345"
