"""Tests for the trivial-turn fast path prototype."""

from types import SimpleNamespace

import pytest

from agent import fast_path


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    monkeypatch.setenv("HERMES_FAST_PATH", "1")
    monkeypatch.delenv("HERMES_FAST_PATH_EFFORT", raising=False)
    monkeypatch.delenv("HERMES_FAST_PATH_TTFB", raising=False)
    fast_path.reset_config_cache()
    yield
    fast_path.reset_config_cache()


def test_short_plain_message_is_trivial():
    assert fast_path.classify_message("ok thanks") is None
    assert fast_path.classify_message("yes go ahead") is None


def test_gateway_wrappers_are_ignored():
    text = '[Replying to: "i need this answer as i have a suitor"]\nIB or RoCE'
    assert fast_path.strip_gateway_wrappers(text) == "IB or RoCE"
    assert fast_path.classify_message(text) is None


@pytest.mark.parametrize(
    "text, reason",
    [
        ("", "empty"),
        ("x" * 200, "too_long"),
        (" ".join(["word"] * 30), "too_many_words"),
        ("run this:\n```\nls\n```", "code"),
        ("see https://example.com/doc", "url"),
        ("[Image: photo.jpg] what is this", "attachment"),
        ("a\nb\nc\nd", "multiline"),
    ],
)
def test_non_trivial_messages(text, reason):
    assert fast_path.classify_message(text) == reason


def test_disabled_by_default(monkeypatch):
    monkeypatch.delenv("HERMES_FAST_PATH")
    fast_path.reset_config_cache()
    agent = SimpleNamespace(reasoning_config=None, _api_call_count=1)
    assert fast_path.plan_for_turn(agent, "ok thanks") is None
    assert agent._fast_path_plan is None
    assert fast_path.effective_reasoning_config(agent) is None


def test_plan_applies_to_first_call_only(monkeypatch):
    monkeypatch.setenv("HERMES_FAST_PATH_EFFORT", "minimal")
    monkeypatch.setenv("HERMES_FAST_PATH_TTFB", "20")
    fast_path.reset_config_cache()
    agent = SimpleNamespace(reasoning_config={"effort": "medium"}, _api_call_count=1)
    plan = fast_path.plan_for_turn(agent, "ok thanks")
    assert plan is not None and plan.reasoning_effort == "minimal"
    assert fast_path.effective_reasoning_config(agent) == {"effort": "minimal"}
    assert fast_path.first_byte_cutoff(agent, 120.0) == 20.0
    assert fast_path.first_byte_cutoff(agent, None) == 20.0
    agent._api_call_count = 2
    assert fast_path.active_plan(agent) is None
    assert fast_path.effective_reasoning_config(agent) == {"effort": "medium"}
    assert fast_path.first_byte_cutoff(agent, 120.0) == 120.0


def test_disabled_reasoning_is_left_alone():
    agent = SimpleNamespace(reasoning_config={"enabled": False}, _api_call_count=1)
    fast_path.plan_for_turn(agent, "ok")
    assert fast_path.effective_reasoning_config(agent) == {"enabled": False}


def test_invalid_effort_falls_back(monkeypatch):
    monkeypatch.setenv("HERMES_FAST_PATH_EFFORT", "turbo")
    fast_path.reset_config_cache()
    assert fast_path.load_config()["reasoning_effort"] == "low"
