"""A /background task runs inside the session context of the chat that started it."""
from types import SimpleNamespace

from gateway.run import background_session_vars
from gateway.session_context import clear_session_vars, get_session_env, set_session_vars


class _Platform:
    value = "telegram"


def _source(**overrides):
    base = dict(platform=_Platform(), chat_id="-1003795075025", chat_name="Itô Ops",
                thread_id=15483, user_id=8177476652, user_name="rahil", profile="ito")
    base.update(overrides)
    return SimpleNamespace(**base)


def test_background_session_vars_mirror_the_origin_source():
    vars_ = background_session_vars(_source(), "bg_153255_3dce37", event_message_id=36935)
    assert vars_ == {
        "platform": "telegram",
        "chat_id": "-1003795075025",
        "chat_name": "Itô Ops",
        "thread_id": "15483",
        "user_id": "8177476652",
        "user_name": "rahil",
        "session_id": "bg_153255_3dce37",
        "message_id": "36935",
        "profile": "ito",
    }


def test_missing_source_fields_become_empty_strings():
    vars_ = background_session_vars(_source(thread_id=None, user_id=None, chat_name="", profile=None), "bg_1")
    assert vars_["thread_id"] == "" and vars_["user_id"] == "" and vars_["chat_name"] == "" and vars_["profile"] == ""
    assert vars_["message_id"] == ""
    assert background_session_vars(SimpleNamespace(platform="slack", chat_id="C0BHDC7LC2F"), "bg_2")["platform"] == "slack"


def test_hooks_see_the_origin_chat_while_the_task_runs():
    tokens = set_session_vars(**background_session_vars(_source(), "bg_153255_3dce37"))
    try:
        assert get_session_env("HERMES_SESSION_PLATFORM") == "telegram"
        assert get_session_env("HERMES_SESSION_CHAT_ID") == "-1003795075025"
        assert get_session_env("HERMES_SESSION_USER_ID") == "8177476652"
        assert get_session_env("HERMES_SESSION_ID") == "bg_153255_3dce37"
    finally:
        clear_session_vars(tokens)
    assert get_session_env("HERMES_SESSION_CHAT_ID", "") == ""
