"""Tests for the Slack /ito-link identity bind flow (spec v1, 2026-08-24).

Covers:
* ``ito_link.is_link_keyword`` DM keyword predicate
* ``ito_link.mint_link_nonce`` auth, payload, failure taxonomy, no-log contract
* ``slack_app_manifest`` declares /ito-link (Socket Mode delivery requirement)
* ``SlackAdapter._handle_ito_link_request`` delivery semantics: ephemeral in
  channels (never a public post), normal reply in DMs, never-silent on error
* ``SlackAdapter.connect`` registers the dedicated /ito-link bolt listener
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


_repo = str(Path(__file__).resolve().parents[2])
if _repo not in sys.path:
    sys.path.insert(0, _repo)


def _ensure_slack_mock() -> None:
    if "slack_bolt" in sys.modules and hasattr(sys.modules["slack_bolt"], "__file__"):
        return
    slack_bolt = MagicMock()
    slack_bolt.async_app.AsyncApp = MagicMock
    slack_bolt.adapter.socket_mode.async_handler.AsyncSocketModeHandler = MagicMock
    slack_bolt.authorization.AuthorizeResult = MagicMock
    slack_sdk = MagicMock()
    slack_sdk.web.async_client.AsyncWebClient = MagicMock
    for name, mod in [
        ("slack_bolt", slack_bolt),
        ("slack_bolt.async_app", slack_bolt.async_app),
        ("slack_bolt.authorization", slack_bolt.authorization),
        ("slack_bolt.adapter", slack_bolt.adapter),
        ("slack_bolt.adapter.socket_mode", slack_bolt.adapter.socket_mode),
        ("slack_bolt.adapter.socket_mode.async_handler",
         slack_bolt.adapter.socket_mode.async_handler),
        ("slack_sdk", slack_sdk),
        ("slack_sdk.web", slack_sdk.web),
        ("slack_sdk.web.async_client", slack_sdk.web.async_client),
    ]:
        sys.modules.setdefault(name, mod)
    sys.modules.setdefault("aiohttp", MagicMock())


_ensure_slack_mock()

from gateway.platforms.base import PlatformConfig  # noqa: E402
from plugins.platforms.slack.adapter import SlackAdapter  # noqa: E402
from plugins.platforms.slack import ito_link  # noqa: E402
from plugins.platforms.slack.ito_link import ItoLinkMintError  # noqa: E402


# ---------------------------------------------------------------------------
# Keyword predicate
# ---------------------------------------------------------------------------


class TestLinkKeyword:
    def test_bare_keyword_matches(self):
        assert ito_link.is_link_keyword("link")
        assert ito_link.is_link_keyword("  Link \n")
        assert ito_link.is_link_keyword("LINK")

    def test_other_text_does_not_match(self):
        assert not ito_link.is_link_keyword("link please")
        assert not ito_link.is_link_keyword("relink")
        assert not ito_link.is_link_keyword("")
        assert not ito_link.is_link_keyword("link my account")


# ---------------------------------------------------------------------------
# mint_link_nonce
# ---------------------------------------------------------------------------


class _FakeResponse:
    def __init__(self, status_code: int, body=None, json_raises=False):
        self.status_code = status_code
        self._body = body
        self._json_raises = json_raises

    def json(self):
        if self._json_raises:
            raise ValueError("not json")
        return self._body


class _FakeClient:
    def __init__(self, response=None, raises=None):
        self._response = response
        self._raises = raises
        self.posts: list = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def post(self, url, json=None, headers=None):
        self.posts.append({"url": url, "json": json, "headers": headers})
        if self._raises is not None:
            raise self._raises
        return self._response


def _mint(client: _FakeClient, env: dict, **kwargs) -> dict:
    with patch.dict(os.environ, env, clear=False), \
         patch.object(ito_link, "create_ssrf_safe_async_client", return_value=client):
        return asyncio.run(ito_link.mint_link_nonce(**kwargs))


class TestMintLinkNonce:
    def test_unconfigured_without_bridge_token(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("OTC_BRIDGE_TOKEN", None)
            with pytest.raises(ItoLinkMintError) as excinfo:
                asyncio.run(ito_link.mint_link_nonce(team_id="T0TEAM", member_id="U0MEMBER"))
        assert excinfo.value.kind == "unconfigured"

    def test_success_posts_bound_payload_and_composes_bind_url(self):
        client = _FakeClient(response=_FakeResponse(200, {
            "ok": True,
            "nonce": "n" * 43,
            "expires_at": "2026-08-25T01:15:00.000Z",
            "bind_path": f"/auth/slack/bind?n={'n' * 43}",
        }))
        result = _mint(
            client,
            {"OTC_BRIDGE_TOKEN": "bridge-secret"},
            team_id="T0TEAM",
            member_id="U0MEMBER",
            display_name="Affaan",
        )
        assert result["bind_url"] == f"https://compute.itomarkets.com/auth/slack/bind?n={'n' * 43}"
        assert result["expires_at"] == "2026-08-25T01:15:00.000Z"
        post = client.posts[0]
        assert post["url"] == "https://compute.itomarkets.com/api/slack/link-nonces"
        assert post["json"] == {
            "slack_team_id": "T0TEAM",
            "slack_member_id": "U0MEMBER",
            "display_name_snapshot": "Affaan",
        }
        assert post["headers"]["authorization"] == "Bearer bridge-secret"

    def test_display_name_omitted_when_absent(self):
        client = _FakeClient(response=_FakeResponse(200, {
            "ok": True,
            "bind_path": f"/auth/slack/bind?n={'n' * 43}",
        }))
        _mint(client, {"OTC_BRIDGE_TOKEN": "x"}, team_id="T0TEAM", member_id="U0MEMBER")
        assert "display_name_snapshot" not in client.posts[0]["json"]

    def test_401_maps_to_unauthorized(self):
        client = _FakeClient(response=_FakeResponse(401, {"ok": False}))
        with pytest.raises(ItoLinkMintError) as excinfo:
            _mint(client, {"OTC_BRIDGE_TOKEN": "x"}, team_id="T0TEAM", member_id="U0MEMBER")
        assert excinfo.value.kind == "unauthorized"

    def test_503_maps_to_unavailable(self):
        client = _FakeClient(response=_FakeResponse(503, {"ok": False}))
        with pytest.raises(ItoLinkMintError) as excinfo:
            _mint(client, {"OTC_BRIDGE_TOKEN": "x"}, team_id="T0TEAM", member_id="U0MEMBER")
        assert excinfo.value.kind == "unavailable"

    def test_transport_error_maps_to_unreachable(self):
        client = _FakeClient(raises=TimeoutError("slow"))
        with pytest.raises(ItoLinkMintError) as excinfo:
            _mint(client, {"OTC_BRIDGE_TOKEN": "x"}, team_id="T0TEAM", member_id="U0MEMBER")
        assert excinfo.value.kind == "unreachable"
        # Transport details must not leak nonce material into the error.
        assert "Bearer" not in str(excinfo.value)

    def test_unexpected_payload_maps_to_unavailable(self):
        for body in [
            {"ok": True},
            {"ok": True, "bind_path": "https://evil.example.com/steal"},
            {"ok": False, "bind_path": "/auth/slack/bind?n=x"},
            "garbage",
        ]:
            client = _FakeClient(response=_FakeResponse(200, body))
            with pytest.raises(ItoLinkMintError) as excinfo:
                _mint(client, {"OTC_BRIDGE_TOKEN": "x"}, team_id="T0TEAM", member_id="U0MEMBER")
            assert excinfo.value.kind == "unavailable"

    def test_origin_override(self):
        client = _FakeClient(response=_FakeResponse(200, {
            "ok": True,
            "bind_path": f"/auth/slack/bind?n={'n' * 43}",
        }))
        result = _mint(
            client,
            {"OTC_BRIDGE_TOKEN": "x", "ITO_PANEL_PUBLIC_ORIGIN": "https://data.itomarkets.com/"},
            team_id="T0TEAM",
            member_id="U0MEMBER",
        )
        assert result["bind_url"].startswith("https://data.itomarkets.com/auth/slack/bind")


# ---------------------------------------------------------------------------
# Manifest declares /ito-link
# ---------------------------------------------------------------------------


class TestManifest:
    def test_ito_link_declared(self):
        from hermes_cli.commands import slack_app_manifest

        manifest = slack_app_manifest()
        commands = [entry["command"] for entry in manifest["features"]["slash_commands"]]
        assert "/ito-link" in commands
        assert commands.count("/ito-link") == 1


# ---------------------------------------------------------------------------
# Adapter delivery semantics
# ---------------------------------------------------------------------------


def _adapter() -> SlackAdapter:
    config = PlatformConfig(enabled=True, token="xoxb-fake")
    adapter = SlackAdapter(config)
    adapter.send = AsyncMock(return_value=MagicMock(success=True))
    adapter.send_private_notice = AsyncMock(return_value=MagicMock(success=True))
    return adapter


class TestItoLinkDelivery:
    def test_channel_request_is_ephemeral_only(self):
        adapter = _adapter()
        with patch.object(ito_link, "mint_link_nonce", new=AsyncMock(return_value={
            "bind_url": "https://compute.itomarkets.com/auth/slack/bind?n=" + "n" * 43,
            "expires_at": "2026-08-25T01:15:00.000Z",
        })) as mint:
            asyncio.run(adapter._handle_ito_link_request(
                channel_id="C0CHANNEL",
                user_id="U0MEMBER",
                team_id="T0TEAM",
                display_name="Affaan",
                is_dm=False,
                reply_to=None,
            ))
        mint.assert_awaited_once_with(team_id="T0TEAM", member_id="U0MEMBER", display_name="Affaan")
        adapter.send_private_notice.assert_awaited_once()
        args, kwargs = adapter.send_private_notice.await_args
        assert args[0] == "C0CHANNEL"
        assert args[1] == "U0MEMBER"
        assert "/auth/slack/bind?n=" in args[2]
        # The bind URL must never become a public channel post.
        adapter.send.assert_not_awaited()

    def test_dm_request_replies_in_dm(self):
        adapter = _adapter()
        with patch.object(ito_link, "mint_link_nonce", new=AsyncMock(return_value={
            "bind_url": "https://compute.itomarkets.com/auth/slack/bind?n=" + "n" * 43,
            "expires_at": None,
        })):
            asyncio.run(adapter._handle_ito_link_request(
                channel_id="D0DM",
                user_id="U0MEMBER",
                team_id="T0TEAM",
                display_name=None,
                is_dm=True,
                reply_to="1700000000.000001",
            ))
        adapter.send.assert_awaited_once()
        args, kwargs = adapter.send.await_args
        assert args[0] == "D0DM"
        assert "/auth/slack/bind?n=" in args[1]
        assert kwargs["reply_to"] == "1700000000.000001"
        adapter.send_private_notice.assert_not_awaited()

    def test_mint_failure_still_answers_privately(self):
        adapter = _adapter()
        with patch.object(
            ito_link,
            "mint_link_nonce",
            new=AsyncMock(side_effect=ItoLinkMintError("unconfigured")),
        ):
            asyncio.run(adapter._handle_ito_link_request(
                channel_id="C0CHANNEL",
                user_id="U0MEMBER",
                team_id="T0TEAM",
                display_name=None,
                is_dm=False,
                reply_to=None,
            ))
        adapter.send_private_notice.assert_awaited_once()
        args, _kwargs = adapter.send_private_notice.await_args
        assert "not configured" in args[2]
        adapter.send.assert_not_awaited()

    def test_unexpected_failure_still_answers_privately(self):
        adapter = _adapter()
        with patch.object(
            ito_link,
            "mint_link_nonce",
            new=AsyncMock(side_effect=RuntimeError("boom")),
        ):
            asyncio.run(adapter._handle_ito_link_request(
                channel_id="C0CHANNEL",
                user_id="U0MEMBER",
                team_id="T0TEAM",
                display_name=None,
                is_dm=False,
                reply_to=None,
            ))
        adapter.send_private_notice.assert_awaited_once()
        args, _kwargs = adapter.send_private_notice.await_args
        assert "Try again" in args[2]

    def test_missing_ids_does_not_crash_or_send(self):
        adapter = _adapter()
        with patch.object(ito_link, "mint_link_nonce", new=AsyncMock(return_value={
            "bind_url": "https://compute.itomarkets.com/auth/slack/bind?n=" + "n" * 43,
            "expires_at": None,
        })):
            asyncio.run(adapter._handle_ito_link_request(
                channel_id="",
                user_id="",
                team_id="T0TEAM",
                display_name=None,
                is_dm=False,
                reply_to=None,
            ))
        adapter.send_private_notice.assert_not_awaited()
        adapter.send.assert_not_awaited()


# ---------------------------------------------------------------------------
# connect() registers the dedicated listener
# ---------------------------------------------------------------------------


class TestConnectRegistration:
    def test_ito_link_command_registered(self):
        import plugins.platforms.slack.adapter as slack_mod

        registered_commands: list = []

        def mock_command(matcher):
            def decorator(fn):
                registered_commands.append((matcher, fn))
                return fn
            return decorator

        mock_app = MagicMock()
        mock_app.event = lambda _t: (lambda fn: fn)
        mock_app.command = mock_command
        mock_app.action = lambda _a: (lambda fn: fn)
        mock_app.client = AsyncMock()

        mock_web_client = AsyncMock()
        mock_web_client.auth_test = AsyncMock(return_value={
            "user_id": "U_BOT",
            "user": "testbot",
            "team_id": "T_FAKE",
            "team": "FakeTeam",
        })

        adapter = _adapter()
        fake_mgr = MagicMock()
        fake_mgr.get_slack_action_handlers.return_value = []

        with patch.object(slack_mod, "AsyncApp", return_value=mock_app), \
             patch.object(slack_mod, "AsyncWebClient", return_value=mock_web_client), \
             patch.object(slack_mod, "AsyncSocketModeHandler", return_value=MagicMock()), \
             patch.dict(os.environ, {"SLACK_APP_TOKEN": "xapp-fake"}), \
             patch("gateway.status.acquire_scoped_lock", return_value=(True, None)), \
             patch("gateway.status.release_scoped_lock"), \
             patch("hermes_cli.plugins.get_plugin_manager", return_value=fake_mgr), \
             patch("asyncio.create_task"):
            result = asyncio.run(adapter.connect())

        assert result is True
        literal_commands = [m for m, _fn in registered_commands if isinstance(m, str)]
        assert "/ito-link" in literal_commands


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
