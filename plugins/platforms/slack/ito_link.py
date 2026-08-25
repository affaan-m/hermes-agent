"""Slack ↔ Ito identity linking — gateway side of the /ito-link flow.

Spec v1 (2026-08-24): the institutional panel is the identity authority.
This module converts a Slack slash command or DM keyword into a single-use
bind URL delivered ONLY through a private reply (ephemeral in channels; the
DM itself is private by definition).

The nonce is minted by the panel (POST /api/slack/link-nonces) under the
existing desk bridge credential (OTC_BRIDGE_TOKEN). The gateway never signs,
persists, or logs nonce material beyond the mint round-trip, and never posts
a bind URL to a channel. The runtime stores only the SHA-256 hash; the nonce
is single-use with a 15-minute TTL and is bound to (slack_team_id,
slack_member_id) at mint time.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

from tools.url_safety import create_ssrf_safe_async_client

logger = logging.getLogger(__name__)

DEFAULT_MINT_URL = "https://compute.itomarkets.com/api/slack/link-nonces"
DEFAULT_PUBLIC_ORIGIN = "https://compute.itomarkets.com"
MINT_TIMEOUT_SECONDS = 10.0
LINK_KEYWORD = "link"


class ItoLinkMintError(Exception):
    """Mint failure with an operator-meaningful kind."""

    def __init__(self, kind: str, detail: str = "") -> None:
        super().__init__(detail or kind)
        self.kind = kind


def is_link_keyword(text: str) -> bool:
    """A DM whose entire content is the link keyword triggers the bind flow."""
    return text.strip().lower() == LINK_KEYWORD


async def mint_link_nonce(
    *,
    team_id: str,
    member_id: str,
    display_name: Optional[str] = None,
) -> dict[str, Any]:
    """Mint one bind nonce from the panel. Never logs the nonce or URL."""
    token = (os.environ.get("OTC_BRIDGE_TOKEN") or "").strip()
    if not token:
        raise ItoLinkMintError(
            "unconfigured",
            "OTC_BRIDGE_TOKEN is not provisioned on this gateway",
        )
    mint_url = (os.environ.get("ITO_PANEL_LINK_NONCE_URL") or "").strip() or DEFAULT_MINT_URL
    origin = (
        (os.environ.get("ITO_PANEL_PUBLIC_ORIGIN") or "").strip().rstrip("/")
        or DEFAULT_PUBLIC_ORIGIN
    )
    payload: dict[str, Any] = {
        "slack_team_id": team_id,
        "slack_member_id": member_id,
    }
    if display_name:
        payload["display_name_snapshot"] = display_name
    try:
        async with create_ssrf_safe_async_client(timeout=MINT_TIMEOUT_SECONDS) as client:
            response = await client.post(
                mint_url,
                json=payload,
                headers={
                    "authorization": f"Bearer {token}",
                    "content-type": "application/json",
                },
            )
    except ItoLinkMintError:
        raise
    except Exception as exc:
        raise ItoLinkMintError("unreachable", f"{type(exc).__name__}") from exc
    if response.status_code == 401:
        raise ItoLinkMintError("unauthorized", "panel rejected the bridge credential")
    if response.status_code != 200:
        raise ItoLinkMintError("unavailable", f"panel answered HTTP {response.status_code}")
    try:
        body = response.json()
    except Exception as exc:
        raise ItoLinkMintError("unavailable", "panel returned a non-JSON body") from exc
    if (
        not isinstance(body, dict)
        or body.get("ok") is not True
        or not isinstance(body.get("bind_path"), str)
        or not body["bind_path"].startswith("/auth/slack/bind?n=")
    ):
        raise ItoLinkMintError("unavailable", "panel returned an unexpected payload")
    return {
        "bind_url": f"{origin}{body['bind_path']}",
        "expires_at": body.get("expires_at"),
    }


def link_ready_message(bind_url: str) -> str:
    return (
        "Link your Slack identity to Itô: "
        f"{bind_url}\n"
        "Single use, expires in 15 minutes. The page asks you to sign in to "
        "your Itô account if you are not already. Never share this URL."
    )


def link_error_message(error: ItoLinkMintError) -> str:
    if error.kind == "unconfigured":
        return (
            "Identity linking is not configured on this gateway yet "
            "(missing panel credential). The operator has been notified."
        )
    if error.kind == "unauthorized":
        return (
            "Identity linking was rejected by the panel (credential issue). "
            "The operator has been notified."
        )
    return "Could not mint your link right now (panel unavailable). Try again in a minute."
