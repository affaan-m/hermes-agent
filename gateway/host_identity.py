"""Cross-host host identity + single-owner gating for shared bot credentials.

Live incident (2026-08-27..28): two hosts on the tailnet — this mini
(``hermes``) and ``alejandros-mac-mini`` — both ran the Hermes ito profile
with the SAME Telegram bot token. Telegram allows exactly one getUpdates
consumer per token, so the two gateways spent days in a 409-conflict storm
(``Conflict: terminated by other getUpdates request``): dropped inbound,
reconnect churn, and desk-wide slowness. A local PID/runtime lock cannot fix
this — the second poller is on a DIFFERENT host.

The mechanism here is a config-flag single-owner gate: ``gateway.telegram_owner``
names the ONE host allowed to hold the telegram token; every other host skips
loading the telegram adapter and runs as a standby. The owner value is matched
against every identifier the local host can plausibly be known by (hostname,
short hostname, explicit ``gateway.host_id``, and the tailnet DNS/node name),
so the operator can name the owner however is natural on the tailnet.

This is intentionally a config flag (not a lease/election): the owner decision
is an operator call, and a shared-store lease is a deliberate later upgrade.
"""

from __future__ import annotations

import json
import socket
import subprocess
from functools import lru_cache


def _norm(value: str) -> str:
    """Normalize an identifier for comparison: casefold, strip trailing dot."""
    return (value or "").strip().casefold().rstrip(".")


def _short(value: str) -> str:
    """Strip the domain suffix from a hostname ('host.example.com' -> 'host')."""
    return _norm(value).split(".")[0]


@lru_cache(maxsize=1)
def _tailnet_names() -> frozenset[str]:
    """Best-effort tailnet identifiers for this host (DNS name + node name).

    Resolved once via ``tailscale status --self --json``; empty when the CLI is
    unavailable or the host is not on a tailnet. Never raises.
    """
    names: set[str] = set()
    try:
        proc = subprocess.run(
            ["tailscale", "status", "--self", "--json"],
            capture_output=True, text=True, timeout=5,
        )
        data = json.loads(proc.stdout or "{}")
        self_node = data.get("Self") or {}
        for key in ("DNSName", "HostName"):
            raw = self_node.get(key)
            if raw:
                names.add(_norm(raw))
                names.add(_short(raw))
    except Exception:
        pass
    return frozenset(n for n in names if n)


@lru_cache(maxsize=1)
def local_host_ids() -> frozenset[str]:
    """All identifiers this host answers to, normalized.

    Includes the FQDN hostname, its short form, and the tailnet DNS/node names
    when resolvable. Cached — the host's identity does not change at runtime.
    """
    ids: set[str] = set()
    hostname = socket.gethostname()
    if hostname:
        ids.add(_norm(hostname))
        ids.add(_short(hostname))
    ids.update(_tailnet_names())
    return frozenset(i for i in ids if i)


def _explicit_host_id(config: dict | None) -> str:
    try:
        return _norm(str((config or {}).get("gateway", {}).get("host_id", "")))
    except Exception:
        return ""


def telegram_owner(config: dict | None) -> str:
    """The configured telegram owner value, or "" for single-host mode."""
    try:
        return _norm(str((config or {}).get("gateway", {}).get("telegram_owner", "")))
    except Exception:
        return ""


def telegram_owner_permits_local(config: dict | None) -> tuple[bool, str]:
    """Decide whether THIS host may hold the telegram bot token.

    Returns ``(permitted, reason)``:
      - owner unset        -> (True, "")  single-host mode, connect normally.
      - owner == local     -> (True, "")  this host is the designated owner.
      - owner != local     -> (False, reason)  standby: skip the telegram adapter.

    A local match is accepted against the FQDN hostname, short hostname, the
    explicit ``gateway.host_id``, and the tailnet DNS/node names.
    """
    owner = telegram_owner(config)
    if not owner:
        return True, ""
    candidates = set(local_host_ids())
    explicit = _explicit_host_id(config)
    if explicit:
        candidates.add(explicit)
    if owner in candidates:
        return True, ""
    local_desc = explicit or socket.gethostname()
    return False, (
        f"telegram disabled on this host: not the designated owner "
        f"(gateway.telegram_owner={owner!r}, this host={local_desc!r}). "
        f"The owner host holds the bot token; this gateway runs telegram as a standby."
    )
