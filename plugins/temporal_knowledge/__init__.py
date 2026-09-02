"""Temporal Knowledge plugin for Hermes.

Exposes baseline_check as a tool via the desk temporal knowledge service.
The service is API-compatible with ito-cloud-runtime/agent-fleet/graphiti.
"""

import json
import os
from pathlib import Path

import requests

from tools.registry import registry

# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------

_plugin_dir = Path(__file__).parent
_default_url = os.environ.get("GRAPHITI_URL", "http://127.0.0.1:8098")


def check_requirements() -> bool:
    return True  # No external deps; service must be running


def _service_url() -> str:
    return os.environ.get("GRAPHITI_URL", _default_url)


def temporal_baseline_check(deal_key: str, as_of_ts: int = 0, task_id: str = None) -> str:
    """Return the CURRENT valid facts for a deal/counterparty and what superseded what."""
    url = _service_url()
    as_of = None
    if as_of_ts:
        from datetime import datetime, timezone
        as_of = datetime.fromtimestamp(as_of_ts, tz=timezone.utc).isoformat()
    try:
        resp = requests.post(
            f"{url}/baseline",
            json={"group_id": "desk", "deal_key": deal_key, "as_of": as_of},
            timeout=30,
        )
        resp.raise_for_status()
        return json.dumps({"success": True, "result": resp.json()})
    except requests.exceptions.ConnectionError:
        return json.dumps({"success": False, "error": f"Temporal knowledge service not running at {url}. Start with: ./plugins/temporal_knowledge/run-desk-service.sh"})
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})


def temporal_ingest(task_id: str = None) -> str:
    """Trigger ledger ingestion into the temporal knowledge service."""
    url = _service_url()
    try:
        resp = requests.post(f"{url}/ingest/ledger?group_id=desk", timeout=300)
        resp.raise_for_status()
        return json.dumps({"success": True, "result": resp.json()})
    except requests.exceptions.ConnectionError:
        return json.dumps({"success": False, "error": f"Temporal knowledge service not running at {url}"})
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})


def temporal_brief(deal_key: str, task_id: str = None) -> str:
    """Get a memory brief for a deal (current facts + what changed)."""
    url = _service_url()
    try:
        resp = requests.post(
            f"{url}/brief",
            json={"group_id": "desk", "center": deal_key, "limit": 20},
            timeout=30,
        )
        resp.raise_for_status()
        return json.dumps({"success": True, "result": resp.json()})
    except requests.exceptions.ConnectionError:
        return json.dumps({"success": False, "error": f"Temporal knowledge service not running at {url}"})
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})


# Register tools
registry.register(
    name="temporal_baseline_check",
    toolset="temporal_knowledge",
    schema={
        "name": "temporal_baseline_check",
        "description": "Verify the current valid facts for a deal or counterparty before drafting external communication. Returns contract state, open asks, and supersession history. Use this before any reply to avoid stale-baseline errors.",
        "parameters": {
            "type": "object",
            "properties": {
                "deal_key": {
                    "type": "string",
                    "description": "Deal or counterparty identifier (e.g. 'pluto', 'ronit', 'deal:pluto')",
                },
                "as_of_ts": {
                    "type": "integer",
                    "description": "Unix timestamp for point-in-time query (default: now)",
                    "default": 0,
                },
            },
            "required": ["deal_key"],
        },
    },
    handler=lambda args, **kw: temporal_baseline_check(
        deal_key=args.get("deal_key", ""),
        as_of_ts=args.get("as_of_ts", 0),
        task_id=kw.get("task_id"),
    ),
    check_fn=check_requirements,
)

registry.register(
    name="temporal_ingest",
    toolset="temporal_knowledge",
    schema={
        "name": "temporal_ingest",
        "description": "Ingest the Itô ledger into the temporal knowledge service. Run periodically to keep the verification layer up to date.",
        "parameters": {"type": "object", "properties": {}},
    },
    handler=lambda args, **kw: temporal_ingest(task_id=kw.get("task_id")),
    check_fn=check_requirements,
)

registry.register(
    name="temporal_brief",
    toolset="temporal_knowledge",
    schema={
        "name": "temporal_brief",
        "description": "Get a memory brief for a deal: current facts plus what changed recently.",
        "parameters": {
            "type": "object",
            "properties": {
                "deal_key": {"type": "string", "description": "Deal or counterparty identifier"},
            },
            "required": ["deal_key"],
        },
    },
    handler=lambda args, **kw: temporal_brief(
        deal_key=args.get("deal_key", ""),
        task_id=kw.get("task_id"),
    ),
    check_fn=check_requirements,
)


def register(ctx):
    """Plugin entry point for Hermes PluginManager."""
    ctx.register_tool(
        name="temporal_baseline_check",
        schema={
            "name": "temporal_baseline_check",
            "description": "Verify current valid facts for a deal/counterparty before drafting external communication.",
            "parameters": {
                "type": "object",
                "properties": {
                    "deal_key": {"type": "string"},
                    "as_of_ts": {"type": "integer", "default": 0},
                },
                "required": ["deal_key"],
            },
        },
        handler=lambda args, **kw: temporal_baseline_check(
            deal_key=args.get("deal_key", ""),
            as_of_ts=args.get("as_of_ts", 0),
            task_id=kw.get("task_id"),
        ),
    )
    ctx.register_tool(
        name="temporal_ingest",
        schema={
            "name": "temporal_ingest",
            "description": "Ingest ledger into the temporal knowledge service.",
            "parameters": {"type": "object", "properties": {}},
        },
        handler=lambda args, **kw: temporal_ingest(task_id=kw.get("task_id")),
    )
    ctx.register_tool(
        name="temporal_brief",
        schema={
            "name": "temporal_brief",
            "description": "Get a memory brief for a deal.",
            "parameters": {
                "type": "object",
                "properties": {"deal_key": {"type": "string"}},
                "required": ["deal_key"],
            },
        },
        handler=lambda args, **kw: temporal_brief(
            deal_key=args.get("deal_key", ""),
            task_id=kw.get("task_id"),
        ),
    )


def register_cli(subparser):
    """Register `hermes temporal` CLI subcommands."""
    # check
    check_parser = subparser.add_parser("check", help="Baseline check for a deal")
    check_parser.add_argument("deal_key", help="Deal or counterparty key")
    check_parser.add_argument("--as-of", type=int, default=0, help="Unix timestamp")
    check_parser.set_defaults(func=_cli_check)

    # ingest
    ingest_parser = subparser.add_parser("ingest", help="Ingest ledger into temporal graph")
    ingest_parser.set_defaults(func=_cli_ingest)

    # brief
    brief_parser = subparser.add_parser("brief", help="Memory brief for a deal")
    brief_parser.add_argument("deal_key", help="Deal or counterparty key")
    brief_parser.set_defaults(func=_cli_brief)


def _cli_check(args):
    result = temporal_baseline_check(args.deal_key, args.as_of)
    print(result)


def _cli_ingest(args):
    result = temporal_ingest()
    print(result)


def _cli_brief(args):
    result = temporal_brief(args.deal_key)
    print(result)
