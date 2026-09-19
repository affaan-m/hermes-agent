#!/usr/bin/env python3
"""Ingest raw channel events (the deal's actual messages) into the desk Graphiti service.

Root cause this closes: the temporal layer only received obligation lifecycle
replays and workstream artifacts, so supplier terms living in raw channel
messages (e.g. Daryl's "30% down payment" H200 block terms, ito.db events id
49649, 2026-08-28) never became memory facts. When Cam asked about
month-to-month terms for the four-node H200 block on 2026-08-31, the desk had
nothing to retrieve, cited stale ledger terms, claimed "not verified", and
escalated to the principal instead of the supplier.

Source of truth: ito.db ``events`` (canonical store, every channel intake
writes here). Incremental via a checkpoint file on ``events.id``. Loopback
HTTP only; ito.db opened read-only; no sends, no mutations, no provider calls.

Usage:
  python3 ingest_events.py --once [--limit 400]            # one incremental pass
  python3 ingest_events.py --backfill-days 14 [--limit 0]  # bounded replay, then exit
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

HERMES_HOME = Path.home() / ".hermes" / "profiles" / "ito"
ITO_DB = HERMES_HOME / "ito.db"
STATE_DIR = HERMES_HOME / "state"
CHECKPOINT = STATE_DIR / "temporal-events-ingest.json"
SERVICE = os.environ.get("GRAPHITI_SERVICE_URL", "http://127.0.0.1:8098")
GROUP_ID = os.environ.get("GRAPHITI_GROUP_ID", "desk")

# Internal operator noise that is not counterparty memory: the bot's own cron
# and system traffic. Obligation lifecycle state arrives through the separate
# obligations ingest; ops chatter adds extraction cost without deal facts.
EXCLUDE_SENDERS = {"itohermes_bot"}
EXCLUDE_PREFIXES = ("Cronjob Response:",)
# Deal-relevant lanes: direct messages, email, and external shared channels.
# Ops-group traffic is excluded unless it is a DM/email/ext lane.
INCLUDE_CONVERSATION_TYPES = ("DM", "dm")


def _iso(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()


def load_checkpoint() -> dict:
    try:
        data = json.loads(CHECKPOINT.read_text())
        return data if isinstance(data, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def save_checkpoint(last_id: int) -> None:
    STATE_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    tmp = CHECKPOINT.with_suffix(".tmp")
    tmp.write_text(json.dumps({"last_id": last_id, "updated_at": int(time.time())}) + "\n")
    os.chmod(tmp, 0o600)
    os.replace(tmp, CHECKPOINT)


def fetch_events(conn: sqlite3.Connection, after_id: int, min_ts: int, limit: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT id, source, account, channel, channel_id, kind, sender, sender_id,
               ts, text, direction, conversation_type
        FROM events
        WHERE id > ?
          AND ts >= ?
          AND text IS NOT NULL AND TRIM(text) != ''
          AND sender NOT IN (%s)
          AND (
            conversation_type IN ('DM', 'dm')
            OR source = 'email'
            OR channel LIKE 'ext-%%'
          )
        ORDER BY id
        LIMIT ?
        """ % ",".join("?" * len(EXCLUDE_SENDERS)),
        (after_id, min_ts, *EXCLUDE_SENDERS, limit),
    ).fetchall()


def episode_body(row: sqlite3.Row) -> str:
    who = row["sender"] or "unknown"
    where = row["channel"] or row["conversation_type"] or row["source"]
    direction = row["direction"] or ""
    header = f"[{row['source']} {where} {direction} {who} at {_iso(row['ts'])}]"
    return f"{header} {row['text'][:1800]}"


def post_episode(row: sqlite3.Row) -> None:
    resp = requests.post(
        f"{SERVICE}/episode",
        json={
            "group_id": GROUP_ID,
            "name": f"event:{row['source']}:{row['id']}",
            "text": episode_body(row),
            "source": "message",
            "source_description": f"{row['source']}/{row['channel'] or row['conversation_type'] or 'dm'}",
            "reference_time": _iso(row["ts"]),
        },
        timeout=10,
    )
    resp.raise_for_status()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="one incremental pass, then exit")
    parser.add_argument("--backfill-days", type=int, default=0,
                        help="ignore any checkpoint and replay events from this many days back")
    parser.add_argument("--limit", type=int, default=400,
                        help="max episodes this run (0 = unbounded)")
    args = parser.parse_args()
    limit = args.limit if args.limit > 0 else 1_000_000

    checkpoint = load_checkpoint()
    if args.backfill_days > 0:
        after_id = 0
        min_ts = int(time.time()) - args.backfill_days * 86400
    else:
        after_id = int(checkpoint.get("last_id") or 0)
        min_ts = 0
        if after_id == 0:
            # No checkpoint and no explicit backfill: refuse to guess a window.
            print("[events-ingest] no checkpoint; pass --backfill-days N to seed the window", flush=True)
            return 2

    conn = sqlite3.connect(f"file:{ITO_DB}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    rows = fetch_events(conn, after_id, min_ts, limit)
    conn.close()

    posted = 0
    skipped_noise = 0
    last_id = after_id
    last_ok_id = after_id
    failures = 0
    for row in rows:
        last_id = row["id"]
        text = (row["text"] or "").strip()
        if any(text.startswith(prefix) for prefix in EXCLUDE_PREFIXES):
            skipped_noise += 1
            last_ok_id = row["id"]
            continue
        try:
            post_episode(row)
            posted += 1
            last_ok_id = row["id"]
        except requests.RequestException as exc:
            failures += 1
            print(f"[events-ingest] POST failed for event {row['id']}: {exc}", flush=True)
            break

    if args.backfill_days == 0 or after_id == 0:
        # Only advance the incremental checkpoint when we are not in a bounded
        # replay (or when seeding from scratch). The checkpoint covers only
        # durably posted (or deliberately skipped) rows: a failed POST holds
        # the cursor so the event replays next run instead of being lost.
        if last_ok_id > after_id:
            save_checkpoint(last_ok_id)

    pending = None
    try:
        pending = requests.get(f"{SERVICE}/health", timeout=5).json().get("pending")
    except requests.RequestException:
        pass
    print(
        f"[events-ingest] posted={posted} skipped_noise={skipped_noise} failures={failures} "
        f"last_id={last_id} queue_pending={pending}",
        flush=True,
    )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
