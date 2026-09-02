#!/usr/bin/env python3
"""Ingest the Itô ledger into the desk Graphiti service.

Follows the upstream reindex-memory.mjs pattern: reads the durable ground
truth (ito.db obligations + workstream artifacts) and replays them as
episodes into the graph so the temporal layer is rebuilt from source.

Usage:
  python3 ingest_ledger.py [--group-id desk] [--db ~/.hermes/profiles/ito/ito.db]
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import requests


def _ts_to_iso(ts: int | None) -> str:
    if not ts:
        return datetime.now(timezone.utc).isoformat()
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def post_episode(url: str, group_id: str, text: str, name: str, reference_time: str, source: str = "json"):
    resp = requests.post(
        f"{url}/episode",
        json={
            "group_id": group_id,
            "text": text,
            "name": name,
            "source": source,
            "reference_time": reference_time,
        },
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def flush(url: str):
    resp = requests.post(f"{url}/flush", timeout=300)
    resp.raise_for_status()
    return resp.json()


def ingest_obligations(url: str, group_id: str, db_path: Path, batch_size: int = 200):
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """SELECT id, counterparty, direction, status, opened_ts, last_touch_ts,
                  expires_ts, summary, updated_at
           FROM obligations ORDER BY id"""
    )
    rows = cur.fetchall()
    print(f"[ingest] {len(rows)} obligations to replay")

    for row in rows:
        oblig_id = row["id"]
        counterparty = row["counterparty"] or "unknown"
        status = row["status"] or "open"
        summary = (row["summary"] or "").strip()
        opened = _ts_to_iso(row["opened_ts"])
        updated = _ts_to_iso(row["updated_at"] or row["opened_ts"])

        # Build a structured episode body for LLM extraction
        body = {
            "type": "obligation_lifecycle",
            "obligation_id": oblig_id,
            "counterparty": counterparty,
            "direction": row["direction"],
            "status": status,
            "opened_at": opened,
            "updated_at": updated,
            "summary": summary[:2000],
        }
        text = json.dumps(body, indent=2)

        post_episode(
            url,
            group_id,
            text,
            name=f"obligation-{oblig_id}-{status}",
            reference_time=updated,
            source="json",
        )
        if oblig_id % 50 == 0:
            print(f"[ingest] queued obligation {oblig_id}")

    conn.close()
    return len(rows)


def ingest_workstream(url: str, group_id: str, ws_dir: Path):
    import glob
    count = 0
    for path in sorted(glob.glob(str(ws_dir) + "/*.json")):
        p = Path(path)
        name = p.name.lower()
        if "pluto" not in name and "obligation" not in name:
            continue
        try:
            data = json.loads(p.read_text())
        except Exception:
            continue
        if not isinstance(data, dict):
            continue

        oblig_id = data.get("obligation_id") or data.get("obligation")
        if not oblig_id:
            continue

        body = {
            "type": "workstream_receipt",
            "obligation_id": oblig_id,
            "status": data.get("status"),
            "superseded_by": data.get("superseded_by"),
            "recorded_at": _ts_to_iso(data.get("recorded_at")),
            "file": p.name,
        }
        text = json.dumps(body, indent=2)
        post_episode(
            url,
            group_id,
            text,
            name=f"receipt-{p.stem}",
            reference_time=_ts_to_iso(data.get("recorded_at")),
            source="json",
        )
        count += 1
    print(f"[ingest] queued {count} workstream receipts")
    return count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8098")
    parser.add_argument("--group-id", default="desk")
    parser.add_argument("--db", default="~/.hermes/profiles/ito/ito.db")
    parser.add_argument("--workstream", default="~/.codex/workstream-results")
    parser.add_argument("--skip-obligations", action="store_true")
    parser.add_argument("--skip-workstream", action="store_true")
    args = parser.parse_args()

    db_path = Path(args.db).expanduser()
    ws_path = Path(args.workstream).expanduser()

    if not args.skip_obligations:
        n = ingest_obligations(args.url, args.group_id, db_path)
        print(f"[ingest] obligations done: {n}")

    if not args.skip_workstream:
        w = ingest_workstream(args.url, args.group_id, ws_path)
        print(f"[ingest] workstream done: {w}")

    print("[ingest] flushing queue...")
    result = flush(args.url)
    print(f"[ingest] flush complete: {result}")


if __name__ == "__main__":
    main()
