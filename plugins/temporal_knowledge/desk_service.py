"""Desk-side temporal knowledge service.

API-compatible with ito-cloud-runtime/agent-fleet/graphiti/service.py but
backed by SQLite for local use (no Docker/FalkorDB/OpenAI required).

Same endpoints: /health, /episode, /search, /brief, /baseline
Same group_id silo convention.
Same async ingestion queue.

When OpenAI/Nous credentials are available, swap the backend to real
Graphiti by changing GRAPHITI_BACKEND=graphiti and pointing at FalkorDB.
"""
from __future__ import annotations

import asyncio
import hmac
import json
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel


# ---------------------------------------------------------------------------
# SQLite temporal graph (same semantics as Graphiti: bi-temporal edges)
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS entities (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL,
    name        TEXT NOT NULL,
    canonical_key TEXT NOT NULL UNIQUE,
    created_ts  INTEGER NOT NULL,
    metadata    TEXT
);

CREATE TABLE IF NOT EXISTS facts (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_id    INTEGER NOT NULL REFERENCES entities(id),
    predicate     TEXT NOT NULL,
    object_value  TEXT,
    object_id     INTEGER REFERENCES entities(id),
    valid_from    INTEGER NOT NULL,
    valid_to      INTEGER,
    recorded_at   INTEGER NOT NULL,
    source_ref    TEXT,
    confidence    REAL DEFAULT 1.0,
    metadata      TEXT
);

CREATE INDEX IF NOT EXISTS idx_facts_subject ON facts(subject_id, valid_from DESC);
CREATE INDEX IF NOT EXISTS idx_facts_predicate ON facts(predicate, valid_from DESC);
CREATE INDEX IF NOT EXISTS idx_facts_valid ON facts(valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_facts_recorded ON facts(recorded_at DESC);

CREATE TABLE IF NOT EXISTS episodes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id    TEXT NOT NULL,
    name        TEXT,
    body        TEXT NOT NULL,
    source      TEXT,
    reference_ts INTEGER NOT NULL,
    ingested_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_episodes_group ON episodes(group_id, reference_ts DESC);
"""


class TemporalGraph:
    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def upsert_entity(self, kind: str, name: str, canonical_key: str, created_ts: int, metadata: dict | None = None) -> int:
        cur = self._conn.execute(
            """INSERT INTO entities (kind, name, canonical_key, created_ts, metadata)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(canonical_key) DO UPDATE SET name=excluded.name, metadata=excluded.metadata
               RETURNING id""",
            (kind, name, canonical_key, created_ts, json.dumps(metadata or {})),
        )
        row = cur.fetchone()
        self._conn.commit()
        return row[0]

    def add_fact(self, subject_id: int, predicate: str, object_value: str | None = None,
                 object_id: int | None = None, valid_from: int = 0, valid_to: int | None = None,
                 recorded_at: int = 0, source_ref: str = "", metadata: dict | None = None) -> int:
        if recorded_at == 0:
            recorded_at = int(time.time())
        cur = self._conn.execute(
            """INSERT INTO facts (subject_id, predicate, object_value, object_id, valid_from, valid_to, recorded_at, source_ref, metadata)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id""",
            (subject_id, predicate, object_value, object_id, valid_from, valid_to, recorded_at, source_ref, json.dumps(metadata or {})),
        )
        row = cur.fetchone()
        self._conn.commit()
        return row[0]

    def close_fact(self, fact_id: int, valid_to: int):
        self._conn.execute("UPDATE facts SET valid_to = ? WHERE id = ?", (valid_to, fact_id))
        self._conn.commit()

    def query_at(self, subject_key: str, ts: int, predicates: list[str] | None = None) -> list[dict]:
        cur = self._conn.execute("SELECT id FROM entities WHERE canonical_key = ?", (subject_key,))
        row = cur.fetchone()
        if not row:
            return []
        sql = """SELECT * FROM facts WHERE subject_id = ? AND valid_from <= ? AND (valid_to IS NULL OR valid_to > ?)"""
        params: list = [row[0], ts, ts]
        if predicates:
            sql += f" AND predicate IN ({','.join('?' * len(predicates))})"
            params.extend(predicates)
        sql += " ORDER BY valid_from DESC"
        return [dict(r) for r in self._conn.execute(sql, params).fetchall()]

    def query_between(self, subject_key: str, start_ts: int, end_ts: int, predicates: list[str] | None = None) -> list[dict]:
        cur = self._conn.execute("SELECT id FROM entities WHERE canonical_key = ?", (subject_key,))
        row = cur.fetchone()
        if not row:
            return []
        sql = """SELECT * FROM facts WHERE subject_id = ? AND valid_from <= ? AND (valid_to IS NULL OR valid_to >= ?)"""
        params: list = [row[0], end_ts, start_ts]
        if predicates:
            sql += f" AND predicate IN ({','.join('?' * len(predicates))})"
            params.extend(predicates)
        sql += " ORDER BY valid_from"
        return [dict(r) for r in self._conn.execute(sql, params).fetchall()]

    def add_episode(self, group_id: str, name: str, body: str, reference_ts: int, source: str = "json"):
        self._conn.execute(
            "INSERT INTO episodes (group_id, name, body, source, reference_ts, ingested_at) VALUES (?, ?, ?, ?, ?, ?)",
            (group_id, name, body, source, reference_ts, int(time.time())),
        )
        self._conn.commit()

    def close(self):
        self._conn.close()


# ---------------------------------------------------------------------------
# Ingestion logic (deterministic, no LLM)
# ---------------------------------------------------------------------------

def _canonical_deal_key(counterparty: str) -> str:
    import re
    s = counterparty.strip().lower()
    s = re.sub(r"<@[uw][a-z0-9]+>", "", s, flags=re.I)
    s = re.sub(r"#[a-z0-9-]+", "", s, flags=re.I)
    s = re.sub(r"\s+", " ", s).strip()
    for token in ["ronit", "cam", "joe", "akash", "pluto", "strike", "daryl", "sandeep"]:
        if token in s:
            return token
    return s.split()[0] if s else "unknown"


def _extract_ask(summary: str) -> dict | None:
    import re
    s = summary.lower()
    if "node" not in s or ("h200" not in s and "h100" not in s):
        return None
    m = re.search(r"(\d+)\s*(?:x\s*)?node", s)
    nodes = int(m.group(1)) if m else None
    gpu = "h200" if "h200" in s else "h100"
    return {"nodes": nodes, "gpu": gpu}


def _is_contract_event(summary: str, status: str) -> bool:
    s = summary.lower()
    keywords = ["contract", "signed", "docusign", "delivered", "agreement"]
    return any(k in s for k in keywords) and status in ("closed", "closed_already_handled", "sent", "approved")


def process_obligation(graph: TemporalGraph, row: dict, group_id: str):
    now = int(time.time())
    oblig_id = row["id"]
    counterparty = row.get("counterparty") or "unknown"
    status = row.get("status") or "open"
    summary = row.get("summary") or ""
    opened_ts = row.get("opened_ts") or now
    updated_ts = row.get("updated_at") or opened_ts

    deal_key = f"deal:{_canonical_deal_key(counterparty)}"
    oblig_key = f"obligation:{oblig_id}"

    deal_id = graph.upsert_entity("deal", _canonical_deal_key(counterparty), deal_key, opened_ts, {"counterparties": [counterparty]})
    oblig_id_e = graph.upsert_entity("obligation", f"obligation-{oblig_id}", oblig_key, opened_ts,
                                     {"counterparty": counterparty, "direction": row.get("direction"), "status": status, "summary": summary[:500]})

    # Facts — deduplicate by closing existing open facts of same predicate from same source
    valid_to = updated_ts if status.startswith("closed") else None
    
    # Close any existing open facts for this obligation before re-adding
    for pred in ["has_status", "owes", "part_of", "has_ask", "contract_state"]:
        existing = graph._conn.execute(
            "SELECT id FROM facts WHERE subject_id IN (?, ?) AND predicate = ? AND source_ref = ? AND valid_to IS NULL",
            (oblig_id_e, deal_id, pred, f"obligation:{oblig_id}")
        ).fetchall()
        for ex in existing:
            graph.close_fact(ex[0], updated_ts)

    graph.add_fact(oblig_id_e, "has_status", status, valid_from=opened_ts, valid_to=valid_to, recorded_at=updated_ts, source_ref=f"obligation:{oblig_id}")
    graph.add_fact(oblig_id_e, "owes", row.get("direction"), valid_from=opened_ts, valid_to=valid_to, recorded_at=updated_ts, source_ref=f"obligation:{oblig_id}")
    graph.add_fact(oblig_id_e, "part_of", object_id=deal_id, valid_from=opened_ts, valid_to=valid_to, recorded_at=updated_ts, source_ref=f"obligation:{oblig_id}")

    ask = _extract_ask(summary)
    if ask:
        ask_json = json.dumps(ask)
        graph.add_fact(oblig_id_e, "has_ask", ask_json, valid_from=opened_ts, valid_to=valid_to, recorded_at=updated_ts, source_ref=f"obligation:{oblig_id}")
        graph.add_fact(deal_id, "has_ask", ask_json, valid_from=opened_ts, valid_to=valid_to, recorded_at=updated_ts, source_ref=f"obligation:{oblig_id}")

    if _is_contract_event(summary, status):
        graph.add_fact(deal_id, "contract_state", "signed_or_delivered", valid_from=updated_ts, recorded_at=updated_ts, source_ref=f"obligation:{oblig_id}")

    # Supersession
    if status == "closed_superseded":
        import re
        m = re.search(r"superseded by obligation (\d+)", summary)
        if m:
            sup_id = int(m.group(1))
            sup_cur = graph._conn.execute("SELECT id FROM entities WHERE canonical_key = ?", (f"obligation:{sup_id}",))
            sup_row = sup_cur.fetchone()
            if sup_row:
                graph.add_fact(oblig_id_e, "superseded_by", object_id=sup_row[0], valid_from=updated_ts, recorded_at=now, source_ref=f"obligation:{oblig_id}")

    graph.add_episode(group_id, f"obligation-{oblig_id}-{status}", json.dumps(row), updated_ts)


# ---------------------------------------------------------------------------
# FastAPI service (same shape as upstream)
# ---------------------------------------------------------------------------

STATE = {"graph": None, "queue": None, "workers": [], "ingested": 0, "failed": 0}
GROUP_LOCKS: dict[str, asyncio.Lock] = {}


def _group_lock(gid: str) -> asyncio.Lock:
    lk = GROUP_LOCKS.get(gid)
    if lk is None:
        lk = asyncio.Lock()
        GROUP_LOCKS[gid] = lk
    return lk


async def _ingest_worker(graph: TemporalGraph, queue: asyncio.Queue):
    while True:
        job = await queue.get()
        try:
            async with _group_lock(job["group_id"]):
                if job.get("type") == "obligation":
                    process_obligation(graph, job["data"], job["group_id"])
                else:
                    # Generic episode — just store it
                    graph.add_episode(job["group_id"], job.get("name", "episode"), job["text"],
                                      int(datetime.fromisoformat(job.get("reference_time", datetime.now(timezone.utc).isoformat()).replace("Z", "+00:00")).timestamp()))
                STATE["ingested"] += 1
        except Exception as exc:
            STATE["failed"] += 1
            print(f"[temporal] ingest failed: {exc}")
        finally:
            queue.task_done()


@asynccontextmanager
async def lifespan(app: FastAPI):
    db_path = Path(os.environ.get("GRAPHITI_DB", "~/.hermes/profiles/ito/temporal_graph.db")).expanduser()
    graph = TemporalGraph(db_path)
    queue: asyncio.Queue = asyncio.Queue()
    concurrency = max(1, int(os.environ.get("GRAPHITI_INGEST_CONCURRENCY", "3")))
    workers = [asyncio.create_task(_ingest_worker(graph, queue)) for _ in range(concurrency)]
    STATE.update(graph=graph, queue=queue, workers=workers)
    try:
        yield
    finally:
        for w in workers:
            w.cancel()
        graph.close()


app = FastAPI(lifespan=lifespan)
GRAPHITI_TOKEN = os.environ.get("GRAPHITI_TOKEN", "").strip()
_OPEN_PATHS = {"/health"}


@app.middleware("http")
async def _require_token(request: Request, call_next):
    if GRAPHITI_TOKEN and request.url.path not in _OPEN_PATHS:
        presented = request.headers.get("authorization", "")
        if not hmac.compare_digest(presented, f"Bearer {GRAPHITI_TOKEN}"):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
    return await call_next(request)


class EpisodeIn(BaseModel):
    group_id: str
    text: str
    name: str | None = None
    source: str | None = None
    source_description: str | None = None
    channel: str | None = None
    reference_time: str | None = None


class SearchIn(BaseModel):
    group_id: str
    group_ids: list[str] | None = None
    query: str = ""
    limit: int = 20
    as_of: str | None = None
    center: str | None = None

    def groups(self) -> list[str]:
        return self.group_ids if self.group_ids else [self.group_id]


def _fact_to_response(f: dict) -> dict:
    return {
        "fact": f"{f['predicate']}: {f.get('object_value', '')}",
        "valid_at": datetime.fromtimestamp(f["valid_from"], tz=timezone.utc).isoformat() if f.get("valid_from") else None,
        "invalid_at": datetime.fromtimestamp(f["valid_to"], tz=timezone.utc).isoformat() if f.get("valid_to") else None,
        "created_at": datetime.fromtimestamp(f["recorded_at"], tz=timezone.utc).isoformat() if f.get("recorded_at") else None,
        "name": f.get("source_ref"),
        "current": f.get("valid_to") is None,
        "predicate": f["predicate"],
        "value": f.get("object_value"),
    }


@app.get("/health")
async def health():
    q = STATE["queue"]
    return {"ok": STATE["graph"] is not None, "ingested": STATE["ingested"], "failed": STATE["failed"], "pending": q.qsize() if q else None}


@app.post("/episode")
async def episode(ep: EpisodeIn):
    STATE["queue"].put_nowait({"type": "episode", **ep.model_dump()})
    return {"queued": True, "pending": STATE["queue"].qsize()}


@app.post("/flush")
async def flush():
    await STATE["queue"].join()
    return {"ok": True, "ingested": STATE["ingested"], "failed": STATE["failed"]}


@app.post("/search")
async def search(q: SearchIn):
    graph: TemporalGraph = STATE["graph"]
    ts = int(time.time()) if not q.as_of else int(datetime.fromisoformat(q.as_of.replace("Z", "+00:00")).timestamp())
    all_facts = []
    for gid in q.groups():
        # Search episodes for query terms
        cur = graph._conn.execute(
            "SELECT DISTINCT subject_id FROM facts WHERE source_ref LIKE ? LIMIT ?",
            (f"%{q.query}%", q.limit),
        )
        for row in cur.fetchall():
            facts = graph.query_at(f"obligation:{row[0]}", ts)
            all_facts.extend(facts)
    return {"group_id": q.group_id, "as_of": q.as_of, "facts": [_fact_to_response(f) for f in all_facts[:q.limit]]}


@app.post("/brief")
async def brief(q: SearchIn):
    graph: TemporalGraph = STATE["graph"]
    ts = int(time.time()) if not q.as_of else int(datetime.fromisoformat(q.as_of.replace("Z", "+00:00")).timestamp())
    deal_key = f"deal:{q.center}" if not q.center.startswith("deal:") else q.center
    current = graph.query_at(deal_key, ts)
    changed = graph.query_between(deal_key, ts - 48 * 3600, ts)
    changed = [f for f in changed if f.get("valid_to") is not None]
    lines = [f"Memory brief — {q.center or 'the desk'}:"]
    if not current and not changed:
        lines.append("  (no memory yet)")
    for f in current[:q.limit]:
        lines.append(f"  • {f['predicate']}: {f.get('object_value', '')}")
    if changed:
        lines.append("  Changed over time:")
        for f in changed[:3]:
            lines.append(f"    ↪ was: {f['predicate']}: {f.get('object_value', '')} (until {datetime.fromtimestamp(f['valid_to'], tz=timezone.utc).isoformat()})")
    return {"text": "\n".join(lines), "current": [_fact_to_response(f) for f in current[:q.limit]], "changed": [_fact_to_response(f) for f in changed[:3]]}


class BaselineIn(BaseModel):
    group_id: str
    deal_key: str
    as_of: str | None = None


@app.post("/baseline")
async def baseline(q: BaselineIn):
    graph: TemporalGraph = STATE["graph"]
    ts = int(time.time()) if not q.as_of else int(datetime.fromisoformat(q.as_of.replace("Z", "+00:00")).timestamp())
    deal_key = f"deal:{q.deal_key}" if not q.deal_key.startswith("deal:") else q.deal_key

    current = graph.query_at(deal_key, ts)
    contract_facts = [f for f in current if f["predicate"] == "contract_state"]
    ask_facts = [f for f in current if f["predicate"] == "has_ask"]

    has_contract = any("signed" in str(f.get("object_value", "")).lower() or "delivered" in str(f.get("object_value", "")).lower() for f in contract_facts)

    # Filter out asks that were created AFTER a contract was already signed
    # (these are stale-baseline artifacts, not legitimate open asks)
    if has_contract and contract_facts:
        earliest_contract_ts = min(f["valid_from"] for f in contract_facts)
        ask_facts = [f for f in ask_facts if f["valid_from"] < earliest_contract_ts]

    return {
        "deal_key": q.deal_key,
        "as_of": q.as_of,
        "has_signed_contract": has_contract,
        "contract_facts": [_fact_to_response(f) for f in contract_facts],
        "open_asks": [_fact_to_response(f) for f in ask_facts],
        "recommendation": "DO NOT ASK ABOUT SPECS — contract already signed/delivered" if has_contract else "Baseline valid — no signed contract on record",
    }


# ---------------------------------------------------------------------------
# Ingestion endpoint for ledger data
# ---------------------------------------------------------------------------

class ObligationIn(BaseModel):
    group_id: str
    data: dict


@app.post("/ingest/obligation")
async def ingest_obligation(item: ObligationIn):
    STATE["queue"].put_nowait({"type": "obligation", **item.model_dump()})
    return {"queued": True, "pending": STATE["queue"].qsize()}


@app.post("/ingest/ledger")
async def ingest_ledger(group_id: str = "desk", db_path: str = "~/.hermes/profiles/ito/ito.db"):
    """Bulk ingest from ito.db. Runs synchronously for simplicity."""
    graph: TemporalGraph = STATE["graph"]
    path = Path(db_path).expanduser()
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    cur = conn.execute("SELECT * FROM obligations ORDER BY id")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    for row in rows:
        await STATE["queue"].put({"type": "obligation", "group_id": group_id, "data": row})
    await STATE["queue"].join()
    return {"ok": True, "ingested": len(rows)}
