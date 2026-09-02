"""Desk temporal knowledge service — REAL graphiti-core edition.

Same API surface as the SQLite prototype (/health, /episode, /search, /brief,
/baseline, /ingest/ledger), now backed by graphiti-core with:
  - KuzuDriver (embedded graph store; no Docker/FalkorDB needed locally)
  - OpenAIClient with GRAPHITI_MODEL (default gpt-5.6-luna, aligned with
    upstream ito-cloud-runtime PR #1452)
  - OpenAIEmbedder with text-embedding-3-large
  - API key resolved from 1Password via op (never printed, never stored):
    op://Ito/OpenAI - ALL KEYS/ITO_INTERNAL_EMBEDDINGS_API_KEY
    (name-based ref to the Ito vault copy; both models allow-listed)

The desk ontology (ontology.py) pins entity/edge extraction to desk objects:
Counterparty, Obligation, Contract, Approval, Deal + HasStatus, SupersededBy,
SignedContract, HasAsk, Owes.

Env:
  GRAPHITI_DB       default ~/.hermes/profiles/ito/graphiti_desk
  GRAPHITI_PORT     default 8098
  GRAPHITI_MODEL    default gpt-5.6-luna
  GRAPHITI_EMBED_MODEL default text-embedding-3-large
  OP_TOKEN_ENV      env var holding the 1Password SA token (default:
                    OP_SERVICE_ACCOUNT_TOKEN, sourced from broker.env)
"""
from __future__ import annotations

import asyncio
import hmac
import json
import os
import re
import sqlite3
import subprocess
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from graphiti_core import Graphiti
from graphiti_core.nodes import EpisodeType
from graphiti_core.driver.kuzu_driver import KuzuDriver
from graphiti_core.llm_client.openai_client import OpenAIClient
from graphiti_core.llm_client.config import LLMConfig
from graphiti_core.embedder.openai import OpenAIEmbedder, OpenAIEmbedderConfig

try:
    from .ontology import ENTITY_TYPES, EDGE_TYPES, EDGE_TYPE_MAP
    from . import memory_layers as ml
except ImportError:  # running as a script from the plugin dir
    from ontology import ENTITY_TYPES, EDGE_TYPES, EDGE_TYPE_MAP
    import memory_layers as ml

STATE = {"graphiti": None, "queue": None, "workers": [], "ingested": 0, "failed": 0,
         "llm": None, "dedup_merged": 0, "evolution": {}, "communities": 0}
GROUP_LOCKS: dict[str, asyncio.Lock] = {}


def _group_lock(gid: str) -> asyncio.Lock:
    lk = GROUP_LOCKS.get(gid)
    if lk is None:
        lk = asyncio.Lock()
        GROUP_LOCKS[gid] = lk
    return lk


def _resolve_api_key() -> str:
    """Resolve the Ito Internal OpenAI key from 1Password via op. Never printed."""
    ref = "op://Ito/OpenAI - ALL KEYS/ITO_INTERNAL_EMBEDDINGS_API_KEY"
    try:
        r = subprocess.run(
            ["op", "read", ref, "--no-newline"],
            capture_output=True, text=True, timeout=30, env=os.environ,
        )
        key = r.stdout.strip()
        if key:
            return key
    except Exception:
        pass
    # fallback: an env var may hold it directly (e.g. local dev)
    key = os.environ.get("ITO_INTERNAL_EMBEDDINGS_API_KEY", "").strip()
    if key:
        return key
    raise RuntimeError("could not resolve the Ito Internal OpenAI key from 1Password")


def _parse_time(value):
    if not value:
        return datetime.now(timezone.utc)
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return datetime.now(timezone.utc)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


# Kuzu's native storage engine SIGSEGVs under concurrent read+write (crash
# reports Python-2026-09-01-161919 etc.: NodeTable::initScanState under
# update while a /search ran during bulk ingest), so on the Kuzu backend ALL
# graph access serializes through _GRAPH_RW_LOCK. Neo4j is fully ACID with
# concurrent transactions — the lock would only stall searches behind bulk
# ingests, so the neo4j backend bypasses it (per-group write serialization
# stays via _group_lock).
_GRAPH_RW_LOCK = asyncio.Lock()


def _graph_lock():
    """The global R/W lock on Kuzu; a no-op context on Neo4j."""
    if STATE.get("backend") == "neo4j":
        class _Noop:
            async def __aenter__(self): return None
            async def __aexit__(self, *exc): return False
        return _Noop()
    return _GRAPH_RW_LOCK


async def _ingest_worker(g: Graphiti, queue: asyncio.Queue):
    while True:
        job = await queue.get()
        try:
            async with _graph_lock():
                async with _group_lock(job["group_id"]):
                    await _apply(g, job)
        finally:
            queue.task_done()


async def _apply(g: Graphiti, job: dict):
    try:
        # "message" must stay EpisodeType.message: text-type extraction of
        # channel traffic returns ZERO edges on this model (proven 2026-09-01:
        # same Daryl terms episode → 5 edges as message, 0 as text). Mapping
        # everything non-json to text silently starved the entire events
        # pipeline of facts.
        kwargs = dict(
            name=job.get("name") or "episode",
            episode_body=job["text"],
            source={"json": EpisodeType.json, "message": EpisodeType.message}.get(
                job.get("source"), EpisodeType.text
            ),
            source_description=job.get("source_description") or job.get("channel") or "desk",
            reference_time=_parse_time(job.get("reference_time")),
            group_id=job["group_id"],
        )
        try:
            result = await g.add_episode(
                **kwargs,
                entity_types=ENTITY_TYPES,
                edge_types=EDGE_TYPES,
                edge_type_map=EDGE_TYPE_MAP,
            )
        except TypeError:
            result = await g.add_episode(**kwargs)
        STATE["ingested"] += 1
        # L2 dedup merge + L4 evolution on the freshly created edges
        # (Kuzu-schema-specific; skipped on the Neo4j backend until ported.)
        try:
            new_edges = list(getattr(result, "edges", []) or [])
            driver = STATE["graphiti"].driver
            if STATE.get("backend") != "neo4j":
                d = await ml.dedup_new_facts(driver, new_edges)
                STATE["dedup_merged"] += d.get("merged", 0)
                if STATE.get("llm") is not None and new_edges:
                    evo = await ml.evolution_pass(driver, STATE["llm"], new_edges, job["group_id"])
                    for k, v in evo.items():
                        STATE["evolution"][k] = STATE["evolution"].get(k, 0) + v
        except Exception as exc:
            print(f"[graphiti-desk] memory-layer pass failed (non-fatal): {exc}")
    except Exception as exc:
        STATE["failed"] += 1
        import traceback
        print(f"[graphiti-desk] ingest failed for group={job.get('group_id')}: {exc}")
        traceback.print_exc()


async def _ensure_fts_indexes(driver: KuzuDriver) -> None:
    """graphiti-core 0.29's deprecated Kuzu driver never executes the FTS index
    creation that its own search path requires (graph_queries.py). Create the
    four indexes here; existing-index errors are ignored."""
    import kuzu
    statements = [
        "CREATE_FTS_INDEX('Community', 'community_name', ['name'])",
        "CREATE_FTS_INDEX('Entity', 'node_name_and_summary', ['name', 'summary'])",
        "CREATE_FTS_INDEX('Episodic', 'episode_content', ['content', 'source', 'source_description'])",
        "CREATE_FTS_INDEX('RelatesToNode_', 'edge_name_and_fact', ['name', 'fact'])",
    ]
    conn = kuzu.Connection(driver.db)
    for stmt in statements:
        try:
            conn.execute(f"CALL {stmt}")
        except Exception:
            pass  # already exists
    conn.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    api_key = _resolve_api_key()
    os.environ["OPENAI_API_KEY"] = api_key  # graphiti's openai clients read env
    model = os.environ.get("GRAPHITI_MODEL", "gpt-5.6-luna")
    small = os.environ.get("GRAPHITI_SMALL_MODEL", model)
    llm = OpenAIClient(
        config=LLMConfig(api_key=api_key, model=model, small_model=small),
        reasoning="none",  # gpt-5.6-luna rejects 'minimal'; 'none' = upstream's fast extraction tier
    )
    embedder = OpenAIEmbedder(config=OpenAIEmbedderConfig(
        api_key=api_key,
        embedding_model=os.environ.get("GRAPHITI_EMBED_MODEL", "text-embedding-3-large"),
        embedding_dim=int(os.environ.get("GRAPHITI_EMBED_DIM", "1024")),
    ))
    backend = os.environ.get("GRAPHITI_BACKEND", "kuzu").strip().lower()
    if backend == "neo4j":
        # Kuzu's unmaintained native engine SIGSEGVd under this workload
        # (five+ crash reports, NodeTable::initScanState under update, even
        # single-writer with serialized reads). Neo4j is graphiti-core's
        # primary, maintained backend; runs loopback-only via brew services.
        from graphiti_core.driver.neo4j_driver import Neo4jDriver
        driver = Neo4jDriver(
            os.environ.get("GRAPHITI_NEO4J_URI", "bolt://127.0.0.1:7687"),
            os.environ.get("GRAPHITI_NEO4J_USER", "neo4j"),
            os.environ["GRAPHITI_NEO4J_PASSWORD"],
        )
    else:
        db_path = Path(os.environ.get("GRAPHITI_DB", "~/.hermes/profiles/ito/graphiti_desk")).expanduser()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        driver = KuzuDriver(db=str(db_path))
        driver._database = "desk"  # graphiti-core 0.29 reads driver._database; KuzuDriver never sets it
    g = Graphiti(graph_driver=driver, llm_client=llm, embedder=embedder)
    await g.build_indices_and_constraints()
    if backend != "neo4j":
        await _ensure_fts_indexes(driver)
        await ml.ensure_salience_schema(driver)
    else:
        # The L1-L5 memory-layer enhancements (salience schema, Kuzu FTS
        # indexes, dedup/evolution/PPR writes) are Kuzu-schema-specific
        # (RelatesToNode_ edge table). On Neo4j they are skipped until
        # ported: core graphiti extraction + search are unaffected, salience
        # defaults to 1.0, ranking falls back to graphiti's own order.
        print("[graphiti-desk] neo4j backend: memory-layer enhancements (L1-L5) deferred")
    queue: asyncio.Queue = asyncio.Queue()
    concurrency = max(1, int(os.environ.get("GRAPHITI_INGEST_CONCURRENCY", "3")))
    workers = [asyncio.create_task(_ingest_worker(g, queue)) for _ in range(concurrency)]
    freshness = asyncio.create_task(_freshness_loop())
    STATE.update(graphiti=g, queue=queue, workers=workers, llm=llm, freshness=freshness, backend=backend)
    try:
        yield
    finally:
        for w in workers:
            w.cancel()
        freshness.cancel()
        await g.close()


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


def _edge_to_fact(e):
    attrs = getattr(e, "attributes", None) or {}
    return {
        "uuid": getattr(e, "uuid", None),
        "fact": getattr(e, "fact", None),
        "valid_at": str(getattr(e, "valid_at", None)) if getattr(e, "valid_at", None) else None,
        "invalid_at": str(getattr(e, "invalid_at", None)) if getattr(e, "invalid_at", None) else None,
        "created_at": str(getattr(e, "created_at", None)) if getattr(e, "created_at", None) else None,
        "name": getattr(e, "name", None),
        "current": getattr(e, "invalid_at", None) is None,
        "archived": bool(attrs.get("archived")),
    }


async def _attach_salience(facts: list[dict]) -> list[dict]:
    """Fetch salience for a batch of fact uuids in one query."""
    uuids = [f["uuid"] for f in facts if f.get("uuid")]
    if not uuids or STATE.get("graphiti") is None:
        return facts
    try:
        rows, _, _ = await STATE["graphiti"].driver.execute_query(
            "MATCH (e:RelatesToNode_) WHERE e.uuid IN $uuids "
            "RETURN e.uuid AS u, e.salience AS s",
            uuids=uuids,
        )
        smap = {r["u"]: r["s"] for r in rows or []}
        for f in facts:
            f["salience"] = smap.get(f.get("uuid"), 1.0)
    except Exception:
        for f in facts:
            f.setdefault("salience", 1.0)
    return facts


def _edge_to_fact(e):
    attrs = getattr(e, "attributes", None) or {}
    return {
        "uuid": getattr(e, "uuid", None),
        "src": getattr(e, "source_node_uuid", None),
        "dst": getattr(e, "target_node_uuid", None),
        "fact": getattr(e, "fact", None),
        "valid_at": str(getattr(e, "valid_at", None)) if getattr(e, "valid_at", None) else None,
        "invalid_at": str(getattr(e, "invalid_at", None)) if getattr(e, "invalid_at", None) else None,
        "created_at": str(getattr(e, "created_at", None)) if getattr(e, "created_at", None) else None,
        "name": getattr(e, "name", None),
        "current": getattr(e, "invalid_at", None) is None,
        "archived": bool(attrs.get("archived")),
    }


async def _attach_entities(facts: list[dict]) -> list[dict]:
    """Attach endpoint entity names per fact (one query). Backend-agnostic:
    uses the edge's source/target node uuids, present on every driver."""
    node_uuids = {f["src"] for f in facts if f.get("src")} | {f["dst"] for f in facts if f.get("dst")}
    if not node_uuids or STATE.get("graphiti") is None:
        return facts
    try:
        rows, _, _ = await STATE["graphiti"].driver.execute_query(
            "MATCH (n:Entity) WHERE n.uuid IN $uuids RETURN n.uuid AS u, n.name AS name",
            uuids=list(node_uuids),
        )
        nmap = {r["u"]: r["name"] for r in rows or []}
        for f in facts:
            f["_entities"] = f"{nmap.get(f.get('src'), '')} {nmap.get(f.get('dst'), '')}".strip()
    except Exception:
        for f in facts:
            f.setdefault("_entities", "")
    return facts


async def _rank_and_reinforce(facts: list[dict], seed: str) -> list[dict]:
    """Attach salience, rank by PPR x salience (L3), reinforce the winners (L1).
    Archived facts (L5) never rank."""
    facts = [f for f in facts if not f.get("archived")]
    if STATE.get("backend") == "neo4j":
        for f in facts:
            f.setdefault("salience", 1.0)
        return facts
    await _attach_salience(facts)
    try:
        ppr = await ml.ppr_rank(STATE["graphiti"].driver, [seed], facts)
        facts = ml.blend_rank(facts, ppr)
    except Exception:
        pass
    if STATE.get("backend") != "neo4j":
        await ml.reinforce(STATE["graphiti"].driver, [f["uuid"] for f in facts[:15] if f.get("uuid")])
    return facts


@app.get("/health")
async def health():
    q = STATE["queue"]
    return {
        "ok": STATE["graphiti"] is not None,
        "backend": f"graphiti-core+{STATE.get('backend', 'kuzu')}",
        "ingested": STATE["ingested"],
        "failed": STATE["failed"],
        "pending": q.qsize() if q else None,
        "memory_layers": {
            "dedup_merged": STATE["dedup_merged"],
            "evolution": STATE["evolution"],
            "community_runs": STATE["communities"],
        },
    }


@app.post("/episode")
async def episode(ep: EpisodeIn):
    STATE["queue"].put_nowait(ep.model_dump())
    return {"queued": True, "pending": STATE["queue"].qsize()}


@app.post("/flush")
async def flush():
    await STATE["queue"].join()
    return {"ok": True, "ingested": STATE["ingested"], "failed": STATE["failed"]}


@app.post("/search")
async def search(q: SearchIn):
    g: Graphiti = STATE["graphiti"]
    query = q.query or (q.center or "")
    try:
        async with _graph_lock():
            results = await g.search(query, group_ids=q.groups(), num_results=q.limit)
    except Exception as exc:
        print(f"[graphiti-desk] search failed for groups={q.groups()}: {exc}")
        return {"group_id": q.group_id, "as_of": q.as_of, "facts": [], "error": "search_failed"}
    facts = [_edge_to_fact(e) for e in results]
    if q.as_of:
        t = q.as_of.replace("Z", "+00:00")
        def alive(f):
            if f["valid_at"] and f["valid_at"] > t:
                return False
            if f["invalid_at"] and f["invalid_at"] <= t:
                return False
            return True
        facts = [f for f in facts if alive(f)]
    async with _graph_lock():
        facts = await _rank_and_reinforce(facts, q.center or q.query or q.group_id)
    return {"group_id": q.group_id, "as_of": q.as_of, "facts": facts}


@app.post("/brief")
async def brief(q: SearchIn):
    g: Graphiti = STATE["graphiti"]
    query = q.center or q.query or "everything"
    try:
        async with _graph_lock():
            results = await g.search(query, group_ids=q.groups(), num_results=max(q.limit, 25))
    except Exception as exc:
        print(f"[graphiti-desk] brief search failed for groups={q.groups()}: {exc}")
        return {"text": f"Memory brief — {q.center or 'the desk'}: (unavailable)", "current": [], "changed": []}
    facts = [_edge_to_fact(e) for e in results]
    async with _graph_lock():
        facts = await _rank_and_reinforce(facts, q.center or q.query or q.group_id)
    current = [f for f in facts if f["current"]]
    changed = [f for f in facts if not f["current"]]
    lines = [f"Memory brief — {q.center or 'the desk'} (via Graphiti):"]
    if not current and not changed:
        lines.append("  (no memory yet)")
    for f in current[: q.limit]:
        lines.append(f"  • {f['fact']}")
    if changed:
        lines.append("  Changed over time:")
        for f in changed[:3]:
            lines.append(f"    ↪ was: {f['fact']} (until {f['invalid_at']})")
    return {"text": "\n".join(lines), "current": current[: q.limit], "changed": changed[:3]}


class BaselineIn(BaseModel):
    group_id: str
    deal_key: str
    as_of: str | None = None


_STOP_TOKENS = {
    "test", "counterparty", "deal", "thread", "channel", "slack", "telegram",
    "email", "unknown", "reply", "follow-up", "follow", "spec", "specs", "client",
}


def _deal_tokens(deal_key: str) -> set[str]:
    """Distinctive tokens that identify the deal in fact text. Used as a
    relevance guard: graphiti's semantic search returns *some* facts for any
    query, so a contract fact only counts if it actually names the deal."""
    import re as _re
    words = set(_re.findall(r"[a-z0-9][a-z0-9-]{2,}", deal_key.lower()))
    tokens = {w for w in words if w not in _STOP_TOKENS and len(w) >= 4}
    # keep hyphenated slugs (e.g. test-newdeal-267gate) even if parts are generic
    tokens |= {w for w in words if "-" in w and len(w) >= 8}
    return tokens


async def _expand_deal_tokens(tokens: set[str]) -> set[str]:
    """Expand deal tokens with graph-derived aliases: entities whose name
    contains a token, plus their 1-hop neighbor names. This is how 'pluto'
    learns 'ronit jain' (and 'strike dfs') from the graph itself, so
    contract facts attached to the person still match the lane."""
    if not tokens or STATE.get("graphiti") is None:
        return tokens
    expanded = set(tokens)
    try:
        for tok in list(tokens):
            rows, _, _ = await STATE["graphiti"].driver.execute_query(
                "MATCH (n:Entity)-[*1..2]-(m:Entity) "
                "WHERE toLower(n.name) CONTAINS $tok "
                "RETURN DISTINCT m.name AS name LIMIT 25",
                tok=tok,
            )
            for r in rows or []:
                for w in re.findall(r"[a-z0-9][a-z0-9-]{2,}", (r.get("name") or "").lower()):
                    if len(w) >= 4 and w not in _STOP_TOKENS:
                        expanded.add(w)
    except Exception:
        pass
    return expanded


@app.post("/baseline")
async def baseline(q: BaselineIn):
    """Pre-draft validation: current valid facts for a deal + signed-contract flag."""
    g: Graphiti = STATE["graphiti"]
    try:
        async with _graph_lock():
            contract_results = await g.search(
                f"{q.deal_key} contract signed delivered docusign",
                group_ids=[q.group_id], num_results=25,
            )
            delivery_results = await g.search(
                f"{q.deal_key} delivered review copy package to counterparty channel",
                group_ids=[q.group_id], num_results=15,
            )
            ask_results = await g.search(
                f"{q.deal_key} ask request nodes specs",
                group_ids=[q.group_id], num_results=15,
            )
    except Exception as exc:
        return {"error": str(exc), "recommendation": "CHECK FAILED — treat as stale"}

    seen: set[str] = set()
    merged = []
    for e in list(contract_results) + list(delivery_results):
        u = getattr(e, "uuid", None)
        if u and u not in seen:
            seen.add(u)
            merged.append(e)
    contract_facts = [_edge_to_fact(e) for e in merged]
    ask_facts = [_edge_to_fact(e) for e in ask_results]

    # relevance guard: facts must name the deal — in their text OR via the
    # entities they attach to (the contract instrument, the channel, the
    # counterparty). Without this, an unknown deal inherits whatever the
    # semantic search surfaced; with ONLY text matching, instrument-named
    # facts ("Itô Dedicated Compute Order") get dropped.
    tokens = _deal_tokens(q.deal_key)
    if tokens:
        tokens = await _expand_deal_tokens(tokens)
        contract_facts = await _attach_entities(contract_facts)
        ask_facts = await _attach_entities(ask_facts)

        def relevant(f) -> bool:
            text = ((f["fact"] or "") + " " + f.get("_entities", "")).lower()
            return any(t in text for t in tokens)
        contract_facts = [f for f in contract_facts if relevant(f)]
        ask_facts = [f for f in ask_facts if relevant(f)]

    as_of_dt = _parse_time(q.as_of) if q.as_of else datetime.now(timezone.utc)

    def current_at(f) -> bool:
        """Temporal currentness AT as_of — not today's. A fact is current at T
        when it was valid by T and not yet invalidated by T."""
        if f["valid_at"] and _parse_time(f["valid_at"]) > as_of_dt:
            return False
        if f["invalid_at"] and _parse_time(f["invalid_at"]) <= as_of_dt:
            return False
        return True

    if q.as_of:
        contract_facts = [f for f in contract_facts if current_at(f)]
        ask_facts = [f for f in ask_facts if current_at(f)]

    # signed/delivered means an EXECUTION or DELIVERY event: a docusign
    # ENVELOPE (not a mere PDF artifact), an explicit signature, or a contract
    # package DELIVERED to a counterparty channel. Preparation, drafts, and
    # acknowledgments do not count.
    def is_signed(f):
        if not current_at(f):
            return False
        text = (f["fact"] or "").lower()
        if "countersign" in text or "signed" in text:
            return True
        if "docusign" in text and ("envelope" in text or "sent" in text or "send" in text):
            return True
        if "delivered to" in text and any(k in text for k in ("contract", "agreement", "order", "terms")):
            return True
        return False

    has_contract = any(is_signed(f) for f in contract_facts)

    # Recency guard: a signed contract older than 7 days before as_of belongs
    # to an earlier deal thread (e.g. the July 4xH100 order), not the current
    # one — re-asking specs there is legitimate, not stale.
    if has_contract and q.as_of:
        from datetime import timedelta
        window = as_of_dt - timedelta(days=7)
        has_contract = any(
            is_signed(f) and f["valid_at"] and _parse_time(f["valid_at"]) >= window
            for f in contract_facts
        )

    current_asks = [f for f in ask_facts if current_at(f)]
    # stale-baseline artifacts: asks filed AFTER the contract was signed are
    # not legitimate open asks — they are the rot this layer exists to catch.
    if has_contract:
        signed_va = [ _parse_time(f["valid_at"]) for f in contract_facts if is_signed(f) and f["valid_at"] ]
        if signed_va:
            earliest = min(signed_va)
            current_asks = [
                a for a in current_asks
                if not a["valid_at"] or _parse_time(a["valid_at"]) < earliest
            ]
    open_asks = current_asks

    # L1+L3: rank what we return and reinforce the hits (importance via use)
    contract_facts = await _rank_and_reinforce(contract_facts, q.deal_key)
    open_asks = await _rank_and_reinforce(open_asks, q.deal_key)

    return {
        "deal_key": q.deal_key,
        "as_of": q.as_of,
        "has_signed_contract": has_contract,
        "contract_facts": contract_facts,
        "open_asks": open_asks,
        "recommendation": (
            "DO NOT ASK ABOUT SPECS — contract already signed/delivered"
            if has_contract
            else "Baseline valid — no signed contract on record"
        ),
    }


# --- L6 communities + L5 prune -----------------------------------------------


class FactsIn(BaseModel):
    group_id: str
    deal_key: str
    limit: int = 12
    kinds: str = "all"  # all | contract | asks | corrections


@app.post("/facts")
async def facts(q: FactsIn):
    """The freshest CURRENT facts for a deal/counterparty, ranked by PPR x
    salience. This is what a drafter should read before writing: rates, units,
    entities, lane distinctions, prior corrections — the stuff the agent used
    to re-ask because it never got indexed."""
    g: Graphiti = STATE["graphiti"]
    queries = {
        "all": f"{q.deal_key} facts rates units entities lanes corrections pricing",
        "contract": f"{q.deal_key} contract signed delivered docusign",
        "asks": f"{q.deal_key} ask request nodes specs",
        "corrections": f"{q.deal_key} corrected distinction clarification not",
    }
    query = queries.get(q.kinds, queries["all"])
    try:
        results = await g.search(query, group_ids=[q.group_id], num_results=max(q.limit * 2, 25))
    except Exception as exc:
        return {"error": str(exc)[:200], "facts": []}
    facts = [_edge_to_fact(e) for e in results]
    tokens = _deal_tokens(q.deal_key)
    if tokens:
        facts = [f for f in facts if any(t in (f["fact"] or "").lower() for t in tokens)]
    facts = [f for f in facts if f["current"]]
    facts = await _rank_and_reinforce(facts, q.deal_key)
    return {
        "deal_key": q.deal_key,
        "facts": facts[: q.limit],
        "as_of": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/communities/build")
async def communities_build():
    """L6: run graphiti's community builder — the tightening/abstraction pass
    that clusters the graph into higher-order summaries."""
    g: Graphiti = STATE["graphiti"]
    try:
        result = await g.build_communities()
        STATE["communities"] += 1
        return {"ok": True, "runs": STATE["communities"],
                "result": str(result)[:400]}
    except Exception as exc:
        return {"ok": False, "error": str(exc)[:300]}


@app.post("/prune")
async def prune(threshold: float = 1.0, apply: bool = False):
    """L5: report (or archive) low-salience facts. Archive = mark archived in
    attributes; NEVER deletes. Hard-truth facts (contracts, decisions,
    deliveries, signatures) are never prunable."""
    driver = STATE["graphiti"].driver
    rows, _, _ = await driver.execute_query(
        "MATCH (e:RelatesToNode_) WHERE e.invalid_at IS NULL AND e.salience < $t "
        "RETURN e.uuid AS uuid, e.name AS name, e.fact AS fact, e.salience AS s "
        "LIMIT 500",
        t=threshold,
    )
    candidates = []
    protected = 0
    for r in rows or []:
        if ml.is_hard_truth(r.get("name"), r.get("fact") or ""):
            protected += 1
            continue
        candidates.append({"uuid": r["uuid"], "fact": (r["fact"] or "")[:120],
                           "salience": r["s"]})
    archived = 0
    if apply:
        for c in candidates:
            await driver.execute_query(
                "MATCH (e:RelatesToNode_ {uuid: $uuid}) "
                "SET e.attributes = $attrs",
                uuid=c["uuid"],
                attrs=json.dumps({"archived": True}),
            )
            archived += 1
    return {
        "threshold": threshold,
        "candidates": len(candidates),
        "protected_hard_truth": protected,
        "archived": archived,
        "apply": apply,
        "sample": candidates[:10],
    }


# --- ingest checkpoints + scheduled freshness loop ---------------------------

_CHECKPOINT_FILE = Path(os.environ.get(
    "GRAPHITI_CHECKPOINTS",
    "~/.hermes/profiles/ito/graphiti_checkpoints.json",
)).expanduser()


def _get_checkpoint(name: str) -> tuple[int, int]:
    try:
        data = json.loads(_CHECKPOINT_FILE.read_text())
    except Exception:
        return (0, 0)
    row = data.get(name, {})
    return (int(row.get("last_id", 0)), int(row.get("last_ts", 0)))


def _set_checkpoint(name: str, last_id: int, last_ts: int) -> None:
    try:
        data = json.loads(_CHECKPOINT_FILE.read_text())
    except Exception:
        data = {}
    data[name] = {"last_id": int(last_id), "last_ts": int(last_ts)}
    _CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    _CHECKPOINT_FILE.write_text(json.dumps(data, indent=1))


async def _freshness_loop():
    """Keep the graph within minutes of the ledger: every interval, pull new
    obligations and new deal-relevant events since their checkpoints. This is
    what closes the message->indexed latency that made the agent lose context
    across turns."""
    interval = int(os.environ.get("GRAPHITI_FRESHNESS_SECONDS", "300"))
    while True:
        await asyncio.sleep(interval)
        try:
            await _ingest_obligations_incremental()
            await _ingest_events_incremental()
        except Exception as exc:
            print(f"[graphiti-desk] freshness tick failed: {exc}")


async def _ingest_obligations_incremental():
    db = Path(os.environ.get("ITO_DB", "~/.hermes/profiles/ito/ito.db")).expanduser()
    conn = sqlite3.connect(str(db), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    cp_id, _ = _get_checkpoint("obligations")
    rows = [dict(r) for r in conn.execute(
        "SELECT id, counterparty, direction, status, opened_ts, last_touch_ts, "
        "expires_ts, summary, updated_at FROM obligations WHERE id > ? ORDER BY id LIMIT 200",
        (cp_id,)).fetchall()]
    conn.close()
    max_id = cp_id
    for row in rows:
        max_id = max(max_id, row["id"])
        body = {
            "type": "obligation_lifecycle",
            "obligation_id": row["id"],
            "counterparty": row.get("counterparty") or "unknown",
            "direction": row.get("direction"),
            "status": row.get("status") or "open",
            "opened_at": _iso(row.get("opened_ts")),
            "updated_at": _iso(row.get("updated_at") or row.get("opened_ts")),
            "summary": (row.get("summary") or "")[:2000],
        }
        STATE["queue"].put_nowait({
            "group_id": "desk",
            "text": json.dumps(body),
            "name": f"obligation-{row['id']}-{row.get('status') or 'open'}",
            "source": "json",
            "source_description": "ito-ledger",
            "reference_time": _iso(row.get("updated_at") or row.get("opened_ts")),
        })
    if max_id > cp_id:
        _set_checkpoint("obligations", max_id, int(time.time()))


async def _ingest_events_incremental():
    db = Path(os.environ.get("ITO_DB", "~/.hermes/profiles/ito/ito.db")).expanduser()
    conn = sqlite3.connect(str(db), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    cp_id, _ = _get_checkpoint("events:all:telegram,slack")
    if cp_id == 0:
        # first run: start from the CURRENT tail — backfill is a deliberate
        # manual operation (/ingest/events), never an implicit 60k-event flood
        row = conn.execute("SELECT COALESCE(MAX(id), 0) FROM events WHERE source IN ('telegram','slack')").fetchone()
        cp_id = int(row[0] or 0)
        _set_checkpoint("events:all:telegram,slack", cp_id, int(time.time()))
    rows = [dict(r) for r in conn.execute(
        "SELECT id, source, channel, sender, ts, text FROM events "
        "WHERE source IN ('telegram','slack') AND id > ? ORDER BY id LIMIT 400",
        (cp_id,)).fetchall()]
    conn.close()
    max_id = cp_id
    for row in rows:
        max_id = max(max_id, row["id"])
        text = (row.get("text") or "").strip()
        if not _is_deal_relevant(text, row.get("channel") or ""):
            continue
        body = {
            "type": "channel_message",
            "event_id": row["id"],
            "channel": row.get("channel"),
            "sender": row.get("sender"),
            "sent_at": _iso(row["ts"]),
            "text": text[:1800],
        }
        STATE["queue"].put_nowait({
            "group_id": "desk",
            "text": json.dumps(body),
            "name": f"event-{row['id']}",
            "source": "json",
            "source_description": f"{row['source']}:{row.get('channel')}",
            "reference_time": _iso(row["ts"]),
        })
    if max_id > cp_id:
        await STATE["queue"].join()
        _set_checkpoint("events:all:telegram,slack", max_id, int(time.time()))


# --- ledger ingestion -------------------------------------------------------

DEAL_KEYWORDS = re.compile(
    r"\b(node|nodes|h100|h200|b200|b300|gb200|gb300|nvl72|rack|racks|gpu|cluster|"
    r"price|pricing|rate|quote|quoted|contract|docusign|cash|deposit|buyer|supplier|"
    r"colo|capacity|delivery|allocation|refurb|counterparty|obligation|signed|"
    r"intro|introduction|referral|non-circ|margin|spread|markup)\b",
    re.I,
)


def _is_deal_relevant(text: str, channel: str) -> bool:
    if not text:
        return False
    if "<>" in (channel or "") or "cust-" in (channel or ""):
        return True  # counterparty-dedicated channels: everything counts
    return bool(DEAL_KEYWORDS.search(text))


@app.post("/ingest/events")
async def ingest_events(group_id: str = "desk",
                        db_path: str = "~/.hermes/profiles/ito/ito.db",
                        sources: str = "telegram,slack",
                        limit: int = 500,
                        channel: str = "",
                        flush: bool = False):
    """Ingest raw ledger events (telegram/slack messages) as episodes.

    The obligations-only ingest misses the message-level facts — rates, units,
    lane distinctions, corrections — which is why the agent re-asks known
    facts. Deal-relevance filter keeps cost sane; counterparty channels pass
    everything. Checkpointed per source+channel so reruns are incremental.
    """
    path = Path(db_path).expanduser()
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    src_list = [s.strip() for s in sources.split(",") if s.strip()]
    placeholders = ",".join("?" * len(src_list))
    sql = f"SELECT id, source, channel, channel_id, sender, ts, text FROM events WHERE source IN ({placeholders})"
    params: list = list(src_list)
    if channel:
        sql += " AND channel = ?"
        params.append(channel)
    cp_id, _ = _get_checkpoint(f"events:{channel or 'all'}:{','.join(src_list)}")
    sql += " AND id > ? ORDER BY id LIMIT ?"
    params.extend([cp_id, limit])
    rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    conn.close()

    queued = 0
    skipped = 0
    max_id = cp_id
    for row in rows:
        max_id = max(max_id, row["id"])
        text = (row.get("text") or "").strip()
        if not _is_deal_relevant(text, row.get("channel") or ""):
            skipped += 1
            continue
        body = {
            "type": "channel_message",
            "event_id": row["id"],
            "channel": row.get("channel"),
            "sender": row.get("sender"),
            "sent_at": _iso(row["ts"]),
            "text": text[:1800],
        }
        STATE["queue"].put_nowait({
            "group_id": group_id,
            "text": json.dumps(body),
            "name": f"event-{row['id']}",
            "source": "json",
            "source_description": f"{row['source']}:{row.get('channel')}",
            "reference_time": _iso(row["ts"]),
        })
        queued += 1
    # checkpoints advance ONLY after the queue drains past them — a crash
    # mid-drain must never silently skip events (the orphan bug that dropped
    # the Faris/McDavid thread facts)
    if flush:
        await STATE["queue"].join()
    if flush or queued == 0:
        _set_checkpoint(f"events:{channel or 'all'}:{','.join(src_list)}", max_id, int(time.time()))
    return {"queued": queued, "skipped_irrelevant": skipped, "last_event_id": max_id,
            "checkpoint_advanced": flush or queued == 0,
            "pending": STATE["queue"].qsize()}

@app.post("/ingest/ledger")
async def ingest_ledger(group_id: str = "desk", db_path: str = "~/.hermes/profiles/ito/ito.db"):
    """Replay the obligation ledger as structured episodes (JSON source)."""
    path = Path(db_path).expanduser()
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """SELECT id, counterparty, direction, status, opened_ts, last_touch_ts,
                  expires_ts, summary, updated_at FROM obligations ORDER BY id"""
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    for row in rows:
        body = {
            "type": "obligation_lifecycle",
            "obligation_id": row["id"],
            "counterparty": row.get("counterparty") or "unknown",
            "direction": row.get("direction"),
            "status": row.get("status") or "open",
            "opened_at": _iso(row.get("opened_ts")),
            "updated_at": _iso(row.get("updated_at") or row.get("opened_ts")),
            "summary": (row.get("summary") or "")[:2000],
        }
        STATE["queue"].put_nowait({
            "group_id": group_id,
            "text": json.dumps(body),
            "name": f"obligation-{row['id']}-{row.get('status') or 'open'}",
            "source": "json",
            "source_description": "ito-ledger",
            "reference_time": _iso(row.get("updated_at") or row.get("opened_ts")),
        })
    return {"queued": len(rows), "pending": STATE["queue"].qsize()}


def _iso(ts):
    if not ts:
        return datetime.now(timezone.utc).isoformat()
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
