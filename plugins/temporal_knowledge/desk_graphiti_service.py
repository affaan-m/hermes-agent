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
import hashlib
import hmac
import json
import os
import re
import sqlite3
import subprocess
import tempfile
import time
from contextlib import asynccontextmanager, closing
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


_FRESHNESS_LOG_INTERVAL = 300
_last_freshness_log = 0.0


def _freshness_marker_path() -> Path:
    """Where the last successful graph commit is advertised.

    The Kuzu file at graphiti_desk stopped being the store on 2026-09-01 when
    the backend moved to Neo4j, so consumers watching file mtimes need an
    explicit marker instead. Written atomically after every successful
    episode commit; the mtime is the freshness signal.
    """
    return Path(
        os.environ.get(
            "GRAPHITI_FRESHNESS_FILE", "~/.hermes/profiles/ito/graphiti_freshness"
        )
    ).expanduser()


def _write_freshness_marker(group_id: str, now: float | None = None) -> Path:
    global _last_freshness_log
    now = time.time() if now is None else now
    path = _freshness_marker_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(
        json.dumps({"ts": now, "group_id": group_id, "ingested": STATE["ingested"]})
    )
    os.replace(tmp, path)
    if now - _last_freshness_log >= _FRESHNESS_LOG_INTERVAL:
        _last_freshness_log = now
        print(
            f"[graphiti-desk] freshness marker written: {path} "
            f"(group={group_id}, ingested={STATE['ingested']})"
        )
    return path


async def _ingest_worker(g: Graphiti, queue: asyncio.Queue):
    while True:
        job = await queue.get()
        succeeded = False
        completion = job.pop("_completion", None)
        try:
            async with _graph_lock():
                async with _group_lock(job["group_id"]):
                    succeeded = await _apply(g, job)
        finally:
            # Queue drainage is not extraction success. Cancellation also
            # resolves false, leaving checkpointed producers free to retry.
            if completion is not None and not completion.done():
                completion.set_result(succeeded)
            queue.task_done()


async def _queue_confirmed(job: dict) -> bool:
    completion = asyncio.get_running_loop().create_future()
    STATE["queue"].put_nowait({**job, "_completion": completion})
    # Canceling a producer must not advance its cursor or cancel the worker's
    # receipt. A later retry can duplicate completed extraction (at-least-once).
    return await asyncio.shield(completion)


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
        try:
            _write_freshness_marker(job["group_id"])
        except Exception as exc:
            print(f"[graphiti-desk] freshness marker write failed (non-fatal): {exc}")
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
        return True
    except Exception as exc:
        STATE["failed"] += 1
        import traceback
        print(f"[graphiti-desk] ingest failed for group={job.get('group_id')}: {exc}")
        traceback.print_exc()
        return False


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


async def _attach_entities(facts: list[dict], *, strict: bool = False) -> list[dict]:
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
            # Keep full aliases from spanning two different endpoint names.
            f["_entities"] = " | ".join(nmap.get(f.get(key), '') or '' for key in ('src', 'dst'))
    except Exception:
        if strict:
            raise
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
        "ingest_reconciliation": _ingest_reconciliation(),
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


def _timestamp(value):
    """Parse a supplied timestamp without silently substituting the current time."""
    dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _fact_current_at(fact: dict, at: datetime) -> bool | None:
    """Current/closed at a reference time; None excludes future or invalid dates."""
    try:
        if fact["valid_at"] and _timestamp(fact["valid_at"]) > at:
            return None
        return not fact["invalid_at"] or _timestamp(fact["invalid_at"]) > at
    except (TypeError, ValueError):
        return None


@app.post("/search")
async def search(q: SearchIn):
    g: Graphiti = STATE["graphiti"]
    query = q.query or (q.center or "")
    try:
        at = _timestamp(q.as_of) if q.as_of else None
        async with _graph_lock():
            results = await g.search(query, group_ids=q.groups(), num_results=q.limit)
            facts = [_edge_to_fact(e) for e in results]
            if at is not None:
                facts = [f for f in facts if _fact_current_at(f, at) is True]
            facts = await _rank_and_reinforce(facts, q.center or q.query or q.group_id)
    except Exception as exc:
        print(f"[graphiti-desk] search failed for groups={q.groups()}: {exc}")
        return {"group_id": q.group_id, "as_of": q.as_of, "facts": [], "error": "search_failed"}
    return {"group_id": q.group_id, "as_of": q.as_of, "facts": facts}


@app.post("/brief")
async def brief(q: SearchIn):
    g: Graphiti = STATE["graphiti"]
    query = q.center or q.query or "everything"
    try:
        at = _timestamp(q.as_of) if q.as_of else datetime.now(timezone.utc)
        async with _graph_lock():
            results = await g.search(query, group_ids=q.groups(), num_results=max(q.limit, 25))
            facts = []
            for edge in results:
                fact = _edge_to_fact(edge)
                current_at = _fact_current_at(fact, at)
                if current_at is None:
                    continue
                fact["current"] = current_at
                facts.append(fact)
            facts = await _rank_and_reinforce(facts, q.center or q.query or q.group_id)
    except Exception as exc:
        print(f"[graphiti-desk] brief search failed for groups={q.groups()}: {exc}")
        return {"text": f"Memory brief — {q.center or 'the desk'}: (unavailable)", "current": [], "changed": []}
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
_DOCUMENT_ALIAS_WORDS = {
    "master", "service", "services", "agreement", "agreements", "contract",
    "contracts", "msa", "nda", "document", "documents", "docusign", "envelope",
    "signature", "signatures", "signed", "review", "copy", "package", "the", "a", "an",
}


def _deal_tokens(deal_key: str) -> set[str]:
    """Distinctive tokens that identify the deal in fact text. Used as a
    relevance guard: graphiti's semantic search returns *some* facts for any
    query, so a contract fact only counts if it actually names the deal."""
    import re as _re
    words = set(_re.findall(r"[a-z0-9][a-z0-9-]{2,}", deal_key.lower()))
    tokens = {w for w in words if w not in _STOP_TOKENS and len(w) >= 3}
    # keep hyphenated slugs (e.g. test-newdeal-267gate) even if parts are generic
    tokens |= {w for w in words if "-" in w and len(w) >= 8}
    return tokens


async def _expand_deal_tokens(tokens: set[str], group_id: str, *, strict: bool = False) -> set[str]:
    """Expand deal tokens with graph-derived aliases: entities whose name
    contains a token, plus their 1-hop neighbor names. This is how 'pluto'
    learns 'ronit jain' (and 'strike dfs') from the graph itself, so
    contract facts attached to the person still match the lane."""
    if not tokens or STATE.get("graphiti") is None:
        return tokens
    expanded = set(tokens)
    if STATE.get("backend") == "neo4j":
        query = (
            "MATCH path=(n:Entity)-[:RELATES_TO*1..2]-(m:Entity) "
            "WHERE toLower(n.name) CONTAINS $tok "
            "AND n.group_id = $group_id AND m.group_id = $group_id "
            "AND all(node IN nodes(path) WHERE node.group_id = $group_id) "
            "AND all(rel IN relationships(path) WHERE rel.group_id = $group_id) "
            "RETURN DISTINCT m.name AS name LIMIT 25"
        )
    else:
        # Kuzu stores the fact as a grouped node between two ungrouped links.
        query = (
            "MATCH (n:Entity)-[:RELATES_TO]-(e:RelatesToNode_)-[:RELATES_TO]-(m:Entity) "
            "WHERE toLower(n.name) CONTAINS $tok "
            "AND n.group_id = $group_id AND e.group_id = $group_id "
            "AND m.group_id = $group_id "
            "RETURN DISTINCT m.name AS name LIMIT 25"
        )
    try:
        for tok in list(tokens):
            rows, _, _ = await STATE["graphiti"].driver.execute_query(
                query, tok=tok, group_id=group_id,
            )
            for r in rows or []:
                alias = " ".join((r.get("name") or "").lower().split())
                words = set(re.findall(r"[a-z0-9][a-z0-9-]*", alias))
                # A generic document kind is not a deal alias. Keep distinctive
                # names intact: sharing a first name or one company word does
                # not establish that another deal belongs to this lane.
                if words - _STOP_TOKENS - _DOCUMENT_ALIAS_WORDS:
                    expanded.add(alias)
    except Exception:
        if strict:
            raise
    return expanded


def _deal_matcher(tokens: set[str]):
    aliases = [re.escape(token).replace(r"\ ", r"\s+") for token in sorted(tokens)]
    return re.compile(r"(?<![a-z0-9-])(?:" + "|".join(aliases) + r")(?![a-z0-9-])" if aliases else r"(?!)")


def _matches_deal(fact: dict, matcher) -> bool:
    return any(matcher.search((fact.get(key) or "").lower()) for key in ("fact", "_entities"))


def _execution_is_uncertain(text: str) -> bool:
    """Recognize execution uncertainty, not uncertainty about a separate subject."""
    for clause in re.split(r"[.;\n]|,\s*(?:but|and)\b", text.lower()):
        if re.search(r"\b(?:signed|signing|signature|executed|execution|completed|completion)\b", clause) and re.search(
            r"\b(?:cannot|can't|could not|couldn't|unable to)\s+(?:confirm|verify|establish)\b|"
            r"\bnot (?:yet )?(?:confirmed|verified)\b|"
            r"\b(?:unconfirmed|unverified|unknown|uncertain|unclear|unsure)\b", clause
        ):
            return True
    return False


def _contract_report_status(text: str) -> str:
    """Classify a narrative report, never authenticate a signature or receipt."""
    text = re.sub(r"\bplease (?:note|be advised)(?: that)?\b", "", text.lower())
    if _execution_is_uncertain(text):
        return "unknown"
    if re.search(r"\bpartial(?:ly)? signed\b|\bsigned by\b.{0,60}\bonly\b", text):
        return "partially_signed"
    if re.search(r"\bunsigned\b", text):
        return "unsigned"
    if re.search(r"\b(?:not|never|no longer|hasn't|haven't|isn't|wasn't)\b.{0,50}\b(?:signed|executed|completed)\b", text):
        return "not_completed"
    if re.search(r"\b(?:voided|declined|cancelled|canceled|expired)\b", text):
        return "not_completed"
    if (re.search(r"\b(?:signed|executed|completed)\b[^?;.\n]{0,80}\?", text) or re.search(
        r"\b(?:please|if|when|once|until|will|would|should|could|may|awaiting|pending)\b[^;.\n]{0,80}"
        r"\b(?:signed|executed|completed|signature|countersignature)\b|"
        r"\b(?:signature|countersignature)\b.{0,40}\b(?:requested|required|pending)\b", text
    )):
        return "signature_requested"
    completed = re.search(
        r"\bfully (?:signed|executed)\b|"
        r"\bsigned by (?:all|both) (?:required )?(?:parties|signatories)\b|"
        r"\ball (?:required )?(?:parties|signatories) (?:have )?signed\b|"
        r"\bdocusign\b.{0,60}\benvelope\b.{0,30}\b(?:status[ :]+)?completed\b", text
    )
    if completed:
        return "completion_reported"
    if re.search(r"\bdelivered\b", text):
        return "delivered"
    if re.search(r"\bsent\b", text):
        return "sent"
    if re.search(r"\b(?:signature|countersignature|countersign)\b", text):
        return "signature_requested"
    return "unknown"


def _is_open_ask(fact: dict) -> bool:
    """Require request evidence; semantic search relevance alone is insufficient."""
    text = (fact.get("fact") or "").lower()
    resolved = False
    current_request = None
    uncertainty = re.compile(
        r"[?]|\b(?:not|never|cannot|can't|don't|doesn't|didn't|hasn't|haven't|isn't|wasn't|weren't|"
        r"unconfirmed|uncertain|unknown|unsure|if|unless|whether|maybe|perhaps|may|might|"
        r"could|would|should|will|please)\b|"
        r"\b(?:pending|awaiting) confirmation\b|\bno (?:evidence|proof|confirmation)\b")
    clauses = []
    for clause in re.split(r"[.;\n]|\b(?:but|however)\b", text):
        # Process affirmative conjunctions in order without detaching a
        # question, negation, or uncertain report from what it qualifies.
        clauses.extend([clause] if uncertainty.search(clause) else re.split(r"\band\b", clause))
    for clause in clauses:
        resolution = re.search(
            r"\b(?:ask|request)\s+(?:(?:has|have|had)\s+been|is|are|was|were)\s+"
            r"(?:(?:already|now|fully|successfully)\s+)?(?:answered|resolved|fulfilled|withdrawn|cancelled|canceled)\b|"
            r"\b(?:answered|resolved|fulfilled|withdrawn|cancelled|canceled)\s+"
            r"(?:the|this|that|our|their|my|your)\s+"
            r"(?:(?:old|earlier|previous|prior|unrelated)\s+)?(?:ask|request)\b|"
            r"\bno (?:open|pending|outstanding) (?:asks?|requests?)\b", clause)
        uncertain = uncertainty.search(clause)
        # Require a completed-state construction or direct completed action,
        # not mere proximity of "request" and "resolved" in pending work.
        # Negation, questions and unconfirmed reports retain the current ask.
        if resolution and not uncertain:
            resolved = True
            # An unqualified later resolution refers to the current request.
            # An explicitly old/unrelated request does not close a new one.
            if not re.search(r"\b(?:old|earlier|previous|prior|unrelated)\s+(?:ask|request)\b", clause):
                current_request = False
            continue
        if re.search(
            r"\b(?:asks?|requests?) (?:for|that|whether|a|an|the|to)\b|"
            r"\b(?:is|are) asking for\b|"
            r"\bplease (?:send|provide|confirm|share|clarify|specify)\b|"
            r"\b(?:can|could|would|will) you\b[^?]*\?", clause):
            current_request = True
    if current_request is not None:
        return current_request
    if resolved:
        return False
    if re.sub(r"[^a-z]", "", (fact.get("name") or "").lower()) == "hasask":
        return True
    return False


def _baseline_unavailable(q: BaselineIn) -> dict:
    return {
        "success": False, "unavailable": True, "error": "BASELINE_UNAVAILABLE",
        "deal_key": q.deal_key, "as_of": q.as_of,
        "has_signed_contract": False, "signature_verified": False,
        "completion_reported": False, "contract_status": "unavailable",
        "evidence_basis": "unavailable", "contract_facts": [], "open_asks": [],
        "recommendation": "Current contract information is unavailable. Verify the source before relying on it.",
    }


@app.post("/baseline")
async def baseline(q: BaselineIn):
    """Return scoped reported evidence; graph extraction is not signature proof."""
    try:
        # Alias/entity reads and ranking/reinforcement also touch Kuzu.
        async with _graph_lock():
            return await _baseline_read(q)
    except Exception:
        # This payload enters model context and the approval gate. Never return
        # backend exception strings, paths or credentials, or a success envelope.
        return _baseline_unavailable(q)


async def _baseline_read(q: BaselineIn):
    tokens = _deal_tokens(q.deal_key)
    if not tokens:
        return _baseline_unavailable(q)
    as_of_dt = (datetime.fromisoformat(q.as_of.replace("Z", "+00:00"))
                if q.as_of else datetime.now(timezone.utc))
    if as_of_dt.tzinfo is None:
        as_of_dt = as_of_dt.replace(tzinfo=timezone.utc)
    g: Graphiti = STATE["graphiti"]
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
    seen: set[str] = set()
    merged = []
    for edge in list(contract_results) + list(delivery_results):
        uid = getattr(edge, "uuid", None)
        if uid and uid not in seen:
            seen.add(uid)
            merged.append(edge)
    contract_facts = [_edge_to_fact(edge) for edge in merged]
    ask_facts = [_edge_to_fact(edge) for edge in ask_results]
    tokens = await _expand_deal_tokens(tokens, q.group_id, strict=True)
    contract_facts = await _attach_entities(contract_facts, strict=True)
    ask_facts = await _attach_entities(ask_facts, strict=True)
    match = _deal_matcher(tokens)

    def relevant(f):
        return _matches_deal(f, match)

    def timestamp(value):
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

    def report_time(f):
        # No event date is invented: use creation time, then this query
        # reference only when the current edge carries neither timestamp.
        value = f.get("valid_at") or f.get("created_at")
        return timestamp(value) if value else as_of_dt

    def current_at(f):
        if f.get("archived"):
            return False
        try:
            if report_time(f) > as_of_dt:
                return False
            if f["invalid_at"] and timestamp(f["invalid_at"]) <= as_of_dt:
                return False
        except (TypeError, ValueError):
            return False
        return True

    contract_facts = [f for f in contract_facts if relevant(f) and current_at(f)]
    open_asks = [f for f in ask_facts if relevant(f) and current_at(f) and _is_open_ask(f)]
    for f in contract_facts:
        f["execution_report"] = _contract_report_status(f["fact"] or "")
        f["evidence_basis"] = "graph_extraction"
    # Retain current historical reports; age alone does not expire a contract.
    # Later negative or explicitly uncertain execution reports must not be
    # hidden by an older affirmative one. Unrelated unknown narrative does not
    # change execution chronology (e.g. a newly prepared review PDF).
    reports = [f for f in contract_facts if f["execution_report"] != "unknown"
               or _execution_is_uncertain(f["fact"] or "")]
    latest = max((report_time(f) for f in reports), default=None)
    statuses = {f["execution_report"] for f in reports if report_time(f) == latest}
    if len(statuses) > 1:
        status = "conflicting_reports"
    else:
        status = next(iter(statuses), "unknown")
    # A later ask can be a legitimate new request. Only explicit temporal
    # invalidation removes it; a contract report never erases asks by timestamp.
    contract_facts = await _rank_and_reinforce(contract_facts, q.deal_key)
    open_asks = await _rank_and_reinforce(open_asks, q.deal_key)
    return {
        "success": True, "unavailable": False, "deal_key": q.deal_key, "as_of": q.as_of,
        "has_signed_contract": False, "signature_verified": False,
        "completion_reported": status == "completion_reported",
        "contract_status": status, "evidence_basis": "graph_extraction",
        "contract_facts": contract_facts, "open_asks": open_asks,
        "recommendation": (
            "Completion is reported in the retrieved evidence. Verify the original agreement before treating it as signed; review open asks separately."
            if status == "completion_reported" else
            "No verified signature determination is available. Review the reported evidence and current open asks."
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
    async with _graph_lock():
        at = datetime.now(timezone.utc)
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
            facts = await _attach_entities([_edge_to_fact(e) for e in results], strict=True)
        except Exception:
            return {"error": "FACTS_UNAVAILABLE", "facts": []}
        match = _deal_matcher(_deal_tokens(q.deal_key))
        facts = [f for f in facts if _matches_deal(f, match)]
        facts = [{**f, "current": True} for f in facts if _fact_current_at(f, at) is True]
        facts = await _rank_and_reinforce(facts, q.deal_key)
        return {
            "deal_key": q.deal_key,
            "facts": facts[: q.limit],
            "as_of": at.isoformat(),
        }


@app.post("/communities/build")
async def communities_build():
    """L6: run graphiti's community builder — the tightening/abstraction pass
    that clusters the graph into higher-order summaries."""
    async with _graph_lock():
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
    async with _graph_lock():
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


_CHECKPOINT_LOCKS: dict[str, asyncio.Lock] = {}
_OBLIGATION_CHECKPOINT = "obligations:updates:v2"


def _checkpoint_lock(name: str) -> asyncio.Lock:
    # Producers in this service share a cursor. No cross-process writer is
    # supported; deployments must run only one service per checkpoint file.
    return _CHECKPOINT_LOCKS.setdefault(name, asyncio.Lock())


def _read_checkpoints() -> dict:
    try:
        data = json.loads(_CHECKPOINT_FILE.read_text())
    except FileNotFoundError:
        return {}
    if not isinstance(data, dict):
        raise ValueError("checkpoint file must contain an object")
    return data


def _checkpoint_record(data: dict, name: str) -> dict:
    row = data.get(name, {})
    if not isinstance(row, dict):
        raise ValueError("checkpoint record must contain an object")
    for key in ("last_id", "last_ts"):
        value = row.get(key, 0)
        if type(value) is not int or value < 0:
            raise ValueError("checkpoint cursor must be a nonnegative integer")
    return row


def _get_checkpoint(name: str) -> tuple[int, int]:
    row = _checkpoint_record(_read_checkpoints(), name)
    return row.get("last_id", 0), row.get("last_ts", 0)


def _write_checkpoint(name: str, record: dict) -> None:
    # Read/merge/replace contains no await: independent producers cannot lose
    # one another's keys. Bad existing state is preserved, never reset/replayed.
    data = _read_checkpoints()
    data[name] = record
    _CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".graphiti-checkpoint-", dir=_CHECKPOINT_FILE.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(data, stream, indent=1)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, _CHECKPOINT_FILE)
        directory = os.open(_CHECKPOINT_FILE.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _set_checkpoint(name: str, last_id: int, last_ts: int) -> None:
    old = _checkpoint_record(_read_checkpoints(), name)
    _write_checkpoint(name, {**old, "last_id": int(last_id), "last_ts": int(last_ts)})


def _validate_obligation_cursor(data: dict) -> dict:
    row = _checkpoint_record(data, _OBLIGATION_CHECKPOINT)
    fingerprints = row.get("boundary_fingerprints")
    if (row.get("version") != 2 or not isinstance(fingerprints, dict)
            or type(row.get("reconciliation_needed")) is not bool
            or "last_id" not in row or "last_ts" not in row):
        raise ValueError("invalid obligation update checkpoint")
    if any(not isinstance(k, str) or not k.isdigit() or not isinstance(v, str)
           or not re.fullmatch(r"[0-9a-f]{64}", v) for k, v in fingerprints.items()):
        raise ValueError("invalid obligation boundary fingerprints")
    return {**row, "boundary_fingerprints": dict(fingerprints)}


def _obligation_cursor() -> dict:
    data = _read_checkpoints()
    if _OBLIGATION_CHECKPOINT in data:
        return _validate_obligation_cursor(data)
    legacy = _checkpoint_record(data, "obligations")
    row = {"version": 2, "last_id": 0, "last_ts": legacy.get("last_ts", 0),
           "boundary_fingerprints": {}, "reconciliation_needed": bool(legacy)}
    if legacy:
        # Legacy last_ts was poll time, NOT an acknowledged row watermark.
        # Keep history intact and do not implicitly replay its historical gap.
        row["legacy_boundary_unverified"] = {
            "last_id": legacy.get("last_id", 0), "last_ts": legacy.get("last_ts", 0)}
    _write_checkpoint(_OBLIGATION_CHECKPOINT, row)
    return row


def _ingest_reconciliation() -> dict:
    try:
        data = _read_checkpoints()
        row = _validate_obligation_cursor(data) if _OBLIGATION_CHECKPOINT in data else {}
        legacy = _checkpoint_record(data, "obligations")
        needed = row.get("reconciliation_needed", bool(legacy))
        return {"obligations": {
            "reconciliation_needed": needed,
            "history_status": "legacy_boundary_unverified" if needed else "no_legacy_boundary",
            "legacy_boundary_unverified": row.get("legacy_boundary_unverified") or (
                {"last_id": legacy.get("last_id", 0), "last_ts": legacy.get("last_ts", 0)}
                if legacy and not row else None),
            "last_acknowledged_update_ts": row.get("last_ts") if row.get("boundary_fingerprints") else None,
        }}
    except (OSError, ValueError, TypeError):
        return {"obligations": {"reconciliation_needed": True, "history_status": "checkpoint_unreadable"}}


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
    async with _checkpoint_lock(_OBLIGATION_CHECKPOINT):
        cursor = _obligation_cursor()
        db = Path(os.environ.get("ITO_DB", "~/.hermes/profiles/ito/ito.db")).expanduser().resolve()
        # Inclusive boundary is deliberate: a lower ID may change in the same
        # second after a higher ID was acknowledged. Fingerprints suppress only
        # unchanged boundary rows. Stream past these before applying the batch
        # limit, so >200 same-timestamp rows cannot starve later IDs.
        rows = []
        with closing(sqlite3.connect(db.as_uri() + "?mode=ro", uri=True)) as conn:
            conn.row_factory = sqlite3.Row
            pending = conn.execute(
                "SELECT id, counterparty, direction, status, opened_ts, last_touch_ts, "
                "expires_ts, summary, updated_at, "
                "MAX(COALESCE(updated_at,0), COALESCE(last_touch_ts,0), COALESCE(opened_ts,0)) AS change_ts "
                "FROM obligations WHERE change_ts >= ? ORDER BY change_ts, id", (cursor["last_ts"],))
            for record in pending:
                row = dict(record)
                fingerprint = hashlib.sha256(json.dumps(row, sort_keys=True).encode()).hexdigest()
                if row["change_ts"] == cursor["last_ts"] and cursor["boundary_fingerprints"].get(str(row["id"])) == fingerprint:
                    continue
                rows.append((row, fingerprint))
                if len(rows) == 200:
                    break
        # Close SQLite before provider work so a long extraction never holds
        # the read transaction open. Advance only through acknowledged rows.
        for row, fingerprint in rows:
            reference = datetime.fromtimestamp(row["change_ts"], tz=timezone.utc).isoformat()
            body = {
                "type": "obligation_lifecycle", "obligation_id": row["id"],
                "counterparty": row.get("counterparty") or "unknown",
                "direction": row.get("direction"), "status": row.get("status") or "open",
                "opened_at": _iso(row.get("opened_ts")), "updated_at": reference,
                "summary": (row.get("summary") or "")[:2000],
            }
            succeeded = await _queue_confirmed({
                "group_id": "desk", "text": json.dumps(body),
                "name": f"obligation-{row['id']}-{row.get('status') or 'open'}", "source": "json",
                "source_description": "ito-ledger", "reference_time": reference,
            })
            if not succeeded:
                break
            if row["change_ts"] > cursor["last_ts"]:
                cursor["boundary_fingerprints"] = {}
                cursor["last_id"] = 0
            cursor["last_ts"] = row["change_ts"]
            cursor["last_id"] = max(cursor["last_id"], row["id"])
            cursor["boundary_fingerprints"][str(row["id"])] = fingerprint
            _write_checkpoint(_OBLIGATION_CHECKPOINT, cursor)


async def _ingest_events_incremental():
    db = Path(os.environ.get("ITO_DB", "~/.hermes/profiles/ito/ito.db")).expanduser().resolve()
    key = "events:all:telegram,slack"
    async with _checkpoint_lock(key):
        # Preserve the existing deliberate no-backfill policy only on an
        # absent checkpoint. A valid zero cursor is not reset to the tail.
        if key not in _read_checkpoints():
            with closing(sqlite3.connect(db.as_uri() + "?mode=ro", uri=True)) as conn:
                row = conn.execute("SELECT COALESCE(MAX(id), 0) FROM events WHERE source IN ('telegram','slack')").fetchone()
            _write_checkpoint(key, {"last_id": int(row[0] or 0), "last_ts": int(time.time()),
                                    "initial_tail_skipped": True})
    await ingest_events(db_path=str(db), limit=400, flush=True)


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
    path = Path(db_path).expanduser().resolve()
    src_list = [s.strip() for s in sources.split(",") if s.strip()]
    key = f"events:{channel or 'all'}:{','.join(src_list)}"
    async with _checkpoint_lock(key):
        placeholders = ",".join("?" * len(src_list))
        sql = f"SELECT id, source, channel, channel_id, sender, ts, text FROM events WHERE source IN ({placeholders})"
        params: list = list(src_list)
        if channel:
            sql += " AND channel = ?"
            params.append(channel)
        cp_id, _ = _get_checkpoint(key)
        sql += " AND id > ? ORDER BY id LIMIT ?"
        params.extend([cp_id, limit])
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as conn:
            conn.row_factory = sqlite3.Row
            rows = [dict(r) for r in conn.execute(sql, params)]
        queued = skipped = failed = 0
        max_id = acknowledged = cp_id
        for row in rows:
            max_id = row["id"]
            text = (row.get("text") or "").strip()
            if not _is_deal_relevant(text, row.get("channel") or ""):
                skipped += 1
            else:
                body = {
                    "type": "channel_message", "event_id": row["id"],
                    "channel": row.get("channel"), "sender": row.get("sender"),
                    "sent_at": _iso(row["ts"]), "text": text[:1800],
                }
                job = {
                    "group_id": group_id, "text": json.dumps(body), "name": f"event-{row['id']}",
                    "source": "json", "source_description": f"{row['source']}:{row.get('channel')}",
                    "reference_time": _iso(row["ts"]),
                }
                queued += 1
                if flush:
                    if not await _queue_confirmed(job):
                        failed += 1
                        break
                else:
                    STATE["queue"].put_nowait(job)
            if flush or queued == 0:
                _set_checkpoint(key, row["id"], int(time.time()))
                acknowledged = row["id"]
        return {"queued": queued, "failed": failed, "skipped_irrelevant": skipped,
                "last_event_id": max_id, "last_acknowledged_event_id": acknowledged,
                "checkpoint_advanced": acknowledged > cp_id, "pending": STATE["queue"].qsize()}

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
