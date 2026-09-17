"""Memory layers for the desk temporal graph — importance-based, not time-based.

Implements the desk variant of the current research frontier:

  L1 salience     — per-fact importance. Base = source authority (contract /
                    decision / delivery > routine message). Reinforced on every
                    retrieval hit (Hebbian: facts that get used get stronger;
                    co-hit facts boost their shared entity). NO time decay:
                    salience only moves via use, authority, and contradiction.
  L2 dedup        — at ingest, exact-normalized duplicate facts MERGE:
                    embedding similarity alone cannot establish equivalence;
                    the survivor keeps the earlier valid_from, gains the
                    loser's episode provenance, and gets a salience bump.
  L3 PPR          — Personalized PageRank over the entity<->fact bipartite
                    graph, seeded at the deal entity; retrieval-time importance
                    from structure (HippoRAG pattern), blended with embeddings.
  L4 evolution    — per new episode, top-K similar existing facts are judged
                    in ONE batched LLM call: KEEP / MERGE / STRENGTHEN /
                    INVALIDATE (A-MEM pattern, bounded + cheap).
  L5 prune        — manual endpoint; archives (never deletes) low-salience
                    facts. Hard-truth class (contracts, decisions, deliveries)
                    is never prunable.
  L6 communities  — endpoint wrapping graphiti's build_communities.

Truth stays sacred: salience only ever affects RANKING. Temporal invalidation
(valid_to / invalid_at) remains the only thing that can close a fact.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
import math
import re
from typing import Any

import numpy as np
from pydantic import BaseModel

# --- salience ----------------------------------------------------------------

HARD_TRUTH_TYPES = (
    "signedcontract", "contract", "docusign", "delivered_to", "delivered",
    "approved", "decision", "delivery",
)

AUTHORITY_BY_EDGE = {
    "signedcontract": 3.0,
    "delivered_to": 3.0,
    "contract_state": 3.0,
    "approved": 2.5,
    "supersededby": 2.5,
    "hasstatus": 2.0,
    "owes": 1.5,
    "hasask": 1.5,
    "has_ask": 1.5,
    "part_of": 1.0,
}

SALIENCE_MAX = 10.0
RETRIEVAL_BOOST = 0.15
ENTITY_COHIT_BOOST = 0.05
MERGE_BOOST = 0.5
STRENGTHEN_BOOST = 0.4

_EVOLUTION_TOP_K = 5
_EVOLUTION_MAX_NEW_FACTS = 5


async def ensure_salience_schema(driver) -> None:
    """Add salience columns (idempotent). graphiti's kuzu schema lacks them."""
    for ddl in (
        "ALTER TABLE RelatesToNode_ ADD salience DOUBLE DEFAULT 1.0",
        "ALTER TABLE Entity ADD salience DOUBLE DEFAULT 1.0",
    ):
        try:
            await driver.execute_query(ddl)
        except Exception:
            pass  # column exists


def base_salience(edge_name: str, fact_text: str = "") -> float:
    name = (edge_name or "").lower()
    base = AUTHORITY_BY_EDGE.get(name, 1.0)
    text = (fact_text or "").lower()
    if any(k in text for k in ("docusign", "signed", "delivered to")):
        base = max(base, 3.0)
    return float(base)


async def _set_salience(driver, uuid: str, value: float, *, table: str = "RelatesToNode_") -> None:
    value = min(float(value), SALIENCE_MAX)
    await driver.execute_query(
        f"MATCH (e:{table} {{uuid: $uuid}}) SET e.salience = $s",
        uuid=uuid, s=value,
    )


async def _get_salience(driver, uuid: str, *, table: str = "RelatesToNode_") -> float:
    try:
        rows, _, _ = await driver.execute_query(
            f"MATCH (e:{table} {{uuid: $uuid}}) RETURN e.salience AS s", uuid=uuid,
        )
        if rows and rows[0].get("s") is not None:
            return float(rows[0]["s"])
    except Exception:
        pass
    return 1.0


async def reinforce(driver, fact_uuids: list[str]) -> None:
    """L1 reinforcement: retrieved facts get stronger; their entities get a
    smaller co-hit boost. Called by /search, /brief, /baseline on every hit."""
    if not fact_uuids:
        return
    for uuid in fact_uuids[:30]:
        try:
            cur = await _get_salience(driver, uuid)
            await _set_salience(driver, uuid, cur + RETRIEVAL_BOOST)
            # entity co-hit boost (both ends of the fact)
            rows, _, _ = await driver.execute_query(
                "MATCH (n:Entity)-[:RELATES_TO]->(e:RelatesToNode_ {uuid: $uuid}) "
                "RETURN n.uuid AS u LIMIT 2",
                uuid=uuid,
            )
            for r in rows or []:
                ent = r.get("u")
                if ent:
                    ecur = await _get_salience(driver, ent, table="Entity")
                    await _set_salience(driver, ent, ecur + ENTITY_COHIT_BOOST, table="Entity")
        except Exception:
            continue


# --- dedup -------------------------------------------------------------------

def _norm_text(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "").lower()).strip()


def _cosine(a, b) -> float:
    va = np.asarray(a, dtype=float)
    vb = np.asarray(b, dtype=float)
    na, nb = np.linalg.norm(va), np.linalg.norm(vb)
    if na == 0 or nb == 0:
        return 0.0
    return float(va @ vb / (na * nb))


async def dedup_new_facts(driver, new_edges: list) -> dict:
    """Merge exact-normalized text only within one directed edge identity.

    High cosine also occurs for contradictory facts. Non-equivalent wording
    stays available to the existing L4 pass, without adding a model call here.
    Preserve survivor provenance before closing the incoming duplicate.
    """
    merged = 0
    kept = 0
    for edge in new_edges or []:
        fact = getattr(edge, "fact", "") or ""
        if not fact:
            continue
        gid = getattr(edge, "group_id", None)
        edge_name = getattr(edge, "name", None)
        source_uuid = getattr(edge, "source_node_uuid", None)
        target_uuid = getattr(edge, "target_node_uuid", None)
        rows = []
        if all(isinstance(value, str) and value.strip()
               for value in (gid, edge_name, source_uuid, target_uuid)):
            rows, _, _ = await driver.execute_query(
                "MATCH (source:Entity)-[:RELATES_TO]->(e:RelatesToNode_)"
                "-[:RELATES_TO]->(target:Entity) "
                "WHERE e.invalid_at IS NULL AND e.uuid <> $uuid "
                "AND e.group_id = $gid AND e.name = $edge_name "
                "AND source.uuid = $source_uuid AND target.uuid = $target_uuid "
                "AND source.group_id = $gid AND target.group_id = $gid "
                "RETURN e.uuid AS uuid, e.fact AS fact, "
                "e.fact_embedding AS emb, e.valid_at AS valid_at, e.salience AS s, "
                "e.episodes AS episodes LIMIT 200",
                uuid=edge.uuid, gid=gid, edge_name=edge_name,
                source_uuid=source_uuid, target_uuid=target_uuid,
            )
        best = next((row for row in rows or []
                     if _norm_text(row["fact"]) == _norm_text(fact)), None)
        if best is not None:
            eps = list(best.get("episodes") or [])
            for ep in (getattr(edge, "episodes", None) or []):
                if ep not in eps:
                    eps.append(ep)
            await driver.execute_query(
                "MATCH (e:RelatesToNode_ {uuid: $uuid}) SET e.episodes = $eps, "
                "e.salience = $s",
                uuid=best["uuid"], eps=eps,
                s=min(float(best.get("s") or 1.0) + MERGE_BOOST, SALIENCE_MAX),
            )
            # If copying provenance fails, leave incoming current. If closing
            # then fails, both remain current and the evidence exists in both;
            # _apply's nonfatal memory-layer behavior must not lose the source.
            await driver.execute_query(
                "MATCH (e:RelatesToNode_ {uuid: $uuid}) SET e.invalid_at = $now",
                uuid=edge.uuid, now=edge.created_at,
            )
            merged += 1
        else:
            # initialize base salience for kept facts
            await _set_salience(driver, edge.uuid, base_salience(getattr(edge, "name", ""), fact))
            kept += 1
    return {"merged": merged, "kept": kept}


# --- evolution (L4) ----------------------------------------------------------

_EVOLUTION_PROMPT = """You maintain a temporal knowledge graph for a trading desk.
A NEW fact was just learned. For each EXISTING fact below, decide one action:
- KEEP: independent, leave it alone
- MERGE: it says the same thing as the new fact (it will absorb the new fact)
- STRENGTHEN: the new fact corroborates it (raise its importance)
- INVALIDATE: the new fact contradicts/supersedes it (close its validity)

NEW FACT: {new_fact}

EXISTING FACTS:
{candidates}

Reply with ONLY a JSON array, one object per existing fact, same order:
[{{"i": 1, "action": "KEEP|MERGE|STRENGTHEN|INVALIDATE", "reason": "short"}}]"""


class _Decision(BaseModel):
    i: int
    action: str
    reason: str = ""


class _Decisions(BaseModel):
    decisions: list[_Decision]


def _invalidation_boundary(edge, target):
    """Close only a fact already valid at the known event boundary."""
    value = getattr(edge, "valid_at", None)
    if value is None:
        value = getattr(edge, "created_at", None)
    try:
        # Match historical reads: naive ISO dates mean UTC, never ingestion
        # time. An unknown/malformed candidate start cannot authorize closure.
        boundary = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        start = datetime.fromisoformat(str(target.get("valid_at")).replace("Z", "+00:00"))
        boundary = boundary if boundary.tzinfo else boundary.replace(tzinfo=timezone.utc)
        start = start if start.tzinfo else start.replace(tzinfo=timezone.utc)
        if start > boundary:
            return None
    except (TypeError, ValueError):
        return None
    return value


async def evolution_pass(driver, llm_client, new_edges: list, group_id: str) -> dict:
    """Judge current facts only within one directed relationship identity.

    The persisted incoming fact must still be current after L2; its in-memory
    edge object can be stale. Similar prose never grants cross-identity writes.
    """
    from pydantic import BaseModel as _BM  # noqa: F401 (clarity for the models above)
    from graphiti_core.prompts.models import Message

    actions_taken = {"MERGE": 0, "STRENGTHEN": 0, "INVALIDATE": 0, "KEEP": 0}
    for edge in (new_edges or [])[:_EVOLUTION_MAX_NEW_FACTS]:
        new_fact = getattr(edge, "fact", "") or ""
        emb = getattr(edge, "fact_embedding", None)
        gid = getattr(edge, "group_id", None)
        edge_name = getattr(edge, "name", None)
        source_uuid = getattr(edge, "source_node_uuid", None)
        target_uuid = getattr(edge, "target_node_uuid", None)
        if not new_fact or gid != group_id or not all(
            isinstance(value, str) and value.strip()
            for value in (gid, edge_name, source_uuid, target_uuid)
        ):
            continue
        # L2 may have closed incoming in storage without updating edge. Both
        # facts must share current, group-scoped, directed endpoint identity.
        rows, _, _ = await driver.execute_query(
            "MATCH (source:Entity)-[:RELATES_TO]->(incoming:RelatesToNode_)"
            "-[:RELATES_TO]->(target:Entity) "
            "MATCH (source)-[:RELATES_TO]->(e:RelatesToNode_)-[:RELATES_TO]->(target) "
            "WHERE incoming.uuid = $uuid AND incoming.invalid_at IS NULL "
            "AND incoming.group_id = $gid AND incoming.name = $edge_name "
            "AND e.uuid <> $uuid AND e.invalid_at IS NULL "
            "AND e.group_id = $gid AND e.name = $edge_name "
            "AND source.uuid = $source_uuid AND target.uuid = $target_uuid "
            "AND source.group_id = $gid AND target.group_id = $gid "
            "RETURN e.uuid AS uuid, e.fact AS fact, "
            "e.fact_embedding AS emb, e.salience AS s, e.episodes AS episodes, "
            "e.valid_at AS valid_at, "
            "incoming.episodes AS incoming_episodes LIMIT 400",
            uuid=edge.uuid, gid=gid, edge_name=edge_name,
            source_uuid=source_uuid, target_uuid=target_uuid,
        )
        if not rows:
            continue
        scored = []
        for r in rows:
            if emb is not None and r.get("emb") is not None:
                scored.append((_cosine(emb, r["emb"]), r))
            else:
                scored.append((0.0, r))
        scored.sort(key=lambda x: x[0], reverse=True)
        candidates = [r for sim, r in scored[:_EVOLUTION_TOP_K] if sim >= 0.5]
        if not candidates:
            continue
        cand_text = "\n".join(f"{i+1}. {c['fact']}" for i, c in enumerate(candidates))
        prompt = _EVOLUTION_PROMPT.format(new_fact=new_fact, candidates=cand_text)
        try:
            resp = await llm_client.generate_response(
                [Message(role="user", content=prompt)],
                response_model=_Decisions,
                max_tokens=500,
            )
            raw = resp.get("decisions", []) if isinstance(resp, dict) else []
            decisions = [
                d.model_dump() if hasattr(d, "model_dump") else d for d in raw
            ]
        except Exception as exc:
            print(f"[memory-layers] evolution llm failed: {exc}")
            continue
        decided_indices = set()
        for d in decisions:
            try:
                idx = int(d.get("i", 0)) - 1
                action = str(d.get("action", "KEEP")).upper()
                if (not (0 <= idx < len(candidates)) or action not in actions_taken
                        or idx in decided_indices):
                    continue
                decided_indices.add(idx)
                target = candidates[idx]
                if action == "MERGE":
                    episodes = list(target.get("episodes") or [])
                    # Earlier new edges in this batch may have merged into
                    # incoming already; its Python object still has old episodes.
                    incoming_episodes = list(target.get("incoming_episodes") or [])
                    incoming_episodes.extend(getattr(edge, "episodes", None) or [])
                    for episode in incoming_episodes:
                        if episode not in episodes:
                            episodes.append(episode)
                    await driver.execute_query(
                        "MATCH (e:RelatesToNode_ {uuid: $uuid}) SET e.episodes = $eps, "
                        "e.salience = $s", uuid=target["uuid"], eps=episodes,
                        s=min(float(target.get("s") or 1.0) + MERGE_BOOST, SALIENCE_MAX),
                    )
                    # Preserve the incoming provenance before closing it. A
                    # failed write leaves the new fact available for recovery.
                    await driver.execute_query(
                        "MATCH (e:RelatesToNode_ {uuid: $uuid}) SET e.invalid_at = $now",
                        uuid=edge.uuid, now=edge.created_at,
                    )
                    actions_taken[action] += 1
                    # The new fact is closed; it cannot invalidate or
                    # strengthen another target from this stale candidate set.
                    break
                elif action == "STRENGTHEN":
                    await _set_salience(driver, target["uuid"],
                                        (target.get("s") or 1.0) + STRENGTHEN_BOOST)
                elif action == "INVALIDATE":
                    boundary = _invalidation_boundary(edge, target)
                    if boundary is None:
                        continue  # No reliable boundary: preserve the prior fact.
                    await driver.execute_query(
                        "MATCH (e:RelatesToNode_ {uuid: $uuid}) SET e.invalid_at = $now",
                        uuid=target["uuid"], now=boundary,
                    )
                actions_taken[action] += 1
            except Exception:
                # A mutation may have partly succeeded. Do not continue making
                # decisions against the original candidate snapshot.
                break
    return actions_taken


# --- PPR retrieval ranking (L3) ----------------------------------------------

async def ppr_rank(driver, seed_entity_names: list[str], facts: list[dict],
                   alpha: float = 0.15) -> dict[str, float]:
    """Personalized PageRank over the entity<->fact bipartite graph seeded at
    the deal's entities. Returns {fact_uuid: ppr_score} for ranking."""
    if not facts:
        return {}
    # collect the subgraph: entities touching the returned facts (1 hop)
    uuids = [f["uuid"] for f in facts if f.get("uuid")]
    if not uuids:
        return {}
    rows, _, _ = await driver.execute_query(
        "MATCH (n:Entity)-[r1:RELATES_TO]->(e:RelatesToNode_)-[r2:RELATES_TO]->(m:Entity) "
        "WHERE e.uuid IN $uuids RETURN n.uuid AS src, e.uuid AS mid, m.uuid AS dst, "
        "n.name AS src_name, m.name AS dst_name",
        uuids=uuids,
    )
    nodes: list[str] = []
    index: dict[str, int] = {}

    def _idx(x: str) -> int:
        if x not in index:
            index[x] = len(nodes)
            nodes.append(x)
        return index[x]

    edges: list[tuple[int, int]] = []
    for r in rows or []:
        a, b, c = _idx(r["src"]), _idx(r["mid"]), _idx(r["dst"])
        edges += [(a, b), (b, a), (b, c), (c, b)]

    if not nodes:
        return {}
    n = len(nodes)
    A = np.zeros((n, n))
    for i, j in edges:
        A[i, j] = 1.0
    deg = A.sum(axis=1)
    deg[deg == 0] = 1.0
    P = A / deg[:, None]

    seeds = np.zeros(n)
    seed_lower = [s.lower() for s in seed_entity_names]
    for r in rows or []:
        for key, name in (("src", r.get("src_name")), ("dst", r.get("dst_name"))):
            nm = (name or "").lower()
            if any(s and (s in nm or nm in s) for s in seed_lower):
                seeds[_idx(r[key])] += 1.0
    if seeds.sum() == 0:
        seeds[:] = 1.0
    seeds = seeds / seeds.sum()

    v = seeds.copy()
    for _ in range(40):
        v = alpha * seeds + (1 - alpha) * (P.T @ v)
    out: dict[str, float] = {}
    for x, i in index.items():
        if x in uuids:
            out[x] = float(v[i])
    return out


def blend_rank(facts: list[dict], ppr: dict[str, float],
               sims: dict[str, float] | None = None, w_ppr: float = 0.6) -> list[dict]:
    """Attach blended importance = w*PPR + (1-w)*embedding_sim, then apply the
    stored salience multiplier. Sorts descending."""
    sims = sims or {}
    pmax = max(ppr.values()) if ppr else 1.0
    smax = max(sims.values()) if sims else 1.0
    for f in facts:
        u = f.get("uuid")
        f["_ppr"] = ppr.get(u, 0.0)
        f["_sim"] = sims.get(u, 0.0)
        base = w_ppr * (f["_ppr"] / pmax if pmax else 0.0) + \
            (1 - w_ppr) * (f["_sim"] / smax if smax else 0.0)
        f["_importance"] = base * float(f.get("salience") or 1.0)
    facts.sort(key=lambda f: f.get("_importance", 0.0), reverse=True)
    return facts


def is_hard_truth(edge_name: str, fact_text: str) -> bool:
    name = (edge_name or "").lower()
    text = (fact_text or "").lower()
    if name in HARD_TRUTH_TYPES:
        return True
    return any(k in text for k in ("docusign", "signed", "delivered to", "approved by"))
