"""Temporal Knowledge Graph — bi-temporal fact store for Itô ledger verification.

Implements Graphiti-style semantics without Neo4j:
  - valid_from / valid_to : when the fact is true in the real world
  - recorded_at           : when the fact was learned by the system
  - superseded_by         : fact_id that replaced this one (pruning edge)

Point-in-time query: given a deal/counterparty and a timestamp, return only
facts where valid_from <= ts AND (valid_to IS NULL OR valid_to > ts).
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS entities (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL,          -- counterparty | deal | obligation | decision | delivery | event
    name        TEXT NOT NULL,
    canonical_key TEXT NOT NULL UNIQUE, -- stable dedup key
    created_ts  INTEGER NOT NULL,
    metadata    TEXT                    -- JSON blob
);

CREATE TABLE IF NOT EXISTS facts (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_id    INTEGER NOT NULL REFERENCES entities(id),
    predicate     TEXT NOT NULL,        -- has_status | has_ask | owes | superseded_by | signed_contract | ...
    object_value  TEXT,                 -- literal value (JSON if complex)
    object_id     INTEGER REFERENCES entities(id),  -- if predicate points to another entity
    valid_from    INTEGER NOT NULL,     -- real-world start of validity
    valid_to      INTEGER,              -- real-world end (NULL = still valid)
    recorded_at   INTEGER NOT NULL,     -- system ingestion timestamp
    source_ref    TEXT,                 -- e.g. "obligation:267", "event:12345"
    confidence    REAL DEFAULT 1.0,
    metadata      TEXT
);

CREATE INDEX IF NOT EXISTS idx_facts_subject ON facts(subject_id, valid_from DESC);
CREATE INDEX IF NOT EXISTS idx_facts_predicate ON facts(predicate, valid_from DESC);
CREATE INDEX IF NOT EXISTS idx_facts_valid ON facts(valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_facts_recorded ON facts(recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_facts_supersession ON facts(predicate, object_id) WHERE predicate = 'superseded_by';

CREATE TABLE IF NOT EXISTS ingest_checkpoints (
    source_name TEXT PRIMARY KEY,
    last_id     INTEGER NOT NULL,
    last_ts     INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);
"""

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Entity:
    kind: str
    name: str
    canonical_key: str
    created_ts: int
    metadata: Dict[str, Any] = field(default_factory=dict)
    id: Optional[int] = None


@dataclass
class Fact:
    subject_id: int
    predicate: str
    object_value: Optional[str] = None
    object_id: Optional[int] = None
    valid_from: int = 0
    valid_to: Optional[int] = None
    recorded_at: int = 0
    source_ref: str = ""
    confidence: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    id: Optional[int] = None


# ---------------------------------------------------------------------------
# Temporal Graph Store
# ---------------------------------------------------------------------------

class TemporalGraph:
    """Bi-temporal SQLite graph. Localhost-only, zero external deps."""

    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    # -- entity ops ---------------------------------------------------------

    def upsert_entity(self, ent: Entity) -> int:
        cur = self._conn.execute(
            """INSERT INTO entities (kind, name, canonical_key, created_ts, metadata)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(canonical_key) DO UPDATE SET
                   name=excluded.name, metadata=excluded.metadata
               RETURNING id""",
            (ent.kind, ent.name, ent.canonical_key, ent.created_ts,
             json.dumps(ent.metadata)),
        )
        row = cur.fetchone()
        self._conn.commit()
        return row[0]

    def get_entity(self, canonical_key: str) -> Optional[Entity]:
        cur = self._conn.execute(
            "SELECT * FROM entities WHERE canonical_key = ?", (canonical_key,)
        )
        row = cur.fetchone()
        if not row:
            return None
        return Entity(
            id=row["id"], kind=row["kind"], name=row["name"],
            canonical_key=row["canonical_key"], created_ts=row["created_ts"],
            metadata=json.loads(row["metadata"] or "{}"),
        )

    # -- fact ops -----------------------------------------------------------

    def add_fact(self, fact: Fact) -> int:
        if fact.recorded_at == 0:
            fact.recorded_at = int(time.time())
        cur = self._conn.execute(
            """INSERT INTO facts
               (subject_id, predicate, object_value, object_id,
                valid_from, valid_to, recorded_at, source_ref, confidence, metadata)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               RETURNING id""",
            (fact.subject_id, fact.predicate, fact.object_value,
             fact.object_id, fact.valid_from, fact.valid_to,
             fact.recorded_at, fact.source_ref, fact.confidence,
             json.dumps(fact.metadata)),
        )
        row = cur.fetchone()
        self._conn.commit()
        return row[0]

    def close_fact(self, fact_id: int, valid_to: int, superseded_by: Optional[int] = None):
        """Invalidate a fact as of `valid_to`. Optionally record supersession."""
        self._conn.execute(
            "UPDATE facts SET valid_to = ? WHERE id = ?", (valid_to, fact_id)
        )
        if superseded_by is not None:
            # add supersession edge
            self._conn.execute(
                """INSERT INTO facts
                   (subject_id, predicate, object_id, valid_from, valid_to, recorded_at, source_ref)
                   SELECT subject_id, 'superseded_by', ?, valid_from, ?, ?, source_ref
                   FROM facts WHERE id = ?""",
                (superseded_by, valid_to, int(time.time()), fact_id),
            )
        self._conn.commit()

    # -- point-in-time query -------------------------------------------------

    def query_at(
        self,
        subject_key: str,
        ts: int,
        predicates: Optional[List[str]] = None,
    ) -> List[Fact]:
        """Return all facts valid at timestamp `ts` for entity `subject_key`."""
        entity = self.get_entity(subject_key)
        if not entity:
            return []
        sql = """
            SELECT * FROM facts
            WHERE subject_id = ?
              AND valid_from <= ?
              AND (valid_to IS NULL OR valid_to > ?)
        """
        params: list = [entity.id, ts, ts]
        if predicates:
            sql += " AND predicate IN ({})".format(
                ",".join("?" * len(predicates))
            )
            params.extend(predicates)
        sql += " ORDER BY valid_from DESC"
        cur = self._conn.execute(sql, params)
        return [self._row_to_fact(r) for r in cur.fetchall()]

    def query_between(
        self,
        subject_key: str,
        start_ts: int,
        end_ts: int,
        predicates: Optional[List[str]] = None,
    ) -> List[Fact]:
        """Return facts whose validity window overlaps [start_ts, end_ts]."""
        entity = self.get_entity(subject_key)
        if not entity:
            return []
        sql = """
            SELECT * FROM facts
            WHERE subject_id = ?
              AND valid_from <= ?
              AND (valid_to IS NULL OR valid_to >= ?)
        """
        params: list = [entity.id, end_ts, start_ts]
        if predicates:
            sql += " AND predicate IN ({})".format(
                ",".join("?" * len(predicates))
            )
            params.extend(predicates)
        sql += " ORDER BY valid_from"
        cur = self._conn.execute(sql, params)
        return [self._row_to_fact(r) for r in cur.fetchall()]

    def current_facts(
        self,
        subject_key: str,
        predicates: Optional[List[str]] = None,
    ) -> List[Fact]:
        """Return facts valid right now."""
        return self.query_at(subject_key, int(time.time()), predicates)

    # -- pruning / supersession ----------------------------------------------

    def find_superseded(self, subject_key: str, as_of: int) -> List[Tuple[Fact, Fact]]:
        """Return (old_fact, new_fact) pairs where old was superseded by new
        at or before `as_of`. Used to audit the pruning pass."""
        entity = self.get_entity(subject_key)
        if not entity:
            return []
        cur = self._conn.execute(
            """SELECT f1.*, f2.*
               FROM facts f1
               JOIN facts f2 ON f2.id = (
                   SELECT object_id FROM facts
                   WHERE subject_id = f1.subject_id
                     AND predicate = 'superseded_by'
                     AND object_id = f2.id
               )
               WHERE f1.subject_id = ?
                 AND f1.valid_to IS NOT NULL
                 AND f1.valid_to <= ?
               ORDER BY f1.valid_from""",
            (entity.id, as_of),
        )
        # Simplified: just return closed facts with their supersession edge
        cur = self._conn.execute(
            """SELECT * FROM facts
               WHERE subject_id = ?
                 AND valid_to IS NOT NULL
                 AND valid_to <= ?
                 AND predicate != 'superseded_by'
               ORDER BY valid_from""",
            (entity.id, as_of),
        )
        closed = [self._row_to_fact(r) for r in cur.fetchall()]
        result = []
        for c in closed:
            sup = self._conn.execute(
                """SELECT * FROM facts
                   WHERE subject_id = ? AND predicate = 'superseded_by'
                     AND object_id IN (
                         SELECT id FROM facts
                         WHERE subject_id = ? AND valid_from > ?
                     )
                   ORDER BY valid_from DESC LIMIT 1""",
                (entity.id, entity.id, c.valid_from),
            ).fetchone()
            if sup:
                result.append((c, self._row_to_fact(sup)))
        return result

    # -- checkpoint ----------------------------------------------------------

    def checkpoint(self, source_name: str, last_id: int, last_ts: int):
        self._conn.execute(
            """INSERT INTO ingest_checkpoints (source_name, last_id, last_ts, updated_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(source_name) DO UPDATE SET
                   last_id=excluded.last_id, last_ts=excluded.last_ts,
                   updated_at=excluded.updated_at""",
            (source_name, last_id, last_ts, int(time.time())),
        )
        self._conn.commit()

    def get_checkpoint(self, source_name: str) -> Tuple[int, int]:
        cur = self._conn.execute(
            "SELECT last_id, last_ts FROM ingest_checkpoints WHERE source_name = ?",
            (source_name,),
        )
        row = cur.fetchone()
        return (row["last_id"], row["last_ts"]) if row else (0, 0)

    # -- helpers -------------------------------------------------------------

    @staticmethod
    def _row_to_fact(row: sqlite3.Row) -> Fact:
        return Fact(
            id=row["id"],
            subject_id=row["subject_id"],
            predicate=row["predicate"],
            object_value=row["object_value"],
            object_id=row["object_id"],
            valid_from=row["valid_from"],
            valid_to=row["valid_to"],
            recorded_at=row["recorded_at"],
            source_ref=row["source_ref"],
            confidence=row["confidence"],
            metadata=json.loads(row["metadata"] or "{}"),
        )

    def close(self):
        self._conn.close()


# ---------------------------------------------------------------------------
# Ingestion from Itô ledger
# ---------------------------------------------------------------------------

def canonical_deal_key(counterparty: str) -> str:
    """Normalize counterparty strings into a stable deal key."""
    # Strip Slack IDs, channel refs, extra whitespace
    import re
    s = counterparty.strip().lower()
    s = re.sub(r"<@[uw][a-z0-9]+>", "", s, flags=re.I)
    s = re.sub(r"#[a-z0-9-]+", "", s, flags=re.I)
    s = re.sub(r"\s+", " ", s).strip()
    # Extract first meaningful token (person or company name)
    for token in ["ronit", "cam", "joe", "akash", "pluto", "strike", "daryl", "sandeep"]:
        if token in s:
            return token
    return s.split()[0] if s else "unknown"


def ingest_obligations(graph: TemporalGraph, ito_db_path: Path | str, batch_size: int = 500):
    """Ingest obligations lifecycle from ito.db into the temporal graph."""
    conn = sqlite3.connect(str(ito_db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row

    last_id, _ = graph.get_checkpoint("obligations")
    cur = conn.execute(
        """SELECT id, counterparty, direction, status, opened_ts, last_touch_ts,
                  expires_ts, summary, updated_at
           FROM obligations WHERE id > ? ORDER BY id LIMIT ?""",
        (last_id, batch_size),
    )
    rows = cur.fetchall()
    if not rows:
        conn.close()
        return 0

    now = int(time.time())
    for row in rows:
        oblig_id = row["id"]
        counterparty = row["counterparty"] or "unknown"
        deal_key = f"deal:{canonical_deal_key(counterparty)}"
        oblig_key = f"obligation:{oblig_id}"

        # Ensure deal entity exists
        deal_ent = Entity(
            kind="deal",
            name=canonical_deal_key(counterparty),
            canonical_key=deal_key,
            created_ts=row["opened_ts"] or now,
            metadata={"counterparties": [counterparty]},
        )
        deal_id = graph.upsert_entity(deal_ent)

        # Obligation entity
        oblig_ent = Entity(
            kind="obligation",
            name=f"obligation-{oblig_id}",
            canonical_key=oblig_key,
            created_ts=row["opened_ts"] or now,
            metadata={
                "counterparty": counterparty,
                "direction": row["direction"],
                "status": row["status"],
                "summary": (row["summary"] or "")[:500],
            },
        )
        oblig_id_e = graph.upsert_entity(oblig_ent)

        # Facts about the obligation
        opened_ts = row["opened_ts"] or now
        updated_ts = row["updated_at"] or opened_ts
        status = row["status"] or "open"

        # has_status fact (valid from open until status changed or now)
        valid_to = None
        if status.startswith("closed"):
            valid_to = updated_ts
        graph.add_fact(Fact(
            subject_id=oblig_id_e,
            predicate="has_status",
            object_value=status,
            valid_from=opened_ts,
            valid_to=valid_to,
            recorded_at=updated_ts,
            source_ref=f"obligation:{oblig_id}",
        ))

        # owes fact
        graph.add_fact(Fact(
            subject_id=oblig_id_e,
            predicate="owes",
            object_value=row["direction"],
            valid_from=opened_ts,
            valid_to=valid_to,
            recorded_at=updated_ts,
            source_ref=f"obligation:{oblig_id}",
        ))

        # Link obligation to deal
        graph.add_fact(Fact(
            subject_id=oblig_id_e,
            predicate="part_of",
            object_id=deal_id,
            valid_from=opened_ts,
            valid_to=valid_to,
            recorded_at=updated_ts,
            source_ref=f"obligation:{oblig_id}",
        ))

        # Extract asks from summary (heuristic)
        summary = (row["summary"] or "").lower()
        if "node" in summary and ("h200" in summary or "h100" in summary):
            # extract node count if present
            import re
            m = re.search(r"(\d+)\s*(?:x\s*)?node", summary)
            node_count = int(m.group(1)) if m else None
            gpu_type = "h200" if "h200" in summary else "h100"
            ask_value = json.dumps({"nodes": node_count, "gpu": gpu_type})
            graph.add_fact(Fact(
                subject_id=oblig_id_e,
                predicate="has_ask",
                object_value=ask_value,
                valid_from=opened_ts,
                valid_to=valid_to,
                recorded_at=updated_ts,
                source_ref=f"obligation:{oblig_id}",
            ))
            # Also record on the deal
            graph.add_fact(Fact(
                subject_id=deal_id,
                predicate="has_ask",
                object_value=ask_value,
                valid_from=opened_ts,
                valid_to=valid_to,
                recorded_at=updated_ts,
                source_ref=f"obligation:{oblig_id}",
            ))

        # Supersession detection from status text
        if status == "closed_superseded":
            # Parse "superseded by obligation NNN" from summary
            m = re.search(r"superseded by obligation (\d+)", summary)
            if m:
                sup_id = int(m.group(1))
                sup_ent = graph.get_entity(f"obligation:{sup_id}")
                if sup_ent:
                    graph.add_fact(Fact(
                        subject_id=oblig_id_e,
                        predicate="superseded_by",
                        object_id=sup_ent.id,
                        valid_from=updated_ts,
                        recorded_at=now,
                        source_ref=f"obligation:{oblig_id}",
                    ))
                    # Also close the deal-level ask if this obligation carried it
                    # (pruning will be a separate pass; here we just record the edge)

        # Contract signed detection
        if "signed" in summary or "docusign" in summary or "contract" in summary:
            if status in ("closed", "closed_already_handled"):
                graph.add_fact(Fact(
                    subject_id=deal_id,
                    predicate="contract_state",
                    object_value="signed_or_delivered",
                    valid_from=updated_ts,
                    recorded_at=updated_ts,
                    source_ref=f"obligation:{oblig_id}",
                ))

        graph.checkpoint("obligations", oblig_id, row["updated_at"] or now)

    conn.close()
    return len(rows)


def ingest_workstream_artifacts(graph: TemporalGraph, workstream_dir: Path | str):
    """Ingest workstream JSON artifacts as entity-relationship-validity facts."""
    import glob
    now = int(time.time())
    count = 0
    for path in glob.glob(str(workstream_dir) + "/*.json"):
        p = Path(path)
        try:
            data = json.loads(p.read_text())
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        # Only process obligation/pluto-related receipts
        name = p.name.lower()
        if "pluto" not in name and "obligation" not in name:
            continue

        # Extract obligation ids and status
        oblig_id = data.get("obligation_id") or data.get("obligation")
        status = data.get("status")
        recorded_at = data.get("recorded_at") or now
        sup_by = data.get("superseded_by")

        if oblig_id:
            oblig_key = f"obligation:{oblig_id}"
            ent = graph.get_entity(oblig_key)
            if not ent:
                # Create stub
                ent = Entity(
                    kind="obligation",
                    name=f"obligation-{oblig_id}",
                    canonical_key=oblig_key,
                    created_ts=recorded_at,
                    metadata={"source": "workstream", "file": p.name},
                )
                ent.id = graph.upsert_entity(ent)

            if status:
                # Close old status facts and open new one
                old_facts = graph.query_at(oblig_key, recorded_at, ["has_status"])
                for of in old_facts:
                    graph.close_fact(of.id, recorded_at)
                graph.add_fact(Fact(
                    subject_id=ent.id,
                    predicate="has_status",
                    object_value=status,
                    valid_from=recorded_at,
                    recorded_at=recorded_at,
                    source_ref=f"workstream:{p.name}",
                ))

            if sup_by:
                sup_ent = graph.get_entity(f"obligation:{sup_by}")
                if sup_ent:
                    graph.add_fact(Fact(
                        subject_id=ent.id,
                        predicate="superseded_by",
                        object_id=sup_ent.id,
                        valid_from=recorded_at,
                        recorded_at=recorded_at,
                        source_ref=f"workstream:{p.name}",
                    ))
                    # Also mark the old ask as invalid at this time
                    old_asks = graph.query_at(oblig_key, recorded_at, ["has_ask"])
                    for oa in old_asks:
                        graph.close_fact(oa.id, recorded_at)

            count += 1
    return count


# ---------------------------------------------------------------------------
# Baseline Check API (the pre-draft validation interface)
# ---------------------------------------------------------------------------

def baseline_check(
    graph: TemporalGraph,
    deal_key: str,
    as_of_ts: Optional[int] = None,
) -> Dict[str, Any]:
    """Return the CURRENT valid facts for a deal and what superseded what.

    Used before drafting any external communication to verify the baseline.
    """
    if as_of_ts is None:
        as_of_ts = int(time.time())

    if not deal_key.startswith("deal:"):
        deal_key = f"deal:{deal_key}"

    # Current valid facts
    current = graph.query_at(deal_key, as_of_ts)
    current_by_pred: Dict[str, List[Dict[str, Any]]] = {}
    for f in current:
        current_by_pred.setdefault(f.predicate, []).append({
            "value": f.object_value,
            "valid_from": f.valid_from,
            "valid_to": f.valid_to,
            "source": f.source_ref,
        })

    # Recently superseded facts (last 48h window)
    window_start = as_of_ts - 48 * 3600
    all_in_window = graph.query_between(deal_key, window_start, as_of_ts)
    superseded = []
    for f in all_in_window:
        if f.valid_to is not None and f.valid_to <= as_of_ts:
            # find what superseded it
            sup_edges = graph.query_at(deal_key, f.valid_to + 1, ["superseded_by"])
            superseded.append({
                "fact": f.predicate,
                "value": f.object_value,
                "valid_from": f.valid_from,
                "valid_to": f.valid_to,
                "superseded_at": f.valid_to,
            })

    # Contract state
    contract_states = current_by_pred.get("contract_state", [])
    has_signed_contract = any(
        cs.get("value") in ("signed_or_delivered", "signed")
        for cs in contract_states
    )

    # Open asks
    asks = current_by_pred.get("has_ask", [])

    return {
        "deal": deal_key,
        "as_of": as_of_ts,
        "as_of_iso": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(as_of_ts)),
        "has_signed_contract": has_signed_contract,
        "contract_states": contract_states,
        "current_asks": asks,
        "current_facts": current_by_pred,
        "recently_superseded": superseded,
        "recommendation": (
            "DO NOT ASK ABOUT SPECS — contract already signed/delivered"
            if has_signed_contract
            else "Baseline valid — no signed contract on record"
        ),
    }


# ---------------------------------------------------------------------------
# Pruning pass
# ---------------------------------------------------------------------------

def run_pruning(graph: TemporalGraph, ito_db_path: Path | str, batch_size: int = 500):
    """Auto-close facts invalidated by newer ledger events.

    Scans obligations that have transitioned to closed_superseded and ensures
    their has_ask / has_status facts are properly closed at the supersession
    timestamp. Idempotent.
    """
    conn = sqlite3.connect(str(ito_db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """SELECT id, counterparty, status, opened_ts, updated_at, summary
           FROM obligations
           WHERE status = 'closed_superseded' AND id > ?
           ORDER BY id LIMIT ?""",
        (graph.get_checkpoint("pruning")[0], batch_size),
    )
    rows = cur.fetchall()
    pruned = 0
    now = int(time.time())
    for row in rows:
        oblig_key = f"obligation:{row['id']}"
        ent = graph.get_entity(oblig_key)
        if not ent:
            continue
        # Close any still-open has_ask or has_status facts on this obligation
        open_facts = graph.query_at(oblig_key, now)
        for f in open_facts:
            if f.predicate in ("has_ask", "has_status", "owes") and f.valid_to is None:
                graph.close_fact(f.id, row["updated_at"] or now)
                pruned += 1
        # Also close deal-level asks that came from this obligation
        deal_key = f"deal:{canonical_deal_key(row['counterparty'] or '')}"
        deal_ent = graph.get_entity(deal_key)
        if deal_ent:
            deal_facts = graph.query_at(deal_key, now, ["has_ask"])
            for df in deal_facts:
                if df.source_ref == f"obligation:{row['id']}" and df.valid_to is None:
                    graph.close_fact(df.id, row["updated_at"] or now)
                    pruned += 1
        graph.checkpoint("pruning", row["id"], now)
    conn.close()
    return pruned


# ---------------------------------------------------------------------------
# CLI / demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Temporal Knowledge Graph for Itô ledger")
    parser.add_argument("--db", default="~/.hermes/profiles/ito/temporal_graph.db")
    parser.add_argument("--ito-db", default="~/.hermes/profiles/ito/ito.db")
    parser.add_argument("--workstream", default="~/.codex/workstream-results")
    parser.add_argument("--ingest", action="store_true")
    parser.add_argument("--prune", action="store_true")
    parser.add_argument("--check", help="Baseline check for deal key")
    parser.add_argument("--as-of", type=int, help="Timestamp for point-in-time query")
    args = parser.parse_args()

    graph = TemporalGraph(Path(args.db).expanduser())

    if args.ingest:
        n = ingest_obligations(graph, Path(args.ito_db).expanduser())
        print(f"Ingested {n} obligations")
        w = ingest_workstream_artifacts(graph, Path(args.workstream).expanduser())
        print(f"Ingested {w} workstream artifacts")

    if args.prune:
        p = run_pruning(graph, Path(args.ito_db).expanduser())
        print(f"Pruned {p} facts")

    if args.check:
        result = baseline_check(graph, args.check, args.as_of)
        print(json.dumps(result, indent=2))

    graph.close()
