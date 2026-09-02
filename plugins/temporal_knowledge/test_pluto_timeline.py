#!/usr/bin/env python3
"""Test the temporal knowledge graph on the Pluto/Ronit timeline.

Proves the system can answer: "was there an open 2-node client ask as of
tonight 19:10?" (correct answer: no, converted to signed 1-node contract).
"""

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from plugins.temporal_knowledge.temporal_graph import (
    TemporalGraph,
    baseline_check,
    ingest_obligations,
    ingest_workstream_artifacts,
    run_pruning,
)


def main():
    db_path = Path("/tmp/test_temporal_graph.db")
    if db_path.exists():
        db_path.unlink()

    graph = TemporalGraph(db_path)

    # Ingest from real ledger
    ito_db = Path.home() / ".hermes" / "profiles" / "ito" / "ito.db"
    ws_dir = Path.home() / ".codex" / "workstream-results"

    print("=== Ingesting obligations ===")
    n = ingest_obligations(graph, ito_db)
    print(f"Ingested {n} obligations")

    print("\n=== Ingesting workstream artifacts ===")
    w = ingest_workstream_artifacts(graph, ws_dir)
    print(f"Ingested {w} workstream artifacts")

    print("\n=== Running pruning pass ===")
    p = run_pruning(graph, ito_db)
    print(f"Pruned {p} facts")

    # Key timestamps from the case study
    # obligation 215 delivered contract at 1787760780 (Aug 26 ~19:53 UTC)
    # obligation 223 docusign at 1787763052 (Aug 26 ~20:30 UTC)
    # obligation 267 filed at 1787785800 (Aug 26 ~23:10 UTC = 19:10 ET)
    ts_contract_delivered = 1787760780
    ts_docusign = 1787763052
    ts_approval_267 = 1787785800  # "tonight 19:10" ET

    print("\n" + "=" * 60)
    print("PLUTO / RONIT TIMELINE VERIFICATION")
    print("=" * 60)

    # Test 1: Before contract was delivered
    print(f"\n--- Test 1: Before contract delivery (ts={ts_contract_delivered - 3600}) ---")
    r = baseline_check(graph, "pluto", ts_contract_delivered - 3600)
    print(f"  Has signed contract: {r['has_signed_contract']}")
    print(f"  Current asks: {r['current_asks']}")
    print(f"  Recommendation: {r['recommendation']}")

    # Test 2: After contract delivered
    print(f"\n--- Test 2: After contract delivered (ts={ts_contract_delivered + 3600}) ---")
    r = baseline_check(graph, "pluto", ts_contract_delivered + 3600)
    print(f"  Has signed contract: {r['has_signed_contract']}")
    print(f"  Contract states: {r['contract_states']}")
    print(f"  Recommendation: {r['recommendation']}")

    # Test 3: At the exact moment approval 267 was filed (the stale baseline)
    print(f"\n--- Test 3: At approval 267 filing time (ts={ts_approval_267}) ---")
    r = baseline_check(graph, "pluto", ts_approval_267)
    print(f"  Has signed contract: {r['has_signed_contract']}")
    print(f"  Current asks: {r['current_asks']}")
    print(f"  Recommendation: {r['recommendation']}")
    assert r["has_signed_contract"], "FAIL: Should detect signed contract"
    assert "DO NOT ASK" in r["recommendation"], "FAIL: Should warn against asking"
    print("  PASS: Would have blocked stale approval 267")

    # Test 4: Check ronit deal directly
    print(f"\n--- Test 4: Ronit deal at approval 267 time ---")
    r = baseline_check(graph, "ronit", ts_approval_267)
    print(f"  Has signed contract: {r['has_signed_contract']}")
    print(f"  Current asks: {r['current_asks']}")
    print(f"  Recently superseded: {len(r['recently_superseded'])} facts")

    # Test 5: Point-in-time query for the 2-node ask
    print(f"\n--- Test 5: Was there an open 2-node ask at {ts_approval_267}? ---")
    r = baseline_check(graph, "pluto", ts_approval_267)
    has_2node_ask = any(
        "2" in str(a.get("value", "")) or "two" in str(a.get("value", "")).lower()
        for a in r["current_asks"]
    )
    print(f"  Open 2-node ask: {has_2node_ask}")
    print(f"  Current asks: {r['current_asks']}")
    assert not has_2node_ask, "FAIL: Should NOT have open 2-node ask"
    print("  PASS: Correctly identifies no open 2-node ask (converted to 1-node contract)")

    # Test 6: What the supersession chain looks like
    print(f"\n--- Test 6: Supersession chain for obligation 267 ---")
    from plugins.temporal_knowledge.temporal_graph import Fact, Entity
    ent = graph.get_entity("obligation:267")
    if ent:
        facts = graph.query_at("obligation:267", ts_approval_267)
        for f in facts:
            print(f"  {f.predicate}: {f.object_value} (valid_from={f.valid_from}, valid_to={f.valid_to})")

    print("\n" + "=" * 60)
    print("ALL TESTS PASSED")
    print("=" * 60)

    graph.close()


if __name__ == "__main__":
    main()
