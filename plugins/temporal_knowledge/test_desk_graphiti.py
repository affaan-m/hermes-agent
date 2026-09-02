#!/usr/bin/env python3
"""Acceptance test: desk Graphiti service answers the 267 case study correctly.

Proves: was there an open 2-node client ask as of 19:10 ET Aug 26 2026?
Correct answer: NO — converted to signed 1-node contract.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import requests

GRAPHITI_URL = os.environ.get("GRAPHITI_URL", "http://127.0.0.1:8098")
GROUP_ID = "desk"

# Key timestamps
TS_CONTRACT_DELIVERED = 1787760780   # obligation 215 delivered
TS_DOCUSIGN = 1787763052              # obligation 223 docusign
TS_APPROVAL_267 = 1787785800          # 19:10 ET = 23:10 UTC


def wait_for_service(timeout: int = 30):
    for i in range(timeout):
        try:
            r = requests.get(f"{GRAPHITI_URL}/health", timeout=2)
            if r.status_code == 200 and r.json().get("ok"):
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def baseline(deal_key: str, as_of: str | None = None):
    r = requests.post(
        f"{GRAPHITI_URL}/baseline",
        json={"group_id": GROUP_ID, "deal_key": deal_key, "as_of": as_of},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def main():
    print("=" * 60)
    print("DESK GRAPHITI — 267 ACCEPTANCE TEST")
    print("=" * 60)

    if not wait_for_service():
        print("FAIL: Graphiti service not running. Start with:")
        print("  ./plugins/temporal_knowledge/run-desk-service.sh")
        sys.exit(1)

    print(f"\nService healthy at {GRAPHITI_URL}")

    # Test 1: Before contract delivery (contract delivered ~16:13 UTC)
    print("\n--- Test 1: Before contract delivery ---")
    r = baseline("pluto", "2026-08-26T15:00:00Z")
    print(f"  has_signed_contract: {r['has_signed_contract']}")
    print(f"  recommendation: {r['recommendation']}")
    assert not r["has_signed_contract"], "Should NOT have contract before delivery"

    # Test 2: After contract delivery
    print("\n--- Test 2: After contract delivery ---")
    r = baseline("pluto", "2026-08-26T17:00:00Z")
    print(f"  has_signed_contract: {r['has_signed_contract']}")
    print(f"  recommendation: {r['recommendation']}")
    assert r["has_signed_contract"], "Should detect contract after delivery"

    # Test 3: At approval 267 time (19:10 ET = 23:10 UTC)
    print("\n--- Test 3: At approval 267 filing time (19:10 ET) ---")
    r = baseline("pluto", "2026-08-26T23:10:00Z")
    print(f"  has_signed_contract: {r['has_signed_contract']}")
    print(f"  open_asks: {len(r['open_asks'])}")
    print(f"  recommendation: {r['recommendation']}")
    assert r["has_signed_contract"], "Should detect signed contract at 19:10 ET"
    assert "DO NOT ASK" in r["recommendation"], "Should warn against asking"

    # Test 4: No open 2-node ask (real 2-node phrasing, not a bare "2")
    print("\n--- Test 4: Open 2-node ask check ---")
    import re
    two_node = re.compile(r"\b(two|2)[ -]node|two or four|needs two\b", re.I)
    has_2node = any(two_node.search(str(a.get("fact", ""))) for a in r["open_asks"])
    print(f"  has_open_2node_ask: {has_2node}")
    print(f"  open_asks: {[a.get('fact','')[:90] for a in r['open_asks']]}")
    assert not has_2node, "Should NOT have open 2-node ask"

    print("\n" + "=" * 60)
    print("ALL ACCEPTANCE TESTS PASSED")
    print("=" * 60)


if __name__ == "__main__":
    main()
