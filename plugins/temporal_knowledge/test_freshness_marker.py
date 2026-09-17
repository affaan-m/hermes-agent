#!/usr/bin/env python3
"""Unit tests for the commit freshness marker in desk_graphiti_service.

The supplier-relation store moved from the Kuzu file (graphiti_desk) to Neo4j
on 2026-09-01, so file-mtime freshness checks on the old store can never
advance. The service now writes an explicit marker after every successful
episode commit; these tests pin that contract with a fake clock and a
temporary directory.

Run with the Hermes venv python (the service module imports graphiti_core):
  ~/.hermes/hermes-agent/venv/bin/python -m unittest test_freshness_marker -v
"""
from __future__ import annotations

import json
import os
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import desk_graphiti_service as svc  # noqa: E402


class FreshnessMarkerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.marker = pathlib.Path(self.tmp.name) / "sub" / "graphiti_freshness"
        self._old_env = os.environ.get("GRAPHITI_FRESHNESS_FILE")
        os.environ["GRAPHITI_FRESHNESS_FILE"] = str(self.marker)
        self._old_ingested = svc.STATE.get("ingested", 0)
        svc.STATE["ingested"] = 41
        svc._last_freshness_log = 0.0

    def tearDown(self) -> None:
        if self._old_env is None:
            os.environ.pop("GRAPHITI_FRESHNESS_FILE", None)
        else:
            os.environ["GRAPHITI_FRESHNESS_FILE"] = self._old_env
        svc.STATE["ingested"] = self._old_ingested
        self.tmp.cleanup()

    def test_default_path_is_profile_marker(self) -> None:
        os.environ.pop("GRAPHITI_FRESHNESS_FILE", None)
        self.assertEqual(
            svc._freshness_marker_path(),
            pathlib.Path("~/.hermes/profiles/ito/graphiti_freshness").expanduser(),
        )

    def test_write_creates_parent_and_atomic_marker(self) -> None:
        path = svc._write_freshness_marker("desk", now=1_800_000_000.0)
        self.assertEqual(path, self.marker)
        payload = json.loads(self.marker.read_text())
        self.assertEqual(payload["ts"], 1_800_000_000.0)
        self.assertEqual(payload["group_id"], "desk")
        self.assertEqual(payload["ingested"], 41)
        self.assertFalse(self.marker.with_name(self.marker.name + ".tmp").exists())

    def test_mtime_advances_on_each_commit(self) -> None:
        svc._write_freshness_marker("desk", now=1_800_000_000.0)
        first = self.marker.stat().st_mtime
        svc._write_freshness_marker("desk", now=1_800_000_100.0)
        second = self.marker.stat().st_mtime
        self.assertGreaterEqual(second, first)
        self.assertEqual(json.loads(self.marker.read_text())["ts"], 1_800_000_100.0)

    def test_log_line_rate_limited(self) -> None:
        from io import StringIO

        captured = StringIO()
        old_stdout = sys.stdout
        sys.stdout = captured
        try:
            svc._write_freshness_marker("desk", now=1_800_000_000.0)
            svc._write_freshness_marker("desk", now=1_800_000_010.0)
            svc._write_freshness_marker("desk", now=1_800_000_400.0)
        finally:
            sys.stdout = old_stdout
        lines = [l for l in captured.getvalue().splitlines() if "freshness marker written" in l]
        self.assertEqual(len(lines), 2)  # first write, then one after the interval

    def test_apply_marks_freshness_only_on_success(self) -> None:
        import asyncio

        class FakeResult:
            edges = []

        class FakeGraphiti:
            def __init__(self, fail: bool):
                self.fail = fail
                self.driver = None

            async def add_episode(self, **kwargs):
                if self.fail:
                    raise RuntimeError("commit boom")
                return FakeResult()

        job = {"group_id": "desk", "text": "x", "source": "message"}
        ok = asyncio.run(svc._apply(FakeGraphiti(fail=False), job))
        self.assertTrue(ok)
        self.assertTrue(self.marker.exists())
        self.marker.unlink()
        failed = asyncio.run(svc._apply(FakeGraphiti(fail=True), job))
        self.assertFalse(failed)
        self.assertFalse(self.marker.exists())


if __name__ == "__main__":
    unittest.main()
