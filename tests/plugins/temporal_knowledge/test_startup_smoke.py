#!/usr/bin/env python3
"""Startup smoke test for the desk Graphiti service (HOTFIX-UPSTREAM).

PR #26 dropped two imports (OpenAIClient, LLMConfig) that are only referenced
inside the lifespan startup, so the module compiled and every stub-based unit
test passed while production died at boot with a NameError. These tests boot
the service headlessly so CI catches that class: module-level name presence
plus a full lifespan run against fakes. graphiti_core is stubbed the same way
as test_ingest_resilience.py (not a fork test dependency).
"""
from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))


def _stub_graphiti_core() -> None:
    if "graphiti_core" in sys.modules:
        return

    class RateLimitError(Exception):
        pass

    gc = types.ModuleType("graphiti_core")
    gc.Graphiti = type("Graphiti", (), {})
    nodes = types.ModuleType("graphiti_core.nodes")
    nodes.EpisodeType = type("EpisodeType", (), {"json": "json", "message": "message", "text": "text"})
    driver = types.ModuleType("graphiti_core.driver")
    kuzu = types.ModuleType("graphiti_core.driver.kuzu_driver")
    kuzu.KuzuDriver = type("KuzuDriver", (), {})
    llm = types.ModuleType("graphiti_core.llm_client")
    llm.RateLimitError = RateLimitError
    llm.LLMConfig = type("LLMConfig", (), {})
    llm.OpenAIClient = type("OpenAIClient", (), {})
    oc = types.ModuleType("graphiti_core.llm_client.openai_client")
    oc.OpenAIClient = llm.OpenAIClient
    cfg = types.ModuleType("graphiti_core.llm_client.config")
    cfg.LLMConfig = llm.LLMConfig
    emb = types.ModuleType("graphiti_core.embedder")
    embo = types.ModuleType("graphiti_core.embedder.openai")
    embo.OpenAIEmbedder = type("OpenAIEmbedder", (), {})
    embo.OpenAIEmbedderConfig = type("OpenAIEmbedderConfig", (), {})
    for name, mod in {
        "graphiti_core": gc,
        "graphiti_core.nodes": nodes,
        "graphiti_core.driver": driver,
        "graphiti_core.driver.kuzu_driver": kuzu,
        "graphiti_core.llm_client": llm,
        "graphiti_core.llm_client.openai_client": oc,
        "graphiti_core.llm_client.config": cfg,
        "graphiti_core.embedder": emb,
        "graphiti_core.embedder.openai": embo,
    }.items():
        sys.modules.setdefault(name, mod)


_stub_graphiti_core()
sys.modules.setdefault("numpy", types.ModuleType("numpy"))

from plugins.temporal_knowledge import desk_graphiti_service as svc  # noqa: E402


class StartupNameTests(unittest.TestCase):
    """The exact #26 regression: every name lifespan references must exist at
    module scope, or production dies at boot while imports compile clean."""

    def test_lifespan_referenced_names_exist(self):
        for name in ("OpenAIClient", "LLMConfig", "OpenAIEmbedder",
                     "OpenAIEmbedderConfig", "KuzuDriver", "Graphiti",
                     "RateLimitError"):
            self.assertTrue(
                hasattr(svc, name),
                f"desk_graphiti_service.{name} missing: the startup NameError class",
            )


class _RecordingClient:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


class _FakeGraphiti:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.built = False
        self.closed = False
        self.driver = kwargs.get("graph_driver")

    async def build_indices_and_constraints(self):
        self.built = True

    async def close(self):
        self.closed = True


class LifespanBootTests(unittest.TestCase):
    def test_lifespan_boots_headlessly(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        saved_state = dict(svc.STATE)
        self.addCleanup(svc.STATE.clear)
        self.addCleanup(svc.STATE.update, saved_state)

        async def noop(*_args, **_kwargs):
            return None

        async def boot():
            with mock.patch.object(svc, "_resolve_api_key", return_value="test-key"), \
                    mock.patch.object(svc, "OpenAIClient", _RecordingClient), \
                    mock.patch.object(svc, "LLMConfig", _RecordingClient), \
                    mock.patch.object(svc, "OpenAIEmbedder", _RecordingClient), \
                    mock.patch.object(svc, "OpenAIEmbedderConfig", _RecordingClient), \
                    mock.patch.object(svc, "KuzuDriver", _RecordingClient), \
                    mock.patch.object(svc, "Graphiti", _FakeGraphiti), \
                    mock.patch.object(svc, "_ensure_fts_indexes", noop), \
                    mock.patch.object(svc.ml, "ensure_salience_schema", noop), \
                    mock.patch.dict(os.environ, {"GRAPHITI_DB": str(Path(tmp.name) / "graph.db")}):
                async with svc.lifespan(svc.app):
                    g = svc.STATE["graphiti"]
                    self.assertIsInstance(g, _FakeGraphiti)
                    self.assertIsInstance(svc.STATE["queue"], asyncio.Queue)
                    self.assertEqual(len(svc.STATE["workers"]), 3)
                    return g

        g = asyncio.run(boot())
        self.assertTrue(g.closed, "shutdown closes the graphiti client")


if __name__ == "__main__":
    unittest.main()
