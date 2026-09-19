#!/usr/bin/env python3
"""Unit tests for the temporal-knowledge ingest resilience fix
(MEMORY-CHECKPOINT-FIX, branch fix/memory-extract-then-checkpoint).

graphiti_core is not a fork test dependency (the desk service runs inside the
mini venv), so it is stubbed in sys.modules before the service module loads.
Everything here runs offline: fake Graphiti clients, temp DLQ/checkpoint
files, in-memory queues.
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
import tempfile
import time
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
    # The real package re-exports all three from the root (the service imports
    # them as `from graphiti_core.llm_client import LLMConfig, OpenAIClient,
    # RateLimitError`); the stub mirrors that surface.
    llm.LLMConfig = type("LLMConfig", (), {})
    llm.OpenAIClient = type("OpenAIClient", (), {})
    oc = types.ModuleType("graphiti_core.llm_client.openai_client")
    oc.OpenAIClient = type("OpenAIClient", (), {})
    cfg = types.ModuleType("graphiti_core.llm_client.config")
    cfg.LLMConfig = type("LLMConfig", (), {})
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
# memory_layers imports numpy at module load; it is not a fork test dependency
# and none of these tests exercise its vector math.
sys.modules.setdefault("numpy", types.ModuleType("numpy"))

from plugins.temporal_knowledge import desk_graphiti_service as svc  # noqa: E402
from plugins.temporal_knowledge import ingest_events as ie  # noqa: E402

JOB = {"group_id": "desk", "text": "gpu price moved", "name": "event-1",
       "source": "json", "source_description": "telegram:chan",
       "reference_time": "2026-09-19T00:00:00Z"}


class _FakeGraphiti:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []
        self.driver = None

    async def add_episode(self, **kwargs):
        self.calls.append(kwargs)
        outcome = self.outcomes.pop(0) if self.outcomes else "ok"
        if outcome == "429":
            raise svc.RateLimitError("rate limited")
        if outcome == "boom":
            raise RuntimeError("boom")
        return types.SimpleNamespace(edges=[])


class ResilienceCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.dlq = root / "dlq.jsonl"
        self.checkpoint = root / "checkpoints.json"
        self._saved_state = dict(svc.STATE)
        svc.STATE.update({"graphiti": None, "queue": None, "workers": [], "ingested": 0,
                          "failed": 0, "llm": None, "dedup_merged": 0, "evolution": {},
                          "communities": 0, "dead_lettered": 0, "backend": "neo4j"})
        self.addCleanup(svc.STATE.clear)
        self.addCleanup(svc.STATE.update, self._saved_state)
        self.sleeps = []

        async def fake_sleep(seconds):
            self.sleeps.append(seconds)

        patchers = [
            mock.patch.object(svc, "_DLQ_PATH", self.dlq),
            mock.patch.object(svc, "_CHECKPOINT_FILE", self.checkpoint),
            mock.patch.object(svc, "RETRY_SLEEP", fake_sleep),
            mock.patch.object(svc, "_write_freshness_marker", lambda *a, **k: None),
        ]
        for patcher in patchers:
            patcher.start()
            self.addCleanup(patcher.stop)

    def run_job(self, fake, job=None):
        async def go():
            svc.STATE["queue"] = asyncio.Queue()
            svc.STATE["graphiti"] = fake
            worker = asyncio.create_task(svc._ingest_worker(fake, svc.STATE["queue"]))
            try:
                return await svc._queue_confirmed(dict(job or JOB))
            finally:
                worker.cancel()
                try:
                    await worker
                except asyncio.CancelledError:
                    pass

        return asyncio.run(go())

    def test_rate_limit_retries_then_succeeds_within_budget(self):
        fake = _FakeGraphiti(["429", "429", "ok"])
        self.assertIs(self.run_job(fake), True)
        self.assertEqual(len(fake.calls), 3, "bounded inline retries, never skip")
        self.assertTrue(all(0 < s < 15 for s in self.sleeps), "jittered backoff")
        self.assertFalse(self.dlq.exists(), "a recovered 429 never dead-letters")
        self.assertEqual(svc.STATE["ingested"], 1)

    def test_rate_limit_exhaustion_dead_letters_without_losing_event(self):
        fake = _FakeGraphiti(["429"] * svc.RATE_LIMIT_ATTEMPTS)
        self.assertEqual(self.run_job(fake), "dead")
        self.assertEqual(len(fake.calls), svc.RATE_LIMIT_ATTEMPTS)
        entries = [json.loads(line) for line in self.dlq.read_text().splitlines()]
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry["job"]["name"], JOB["name"])
        self.assertNotIn("_completion", entry["job"])
        self.assertEqual(entry["attempts"], svc.RATE_LIMIT_ATTEMPTS)
        self.assertGreater(entry["next_retry_at"], int(time.time()))
        self.assertIn("rate_limit_exhausted", entry["error"])
        self.assertEqual(svc.STATE["dead_lettered"], 1)

    def test_dlq_replay_recovers_the_event_zero_loss(self):
        fake = _FakeGraphiti(["429"] * svc.RATE_LIMIT_ATTEMPTS)
        self.assertEqual(self.run_job(fake), "dead")
        self.assertEqual(svc.STATE["ingested"], 0)
        fake.outcomes = ["ok"]

        async def go():
            svc.STATE["queue"] = asyncio.Queue()
            svc.STATE["graphiti"] = fake
            worker = asyncio.create_task(svc._ingest_worker(fake, svc.STATE["queue"]))
            try:
                return await svc._dlq_replay(fake, now=time.time() + 10000)
            finally:
                worker.cancel()
                try:
                    await worker
                except asyncio.CancelledError:
                    pass

        report = asyncio.run(go())
        self.assertEqual(report, {"replayed": 1, "remaining": 0})
        self.assertEqual(svc.STATE["ingested"], 1, "the dead-lettered event is never lost")
        survivors = [line for line in self.dlq.read_text().splitlines() if line.strip()]
        self.assertEqual(survivors, [])

    def test_unknown_error_holds_cursor_without_dead_letter(self):
        fake = _FakeGraphiti(["boom"])
        self.assertIs(self.run_job(fake), False)
        self.assertFalse(self.dlq.exists())
        self.assertEqual(svc.STATE["ingested"], 0)

    def _events_db(self, rows):
        db = Path(self.tmp.name) / "ito.db"
        with sqlite3.connect(db) as con:
            con.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, source TEXT, "
                        "channel TEXT, channel_id TEXT, sender TEXT, ts INTEGER, text TEXT)")
            for row in rows:
                con.execute("INSERT INTO events VALUES (?,?,?,?,?,?,?)", row)
        return db

    def _ingest_endpoint(self, fake, db, checkpoint_key="events:all:telegram,slack"):
        async def go():
            svc.STATE["queue"] = asyncio.Queue()
            svc.STATE["graphiti"] = fake
            worker = asyncio.create_task(svc._ingest_worker(fake, svc.STATE["queue"]))
            try:
                return await svc.ingest_events(group_id="desk", db_path=str(db),
                                               sources="telegram,slack", limit=100,
                                               channel="", flush=True)
            finally:
                worker.cancel()
                try:
                    await worker
                except asyncio.CancelledError:
                    pass

        return asyncio.run(go())

    def test_checkpoint_advances_only_after_successful_extraction(self):
        rows = [(i, "telegram", "chan", "1", "buyer", 1789600000 + i, "gpu price question")
                for i in (101, 102, 103)]
        db = self._events_db(rows)
        fake = _FakeGraphiti(["ok", "boom", "ok"])
        report = self._ingest_endpoint(fake, db)
        self.assertEqual(report["failed"], 1)
        self.assertEqual(report["last_acknowledged_event_id"], 101,
                         "a failed extraction holds the cursor; the event replays")
        fake.outcomes = ["ok", "ok"]
        report = self._ingest_endpoint(fake, db)
        self.assertEqual(report["last_acknowledged_event_id"], 103)
        self.assertEqual(report["failed"], 0)
        ingested_names = [c["name"] for c in fake.calls]
        self.assertEqual(ingested_names.count("event-102"), 2, "the failed event replayed once")
        self.assertEqual(svc.STATE["ingested"], 3, "zero loss across the retry")

    def test_dead_lettered_event_advances_with_note_and_replays_from_file(self):
        rows = [(i, "telegram", "chan", "1", "buyer", 1789600000 + i, "gpu price question")
                for i in (201, 202, 203)]
        db = self._events_db(rows)
        fake = _FakeGraphiti(["ok", *["429"] * svc.RATE_LIMIT_ATTEMPTS, "ok"])
        report = self._ingest_endpoint(fake, db)
        self.assertEqual(report["dead_lettered"], 1)
        self.assertEqual(report["failed"], 0)
        self.assertEqual(report["last_acknowledged_event_id"], 203,
                         "the batch never stalls behind one poison event")
        entries = [json.loads(line) for line in self.dlq.read_text().splitlines()]
        self.assertEqual([e["job"]["name"] for e in entries], ["event-202"])


class IngestScriptCheckpointTests(unittest.TestCase):
    """plugins/temporal_knowledge/ingest_events.py: a failed POST must hold the
    file checkpoint so the event replays instead of being skipped forever."""

    def test_failed_post_holds_checkpoint_at_last_success(self):
        rows = [
            {"id": 11, "text": "gpu price one", "source": "telegram",
             "channel": "c", "conversation_type": "ext", "sender": "b", "ts": 1789600001},
            {"id": 12, "text": "gpu price two", "source": "telegram",
             "channel": "c", "conversation_type": "ext", "sender": "b", "ts": 1789600002},
            {"id": 13, "text": "gpu price three", "source": "telegram",
             "channel": "c", "conversation_type": "ext", "sender": "b", "ts": 1789600003},
        ]
        saved = []
        real_requests = ie.requests

        def post_episode(row):
            if row["id"] == 12:
                raise real_requests.RequestException("service down")

        conn = sqlite3.connect(":memory:")
        with mock.patch.object(ie, "load_checkpoint", return_value={"last_id": 10}), \
                mock.patch.object(ie.sqlite3, "connect", return_value=conn), \
                mock.patch.object(ie, "fetch_events", return_value=rows), \
                mock.patch.object(ie, "post_episode", side_effect=post_episode), \
                mock.patch.object(ie, "save_checkpoint", side_effect=saved.append), \
                mock.patch.object(ie, "requests") as requests_mock:
            requests_mock.RequestException = real_requests.RequestException
            requests_mock.get.side_effect = real_requests.RequestException("no health")
            with mock.patch.object(sys, "argv", ["ingest_events.py", "--once"]):
                code = ie.main()
        conn.close()
        self.assertEqual(code, 1)
        self.assertEqual(saved, [11], "checkpoint covers only durably posted rows;"
                                     " events 12 and 13 replay next run")


if __name__ == "__main__":
    unittest.main()
