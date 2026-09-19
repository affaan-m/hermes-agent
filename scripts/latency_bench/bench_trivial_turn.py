#!/usr/bin/env python3
"""In-process latency benchmark for one trivial gateway turn.

Builds a throwaway HERMES_HOME, starts the mock model server, constructs a
real ``GatewayRunner`` with a fake Telegram adapter, optionally imports a real
session transcript from a live ``state.db`` (read-only), then pushes N
trivial messages through ``GatewayRunner._handle_message`` and reports the
per-stage latency breakdown collected by ``agent.latency_trace``.

Nothing here touches a live profile: the live DB is opened read-only and the
scratch home is deleted only when ``--clean`` is passed.

Example:
  python scripts/latency_bench/bench_trivial_turn.py \
      --home /tmp/hermes-bench --turns 5 \
      --profile ~/.hermes/profiles/ito \
      --live-db ~/.hermes/profiles/ito/state.db --history-session 20260811_150601_4623e079
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import sqlite3
import statistics
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--home", required=True, help="scratch HERMES_HOME (created)")
    ap.add_argument("--profile", default="", help="profile dir to copy SOUL.md/memories/skills from")
    ap.add_argument("--live-db", default="", help="live state.db to import history from (read-only)")
    ap.add_argument("--history-session", default="", help="session id in --live-db to import")
    ap.add_argument("--turns", type=int, default=5)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--message", default="ok thanks")
    ap.add_argument("--mock-port", type=int, default=18642)
    ap.add_argument("--ttft-ms", type=float, default=0.0)
    ap.add_argument("--tps", type=float, default=0.0)
    ap.add_argument("--toolsets", default="hermes-cli")
    ap.add_argument("--label", default="baseline")
    ap.add_argument("--out", default="")
    ap.add_argument("--clean", action="store_true")
    ap.add_argument("--fast-path", default="", help="value for HERMES_FAST_PATH env (prototype switch)")
    ap.add_argument("--stall-turn", type=int, default=-1, help="arm the mock to stall the first model request of this turn index")
    ap.add_argument("--api-mode", default="", help="force the agent wire protocol, e.g. codex_responses (the live Ito path); default is what the config resolves")
    return ap.parse_args()


def _build_home(args) -> Path:
    home = Path(args.home).expanduser().resolve()
    if home.exists() and args.clean:
        shutil.rmtree(home)
    home.mkdir(parents=True, exist_ok=True)
    cfg = {
        "model": {
            "provider": "custom",
            "base_url": f"http://127.0.0.1:{args.mock_port}/v1",
            "default": "bench-model",
            "api_key": "bench-local",
        },
        "toolsets": [t for t in args.toolsets.split(",") if t],
        "agent": {"max_turns": 200},
        "streaming": {"enabled": True},
        "display": {
            "platforms": {
                "telegram": {"show_reasoning": False, "tool_progress": "none", "interim_assistant_messages": False}
            }
        },
        "platforms": {"telegram": {"enabled": False}},
        "_config_version": 33,
    }
    import yaml
    (home / "config.yaml").write_text(yaml.safe_dump(cfg, sort_keys=False))
    (home / ".env").write_text("OPENAI_API_KEY=bench-local\n")
    if args.profile:
        prof = Path(args.profile).expanduser()
        for name in ("SOUL.md",):
            if (prof / name).exists():
                shutil.copy(prof / name, home / name)
        if (prof / "memories").exists():
            shutil.copytree(prof / "memories", home / "memories", dirs_exist_ok=True)
        if (prof / "skills").exists() and not (home / "skills").exists():
            os.symlink(prof / "skills", home / "skills")
    for d in ("sessions", "logs"):
        (home / d).mkdir(exist_ok=True)
    return home


def _start_mock(args, home: Path) -> subprocess.Popen:
    log = home / "mock_requests.jsonl"
    if log.exists():
        log.unlink()
    proc = subprocess.Popen(
        [sys.executable, str(REPO / "scripts/latency_bench/mock_openai_server.py"),
         "--port", str(args.mock_port), "--ttft-ms", str(args.ttft_ms), "--tps", str(args.tps),
         "--log", str(log)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{args.mock_port}/v1/models", timeout=1).read()
            return proc
        except Exception:
            time.sleep(0.1)
    proc.kill()
    raise SystemExit("mock server did not start")


def _import_history(session_store, session_id: str, live_db: str, live_session: str) -> int:
    con = sqlite3.connect(f"file:{live_db}?mode=ro", uri=True)
    rows = con.execute(
        "SELECT role, content, tool_call_id, tool_calls, tool_name, reasoning, timestamp "
        "FROM messages WHERE session_id=? AND active=1 ORDER BY id",
        (live_session,),
    ).fetchall()
    con.close()
    n = 0
    for role, content, tool_call_id, tool_calls, tool_name, reasoning, ts in rows:
        msg = {"role": role, "content": content}
        if tool_call_id:
            msg["tool_call_id"] = tool_call_id
        if tool_calls:
            try:
                msg["tool_calls"] = json.loads(tool_calls)
            except Exception:
                pass
        if tool_name:
            msg["name"] = tool_name
        session_store.append_to_transcript(session_id, msg)
        n += 1
    return n


class _Recorder:
    def __init__(self):
        self.sends = []
        self.edits = []

    def reset(self):
        self.sends.clear()
        self.edits.clear()


def _make_fake_adapter(recorder):
    from gateway.config import Platform, PlatformConfig
    from gateway.platforms.base import BasePlatformAdapter, SendResult

    class FakeTelegramAdapter(BasePlatformAdapter):
        def __init__(self):
            super().__init__(PlatformConfig(), Platform.TELEGRAM)
            self._n = 0

        async def connect(self, *, is_reconnect: bool = False) -> bool:
            return True

        async def disconnect(self) -> None:
            return None

        async def send(self, chat_id, content, reply_to=None, metadata=None) -> SendResult:
            self._n += 1
            recorder.sends.append((time.perf_counter(), len(content or "")))
            return SendResult(success=True, message_id=str(self._n))

        async def edit_message(self, chat_id, message_id, content, metadata=None, **kwargs):
            recorder.edits.append((time.perf_counter(), len(content or "")))
            return SendResult(success=True, message_id=str(message_id))

        async def get_chat_info(self, chat_id):
            return {"id": chat_id, "type": "group", "name": "bench"}

    return FakeTelegramAdapter()


async def _run(args, home: Path):
    os.environ["HERMES_HOME"] = str(home)
    os.environ["HERMES_LATENCY_TRACE_JSONL"] = str(home / "latency.jsonl")
    os.environ.setdefault("OPENAI_API_KEY", "bench-local")
    if args.fast_path:
        os.environ["HERMES_FAST_PATH"] = args.fast_path
    sys.path.insert(0, str(REPO))

    import logging
    logging.basicConfig(level=logging.INFO, filename=str(home / "logs" / "bench.log"), format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    from gateway.config import Platform, load_gateway_config
    from gateway.session import SessionSource
    from gateway.platforms.base import MessageEvent
    import gateway.run as gateway_run
    from gateway.run import GatewayRunner

    if args.api_mode:
        # Harness-only override: the live desk talks Responses API to
        # api.openai.com, which the runtime resolver only selects for real
        # OpenAI hosts. Force the same wire protocol against the mock.
        _orig_resolve = gateway_run._resolve_runtime_agent_kwargs

        def _resolve_with_mode(*a, **kw):
            kwargs = dict(_orig_resolve(*a, **kw))
            kwargs["api_mode"] = args.api_mode
            return kwargs

        gateway_run._resolve_runtime_agent_kwargs = _resolve_with_mode

    recorder = _Recorder()
    config = load_gateway_config()
    runner = GatewayRunner(config)
    runner._gateway_loop = asyncio.get_running_loop()
    runner.adapters = {Platform.TELEGRAM: _make_fake_adapter(recorder)}
    runner._is_user_authorized = lambda _source: True

    source = SessionSource(platform=Platform.TELEGRAM, chat_id="-100424242", chat_type="group", user_id="4242", user_name="bench")
    entry = runner.session_store.get_or_create_session(source)
    imported = 0
    if args.live_db and args.history_session:
        imported = _import_history(runner.session_store, entry.session_id, os.path.expanduser(args.live_db), args.history_session)

    trace_path = home / "latency.jsonl"
    if trace_path.exists():
        trace_path.unlink()

    results = []
    total_turns = args.warmup + args.turns
    for i in range(total_turns):
        recorder.reset()
        if i == args.stall_turn:
            req = urllib.request.Request(f"http://127.0.0.1:{args.mock_port}/__control", data=json.dumps({"stall_next": 1}).encode(), headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=5).read()
        event = MessageEvent(text=args.message, source=source, message_id=f"bench-{i}")
        t0 = time.perf_counter()
        await runner._handle_message(event)
        t1 = time.perf_counter()
        first_delivery = None
        if recorder.sends or recorder.edits:
            first_delivery = min([t for t, _ in recorder.sends] + [t for t, _ in recorder.edits]) - t0
        trace = None
        if trace_path.exists():
            lines = trace_path.read_text().strip().splitlines()
            if lines:
                trace = json.loads(lines[-1])
        results.append({
            "turn": i, "warmup": i < args.warmup, "wall_s": t1 - t0, "stalled": i == args.stall_turn,
            "first_delivery_s": first_delivery, "sends": len(recorder.sends), "edits": len(recorder.edits),
            "trace": trace,
        })
        print(f"turn {i}{' (warmup)' if i < args.warmup else ''}: wall={t1 - t0:.3f}s first_delivery={first_delivery if first_delivery is None else round(first_delivery, 3)} sends={len(recorder.sends)} edits={len(recorder.edits)}", flush=True)

    # Give background persistence a moment, then tear down.
    await asyncio.sleep(0.2)
    return results, imported, entry.session_id


def _stage_table(results):
    measured = [r for r in results if not r["warmup"] and r.get("trace")]
    if not measured:
        return [], {}
    order = []
    per_stage = {}
    for r in measured:
        prev = 0.0
        for m in r["trace"]["marks"]:
            label = m["label"]
            if label not in per_stage:
                per_stage[label] = []
                order.append(label)
            per_stage[label].append(m["t"] - prev)
            prev = m["t"]
    rows = []
    for label in order:
        v = per_stage[label]
        rows.append((label, statistics.median(v), max(v), len(v)))
    totals = {
        "wall_median_s": statistics.median(r["wall_s"] for r in measured),
        "trace_total_median_s": statistics.median(r["trace"]["total_s"] for r in measured),
        "first_delivery_median_s": statistics.median([r["first_delivery_s"] for r in measured if r["first_delivery_s"] is not None] or [0.0]),
        "n": len(measured),
    }
    return rows, totals


def main():
    args = _parse_args()
    home = _build_home(args)
    mock = _start_mock(args, home)
    try:
        results, imported, session_id = asyncio.run(_run(args, home))
    finally:
        mock.terminate()
    rows, totals = _stage_table(results)
    mock_reqs = []
    mlog = home / "mock_requests.jsonl"
    if mlog.exists():
        mock_reqs = [json.loads(l) for l in mlog.read_text().splitlines() if l.strip()]
    print(f"\n== {args.label}: history imported={imported} session={session_id} turns={totals.get('n')} ==")
    print(f"wall median={totals.get('wall_median_s', 0):.3f}s  trace total median={totals.get('trace_total_median_s', 0):.3f}s  first delivery median={totals.get('first_delivery_median_s', 0):.3f}s")
    if mock_reqs:
        last = mock_reqs[-1]
        print(f"model request shape (last): messages={last['messages']} tools={last['tools']} prompt_bytes={last['prompt_bytes']} approx_tokens={last['approx_tokens']} stream={last['stream']} path={last['path']} reasoning={last['reasoning']}")
    print("\n| stage | median ms | max ms |\n|---|---:|---:|")
    for label, med, mx, n in rows:
        print(f"| {label} | {med * 1000:.0f} | {mx * 1000:.0f} |")
    if args.out:
        Path(args.out).write_text(json.dumps({"label": args.label, "args": vars(args), "results": results, "stages": rows, "totals": totals, "mock_requests": mock_reqs}, indent=1, default=str))
        print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
