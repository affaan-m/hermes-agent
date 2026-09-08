#!/usr/bin/env python3
"""In-process latency benchmark for one trivial gateway turn.

Builds a throwaway HERMES_HOME, starts the mock model server, constructs a
real ``GatewayRunner`` with a fake Telegram adapter, seeds deterministic
synthetic conversation history, then pushes N
trivial messages through ``GatewayRunner._handle_message`` and reports the
per-stage latency breakdown collected by ``agent.latency_trace``.

No profile or database imports are supported. Scratch homes must be new and
are retained for inspection. ``--prepare-only`` uses only the standard library
and does not import Hermes, start a server, or change runtime environment.

Example:
  python scripts/latency_bench/bench_trivial_turn.py \
      --prepare-only --history-pairs 8
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
import runpy
import select
import socket
import statistics
import subprocess
import sys
import tempfile
import time
import tomllib
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
_MOCK_STARTUP_TIMEOUT = 15.0


def _parse_args(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--home", default="", help="new scratch directory; default: unique temporary directory")
    ap.add_argument("--history-pairs", type=int, default=8, help="synthetic user/assistant pairs (0 to 10000)")
    ap.add_argument("--prepare-only", action="store_true", help="write synthetic inputs and source manifest without importing Hermes or starting the mock")
    ap.add_argument("--turns", type=int, default=5)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--message", default="ok thanks")
    ap.add_argument("--mock-port", type=int, default=18642)
    ap.add_argument("--ttft-ms", type=float, default=0.0)
    ap.add_argument("--tps", type=float, default=0.0)
    ap.add_argument("--toolsets", default="hermes-cli")
    ap.add_argument("--label", default="baseline")
    ap.add_argument("--out", default="")
    ap.add_argument("--fast-path", choices=("0", "1"), default="0", help="prototype switch in this benchmark process only (default: 0)")
    ap.add_argument("--stall-turn", type=int, default=-1, help="arm the mock to stall the first model request of this turn index")
    ap.add_argument("--api-mode", default="", help="force the agent wire protocol, e.g. codex_responses (the live Ito path); default is what the config resolves")
    args = ap.parse_args(argv)
    if not 0 <= args.history_pairs <= 10000:
        ap.error("--history-pairs must be between 0 and 10000")
    if args.turns < 1 or args.warmup < 0:
        ap.error("--turns must be positive and --warmup nonnegative")
    if not 1 <= args.mock_port <= 65535:
        ap.error("--mock-port must be between 1 and 65535")
    if any(not math.isfinite(v) or v < 0 for v in (args.ttft_ms, args.tps)):
        ap.error("--ttft-ms and --tps must be finite and nonnegative")
    if args.stall_turn < -1 or args.stall_turn >= args.turns + args.warmup:
        ap.error("--stall-turn must be -1 or a valid turn index including warmup")
    return args


def _synthetic_history(pairs: int) -> list:
    messages = []
    for i in range(pairs):
        messages.extend([
            {"role": "user", "content": f"Synthetic benchmark note {i}: the sample job is pending."},
            {"role": "assistant", "content": f"Recorded synthetic note {i}. No action taken."},
        ])
    return messages


def _fixture_record(messages: list) -> dict:
    data = json.dumps(messages, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {"kind": "synthetic-v1", "messages": len(messages), "bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest()}


def _source_provenance() -> dict:
    def git(*args):
        return subprocess.check_output(["git", "-C", str(REPO), *args], text=True).strip()

    files = (
        "pyproject.toml", "run_agent.py", "gateway/run.py", "agent/fast_path.py",
        "agent/latency_trace.py", "agent/chat_completion_helpers.py",
        "agent/conversation_loop.py", "agent/turn_context.py", "agent/turn_finalizer.py",
        "scripts/latency_bench/bench_trivial_turn.py", "scripts/latency_bench/mock_openai_server.py",
    )
    return {
        "repo": str(REPO), "head": git("rev-parse", "HEAD"),
        "dirty": bool(git("status", "--porcelain", "--untracked-files=normal")),
        "checkout_version": tomllib.loads((REPO / "pyproject.toml").read_text())["project"]["version"],
        "python": sys.version.split()[0], "executable": sys.executable,
        "file_sha256": {name: hashlib.sha256((REPO / name).read_bytes()).hexdigest() for name in files},
        "installed_gateway_identity": "not verified",
    }


def _build_home(args) -> Path:
    if args.home:
        home = Path(args.home).expanduser().absolute()
        # mkdir is exclusive, including when the requested path is a symlink.
        # Never reuse or recursively delete a directory supplied by the caller.
        home.mkdir(mode=0o700, exist_ok=False)
    else:
        home = Path(tempfile.mkdtemp(prefix="hermes-latency-bench-"))
    home = home.resolve()
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
    # JSON is valid YAML and keeps preparation independent of gateway deps.
    (home / "config.yaml").write_text(json.dumps(cfg, indent=2), encoding="utf-8")
    (home / "SOUL.md").write_text("You are a synthetic latency benchmark assistant.\n", encoding="utf-8")
    for d in ("sessions", "logs"):
        (home / d).mkdir(exist_ok=True)
    return home


def _wait_mock_ready(proc, ready_fd: int, port: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    data = b""
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"Mock child exited before readiness (code {proc.returncode})")
        readable, _, _ = select.select([ready_fd], [], [], min(0.05, max(0, deadline - time.monotonic())))
        if not readable:
            continue
        chunk = os.read(ready_fd, 4096)
        if not chunk:
            raise RuntimeError("Mock child closed its readiness pipe")
        data += chunk
        if len(data) > 4096:
            raise RuntimeError("Mock child sent an oversized readiness message")
        if b"\n" in data:
            try:
                message = json.loads(data)
            except (ValueError, UnicodeError) as exc:
                raise RuntimeError("Mock child sent invalid readiness JSON") from exc
            expected = {"pid": proc.pid, "host": "127.0.0.1", "port": port}
            if message != expected or proc.poll() is not None:
                raise RuntimeError("Mock child readiness identity mismatch or early exit")
            return
    raise RuntimeError("Mock child startup timed out")


def _stop_mock(proc) -> None:
    try:
        if proc.poll() is None:
            proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
    finally:
        listener = getattr(proc, "_mock_listener", None)
        if listener is not None:
            listener.close()


def _start_mock(args, home: Path) -> subprocess.Popen:
    if os.name != "posix":
        raise RuntimeError("Mock startup requires POSIX socket/pipe inheritance")
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ready_read = ready_write = None
    proc = None
    try:
        # Keep this descriptor open until child shutdown, including after ready.
        # SO_REUSEADDR permits sequential runs after TCP TIME_WAIT. Listen before
        # handoff so another reusable bind cannot become the serving endpoint.
        # No SO_REUSEPORT or bind/probe/close gap, and no request to existing services.
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", args.mock_port))
        listener.listen(socket.SOMAXCONN)
        ready_read, ready_write = os.pipe()
        with (home / "mock-startup.stderr").open("xb") as stderr:
            proc = subprocess.Popen(
                [sys.executable, "-I", "-B", str(Path(__file__).resolve()), "--mock-child",
                 str(listener.fileno()), str(ready_write),
                 "--port", str(args.mock_port), "--ttft-ms", str(args.ttft_ms), "--tps", str(args.tps),
                 "--log", str(home / "mock_requests.jsonl")],
                pass_fds=(listener.fileno(), ready_write), cwd=home,
                env={"PATH": os.defpath, "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
                     "HERMES_HOME": str(home)},
                stdout=subprocess.DEVNULL, stderr=stderr,
            )
        proc._mock_listener = listener
        os.close(ready_write)
        ready_write = None
        _wait_mock_ready(proc, ready_read, args.mock_port, _MOCK_STARTUP_TIMEOUT)
        return proc
    except BaseException:
        if proc is not None:
            _stop_mock(proc)
        else:
            listener.close()
        raise
    finally:
        for fd in (ready_read, ready_write):
            if fd is not None:
                os.close(fd)


async def _serve_mock_app(app, listener, ready_fd: int) -> None:
    from aiohttp import web

    runner = web.AppRunner(app)
    try:
        await runner.setup()
        await web.SockSite(runner, listener).start()
        # SockSite.start has registered the inherited listener with the event loop.
        message = {"pid": os.getpid(), "host": "127.0.0.1", "port": listener.getsockname()[1]}
        os.write(ready_fd, (json.dumps(message) + "\n").encode("utf-8"))
        os.close(ready_fd)
        ready_fd = None
        await asyncio.Event().wait()
    finally:
        if ready_fd is not None:
            os.close(ready_fd)
        await runner.cleanup()


def _mock_child(listener_fd: int, ready_fd: int, argv: list) -> None:
    from aiohttp import web

    listener = socket.socket(fileno=listener_fd)
    # Reuse the existing mock's routes/app construction without changing that file.
    # Replace only its CLI runner so it adopts our reserved socket and signals ready.
    def run_app(app, **_kwargs):
        return asyncio.run(_serve_mock_app(app, listener, ready_fd))

    web.run_app = run_app
    server = REPO / "scripts/latency_bench/mock_openai_server.py"
    sys.argv = [str(server), *argv]
    runpy.run_path(str(server), run_name="__main__")


def _seed_history(session_store, session_id: str, messages: list) -> int:
    for message in messages:
        session_store.append_to_transcript(session_id, dict(message))
    return len(messages)


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


async def _run(args, home: Path, history: list):
    os.environ["HERMES_HOME"] = str(home)
    os.environ["HERMES_LATENCY_TRACE_JSONL"] = str(home / "latency.jsonl")
    os.environ["OPENAI_API_KEY"] = "bench-local"
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
    imported = _seed_history(runner.session_store, entry.session_id, history)

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


def _write_result(home: Path, output: str, report: dict) -> Path:
    # Preserve completed measurements even if an optional export is rejected.
    primary = home / "result.json"
    data = json.dumps(report, indent=1, default=str)
    with primary.open("x", encoding="utf-8") as fh:
        fh.write(data)
    if output and Path(output).absolute() != primary.absolute():
        try:
            with Path(output).open("x", encoding="utf-8") as fh:
                fh.write(data)
        except OSError as exc:
            raise OSError(f"Result retained at {primary}; could not write --out") from exc
    return primary


def main():
    args = _parse_args()
    source = _source_provenance()
    home = _build_home(args)
    history = _synthetic_history(args.history_pairs)
    fixture = _fixture_record(history)
    (home / "history.json").write_text(json.dumps(history, indent=2), encoding="utf-8")
    manifest = {"status": "prepared_only", "source": source, "fixture": fixture,
                "args": vars(args), "scratch_home": str(home)}
    (home / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    if args.prepare_only:
        print(json.dumps(manifest, indent=2))
        return
    mock = _start_mock(args, home)
    try:
        results, imported, session_id = asyncio.run(_run(args, home, history))
    finally:
        _stop_mock(mock)
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
    source["imported_modules"] = {
        name: str(Path(module.__file__).resolve())
        for name in ("gateway.run", "run_agent", "agent.fast_path", "agent.chat_completion_helpers")
        if (module := sys.modules.get(name)) is not None and getattr(module, "__file__", None)
    }
    out = _write_result(home, args.out, {
        "label": args.label, "args": vars(args), "source": source,
        "fixture": fixture, "results": results, "stages": rows,
        "totals": totals, "mock_requests": mock_reqs,
    })
    print(f"\nwrote {out}")


if __name__ == "__main__":
    if len(sys.argv) >= 4 and sys.argv[1] == "--mock-child":
        _mock_child(int(sys.argv[2]), int(sys.argv[3]), sys.argv[4:])
    else:
        main()
