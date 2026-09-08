"""Portable benchmark inputs must not depend on or overwrite operator state."""

import importlib.util
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest


BENCH_PATH = Path(__file__).resolve().parents[2] / "scripts/latency_bench/bench_trivial_turn.py"
spec = importlib.util.spec_from_file_location("latency_bench", BENCH_PATH)
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


def test_synthetic_history_is_repeatable_and_alternates():
    history = bench._synthetic_history(8)
    assert [m["role"] for m in history] == ["user", "assistant"] * 8
    assert bench._fixture_record(history) == bench._fixture_record(bench._synthetic_history(8))
    assert bench._fixture_record(history) != bench._fixture_record(bench._synthetic_history(9))
    assert bench._synthetic_history(0) == []


def test_seed_uses_copies_and_keeps_session_identity():
    history = bench._synthetic_history(2)
    appended = []

    class Store:
        def append_to_transcript(self, session, message):
            appended.append((session, dict(message)))
            message["content"] = "store normalization"

    assert bench._seed_history(Store(), "synthetic-session", history) == len(history)
    assert appended == [("synthetic-session", m) for m in history]
    assert history == bench._synthetic_history(2)


@pytest.mark.parametrize("symlink", [False, True])
def test_existing_home_is_refused_without_modification(tmp_path, symlink):
    target = tmp_path / "existing"
    target.mkdir()
    sentinel = target / "keep.txt"
    sentinel.write_text("preserve")
    requested = target
    if symlink:
        requested = tmp_path / "alias"
        requested.symlink_to(target, target_is_directory=True)
    args = bench._parse_args(["--home", str(requested)])
    with pytest.raises(FileExistsError):
        bench._build_home(args)
    assert sentinel.read_text() == "preserve"
    assert list(target.iterdir()) == [sentinel]


def test_dangling_home_symlink_is_refused(tmp_path):
    target = tmp_path / "absent"
    alias = tmp_path / "alias"
    alias.symlink_to(target, target_is_directory=True)
    with pytest.raises(FileExistsError):
        bench._build_home(bench._parse_args(["--home", str(alias)]))
    assert not target.exists()


@pytest.mark.parametrize("option", ["--profile", "--live-db", "--history-session", "--clean"])
def test_legacy_operator_state_options_are_rejected(option):
    with pytest.raises(SystemExit) as error:
        bench._parse_args([option])
    assert error.value.code == 2


@pytest.mark.parametrize("argv", [
    ["--history-pairs", "-1"], ["--history-pairs", "10001"],
    ["--turns", "0"], ["--warmup", "-1"], ["--mock-port", "0"],
    ["--ttft-ms", "nan"], ["--tps", "inf"], ["--stall-turn", "6"],
])
def test_invalid_inputs_are_rejected(argv):
    with pytest.raises(SystemExit) as error:
        bench._parse_args(argv)
    assert error.value.code == 2


def test_prepare_does_not_start_runtime_or_change_environment(tmp_path, monkeypatch, capsys):
    import os

    scratch = tmp_path / "prepared"
    monkeypatch.setattr("sys.argv", [str(BENCH_PATH), "--prepare-only", "--home", str(scratch)])
    monkeypatch.setenv("HERMES_FAST_PATH", "1")
    before = dict(os.environ)

    def no_runtime(*args, **kwargs):
        pytest.fail("prepare-only must not start the mock or gateway")

    monkeypatch.setattr(bench, "_start_mock", no_runtime)
    monkeypatch.setattr(bench, "_run", no_runtime)
    bench.main()
    manifest = json.loads(capsys.readouterr().out)
    assert manifest["status"] == "prepared_only"
    assert manifest["args"]["fast_path"] == "0"
    assert manifest["fixture"] == bench._fixture_record(json.loads((scratch / "history.json").read_text()))
    assert manifest["source"]["installed_gateway_identity"] == "not verified"
    assert manifest == json.loads((scratch / "manifest.json").read_text())
    assert not (scratch / "result.json").exists()
    assert not (scratch / ".env").exists()
    assert dict(os.environ) == before


@pytest.mark.parametrize("existing", [False, True])
def test_result_retained_when_export_fails(tmp_path, existing):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    destination = tmp_path / "existing" if existing else tmp_path / "absent" / "result.json"
    if existing:
        destination.write_text("preserve")
    report = {"totals": {"n": 1, "wall_median_s": 0.1}}
    with pytest.raises(OSError, match="Result retained"):
        bench._write_result(scratch, str(destination), report)
    assert json.loads((scratch / "result.json").read_text()) == report
    if existing:
        assert destination.read_text() == "preserve"


def test_result_export_matches_retained_measurements(tmp_path):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    destination = tmp_path / "export.json"
    report = {"totals": {"n": 1}}
    primary = bench._write_result(scratch, str(destination), report)
    assert json.loads(primary.read_text()) == report
    assert primary.read_bytes() == destination.read_bytes()


@pytest.fixture
def unused_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


@pytest.fixture
def stub_child(tmp_path, monkeypatch):
    """Real child/socket/pipe lifecycle, with a stdlib server replacing aiohttp."""
    stub = tmp_path / "stub_child.py"
    template = '''
import json, os, socket, sys, time
from pathlib import Path
mode = MODE
listener = socket.socket(fileno=int(sys.argv[2]))
ready_fd = int(sys.argv[3])
Path("child-audit.json").write_text(json.dumps({
    "isolated": sys.flags.isolated,
    "no_bytecode": sys.dont_write_bytecode,
    "inherited_credential": "EXAMPLE_API_KEY" in os.environ,
    "inherited_proxy": "HTTP_PROXY" in os.environ,
    "cwd": os.getcwd(),
}))
if mode == "exit":
    sys.exit(7)
if mode == "eof":
    os.close(ready_fd)
    time.sleep(10)
if mode == "silent":
    time.sleep(10)
listener.listen()
message = {"pid": os.getpid(), "host": "127.0.0.1", "port": listener.getsockname()[1]}
if mode == "wrong-pid":
    message["pid"] += 1
if mode == "wrong-port":
    message["port"] += 1
data = (json.dumps(message) + "\\n").encode()
if mode == "invalid":
    data = b"invalid JSON\\n"
if mode == "oversized":
    data = b"x" * 4097
if mode == "split":
    os.write(ready_fd, data[:8])
    time.sleep(0.03)
    os.write(ready_fd, data[8:])
else:
    os.write(ready_fd, data)
os.close(ready_fd)
while True:
    client, _ = listener.accept()
    with client:
        client.recv(4096)
        client.sendall(b"HTTP/1.1 200 OK\\r\\nContent-Length: 10\\r\\nConnection: close\\r\\n\\r\\nstub-owned")
'''
    real_popen = subprocess.Popen
    launched = []

    def install(mode):
        stub.write_text(template.replace("MODE", repr(mode)), encoding="utf-8")

        def spawn(command, **kwargs):
            assert command[1:3] == ["-I", "-B"] and command[4] == "--mock-child"
            child = real_popen([*command[:3], str(stub), *command[4:]], **kwargs)
            launched.append(child)
            return child

        monkeypatch.setattr(bench.subprocess, "Popen", spawn)
        monkeypatch.setattr(bench, "_MOCK_STARTUP_TIMEOUT", 0.5)
        return launched

    yield install
    for child in launched:
        bench._stop_mock(child)


def test_occupied_port_is_rejected_without_any_http_or_child(tmp_path, monkeypatch):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"object":"list","data":[]}')

        do_POST = do_GET

        def log_message(self, *args):
            pass

    def no_child(*args, **kwargs):
        pytest.fail("An occupied port must be rejected before spawning a child")

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
    thread.start()
    try:
        monkeypatch.setattr(bench.subprocess, "Popen", no_child)
        args = bench._parse_args(["--mock-port", str(server.server_port)])
        with pytest.raises(OSError):
            bench._start_mock(args, tmp_path)
        assert requests == []
        assert not (tmp_path / "mock-startup.stderr").exists()
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


@pytest.mark.parametrize("mode", ["ready", "split"])
def test_ready_child_owns_reserved_listener(tmp_path, monkeypatch, unused_port, stub_child, mode):
    children = stub_child(mode)
    monkeypatch.setenv("EXAMPLE_API_KEY", "synthetic-do-not-inherit")
    monkeypatch.setenv("HTTP_PROXY", "http://invalid.example")
    proc = bench._start_mock(bench._parse_args(["--mock-port", str(unused_port)]), tmp_path)
    assert proc is children[0] and proc.poll() is None
    audit = json.loads((tmp_path / "child-audit.json").read_text())
    assert audit == {"isolated": 1, "no_bytecode": True, "inherited_credential": False,
                     "inherited_proxy": False, "cwd": str(tmp_path)}
    with socket.create_connection(("127.0.0.1", unused_port), timeout=1) as client:
        client.sendall(b"GET /stub-only HTTP/1.1\r\nHost: localhost\r\n\r\n")
        response = b""
        while chunk := client.recv(4096):
            response += chunk
        assert b"stub-owned" in response
    # A dead child still cannot hand the reserved port to another responder.
    proc.terminate()
    proc.wait(timeout=2)
    with socket.socket() as competing:
        with pytest.raises(OSError):
            competing.bind(("127.0.0.1", unused_port))
    bench._stop_mock(proc)
    assert proc._mock_listener.fileno() == -1
    # The same fixed port must be reusable immediately after accepted traffic.
    next_home = tmp_path / "next-run"
    next_home.mkdir()
    restarted = bench._start_mock(bench._parse_args(["--mock-port", str(unused_port)]), next_home)
    assert restarted.poll() is None


@pytest.mark.parametrize("mode,reason", [
    ("exit", "exited|closed"), ("eof", "closed"), ("silent", "timed out"),
    ("wrong-pid", "identity"), ("wrong-port", "identity"),
    ("invalid", "invalid"), ("oversized", "oversized|closed"),
])
def test_failed_startup_reaps_child_and_releases_port(tmp_path, unused_port, stub_child, mode, reason):
    children = stub_child(mode)
    with pytest.raises(RuntimeError, match=reason):
        bench._start_mock(bench._parse_args(["--mock-port", str(unused_port)]), tmp_path)
    assert len(children) == 1 and children[0].poll() is not None
    assert children[0]._mock_listener.fileno() == -1
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", unused_port))


def test_spawn_failure_closes_inherited_descriptors(tmp_path, monkeypatch, unused_port):
    descriptors = []

    def fail_spawn(*args, **kwargs):
        descriptors.extend(kwargs["pass_fds"])
        raise OSError("synthetic spawn failure")

    monkeypatch.setattr(bench.subprocess, "Popen", fail_spawn)
    with pytest.raises(OSError, match="synthetic spawn"):
        bench._start_mock(bench._parse_args(["--mock-port", str(unused_port)]), tmp_path)
    for fd in descriptors:
        with pytest.raises(OSError):
            os.fstat(fd)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", unused_port))


@pytest.mark.parametrize("fail_start", [False, True])
def test_child_signals_only_after_site_start_and_cleans_up(monkeypatch, fail_start):
    import asyncio
    from types import SimpleNamespace

    events = []
    ready_read, ready_write = os.pipe()
    listener = SimpleNamespace(getsockname=lambda: ("127.0.0.1", 18642))

    class Runner:
        def __init__(self, app):
            assert app == "stub-app"

        async def setup(self):
            events.append("setup")

        async def cleanup(self):
            events.append("cleanup")

    class Site:
        def __init__(self, runner, inherited):
            assert inherited is listener

        async def start(self):
            events.append("start")
            if fail_start:
                raise OSError("synthetic listen failure")

    monkeypatch.setitem(sys.modules, "aiohttp", SimpleNamespace(web=SimpleNamespace(AppRunner=Runner, SockSite=Site)))

    async def exercise():
        task = asyncio.create_task(bench._serve_mock_app("stub-app", listener, ready_write))
        await asyncio.sleep(0)
        if fail_start:
            with pytest.raises(OSError, match="synthetic listen"):
                await task
            assert os.read(ready_read, 4096) == b""
        else:
            assert events == ["setup", "start"]
            assert json.loads(os.read(ready_read, 4096)) == {
                "pid": os.getpid(), "host": "127.0.0.1", "port": listener.getsockname()[1],
            }
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert events == ["setup", "start", "cleanup"]

    try:
        asyncio.run(exercise())
    finally:
        os.close(ready_read)


@pytest.mark.parametrize("mode,reason", [
    ("ready", None), ("split", None), ("exit", "exited|closed"),
    ("eof", "closed"), ("silent", "timed out"),
    ("wrong-pid", "identity"), ("wrong-port", "identity"),
    ("invalid", "invalid"), ("oversized", "oversized|closed"),
])
def test_readiness_pipe_protocol_without_network(tmp_path, monkeypatch, mode, reason):
    """Exercise real child framing and timeout even where TCP bind is denied."""
    code = '''
import json, os, sys, time
fd, mode = int(sys.argv[1]), sys.argv[2]
if mode == "exit":
    sys.exit(7)
if mode == "eof":
    os.close(fd)
    time.sleep(10)
if mode == "silent":
    time.sleep(10)
message = {"pid": os.getpid(), "host": "127.0.0.1", "port": 18642}
if mode == "wrong-pid":
    message["pid"] += 1
if mode == "wrong-port":
    message["port"] += 1
data = (json.dumps(message) + "\\n").encode()
if mode == "invalid":
    data = b"invalid JSON\\n"
if mode == "oversized":
    data = b"x" * 4097
if mode == "split":
    os.write(fd, data[:8])
    time.sleep(0.03)
    os.write(fd, data[8:])
else:
    os.write(fd, data)
os.close(fd)
time.sleep(10)
'''
    def no_http(*args, **kwargs):
        pytest.fail("Readiness must not make HTTP requests")

    monkeypatch.setattr(bench.urllib.request, "urlopen", no_http)
    ready_read, ready_write = os.pipe()
    proc = subprocess.Popen(
        [sys.executable, "-I", "-B", "-c", code, str(ready_write), mode],
        pass_fds=(ready_write,), cwd=tmp_path, env={"PATH": os.defpath},
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    os.close(ready_write)
    try:
        if reason is None:
            bench._wait_mock_ready(proc, ready_read, 18642, 0.5)
            assert proc.poll() is None
        else:
            with pytest.raises(RuntimeError, match=reason):
                bench._wait_mock_ready(proc, ready_read, 18642, 0.5)
    finally:
        os.close(ready_read)
        bench._stop_mock(proc)
    assert proc.poll() is not None
