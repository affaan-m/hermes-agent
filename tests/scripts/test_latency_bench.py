"""Portable benchmark inputs must not depend on or overwrite operator state."""

import importlib.util
import json
from pathlib import Path

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
