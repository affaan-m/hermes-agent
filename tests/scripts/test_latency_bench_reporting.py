"""Synthetic report integrity regressions; no Hermes or network imports."""

import importlib.util
import json
import os
from pathlib import Path

import pytest


DEFAULT_BENCH = Path(__file__).resolve().parents[2] / "scripts/latency_bench/bench_trivial_turn.py"
# Root may point at the recorded, local base source to reproduce the original bug.
BENCH_PATH = Path(os.environ.get("LATENCY_BENCH_SOURCE_UNDER_TEST", DEFAULT_BENCH))
spec = importlib.util.spec_from_file_location("latency_bench_reporting", BENCH_PATH)
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


def trace(key="bench:1", total=2.0, marks=None):
    return {"key": key, "total_s": total, "marks": marks or [
        {"label": "gw.inbound", "t": 0.0},
        {"label": "gw.response_ready", "t": total},
    ]}


def result(record, wall=2.0, delivery=1.0, warmup=False, **kwargs):
    return {"trace": record, "wall_s": wall, "first_delivery_s": delivery,
            "warmup": warmup, **kwargs}


def test_untraced_attempt_still_counts_in_wall_latency():
    _, totals = bench._stage_table([result(trace(), wall=2), result(None, wall=100)])
    assert totals["n"] == 2
    assert totals["wall_median_s"] == 51
    assert totals["traced_n"] == 1
    assert totals["valid_for_comparison"] is False


def test_missing_delivery_is_unavailable_not_zero():
    _, totals = bench._stage_table([result(trace(), delivery=None)])
    assert totals["first_delivery_median_s"] is None
    assert totals["delivered_n"] == 0
    assert totals["valid_for_comparison"] is False


def test_repeated_stage_durations_are_summed_per_turn():
    record = trace(total=8, marks=[
        {"label": "api.request_start", "t": 0},
        {"label": "api.request_end", "t": 3},
        {"label": "api.request_start", "t": 3},
        {"label": "api.request_end", "t": 8},
        {"label": "gw.response_ready", "t": 8},
    ])
    rows, totals = bench._stage_table([result(record, wall=8)])
    assert next(row for row in rows if row[0] == "api.request_end") == ("api.request_end", 8, 8, 1)
    assert totals["valid_for_comparison"] is True


def test_good_warmup_excluded_and_failed_warmup_invalidates_comparison():
    _, totals = bench._stage_table([result(trace(total=100), wall=100, warmup=True), result(trace())])
    assert totals["n"] == 1 and totals["wall_median_s"] == 2
    assert totals["valid_for_comparison"] is True
    _, totals = bench._stage_table([result(None, warmup=True), result(trace())])
    assert totals["warmup_invalid_n"] == 1
    assert totals["valid_for_comparison"] is False


def test_all_missing_traces_preserve_attempt_denominator():
    rows, totals = bench._stage_table([result(None, wall=20, delivery=None)])
    assert rows == []
    assert totals["n"] == 1 and totals["wall_median_s"] == 20
    assert totals["trace_total_median_s"] is None
    assert totals["valid_for_comparison"] is False


@pytest.mark.parametrize("record", [
    {"total_s": 2, "marks": []},
    trace(total=float("nan")), trace(total=float("inf")),
    trace(marks=[{"label": "gw.response_ready", "t": -1}]),
    trace(marks=[{"label": "a", "t": 2}, {"label": "gw.response_ready", "t": 1}]),
    trace(total=1, marks=[{"label": "gw.response_ready", "t": 2}]),
    trace(marks=[{"label": "api.request_start", "t": 1}]),
    trace(marks=[{"label": "gw.response_ready", "t": True}]),
])
def test_invalid_trace_never_supplies_stage_statistics(record):
    rows, totals = bench._stage_table([result(record)])
    assert rows == [] and totals["traced_n"] == 0
    assert totals["valid_for_comparison"] is False


def test_exception_is_counted_even_when_trace_looks_complete():
    _, totals = bench._stage_table([result(trace(), turn_error="SyntheticError")])
    assert totals["turn_error_n"] == 1
    assert totals["n"] == 1 and totals["wall_median_s"] == 2
    assert totals["valid_for_comparison"] is False


def test_empty_run_is_not_comparable():
    _, totals = bench._stage_table([])
    assert totals["n"] == 0 and totals["wall_median_s"] is None
    assert totals["valid_for_comparison"] is False


def test_trace_reader_consumes_only_current_append(tmp_path):
    path = tmp_path / "trace.jsonl"
    previous = (json.dumps(trace("previous")) + "\n").encode() * 1000
    current = (json.dumps(trace()) + "\n").encode()
    path.write_bytes(previous + current)
    record, error, bytes_read = bench._read_turn_trace(path, len(previous), "bench:1")
    assert record == trace() and error is None
    assert bytes_read == len(current)


def test_no_append_does_not_reuse_previous_trace(tmp_path):
    path = tmp_path / "trace.jsonl"
    path.write_text(json.dumps(trace("previous")) + "\n", encoding="utf-8")
    record, error, read = bench._read_turn_trace(path, path.stat().st_size, "bench:1")
    assert record is None and error == "missing_trace" and read == 0


@pytest.mark.parametrize("data,error", [
    (json.dumps(trace("other")) + "\n", "foreign_trace"),
    ((json.dumps(trace()) + "\n") * 2, "multiple_trace_records"),
    (json.dumps(trace()), "incomplete_trace_record"),
    ("bad json\n", "invalid_trace_record"),
    ("[]\n", "invalid_trace_record"),
    ("[" * 2000 + "0" + "]" * 2000 + "\n", "invalid_trace_record"),
])
def test_trace_reader_rejects_ambiguous_records(tmp_path, data, error):
    path = tmp_path / "trace.jsonl"
    path.write_text(data, encoding="utf-8")
    record, problem, _ = bench._read_turn_trace(path, 0, "bench:1")
    assert record is None and problem == error


def test_trace_reader_reports_truncation_and_bounds_reads(tmp_path, monkeypatch):
    path = tmp_path / "trace.jsonl"
    path.write_bytes(b"x" * 100)
    assert bench._read_turn_trace(path, 101, "bench:1")[1] == "trace_log_truncated"
    monkeypatch.setattr(bench, "_MAX_TURN_TRACE_BYTES", 32)
    record, error, read = bench._read_turn_trace(path, 0, "bench:1")
    assert record is None and error == "trace_record_too_large" and read == 33


@pytest.mark.parametrize("wall,delivery", [(float("nan"), 1), (2, 3), (2, -1), (2, True), (10**1000, 1)])
def test_invalid_wall_or_delivery_invalidates_accounting(wall, delivery):
    _, totals = bench._stage_table([result(trace(), wall=wall, delivery=delivery)])
    assert totals["valid_for_comparison"] is False


def test_incomplete_main_retains_report_and_fails_without_gateway(tmp_path, monkeypatch):
    import sys

    scratch = tmp_path / "scratch"
    monkeypatch.setattr(sys, "argv", [str(BENCH_PATH), "--home", str(scratch)])
    monkeypatch.setattr(bench, "_source_provenance", lambda: {"test": "synthetic"})
    monkeypatch.setattr(bench, "_start_mock", lambda *args: None)
    monkeypatch.setattr(bench, "_stop_mock", lambda *args: None)

    async def fake_run(*args):
        return [result(trace(), wall=2), result(None, wall=100, delivery=None)], 0, "synthetic"

    monkeypatch.setattr(bench, "_run", fake_run)
    with pytest.raises(SystemExit, match="Benchmark incomplete"):
        bench.main()
    report = json.loads((scratch / "result.json").read_text())
    assert report["benchmark_status"] == "incomplete"
    assert report["totals"]["n"] == 2
    assert report["totals"]["wall_median_s"] == 51
    assert report["totals"]["valid_for_comparison"] is False


def test_trace_longer_than_turn_wall_is_rejected():
    _, totals = bench._stage_table([result(trace(total=10), wall=2)])
    assert totals["traced_n"] == 0 and totals["valid_for_comparison"] is False
    _, totals = bench._stage_table([result(trace(total=2.00005), wall=2)])
    assert totals["valid_for_comparison"] is True


@pytest.mark.parametrize("turns,warmups", [(2, 0), (1, 1)])
def test_missing_requested_turns_or_warmup_invalidates_comparison(turns, warmups):
    _, totals = bench._stage_table([result(trace())], expected_turns=turns, expected_warmup=warmups)
    assert totals["requested_n"] == turns and totals["requested_warmup_n"] == warmups
    assert totals["valid_for_comparison"] is False


@pytest.mark.parametrize("records,turns,warmups", [
    ([result(trace(), turn=0), result(trace(), turn=0)], 2, 0),
    ([result(trace(), turn=0), result(trace(), turn=1, warmup=True)], 1, 1),
    ([result(trace(), turn=0.0)], 1, 0),
])
def test_duplicate_or_misordered_schedule_is_invalid(records, turns, warmups):
    _, totals = bench._stage_table(records, expected_turns=turns, expected_warmup=warmups)
    assert totals["schedule_valid"] is False
    assert totals["valid_for_comparison"] is False


@pytest.mark.parametrize("raises", [False, True])
def test_turn_context_starts_before_callback_and_clears_after_it(raises):
    import asyncio

    calls = []

    class Tracer:
        def start(self, key, **meta):
            calls.append(("start", key, meta))

        def bind(self, value):
            calls.append(("bind", value))

    async def handle(event):
        calls.append(("handle", event))
        if raises:
            raise ValueError("synthetic message must not appear in report")

    error = asyncio.run(bench._invoke_benchmark_turn(handle, "synthetic-event", Tracer(), "bench:7", 7))
    assert error == ("ValueError" if raises else None)
    assert calls == [("start", "bench:7", {"benchmark_turn": 7}),
                     ("handle", "synthetic-event"), ("bind", None)]
