# Trivial-turn latency bench

Tools used to profile one trivial gateway turn end to end and to compare the
trivial-turn fast path against the default path.

* `mock_openai_server.py` serves an OpenAI-compatible model (`/v1/chat/completions`
  and `/v1/responses`, streaming and non-streaming) with a canned reply, a
  configurable first-token delay, and a control endpoint that makes the next
  request hang without sending a byte. Every request shape is logged as JSONL.
* `bench_trivial_turn.py` builds a throwaway `HERMES_HOME`, starts the mock,
  constructs a real `GatewayRunner` with a fake Telegram adapter, optionally
  imports a real transcript from a live `state.db` (opened read-only), and
  drives `GatewayRunner._handle_message` N times. Per-stage timings come from
  `agent/latency_trace.py`, which the gateway and agent call at each stage
  boundary. The summary line also lands in the normal logs as
  `latency trace: ...` on every gateway turn (set `HERMES_LATENCY_TRACE=0` to
  silence it, `HERMES_LATENCY_TRACE_JSONL=path` to keep records).

Example, mirroring the live Ito desk path (Responses API, real Ito Ops history):

```
python scripts/latency_bench/bench_trivial_turn.py \
  --home /tmp/hermes-bench --turns 5 --warmup 1 --clean \
  --profile ~/.hermes/profiles/ito \
  --live-db ~/.hermes/profiles/ito/state.db --history-session 20260811_150601_4623e079 \
  --api-mode codex_responses [--fast-path 1] [--stall-turn 2]
```

`--fast-path 1` sets `HERMES_FAST_PATH=1` (see `agent/fast_path.py`).
`--stall-turn N` arms the mock so the first model request of turn N never
sends a byte, which exercises the first-byte watchdog and retry loop.
