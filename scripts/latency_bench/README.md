# Trivial-turn latency bench

This is the recovered gateway benchmark from feature commit
`d86a7a79aead3f2c5fffcda3f05d13591b7cee70`. The local continuation replaces
profile/database imports with deterministic synthetic history. It retains the
existing mock server, gateway timing instrumentation and disabled prototype.

Prepare inspectable inputs with Python 3.11 or later, without importing Hermes,
starting the mock server or changing environment variables:

```sh
python3 scripts/latency_bench/bench_trivial_turn.py --prepare-only --history-pairs 8
```

The command prints the unique scratch directory and a source manifest. An
optional `--home` must name a nonexistent directory whose parent already exists.
Existing directories and symlinks are refused. There is no cleanup operation;
artifacts are retained for review. The old `--profile`, `--live-db`,
`--history-session` and `--clean` arguments are no longer accepted.

Each preparation writes:

- `history.json`: alternating synthetic user/assistant messages, ending with an
  assistant reply. `--history-pairs 0` prepares a fresh session.
- `config.yaml` and `SOUL.md`: generated benchmark inputs, with a loopback mock
  endpoint and a dummy credential. No credential file is copied or generated.
- `manifest.json`: status `prepared_only`, checkout HEAD/version, dirty state,
  Python executable/version, hashes of the benchmark and latency source files,
  and a canonical synthetic fixture digest. Preparation is not a latency result.

The full benchmark requires the project's installed dependencies and supported
Python range from `pyproject.toml`. Run only in a disposable environment with
external egress blocked, no inherited credentials, and no real user configuration
or plugins accessible. The harness's temporary `HERMES_HOME` alone does not
establish that isolation. Root owns environment provisioning and permission to
run the gateway path in the recovered workspace.

When that environment is available, omitting `--prepare-only` starts
`mock_openai_server.py` and drives the real `GatewayRunner._handle_message`
with a fake Telegram adapter. `--api-mode` can select `chat_completions` or
`codex_responses`. `--fast-path` defaults explicitly to `0`; its benchmark-only
`1` option selects the existing prototype. Never set this on a live gateway.
`--stall-turn N` stalls the first request of turn N, indexed from zero including
warmup. The mock supports streaming and non-streaming on both transports.

Mock startup on POSIX systems reserves `127.0.0.1:--mock-port` in the parent
and passes that socket to a Python `-I -B` child with a minimal environment. An
occupied port fails at bind, before spawning the child or making any HTTP,
model or control request. No `/v1/models` readiness probe is used. The child
adopts the listener with `aiohttp.web.SockSite` and acknowledges its PID and
endpoint over a private inherited pipe only after the site starts. The parent
checks that identity and child liveness within 15 seconds. Invalid readiness,
early exit and timeout stop/reap the child and close the reserved descriptors.
`SO_REUSEADDR` permits a same-port restart after prior TCP traffic; the parent
calls `listen` before handoff and never enables `SO_REUSEPORT`.
The parent retains its socket until shutdown, preventing port takeover even if
the child exits after readiness. Startup diagnostics remain in
`mock-startup.stderr` in the new scratch directory.

Preparation remains independent of the POSIX startup mechanism. Full mock
startup currently requires POSIX descriptor inheritance; unsupported systems
fail before opening a listener. Focused readiness tests use stdlib stub servers,
real child processes and inherited descriptors, plus a fake aiohttp interface
to check acknowledgement ordering. They do not validate the actual aiohttp
runtime, Hermes imports or model calls.

Completed runs always retain `result.json` in the scratch directory. `--out`
requests an additional copy; existing output files are refused, with the scratch
result preserved even if export fails. Results include actual imported
module paths, request shapes, per-stage timings and fixture/source provenance.
Use matching fixture digests, source hashes, transport, warmup, delay and turn
counts for paired comparisons. Synthetic fixture sizes are not token counts,
and this workload does not reproduce the historical Ito Ops transcript.

Each benchmark turn starts a unique trace context and reads only the bytes
appended during that turn, capped at 2 MiB. Missing, foreign, duplicate,
truncated, malformed or unfinished traces are recorded as errors instead of
reusing the preceding turn's record. A caught turn exception stops further
turns, retains the attempted turn's wall time and records its exception class.

Summary `n` counts every measured attempt, including untraced attempts. Separate
`wall_n`, `traced_n` and `delivered_n` counters describe coverage; missing timing
values are `null`, displayed as `unavailable`. Repeated stage intervals are
summed within each turn before taking medians across observed turns. The table
reports each stage's observed-turn count; stage medians need not sum to a total
median. Labels describe time since the preceding mark, not isolated component
costs. The first adapter delivery may be a status/error message, not final text.

Missing or invalid measured/warmup accounting yields `benchmark_status:
incomplete`, retains `result.json` and exits unsuccessfully.
The complete requested turn order, unique turn indices and warmup flags must
match; a short run cannot pass by reporting only its surviving attempts. Trace
totals must fit inside the enclosing wall time, allowing for trace rounding.
`valid_for_comparison` is an accounting precondition only: it does not verify
response correctness, matching source/fixture/payloads between runs, actual
runtime identity, or model-side improvement. Those checks remain required.

The September 2 receipt reported checkout 0.18.0 versus an installed gateway
copy of 0.19.0. The installed source identity is still unverified. Neither a
preparation manifest nor a mock benchmark proves deployed compatibility or a
model-side latency improvement. An exact imported-package source snapshot and
separate authorized model measurement remain necessary for that handoff.

Focused regression checks, once root supplies the test environment:

```sh
scripts/run_tests.sh -j 1 tests/scripts/test_latency_bench.py tests/agent/test_fast_path.py
```
