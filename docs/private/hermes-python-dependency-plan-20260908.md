# Focused Telegram Python validation dependency proposal

Prepared 2026-09-08 for root. Source HEAD remains `330b9f7425c8e137e4f18e288e7520903121c615`. This is an AST/TOML/source-only plan. No Hermes, Telegram SDK or other dependency module was imported, no installation was attempted, and no Python test ran. Standard-library metadata inspection used isolated Python; that helper is not the validation runtime.

## Minimal pins

Target a separate normal-GIL CPython 3.13 macOS arm64 venv at `03-fork/validation-environment/python-telegram`, outside the repository and separate from root's partial validation venv. Captured `requires-python` is `>=3.11,<3.14`. Root's existing environment receipt reports Python 3.13.2, but the interpreter for this new venv still needs root selection and attestation. The current metadata helper is Python 3.14.3 and is unsuitable for runtime validation.

| Package | Exact captured pin | Reason |
| --- | --- | --- |
| python-telegram-bot | 22.6 | Real `telegram`, `telegram.ext`, constants and request imports |
| httpx | 0.28.1 | Telegram SDK and `telegram_network.py` |
| pyyaml | 6.0.3 | `utils`, `hermes_cli.config`, plugin manifest metadata |
| pytest | 9.0.2 | Focused tests and conftests |
| anyio | 4.12.1 | httpx dependency |
| certifi | 2026.5.20 | httpx/httpcore dependency |
| httpcore | 1.0.9 | httpx dependency |
| idna | 3.15 | httpx/anyio dependency |
| h11 | 0.16.0 | httpcore dependency |
| iniconfig | 2.3.0 | pytest dependency |
| packaging | 26.0 | pytest dependency |
| pluggy | 1.6.0 | pytest dependency |
| pygments | 2.19.2 | pytest dependency |
| psutil | 7.2.2 | Retain existing autouse process-protection fixture |

There are **13 required packages plus one guard package**. `psutil` is not strictly required to collect: `tests/conftest.py:582` catches its absence. Include it so the existing live-system guard retains descendant-process checks. All 14 versions and selected wheel hashes come from the captured `uv.lock`; direct project pins agree where declared. No broad `[messaging]`, `[dev]`, editable Hermes installation or full project environment is needed for this bounded path.

[Hash-pinned requirements proposal](hermes-python-validation-requirements-20260908.txt) includes exactly those 14 wheels. [Machine-readable evidence](hermes-python-dependency-plan-20260908.json) contains URLs, sizes, dependencies/markers, hashes and manifest identities. Root should use wheels only and hash verification, and inspect installed wheel METADATA for consistency with this lock-derived closure. A metadata discrepancy is a handoff back to this lane, not permission to expand the environment automatically.

Excluded intentionally:

- `python-telegram-bot[webhooks]` would add `tornado==6.5.5`; these tests import the real SDK without initializing webhooks or a bot. No webhook extra is needed by the captured test branches.
- The project uses `httpx[socks]` generally, but this path does not instantiate a proxy client. No socks/http2 extra is needed here.
- Gateway conftest optionally imports `filelock` for its guard cache; missing filelock uses a caught no-lock fallback. A single-worker lease does not need that optional package.
- `pytest-asyncio==1.3.0` is unnecessary: the focused async handler tests use `asyncio.run` from synchronous tests. No async pytest marker or fixture is used.
- `typing-extensions==4.15.0` is conditional on Python below 3.13 in this closure; `colorama` is conditional on Windows. Neither belongs in this Python 3.13 macOS selection.
- Provider discovery reaches `agent/__init__.py` through the Nous provider and `agent.portal_tags`. `agent/jiter_preload.py` attempts `jiter.jiter` and `jiter.from_json`, catches the missing import and returns false. `jiter==0.13.0` is an optional best-effort preload, not needed for these sticker tests. This omission does not mock the Telegram SDK.
- No eager requirement for OpenAI, dotenv, cryptography, Pillow or vision-provider packages was found in the inspected closure. The static-cache-hit and animated/video branches return before sticker download and `tools.vision_tools` loading.

## Real import path and side effects

`tests/gateway/test_sticker_cache.py` imports `gateway.sticker_cache` during collection. Python first executes `gateway/__init__.py`, which imports gateway config/session/delivery. The cache module imports `hermes_cli.config` and computes `CACHE_PATH` at module scope. The real adapter loads gateway base/helpers/session/config, Telegram ID/network helpers, and real Telegram SDK classes. Gateway base computes several cache roots at import time.

`hermes_cli.config:8014` calls `_inject_profile_env_vars()` at import time. This calls `providers.list_providers()` and discovery of 29 bundled model-provider registrations, then checks `$HERMES_HOME/plugins/model-providers` for user Python plugins and finally legacy `providers/*.py`. Bundled registration imports are source metadata/class registration, with the optional Jiter preload described above. If launched with a real Hermes home, discovery can execute user plugin code before pytest fixtures start. `hermes_cli.config:8111` also injects platform environment-variable descriptions from bundled plugin YAML manifests; reading these source manifests is distinct from reading an actual user configuration or credentials.

`hermes_constants.get_hermes_home()` falls back to `Path.home()/.hermes`, can read `active_profile`, and prints a warning naming it. `get_hermes_dir()` checks legacy directory contents. Both HOME and Hermes path resolution must therefore be isolated before collection. `tests/conftest.py:329` redirects HERMES_HOME per test but explicitly leaves HOME unchanged; it is too late to protect collection-time imports. Its plugin-singleton reset imports `hermes_cli.plugins` during setup. No actual user configuration was read for this plan.

`plugins/platforms/telegram/adapter.py:113`, `check_telegram_requirements()`, can call `tools.lazy_deps.ensure("platform.telegram", prompt=False)` when the SDK is unavailable. Do not invoke registration/check/connect/start operations. `tools/lazy_deps.py:419` consults `load_config()` and permits installation by default. `security.allow_lazy_installs: false` is the absolute configuration opt-out. `HERMES_DISABLE_LAZY_INSTALLS=1` alone is insufficient when `HERMES_LAZY_INSTALL_TARGET` is set, because that combination redirects installation into a durable target. The sanitized launch must exclude the durable target and block installer subprocesses, in addition to any synthetic configuration or environment switch. A synthetic launch-home config alone is insufficient after fixtures replace HERMES_HOME. Tirith auto-install is disabled by the existing fixture, but launch-time protection should not depend on fixture timing.

`telegram_network.py` defines HTTPX/DNS-over-HTTPS behavior; the intended cases do not initialize transports. The sticker handler returns before downloads/vision for all three adapter cases. Tests use the real installed SDK module and require a real `Bot` type and matching adapter binding, removing/restoring conftest's Telegram mocks around adapter loading. They never construct a Bot or connect an adapter. This is genuine SDK import compatibility and local synthetic handler behavior, not a Telegram service test.

## Existing command and missing sanitized launcher evidence

The accepted repair plan records this canonical source command, from the repository root:

```sh
scripts/run_tests.sh -j 1 tests/gateway/test_sticker_cache.py -q
```

The repository wrapper is present and statically inspected. Its SHA256 is `e40e4d8cbf417a8aacf0c2bab67e43e42232df5cab3526950073262f4fd29d00`; `scripts/run_tests_parallel.py` is `338978835b49d96527ca0f562c4e1c2d894513487826bcf894730df3268047a1`.

**No separate root-reviewed sanitized Python wrapper, guard artifact or review receipt has been supplied in this lane's validation-environment or referenced by the accepted plan.** The plan requires root to supply a sanitized process environment and temporary home; that requirement is not evidence that the launcher already exists. The completed Node guard is not a Python launcher. Request only the missing sanitized Python path/hash/review receipt through STATUS.md; do not inspect the actual home guard.

The captured canonical wrappers have these relevant limits:

1. The shell wrapper probes repo `.venv`, repo `venv`, then `$HOME/.hermes/hermes-agent/venv` before clearing the environment. It also checks `$HOME/.hermes/pytest_live_guard.py` before clearing it. A temporary HOME must be in place before invoking this script. It has no explicit external-venv option.
2. Its `env -i` retains HOME and PATH but drops caller HERMES_HOME, TMPDIR, PYTEST_DISABLE_PLUGIN_AUTOLOAD and HERMES_DISABLE_LAZY_INSTALLS. It conditionally forwards the home guard using PYTHONPATH/PYTEST_PLUGINS. Root's reviewed launcher must account for these resets and verify the effective environment in the pytest child. No home-installed guard or venv may be used.
3. The Python runner launches `sys.executable -m pytest` in a fresh per-file process. It supports `-j 1`, `--file-timeout 120` and forwarded pytest flags such as `-p no:cacheprovider`. It unconditionally writes `repo/test_durations.json` after a completed file and has no duration-output flag. That write is outside the eight-path repair ownership. Root must supply a reviewed temporary duration sink or authorize the narrowly scoped launcher adaptation; do not change repository scripts or overwrite an existing duration cache.
4. Gateway `tests/gateway/conftest.py:405` also writes and prunes `Path.cwd() / ".pytest-cache"` for its adapter-import guard. The runner fixes child CWD to the repo. This is independent of pytest cacheprovider and cache_dir flags. Root must isolate this output too, preserving the real guard execution and any existing cache entries.
5. Pytest entrypoint autoload must be disabled before collection, or root must verify the fresh venv contains no unexpected entrypoint plugins. Deny network in the pytest child before SDK/application imports and prohibit installer subprocesses. The existing conftest process guard is not a network guard. Avoid raw inherited PYTHONPATH, PYTEST_PLUGINS and `.env`/user-site settings.

Preferred root handoff: separate venv, explicit interpreter in a reviewed temporary launcher retaining the canonical per-file runner, clean temporary HOME/HERMES_HOME/cache/output directories, duration and gateway guard-cache output redirected outside the repo, and a pre-collection guard. This is a proposed launcher adaptation, not an already reviewed executable command. If root has an existing reviewed solution, use its supplied bytes after reconciling these five limits. Preserve the repository wrappers and eight-file patch.

## Independent static review

The existing bounded security reviewer rechecked the dependency closure and launcher source. No additional mandatory package was found. Its gateway guard-cache finding is incorporated above. Review supports the dependency proposal only; launcher approval remains pending because root has not supplied executable launcher/guard bytes. No code changes, imports, installs or runtime tests were performed by the reviewer.

## Budget, acceptance and next step

Selected 14 wheel downloads total **3,252,666 bytes (3.102 MiB)** according to uv.lock, without interpreter/bootstrap downloads. Propose a **150 MiB disk envelope** for the separate venv, wheel artifacts, temporary homes and logs, reusing a root-selected existing CPython 3.13 interpreter. Actual expanded size, interpreter availability and current disk capacity require root verification. No source builds, Homebrew/system packages, home runtime mutation or automatic dependency expansion. A new interpreter download would require a separate budget.

Execution, only after root's dependency receipt and serialized lease: one focused file, one worker, **120-second per-file timeout**, expected **21 cases** from source metadata, temporary pytest cache/output, no other suite. Treat zero collected/skipped adapter coverage as incomplete even if the canonical runner returns success; it treats pytest exit 5 as a pass. Verify the fixture's genuine Telegram module/type assertions execute. Report runtime failures without relaxing SDK checks or adding mocks. No Python test lease is currently held; the Node lease remains released.

Missing evidence is limited to the separate interpreter/venv and installed wheel metadata receipt; exact sanitized Python launcher/guard bytes and review receipt resolving output/early-import isolation; and the Python execution lease. No additional upstream provenance is required for this dependency plan. Root should install only after it accepts this bounded environment proposal. This lane remains promptable and has performed no installation or dependency-bound execution.
