# Five-module cache restoration proposal

Private local proposal, September 8, 2026. Source session `7000f9ed-0eb9-4da1-b525-b1490771ba24`. Root accepted the eight-path scope and dispatched tests-first preparation plus exact-blob restoration. The local patch is prepared; runtime imports/tests remain pending root dependencies and a serialized lease.

## Provenance and repair boundary

Root's `03-fork/ROOT-UPSTREAM-TAG.json`, observed at `2026-09-08T15:17:05.093710+00:00`, confirms the Hermes tag `refs/tags/ito-upstream-base` points to `1c4cc00f73f8843f642970c4f35b6aeec22dff5e`. That exact local commit is available. The local tag ref remains absent; it does not need to be created to perform the repair.

The proposed baseline is the existing isolated branch at `330b9f7425c8e137e4f18e288e7520903121c615`. Add the following five production files byte-for-byte from the selected base, with their original `100644` modes. Every listed blob was independently verified identical at base and at `fdcb8b3c5ef262e824b447e8cbbe3acb748594fe`, the parent of deletion commit `77da00adec455eb58a8800a5b6b840daec9d9bad`.

| Production path | Exact source blob | Bytes | Last upstream path change at base |
| --- | --- | --- | --- |
| `gateway/sticker_cache.py` | `c53681730674e252ba770c23f7024d480f245358` | 3,468 | `62573f44cfee8895eec2cb18e7c45b9bff97081a` |
| `apps/desktop/src/app/session/hooks/use-session-state-cache.ts` | `3f8e02c8ca8a24a25996554a798a1634882008b6` | 11,348 | `62fe9fd1011a4a5931a32a2b61324d5af6eafa67` |
| `ui-tui/packages/hermes-ink/src/ink/cache-eviction.ts` | `f0155eb9b0d2c4d282dbf4d76f874aa11eba30b3` | 1,386 | `ffa33e53f6b2943624cd95365ba1a0f8bc8362c5` |
| `ui-tui/packages/hermes-ink/src/ink/line-width-cache.ts` | `71b02b62268fd2081112bff15ff21479eaa37171` | 874 | `b1c49d5e73b85ee1713e5041c336364af46b3677` |
| `ui-tui/packages/hermes-ink/src/ink/node-cache.ts` | `fe11e067ec1348d05883953aac180ff93c466a58` | 1,648 | `8760faf991ec13231ab790bdf3d5ab1d86850770` |

Total production restoration: 18,724 bytes. Preserve all comments, attribution and existing source notices exactly. Root `LICENSE` is the same at base and tip, blob `75410e73319c72cd3e991a501c5455eb78f38375`, containing the MIT notice and Nous Research copyright. Leave that license and all existing third-party notices in place; do not relabel upstream source as newly authored. Record the source commit/blob lineage in the eventual repair description.

All existing callers, exports outside these files, manifests, lockfiles and runtime configuration remain unchanged. Specifically, `gateway/run.py`, the Telegram adapter, desktop controller, and current Ink renderer/DOM callers are outside the proposed edit set. The base implementations provide the missing APIs those callers already use. Preserve all 15 historical commits and the original tip. Do not revert the entire deletion commit, which would restore 22 additional paths beyond the production scope.

## Proposed regression paths

The code/test edit set is exactly eight paths: the five production files above and these three test files. Private report/status updates are documentation, outside this code/test set.

| Test path | Planned change |
| --- | --- |
| `tests/gateway/test_sticker_cache.py` | Restore source tests from base blob `9223a11e17d972f74c14a7c66c7b0377e5ca2bbd`; retain their cases and append real-import adapter regression coverage. |
| `apps/desktop/src/app/session/hooks/use-session-state-cache.test.tsx` | Restore source tests from base blob `025cb34b90f6db237cbee3430c2eef814737a8d7`; retain their timer, metadata and error-isolation coverage, then append missing transition/RAF tests. |
| `ui-tui/packages/hermes-ink/src/ink/cache-recovery.test.ts` | Add focused behavioral coverage using the real restored Ink modules and existing dependencies. |

Both restored test blobs also match the parent of the deletion commit. Restoring these two directly relevant test sources does not authorize restoration of the other omitted tests or skill-index assets.

## Regression plan

1. Establish failing behavior with the proposed test files before adding the production files. Capture collection/import failures for the missing modules separately from dependency failures. No dependency failure counts as reproduction of the omission. After restoration, use the same focused commands to demonstrate the change.

2. Telegram: retain upstream cache round-trip, missing/corrupt file, overwrite and sticker-text cases. Append cases that import `plugins.platforms.telegram.adapter.TelegramAdapter` and the real cache module, then call the real `_handle_sticker` method with synthetic message/event objects. Exercise both animated and video stickers, and a static sticker with a prepopulated temporary cache. Assert injected text and that no file download or vision call occurs. Use sentinel failures on those external boundaries; never stub `gateway.sticker_cache` or replace the handler. Use the installed Telegram SDK for import coverage without connecting a bot. Add a write-failure case demonstrating the previous valid cache remains intact when atomic replacement fails. All cache I/O stays under a temporary Hermes home/cache path.

3. Desktop: retain the upstream tests for per-session clocks, focused model metadata and cross-thread error isolation. Add behavior checks that background updates cannot displace a pending foreground view, unchanged same-session heartbeats do not republish the transcript, completion/needs-input flush immediately even when RAF is suspended, and unmount cancels the pending frame. Use real hook/store imports with synthetic sessions and controlled RAF callbacks. No Electron launch, gateway connection or model request. Reset stores and timers between tests.

4. Ink: compare `lineWidth` with the actual `stringWidth` result for ASCII, ANSI, wide characters, combining characters and empty input; repeat after eviction to verify preserved results. Fill the real caches and check half/full eviction reduces counts according to the public operation, without freezing private capacity constants. Check cached node layouts remain node-specific, pending-clear rectangles accumulate on the correct parent, and the absolute-removal flag is consumed once. Re-run existing `hit-test.test.ts` unchanged to exercise a real DOM caller. Bundle the real `entry-exports.ts` into a root-approved temporary output path to confirm internal relative imports resolve, without running the bundle or rebuilding packaged applications.

5. Verify all five restored files still hash to the exact listed blobs and have mode `100644`; no existing tracked caller, manifest, license or runtime file may change. Check the final code/test diff against the eight-path allowlist and record the resulting test output. Request bounded independent code and security review of the exact proposed patch before any local commit handoff. Unexpected fixes requiring additional paths return to root as a scope amendment.

## Execution prerequisites and commands

No tests were run for this proposal. Local `.venv/bin/python`, `venv/bin/python`, and root `node_modules/.bin/{vitest,esbuild,tsc}` are absent. Root alone supplies the isolated Python environment and locked workspace dependencies, and serializes the test/build lease. No installs or dependency changes are proposed in this lane.

Use a root-supplied sanitized process environment and temporary home, so the Python wrapper cannot load the global live-gateway guard or fall back to a home-installed virtualenv. The repository's canonical Python wrapper and existing test isolation fixtures remain unchanged. Do not use raw global auth/env/config files. Deny external network access during these synthetic checks.

After root dispatches the eight-path scope and supplies that environment, the focused commands from the repository root are:

```sh
scripts/run_tests.sh -j 1 tests/gateway/test_sticker_cache.py -q
npm run test:ui --workspace apps/desktop -- src/app/session/hooks/use-session-state-cache.test.tsx --maxWorkers=1
npm run test --workspace ui-tui -- packages/hermes-ink/src/ink/cache-recovery.test.ts packages/hermes-ink/src/ink/hit-test.test.ts --maxWorkers=1
```

For the temporary Ink bundle, invoke the supplied local esbuild binary on `ui-tui/packages/hermes-ink/src/entry-exports.ts` with `--bundle --platform=node --format=esm --packages=external` and an explicit output file inside the approved temporary directory. No package build scripts, desktop install-stamp scripts, release artifacts or broad test suites are part of this bounded plan. Check TypeScript signatures through the focused source graph and available typechecking configuration; any unrelated baseline type errors must be reported separately, not repaired here.

## Completion boundary

The repair will establish local source/module resolution and synthetic behavior only. It will not update built desktop/TUI assets or a running gateway. Preserve the remote dirty gateway change and all unrelated cache/test/assets history. The broader historical closest-base search through July 7 remains an optional provenance follow-up; root's verified tag and the available exact blobs are sufficient to specify this restoration. Root dispatched this implementation scope. Static checks and independent review are recorded separately in `hermes-cache-repair-validation-20260908.json`; the planned failing/passing runtime test sequence has not run because root has not supplied the required environment or execution lease.
