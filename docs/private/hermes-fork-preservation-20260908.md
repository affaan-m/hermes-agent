# Hermes fork preservation and upstream delta

Preservation-phase receipt, September 8, 2026. A later root-authorized eight-path local repair is recorded in `hermes-cache-repair-validation-20260908.json` and lane `STATUS.md`; the tree comparisons below concern the original commits. Source Claude session: `7000f9ed-0eb9-4da1-b525-b1490771ba24`. This verifies the recovered Git snapshot, not the current remote runtime.

## PRODUCTION CHANGES

NONE. During the preservation phase, only private audit/proposal documents, JSON evidence and lane `STATUS.md` were written. The subsequent root-authorized local repair adds five source files and three test files without changing historical commits, existing callers or services. No providers, remote worktrees or global configuration were accessed or changed. Nothing was committed or pushed.

## Preservation result

| Identity | Verified local value |
| --- | --- |
| Worktree branch | `resume/hermes-fork-hermes-20260908` |
| Original ito tip | `330b9f7425c8e137e4f18e288e7520903121c615` |
| Captured main commit | `72cf4da10c78c37710ac32edb6841e9605e9660f` |
| Selected upstream base | `1c4cc00f73f8843f642970c4f35b6aeec22dff5e` |
| Base tree | `613812c9a3b0d3c3c5f9f0db7e5cbd0f5284313d` |
| Identical ito and captured-main tree | `4c4239a9e779b3951a3361018237e6a051f9437f` |

The worktree was clean before this audit. The complete recursive Git tree entries, including modes and object IDs, match between ito and captured main: 6,488 entries each. The selected base has 5,973 entries. There are exactly 15 linear commits from base to tip. The net delta is 603 paths: 543 added, 32 modified and 28 deleted, with rename detection disabled.

The saved remote inventory records an uncommitted `gateway/run.py` change owned elsewhere. Tree equality proves preservation of the captured commit only. It does not preserve or validate that later dirty change. The separate latency branch was not modified.

## Upstream-base evidence and limits

The selected base is the actual parent of the first preservation commit and its `pyproject.toml` declares `0.18.0`. Its commit date is July 3, 2026. A comparison of Git paths, modes and blob IDs against captured main ranks it uniquely closest among 436 available first-parent ancestors with committer dates since July 1 UTC. It differs on 603 paths; its parent `eb99f82ce49a3a7317243dad11b13543d646ad5f` differs on 604. The next two candidates each differ on 608 paths. Full candidate scores are in the accompanying JSON.

Root's read-only Hermes observation in `03-fork/ROOT-UPSTREAM-TAG.json`, timestamp `2026-09-08T15:17:05.093710+00:00`, confirms `refs/tags/ito-upstream-base` resolves to `1c4cc00f73f8843f642970c4f35b6aeec22dff5e`. This lane read that receipt and verified the same local commit object. The remote tag binding is now confirmed; the local tag ref remains absent and was not recreated.

The 436-candidate comparison remains bounded, not the original claimed search across every tag and all first-parent commits through July 7. The imported repository is shallow and later upstream candidates were not available in the inspected refs. The historical bootstrap-marker assertion was not independently checked; no raw environment or configuration files were read. Current upstream state and remote URL configuration were not verified.

## Existing series by concern

These are preserved commit subjects and path scopes, not new implementation or functional-test approvals. Full SHAs, parents and path inventories are in the JSON; the 501-path artifact capture is summarized by top-level counts.

| Commit | Concern | Scope |
| --- | --- | --- |
| `c6c576817` | Capture setup | Short `.gitignore` and deletion of `.envrc`; 2 paths. No env contents inspected. |
| `9fead2dbb` | Telegram ownership | `gateway/host_identity.py` and `gateway/run.py`; 2 paths. |
| `2d23749f5` | Slack response URL failure reporting | Slack adapter; 1 path. |
| `27f5c5d6a` | Slack command classification after mention stripping | Slack adapter; 1 path. |
| `63b43695a` | Acknowledgment for directly addressed turns | Slack adapter and gateway together; 2 paths. |
| `01f45d5fe` | Slack diagnostic tracing | Separate SLKTRACE adapter commit; 1 path. |
| `ed69dec9d` | Busy-session persistence fix | Gateway plus 2 regression-test files; 3 paths. |
| `715989a74` | Background delegation routing | Delegate modules plus routing regression tests; 3 paths. |
| `7149c4e02` | Ito display branding | Login page, dashboard, locales and theme; 20 paths. |
| `c236250f6` | Built display output | `hermes_cli/web_dist`; 22 paths. |
| `47b909597` | WhatsApp external supervisor attachment | WhatsApp adapter; 1 path. |
| `9a9a93a30` | WhatsApp bridge spool and send queue | Bridge modules and lockfile; 5 paths. |
| `fdcb8b3c5` | Temporal knowledge | Plugin source, service definitions, tests and toolset registration; 17 paths. |
| `77da00ade` | Captured omissions | Deletes all 27 base paths containing lowercase `cache`. |
| `330b9f742` | Captured artifacts | 501 paths, primarily desktop build/release output. Also changes root `package-lock.json`, not solely added artifacts. |

The series separates functional concerns, branding, diagnostics and capture fidelity. The acknowledgment change keeps its Slack and gateway halves together. The artifact and omission commits preserve the original snapshot; they should not be interpreted as generally applicable upstream fixes. Any later contribution selection needs separate review without rewriting this preservation history.

## Cache reconciliation

All 27 lowercase `cache` paths present at the selected base are absent from both captured main and ito, and from this checkout. Commit `77da00ade` records those deletions explicitly. This is a committed omission, not a September 8 transfer omission. The pattern alone does not prove why the files were removed from the original runtime tree.

| Missing production source | Retained caller and effect |
| --- | --- |
| `gateway/sticker_cache.py` | `plugins/platforms/telegram/adapter.py:7619` imports it as the first executable statement of `_handle_sticker`, before animated/video handling and outside the later vision-analysis try block. |
| `apps/desktop/src/app/session/hooks/use-session-state-cache.ts` | `apps/desktop/src/app/desktop-controller.tsx:122` still imports the hook. |
| `ui-tui/packages/hermes-ink/src/ink/cache-eviction.ts` | `ui-tui/packages/hermes-ink/src/entry-exports.ts:4` still re-exports it. |
| `ui-tui/packages/hermes-ink/src/ink/line-width-cache.ts` | `measure-text.ts:1` and `widest-line.ts:1` still import it. |
| `ui-tui/packages/hermes-ink/src/ink/node-cache.ts` | `dom.ts:6`, `ink.tsx:31`, renderer and hit-test code still import it. |

The other omissions are 18 test files and 4 skill-index JSON files. The evidence JSON lists every missing path and its base blob ID, allowing exact restoration to be proposed without fetching or substituting newer implementations.

Offline Python AST inspection confirms the sticker import placement. `PathFinder.find_spec` with the explicit repository gateway directory returns no module, without executing gateway initialization. Under normal source resolution this import cannot succeed; any outer handler behavior was not exercised. Four missing TypeScript source modules and their retained callers were also checked. The TypeScript build and packaged desktop executable were not run, so this is source-resolution evidence, not a packaged-runtime failure claim.

## Private preservation and next step

Retain the existing tip and captured-main objects unchanged. The historical proposal was a new empty private repository named `Ito-Markets/ito-agent`. Its existence, privacy, access and suitability have not been verified, and no remote was configured or push attempted. The built artifacts and runtime-specific paths remain part of the preserved tree; this audit is not a full content/security audit or build reproducibility certification.

The precise next implementation proposal is a separate local restoration commit for the five missing production modules using their exact base blobs, with meaningful offline regression coverage and independent review, after root dispatches that repair scope. That would intentionally change the source tree and is separate from the completed preservation audit. Do not copy the remote dirty gateway file or fold restoration into historical commits.

The [bounded repair proposal](hermes-cache-repair-plan-20260908.md) now specifies five exact production blobs, their unchanged lineage through the pre-deletion commit, license preservation and three regression-test paths. Root subsequently authorized and dispatched that exact eight-path scope. Its local patch and static-review evidence are separate from this preservation audit; runtime tests remain pending. The historical tag-binding uncertainty is resolved. A sanitized original base-search result and post-base candidates through `v2026.7.7` would still be needed to validate the stronger exhaustive closest-base claim; that provenance follow-up does not block specifying the exact-blob restoration.

## Validation

Verified exact HEAD, clean initial status, complete tree-entry equality, 15 consecutive parent links, 603-path net delta, all 27 committed cache omissions, 436 candidate distances, and offline source-resolution checks for five modules. The accompanying [JSON evidence](hermes-fork-preservation-20260908.json) records hashes and comparison scope. No dependency installs, application tests, builds, live production tests or independent functional/security review were performed. Those checks describe the pre-repair preservation phase. The later repair adds only the authorized eight code/test paths, with its own private validation evidence.
