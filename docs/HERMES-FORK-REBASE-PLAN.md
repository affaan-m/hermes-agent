# Hermes fork rebase plan (ito line onto upstream 0.21.3)

Written 2026-09-17 against:

- ito line head: `9ad86cc82e61767245cad5aa14a19f69ef989c1f` (`fork/ito`,
  identical to the live production checkout `~/.hermes/hermes-agent`)
- verified upstream base: `c6763abd84` on the ito line = upstream `1c4cc00f`
  (v0.18.0, identical patch-id, tagged `ito-upstream-base`)
- upstream head: `abc19b1349749094daf4683032a15134680ecd7a` (main, just past
  tag `v2026.9.14` = 0.21.3)

Source: L8-ANALYSIS (2026-09-17). Nothing here is executed by this document;
this is the plan for a scheduled window. Upstream rewrites its main branch, so
every rebase step pins upstream by SHA, never by ref name.

## 1. The delta: 21 real commits on the base, in order

(L8-ANALYSIS counted 18; three more landed since: 3b529ff5a8, b2ef2827ec,
ccae8e3bdc, plus docs/hygiene commits 39e54606f9, 04c45b6bfb and the P3.4
marker 28afe49074. 21 non-merge commits total today.)

| # | Commit | Carries |
|---|---|---|
| 1 | fcee6ab4e1 | Deployed short .gitignore replaces upstream's |
| 2 | bbd67eff2b | gateway/host_identity.py (new, +121): single-owner gate so only one host polls a shared Telegram bot token; gateway/run.py hook |
| 3 | b8edaf5b41 | Slack adapter: response_url swap failures reported, not claimed as success |
| 4 | 0d1c9afde0 | Slack adapter: "@bot !cmd" / "@bot /cmd" classified as commands after mention stripping |
| 5 | 3e3672b83b | Never-silent ack for directly addressed turns (gateway/run.py +78, slack adapter +11) |
| 6 | 99373b56b6 | SLKTRACE inbound tracing at info level |
| 7 | a0b4d89a91 | Busy-session interrupt with None so queued text is not persisted twice (+ 2 boundary test updates) |
| 8 | 9f9651f222 | Delegate routing metadata so background async delegations reach the gateway (+ 264-line test) |
| 9 | 66e32914b6 | Ito Agent rebrand: login page, web/index.html, favicon, 16 i18n files |
| 10 | c9fadc78c5 | Built dashboard committed (hermes_cli/web_dist, 22 files) |
| 11 | 2a89acae64 | WhatsApp adapter attaches to an externally managed bridge, follows generation wrappers |
| 12 | 44b151a691 | whatsapp-bridge: durable ingest spool (ingest_spool.js +567), serialized sends, passive-authority check |
| 13 | 0906d45856 | temporal_knowledge plugin + toolset (17 files, ~3,700 lines) |
| 14 | eb0cc5a3fa | Capture: 27 upstream "cache"-path files absent from the deployed tree (deletions) |
| 15 | d3ac8b2443 | Capture: build outputs and runtime markers present in the deployed tree (~500 files, mostly generated) |
| 16 | 3b529ff5a8 | Sanitized shared delivery failures (gateway/delivery.py +21, test +27) |
| 17 | b2ef2827ec | Production capture 2026-09-16: cron/final_only.py (new +172), cron/scheduler.py, gateway/run.py +51, web_server.py, temporal_knowledge service rework, web_dist rebuild |
| 18 | ccae8e3bdc | T11: per-conversation serial dispatch and bounded concurrent conversations (gateway/dispatch.py new +236, platforms/base.py, config keys, 16 tests) |
| 19 | 39e54606f9 | docs/ITO-DELTA.md |
| 20 | 04c45b6bfb | .gitignore: apps/desktop/release binaries |
| 21 | 28afe49074 | temporal_knowledge: graphiti freshness marker after each commit (P3.4) |

Generated mass (commits 10 and 15, about 490 desktop build outputs + 22
web_dist files + egg-info) must NOT be re-landed by hand in a rebase; it is
rebuilt or re-captured after the source delta lands.

## 2. The 42 conflicting delta files, by resolution approach

From the L8 trial merge (3,633 conflicting files total, 42 intersect the Itô
delta).

### Keep ours (Itô behavior upstream does not have)

- gateway/host_identity.py (new file; conflicts only if upstream adds one)
- gateway/dispatch.py (T11 slot pool; new file)
- cron/final_only.py (new file)
- plugins/temporal_knowledge/ (16 files, Ito-only)
- scripts/whatsapp-bridge/ingest_spool.js, send_queue.js (new files)
- tests/gateway/test_dispatch_ordering.py, tests/tools/test_async_delegation_routing.py

### Take upstream, then re-apply the Itô hunk by hand (core, about 15 files)

- gateway/run.py (commits 2, 5, 7, 17, 18 overlap heavily; upstream rewrote
  this file repeatedly)
- gateway/delivery.py (commit 16; upstream 0.19 added its own delivery
  ledger, re-apply only the sanitization hunk)
- cron/scheduler.py (commit 17)
- toolsets.py, tools/delegate_tool.py, tools/async_delegation.py
  (commit 8; upstream 0.21 has its own delegation durability, reconcile)
- plugins/platforms/slack/adapter.py (commits 3, 4, 5, 6)
- plugins/platforms/whatsapp/adapter.py (commit 11)
- scripts/whatsapp-bridge/bridge.js, allowlist.js (commit 12)
- hermes_cli/web_server.py (commit 17)
- tests/gateway/test_busy_session_ack.py, test_delivery.py,
  test_internal_event_never_interrupts_busy_session.py

### Rewrite as profile/skin, not source (upstream now ships a skins feature)

- web/index.html, web/src/App.tsx, index.css, themes/presets.ts, lib/api.ts,
  lib/sidebar-status-poll.ts, components/SidebarStatusStrip.tsx,
  hooks/useSidebarStatus.ts, the 16 web/src/i18n files,
  hermes_cli/dashboard_auth/login_page.py, .gitignore
- Do this extraction BEFORE the rebase window: move the Ito Agent rebrand
  into a skin/profile and the whatsapp-bridge scripts into a profile-side
  install, so the source delta shrinks to gateway, cron, slack adapter and
  the two tool files. hermes_cli/web_dist stops being committed and is
  rebuilt from the skin at install time.

### Mechanical (regenerate, never hand-merge)

- package-lock.json
- scripts/whatsapp-bridge/package-lock.json

## 3. Upstream features the desk needs from 0.19 to 0.21

Named from the upstream release notes (no CHANGELOG.md exists):

- 0.19.0 (v2026.7.20, Quicksilver): the delivery-obligation ledger (final
  responses recorded in state.db and redelivered after a crash; closes the
  silent-loss window our commit 16 partially patches); durable background
  delegation results with ownership-checked ledger; profile-based message
  routing with multiplex hardening; Bitwarden/1Password SecretSource
  (`op://` references; the desk already keeps its credentials in 1Password);
  ~80% first-token cold-start cut.
- 0.20.0 (v2026.8.3, Herald): signed outbound webhooks (HMAC lifecycle
  events; the desk currently polls); A2A v1.0 plugin; user-defined deny
  rules and smarter approvals; context compression overhaul (per-turn
  micro-compaction, guaranteed tail).
- 0.21.0 (v2026.8.31, Pantheon): cron jobs with persistent memory,
  continuity=true and durable notepads (the desk runs 10-minute maintenance
  crons that today hold cursors by hand); steerable subagents (live
  steer/stop for delegate_task); protected agent-instruction files requiring
  write approval; the redaction sweep.
- 0.21.1 (v2026.9.7): codebase modularization, MCP authorization
  improvements, cron scheduling and delivery fixes, delegation reliability.
- 0.21.2 (v2026.9.11): the state.db reliability campaign (six PRs; second
  writers cancelling locks, healthy DBs reported corrupt). Relevant because
  the desk runs multiple writers against profile state.
- 0.21.3 (v2026.9.14): current patch head; pin target.
- Security backports that touch Ito-overlapping files and should land BEFORE
  the window regardless (8-16 h, from L8-ANALYSIS): 3966e5de94
  async_delegation state.db hardening, 1916cb249d owner-only state DBs,
  226df89f74 redaction refactor, d3fc0cca0f OAuth XSS fix, 0997a23e57 config
  secret redaction.

## 4. Test gate (must be green before the gateway restarts on the rebased line)

1. The 116 boundary tests: S2's six gateway boundary modules
   (gateway/message_audience.py, message_failure.py + six tests/gateway/
   modules, commit 3828bdda73 on the 03-fork line, 116 passed / 145 subtests
   when written). These are NOT yet on `ito`; landing them is part of the
   window's prep, and they rerun against the rebased tree.
2. The 16 T11 tests: tests/gateway/test_dispatch_ordering.py (ordering, no
   overlap, no merge, cap across sessions, FIFO admission, error release,
   config, log line, runner path). Plus the known-pre-existing pair
   test_interrupt_still_fires_* which fail identically before and after T11;
   recheck they still fail identically after the rebase, no worse.
3. The synthetic evals: the desk conduct and failure-replay corpora
   (ito-desk monitors/desk_conduct_evals.py, desk_failure_replay.py with
   monitors/eval_corpus/*.json) run against the rebased gateway code path.
4. Live gateway soak after restart: one business day with
   `grep dispatch agent.log` confirming `running` never exceeds the
   configured cap and msg_id order holds per session.

## 5. ito-desk runtime-patches/ entries the rebase supersedes

All hermes patch bundles target the 0.18/0.19-era base; once the delta lives
as commits on a 0.21.3-based ito line they are retired (kept for history,
marked SUPERSEDED):

- runtime-patches/hermes-0.19/ito-supplier-20260909 (supplier.patch,
  stream.patch, interruption.patch, diagnostic-sink.patch,
  continuation.patch)
- runtime-patches/hermes-0.19/outer-error-20260911
- runtime-patches/hermes-0.19/startup-delivery-20260911 (upstream 0.19's
  delivery ledger covers this class natively; verify before retiring)
- runtime-patches/hermes/20260912-inventory-and-delivery,
  20260913-flat-reply-inventory, 20260913-table-context,
  20260914-nonpricing-questions (desk-behavior bundles; confirm each is
  represented in commits 16/17 before retiring)

Unaffected (dependency pins, not hermes patches):
runtime-patches/temporal-knowledge, runtime-patches/playwright-1.58.0,
runtime-patches/slack-sdk-3.43.0.

## 6. Restart window checklist

Pre-window (no production impact):
1. Land the skin/profile extraction (section 2, rewrite group) and the
   whatsapp-bridge profile-side install as their own PRs against `ito`.
2. Land the S2 boundary modules on `ito` (currently stranded on the
   unpushed 03-fork line by the 168 MB Electron binary in shared history;
   that push decision is a separate open item).
3. Land the five security backports from section 3.
4. Pin the exact upstream target SHA (abc19b1349 at writing; re-pin at
   window time because upstream rewrites main).

Window (estimated 60-100 hours, one owner, no parallel gateway changes):
1. M2-style bundle: capture the full production state first. Copy
   ~/.hermes/profiles/ito/ (config, state.db, cron state, memory), the live
   checkout `~/.hermes/hermes-agent` at 9ad86cc82e, and the launchd label
   list to a timestamped bundle under /Volumes/Agent-Runtime/backups/.
2. Record the rollback SHA: 9ad86cc82e61767245cad5aa14a19f69ef989c1f.
3. Rebase the 21-commit line onto the pinned upstream SHA in a worktree,
   resolving per section 2; regenerate the two lockfiles; rebuild web_dist
   from the skin.
4. Run the section 4 test gate to green on the rebased tree.
5. Stop the gateway (launchctl bootout gui/$(id -u)/ai.hermes.gateway-ito),
   swap the checkout, restart, run the one-day soak.
6. Rollback path: bootout, `git checkout 9ad86cc82e` in a restored copy of
   the pre-window checkout, restore the profile bundle, reload the label.
   Rollback is complete when the gateway answers on the old SHA with the old
   state.db.

Nothing in this plan is executed yet; the window is scheduled by MAIN.
