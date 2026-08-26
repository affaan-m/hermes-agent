# Supplier-Desk Autonomy Pattern

How to run a Hermes gateway agent as a counterparty-facing desk (supplier or
buyer channels) with production discipline. All of it is config keyed by
channel id — no per-channel code. Adopting it for a new channel is one entry
per knob.

Proven in production on a live OTC compute desk (Slack supplier channels,
Slack Connect, Telegram ops) on 2026-08-25/26.

## The behavior contract

| Trigger | Behavior |
|---|---|
| Directly addressed (mention, DM, bot-thread reply) | Full answer, working surface visible (typing status + streamed answer), never silent. |
| Unaddressed, owns the answer (status, terms, facts, clarification) | Answers directly, in-channel, no filing. |
| Unaddressed, has something material but unasked | One short offer line, then waits. |
| Unaddressed, nothing material | Silent. Context is still ingested. |
| Anything that makes or changes a commitment (pricing, terms, dates, contract docs, allocations, first outreach) | Files an exact-payload draft for operator approval; the approved draft auto-sends into the thread. |
| Operator talking in the supplier channel | Answer reroutes to the internal home channel, tagged as bookkeeping. |

Silence is a delivery decision, never an intake decision: free-response
ingestion is unconditional, so nothing is lost by listening quietly.

## The knobs (per channel id)

- **Chat-scoped authz** — `SLACK_GROUP_ALLOWED_CHATS` /
  `TELEGRAM_GROUP_ALLOWED_CHATS`: any member of the listed channel may talk
  to the desk. Default-deny everywhere else is unchanged.
- **Free-response** — `platforms.<name>.extra.free_response_channels`: the
  desk processes unmentioned messages (listening mode).
- **Quiet channels** — `display.quiet_channels`: the working surface is
  muted (no reasoning prepend, no tool-progress bubbles, no status or
  self-improvement notices, no "Interrupting current task", no
  "Working — N min" heartbeat). Final answers still stream in normally.
  The typing indicator is the only activity surface.
- **Persona** — `platforms.<name>.channel_overrides.<channel_id>.system_prompt`:
  the desk's behavior contract (tone, autonomy boundary, identity rules,
  house style). This is where "answer directly / offer / stay silent / file
  commitments" lives for the model.
- **Operator bookkeeping reroute** — automatic for quiet channels: an
  operator's unaddressed message gets its answer in the platform home
  channel instead of in front of the counterparty.

## The internal splits

Internal traffic separates by purpose so the home channel stays a clean ops
summary:

- `gateway.jobs_channel` — background/queue job lifecycle notices.
- `gateway.desk_log_channel` — self-improvement and background-review traces.
- `SLACK_APPROVALS_CHANNEL` — approval filings mirror (desk_approval plugin).

## The approval boundary

Approvals are actions: the desk files exact-payload drafts; an operator
approval sends automatically. The queue is only for commitments. Batch
approvals where possible (one filing covering a whole outbound batch).

The boundary must live in the approval tool's own description (the model
reads it): file only what an operator must stand behind — never courtesy
replies, acknowledgments, owned status answers, simple facts, or clarifying
questions.

## Reliability notes (production-agent requirements)

- Socket-mode connections can go quiet while reporting "connected". Watch
  event-flow liveness, not just connection state.
- Gateway restarts interrupt in-flight turns; auto-resume covers them, but
  config that reads per-turn (the quiet/display paths) should be preferred
  over restart-requiring changes during live deal work.
