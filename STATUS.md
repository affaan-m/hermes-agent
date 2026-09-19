# CALLER-ACTIVATE status

Implemented on feat/caller-host-activation from fork ito c56952d9e1.
Default-off profile composition binds authenticated exact original message and
delivery, explicit typed result approval, typed proposal exchange with independent
hash lookup, existing V2 observer and terminal cleanup. No provider action,
configuration mutation, install, restart, merge or deployment.

Final validation: scripts/run_tests.sh on caller activation/startup, cloud caller,
host adapters, context tool/refresh and gateway configuration: 206 passed,
0 failed, 4 existing captured segmented-executor skips. Diff check clean.
Independent code and security review completed; missing startup validation,
callback deadline propagation and flagged-on runner coverage addressed.

Operator-owned authenticated provider, reviewer and receipt-producing services
remain required before enabling. See docs/ito/caller-host-activation.md.
PR creation and requested desk receipt are the remaining delivery steps.
