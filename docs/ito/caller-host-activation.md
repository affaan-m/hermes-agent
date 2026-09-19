# Native caller host activation

`gateway.caller_activation` composes the installed native caller, typed proposal
reviewer, V2 ECS/IAM/ENI observer, terminal receipt reconciliation, and owned
artifact cleanup. It is disabled by default. This change does not enable, install,
or restart a gateway and does not authorize a cloud pilot.

In the selected profile's `config.yaml`:

```yaml
gateway:
  caller_host_enabled: false
  caller_host_provider: "operator_caller:build_services"
```

Only YAML boolean `true` enables this path. With the flag absent/false the gateway
never imports the activation module or provider, resolves credentials, or adds
the scoped tool. The existing explicit `context_tool_factory` host injection
remains compatible. Combining it with profile activation, or enabling activation
on a profile multiplexer, fails startup. There is no model or environment flag.

The provider is **trusted installed operator Python code**, not an agent-selected
module. Its `build_services(profile_home=Path(...))` must return `HostServices`.
It receives no agent or model arguments. It must assemble authenticated services
without launching work. Applications embedding GatewayRunner may instead inject
`caller_host_services=services` with the same profile flag.

Required service contracts:

- `session`, `policy`, and exact `mqtt_config`: authenticated bounded boto session,
  reviewed ContextLaunchPolicy V2 and per-attempt X509 configuration. The existing
  `infra.aws` canonical/exchange/runner package must be importable. No default
  credentials or substitute policy is constructed here.
- `enrollment_lookup(attestation)`: bounded, fresh protected read returning stable
  opaque host/controller handles, exact dispatcher `run_kwargs`, and `expires_at`.
  The attestation includes profile/user/platform/workspace/channel/thread,
  actual delivery identity, original message ID and authenticated bot identity.
  Return None on denial/revocation. Reuse handles for this request; do not mint
  new handles on revalidation. Scope/task/input must be bound to this exact
  authenticated request by this service. No lookup may launch a provider action.
- `create_owner()`: one canonical owner graph, opened during invoke on the original
  foreground thread. The existing dispatcher owns closing its resources.
- `typed_reviewer(typed=bytes, spec=bytes, deadline_at=float)`: independently
  authenticated, deadline-bounded proposal exchange. Immutable typed proposal and
  request-spec bytes are sent once; exceptions/unknown outcomes are never retried.
  The reviewer must independently authorize and durably record the proposal.
- `approval_resolver(spec_sha256=..., request_spec_sha256=..., deadline_at=...)`:
  independently authenticated read of that exact reviewer decision. ExistingApproval
  still checks reviewer independence, expiry, hashes and canonical state.
  The gateway never calls canonical.approve or canonical.accept.
- `receipt_reader(key, deadline_at=...)`: bounded read of independently produced
  claim/start/terminal receipts, consumed by TerminalReceipts. ECS STOPPED alone
  does not establish cleanup or independent receipt authority.
- `approve_result(request_sha256=..., result_sha256=..., context=...,
  deadline_at=...)`: explicit conversation-safe approval returning `ApprovedContext`
  with those exact hashes, approved public text and a fresh expiry. A dict, bool,
  self-declared worker scope, stale approval or mismatched hash is denied.
- `audit_receipt(receipt, deadline_at=...)`: durably persist the sanitized
  `caller-activation/1` receipt before public release; raise on failure. Receipt
  contains request/result hashes and pending-review/cleanup/capacity state only.
  It records approved projection after clean dispatcher closure, not delivery
  acknowledgment, canonical acceptance or released durable capacity.

All callbacks are trusted bounded host code; Python cannot preempt a blocked
callback. Deadline-bearing callbacks must enforce the supplied deadline including
network timeouts and disabled retries. The enrollment read must be local/bounded.
The host's enrollment service owns external handle revocation; foreground exit
retires the native scoped capability and releases its retained request graph.

The operator provider, independent reviewer/terminal-receipt services and deployment
credentials are not shipped by this fork. The older p12 ports helper cannot be
used as-is: it creates new handles on lookup and its result projection is not
explicit audience approval. Enabling the flag is insufficient without implementing
these authenticated services. Missing static services/dependencies fail startup;
unknown enrollment or review holds the individual call without fallback.

Validation uses actual gateway construction/start/stop with isolated profiles and
no platforms for the disabled cases. The enabled synthetic test uses real native
intake, scoped registry dispatch, HostBundle assembly and audit sink, with a
synthetic dispatcher outcome. Focused existing host tests cover reviewer, V2
observer and physical cleanup contracts. None of these are live provider or Mini
activation evidence.
