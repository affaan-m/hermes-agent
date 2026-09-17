# Ito Delta

The Ito delta is the set of changes that turns stock Hermes into the production
Ito desk gateway. It lives as branch `ito` on the fork
(github.com/affaan-m/hermes-agent). The upstream base is commit `1c4cc00f`
(tag `ito-upstream-base` on the Hermes remote). Everything outside the allowed
surface below belongs to upstream and must not drift on `ito`.

## Allowed surface

Only these paths may differ from the upstream base:

- `cron/scheduler.py`
- `gateway/run.py`
- `gateway/dispatch.py`
- `gateway/platforms/base.py`
- `plugins/platforms/slack/adapter.py`
- `plugins/temporal_knowledge/`
- `hermes_cli/web_server.py` and `web/`
- `tools/send_message_tool.py`

Any other path changed on `ito` is a defect: rebase it out or move it to one
of the paths above before pushing.

## Profile-side plugins (outside this repo)

Three plugins are deployed through the Hermes profile, not the repo tree. They
live only under `~/.hermes/profiles/ito/plugins/` on the mini:

- `desk-approval`
- `desk-channel-guard`
- `desk-tools`

Editing them is a Hermes profile change and requires explicit operator
approval; they are never installed by a repo commit.

## Install and restart procedure

The production gateway runs from the live checkout `~/.hermes/hermes-agent`
(branch `ito`) under launchd label `ai.hermes.gateway-ito`.

1. Land the reviewed change on fork branch `ito` (PR, never a direct push to
   the live checkout's remote).
2. On the mini, fast-forward the live checkout to the new `ito` head. Do this
   only in a coordinated window: the checkout is the running code.
3. Restart the gateway:

   ```sh
   launchctl kickstart -k gui/$(id -u)/ai.hermes.gateway-ito
   ```

4. Verify the process is back and healthy before closing the window.

Never check out, reset, stash, rebase or clean the live checkout while the
gateway is up, and never restart the gateway outside a coordinated window.
