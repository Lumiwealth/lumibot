# External OAuth Managed-Child Validation - 2026-07-02

## Scope

This note covers the LumiBot-only fix for BotSpot-managed Schwab and Tradier
OAuth deployments. The goal is that a managed child strategy process can run
with `LUMIBOT_OAUTH_REFRESH_MODE=external`, read only access-token files, reload
atomic parent file replacements, and never receive or retain refresh-token
material.

Release branch: `version/4.5.64`
Fix commit: `4d68831dd573a5ff23d063d34f69137f12eeea0f`

## What Changed

- Schwab external OAuth mode strips refresh-token fields from in-memory token
  payloads before creating the OAuth session and on every token-file reload.
- Schwab external OAuth mode no longer shows the missing app-secret auto-refresh
  warning, because the child is not supposed to refresh provider tokens.
- Schwab external OAuth mode no longer deletes the externally managed token file
  after a client-init failure; parent-managed files should not be removed by the
  child runtime.
- Tradier external OAuth mode strips refresh-token fields from token-file payloads
  and clears `_oauth_refresh_token` in child broker state.
- Provider docs and the changelog now describe external mode as an access-token
  only managed-child contract.

## Local Test Evidence

Passed:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 180 python3 -m pytest tests/test_broker_initialization.py -k 'schwab'
```

Result: `6 passed, 2 deselected, 1 warning`.

Passed:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 180 python3 -m pytest tests/test_tradier_oauth_refresh.py tests/test_tradier_force_refresh.py
```

Result: `13 passed, 1 warning`.

The tests include:

- Schwab external mode strips stale refresh-token material.
- Schwab external mode reloads an atomically replaced access-token file after an
  auth-expiry path without calling provider refresh.
- Schwab external mode supports multiple child broker instances sharing one
  externally replaced token file across repeated replacements.
- Tradier external mode strips stale refresh-token material.
- Tradier external mode retries after an auth error by reloading an access-token
  file and never calling provider refresh.
- Tradier external mode supports multiple child broker instances sharing one
  externally replaced token file across repeated replacements.

## Live Harness Evidence

Existing harness:

```bash
/Users/robertgrzesik/Development/bot_manager/scripts/oauth_parent_refresh_harness.py
```

The Tradier half passed with saved local token material:

```text
tradier_parent_external_reload=pass account=****2015
run_dir=/Users/robertgrzesik/.config/botspot/oauth-test-tokens/tradier-only-parent-harness-20260702T052032Z
```

The Schwab half did not reach LumiBot child reload because every saved local
Schwab refresh-token snapshot was rejected by Schwab with HTTP 400 during the
parent refresh call. This points to stale/invalid local Schwab refresh-token
material, not the child reload path. A fresh Schwab external login is required
before completing the live Schwab parent/child harness proof.

## Deployment Handoff

The LumiBot deploy agent should release from `version/4.5.64` at commit
`4d68831dd573a5ff23d063d34f69137f12eeea0f` or a later commit that includes it,
following `docs/DEPLOYMENT.md`.

After the LumiBot package release, Bot Manager dev validation should rebuild
against the released LumiBot version before testing managed Schwab/Tradier
deployments. Do not treat Bot Manager dev validation as meaningful if the dev
image still has an older LumiBot package.

The remaining external proof before widening rollout is:

- Fresh Schwab local login, then rerun the parent/child live harness.
- Bot Manager dev managed-run proof for Schwab always-on through access-token
  replacement.
- Bot Manager dev managed-run proof for multiple Schwab child deployments on the
  same broker authorization.
- Bot Manager dev managed-run proof for Tradier multi-account authorization and
  access-token replacement.
