# Tradier OAuth Payload + Refresh Support (LumiBot)

Date: 2026-02-07  
Owner: Codex (implementation + research)  
Status: Implemented in `lumibot` (commit `52eef70b`)

## Why this exists

BotSpot deployments can link Tradier via OAuth. Tradier OAuth access tokens expire (per Tradier docs), so a long-running bot needs refresh-token support (when available) to keep trading without requiring users to re-link every day.

## Runtime Inputs (env vars)

This implementation supports both existing “manual token” Tradier usage and OAuth usage.

### Existing (manual token)

- `TRADIER_ACCESS_TOKEN`
- `TRADIER_ACCOUNT_NUMBER`
- `TRADIER_IS_PAPER` (`true`/`false`)

### OAuth mode (BotSpot)

BotSpot injects:

- `TRADIER_TOKEN`
  - base64url JSON payload from the OAuth token exchange
  - expected fields: `access_token`, `expires_in` (optional), `issued_at` (optional), `refresh_token` (optional)
- `TRADIER_REFRESH_TOKEN` (optional)
- `TRADIER_OAUTH_CLIENT_ID` (required to refresh)
- `TRADIER_OAUTH_CLIENT_SECRET` (required to refresh)
- `BOTSPOT_TRADIER_TOKEN_ROTATION_PATH` (internal BotSpot runtime handoff path)

Notes:

- Tradier refresh tokens are **partner-only**; if `TRADIER_REFRESH_TOKEN` is not present, the bot can still start, but it may stop working once the access token expires.

## What changed

File: `lumibot/brokers/tradier.py`

- Added support for decoding `TRADIER_TOKEN` (base64url JSON).
  - If `access_token` is missing/blank in config/args, it is sourced from the decoded payload.
- Added best-effort refresh support via:
  - proactive refresh near expiry (when expiry metadata exists)
  - forced refresh and single retry when an API call fails with `401`
- Refresh uses Tradier endpoint:
  - `POST https://api.tradier.com/v1/oauth/refreshtoken`
  - Basic Auth: `TRADIER_OAUTH_CLIENT_ID:TRADIER_OAUTH_CLIENT_SECRET`
  - Body: `grant_type=refresh_token&refresh_token=<TRADIER_REFRESH_TOKEN>`
- When a refresh succeeds, the broker updates the access token across:
  - broker’s `lumiwealth_tradier` client
  - data source’s `lumiwealth_tradier` client
- When `BOTSPOT_TRADIER_TOKEN_ROTATION_PATH` is configured, a successful refresh
  also writes a 0600 JSON handoff file containing only allowlisted rotated token
  fields for BotManager's audited Vault rotation path.

## Limitations / Operational Notes

- Task environment variables are still ephemeral. Durability comes from
  BotManager reading the handoff file after the run and calling the platform's
  narrow token-rotation endpoint.
- If the handoff path is not configured, refresh remains in-process only.

## Tests

File: `tests/test_tradier.py`

- Added unit tests for:
  - decoding OAuth payload into `access_token`
  - refreshing token when payload is expired (requests mocked; no network)
  - writing the token-rotation handoff file without logging token values
