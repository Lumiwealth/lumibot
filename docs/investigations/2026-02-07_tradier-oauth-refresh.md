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

## Limitations / Operational Notes

- If Tradier rotates the refresh token on refresh, the new refresh token cannot be persisted back into task env vars.
  - The code logs a warning if refresh-token rotation is detected.
  - If rotation happens, users may need to re-link after the old refresh token becomes invalid.

## 2026-06-18 Addendum: Public Token File Contract

The limitation above was not an acceptable endpoint for OAuth support. Tradier now has a public provider-token file path so OAuth refresh can be durable without exposing BotSpot-specific runtime artifacts in LumiBot's public API.

Implemented public LumiBot contract:

- Support `TRADIER_TOKEN_PATH`.
- Load a provider token payload from that file.
- Accept access token, refresh token, expiry metadata, and provider/client metadata needed for refresh.
- Refresh on expiry or after a recoverable auth failure when a refresh token and OAuth client credentials are present.
- Atomically write the updated provider token payload back to the same file.
- Fail the refresh if the updated provider token file cannot be written.
- Keep manual `TRADIER_ACCESS_TOKEN` mode working for existing API-token users.
- Do not expose BotSpot, Bot Manager, Vault, broker credential ids, token rotation grants, or managed-runtime artifact paths in the public API/docs.

BotSpot can privately map this token file to Node/Vault writeback inside Bot Manager, but LumiBot should only care about provider token semantics.

Proof required before calling this fixed:

1. Create or receive a real Tradier OAuth token file with a refresh token.
2. Force or simulate access-token expiry.
3. Run a read-only Tradier broker call.
4. Verify refresh succeeds, the in-memory access token updates, and the token file changes.
5. Restart from the changed token file and verify the next read-only call succeeds without relinking.

## Tests

File: `tests/test_tradier.py`

- Added unit tests for:
  - decoding OAuth payload into `access_token`
  - refreshing token when payload is expired (requests mocked; no network)
- 2026-06-18 follow-up tests cover `TRADIER_TOKEN_PATH` forced refresh writing the updated token file and failing loudly if the token file cannot be written.
- 2026-06-18 Schwab follow-up tests cover forced refresh failing loudly if the refreshed Schwab token file cannot be written while preserving the old valid token file.
