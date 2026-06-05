# Schwab Local Token Refresh Notes

Date: 2026-06-01

## Finding

The local `schwab_token.json` file can contain a valid Schwab refresh token and
still appear to "expire" after about 30 minutes if the local Python process does
not have `SCHWAB_APP_SECRET` available. The refresh token itself is not enough;
the OAuth refresh exchange needs the Schwab app key and secret so the client can
refresh with the required auth headers.

## Verified

- `schwab_token.json` is gitignored and should remain local only.
- `schwab_token.json` is saved with file mode `600`.
- A forced-expired temp token copy successfully refreshed when the Schwab app
  secret was loaded from the local BotSpot Node env file.
- The Rob-owned Titus live-smoke helper now loads the existing local Schwab app
  values from `../botspot_node/.env-local` when they are not already exported,
  so future local live Schwab smokes should not require a new OAuth payload every
  30 minutes.

## Operational Rule

For Rob-owned local Schwab live tests, use `lumibot/schwab_token.json` plus the
local Schwab app credentials from `botspot_node/.env-local`. Do not paste or
document raw token values. If refresh fails again, first check whether the app
secret was available to the Python process before asking for a new OAuth token.

