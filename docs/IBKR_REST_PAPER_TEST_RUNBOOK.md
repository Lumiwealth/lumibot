# IBKR REST Paper-Test Runbook

Credential-free procedure for authorized IBKR REST paper-order validation.

**Last Updated:** 2026-08-31
**Status:** Active operator runbook
**Audience:** Authorized LumiBot maintainers

## Overview

The IBKR REST paper suites are real broker API tests. The advanced-order suite
creates real paper orders; it is not a unit-test suite. Never use a production
account. These tests are marked `apitest` and `ibkr`, and normal CI excludes
them.

## Before running

- Use a dedicated IBKR paper username and paper account.
- Store credentials only in an untracked secret manager or untracked `<repo>/.env`.
- Never copy credentials, account identifiers, cookies, tokens, gateway-session
  material, or raw IBKR responses into logs, tickets, or chat.
- Close other trading-enabled sessions using the dedicated username when they
  could displace the Client Portal session.

## Location and environment

Run commands from the repository root, represented as `<repo>`, containing
`lumibot/`, `tests/`, and `setup.py`.

```powershell
cd <repo>
# If using Conda, activate your existing LumiBot test environment:
conda activate <your-lumibot-environment>
```

For pytest, use the untracked configuration file `<repo>/.env`.

## Gateway choice

### Local IBeam gateway

Use this mode only when LumiBot may manage a local IBeam container. Do not set
`IB_API_URL`. Docker must be installed and running; on Windows, start Docker
Desktop and wait until its engine is running.

The untracked configuration needs the existing settings, using only paper
values:

```dotenv
IB_USERNAME=<paper-username>
IB_PASSWORD=<paper-password>
IB_ACCOUNT_ID=<paper-account-id>
IB_USE_PAPER_ACCOUNT=true
```

LumiBot starts local IBeam and waits for Client Portal authentication and any
required 2FA. Complete authentication only for the intended paper session.

Within one pytest invocation, the paper suite reuses one authenticated gateway
and data source. Do not run these paper modules concurrently from separate
terminals. If an interrupted or older run left a `lumibot-client-portal-*`
container behind, stop and remove it in Docker Desktop before starting a new
run.

### Externally managed gateway

Use this mode only when an approved Client Portal REST gateway is already
running and authenticated to the intended paper account:

```dotenv
IB_API_URL=https://<your-paper-gateway>
IB_ACCOUNT_ID=<paper-account-id>
IB_USE_PAPER_ACCOUNT=true
```

`IB_USE_PAPER_ACCOUNT=true` does not convert an external live session into a
paper session.

## Preflight collection

This command only collects tests; it does not start a gateway or contact IBKR:

```powershell
python -m pytest --collect-only -q tests/test_ibkr_rest_advanced_orders_paper_apitest.py tests/test_ibkr_rest_gtd_paper_apitest.py
```

## GTD capability probe

Run the GTD probe before advanced submissions:

```powershell
python -m pytest -q --tb=no -r f -m "apitest and ibkr" tests/test_ibkr_rest_gtd_paper_apitest.py
```

It authenticates to the paper gateway and uses only `/orders/whatif`; it never
calls the order-submission endpoint. A rejection intentionally fails the test
with a sanitized reason. A passing probe is evidence for a separate reviewed
implementation decision; it does not enable production GTD support.

LumiBot's legacy IBKR socket adapter has separate GTD support. REST exact-date
GTD serialization remains disabled. The 2026-08-30 Sunday probe rejected the
candidate REST `tif=GTD` and `goodTillDate` request; an authorized 2026-08-31
US regular-trading-hours probe rejected the same candidate. This is current
evidence that the configured Client Portal REST API does not accept it.

## Advanced-order paper smoke suite

Run this order-submission suite only during US regular trading hours on a
market day, preferably 10:00–15:30 America/New_York. It does not enable
outside-regular-trading-hours execution. Confirm the paper gateway and account,
then run it once:

```powershell
python -m pytest -q --tb=no -r f -m "apitest and ibkr" tests/test_ibkr_rest_advanced_orders_paper_apitest.py
```

The suite uses the public LumiBot order-submission path for one-share,
deliberately non-marketable BRACKET, OTO, and OCO packages. It polls order
status and attempts cancellation in `finally` cleanup. Only masked account
suffixes may appear in test output.

## After every run

- Inspect the paper account directly and confirm no test orders remain working.
- Inspect it especially after a timeout, failure, terminal interruption, or
  computer sleep: process termination can interrupt cleanup.
- Do not interpret a passing paper run as proof of production OAuth readiness
  or authorization for production testing.

## Safety gates to expect

- Missing local credentials or an unavailable gateway skips the API test.
- `IB_USE_PAPER_ACCOUNT` must be explicitly `true` for order-changing tests.
- The authenticated selected account must follow the paper-account `DU`
  convention.
- When configured, `IB_ACCOUNT_ID` must match the authenticated selected
  account.
- A non-paper account fails before the test constructs or submits an order.
