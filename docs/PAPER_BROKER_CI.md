# Paper Broker CI Gate

One-line description: Required Alpaca and Tradier paper-account coverage for live broker boundaries.

Last Updated: 2026-07-13

Status: Active

Audience: Developers, AI Agents

## Overview

The `Live Broker Gate (Alpaca + Tradier paper)` job in `.github/workflows/cicd.yaml` is part of the primary CI workflow. It fails closed when credentials or real broker behavior are unavailable, and the aggregate `LintAndTest` job cannot pass without it.

The job uses repository environment secrets, runs only against paper or sandbox accounts, and serializes all runs in one repository-wide concurrency group. Fork pull requests do not receive the secrets and therefore cannot exercise the shared accounts.

## Credentials

The `unit-tests` GitHub environment must provide:

- `ALPACA_TEST_API_KEY`
- `ALPACA_TEST_API_SECRET`
- `TRADIER_TEST_ACCOUNT_NUMBER`
- `TRADIER_TEST_ACCESS_TOKEN`

Never commit real credential values. No production broker credentials are used by this gate.

## Coverage

`tests/test_live_broker_gate.py` performs four real paper-broker checks:

- Alpaca account, position, and order reads plus submit/read/cancel of one non-marketable AAPL limit order.
- Tradier account, position, and order reads plus the same paper-only order lifecycle.
- A one-iteration Alpaca `Strategy` lifecycle that reads AAPL quotes and daily bars, reads SPY call and put chains, resolves a valid option contract, submits through the public strategy API, and cancels during strategy shutdown.
- A one-iteration Tradier `Strategy` lifecycle that reads account state, submits through the public strategy API, and cancels during strategy shutdown.

The market clock is read from each real broker before the strategy tests apply a local run-once override. This keeps weekend and overnight CI deterministic without bypassing broker authentication, data calls, order submission, order reads, cancellation, or cleanup.

The ordinary unit-test shards continue to cover local agent runtime, MCP transport, permission, provider-key, and built-in Alpaca news behavior. Those deterministic tests are not duplicated in the real-account job.

## Safety

- Both broker configurations are asserted to use paper or sandbox mode.
- Orders are one-share AAPL buy limits priced far below the market.
- Every submitted order is cancelled in normal and cleanup paths.
- The job does not start broker streams or background order threads.
- Shared paper accounts are protected by non-cancelling repository-wide concurrency.
- Missing credentials and incomplete broker behavior fail the gate instead of skipping it.

## Local Run

Use dedicated paper credentials only:

```bash
export ALPACA_TEST_API_KEY="..."
export ALPACA_TEST_API_SECRET="..."
export TRADIER_TEST_ACCOUNT_NUMBER="..."
export TRADIER_TEST_ACCESS_TOKEN="..."

python -m tests.test_live_broker_gate
```

The direct module invocation is intentional: it runs only this fail-closed gate and avoids unrelated legacy `apitest` credential requirements.
