# Alpaca Live Broker CI

One-line description: Real Alpaca paper-account CI for live broker and agent-tool smoke coverage.

Last Updated: 2026-07-03

Status: Active

Audience: Developers, AI Agents

## Overview

`.github/workflows/alpaca-live-broker.yml` runs opt-in live paper API coverage for Alpaca. It is separate from the normal `not apitest` suite because it contacts Alpaca, reads broker/data endpoints, and submits then cancels real paper orders.

The workflow runs for internal PRs/pushes and manual dispatches when relevant broker, agent, dependency, or workflow files change. Fork PRs are skipped because repository secrets are unavailable.

## Credentials

Repository secrets required:

- `ALPACA_TEST_API_KEY`
- `ALPACA_TEST_API_SECRET`

Optional repository secrets:

- `ALPACA_NEWS_API_KEY`
- `ALPACA_NEWS_API_SECRET`

The live news apitest uses `ALPACA_NEWS_API_KEY` / `ALPACA_NEWS_API_SECRET` when present and falls back to `ALPACA_TEST_API_KEY` / `ALPACA_TEST_API_SECRET` for CI/local smoke runs. The built-in `alpaca_news` tool intentionally does not read generic `ALPACA_API_KEY` values unless it is bound to an active Alpaca broker. Do not commit real key values.

## Test Coverage

`tests/test_alpaca_live_broker_apitest.py` is marked `apitest` and `alpaca`.

- `_require_alpaca()` validates paper credentials, authenticates through the real Alpaca API, fails if the paper account is trading-blocked, and configures the broker test market as `24/7` so scheduled one-shot tests run outside NYSE hours.
- `_LiveOrderDataStrategy` calls `run_live(run_once=True)`, reads AAPL last price and daily bars, submits a non-marketable AAPL limit buy, cancels it, waits for terminal cancel state, and verifies order retrieval.
- `_LiveOptionsChainStrategy` calls `run_live(run_once=True)`, reads SPY price, pulls SPY option chains through the broker data source, selects an expiration, and resolves a valid call contract.

`tests/test_agent_alpaca_news_live_apitest.py` uses Alpaca news credentials, falling back to the paper test credentials, to verify the built-in Alpaca news agent tool against real historical news pagination and full-content reads.

The workflow also runs local-only agent runtime, MCP transport, permission, provider-key, and backtest tool coverage before the live broker apitests.

## Concurrency

The live broker job has a repository-wide concurrency group. This prevents overlapping runs from using the same shared Alpaca paper account at once and avoids one run canceling or observing another run's test orders.

## Local Run

Use paper credentials only:

```bash
export ALPACA_TEST_API_KEY="..."
export ALPACA_TEST_API_SECRET="..."

python -m pytest -q \
  tests/test_agent_runtime_remote_mcp.py \
  tests/test_agent_runtime_mcp_transports.py \
  tests/test_agent_runtime_provider_keys.py \
  tests/test_agent_runtime_errors.py \
  tests/test_agent_tool_permissions.py \
  tests/test_agent_alpaca_news_builtin.py \
  tests/test_agent_alpaca_news_live_apitest.py \
  tests/backtest/test_agent_runtime_backtest.py \
  tests/backtest/test_ai_committee_builtin_tools_backtest.py

python -m pytest -q -m apitest --tb=short \
  tests/test_alpaca_live_broker_apitest.py
```

If credentials are missing, `alpaca`-marked apitests skip with the missing Alpaca variables instead of requiring unrelated Polygon or ThetaData credentials.
