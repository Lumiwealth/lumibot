# google-adk 2.9.1 upgrade: test receipt

Date: 2026-09-18. Branch `version/4.5.92`. Commits `35bad12f`, `0d7ae1b2`.

## What changed

```
google-adk[extensions]  >=2.1.0,<3.0.0      ->  >=2.9.1,<3.0.0
google-genai            >=1.72.0,<2.0.0     ->  >=2.24.0,<3.0.0
litellm                 >=1.83.7,<=1.83.14  ->  >=1.101.0
```

adk 2.9.1 requires `google-genai>=2.19` and `litellm>=1.84`, so this is a
three-package move, not a single bump.

The litellm ceiling was introduced by `27551d4e` ("Upgrade agents to ADK 2",
2026-05-20), which changed an open `>=1.77.0` into `<=1.83.14`. Nothing in that
commit or its tests points at a litellm defect, so it read as pin-to-what-I-
tested rather than protection against a known break. Removed.

## How it was verified

Two environments, same checkout. Current is the machine's Python 3.11
(adk 2.0.0, genai 1.75.0, litellm 1.83.14). Upgrade is an isolated venv at
`/tmp/adk29venv` (adk 2.9.1, genai 2.24.0, litellm 1.101.0).

**Targeted: everything touching agents, models and providers.**

```
pytest tests/ -q -p no:randomly -k "(agent or provider or model or llm or gemini
  or litellm or openai or grok or anthropic) and not apitest and not live
  and not polygon and not thetadata"

current   345 passed, 2 skipped   226.10s
upgrade   345 passed, 2 skipped   231.65s
new failures caused by the upgrade: none
```

This selection covers the LiteLLM path that OpenAI, Grok and Anthropic run
through, plus `test_agent_runtime_provider_keys.py` and
`test_managed_ai_provider_contract.py`.

**Full non-network suite, both environments.**

```
pytest tests/ -q -p no:randomly -k "not apitest and not live and not polygon
  and not thetadata"

current   6 failed, 2614 passed, 17 skipped   1:26:05
upgrade   4 failed, 2616 passed, 17 skipped   1:25:17
new failures caused by the upgrade: none
```

## Reading the 2-test difference honestly

It is not an upgrade effect. Both are accounted for:

1. `test_growth_entrypoints.py::test_traditional_strategies_are_first_screen_and_start_here_choices`
   was broken by an unrelated README rewrite earlier the same day and fixed in
   `6f64fde1` between the two runs. Nothing to do with adk.
2. `test_transient_network_logs_are_not_errors.py::test_update_broker_balances_exception_logs_info`
   was re-run three times on each environment in isolation and passed 3/3 on
   both. It is flaky under full-suite load, not fixed by the upgrade.

## Failures that remain, all pre-existing

```
tests/backtest/test_acceptance_backtests_ci.py::test_acceptance_backdoor_butterfly
tests/backtest/test_acceptance_backtests_ci.py::test_acceptance_backdoor_smartlimit
tests/backtest/test_acceptance_backtests_ci.py::test_acceptance_spx_short_straddle
tests/test_alpaca.py::TestAlpacaBroker::test_initialize_broker_legacy
```

These fail on both environments and are unrelated to the dependency change.
They are worth their own investigation and are not tracked here.

## Not covered

Tests excluded by the selection: `apitest`, `live`, `polygon`, `thetadata`.
Those need network and credentials. Nothing in this receipt speaks to them.
