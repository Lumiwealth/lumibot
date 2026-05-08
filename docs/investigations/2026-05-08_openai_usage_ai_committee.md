# OpenAI Usage During AI Committee Testing

**Date:** 2026-05-08  
**Scope:** OpenAI Platform dashboard export for the BotSpot project, 2026-04-23 through 2026-05-08.

## Dashboard Totals

- Total spend: `$191.2255959`
- May 7 spend: `$45.81722739`
- May 8 spend: `$42.92013214`
- May 7 plus May 8 spend: `$88.73735953`
- Total requests: `4,114`
- Input tokens: `115,216,472`
- Cached input tokens: `67,973,760`
- Uncached input tokens: `47,242,712`
- Output tokens: `3,667,523`
- Total tokens: `118,883,995`

The local AI committee artifact estimate was only `$2.3363` across `39` recorded successful calls. That estimate is not reliable as account-level spend. The dashboard showed hundreds of GPT-5.5 requests during the same time windows, all attributed to the shared BotSpot AI key.

## Main Cost Drivers

- `gpt-5.4-2026-03-05` input: `$54.706770`
- `gpt-5.5-2026-04-23` input: `$43.992640`
- `gpt-5.5-2026-04-23` output: `$30.336330`
- `gpt-5.1-codex-max` output: `$18.619740`
- `gpt-5.1-codex-max` input: `$9.611358`
- `gpt-5.4-2026-03-05` cached input: `$8.604064`
- `gpt-5.5-2026-04-23` cached input: `$8.226496`

May 7 plus May 8 GPT-5.5 alone was approximately `$74.257557`.

## Committee Result Evidence

Final usable partial GPT committee artifact:

`/Users/robertgrzesik/Development/lumibot/artifacts/ai_committee_real_backtests/20260507_215455`

It did not finish the full month because the OpenAI account hit quota/billing limits. The completed partial period was 2026-03-30 through 2026-04-20 09:30 ET.

- Portfolio value: `$10,164.11172543335`
- Partial return: `+1.6411172543334906%`
- Max drawdown: `-0.358629096716101%`
- Positions at stop: `6 AAPL`, `3 GOOGL`

Do not treat this as a final benchmark. It is promising, but not comparable to the full-month BotSpot results until we can run the same full window with isolated attribution and hard spend caps.

## Safety Follow-Up

Implemented safeguards:

- `LUMIBOT_AGENT_MAX_MODEL_CALLS`: hard cap uncached model calls in a strategy run.
- `LUMIBOT_AGENT_MAX_RUN_ATTEMPTS`: configurable retry budget. Backtests default to `2` attempts instead of the previous broad retry behavior.
- `scripts/run_ai_committee_real_backtest.py` now requires `LUMIBOT_ALLOW_PAID_AI_COMMITTEE_BACKTEST=1` before making paid model calls.

Next real benchmark should use either a dedicated API key/project or provider-side attribution metadata before spending more money.
