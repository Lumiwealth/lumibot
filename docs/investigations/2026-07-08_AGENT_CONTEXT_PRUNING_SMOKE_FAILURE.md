# AI Agent Context Pruning Smoke Failure

One-line description: Records the BotSpot Ray/Citadel data-on smoke blocker caused by LumiBot agent context pruning expanding request payloads.

Last Updated: 2026-07-08

Status: Fixed locally in LumiBot; requires deploy before BotSpot long backtests should resume.

Audience: LumiBot release agents, BotSpot strategy/backtest agents, Bot Manager runtime investigators.

## Overview

BotSpot data-on strategy smokes for the Ray Dalio and Citadel AI trading teams were gated on proving that FRED and Alpaca News work before launching one-year backtests. The first fresh Ray regular data-on smoke proved both data integrations worked inside the ECS backtest runtime, but it stalled before the trader/order step because LumiBot's ADK context-pruning callback repeatedly expanded or failed to shrink the serialized model request.

Do not treat this incident as an Alpaca credential failure. The credential path was separately verified locally and in ECS.

## Evidence

- Local Alpaca News credentials returned HTTP 200 against the Alpaca News API. Generic Alpaca broker environment variables were intentionally not used for news.
- A temporary BotSpot diagnostic strategy verified runtime injection inside ECS: `ALPACA_NEWS_DIAGNOSTIC status=200 count=20`.
- The Ray regular data-on smoke over `2026-05-26` to `2026-06-02` showed FRED tool results with `source=fred` and Alpaca News results with returned headlines.
- The same smoke had no `401`, `Unauthorized`, `API_KEY_INVALID`, `ORDER_READINESS_REQUIRED`, `Traceback`, or `ContextWindowExceededError` matches at the time it was stopped.
- Runtime logs showed repeated pruning warnings such as request chars growing from about `70072` to `75033` against a budget of `52428`, while the strategy remained before the trader/order step.

The smoke was force-stopped to avoid wasting backtest runtime and model credits. The remaining Ray/Citadel smokes and all one-year promotions should wait until the LumiBot fix is deployed to the BotSpot backtest runtime.

## Root Cause

`lumibot/components/agents/runtime.py` had two related issues:

1. The pruning helper replaced older function-response payloads without checking whether the replacement notice was actually shorter than the original payload. Compact tool results such as last-price responses could therefore be replaced with a larger pruning notice.
2. The default serialized-character budget was calculated as `context_limit_tokens * 0.05`. For Gemini 3.1's roughly one-million-token registry entry, this produced a budget around `52k` JSON characters, causing pruning far below the real model window.

Together, these made pruning fire too early and sometimes increase the request size, which can trap an agent before it reaches the final trader step.

## Fix

The local LumiBot fix:

- skips pruning a function response when the replacement would not shrink the serialized payload;
- skips function responses already marked as `lumibot_context_pruned`;
- uses a realistic serialized-character budget derived from the model context registry instead of pruning at five percent of the token limit.

Regression coverage was added in `tests/test_agent_deepseek_usage_accounting.py` for:

- not expanding small tool results;
- not repeatedly pruning already-pruned results;
- not pruning moderate Gemini 3.1 request payloads far below the model's real context window.

Focused verification run:

```bash
/Users/robertgrzesik/bin/safe-timeout 600s python3 -m pytest tests/test_agent_deepseek_usage_accounting.py tests/test_agent_runtime_provider_keys.py
```

Result: `38 passed`.

## Follow-Up

After this LumiBot version is deployed to the BotSpot backtest runtime:

1. Rerun only the cheap Ray regular data-on smoke first over `2026-05-26` to `2026-06-02`.
2. Confirm FRED and Alpaca News still work and that agent_detail/logs show the trader step and real filled orders.
3. Only then smoke the other three separate strategies:
   - Ray Dalio Idea Meritocracy - Leveraged Data-On
   - Citadel Sector Pods - Regular Data-On
   - Citadel Sector Pods - Leveraged Data-On
4. Promote passing smokes to one-year backtests over `2025-06-24` to `2026-06-24`.

No paper deployment should happen until a final strategy has a clean one-year result, acceptable drawdown, real fills, FRED success, Alpaca News success, and agent-detail evidence.
