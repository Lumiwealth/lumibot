# AI Committee Provider Benchmark Plan

**Date:** 2026-05-20
**Scope:** Benchmarking the LumiBot AI Investment Committee across Gemini, OpenAI, Together AI, Kimi, Qwen, Cerebras, and optional direct DeepSeek models.

## Recommendation

Use the AI Investment Committee example as the primary benchmark. It is the right workload because it stresses the exact behavior we care about:

- multi-agent handoff quality
- built-in LumiBot tool calling
- SEC/FRED/news/indicator usage
- order placement through the portfolio-manager agent
- backtest speed, cost, and trading result quality

Do not start with a full two or three month run for every model. Run the benchmark in stages:

1. Single-day smoke test for every candidate model.
2. Fourteen-trading-day comparison for models that pass the smoke test.
3. Two or three month comparison only for finalists.

This prevents wasting spend on models that fail function calling or produce useless committee behavior.

## Existing Benchmark Shape

The best prior real committee artifact is:

`/Users/robertgrzesik/Development/lumibot/artifacts/ai_committee_real_backtests/20260507_215455`

That partial run covered 14 trading days and produced:

- Model calls: `56`
- Input tokens: `1,872,570`
- Output tokens: `143,048`
- Cached input tokens recorded: `1,526,784`
- Tool calls: `1,522`
- Final partial return: `+1.6411172543334906%`
- Final positions: `6 AAPL`, `3 GOOGL`

Scale factors from this profile:

- 1 month / 14 trading days: roughly `1.5x`
- 2 months / 14 trading days: roughly `3.0x`
- 3 months / 14 trading days: roughly `4.5x`

## Existing Code Entry Points

- Strategy: `/Users/robertgrzesik/Development/lumibot/lumibot/example_strategies/ai_investment_committee.py`
- Real paid runner: `/Users/robertgrzesik/Development/lumibot/scripts/run_ai_committee_real_backtest.py`
- Provider benchmark runner: `/Users/robertgrzesik/Development/lumibot/scripts/run_ai_committee_provider_benchmark.py`
- Tool-smoke runner: `/Users/robertgrzesik/Development/lumibot/scripts/run_ai_committee_smoke_backtest.py`
- Prior cost incident note: `/Users/robertgrzesik/Development/lumibot/docs/investigations/2026-05-08_openai_usage_ai_committee.md`

The committee currently supports four separate model env vars:

- `COMMITTEE_RESEARCH_MODEL`
- `COMMITTEE_BULL_MODEL`
- `COMMITTEE_BEAR_MODEL`
- `COMMITTEE_TRADER_MODEL`

For fair model comparison, set all four to the same candidate model first. Mixed committees can be tested after single-model runs identify winners.

## Candidate Models

### Required Baselines

| Candidate | Model string | Why test |
|---|---|---|
| Current OpenAI mini baseline | `openai/gpt-5.4-mini` | Existing cheaper OpenAI baseline and easy cost comparator. |
| Current strong OpenAI baseline | `openai/gpt-5.5` | Prior committee used GPT-5.5 for bull/bear/trader. Expensive but useful quality anchor. |
| Gemini 3.5 Flash | `gemini-3.5-flash` | New May 2026 Google model. Google positions it as fast frontier-level agentic model with function calling and strong finance-agent evals. |

### Together AI

| Candidate | Model string | Why test |
|---|---|---|
| Qwen3 235B FP8 throughput | `together_ai/Qwen/Qwen3-235B-A22B-Instruct-2507-tput` | Very cheap throughput model. Good candidate for "can a low-cost open model actually trade?" |
| GPT-OSS 120B on Together | `together_ai/openai/gpt-oss-120b` | Open-weight reasoning baseline via Together. Low cost, tool support listed by Together. |
| Qwen3.6 Plus | `together_ai/Qwen/Qwen3.6-Plus` | Cheaper broad reasoning candidate. Test only if it passes tool-call smoke. |
| Kimi K2.6 | `together_ai/moonshotai/Kimi-K2.6` | Agentic/model-swarm positioning, 256K context, function calling listed by Together. Expensive enough that it should not be in the first cost-sensitive finalist set unless smoke quality is clearly strong. |
| Together DeepSeek V4 Pro | `together_ai/deepseek-ai/DeepSeek-V4-Pro` | Together-hosted DeepSeek option. More expensive than direct DeepSeek and no documented Together V4 Flash option was found, but it avoids sending requests to `api.deepseek.com`. Requires `TOGETHERAI_API_KEY`. |

### Cerebras

| Candidate | Model string | Why test |
|---|---|---|
| GPT-OSS 120B on Cerebras | `cerebras/gpt-oss-120b` | Best first Cerebras candidate: production model, 120B, official catalog lists about 3000 tokens/sec. |
| Z.ai GLM 4.7 on Cerebras | `cerebras/zai-glm-4.7` | Preview but probably the smartest public Cerebras-hosted model for coding/tool agents. Official catalog lists about 1000 tokens/sec. |

Skip `cerebras/llama3.1-8b` for committee quality unless the goal is pure speed sanity testing. It is very fast but likely too weak for this benchmark. Also note Cerebras says `llama3.1-8b` and `qwen-3-235b-a22b-instruct-2507` are scheduled for deprecation on 2026-05-27.

### Optional Direct DeepSeek

| Candidate | Model string | Why not default |
|---|---|---|
| DeepSeek V4 Flash direct | `deepseek/deepseek-v4-flash` | Cheapest and likely fastest DeepSeek option, but it sends requests to DeepSeek's own API endpoint. That is a privacy concern for trading data. Treat as optional Lumibot support, not the default benchmark path for proprietary trading work. |
| DeepSeek V4 Pro direct | `deepseek/deepseek-v4-pro` | Stronger direct DeepSeek option. Currently discounted through 2026-05-31 per official docs, but same privacy concern as direct Flash. |

Direct DeepSeek is useful for an open-source LumiBot feature because some users will want it. For our own trading benchmark, prefer Together/Cerebras/Gemini/OpenAI unless Rob explicitly accepts the privacy trade-off for a direct DeepSeek run.

## Cost Estimates

The following estimates use the prior 14-trading-day committee token profile:

- Input: `1.872570M`
- Output: `0.143048M`
- Cached input recorded: `1.526784M`

Cache-adjusted costs are directional only. Provider-specific caching behavior differs, and LumiBot does not yet normalize provider-side cache reporting across all providers.

| Candidate | 14-day no-cache estimate | 14-day cache-adjusted estimate | 3-month no-cache estimate | 3-month cache-adjusted estimate |
|---|---:|---:|---:|---:|
| Together GPT-OSS 120B | `$0.37` | n/a | `$1.65` | n/a |
| Together Qwen3 235B throughput | `$0.46` | n/a | `$2.07` | n/a |
| Gemini 3.1 Flash-Lite | `$0.68` | `$0.34` | `$3.07` | `$1.53` |
| Cerebras GPT-OSS 120B | `$0.76` | n/a | `$3.43` | n/a |
| Cerebras Qwen 3 235B preview | `$1.30` | n/a | `$5.83` | n/a |
| Together Kimi K2.5 | `$1.34` | n/a | `$6.02` | n/a |
| Gemini 3 Flash Preview standard | `$1.37` | `$0.68` | `$6.14` | `$3.05` |
| Together Qwen3.6 Plus | `$1.37` | n/a | `$6.14` | n/a |
| GPT-5.4 mini | `$2.05` | `$1.02` | `$9.22` | `$4.58` |
| Together Kimi K2.6 | `$2.89` | `$1.36` | `$13.01` | `$6.14` |
| Together DeepSeek V4 Pro | `$4.56` | `$1.66` | `$20.53` | `$7.47` |
| Cerebras ZAI GLM 4.7 preview | `$4.61` | n/a | `$20.73` | n/a |
| Gemini 3.5 Flash standard | `$7.37` | `$3.66` | `$33.18` | `$16.48` |
| Direct DeepSeek V4 Flash (optional/privacy-sensitive) | `$0.30` | `$0.09` | `$1.36` | `$0.42` |
| Direct DeepSeek V4 Pro promo (optional/privacy-sensitive) | `$0.94` | `$0.28` | `$4.23` | `$1.26` |
| Direct DeepSeek V4 Pro list (optional/privacy-sensitive) | `$3.76` | `$1.12` | `$16.90` | `$5.05` |

Cerebras prices found in the current pricing page data:

- ZAI GLM 4.7: `~1000 tokens/s`, `$2.25/M input`, `$2.75/M output`.
- GPT OSS 120B: `~3000 tokens/s`, `$0.35/M input`, `$0.75/M output`.
- Llama 3.1 8B: `~2200 tokens/s`, `$0.10/M input`, `$0.10/M output`; not recommended for committee quality.
- Qwen 3 235B Instruct: `~1400 tokens/s`, `$0.60/M input`, `$1.20/M output`; preview/deprecation-sensitive, so not a primary pick.

Direct DeepSeek official pricing notes:

- `deepseek-v4-flash`: `$0.14/M cache-miss input`, `$0.0028/M cache-hit input`, `$0.28/M output`.
- `deepseek-v4-pro`: list is `$1.74/M cache-miss input`, `$0.0145/M cache-hit input`, `$3.48/M output`.
- `deepseek-v4-pro` promo is 75% off through 2026-05-31 15:59 UTC, making it `$0.435/M cache-miss input`, `$0.003625/M cache-hit input`, `$0.87/M output`.

Together DeepSeek V4 Pro costs more than direct DeepSeek V4 Pro at list and much more than direct DeepSeek V4 Pro during the promo. The reason to use Together is privacy/vendor posture and operational simplicity, not lower cost.

## Sources Checked

- Google Gemini 3.5 announcement: https://blog.google/innovation-and-ai/models-and-research/gemini-models/gemini-3-5/
- Google Gemini API pricing: https://ai.google.dev/gemini-api/docs/pricing
- Google Gemini models: https://ai.google.dev/gemini-api/docs/models
- Google DeepMind Gemini 3.5 Flash model card: https://deepmind.google/models/model-cards/gemini-3-5-flash/
- DeepSeek official pricing: https://api-docs.deepseek.com/quick_start/pricing/
- DeepSeek V4 release note: https://api-docs.deepseek.com/news/news260424
- LiteLLM DeepSeek provider: https://docs.litellm.ai/docs/providers/deepseek
- Together pricing: https://www.together.ai/pricing
- Together serverless models: https://docs.together.ai/docs/serverless/models
- Together Kimi K2.6 model page: https://www.together.ai/models/kimi-k26
- LiteLLM Together provider: https://docs.litellm.ai/docs/providers/togetherai
- Cerebras model catalog: https://inference-docs.cerebras.ai/models/overview
- Cerebras pricing page: https://www.cerebras.ai/pricing
- Cerebras inference page: https://www.cerebras.ai/inference
- LiteLLM Cerebras provider: https://docs.litellm.ai/docs/providers/cerebras
- OpenAI GPT-5.4 mini docs/pricing: https://developers.openai.com/api/docs/models/gpt-5.4-mini

## Execution Plan

### Phase 1: Thin Provider Support

Add small runtime polish before paid tests:

- Done in commit after this plan: add `TOGETHER_API_KEY` / `TOGETHERAI_API_KEY` alias handling.
- Done in commit after this plan: add provider hints in backtest crash banners for `deepseek/`, `together_ai/`, and `cerebras/`.
- Done in commit after this plan: add unit tests that `deepseek/`, `together_ai/`, and `cerebras/` model strings route through `LiteLlm`.
- Done in commit after this plan: add `/Users/robertgrzesik/Development/lumibot/scripts/run_ai_committee_provider_benchmark.py`, a dedicated benchmark runner that records model, rates used, elapsed wall time, token usage, tool calls, trades/positions, backtest result, artifact paths, and error class.

### Phase 2: One-Day Smoke

Run every candidate over one trading day with `LUMIBOT_AGENT_MAX_MODEL_CALLS=8`.

Pass/fail criteria:

- Every committee role completes.
- At least one read-only built-in tool is called by research.
- Portfolio manager checks positions/cash/open orders before deciding.
- Any order submitted respects max position limits.
- Trace files and usage rows are written.

### Phase 3: Fourteen Trading Days

Run candidates that pass Phase 2 over the same prior 14-trading-day window. Use all-four-roles same model to keep comparison clean.

Primary metrics:

- Total wall-clock runtime.
- First-event latency and total latency per agent call.
- Tokens and cost by agent role.
- Tool calls by agent role and tool category.
- Backtest return, drawdown, trades, and final positions.
- Qualitative evidence quality: did it use SEC/FRED/news/indicators, or did it hallucinate?

### Phase 4: Two or Three Month Finalists

Only run the finalists for two or three months. Recommended finalists likely:

- `gemini-3.5-flash`
- `openai/gpt-5.4-mini`
- `together_ai/Qwen/Qwen3-235B-A22B-Instruct-2507-tput` if tool-calling quality is acceptable
- `together_ai/moonshotai/Kimi-K2.6` only if smoke quality justifies the higher cost
- `cerebras/gpt-oss-120b`
- `cerebras/zai-glm-4.7` if the one-day smoke is good
- Optional privacy-sensitive comparison only if explicitly approved: `deepseek/deepseek-v4-flash` and `deepseek/deepseek-v4-pro`

## Expected Findings

- Together-hosted DeepSeek V4 Pro is not a cost winner. It is more expensive than direct DeepSeek V4 Pro and far more expensive than direct DeepSeek V4 Flash. Use it only if we specifically want DeepSeek behavior without calling DeepSeek's own API endpoint.
- Direct DeepSeek V4 Flash is the best raw cost bet, but it has a privacy posture Rob does not like for proprietary trading data. Keep it optional.
- Gemini 3.5 Flash is the likely closed-model quality/speed baseline. Google published strong tool-use and finance-agent model-card numbers, so it belongs in the benchmark.
- Kimi K2.6 is worth a smoke test because it is explicitly marketed as an agentic model and Together lists function calling, but the current cost is high enough that it should not be a default finalist unless quality is clearly better.
- Cerebras is worth testing for speed, but use `gpt-oss-120b` or `zai-glm-4.7`, not small Llama, if the goal is committee quality.
- Qwen throughput is cheap enough to smoke early. Keep it only if tool discipline is acceptable.

## API Keys To Get

Get these:

- `GOOGLE_API_KEY` or `GEMINI_API_KEY` for Gemini 3.5 Flash.
- `TOGETHER_API_KEY` for Kimi, Qwen, GPT-OSS, and Together-hosted DeepSeek.
- `CEREBRAS_API_KEY` for fast GPT-OSS 120B and GLM 4.7 runs.
- Optional only: `DEEPSEEK_API_KEY` for direct DeepSeek V4 Flash and V4 Pro if Rob explicitly accepts the privacy trade-off. Together currently documents DeepSeek V4 Pro, not DeepSeek V4 Flash, so direct DeepSeek is the clean path for Flash but is not the preferred default for private trading data.

## Open Implementation Questions

- Whether Google ADK accepts `gemini-3.5-flash` immediately in the installed SDK version, or whether `google-adk` / `google-genai` need upgrades.
- Whether DeepSeek V4 thinking-mode parameters need explicit LiteLLM kwargs for fair Flash vs Pro runs.
- Whether Cerebras `zai-glm-4.7` accepts the current ADK/LiteLLM tool schema without stricter validation changes.
- Whether to cap output tokens lower for benchmark comparability. The current runtime allows up to `65535` output tokens, which is useful but can distort cost and latency across reasoning-heavy providers.
