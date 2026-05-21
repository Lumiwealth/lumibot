# AI Committee Provider Benchmark Plan

**Date:** 2026-05-20
**Scope:** Benchmarking the LumiBot AI Investment Committee across Gemini, OpenAI, Together AI, Kimi, Qwen, Cerebras, and optional direct DeepSeek models.

## Correction: Hidden Safety Rails Invalidated The Enforced Results

The enforced 14-day and three-month benchmark results below are not valid
trading-performance evidence. They used hidden behavior controls that changed
the thing being measured:

- Runtime tool-call budgets returned budget-exceeded payloads instead of
  executing additional tools. This blocked `orders_submit_order`, so models
  that tried to trade could be forced into all-cash results.
- Prompt-level numeric tool-call budgets changed the agent behavior under test.
  Benchmark prompts may ask agents to be concise and targeted, but they should
  not impose arbitrary tool counts unless the experiment is explicitly about
  constrained agents.
- Handoff/tool-result truncation changed the evidence available to downstream
  agents. Context problems should be handled with narrower tools, structured
  outputs, provider-appropriate model selection, or clear diagnostic failures,
  not hidden middle truncation.
- The cost summaries undercounted usage because the compact summary read a
  last-writer agent detail artifact instead of aggregating every committee role
  from raw traces or a combined all-agent detail file.

Before spending on another full benchmark, remove those hidden controls, fix
usage aggregation, and rerun a very small smoke test that verifies order tools
execute normally.

## Post-Correction Smoke: 2026-05-20 Local

Fixes applied before rerunning:

- Removed runtime tool-call budget enforcement from the agent runtime.
- Removed prompt-level numeric tool-call budgets from the AI Investment
  Committee example and benchmark runner.
- Removed handoff/tool-result truncation from the benchmark path.
- Fixed benchmark usage accounting to aggregate raw trace files across every
  committee role instead of trusting a last-writer detail artifact.

Validation:

- Focused tests passed: `python3 -m pytest tests/test_agent_runtime_provider_keys.py tests/test_ai_investment_committee_example.py`.
- Raw-trace usage aggregation was checked against an old OpenAI artifact and
  correctly found `248` traces across `evidence_researcher`,
  `bull_researcher`, `bear_researcher`, and `portfolio_manager`, instead of
  the broken `62`-call last-agent summary.

Paid smoke attempts:

- Together Kimi K2.5 could not be rerun because Together returned
  `Credit limit exceeded` before the first model call. Kimi K2.5 is now a
  historical artifact only; do not include it in new benchmark slates.
- After Rob added Together credits, a one-day Qwen3 235B throughput smoke was
  attempted with the historical `--max-model-calls 4` retry flag, but Together still returned
  `Credit limit exceeded` before the first model call. Do not retry repeatedly;
  Together's billing message says balances can take up to five minutes to
  update.
- Direct DeepSeek V4 Flash was rerun over `2026-02-12` through `2026-02-14`,
  a small window that previously had blocked order attempts.
- Artifact root:
  `<repo_root>/artifacts/ai_committee_provider_benchmarks/20260520_204010/deepseek_deepseek-v4-flash`.
- Result: passed mechanically; `8` raw traces, `315` tool calls,
  `3,584,209` input tokens, `3,212,672` cached input tokens, `73,136` output
  tokens, `30,119` thinking tokens.
- Estimated cost using static price map: `$0.522267` no-cache,
  `$0.081489` cache-adjusted.
- Trading result: still `0%` return and cash-only, but this was a model
  decision, not a budget block. Portfolio-manager traces for both days say
  `NO TRADE`; no `orders_submit_order` call was blocked.

Next Together smoke should use a cheaper current model first:
`together_ai/Qwen/Qwen3-235B-A22B-Instruct-2507-tput` or
`together_ai/openai/gpt-oss-120b`. Kimi should mean Kimi K2.6 only, and only as
an expensive compatibility/quality sample, not as a cost-sensitive benchmark
default.

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
| Qwen3.6 Plus | `together_ai/Qwen/Qwen3.6-Plus` | Cheaper broad reasoning candidate, but Together's current table does not clearly list function-calling support for it. Test only if a tool-call smoke confirms it works. |
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

Pricing sources checked on 2026-05-20:

- OpenAI API pricing: `https://openai.com/api/pricing/`
- Google Gemini API pricing: `https://ai.google.dev/gemini-api/docs/pricing`
- DeepSeek model pricing: `https://api-docs.deepseek.com/quick_start/pricing`
- Together serverless model catalog: `https://docs.together.ai/docs/serverless/models`
- Cerebras pricing page: `https://www.cerebras.ai/pricing`
- Cerebras gpt-oss-120B launch pricing note: `https://www.cerebras.ai/blog/cerebras-launches-openai-s-gpt-oss-120b-at-a-blistering-3-000-tokens-sec`

| Candidate | 14-day no-cache estimate | 14-day cache-adjusted estimate | 3-month no-cache estimate | 3-month cache-adjusted estimate |
|---|---:|---:|---:|---:|
| Direct DeepSeek V4 Flash (optional/privacy-sensitive) | `$0.30` | `$0.09` | `$1.36` | `$0.42` |
| Together GPT-OSS 120B | `$0.37` | n/a | `$1.65` | n/a |
| Together Qwen3 235B throughput | `$0.46` | n/a | `$2.07` | n/a |
| Gemini 3.1 Flash-Lite | `$0.68` | `$0.34` | `$3.07` | `$1.53` |
| Cerebras GPT-OSS 120B | `$0.57` | n/a | `$2.55` | n/a |
| Direct DeepSeek V4 Pro promo (optional/privacy-sensitive) | `$0.94` | `$0.28` | `$4.23` | `$1.26` |
| Cerebras Qwen 3 235B preview | `$1.30` | n/a | `$5.83` | n/a |
| Together Kimi K2.5 | `$1.34` | n/a | `$6.02` | n/a |
| Gemini 3 Flash Preview standard | `$1.37` | `$0.68` | `$6.14` | `$3.05` |
| Together Qwen3.6 Plus | `$1.37` | n/a | `$6.14` | n/a |
| GPT-5.4 mini | `$2.05` | `$1.02` | `$9.22` | `$4.58` |
| Together Kimi K2.6 | `$2.89` | `$1.36` | `$13.01` | `$6.14` |
| Direct DeepSeek V4 Pro list (optional/privacy-sensitive) | `$3.76` | `$1.12` | `$16.90` | `$5.05` |
| Gemini 3.5 Flash standard | `$4.10` | `$2.04` | `$18.43` | `$9.16` |
| Together DeepSeek V4 Pro | `$4.56` | `$1.66` | `$20.53` | `$7.47` |
| Cerebras ZAI GLM 4.7 preview | `$4.61` | n/a | `$20.73` | n/a |
| Gemini 3.5 Flash priority | `$7.37` | `$3.66` | `$33.18` | `$16.48` |

Cerebras pricing note:

- ZAI GLM 4.7: `~1000 tokens/s`, `$2.25/M input`, `$2.75/M output`.
- GPT OSS 120B: Cerebras' launch note lists `~3000 tokens/s`, `$0.25/M input`, `$0.69/M output`. The current pricing page emphasizes account tiers and did not expose a clearer per-model replacement rate on 2026-05-20.
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

### Phase 2 Smoke Results: 2026-05-19

Artifacts:

- `/Users/robertgrzesik/Development/lumibot/artifacts/ai_committee_provider_benchmarks/20260519_222927`
- `/Users/robertgrzesik/Development/lumibot/artifacts/ai_committee_provider_benchmarks/20260519_224952`

Window: `2026-03-30` through `2026-03-31`.

Results:

- `deepseek/deepseek-v4-flash`: passed. Wall time `496.4s`; model latency `82.6s`; input `159,720`, cached input `142,336`, output `6,890`; tool calls `15`; estimated cost `$0.024290` no-cache / `$0.004762` cache-adjusted. No trade was placed.
- `together_ai/Qwen/Qwen3-235B-A22B-Instruct-2507-tput`: passed with a retry warning. Wall time `281.1s`; model latency `26.1s`; input `161,158`, output `1,297`; tool calls `21`; estimated cost `$0.033010`. It attempted a `list_fred_series` tool that is not in the committee toolset before retrying. No trade was placed.
- `together_ai/openai/gpt-oss-120b`: passed but made `0` tool calls. Wall time `140.2s`; model latency `8.9s`; input `15,492`, output `1,036`; estimated cost `$0.002945`. Treat as a poor fit for the committee unless prompting or tool forcing is improved.
- `together_ai/moonshotai/Kimi-K2.5`: passed and placed one AAPL order. Wall time `428.0s`; model latency `83.9s`; input `136,798`, cached input `99,200`, output `4,423`; tool calls `25`; estimated cost `$0.080783`; one-day total return `0.0017899582824705274`.
- `cerebras/gpt-oss-120b`: failed with `BadRequestError`: Cerebras rejected `messages.*.assistant.reasoning_content`. This looks like an ADK/LiteLLM message-normalization issue for Cerebras, not a definitive model-quality failure.
- The first combined batch timed out before it could write `summary.json` or test Kimi. Individual `result.json` files were still written for completed model legs. Kimi was rerun separately and completed.

Current key state:

- DeepSeek, Together, and Cerebras keys are present locally in the ignored env file.
- Gemini/OpenAI keys were not present locally during this smoke, so Gemini 3.5 Flash and GPT-5.4 mini were not run yet.

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
- Kimi K2.5 looked promising in historical smoke runs because it used tools and placed bounded orders, but it should not be used going forward. If testing Kimi, use Kimi K2.6 only, and treat it as an expensive compatibility/quality sample.
- Cerebras is worth testing for speed, but the current ADK/LiteLLM path needs a message-normalization fix for `reasoning_content` before `cerebras/gpt-oss-120b` can complete.
- Qwen throughput is cheap and fast enough to keep testing, but the `list_fred_series` hallucinated tool call means we should watch tool discipline carefully.

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

## ADK 2.0 Upgrade Note: 2026-05-20

Google released ADK Python `2.0.0` on 2026-05-19. The public docs currently describe ADK 2.0 as pre-GA/Beta in some places while the broader docs navigation labels ADK Python 2.0 GA as live. Treat the dependency as current but still worth extra smoke coverage. The main new value is the Workflow Runtime: graph-based workflows, dynamic workflows, and collaborative multi-agent workflows. This is relevant to LumiBot long term because the AI Investment Committee is naturally a workflow graph, but it is not required for the current provider/model benchmark.

Current local state during the provider benchmark:

- LumiBot was upgraded from `google-adk[extensions]>=1.19.0,<2.0.0` to `google-adk[extensions]>=2.0.0,<3.0.0`.
- Supporting dependency pins were updated to `google-genai>=1.72.0,<2.0.0`, `litellm>=1.83.7,<=1.83.14`, and `setuptools<81`.
- The local compatibility environment after the upgrade was `google-adk 2.0.0`, `google-genai 1.75.0`, `litellm 1.83.14`, and `setuptools 80.10.2`.
- An isolated temporary venv import smoke with `google-adk[extensions]>=2.0.0,<2.1.0` succeeded for the runtime symbols LumiBot currently imports: `LlmAgent`, `InMemoryRunner`, `FunctionTool`, `LiteLlm`, and `google.adk.planners`.
- Focused tests passed after one compatibility update for ADK 2's `FunctionDeclaration` schema key change from `parameters` to `parameters_json_schema`.
- ADK 2's extension dependency chain initially installed `setuptools 82`, which removed the old `pkg_resources.get_distribution` API used by `pandas_ta_classic`. Pinning `setuptools<81` preserved the current indicator tool behavior.

Tests run:

- `python3 -m pytest tests/test_agent_runtime_provider_keys.py tests/test_agent_tool_permissions.py -q`: `20 passed`.
- `python3 -m pytest tests/test_agent_runtime_remote_mcp.py tests/test_agent_runtime_mcp_transports.py tests/backtest/test_agent_runtime_backtest.py tests/backtest/test_ai_committee_builtin_tools_backtest.py -q`: `20 passed`.
- `python3 -m py_compile lumibot/components/agents/runtime.py lumibot/components/agents/manager.py scripts/run_ai_committee_provider_benchmark.py`: passed.
- Paid ADK 2 provider smoke: `deepseek/deepseek-v4-flash` over `2026-03-30` through `2026-03-31` passed in `/Users/robertgrzesik/Development/lumibot/artifacts/ai_committee_provider_benchmarks/20260520_002033`. It made `27` tool calls, used `188,335` input tokens and `6,974` output tokens, estimated `$0.028320` no-cache / `$0.006754` cache-adjusted, and placed no trades. It had a transient `list_fred_series` unavailable-tool retry before completion, which means prompt/tool availability should be tightened before large runs.

User-facing ADK 2 feature exposure recommendation:

- Do not expose raw ADK 2 classes directly in LumiBot's public agent API. That would leak framework churn into strategy code and make future ADK changes harder to absorb.
- Expose a LumiBot-native workflow layer later, backed by ADK 2 internally. The right public concepts are likely `AgentTeam`, sequential/parallel steps, router rules, explicit output schemas, human approval checkpoints, and resumable/cancellable runs.
- First useful product feature: declarative committee/team recipes, for example research agents in parallel, a risk reviewer, then a trader. This maps cleanly to ADK 2 collaborative/graph workflows but can stay stable as LumiBot API.
- Second useful product feature: structured outputs per agent step, so portfolio/risk/order decisions can be validated before orders are submitted.
- Third useful product feature: optional human-in-the-loop checkpoints for live trading workflows, especially before mutating order tools.
- Defer fully custom graph authoring until the basic provider benchmark and team recipe API are stable. Most users need safer presets before they need arbitrary workflow graphs.

Small practical feature plan:

1. Add a guardrail-only approval hook around mutating tools, not an interactive chat pause inside the model call. In server/backtest mode it should default to `auto_deny` or `auto_approve`; in BotSpot/live mode it can create a pending approval record and return a "pending approval" tool result instead of placing the order. This preserves server operation and avoids blocking a trading loop waiting for a human.
2. Add cancellable runs as cooperative cancellation. Store run state keyed by `run_id`, expose `cancel_agent_run(run_id)`, and have runtime/tool wrappers check a cancellation flag between model/tool events. This will stop future work but should not pretend to interrupt an in-flight provider HTTP request.
3. Add resumable runs only at step boundaries first. Full mid-token/mid-tool resume is too much. Store step outputs, trace events, model, prompt, context, and tool results; rerun only incomplete team steps.
4. Add `AgentTeam` as a thin sequential/parallel orchestrator over existing `AgentManager.create()` agents. First version can run named agents in order or simple parallel groups, then pass summaries into the final agent. Do not expose arbitrary ADK graph nodes yet.
5. Add structured output validation for trading decisions before adding custom graph authoring. A `decision_schema` or `output_schema` is easier to test and directly improves trading safety.

## Full Benchmark Game Plan: 2026-05-20

Goal: benchmark the AI Investment Committee across the practical fast/cheap model slate over the same 3-month window, using the same starting cash, fees, data, committee prompts, tool set, and risk settings. The benchmark should compare model quality, tool discipline, runtime, token/cost profile, final return, drawdown, trade count, and qualitative evidence quality.

Primary model slate:

- `deepseek/deepseek-v4-flash`
- `gemini-3.5-flash`
- `openai/gpt-5.4-mini`
- `together_ai/moonshotai/Kimi-K2.5`
- `together_ai/Qwen/Qwen3-235B-A22B-Instruct-2507-tput`
- `cerebras/gpt-oss-120b`

Optional second-wave models only after the primary slate works:

- `cerebras/zai-glm-4.7`
- `together_ai/moonshotai/Kimi-K2.6`
- `deepseek/deepseek-v4-pro`, privacy-sensitive/direct DeepSeek
- `together_ai/deepseek-ai/DeepSeek-V4-Pro`, if vendor posture matters more than cost

Current blockers before long runs:

1. Finish the fixed-runtime 14-trading-day qualifier now running in parallel.
2. Inspect every completed artifact for tool usage, failures, trade behavior, token pressure, and whether any provider is looping or staying in cash.
3. Start the three-month run only for models that complete the qualifier mechanically.

Execution phases:

1. One-day smoke: run every primary model for `2026-03-30` through `2026-03-31`. Passing means the run completes, writes `result.json`, writes `stats_agent_detail.parquet`, produces meaningful tool calls, and does not crash on unavailable tools.
2. Fourteen-trading-day qualifier: run all models that pass smoke on the existing 14-trading-day baseline window. This catches high-context/tool-loop failures without spending time on 3-month runs.
3. Three-month benchmark: run every qualifier on the exact same 3-month window. Recommended first window: the most recent completed three calendar months with stable data availability at execution time, or a fixed historical window chosen once and reused.
4. Six-month benchmark: run only after the 3-month output looks mechanically valid. Six months is worth doing, but it should not be the first long run because bad prompt/tool behavior doubles both runtime and debugging noise.

Cost estimates for the primary 6-model slate, scaled from the prior 14-trading-day AI committee artifact:

- 14-trading-day qualifier total: about `$8.82` no-cache / about `$5.52` cache-adjusted where cache pricing exists.
- 3-month primary slate total: about `$39.65` no-cache / about `$24.80` cache-adjusted where cache pricing exists.
- 6-month primary slate total: about `$79.30` no-cache / about `$49.60` cache-adjusted where cache pricing exists.

Per-model 3-month / 6-month estimates:

- `deepseek/deepseek-v4-flash`: 3-month `$1.36` no-cache / `$0.42` cache-adjusted; 6-month `$2.72` no-cache / `$0.84` cache-adjusted.
- `together_ai/Qwen/Qwen3-235B-A22B-Instruct-2507-tput`: 3-month `$2.07`; 6-month `$4.14`.
- `cerebras/gpt-oss-120b`: 3-month `$2.55`; 6-month `$5.10`.
- `together_ai/moonshotai/Kimi-K2.5`: 3-month `$6.02`; 6-month `$12.04`.
- `openai/gpt-5.4-mini`: 3-month `$9.22` no-cache / `$4.58` cache-adjusted; 6-month `$18.44` no-cache / `$9.16` cache-adjusted.
- `gemini-3.5-flash`: 3-month `$18.43` no-cache / `$9.16` cache-adjusted; 6-month `$36.86` no-cache / `$18.32` cache-adjusted.

If adding `cerebras/zai-glm-4.7`, add about `$20.73` for 3 months or `$41.46` for 6 months. If adding `together_ai/moonshotai/Kimi-K2.6`, add about `$13.01` for 3 months or `$26.02` for 6 months.

Recommendation:

- Do the full 3-month primary slate after the smoke/qualifier fixes. The expected total is low enough that cost should not stop us.
- Do not start with a 6-month full slate. Run 3 months first, verify trace quality and backtest outputs, then run a 6-month extension for the finalists or for all six if runtime is acceptable.
- Drop `together_ai/openai/gpt-oss-120b` from the benchmark for now because the earlier smoke made zero tool calls.

## Post-Fix One-Day Smoke: 2026-05-20

Artifact root: `/Users/robertgrzesik/Development/lumibot/artifacts/ai_committee_provider_benchmarks/20260520_025556`.

Window: `2026-03-30` through `2026-03-31`.

Fixes validated:

- Runtime now includes actual available tool names in the prompt and no longer injects FRED tool names when FRED tools are not registered.
- AI committee example now asks for FRED macro data only when FRED tools are available.
- Cerebras ADK/LiteLLM wrapper strips thought/reasoning parts before LiteLLM serializes history back to Cerebras, avoiding the prior `reasoning_content` rejection.

Results:

- `deepseek/deepseek-v4-flash`: passed. Wall time `571.3s`; model latency `121.2s`; tool calls `29`; input `276,965`, cached input `253,696`, output `8,616`; estimated cost `$0.041188` no-cache / `$0.006380` cache-adjusted. No trade.
- `together_ai/moonshotai/Kimi-K2.5`: passed. Wall time `409.8s`; model latency `55.3s`; tool calls `11`; input `51,804`, cached input `28,160`, output `3,366`; estimated cost `$0.035327`. Bought `3` GOOGL and `1` META; one-day return `0.0004934286712645619`.
- `together_ai/Qwen/Qwen3-235B-A22B-Instruct-2507-tput`: passed. Wall time `283.3s`; model latency `43.6s`; tool calls `16`; input `134,909`, output `1,393`; estimated cost `$0.027818`. No trade.
- `cerebras/gpt-oss-120b`: passed. Wall time `45.8s`; model latency `2.6s`; tool calls `3`; input `60,239`, cached input `31,616`, output `1,954`; estimated cost `$0.022549`. No trade.

Interpretation:

- All currently keyed non-Gemini/non-OpenAI primary candidates now pass one-day smoke under ADK 2.
- Cerebras is now mechanically working and extremely fast, but the low tool-call count means the 14-day qualifier must inspect whether it is doing enough evidence gathering.
- Kimi remains the most interesting Together model because it used tools and placed bounded trades in both smoke runs.
- DeepSeek Flash is cheap and tool-heavy but slow in wall time, so it belongs in the qualifier.

## 14-Day Qualifier Progress: 2026-05-20

Window: `2026-03-16` through `2026-04-02`.

Important benchmark runner fixes from this phase:

- The paid benchmark runner now defaults `--max-model-calls` to `80` instead of `8`; the original default only allowed about two committee cycles and caused an artificial `LUMIBOT_AGENT_MAX_MODEL_CALLS` failure.
- The paid benchmark runner now prints JSON `model_start` and `model_finished` events so long runs are observable.
- Benchmark artifacts can be summarized with `/Users/robertgrzesik/Development/lumibot/scripts/summarize_ai_committee_provider_benchmarks.py`, which reads per-model `result.json` files and writes compact JSON/Markdown comparisons.
- The runner accepts `--agent-run-timeout-seconds` for slow provider qualifiers. This is a per-agent timeout, not the overall benchmark timeout. Keep the default for fast providers; use a higher value for Qwen/Kimi only if the model is making progress but individual calls exceed the runtime's default safety rail.
- Historical note, now reverted: the AI committee example briefly applied a reusable token-budget helper at `handoff_max_tokens` before passing text to the next role. This was a bad benchmark control because middle truncation changed the evidence seen by downstream agents.

Artifacts:

- Cerebras full qualifier: `/Users/robertgrzesik/Development/lumibot/artifacts/ai_committee_provider_benchmarks/20260520_034143`.
- Qwen uncapped failure: `/Users/robertgrzesik/Development/lumibot/artifacts/ai_committee_provider_benchmarks/20260520_035752`.
- Qwen capped rerun partial: `/Users/robertgrzesik/Development/lumibot/artifacts/ai_committee_provider_benchmarks/20260520_044254`.

Results so far:

- `cerebras/gpt-oss-120b`: passed the full window. Wall time `926.6s`; model latency `38.1s`; call summaries `13`; tool calls `27`; input `646,373`, cached input `464,384`, output `24,428`; estimated cost `$0.244552`. It stayed in cash, so mechanical speed is strong but strategy quality still needs review.
- `together_ai/Qwen/Qwen3-235B-A22B-Instruct-2507-tput`, uncapped: failed with `ContextWindowExceededError` after sending about `2,951,306` tokens into a `262,144` token context window. Root cause was oversized role handoffs in the committee example, not a bad API key.
- `together_ai/Qwen/Qwen3-235B-A22B-Instruct-2507-tput`, bounded-handoff rerun: hit the one-hour process timeout after `49` agent run summaries / `12` complete committee cycles with no repeated context-window failure. Partial usage: input `2,025,198`, output `41,640`, tool calls `363`, estimated cost `$0.430024`. The handoff contract fixed the failure mode, but Qwen needs a longer timeout to finish the qualifier.
- Parallel rerun after token-budgeted handoffs showed `deepseek/deepseek-v4-flash` can still exceed context with raw tool results: provider rejected about `7,033,087` requested tokens against a `1,048,576` context window. This exposed a second boundary: tool results, especially raw SEC/companyfacts-style payloads, must also be token-budgeted before entering model context.
- Historical note, now reverted: runtime tool-result budgeting was added after the DeepSeek failure, then removed because bounded excerpts changed model-visible evidence and invalidated the benchmark.
- The first Qwen run after tool-result budgeting still failed with Together's generic `Input validation error` after an earlier 300-second agent timeout. The likely remaining issue was accumulated context/request shape, not credentials.
- Fixed-budget rerun state: Qwen rerun with 4K tool-result budget and 900-second per-agent timeout started in `/Users/robertgrzesik/Development/lumibot/artifacts/ai_committee_provider_benchmarks/20260520_144512`. DeepSeek, Gemini, Kimi, OpenAI, and Cerebras fixed-budget reruns started in `/Users/robertgrzesik/Development/lumibot/artifacts/ai_committee_provider_benchmarks/20260520_144705`.
- Cerebras fixed-budget rerun failed immediately with provider billing error: `Payment required to access this resource`. Earlier Cerebras qualifier passed mechanically, so the integration works, but the account/key needs billing credits before Cerebras can be included in the final three-month benchmark.
- Historical note, now reverted: a runtime tool-call enforcement change was added after DeepSeek used `65` tools in one evidence call. That was the wrong fix because it blocked later execution tools and invalidated trading results.
- Enforced-budget 14-day qualifier artifacts:
  - Summary JSON: `/Users/robertgrzesik/Development/lumibot/artifacts/ai_committee_provider_benchmarks/enforced_14d_compact_summary.json`.
  - Summary Markdown: `/Users/robertgrzesik/Development/lumibot/artifacts/ai_committee_provider_benchmarks/enforced_14d_summary.md`.
  - Shared root for OpenAI, DeepSeek, Kimi, and partial Gemini: `/Users/robertgrzesik/Development/lumibot/artifacts/ai_committee_provider_benchmarks/20260520_145742`.
  - Qwen root: `/Users/robertgrzesik/Development/lumibot/artifacts/ai_committee_provider_benchmarks/20260520_145816`.

Enforced-budget 14-day qualifier results:

- `openai/gpt-5.4-mini`: passed mechanically. Wall time `629.9s`; calls `13`; tool calls `125`; input `378,434`, cached input `276,096`, output `7,740`; estimated cost `$0.318655` no-cache / `$0.132291` cache-adjusted. Stayed entirely in cash, total return `0%`.
- `deepseek/deepseek-v4-flash`: passed mechanically. Wall time `2295.0s`; calls `13`; tool calls `122`; input `1,052,857`, cached input `896,512`, output `47,393`; estimated cost `$0.160670` no-cache / `$0.037669` cache-adjusted. Stayed entirely in cash, total return `0%`.
- `together_ai/Qwen/Qwen3-235B-A22B-Instruct-2507-tput`: passed mechanically. Wall time `2077.2s`; calls `13`; tool calls `88`; input `518,011`, output `6,300`; estimated cost `$0.107382`. Stayed entirely in cash, total return `0%`.
- `together_ai/moonshotai/Kimi-K2.5`: passed mechanically. Wall time `3500.2s`; calls `13`; tool calls `90`; input `506,210`, cached input `304,256`, output `26,157`; estimated cost `$0.326345`. Stayed entirely in cash, total return `0%`.
- `gemini-3.5-flash`: did not complete the enforced 14-day qualifier in a reasonable wall-clock window and was stopped after provider demand/retry noise. Partial stats before stop: calls `7`, tool calls `170`, input `922,275`, cached input `392,443`, output `16,759`. The key/model string works, but this workload is operationally too slow/noisy in the current run.

Current interpretation:

- OpenAI, DeepSeek Flash, Qwen throughput, and Kimi K2.5 are mechanically viable through ADK 2 with the enforced budgets.
- The current AI Investment Committee settings are too conservative for a meaningful performance benchmark: every mechanically successful enforced 14-day qualifier stayed entirely in cash. A three-month run from this exact configuration would likely measure provider cost/latency more than trading quality.
- Before spending hours on a three-month slate, either relax the portfolio manager's trade threshold / require at least one small position when the evidence is acceptable, or split the benchmark into two tracks: `mechanical/tool discipline` and `trade-seeking committee`.
- Cerebras should remain in the slate after billing is fixed. The earlier qualifier passed mechanically and was much faster, but the current account/key hit a provider billing gate on rerun.
- Gemini 3.5 Flash should not be included in the first three-month run unless the goal is specifically to measure Google provider availability under load; it did not finish the 14-day qualifier here.

## Three-Month Benchmark Run: 2026-05-20

Window: `2026-01-02` through `2026-04-02`.

Launched models:

- `openai/gpt-5.4-mini`
- `deepseek/deepseek-v4-flash`
- `together_ai/moonshotai/Kimi-K2.5`
- `together_ai/Qwen/Qwen3-235B-A22B-Instruct-2507-tput`

Excluded models:

- `gemini-3.5-flash`: excluded because it did not complete the enforced 14-day qualifier in a reasonable wall-clock window.
- `cerebras/gpt-oss-120b`: excluded because the current key/account hit a Cerebras billing gate on rerun, even though an earlier 14-day qualifier passed mechanically.

Artifact root: `/Users/robertgrzesik/Development/lumibot/artifacts/ai_committee_provider_benchmarks/20260520_160534`.

Parallel launcher root: `/Users/robertgrzesik/Development/lumibot/artifacts/ai_committee_provider_benchmarks/parallel_3m_enforced_20260520_160510`.

Launch status: all four models printed `model_start`; no key/config failure on launch.

Final summary artifacts:

- Compact JSON: `/Users/robertgrzesik/Development/lumibot/artifacts/ai_committee_provider_benchmarks/three_month_enforced_compact_summary.json`.
- Markdown summary: `/Users/robertgrzesik/Development/lumibot/artifacts/ai_committee_provider_benchmarks/three_month_enforced_summary.md`.

Final results:

- `openai/gpt-5.4-mini`: passed mechanically. Wall time `2663.3s` (`44.4m`); calls `62`; tool calls `516`; input `1,905,639`, cached input `1,401,600`, output `38,605`; estimated cost `$1.602952` no-cache / `$0.656872` cache-adjusted. Stayed entirely in cash, total return `0%`, max drawdown `0%`, Sharpe `0`.
- `deepseek/deepseek-v4-flash`: passed mechanically. Wall time `10343.8s` (`172.4m`); calls `62`; tool calls `552`; input `5,941,369`, cached input `5,131,008`, output `227,977`; estimated cost `$0.895625` no-cache / `$0.191651` cache-adjusted. Stayed entirely in cash, total return `0%`, max drawdown `0%`, Sharpe `0`.
- `together_ai/moonshotai/Kimi-K2.5`: failed before completion because Together returned `Credit limit exceeded`. Partial `stats_agent_detail.parquet` before failure had calls `22`, tool calls `603`, input `1,911,570`, cached input `1,239,168`, output `92,701`.
- `together_ai/Qwen/Qwen3-235B-A22B-Instruct-2507-tput`: failed before completion because Together returned `Credit limit exceeded`. Partial `stats_agent_detail.parquet` before failure had calls `19`, tool calls `132`, input `847,319`, cached input `0`, output `11,921`.

Interpretation:

- The ADK 2 + LiteLLM path works for OpenAI, direct DeepSeek, Together, and Cerebras mechanically. Provider failures in this benchmark were account/credit or operational-speed issues, not basic model-string/key wiring failures.
- DeepSeek V4 Flash direct was cheaper than OpenAI GPT-5.4 Mini on this run, especially after cache adjustment, but it was much slower and produced far more output tokens. For this workload, DeepSeek was not a speed replacement for OpenAI.
- The current AI Investment Committee configuration is too conservative for a trading-performance benchmark. Both completed 3-month runs stayed 100% cash, exactly matching the 14-day qualifier pattern. The next meaningful benchmark should use a `trade_seeking` committee variant or a relaxed portfolio-manager decision threshold, then rerun 14 days before spending another 3-month pass.
- Together needs account credits before rerunning Kimi/Qwen. Based on partial usage, Kimi can consume credits quickly because it kept trying tools heavily; Qwen was lighter before the account stopped it.
- Cerebras should be rerun after billing is fixed because the earlier 14-day run proved mechanical compatibility and it remains the only provider that looked truly fast. Do not judge Cerebras quality from the current final slate because it was excluded by billing, not model behavior.
- Gemini 3.5 Flash should stay out of the next performance slate unless the purpose is specifically to test Google provider availability under this heavy agent workload.
