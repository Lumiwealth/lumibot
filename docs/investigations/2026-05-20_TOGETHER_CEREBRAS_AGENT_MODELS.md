# Together AI and Cerebras Agent Model Feasibility

**Date:** 2026-05-20
**Scope:** LumiBot `lumibot.components.agents` provider wiring for adding Together AI and Cerebras model options.

## Summary

Adding Together AI and Cerebras to LumiBot agents is low implementation effort because the current agent runtime already routes every non-Gemini model string through Google ADK's `LiteLlm` connector. Gemini stays on the native ADK path; OpenAI, Anthropic, and xAI/Grok already use the same provider-prefixed `LiteLlm` path that Together and Cerebras would use.

The likely code work is small:

- Add key alias helpers for product-facing env vars if needed.
- Add provider hints for backtest auth/billing error messages.
- Add smoke/unit tests proving provider-prefixed model strings resolve through `LiteLlm`.
- Update examples/docs with current model IDs and required env vars.
- Run at least one paid smoke call per provider with built-in LumiBot tools enabled before calling it supported.

## Current LumiBot Architecture

Relevant files:

- `/Users/robertgrzesik/Development/lumibot/lumibot/components/agents/runtime.py`
- `/Users/robertgrzesik/Development/lumibot/lumibot/components/agents/manager.py`
- `/Users/robertgrzesik/Development/lumibot/tests/test_agent_runtime_provider_keys.py`
- `/Users/robertgrzesik/Development/lumibot/lumibot/example_strategies/agent_discretionary.py`

Current behavior:

- `_resolve_model_for_adk()` sends `gemini-*` and `models/gemini*` strings through ADK's native Gemini path.
- Every other string with a provider prefix is wrapped in `google.adk.models.lite_llm.LiteLlm`.
- `litellm>=1.77.0` is already a core dependency.
- The runtime already enables `litellm.drop_params = True`, quiets provider lookup noise, and configures retries.
- Explicit temperature is only sent to native Gemini models, avoiding common provider errors for OpenAI-style/reasoning models.
- The public demo path already supports `AGENT_MODEL`, so users can swap model strings without changing strategy code.

## Provider Notes

### Together AI

Docs checked:

- https://docs.together.ai/docs/inference/openai-compatibility
- https://docs.together.ai/docs/serverless/models
- https://docs.together.ai/docs/inference/recommended-models
- https://docs.together.ai/docs/inference/function-calling/overview
- https://docs.litellm.ai/docs/providers/togetherai

Together's API is OpenAI-compatible at `https://api.together.ai/v1` and uses `TOGETHER_API_KEY` for the OpenAI-compatible client path. LiteLLM's Together provider examples use the `together_ai/` prefix and `TOGETHERAI_API_KEY`.

Current Together catalog examples relevant to agents:

- `moonshotai/Kimi-K2.5`
- `Qwen/Qwen3-Coder-480B-A35B-Instruct-FP8`
- `Qwen/Qwen3-235B-A22B-Instruct-2507-tput`
- `meta-llama/Llama-3.3-70B-Instruct-Turbo`
- `deepseek-ai/DeepSeek-R1`
- `openai/gpt-oss-120b`

Together's docs also reference cached-token pricing for Kimi K2.6 and DeepSeek-V4-Pro. Verify exact model IDs from the live Together catalog before hardcoding defaults because their catalog changes quickly.

Likely LumiBot model strings:

- `together_ai/moonshotai/Kimi-K2.5`
- `together_ai/Qwen/Qwen3-Coder-480B-A35B-Instruct-FP8`
- `together_ai/Qwen/Qwen3-235B-A22B-Instruct-2507-tput`
- `together_ai/meta-llama/Llama-3.3-70B-Instruct-Turbo`
- `together_ai/deepseek-ai/DeepSeek-R1`

Implementation difficulty: easy. The only nuance is whether to support both `TOGETHER_API_KEY` and `TOGETHERAI_API_KEY` by mirroring `TOGETHER_API_KEY` into `TOGETHERAI_API_KEY` when the latter is absent.

Risk: medium-low until tool-calling smoke tests pass. Together documents function calling, but support is model-dependent. LumiBot agents rely heavily on tool calls, so each recommended model should get a real tool-call smoke test.

### Cerebras

Docs checked:

- https://inference-docs.cerebras.ai/integrations/litellm
- https://inference-docs.cerebras.ai/resources/openai
- https://inference-docs.cerebras.ai/capabilities/tool-use
- https://inference-docs.cerebras.ai/models/overview
- https://docs.litellm.ai/docs/providers/cerebras

Cerebras has a LiteLLM integration, uses `CEREBRAS_API_KEY`, and supports `cerebras/<model-id>` strings. Current production model IDs in the docs include:

- `cerebras/llama3.1-8b`
- `cerebras/gpt-oss-120b`

Docs also mention `zai-glm-4.7` as a reasoning/tool-capable model and note that more model families may be available through dedicated endpoints. As of the docs checked, Cerebras warns that `llama3.1-8b` and `qwen-3-235b-a22b-instruct-2507` are scheduled for deprecation on 2026-05-27.

Implementation difficulty: easy for API plumbing, medium for picking durable defaults. Cerebras is attractive for very fast inference, but the shared/serverless catalog is smaller than Together's. Do not present Cerebras as the broad Llama/DeepSeek/Kimi/Qwen catalog unless a dedicated endpoint supports those specific models.

Risk: medium-low. Cerebras has explicit tool-calling docs, but there is an upcoming API validation change on 2026-07-21 that tightens structured-output and tool schemas. LumiBot should run a tool-call smoke test with the exact model and set of tool schemas before advertising production support.

## Suggested Implementation Plan

1. Add `_sync_together_api_key_alias()` that mirrors `TOGETHER_API_KEY` to `TOGETHERAI_API_KEY` if needed.
2. Optionally add `_sync_cerebras_api_key_alias()` only if we want a product alias. LiteLLM and Cerebras already agree on `CEREBRAS_API_KEY`, so this may not be needed.
3. Extend `_resolve_model_for_adk()` to call the Together alias helper when the lowercased model starts with `together_ai/`.
4. Extend `AgentHandle._log_fatal_backtest_error()` provider hints for `together_ai/` and `cerebras/`.
5. Extend `agent_discretionary.py` auth precheck so demos fail clearly before a paid backtest.
6. Add unit tests for Together/Cerebras `LiteLlm` construction and API-key alias behavior.
7. Add docs/examples only after a real smoke call succeeds for each recommended provider/model.

## Recommended Validation

Minimum support gate for each provider/model:

- One direct agent run with a tiny task and no trading mutation.
- One run that calls at least one built-in read-only LumiBot tool.
- One run that asks the agent to call multiple tools, because this is where provider tool-call behavior diverges.
- One backtest replay rerun confirming the same model string participates in the replay-cache key and no extra paid call is made on a cache hit.
- Auth, bad-model, and no-credit errors classified into clean `auth`, `config`, or `billing` messages.

## Bottom Line

Together AI should be a one-session implementation plus smoke testing. It gives the broad catalog Rob is asking for: Kimi, DeepSeek, Qwen, Llama, and other fast hosted open models.

Cerebras should also be a one-session integration, but the product promise should be narrower: extremely fast supported Cerebras-hosted models, not "every open model." The main decision is whether to expose Cerebras as a simple model-prefix option only, or add a curated "fast mode" preset after benchmarking latency, tool correctness, and trading decision quality.
