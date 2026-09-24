# Default agent model

New `strategy.agents.create(name="trader")` calls default to
`openai/gpt-6-luna` with medium reasoning effort. This replaces the old Gemini default, not a strategy's
explicit `model` or `default_model` selection. Existing agent instances retain
their model; there is no silent model migration during a tool loop.

Medium reasoning is applied only when the resolved model is the default and the
caller did not pass `reasoning_effort`. An explicit `reasoning_effort`, or any
other model, is left exactly as written. GPT-6 Luna needs `OPENAI_API_KEY` for
native calls; LumiBot routes it through the OpenAI Responses API because
reasoning plus tool calls is not accepted on Chat Completions.

Managed deployments may explicitly choose `openai/luna` or `google/gemini-flash-lite` to use
the gateway's reviewed family mapping. The gateway resolves and pins one exact
model for each decision. Native provider callers should use an exact supported
provider model. A family is not an unauthenticated fallback or a way around
provider availability and billing checks.

The regression in `tests/test_agent_skills.py` checks the new default alongside
explicit model and family preservation. Availability must still be verified
against the configured provider; a default string is not an availability claim.
