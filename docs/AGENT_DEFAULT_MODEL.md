# Default agent model

New `strategy.agents.create(name="trader")` calls default to
`gemini-3.5-flash-lite`. This replaces the old preview default, not a strategy's
explicit `model` or `default_model` selection. Existing agent instances retain
their model; there is no silent model migration during a tool loop.

Managed deployments may explicitly choose `google/gemini-flash-lite` to use
the gateway's reviewed family mapping. The gateway resolves and pins one exact
model for each decision. Native provider callers should use an exact supported
provider model. A family is not an unauthenticated fallback or a way around
provider availability and billing checks.

The regression in `tests/test_agent_skills.py` checks the new default alongside
explicit model and family preservation. Availability must still be verified
against the configured provider; a default string is not an availability claim.
