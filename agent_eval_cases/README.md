# LumiBot Real-Model Agent Evals

Every JSON file in this directory is a production release gate. The acting model
uses LumiBot's real Google ADK runtime and built-in runtime skills. Market and
broker responses are deterministic fixtures, so the eval measures agent behavior
without placing a trade or requiring paid market data.

Semantic quality is scored by a real LLM judge. Deterministic checks cover only
machine facts such as tool order, order count, exact leg sides and quantities,
and final fixture positions.

New or materially changed cases require a preserved honest red baseline and three
consecutive passing repetitions. Passing freshness lasts 30 days for the exact
case, acting model, judge model, fixture, runtime, tool, rule, and skill
fingerprints. Release CI reruns only stale coverage.
