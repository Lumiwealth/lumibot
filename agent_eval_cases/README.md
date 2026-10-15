# LumiBot Real-Model Agent Evals

Every JSON file in this directory is a production release gate. The acting model
is ChatGPT GPT-6 Luna (`chatgpt/gpt-6-luna`) and uses LumiBot's real Google ADK
runtime and built-in runtime skills. Native receipts record the effective
reasoning effort; qualification follows that recorded configuration. Market and
broker responses are deterministic fixtures, so the eval measures agent behavior
without placing a trade or requiring paid market data.

Semantic quality is scored by a real ChatGPT GPT-6.1 Sol judge. Both roles use
the developer's own authenticated ChatGPT connection with no API-key billing.
Private login credentials stay outside the repository and CI.

Deterministic checks cover only
machine facts such as tool order, order count, exact leg sides and quantities,
and final fixture positions.

Cases declare three or five required consecutive repetitions. The current
catalog selects five because it exercises shared runtime and security behavior.
Preserve genuine failing attempts; never manufacture a red result or relabel
missing evidence as successful execution. Passing freshness lasts 90 days for the exact
case, acting model, judge model, fixture, runtime, tool, rule, and skill
fingerprints. Release CI verifies complete source-bound actor/judge qualification
and separately required positive/negative judge calibration. It does not copy a
developer's rotating login or fall back to API billing.

The existing release workflow also supports manual candidate validation with
`gh workflow run release.yml --ref version/4.6.19`. It runs the same package
build/install, unit, backtest and native qualification gates without publishing.
The installed wheel must import the ChatGPT adapter and expose the login CLI.
Only a version-tag push may publish to PyPI, create a GitHub release or advance
the next version; a successful manual validation is not a release.

Historical source refusal and live research are separate contracts. Live calendar
filings may support current research without proving original-vintage replay.
The historical macro case must return a typed blocked decision when publication
and original-version proof are unavailable; an unsupported source is not a
completed historical analysis.
