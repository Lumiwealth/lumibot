# LumiBot Real-Model Agent Evals

Every JSON file in this directory is a production release gate. The acting model
is ChatGPT GPT-6 Luna (`chatgpt/gpt-6-luna`) and uses LumiBot's real Google ADK
runtime and built-in runtime skills. Native receipts record the effective
reasoning effort; qualification follows that recorded configuration. Market and
broker responses are deterministic fixtures, so the eval measures agent behavior
without placing a trade or requiring paid market data.

Semantic quality is scored by a real ChatGPT GPT-6 Luna judge with its qualified
Extra High profile. Both roles use
the developer's own authenticated ChatGPT connection with no API-key billing.
Private login credentials stay outside the repository and CI.

Deterministic checks cover only
machine facts such as tool order, order count, exact leg sides and quantities,
and final fixture positions.

Portfolio cases exercise nine- and twenty-five-name completion, existing-position
rebalancing, current executable quote sizing, dated historical-row selection and
a basket with one unavailable constituent. The native broker's fills, quantities
and cash are checked alongside the semantic judge. A one-repetition `--pilot`
run finds setup or behavior failures without lowering the configured five-pass
requirement; successful compatible repetitions are retained on resume.

Cases declare three or five required consecutive repetitions. The current
catalog selects five because it exercises shared runtime and security behavior.
Preserve genuine failing attempts; never manufacture a red result or relabel
missing evidence as successful execution. Passing freshness lasts 90 days for the exact
case, acting model, judge model, fixture, runtime, tool, rule, and skill
fingerprints. Release CI verifies complete source-bound actor/judge qualification
and separately required positive/negative judge calibration. It does not copy a
developer's rotating login or fall back to API billing.

Actor and judging evidence have different compatibility boundaries. Actor proof
binds the complete acting runtime, tools, skills and case. Judge calibration binds
the actual tool-free judging request, rubric, labeled control, model profile and
judging protocol/dependencies. Unrelated acting changes do not require buying the
same unchanged judge decisions again. The maintained calibration runner accepts
`--reuse-source <full-commit>` only after verifying the immutable original source,
native call identity and exact judging input. Original verdicts, times and logical
repetition slots remain unchanged. The legacy adapter applies only to still-fresh
records from the previous fingerprint algorithm and can retire with those records.

Use the maintained rejudge command for grading-only repairs. It verifies unchanged
actor inputs and records the actor's original source separately from the new judge.
Equivalent exact-contract quote tools may satisfy the existing alternative-tool
contract; order counts, quantities, identities and semantic quote quality remain
required. A changed actor prompt, skill or runtime requires fresh actor evidence.

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
