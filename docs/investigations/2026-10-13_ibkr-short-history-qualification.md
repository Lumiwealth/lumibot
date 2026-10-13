# IBKR declared history minimum

`length` controls the fetch/return window. `minimum_bars` optionally declares the
latest completed lookback actually required by the strategy's decision. The
default remains `length`, preserving existing strict MGC/other requirements.
This does not infer a custom indicator's requirements or rewrite strategy rules.

Example: `get_historical_prices(asset, 500, timestep="day", minimum_bars=51)`
fetches the same 500-bar window and returns the same prices. Required-data
validation examines the last 51 returned rows, before filtering absence/null
markers. A hole inside those 51 rows still invalidates the result; an irrelevant
older incomplete prefetch does not. A strategy declaring this minimum must
actually operate on this lookback. Framework input evidence retains the full
returned frame independently of validation.

Source baseline: `1d5025571b9ff010b961a731662996df720736da`.
Red regression: seven API/validation cases failed before the patch; a second
focused run reproduced five failures, including both real direct/routed engine
cases and missing-row validation with enough older rows to hide the required gap.
After the fix, all 102 cases in the indicator-prefetch, IBKR history-health and
frozen-engine modules passed (24.46 seconds). Existing strict-request and
genuinely incomplete/no-trade tests were retained.

Two actual frozen-input backtests request 500 bars while consuming three;
their bars, signals, real fills, cash, holdings, equity and input hashes equal
the normal three-bar-request baseline exactly. No network access is allowed.
Provider/candidate live acceptance and the full release gates are separate.

The release is not published merely because these focused checks pass. The
latest version branch must pass normal cloud gates and required review before
tagging/publication and downstream consumption. Platform deployment belongs
to the dedicated deployment task under Rob's October 13 ownership rule.
