# AI Trading Team Provider Benchmarks - 2026-05-25

This note records the first provider comparison for the simple `AITradingTeamStrategy`
example in `lumibot/example_strategies/ai_trading_team.py`.

Strategy universe:

`TQQQ`, `SQQQ`, `UPRO`, `SPXU`, `UDOW`, `SDOW`, `TNA`, `TZA`, `TECL`, `TECS`, `SOXL`, `SOXS`, `WEBL`, `WEBS`, `FAS`, `FAZ`, `LABU`, `LABD`, `ERX`, `ERY`, `GUSH`, `DRIP`, `DRN`, `DRV`, `TMF`, `TMV`, `NUGT`, `DUST`

Models tested:

- `deepseek/deepseek-v4-flash`
- `deepseek/deepseek-v4-pro`
- `gemini-3.1-flash-lite`

Artifact roots:

- One-day smoke: `/Users/robertgrzesik/Development/lumibot/artifacts/ai_trading_team_provider_benchmarks/20260524_232138`
- Seven-day window: `/Users/robertgrzesik/Development/lumibot/artifacts/ai_trading_team_provider_benchmarks/20260524_235130`

Pricing source:

- Static provider pricing table embedded in `scripts/run_ai_committee_provider_benchmark.py` as of 2026-05-24.
- Cost estimates below use raw trace usage, including cached input tokens when the provider reports them.

## One-Day Smoke

Window: 2026-05-21 to 2026-05-22.

`deepseek/deepseek-v4-flash`

- Status: passed
- Wall time: 13.10 minutes
- Return: -5.09%
- Max drawdown: 5.09%
- Model calls: 4
- Tool calls: 305
- Input tokens: 2,701,037
- Cached input tokens: 2,135,296
- Output tokens: 60,751
- Cache hit rate: 79.05%
- Estimated cost with cache pricing: $0.102193
- Estimated cost without cache pricing: $0.395155
- Final position: 496 shares of `SOXL`, with about $15,055 cash
- Submitted orders: one market buy for `SOXL`

`deepseek/deepseek-v4-pro`

- Status: passed
- Wall time: 14.24 minutes
- Return: 0.00%
- Max drawdown: 0.00%
- Model calls: 4
- Tool calls: 109
- Input tokens: 607,650
- Cached input tokens: 419,328
- Output tokens: 35,602
- Cache hit rate: 69.01%
- Estimated cost with cache pricing: $0.114414
- Estimated cost without cache pricing: $0.295301
- Final position: all cash
- Submitted orders: one limit buy for `SOXL` at $161.00; it did not fill in the one-day window

`gemini-3.1-flash-lite`

- Status: passed
- Wall time: 0.42 minutes
- Return: -6.37%
- Max drawdown: 6.37%
- Model calls: 4
- Tool calls: 20
- Input tokens: 157,425
- Cached input tokens: 88,254
- Output tokens: 1,676
- Cache hit rate: 56.06%
- Estimated cost with cache pricing: $0.022013
- Estimated cost without cache pricing: $0.041870
- Final position: 621 shares of `SOXL`, with about -$6,352 cash
- Submitted orders: one market buy for `SOXL`

## Seven-Day Window

Window: 2026-05-15 to 2026-05-22.

`deepseek/deepseek-v4-flash`

- Status: passed
- Wall time: 38.74 minutes
- Return: -4.08%
- Max drawdown: 22.35%
- Model calls: 20
- Tool calls: 1,256
- Input tokens: 8,128,308
- Cached input tokens: 6,690,432
- Output tokens: 237,028
- Cache hit rate: 82.31%
- Estimated cost with cache pricing: $0.286404
- Estimated cost without cache pricing: $1.204331
- Final position: 508 shares of `TECL`, with about -$123 cash
- Submitted orders: bought `SOXL`, added `SOXL`, sold `SOXL`, bought `TECL`, added `TECL`

`deepseek/deepseek-v4-pro`

- Status: passed
- Wall time: 84.10 minutes
- Return: -6.07%
- Max drawdown: 16.11%
- Model calls: 20
- Tool calls: 1,044
- Input tokens: 6,053,749
- Cached input tokens: 4,952,576
- Output tokens: 216,318
- Cache hit rate: 81.81%
- Estimated cost with cache pricing: $0.685160
- Estimated cost without cache pricing: $2.821577
- Final position: 2,040 shares of `GUSH`, with about $9,902 cash
- Submitted orders: bought `SOXL`, sold `SOXL`, bought `GUSH`

`gemini-3.1-flash-lite`

- Status: passed
- Wall time: 1.79 minutes
- Return: -3.28%
- Max drawdown: 20.87%
- Model calls: 20
- Tool calls: 95
- Input tokens: 754,052
- Cached input tokens: 502,661
- Output tokens: 9,398
- Cache hit rate: 66.66%
- Estimated cost with cache pricing: $0.089511
- Estimated cost without cache pricing: $0.202610
- Final position: 547 shares of `SOXL`, with about $8,651 cash
- Submitted orders: one market buy for `SOXL`

## Takeaways

- All three model paths can run the simple AI Trading Team strategy and submit orders.
- Caching is working for all three tested paths. DeepSeek cached input pricing materially changes the economics.
- Gemini 3.1 Flash-Lite is dramatically faster on this exact prompt and universe.
- DeepSeek V4 Pro used fewer tool calls than Flash in the one-day smoke, but it was slower in the seven-day run.
- DeepSeek models are currently tool-hungry on the expanded 28-ETF universe. A full three-month benchmark is technically possible, but at the observed seven-day pace it is an overnight or multi-day job, not a same-turn interactive benchmark.
- The current prompt does not force market orders. DeepSeek V4 Pro used a limit buy in the one-day smoke, which did not fill. That is valid agent behavior, but it affects short-window comparisons.
