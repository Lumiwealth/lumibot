# AI Trading Team Provider Benchmarks - 2026-05-25

This note records the first provider comparison for the simple `AITradingTeamStrategy`
example that now lives at `lumibot/example_strategies/ai_trading_team_bull_bear_leveraged_etf.py`.
The old `lumibot/example_strategies/ai_trading_team.py` import path is kept as a compatibility wrapper.

Strategy universe:

`TQQQ`, `SQQQ`, `UPRO`, `SPXU`, `UDOW`, `SDOW`, `TNA`, `TZA`, `TECL`, `TECS`, `SOXL`, `SOXS`, `WEBL`, `WEBS`, `FAS`, `FAZ`, `LABU`, `LABD`, `ERX`, `ERY`, `GUSH`, `DRIP`, `DRN`, `DRV`, `TMF`, `TMV`, `NUGT`, `DUST`

Models tested:

- `deepseek/deepseek-v4-flash`
- `deepseek/deepseek-v4-pro`
- `gemini-3.1-flash-lite`

Artifact roots:

- One-day smoke: `/Users/robertgrzesik/Development/lumibot/artifacts/ai_trading_team_provider_benchmarks/20260524_232138`
- Seven-day window: `/Users/robertgrzesik/Development/lumibot/artifacts/ai_trading_team_provider_benchmarks/20260524_235130`
- README-window memory smoke:
  - `/Users/robertgrzesik/Development/lumibot/artifacts/ai_trading_team_provider_benchmarks/20260525_220127_693539_44135/gemini-3.1-flash-lite`
  - `/Users/robertgrzesik/Development/lumibot/artifacts/ai_trading_team_provider_benchmarks/20260525_220127_693541_44137/gemini-3.1-flash-lite`
  - `/Users/robertgrzesik/Development/lumibot/artifacts/ai_trading_team_provider_benchmarks/20260525_220127_693544_44136/gemini-3.1-flash-lite`

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

## README-Window Memory Smoke

Window: 2026-04-07 to 2026-05-22.

Command shape:

```bash
LUMIBOT_ALLOW_PAID_AI_TRADING_TEAM_BACKTEST=1 \
python3 scripts/run_ai_trading_team_provider_benchmark.py \
  --models gemini-3.1-flash-lite \
  --start 2026-04-07 \
  --end 2026-05-22 \
  --max-run-attempts 1 \
  --agent-run-timeout-seconds 1800 \
  --env-file .env.local
```

Three runs were started in parallel as separate processes. The process-level wall
time was about 15 minutes because the slowest individual run finished in 14.63
minutes.

External Yahoo adjusted-close reference points:

- 2026-04-07 close through 2026-05-21 close:
  - `SPY`: +12.67%
  - `QQQ`: +21.39%
  - `SOXL`: +215.46%
- 2026-04-07 close through 2026-05-22 close:
  - `SPY`: +13.11%
  - `QQQ`: +21.91%
  - `SOXL`: +236.98%

Important audit note: those adjusted-close reference points are not exactly the
same as Lumibot's simulated execution and valuation path. The three backtests
iterate through 2026-05-21, and the agent saw `SOXL` at $161.00 on the final
iteration. Runs 1 and 2 bought 1,860 shares of `SOXL` once on 2026-04-07 and
held it. Their +198.42% result is therefore best interpreted as the Lumibot
execution-path buy-and-hold result for that initial `SOXL` trade, not as a
separate active-trading edge over `SOXL`.

Run 1:

- Artifact root: `/Users/robertgrzesik/Development/lumibot/artifacts/ai_trading_team_provider_benchmarks/20260525_220127_693539_44135/gemini-3.1-flash-lite`
- Status: passed
- Return: +198.42%
- Max drawdown: 23.24%
- Wall time: 13.39 minutes
- Model calls: 132
- Tool calls: 1,100
- Input tokens: 4,615,701
- Cached input tokens: 3,273,066
- Output tokens: 103,621
- Cache hit rate: 70.91%
- Estimated cost with cache pricing: $0.572917
- Estimated cost without cache pricing: $1.309357
- Final position: 1,860 shares of `SOXL`, with about -$1,035 cash
- Memory artifacts: 46 memory events, 10 retrievals, 13 current memory-state rows
- Memory result: no `position_order_without_memory_thesis` warnings

Run 2:

- Artifact root: `/Users/robertgrzesik/Development/lumibot/artifacts/ai_trading_team_provider_benchmarks/20260525_220127_693541_44137/gemini-3.1-flash-lite`
- Status: passed
- Return: +198.42%
- Max drawdown: 23.24%
- Wall time: 13.16 minutes
- Model calls: 132
- Tool calls: 1,195
- Input tokens: 4,366,934
- Cached input tokens: 2,799,200
- Output tokens: 117,594
- Cache hit rate: 64.10%
- Estimated cost with cache pricing: $0.638305
- Estimated cost without cache pricing: $1.268124
- Final position: 1,860 shares of `SOXL`, with about -$1,035 cash
- Memory artifacts: 6 memory events, 8 retrievals, 6 current memory-state rows
- Memory result: no `position_order_without_memory_thesis` warnings

Run 3:

- Artifact root: `/Users/robertgrzesik/Development/lumibot/artifacts/ai_trading_team_provider_benchmarks/20260525_220127_693544_44136/gemini-3.1-flash-lite`
- Status: passed
- Return: +198.81%
- Max drawdown: 23.17%
- Wall time: 14.63 minutes
- Model calls: 132
- Tool calls: 1,015
- Input tokens: 5,713,162
- Cached input tokens: 4,050,556
- Output tokens: 107,294
- Cache hit rate: 70.90%
- Estimated cost with cache pricing: $0.677856
- Estimated cost without cache pricing: $1.589231
- Final position: 1,856 shares of `SOXL`, with about -$7 cash
- Memory artifacts: 65 memory events, 30 retrievals, 32 current memory-state rows
- Memory result: no `position_order_without_memory_thesis` warnings

Observations:

- The README-window behavior reproduced: all three runs identified `SOXL` as the
  strongest ETF on every researcher call and returned about +198% over the
  README window.
- Runs 1 and 2 were effectively one-trade `SOXL` buy-and-hold runs. Run 3
  briefly rotated from `SOXL` to `LABU` on 2026-05-11, reversed back to `SOXL`
  on 2026-05-12, and still ended near the same result.
- The team beat `SPY` and `QQQ` during this window. It should not be described
  as beating `SOXL`; on the two identical runs, it mostly replicated a leveraged
  `SOXL` hold after selecting it on the first day.
- SQLite-backed memory exported all three queryable Parquet artifacts:
  `*_memory_events.parquet`, `*_memory_retrievals.parquet`, and
  `*_memory_state.parquet`.
- `Lumibot Memory State JSON` was present on every agent call summary row.
- The non-blocking position-thesis warning did not fire, which means these runs
  did not show the trader changing a held symbol without first retrieving memory.
- The remaining noise is mostly DuckDB query syntax/column mistakes from the
  model. Those are prompt/tool-ergonomics issues, not memory-store failures.
- The convergence appears to come mainly from the example design: the researcher
  ranks 252-day ETF momentum every day, `SOXL` dominates that ranking throughout
  the window, and the trader prompt says to buy one ETF aggressively with nearly
  all cash. Memory likely makes the hold rationale stickier, but the first-day
  `SOXL` selection happens before memory has any history.

## Takeaways

- All three model paths can run the simple AI Trading Team strategy and submit orders.
- Caching is working for all three tested paths. DeepSeek cached input pricing materially changes the economics.
- Gemini 3.1 Flash-Lite is dramatically faster on this exact prompt and universe.
- DeepSeek V4 Pro used fewer tool calls than Flash in the one-day smoke, but it was slower in the seven-day run.
- DeepSeek models are currently tool-hungry on the expanded 28-ETF universe. A full three-month benchmark is technically possible, but at the observed seven-day pace it is an overnight or multi-day job, not a same-turn interactive benchmark.
- The current prompt does not force market orders. DeepSeek V4 Pro used a limit buy in the one-day smoke, which did not fill. That is valid agent behavior, but it affects short-window comparisons.
