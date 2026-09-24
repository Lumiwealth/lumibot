# AI strategy backtests, September 23, 2026

Every AI example strategy was rerun on `openai/gpt-6-luna` (high reasoning), the
new LumiBot default. Each run was inspected trade by trade for lookahead, sizing,
entries and exits, and cash. Artifacts (`*_trades.csv`, `*_settings.json`,
`*_tearsheet_metrics.json`, logs) sit in this folder; `stats.csv` files are in
`logs/`. The runner is
`scripts/run_ai_strategy_flash_backtests.py`
(`--wave N --already <spent>`), with a $25 cap. Total model spend for the day
was about $22.72 before the evening reruns and about $24 after them (the spend.txt figure is slightly higher because each wave started from a padded baseline).

## Latest result per strategy

| Run | Window | Data | Result | Checked |
|---|---|---|---|---|
| large-cap-luna-v3 | Jan 5 to 15 | Yahoo | -2.16% (SPY about +1%) | Cash never negative (low $206). No same-day buy and sell. |
| leveraged-etf-luna-v7 | Jan 5 to 15 | Yahoo | +1.47% | UPRO, then UPRO with TQQQ. Never an ETF with its inverse. Cash low $189. v6 skipped every rebalance after netting; fixed by the rescale rule. |
| congress-pelosi-luna-v4 | Jan 22 to 29 | Yahoo | +8.7% | Bought AB, GOOGL, TEM, VST at the Jan 26 open, the first session after the Jan 23 filing. Cash low $1,436. |
| public-fetch-luna-v3 | Jan 22 to 29 | Yahoo | +7.3% | Same first-eligible-open timing. Cash low $63. |
| buffett-luna-v2 | Jan 5 to 15 | Yahoo | +2.64% | 708 PG, nearly fully invested. Cash low $207. |
| ackman-luna | Jan 5 to 15 | Yahoo | +3.95% | GOOGL, MSFT, later UBER. Cash low $513. One tiny Jan 14 tweak (sold 1 GOOGL, bought 2 MSFT) predates the churn rule. |
| iron-condor-luna-v3 | Jan 5 to 15 | Alpaca options | ended $99,734 | 38 SPY Feb 645/650 put and 715/720 call spreads at real Alpaca prices. |
| credit-spread-luna-v3 | Jan 5 to 15 | Alpaca options | ended $100,627 | 33 SPY Feb 655/650 put spreads. |
| spy-0dte-luna-v5 | Jan 6 to 7 | Alpaca options | ended $98,640 | 40-lot same-day call spreads opened and closed. |
| orb-luna-v3 | Jan 5 to 6 | Alpaca minute | -0.08% | DE, DIS, SPGI at about 10%. SPGI was bought and sold inside the same bar (predates the churn rule; not rerun). |
| vwap-luna-v5 | Jan 5 to 6 | Alpaca minute | flat | No dip-and-reclaim appeared, so no order. |
| researcher-trader-luna | Jan 5 to 15 | Yahoo | flat | 14 SPY, about 10% by design. |

Alpaca options backtesting works: chains and option bars load and fills use
real Alpaca prices.

## General fixes made (no strategy-specific hacks)

All in `lumibot/example_strategies/`:

1. `agent_cycle.py` `trader_prompt`: plan every order from one account read,
   leave holdings within 2 points of target, never buy and sell the same
   symbol in a session (a14f930e).
2. Bull-bear exit rule: sell only what today's weights dropped instead of the
   whole book every morning (ee239cc1, 989a9f88).
3. `trader_prompt` cash rule: new buys stay below cash plus sale proceeds with
   about 1% spare, never negative cash (989a9f88).
4. Leveraged ETF book: one direction per index, and sell the whole opposite
   side before switching (989a9f88, 2da15c25).
5. `trader_prompt`: rescale weights a book rule netted away instead of
   skipping the rebalance (a239bf44).
6. Congress strategy: the researcher line carries the `[ST]`/`[AB]` code the
   trader checks (a66449e6). Pelosi v3 had refused every ticker without it.

## Marketplace listings withdrawn

Every AI listing was checked by reading its backing backtest's `stats.csv`
through the owner BotSpot MCP. These went below zero cash (the engine before
the 515fc712 fill-drain fix let one session's buys all see the same cash), so
their tear sheets overstated results. Unpublished today:

- Citadel-Style Sector Pods (leveraged): cash low -$58,933
- Citadel-Style Sector Pods, Regular: -$24,775
- Ray Dalio Idea Meritocracy: -$430,482
- Ray Dalio Idea Meritocracy, Leveraged: -$17,462
- Gemini AI Daily Stock & ETF Market Analyst: -$27,150
- Sector Rotation AI Multi-Pod Strategy: -$4,395

Earlier the same day: AI Multi-Agent Large-Cap Growth, AI Leveraged ETF
Rotation, Buffett-Style AI Value Investor, Bill Ackman Concentrated Stock Team.

Still public and cash-clean: AI Investment Committee (low $2,232), AI-Powered
Crypto Breakout (low $81,157), Macro Insight AI: Bridgewater-Style (low $2).
Non-AI listings were not audited.

## Open items

- No new listing was published. Republishing needs a backtest on BotSpot
  itself; these runs were local.
- ALPACA_NEWS credentials are missing; ThetaData is off; FRED is not set up.
- sec-insider-luna-v4 and orb-luna-v3 predate the churn and cash rules.
