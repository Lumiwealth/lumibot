# AI strategy inspection, 2026-09-23

Every run below used `gemini-3.5-flash-lite` through `initialize` and `on_trading_iteration` only. The example strategy files do not contain `execution_mode`, `create_order`, or a proof switch. Orders came from the trading agent's order tool, or there was no order.

Spend for this batch: $3.09 of a $25 hard cap (inside the $20 to $30 limit). A no-order result was left as a no-order result.

The old `docs/research/tearsheets/2026-09-22-*.html` files are not AI results. See `docs/research/tearsheets/2026-09-22-INVALID.md`.

Trades below are `status=fill` rows from `docs/research/2026-09-23-ai-strategy-backtests/`.

## Passed

### Large-cap stocks

Window 2026-01-05 to 2026-01-08. Universe AAPL, MSFT, NVDA, AMZN. Account $100,000.

- 2026-01-05 bought NVDA 263, AAPL 73, AMZN 86, MSFT 20. Notional about $99,400.
- 2026-01-06 sold all four, then bought NVDA 234, AMZN 152, MSFT 21, AAPL 36.
- 2026-01-07 sold those four, then bought AMZN 215, NVDA 157, MSFT 21, AAPL 37.

Entries, exits, and the next-day rotation are real agent-tool fills, sized to the account.

### Leveraged ETFs

Same window. Universe TQQQ, SQQQ, UPRO, SPXU.

- 2026-01-05 bought SQQQ 450 and SPXU 1430. Notional about $99,900.
- 2026-01-06 sold both, then bought UPRO 338 and SPXU 1229.
- 2026-01-07 sold the 1229 SPXU and bought SPXU 615. The UPRO 338 was still open at the end of this short window.

Rotation happened. The last UPRO lot did not exit inside three days.

## Failed

### Nancy Pelosi

Filing 20033725 is signed 2026-01-23. Counted stock and units book, using range midpoints and skipping gifts, spinoffs, options, and the GOOGL donor-advised line: AB, GOOGL, TEM, VST. Do not buy AAPL, AMZN, NVDA, PYPL, or DIS. MSFT is not in the filing.

| Run | First trade day | What filled | Judgment |
| --- | --- | --- | --- |
| v1 | 2026-01-23 | AB, GOOGL, NVDA, AMZN, AAPL, VST, TEM, about $97k | Clock ok. Book wrong (bought net sales). No exit. |
| v2 | 2026-01-26 | AB, GOOGL, NVDA, AMZN, VST, TEM, about $342k | Clock ok. Book wrong. Size wrong. No exit. |
| v3 | 2026-01-26 | AB, GOOGL, AAPL, NVDA, VST, TEM, about $314k | Clock ok. Book wrong. Size wrong. No exit. |
| v4 | 2026-01-23 | NVDA 324, TEM 181, VST 168, about $100k | Clock ok. Size ok. Missed AB and GOOGL. No exit. |
| v5 | 2026-01-22 | AB 2299, TEM 34, VST 33, about $101k | Lookahead fail. Signature is the next day. Missed GOOGL because the model counted the donor-advised line as a sale. It also invented MSFT in the research line and did not buy it. Later sessions held. No exit. Cash finished about -$1,100. |

Five prompt passes did not produce AB, GOOGL, TEM, and VST on or after 2026-01-23. v1 through v4 left 2026-01-22 flat. v5 traded that day. The PDF text reached the model as one line per transaction. The model did the netting and the clock wrong.

### Iron condor

- Alpaca: `options_get_chain` returned no contracts. The agents placed no order. That chain method is empty, so this is not a traded condor.
- Polygon, first attempt: killed while downloading the full SPY chain. No order.
- Polygon, second attempt, 2026-01-05: chain was available. Researcher proposed 2026-02-06, short put 667, short call 712, wings at 662 and 717. Trader refused. Verified short-put delta was -0.2604, outside 0.04 of the 0.16 target. No `orders_submit_multileg` call.
- Next session, 2026-01-06: closest put delta found was -0.2312, still outside the band, while Polygon slept 60 seconds per extra strike. That process was stopped. No condor fill, no take-profit, no stop, no 21 DTE exit.

### VWAP

Daily bars at the open have no completed intraday reclaim. First session also hit a model 503. Trades file has a header only. No VWAP entry or exit.

### Buffett

2026-01-05 bought AAPL 146, JPM 93, AXP 54. About $90k. KO was not bought. Later days were 1-share adds and trims. Not a rotation and not an exit of the book.

### Ackman

2026-01-05 bought GOOGL 94, CMG 671, UBER 304, HLT 69. About $100k. No sells in the window. Entry size is right. Exit is not shown.

## Not a traded proof yet

Opening range, credit spread, and SPX were not run in this batch. Opening range is hourly, so a wide universe multiplies model calls. Credit spread and SPX need an option chain. The Polygon chain path was rate-limited during the iron condor run.

Gottheimer and McClain do not have their own strategy files. The congress example is the Pelosi filing set.

Ray Dalio and Citadel files were not run.

## Longer window, same cap

Spend after this batch: $4.97. Same model. Same four-name universes for the two stock examples. Window 2026-01-05 through 2026-01-15.

### Large-cap, longer

Every session sold the prior book and bought the new weights. The interpreter kept about 20% in cash and 0% in MSFT. Typical book was about 50% AAPL, 20% AMZN, 10% NVDA, near $80k of a $100k account. On 2026-01-15 it flipped to about 50% AMZN, 20% NVDA, 10% AAPL. Entries and exits are real. Size is light versus a fully invested account, and MSFT was never bought.

### Leveraged ETFs, longer

2026-01-05 placed no order because the trader call returned HTTP 503. From 2026-01-06 the interpreter assigned 50% to one ETF and 50% cash. Fills: SQQQ 747, then sold and bought UPRO 421, held, then sold and bought SPXU 1027, then sold and bought UPRO 411, then sold and bought UPRO 398. Each switch closed the old ETF first. Size is about half the account, not fully invested. The last session held the open UPRO lot because the target had not changed.

### SEC insider filings

2026-01-05, 2026-01-06, and 2026-01-07. The research agent read the live Form 4 feed. Every entry was published in September 2026, after the backtest day. The trader placed no order. That is the correct lookahead result for this feed. It does not show a copied insider purchase, because a January clock cannot see a September filing.

### Public page

The page is Pelosi filing 20033725. The trader placed no order. It treated the filing as many lagged lines, not one ticker and one size. The trades file has a header only.

## Still open

Opening range, credit spread, and SPX were not run. They need an hourly loop or an option chain. The iron condor chain path was rate-limited and then refused the structure on delta. Pelosi still does not match the filing. Those were not rerun in the longer batch.
