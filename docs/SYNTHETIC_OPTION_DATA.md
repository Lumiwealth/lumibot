# SYNTHETIC_OPTION_DATA.md: Synthetic Option Data for Backtests

> Design note for the `synthetic` option data route: option chains, prices, bid/ask and greeks computed from the underlying's bars, always labeled as synthetic.

**Last Updated:** 2026-11-14
**Status:** Built, not released. Accuracy measured (see "How accurate is it"). Not accurate enough to be relied on without a warning.
**Audience:** Developers, AI Agents (engineering docs). Public page: `docsrc/backtesting.synthetic_options.rst`.

## Overview

A backtest that trades options needs option prices. When no option data is available, the
`synthetic` route computes them from the underlying's own bars. This was approved by the
project owner as an explicit, labeled exception to RULE #1 in `BACKTESTING_ARCHITECTURE.md`
("never fabricate data"):

- It is never silent. It only runs when the routing says `"option": "synthetic"`.
- Every bar, quote and fill it produces is labeled `synthetic`. A notice is logged at the
  start, and `data_provenance.json` reports the count and share of synthetic fills and the
  volatility input used.
- It only makes option data. Underlying bars are never made up: they come from the
  underlying's normal route, and when they are missing there is no option price either.

## How to turn it on

```python
config = {"backtesting_data_routing": {"default": "ibkr", "option": "synthetic"}}
```

Optional: `"option_real": "<route>"` names a real option route. Bars for which that route has a
real quote or trade override the synthetic bars (each bar keeps its own label).

| Setting | Default | Meaning |
|---|---|---|
| `LUMIBOT_SYNTHETIC_OPTION_VOL_INPUT` | `auto` | `auto`: volatility index when available, else realized volatility. `index`, `realized` force one. |
| `LUMIBOT_SYNTHETIC_OPTION_VOL_RISK_PREMIUM` | `1.15` | Implied / realized volatility ratio for the realized input. |
| `LUMIBOT_SYNTHETIC_OPTION_RATE` | `0.04` | Constant interest rate. |
| `LUMIBOT_SYNTHETIC_OPTION_DIVIDEND_YIELD` | `0` | Constant dividend yield. |
| `LUMIBOT_SYNTHETIC_OPTION_PARAMS` | unset | Path of a JSON parameter file (per-underlying shape parameters). |
| `LUMIBOT_SYNTHETIC_OPTION_SPREAD_ON_REAL_TRADES` | off | With `option_real`: put a synthetic bid/ask around real trades that have no quote. Changes results, so it is off. |

## Design

Code: `lumibot/tools/synthetic_options.py` (pure model), `lumibot/backtesting/synthetic_option_routing.py`
(routing adapter and cache), small hooks in `routed_backtesting.py`, `backtesting_broker.py`
and `data_provenance.py`.

### 1. Chains (rule-based calendar)

Small tables at the top of `synthetic_options.py`, easy to extend:

- Expirations: every Friday for 8 weeks, third Friday of each month for 12 months. A Friday
  holiday moves to the trading day before. Underlyings in `DAILY_EXPIRATION_SYMBOLS` also list
  every trading day for 35 days.
- Strikes: step by symbol (`STRIKE_STEP_BY_SYMBOL`) or by price level (`STRIKE_STEP_BY_PRICE`),
  listed 15% / 30% / 50% around the previous close for expirations up to 7 / 45 / more days.
  The previous close is used so the chain is known before the session.
- A contract outside the listed range can still be priced: pricing does not depend on the chain.
- Not built: reading the contract list from a real route. The adapter has one place for it
  (`SyntheticOptionEngine.chains`).

### 2. Prices

Black (1976) on the forward `F = S * exp((r - q) * T)`, time in calendar years to 16:00 New
York on the expiration day. Volatility for a contract is built in three steps:

1. Level: the 30-day volatility known at the previous close.
   - (a) Real option trades from `option_real`: the contract's own last real trade shifts its
     synthetic volatility from then on (never backward). The chain-wide smile fit of
     `option_price_estimate.py` is not wired into this route.
   - (b) A volatility index of the underlying (`VOLATILITY_INDEX` table), fetched as daily bars
     through the normal index route: previous close / 100. Optional: any failure falls to (c).
   - (c) Realized volatility of the underlying's daily closes (exponentially weighted, decay
     0.94, blended 70/30 with the one-year value) times a risk premium.
   - Intraday, the level moves against the underlying since the previous close:
     `level * exp(-beta * ln(S / previous close))`, beta 6 index-like, 3 others, capped.
2. Term structure: variance reverts from the 30-day level to the long-run level at speed 4 per
   year, then an optional table of multipliers by days to expiry (all 1.0 by default).
3. Skew: `1 + slope * z + curve * z^2` with `z = ln(K/F) / (vol * sqrt(T))`, clipped at 4 and
   floored at 0.75. Defaults: index-like -0.12 / 0.03, others -0.06 / 0.03.

American exercise premium and discrete dividends are not modeled. Everything is deterministic:
same bars and settings give the same prices.

All shape numbers live in `ModelParameters`. The built-in values are neutral, generic defaults.
A JSON file (`LUMIBOT_SYNTHETIC_OPTION_PARAMS`) can replace them for all underlyings, a class
or a symbol. The file holds parameters only, never prices:

```json
{"version": "example-1",
 "default": {"risk_premium": 1.2},
 "kinds": {"index": {"skew_slope": -0.15}},
 "symbols": {"XYZ": {"spot_vol_beta": 2.0, "term_multiplier": [[0, 1.3], [30, 1.0]], "spread_scale_calm": 1.5}}}
```

### 3. Bid and ask

`half_spread()` starts from measured median half-spreads of index options (10.0 cents on calm
days, 12.5 cents on volatile days, underlying near 6000) and scales by: underlying price
(linear), absolute delta (0.25x far out of the money to 9.5x deep in the money), days to
expiry (square root, relative to 7 days), and up to 4x when the underlying's bar range is far
above what its volatility implies. It is kept between a minimum share of the mid (0.25%
index-like, 1.5% others) and 40% of the mid. Quotes are on a one-cent grid.

Fills use the existing rules: buys at the ask, sells at the bid; limit and smart-limit orders
work against the synthetic quote.

### 4. Real data wins, bar by bar

With `option_real`, each bar is one of: `quote` (real bid/ask), `trade` (real trade, no
quote), `trade_synthetic_spread` (real trade with a synthetic bid/ask, only when the setting
above is on) or `synthetic`. The label is stored per bar and copied to each fill.

### 5. Labels

- Trades file: `price_source=synthetic` on every fill priced from a synthetic bar, on every
  order path (market, limit, smart limit, multi-leg).
- `data_provenance.json`: `optionFillPrices` (counts), `optionFillPriceShares`, and
  `syntheticOptions` (model version, volatility input, parameter version, notice).
- Log, once: "Option prices in this backtest are synthetic: computed from the underlying price
  and volatility. They are estimates."
- Volume and open interest are NaN (not available).

### 6. Speed and cache

- One expiration-day (390 minute bars x 358 contracts, all prices, greeks, bid/ask): about
  30 ms on a laptop. Target was 300 ms.
- Cached: the per-bar model inputs of one underlying-day (underlying bars, previous close,
  volatility level, long-run level), not prices. Prices are recomputed from them in
  microseconds per contract. File: `synthetic_options/v1/<SYMBOL>/<timestep>/<SYMBOL>_<date>_<timestep>_synthetic_v1_<parameters>.parquet`
  in the normal local cache, mirrored to the remote cache when one is configured. About 25 KB
  per symbol-day of minute bars. The name contains "synthetic", the model version and a
  fingerprint of the parameters.

## How accurate is it

Checked against real option trade prices (1-minute trade bars from a broker data feed, SPY and
QQQ, strikes within 10% of the underlying, expirations 0 to 45 days), built-in default
parameters, nothing tuned to this sample. 1.85 million trade bars. Calm: June 10 to 14, 2024.
Stressed: August 5, 2024 and April 3, 4, 7, 8, 9, 2025. Error = synthetic mid minus real trade
price at the same minute.

Contracts from 5 delta up:

| Days | Volatility input | Median error | 90th percentile | Median % of price | Median signed error |
|---|---|---|---|---|---|
| Calm | (b) volatility index | 34.5 cents | 107.6 cents | 21.7% | -14.1 cents |
| Calm | (c) realized volatility | 37.3 cents | 103.4 cents | 22.6% | -12.9 cents |
| Stressed | (b) volatility index | 198.4 cents | 476.8 cents | 28.8% | -120.4 cents |
| Stressed | (c) realized volatility | 211.2 cents | 541.9 cents | 31.3% | -170.8 cents |

By moneyness (absolute delta of the synthetic price), median cents / median % of price:

| Moneyness | Calm (b) | Calm (c) | Stressed (b) | Stressed (c) |
|---|---|---|---|---|
| Under 5 delta | 3.0 / 98% | 2.9 / 98% | 19.9 / 100% | 25.0 / 100% |
| 5 to 15 delta (wings) | 18.0 / 45% | 18.6 / 49% | 147.0 / 77% | 219.2 / 83% |
| 15 to 40 delta | 45.9 / 35% | 55.1 / 37% | 257.5 / 51% | 273.9 / 59% |
| 40 to 60 delta (at the money) | 79.1 / 24% | 85.7 / 24% | 298.3 / 24% | 276.1 / 25% |
| Over 60 delta | 19.8 / 5% | 20.1 / 5% | 108.6 / 9% | 120.2 / 9% |

By days to expiry, median cents / median % of price / median signed cents:

| Days to expiry | Calm (b) | Calm (c) | Stressed (b) | Stressed (c) |
|---|---|---|---|---|
| 0 | 17.4 / 16% / -17 | 16.3 / 15% / -16 | 116.2 / 30% / -115 | 119.6 / 28% / -118 |
| 2 to 7 | 51.1 / 29% / -41 | 62.7 / 31% / -43 | 244.1 / 36% / -244 | 373.2 / 48% / -373 |
| 8 to 30 | 43.6 / 14% / +43 | 52.4 / 14% / +52 | 219.6 / 17% / +159 | 208.1 / 19% / -98 |
| 31 to 45 | 39.3 / 9% / +37 | 53.7 / 11% / +53 | 351.5 / 22% / +339 | 205.1 / 11% / +142 |

Share of synthetic mids within the synthetic half-spread of the real trade: about 5% calm,
2% stressed.

### What this says, plainly

- **The error is large.** A typical synthetic price is 20 to 30% away from where the contract
  really traded; on stressed days it is dollars, not cents. Option prices are roughly
  proportional to implied volatility, and without option market data the volatility of a
  given day is only known to within a few volatility points.
- **Wings are worse.** 5 to 15 delta contracts are 45 to 50% off on calm days and about 80%
  off on stressed days, mostly too cheap. A condor's credit and its tail risk depend on those
  contracts, so iron condor results on synthetic data are rough.
- **Short expirations are too cheap, longer ones too expensive** (the sign flips around one
  week). The default term structure does not capture event and same-day premium.
- **Stressed days are not captured.** The volatility level is set from the previous close; an
  overnight shock is only followed through the fixed spot-volatility rule. Both inputs priced
  options far too cheap on those days.
- **A volatility index (b) is only a little better than realized volatility (c)** with default
  parameters.
- **The synthetic spread is much narrower than the pricing error**, so "buys at the ask, sells
  at the bid" does not make results conservative.
- Structure matters less than level: relative prices inside one expiration (verticals,
  butterflies) are more reliable than absolute premium collected.

Verdict: usable to check that a strategy's logic runs (entries, exits, sizing, expiry
handling) and for a rough idea of results, with a visible warning. Not accurate enough to
treat backtest profit and loss as an estimate of real profit and loss. Real quotes or trades
(`option_real`, or any real option route) are far better where they exist: with real trades as
the volatility input, the existing estimator was measured at about 1 to 2% at the money.

Parameters can be fitted per underlying against real quotes and supplied through the parameter
file. That reduces the bias (level, term structure, skew, spread scale) but not the day-to-day
uncertainty of volatility, which is the larger part of the error.

## Tests

- `tests/test_synthetic_options.py`: calendar and strike rules; no-arbitrage basics (put-call
  parity, monotonic in strike, non-negative, at least intrinsic, intrinsic at expiry);
  determinism; skew, term structure and volatility input order; spread model; speed; real bars
  overriding synthetic bars; the opt-in synthetic spread on real trades; cache names; the
  parameter file; arbitrage-consistent package prices for vertical credit and debit spreads,
  box, iron condor, butterfly, iron butterfly, straddle, strangle, calendar and ratio spreads,
  calm and stressed.
- `tests/backtest/test_synthetic_option_route.py`: end-to-end routed backtests on made-up
  underlying bars: put credit spread and 4-leg iron condor fill at the synthetic bid/ask and
  settle at expiry; an 8% gap down (settlement, prices and spreads react); smart-limit fills
  inside the synthetic spread; chain rules; provenance counts and shares.

## Not done

- No release. No platform default was changed.
- American exercise premium and discrete dividends.
- Chain-wide smile fit from real trades inside this route (only per-contract anchoring).
- A per-underlying implied-volatility history as input (b) for single stocks. The table only
  knows index-style volatility indexes. A route that can serve such a series would plug in at
  `SyntheticOptionEngine.surface`.
- An earnings-date volatility bump for single stocks.
- Contract lists from a real route for the chain.
- The synthetic spread around real trades (`LUMIBOT_SYNTHETIC_OPTION_SPREAD_ON_REAL_TRADES`)
  exists only inside the synthetic route with `option_real`; routes that serve real option
  trades by themselves are unchanged.
- Daily-bar backtests run (one quote per day, at the open) but were not accuracy-checked.
