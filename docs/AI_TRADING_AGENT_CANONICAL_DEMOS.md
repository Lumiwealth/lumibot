# AI Trading Agent Canonical Demos

> Four reference strategies that validate the AI agent runtime end to end.

**Last Updated:** 2026-03-30
**Status:** Active
**Audience:** Both

---

## The Four Demos

All four demos use the `@agent_tool` pattern with the `requests` library:

1. **News Sentiment Strategy** (`agent_news_sentiment.py`) -- event-driven stock selection using Alpaca news data
2. **Macro Risk Strategy** (`agent_macro_risk.py`) -- macro regime allocation using Alpaca market data
3. **Momentum Allocator Strategy** (`agent_momentum_allocator.py`) -- momentum and sentiment allocation using Alpaca price bars and news
4. **M2 Liquidity Strategy** (`agent_m2_liquidity.py`) -- liquidity-driven allocation using FRED money supply data

---

## Why These Demos Exist

They validate:

- `@agent_tool` wrapping REST APIs with the `requests` library
- Source code auto-inclusion in tool descriptions
- Docstrings with `Args` sections for parameter documentation
- Built-in tool usage (positions, portfolio, prices, DuckDB, orders)
- Replay cache behavior (deterministic warm reruns)
- Observability (traces, summaries, warnings)
- Benchmarked tearsheets against SPY

---

## News Sentiment Strategy

**File:** `agent_news_sentiment.py`

Event-driven stock selection using news data from the Alpaca News API.

**Tool:** `search_news` -- wraps `https://data.alpaca.markets/v1beta1/news` via `requests`

**What it does:** Searches recent stock market news headlines, identifies strong catalysts (earnings beats, upgrades, product launches, deals), and buys stocks with positive sentiment. Parks capital in SHV when news is negative or unclear. Holds 2-4 equity positions.

Validates:
- `@agent_tool` wrapping the Alpaca News REST API
- Agent-driven stock discovery from news sentiment
- Portfolio rotation and no-trade decisions
- Trace readability for real external data

---

## Macro Risk Strategy

**File:** `agent_macro_risk.py`

Binary allocation between TQQQ and SHV based on market trends using Alpaca market data.

**Tools:**
- `get_stock_bars` -- wraps `https://data.alpaca.markets/v2/stocks/{symbol}/bars` for OHLCV data
- `get_market_movers` -- wraps `https://data.alpaca.markets/v1beta1/screener/stocks/movers` for top gainers/losers

**What it does:** Checks TQQQ and SPY price trends using historical bars. If TQQQ is trending up, allocates fully to TQQQ. If trending down, allocates fully to SHV. Uses market movers for additional context.

Validates:
- Multiple `@agent_tool` functions in a single strategy
- Agent discovery of market trends from price data
- Binary allocation between TQQQ and SHV
- De-risking during adverse market conditions
- Benchmarked evaluation against SPY

---

## Momentum Allocator Strategy

**File:** `agent_momentum_allocator.py`

Momentum and sentiment allocation combining Alpaca price bars with news data.

**Tools:**
- `get_stock_bars` -- wraps the Alpaca bars API for OHLCV data
- `search_news` -- wraps the Alpaca News API for recent headlines

**What it does:** Combines price momentum (recent closes higher than earlier closes) with news sentiment to decide between TQQQ and SHV. Buys TQQQ when momentum is positive and news is not terrible. Switches to SHV when momentum is negative or news is very negative.

Validates:
- Combining multiple data sources through `@agent_tool`
- Momentum-based allocation with sentiment confirmation
- Agent reasoning over both quantitative and qualitative inputs
- Replay caching with multiple external tool calls per iteration

---

## M2 Liquidity Strategy

**File:** `agent_m2_liquidity.py`

Liquidity-driven allocation using FRED (Federal Reserve Economic Data) money supply data.

**Tool:** `get_fred_series` -- wraps `https://fred.stlouisfed.org/graph/fredgraph.csv` for economic data (no API key needed)

**What it does:** Fetches M2 money supply data and compares recent values to values from 3-6 months ago. If M2 is growing, allocates to TQQQ. If M2 is flat or shrinking, allocates to SHV. Can also check FEDFUNDS and T10Y2Y for confirmation.

Validates:
- `@agent_tool` wrapping a public government data API
- AI reasoning over money supply and liquidity data
- Defensive parking behavior
- Benchmarked tearsheet generation

---

## What to Inspect for Every Demo

- Summary log lines
- Trace JSON files
- Trades chart and `trades.csv`
- `trade_events.csv`
- Tearsheet with benchmark comparison
- Replay cache status (warm reruns should show zero model calls)
