# BROKER_ORDER_SEMANTICS.md

> Notes on live broker behavior that affect backtesting semantics (extended hours, order types, and “market closed / no data” handling).

**Last Updated:** 2026-09-05
**Status:** Active
**Audience:** Developers, AI Agents

---

## Why this doc exists

LumiBot’s stated accuracy goal is **live broker realism** (see `docs/BACKTESTING_ACCURACY_VALIDATION.md`).

However, “what happens when the market is closed?” is broker- and asset-class-specific:
- some brokers support extended hours for equities
- some order types are rejected outside regular/extended sessions
- futures have daily maintenance gaps + weekend gaps + holiday early closes
- crypto trades 24/7 but historical datasets can still have **data gaps**

This document records:
1) what we can cite from public broker docs as of the timestamp above
2) what we still must validate via live/paper `apitest` smoke tests

---

## Guiding principles for backtesting (matching brokers without overfitting calendars)

### Principle A — Execution correctness is data-driven

Even when the strategy uses `set_market("24/7")` for mixed-asset scheduling, fills must be based on **actionable data availability**:

- If there is OHLC (or otherwise a last-trade price series) at time `t`, we can execute fills at `t` following the fill model.
- If OHLC is missing but quotes (bid/ask) exist and the modeled order type supports it, quotes can be used.
- If neither OHLC nor quotes exist, no fill is possible.

See: `docs/BACKTESTING_SESSION_GAPS_AND_DATA_GAPS.md`.

### Principle B — “What would the broker do?” is broker- and product-specific

Backtesting must not assume a single universal rule for “market closed” because:
- Alpaca equities have extended hours
- some brokers require limit orders in extended sessions
- futures brokers differ on stop triggering outside “RTH”

When behavior differs across brokers, we need broker-scoped semantics (or a documented approximation).

### Crypto-futures close invariant

`Position.get_selling_order()` intentionally does not synthesize a generic
crypto-futures sell because a plain opposite-side order can increase or reverse
exposure. The broker owns the closing semantics:

- `Broker.close_position()` must always create a side-correct reduce-only order
  for a nonzero crypto-futures position: sell a long and buy a short.
- Partial closes scale the absolute position quantity by a fraction in `(0, 1]`.
- `sell_all()` must never pass `None` into bulk submission.
- No broker, including `BacktestingBroker`, may submit a null order.
- Backtesting applies the close fill to the tracked position and removes it when
  the quantity reaches zero.

### Live crypto history completeness invariant

Live history must not silently return fewer bars because of a provider page
limit or inclusive timestamp cursor:

- Bitunix uses mapped native intervals when available and paginates bounded
  `startTime`/`endTime` windows with at most 200 candles per request.
- CCXT advances `since` to `last_candle_timestamp + timeframe`.
- If the available provider history is genuinely shorter than requested, the
  data source raises a diagnostic with returned and requested counts.

---

## Broker notes (public sources, summarized)

### Unknown Broker Objects And Refresh Resilience

Live brokers can return account records that LumiBot does not fully understand yet:

- future order types or statuses added by the broker
- option exercise/account-history rows
- mutual funds, bonds, sweep funds, cash-equivalent rows, or future asset classes
- partial rows with fields missing or formatted differently than expected

LumiBot should not fail an entire order, position, or balance refresh because one broker row is unfamiliar. The current policy is:

- Preserve representable unknown orders and positions with `UNKNOWN` enum values.
- Attach raw broker metadata to the returned entity so the row can be diagnosed later.
- Warn instead of raising for unknown broker asset/order/status/side values.
- Skip only truly unrepresentable rows, such as no usable symbol/instrument identifier or non-numeric quantity.
- If a Schwab position refresh is degraded by skipped rows, update parsed rows but do not remove tracked positions just because they were absent from the partial parse.
- Never write cash or portfolio value as `0` because a broker balance refresh failed. `get_cash()` and `get_portfolio_value()` return `None` on failed fresh reads and leave cached internal values unchanged.

This policy is intentionally different from order mutation behavior. Submit, cancel, and modify failures still fail loudly. The resilience policy applies to reading/parsing broker state, not hiding failed state-changing requests.

## Verified behavior table (append-only)

This table is intentionally small and focuses on “closed session / no data” semantics.

| Broker | Asset class | What we know (public docs) | Source | Verification |
|--------|------------|-----------------------------|--------|--------------|
| IBKR | US futures | Simulated stop orders only trigger during **regular trading hours** unless configured otherwise. | https://www.interactivebrokers.com/en/trading/us-futures-stop-order.php | Needs LumiBot `apitest` probes for our IBKR live path (paper/live) |
| Tradovate | Futures | GTC orders placed outside active hours can appear as **Suspended** until conditions allow activation; indicates “accepted/held”, not necessarily rejected. | https://support.tradovate.com/s/article/Tradovate-Order-Statuses?language=en_US | Needs LumiBot `apitest` probes (Tradovate demo/live) |
| ProjectX | Futures | Order placement API returns `{ success, errorCode, errorMessage }`; docs do not specify “market closed” behavior or which codes represent it. | https://gateway.docs.projectx.com/docs/api-reference/order/order-place/ | Needs LumiBot `apitest` probes (ProjectX demo/live) |
| Coinbase | Crypto | Market orders buy/sell at “market price”; stop orders trigger based on **last trade price**. | https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/guides/orders | Needs LumiBot `apitest` probes for execution-price conventions (bid/ask vs trade) |

### Alpaca (equities)

Alpaca supports extended hours trading windows and documents explicit session times.

Important constraints (as described in public Alpaca docs):
- extended hours exist (pre-market + after-hours)
- extended-hours trading historically required **limit** orders and a request flag for eligibility

Links (public):
- https://alpaca.markets/support/extended-hours-trading
- https://alpaca.markets/learn/how-to-enable-stock-market-trading-from-4-am-to-8-pm-et

Backtesting implications:
- “no fills outside 9:30–4:00” is **not** a valid global invariant for equities.
- execution should be tied to the data source’s bar/quote availability and the broker’s order eligibility rules.

### Schwab (equities)

Schwab supports extended hours trading; public docs emphasize:
- regular-session stock orders can use market, limit, stop-limit, and other
  order types depending on product/platform eligibility
- extended-hours/pre-market/after-hours stock trading accepts **only limit
  orders**
- Day + Extended and GTC + Extended orders are known as seamless orders and are
  available only for limit orders
- certain order types (e.g., market and stop orders) are not eligible in
  extended sessions

Links (public):
- https://www.schwab.com/content/how-to-place-trade-during-extended-hours
- https://international.schwab.com/content/stock-order-types-and-conditions-overview
- https://www.schwab.com/learn/story/mastering-order-types-limit-orders

Backtesting implications:
- if we model Schwab extended sessions, order-type restrictions must be documented and tested.

Live trading implications:
- Schwab stock `LIMIT` orders may use `SEAMLESS` when extended-hours
  participation is intended.
- Schwab stock `MARKET` orders must use `NORMAL`; a live July 2026 production
  customer run rejected `session=SEAMLESS`, `orderType=MARKET` with HTTP 400
  `Invalid request data`.
- Do not generalize "seamless spans all sessions" into "seamless works with all
  order types." The session span and order type eligibility are separate broker
  constraints.

### Tradier (equities/options)

Tradier extended-hours behavior is not as well documented in this repo; one useful third-party summary (QuantConnect) states:
- Tradier does not support extended market hours trading
- orders placed outside regular hours are processed at market open

Link (public):
- https://www.quantconnect.com/docs/v2/cloud-platform/live-trading/brokerages/tradier

Backtesting implications:
- for Tradier-style behavior, “order submitted outside session” likely means “accepted and held until open”.

Live read-path implications:
- Tradier's Brokerage REST advanced-order submit path uses `otoco` for bracket orders. Returned `otoco` rows must be
  parsed back into LumiBot `OrderClass.BRACKET`, with the entry leg as the parent order and the take-profit/stop-loss
  legs attached as children.
- Tradier `combo` rows map to LumiBot `OrderClass.MULTILEG`.
- Tradier account/order refresh may encounter future-like or unsupported broker asset rows. The read path should
  preserve these as `future`, `cont_future`, or `unknown` assets instead of forcing every non-option row into stock
  parsing. This is read-model preservation only; it does not mean Tradier Brokerage REST futures order execution is
  supported.
- If a Tradier order row cannot be parsed, logs must include only a sanitized row shape: class/type/status, field
  presence, leg count, and per-leg field presence. Do not log raw symbols, account identifiers, tags, credentials, or
  the full broker payload.

### IBKR (equities/options)

IBKR supports “Outside RTH” for eligible products:
- the “Fill Outside RTH” attribute is product-dependent
- order-type eligibility can vary

Links (public):
- https://www.interactivebrokers.com/campus/trading-lessons/trading-outside-regular-trading-hours-rth/
- https://www.interactivebrokers.com/campus/glossary-terms/outside-rth/

Backtesting implications:
- whether an order can fill outside RTH is not universal; it depends on product + order type + flag.

### IBKR (futures)

IBKR documents futures order handling rules (including stop/stop-limit behavior and “outside RTH” triggering options).

Links (public):
- https://www.interactivebrokers.com/en/trading/us-futures-stop-order.php

LumiBot truth-probes (read-only / safe):
- `tests/backtest/test_ibkr_futures_downloader_apitest.py` (contract info + trading hours metadata via Client Portal)

Backtesting implications:
- “stop triggers outside RTH” can be a configurable property in live; backtesting must document what we assume.
- futures gaps (maintenance/holiday) must be treated as “no fills until next actionable data”.

### Tradovate (futures)

Tradovate’s order status definitions explicitly call out orders placed outside of active trading hours.

Link (public):
- https://support.tradovate.com/s/article/Tradovate-Order-Statuses?language=en_US

Backtesting implications:
- Some orders can be accepted and remain non-working until the session is active (status: “Suspended”).
- This supports a default backtesting stance of “accept-and-hold until data resumes” (still data-driven: no fill without OHLC/quotes).

### ProjectX (futures)

ProjectX publishes an API reference for placing and managing orders.

Links (public):
- https://gateway.docs.projectx.com/docs/api-reference/order/order-place/
- https://gateway.docs.projectx.com/docs/category/orders/

Backtesting implications:
- The API surface is documented (order types, params), but market-closed/reject semantics are not clearly specified in the public reference.
- We should treat this as “unknown until probed” and rely on data-driven backtesting rules in the meantime.

### Crypto (Coinbase; 24/7)

Crypto markets are 24/7, but:
- orders can have explicit time-in-force (GTC/GTD/IOC/FOK, etc.)
- bracket/attached orders have caveats on execution guarantees under volatility

Links (public):
- https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/guides/orders

Backtesting implications:
- missing bars are “no fill possible” (data gaps), even if the market is conceptually 24/7.

---

## Required follow-up (turn documentation into truth)

Public docs are not enough. We need live/paper verification for our supported brokers.

### A) Add broker behavior smoke tests (apitest)

For each broker (where supported in this repo), add small `pytest.mark.apitest` checks:
- can submit order outside the regular session?
- does broker reject vs accept-and-hold?
- which order types are accepted in extended sessions?
- what happens with stop/stop-limit triggers outside RTH?

### B) Maintain a versioned “behavior table”

Maintain a small table in this doc (append-only) for each broker:
- asset class
- session type (regular/extended/overnight)
- order type
- expected behavior (accept/hold/reject; eligible-to-fill)
- last verified date + environment (paper/live)
