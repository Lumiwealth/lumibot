# BROKER_ORDER_SEMANTICS.md

> Notes on live broker behavior that affect backtesting semantics (extended hours, order types, and “market closed / no data” handling).

**Last Updated:** 2026-11-08
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

### SmartLimit mutation confirmation

Native broker modification remains the preferred reprice path. If it fails or
a package must change debit/credit/even type, the executor retains one replacement
intent instead of immediately submitting the same object again. It refreshes the
original order through the broker's normalized read and existing observation
reducer. It follows only broker-reported replacement links, not guessed IDs.

- A cancel acknowledgement is not terminal confirmation. Delayed/missing reads
  keep the outstanding intent visible and cannot authorize another purchase.
- A confirmed fill wins the race and suppresses replacement. A confirmed partial
  cancellation retains its executions; a new order contains only the remainder,
  with fresh identity and clean lifecycle events/transactions.
- A package replacement also verifies that remaining leg quantities match the
  remaining package ratio before submitting. Inconsistent leg observations wait
  for reconciliation instead of duplicating a partially executed leg.
- Once a fresh submission is attempted, an uncertain response does not create a
  second submission identity. Unresolved work fails the existing bounded drain
  visibly; this is not a claim that a network failure can always be auto-repaired.
- Final-hold cancellation and pending cancel/replace states receive throttled
  broker reads even without stream callbacks. Closed historical orders are not
  scanned in the normal active-order fast path.
- The Alpaca adapter normalizes SDK enum values and reconciles terminal status
  before deciding not to modify. Live position snapshots remain broker-owned.

`tests/test_smart_limit_broker_lifecycle.py` exercises the actual Alpaca SDK
response model, adapter, executor and common reducer without network access.
The pre-fix regressions reproduce six canceled orders still counted as pending,
terminal enum misclassification and a resubmission attempt after a fill won the
reprice race. Pure executor fixtures now distinguish a terminal cancellation
callback from a no-op cancel request. These are deterministic regressions, not
proof of real provider execution. Backtesting keeps its existing fill model and
does not execute the live repricing loop.

Alpaca documents that an existing order can fill before a replacement reaches
the venue, even when the replace request initially succeeds:
[retail replacement API](https://docs.alpaca.markets/us/reference/patchorderbyorderid-1).

### Bitunix futures submission contract

- Cache validated `basePrecision`, `quotePrecision`, and `minTradeVolume` by
  symbol for the broker session. Failed metadata requests are not cached and
  must never fall back to guessed precision.
- Use Decimal rounding down for quantity and prices. Serialize fixed-point
  strings at the client boundary, including native TP/SL keyword fields.
  Track the executable quantity so a full fill does not leave an artificial
  remainder. Below-minimum sizes fail locally, including rounded-to-zero sizes.
- Preserve constructor leverage for `CRYPTO_FUTURE` as for `FUTURE`. This is
  desired leverage, not proof that the exchange accepted the leverage change;
  the existing warning behavior for leverage API failures remains.
- Apply desired leverage only to opening orders. Reduce-only full and partial
  closes must not call the leverage API or update the local leverage cache,
  even when a reconstructed asset defaults to 1x or a restart clears the cache.
  Position identification, HEDGE validation, and quantity rules still apply.
- Only submit after HEDGE initialization is confirmed. Failures leave the
  initialization flag unset and block the order, so a later submission retries.
  Never infer ONE_WAY mode from a failed mode-change request.
- HEDGE opens use `tradeSide=OPEN`; reduce-only closes use `CLOSE`, the unique
  matching exchange position ID, and the position side (BUY for long, SELL for
  short). LumiBot retains the opposite execution side on the Order, and the
  response parser translates CLOSE rows back to that execution side.
  Position reads recognize both LONG/SHORT and older BUY/SELL side labels so a
  short remains negative when constructing the close.
- Bitunix fractional closes convert both quantity and fraction to Decimal.
  A rounded partial close can leave a real residual position. No changes to
  the shared base-broker close or history-pagination paths are needed.
- Position snapshots are atomic: reject a failed response or any row lacking a
  usable symbol, recognized position side, or finite quantity/entry price.
  Raise `LumibotBrokerAPIError` before returning any partial list, preserving
  tracked positions and allowing the next refresh to retry. Bitunix cannot
  represent an unknown side without guessing its signed exposure.
- Reject multiple nonzero rows for the same asset, including opposite-side
  HEDGE positions. The shared tracker identifies positions by asset and cannot
  represent both independently; publishing them would make exposure depend on
  response order. Zero-quantity rows do not create this ambiguity.
- Shared stale-position pruning iterates a copy of the tracker list, so an
  explicitly successful empty snapshot removes every stale non-cash position.
  The existing pre-snapshot identity guard still protects fills added while the
  remote read is in flight. Snapshot failure and an empty account remain
  distinct; neither implies a customer strategy's own variables are reconciled.

Sources: [place order](https://www.bitunix.com/api-docs/futures/trade/place_order.html),
[pair metadata](https://www.bitunix.com/api-docs/futures/market/get_trading_pairs.html),
[position mode](https://www.bitunix.com/api-docs/futures/account/change_position_mode.html).

Position snapshot contract:
[pending positions](https://www.bitunix.com/api-docs/futures/position/get_pending_positions.html).
`tests/test_bitunix_position_snapshot.py` exercises the real broker and client
with intercepted transport, including rejected/malformed snapshots, preserved
state, retry, valid empty accounts, and signed long/short positions.

Regression evidence: `tests/test_bitunix_place_order_params.py` intercepts
HTTP transport and exercises the real broker/client serialization path,
including a simulated code-10002 validator. The initial RED run was 21 failed,
1 passed; additional close-default/response tests failed 3/3 before their fix.
This is deterministic contract validation, not live execution qualification.
The exact saved customer strategy and account path were not run: live keys
and live smoke testing were explicitly excluded from this task.

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
- Bitunix position sync requires an atomic snapshot and raises on unrepresentable
  rows instead of returning a partial list that would authorize stale pruning.
- Never write cash or portfolio value as `0` because a broker balance refresh failed. `get_cash()` and `get_portfolio_value()` return `None` on failed fresh reads and leave cached internal values unchanged.

This policy is intentionally different from order mutation behavior. Submit, cancel, and modify failures still fail loudly. The resilience policy applies to reading/parsing broker state, not hiding failed state-changing requests.

## Verified behavior table (append-only)

This table is intentionally small and focuses on “closed session / no data” semantics.

| Broker | Asset class | What we know (public docs) | Source | Verification |
|--------|------------|-----------------------------|--------|--------------|
| IBKR | US futures | Simulated stop orders only trigger during **regular trading hours** unless configured otherwise. | https://www.interactivebrokers.com/en/trading/us-futures-stop-order.php | Needs LumiBot `apitest` probes for our IBKR live path (paper/live) |
| Tradovate | Futures | GTC orders placed outside active hours can appear as **Suspended** until conditions allow activation; indicates “accepted/held”, not necessarily rejected. | https://support.tradovate.com/s/article/Tradovate-Order-Statuses?language=en_US | Needs LumiBot `apitest` probes (Tradovate demo/live) |
| ProjectX | Futures | Place receipts document code `5` as `OutsideTradingHours`; actual session-specific order behavior still needs provider probes. | https://gateway.docs.projectx.com/docs/api-reference/order/order-place/ | Needs LumiBot `apitest` probes (ProjectX demo/live) |
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
- Single-order limit prices (stock limit, stock stop-limit, option limit) are
  passed to schwab-py as strings by `_format_schwab_single_price`. schwab-py's
  float path is deprecated, and it truncates in binary: 1.15 was sent as
  "1.14". The string keeps schwab-py's rule (4 decimals under $1, 2 at or
  above, toward zero) using `Decimal`. `tests/test_schwab_single_order_prices_unit.py`
  turns the deprecation warning into an error. Stop prices were already sent
  as `str(stop_price)` and are not reformatted.

### Schwab (multi-leg options)

Schwab receives a multi-leg option package as one `orderStrategyType=SINGLE`
order (`lumibot/brokers/schwab.py`, `_prepare_multileg_option_order_builder`).

Wire contract:
- 2 to 4 option legs, all on one underlying. Stock legs, more legs, or mixed
  underlyings are rejected before any HTTP request; no leg is sent alone.
- `orderType` is `NET_CREDIT`, `NET_DEBIT` or `NET_ZERO`, with a positive price
  (`NET_ZERO` is `0.00`). Market packages, other order types, and durations
  other than `day`/`gtc` are rejected. The type comes from the
  `credit`/`debit`/`even` keyword, else from the parent side and price sign.
  Zero is judged on the price as sent (four decimals under $1, chosen from the
  original amount, so 0.9951 stays four decimals). A credit or debit that
  rounds to `0.00` after the rounding that will be sent is refused: half-up
  turns a `0.00006` debit into `0.0001`, and debit rounding then turns it into
  `0.00`. An unhinted price that rounds to `0.00` with half-up is `NET_ZERO`,
  never `NET_CREDIT`/`NET_DEBIT` at `0.00`. A debit rounds down and a credit
  rounds up, so the sent price is never more aggressive than requested.
  The strike in the OCC symbol keeps thousandths (`100.125` stays `100.125`).
  A `MARKET` or `STOP_LIMIT` parent is rejected even when it carries a limit
  price; the stop trigger would otherwise be dropped.
- `Strategy.submit_order(legs, order_type='limit', price=...)` infers
  credit/debit/even from leg quotes weighted by each leg's ratio to the package
  quantity, for Schwab, Tradier, and Alpaca. Without that a positive Schwab
  limit would always be a debit. A list of one option is a single order. A
  successful submit stores the net effect, so a later replace cannot turn a
  credit into `NET_ZERO` or an even package into a debit.
- `complexOrderStrategyType` is `VERTICAL` only for one buy and one sell with
  the same expiry and right, different strikes and equal quantities; anything
  else is `CUSTOM`.
- Legs keep their own contract counts. The top-level `quantity` is the GCD of
  the leg quantities; a hand-built parent must match it.

Lifecycle:
- A snapshot whose legs are all options parses as one `OrderClass.MULTILEG`
  parent with the broker order id. Child legs get `<id>-leg-<n>` identifiers
  because `Order` equality uses the identifier. An all-option snapshot whose
  legs cannot all be read is skipped, never returned as its first leg; the
  warning is logged once per order id because the broad poll re-reads seven
  days of history every 30 seconds.
- Fills are counted in packages, never contracts: summing `executionLegs`
  directly would count every contract on every side. The net fill price is
  each leg's execution VWAP times its leg ratio (buys positive, sells negative),
  reported positive. Schwab's `price` is the limit, not the fill. Only when an
  execution has no `legId`, a leg has no execution, or legs filled out of ratio
  does the parser use `filledQuantity` and `averagePrice` (then `price`).
  Each new package fill uses the incremental price of that fill, derived from
  the cumulative value, so two fills at 1.30 and 1.20 average to 1.25. A single
  order uses the same rule: one unit at 1.10 and then an average of 1.20 over
  two is a new fill at 1.30, not 1.20. After each fill the order's cached
  `avg_fill_price` is the broker's cumulative average, so `get_fill_price()`
  does not keep an older value. Zero is a real net price and is not replaced by
  the limit. A leg whose side is neither
  buy nor sell uses that same fallback, and the package parent is marked
  degraded when a leg is. A parsed `NET_CREDIT`, `NET_DEBIT`, or `NET_ZERO`
  keeps that effect, so a replace after a restart follows the same credit,
  debit, and even rules as the original submit.
- A working order whose Schwab `tag` matches this strategy is recovered after a
  restart; untagged and foreign orders never are. Recovery runs in
  `set_strategy_name`, which the executor calls right before `initialize()`, so
  a close submitted there sees the working close instead of duplicating it. A
  working order with fills (WORKING with a filled quantity, or
  PARTIALLY_FILLED) is recovered with that history. An order already tracked
  by id is never skipped as foreign by the poll, because its tag can be a user
  label rather than the strategy name. Fills it already had are
  recorded without a callback, at Schwab's `averagePrice` or execution price.
  When neither exists the parser's limit fallback is not a fill, so that
  quantity is left unpriced; the next fill values it at the new average, and
  the shared sync importer does the same instead of failing on the missing
  price. A later fill whose only price is that limit fallback is recorded at
  the limit, never derived from it as if it were a cumulative average. A
  recovered stop-limit keeps its limit in `stop_limit_price`, where the
  stock builder reads it. Every replacement (package, stock or option) carries
  the same tag, so a repriced order is still recognized after the next
  restart. A history pull that cannot read a package keeps that package's last
  known fill, because an unreadable snapshot is not evidence the fill is gone,
  and a stale pull reporting a smaller fill does not lower it: fills only grow.
- An unlabeled (`Unknown`) position row counts as this strategy's only when no
  other strategy has a named row for the asset. Otherwise it may be that
  strategy's holding, and it cannot classify a sell as a close or authorize one.
  A named position sync follows the same rule: it keeps that row's quantity
  current but does not relabel it as this strategy's.
- When the package fills, is canceled, or errors, its original child orders are
  marked the same way and their close events are set. A submit rejection does
  this when the error event is applied, including after a held event is
  released. A package that was canceled and is later confirmed filled moves
  those legs to filled; that update does not book a second cash change. A
  local error that a later partial fill supersedes reopens the parent and its
  legs, so the outstanding contracts stay reserved. The parent fill does not
  move cash (`Order.is_parent()`); leg positions come from the regular Schwab
  position sync.
- A working package's unfilled close legs count as reserved contracts. Filled
  close contracts stay reserved as well until position sync removes them from
  the book; the sync drops that hold and leaves only the unfilled remainder.
  The package being replaced does not reserve against itself. An explicit close
  with no matching position is rejected before the request.
- `modify_order` replaces the package through the same builder, so SMART_LIMIT
  repricing changes only the net price. A failed order read, including HTTP 429,
  returns `False` and leaves the local price unchanged; the strategy does not
  copy the requested price, and SMART_LIMIT retries that ladder step for both
  a package and a single order. A change
  between credit, debit and even is a cancel and resubmit (strategy executor);
  modifying an `even` package to a non-zero price is refused because `NET_ZERO`
  is always `0.00`.
- Durations are checked in the package builder, which every path uses
  (`submit_orders`, a hand-built parent, modify): `day`/`gtc` in any case are
  sent; anything else is refused instead of being sent with no duration.
  `time_in_force` is accepted as an alias for `duration` on `submit_orders`.

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

Authenticated streams require `signalrcore>=1.0.2,<2`. Both wheel metadata and
source requirements declare this dependency; missing it raises an `ImportError`
instead of silently claiming that streaming started. Dependency declaration
parity does not resolve advisories in the SDK's pinned transitive dependencies.

Links (public):
- https://gateway.docs.projectx.com/docs/api-reference/order/order-place/
- https://gateway.docs.projectx.com/docs/category/orders/
- https://gateway.docs.projectx.com/docs/realtime/

Candidate execution and recovery contract, reviewed 2026-11-02:

- Classify placement receipts from strict structured fields and HTTP provenance.
  A timeout, malformed response, pending code `6`, or unknown-error code `7`
  remains uncertain. Both client placement entry points and the broker use the
  same classifier. Retain the exact tag for reconciliation; do not create a
  replacement after an uncertain handoff. Only structured HTTP 400 model
  validation permits the existing compatibility wrapper retry.
- Locally sent entries, including bracket parents, stay in active `SUBMITTED`
  tracking while acceptance is unknown. This status means sent, not accepted or
  filled. The original object, local identifier, exact tag and immutable intent
  reserve the handoff before transport. Within a running broker, repeating that
  identity never posts a second order; conflicting terms fail visibly. A fresh tag is a distinct intent,
  so callers must reconcile uncertainty before creating replacements.
- Exact provider receipts bind to the original object and preserve its bracket
  intent and execution ledger. Conflicting identity or terms cannot authorize a
  fill. Stream evidence received before the REST reply remains authoritative;
  later ambiguous replies cannot undo proven executions. An explicit unresolved
  marker restored without local ownership refuses resubmission; it does not
  reconstruct a durable ProjectX reservation after restart.
- Synthetic bracket exposure is cumulative entry executions minus all observed
  protective executions, including older generations and late fills. A live
  partial exit's total quantity includes its own fills plus remaining exposure.
  Wait for old accepted siblings to become terminal before creating fresh exits.
- A successful cancellation response is an acknowledgement, not terminal order
  evidence. Internal sibling cancellation preserves strategy intent. An explicit
  public cancellation disables that exit kind; cancel-all cannot re-arm it.
- Apply a complete poll batch before protective decisions. A failed batch stays
  incomplete until every retained family identifier is reconciled. Preserve
  exact strategy ownership and coalesce concurrent execution updates.
  Trade events also validate supplied contract and side against the bound intent
  before recording an execution. Order and trade reducers drain protective
  requests after releasing the observation lock.
- Cold recovery links accepted tags and executions, but missing history or
  unknown current intent holds new submissions and resizes. Proven original
  sibling closure can still cancel its exact accepted sibling. Later entry fills
  may remain unprotected during a recovery hold; these are client-managed,
  non-atomic exits. See [the candidate user guide](../docsrc/brokers.projectx.rst).

These paths have offline regression coverage; this review did not perform live
ProjectX orders. Venue account support and market-session behavior remain
integration qualifications. The current
[Place reference](https://gateway.docs.projectx.com/docs/api-reference/order/order-place/)
documents code `5` as `OutsideTradingHours`, without proving how every order type
behaves around a session boundary. The
[Cancel reference](https://gateway.docs.projectx.com/docs/api-reference/order/order-cancel/)
also documents account eligibility restrictions. Do not infer live-account
support from simulated-account tests.

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

### Overlapping position refreshes

The shared broker assigns a sequence number before each position read and applies
a completed snapshot under the tracker lock. Once a newer request has applied, an
older response is ignored in full: it cannot prune, resurrect, or change positions.
A failed newer read does not invalidate an older successful response. Network I/O
runs outside the tracker lock so fills and strategy accessors can still proceed.
The existing pre-read position identity check continues to protect new local fills
from stale pruning and same-asset field/owner overwrites. The next fresh read
reconciles those positions normally. This ordering is local request ordering; it cannot establish
the exchange's internal snapshot timestamp or historical incident cause.


## Cumulative execution observations (October 31, 2026)

A partial broker observation contains cumulative executed quantity, not ordered
quantity. Hydrating it records only the unprocessed delta and derives that delta's
price from cumulative execution value. Repeated or older observations cannot
replay executions into crypto quote cash. Missing partial execution quantity
remains unknown; it never becomes the full order size. Pending cancellation
remains active until confirmation; pending replacement is not final cancellation.
Alpaca parsing retains the provider status and cumulative filled quantity, and
Alpaca and Bitunix polling share the cumulative reconciliation owner for partial and final fills. Bitunix preserves pending cancellation, expiration and unknown states instead of reporting cancellation prematurely. A late cancellation snapshot cannot erase a fully recorded execution. Reparsed snapshots bind to the existing execution ledger before applying new quantities.

The shared status contract retains pending replacement, done-for-day, suspended,
and stopped as distinct working states. Replaced is terminal for that order ID;
it does not erase its executions or imply that its replacement filled. Assignment
and exercise aliases normalize before validation to assigned and exercised.
Native status tests qualify this shared mapping; they do not establish that every
broker's transport or every CCXT exchange has passed live integration testing.

IBKR REST and Tradier polling also reconcile cumulative executions when the
status string has not changed. Tradier retains its historical closed-order
memory bound and performs an exact lookup before inferring anything from a
broad-list miss. IBKR REST preserves the provider side and cumulative quantity;
a combo average is not assigned as a per-leg fill price. Native tests cover
partial, duplicate, full, canceled, expired, replaced and rejected observations.
ProjectX, Tradovate and CCXT retain expiration as distinct from cancellation.

## Submission certainty in agent tools

ProjectX and MetaMask expose `Order.submission_outcome` independently of order
lifecycle status. `broker_state_certainty` is `confirmed_submitted`,
`confirmed_rejected`, `confirmed_not_submitted` for a proven native MetaMask
pre-release cancellation, or `unknown`. `reconciliation_required` marks an
unresolved handoff or conflicting evidence. Submission acknowledgement is never
a fill.

ProjectX gives one caller ownership of preparing each order object. Concurrent
submissions return that object with unknown certainty until preparation and
handoff resolve; they cannot reset an in-flight outcome. A failed preflight
reports confirmed rejection and releases preparation ownership without sending
an order.

Single-order, order-list and submission tools retain this metadata. Unknown or
malformed explicit certainty produces an incomplete, non-retryable submission
result requiring reconciliation. Mixed batches retain each returned order and
cannot report complete success when any result remains uncertain. Adapters
without this explicit metadata retain their legacy result contract; this does
not independently qualify their transport semantics.

A native MetaMask local cancellation with validated pre-release provenance reports
`confirmed_not_submitted`. It is an incomplete, non-retryable order-submission
outcome, not a claim that no signing or leverage mutation occurred. Unproven
legacy cancellation stays unknown. Positive venue identity or executions take
precedence over a stale no-release marker; a canceled status alone proves neither
acceptance nor absence of submission. Historical missing evidence requires
operator reconciliation and does not retroactively restore a lost journal hold.
