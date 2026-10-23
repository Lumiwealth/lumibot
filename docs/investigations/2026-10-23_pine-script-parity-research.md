# Pine Script parity research for LumiBot indicators

What TradingView Pine Script v6 can do, how it is shared and run, how
Python/JS/C# competitors shape indicators, and what LumiBot needs to match and
beat it.

Last Updated: 2026-10-23
Status: Research only, no code changed
Audience: LumiBot and BotSpot engineers and AI agents designing the indicator API

## Overview

The question

Rob wants LumiBot (Python) indicators to match TradingView Pine Script and then
beat it. This doc answers five things:

1. What Pine Script v6 can actually do, grouped, with the numbers that limit it.
2. How Pine scripts are shared, protected and versioned.
3. Where Pine runs and how TradingView keeps the cost down.
4. How multi-symbol charts work.
5. How competitors that use Python, JavaScript or C# shape an indicator class,
   how they run untrusted code, and what they charge.

It ends with a parity checklist for LumiBot, a list of ideas to beat Pine, and
a "Sources checked" table.

Labels used below: **[official]** = TradingView, QuantConnect, NinjaTrader,
cTrader, TradeStation, TrendSpider or Freqtrade docs. **[practitioner]** =
Reddit, Hacker News, X, YouTube, Medium. **[inferred]** = my reasoning, not
stated by a source.

## What LumiBot has today (baseline, read 2026-10-23 on `version/4.6.26`)

- `self.indicators.<name>(asset, ...)` passes through to pandas-ta-classic
  (about 130 indicators), returns the value at the current bar, memoizes by
  observed input, and refuses negative offsets and noncausal parameters
  (`lumibot/indicators/indicators.py`).
- `self.indicators.custom(name, fn, asset, timestep=, length=)` runs a user
  function on a dataframe.
- Chart output is three calls on `Strategy`: `add_line` (name, value, color,
  style solid/dotted/dashed, width, detail_text, dt, plot_name, asset),
  `add_marker` (Plotly marker symbols, size, detail_text), and `add_ohlc`
  (candles). They write to `indicators.csv` and render after a backtest
  (`lumibot/strategies/strategy.py` lines 4062, 4242, 4385).
- There is no indicator class, no declared inputs, no fills, no boxes, labels
  or tables, no alerts from indicators, no publish or version model, and no
  per-bar streaming update contract.

So the gap is mostly in **declaration, visuals, sharing and runtime**, not in
the math. The math (pandas-ta) is already broader than Pine's `ta.*`.

---

## 1. Pine Script v6 capability catalog

### 1.1 Declaration and script types [official]

- Three script kinds: `indicator()`, `strategy()`, `library()`.
- Declaration options include `overlay` (price pane vs own pane),
  `format` / `precision`, `scale` (left, right, none), `max_bars_back`,
  `max_lines_count` / `max_labels_count` / `max_boxes_count` /
  `max_polylines_count`, `calc_bars_count`, `dynamic_requests`,
  `behind_chart`, `explicit_plot_zorder`.
- `force_overlay=true` on a single plot or drawing puts it on the price pane
  even when the script lives in its own pane. (Plots page.)
- **LumiBot needs:** an indicator declaration object: title, short title,
  overlay or pane, scale, format, precision, default history depth, drawing
  caps.

### 1.2 Inputs [official, Inputs page]

14 input functions: `input()`, `input.int`, `input.float`, `input.bool`,
`input.color`, `input.string`, `input.text_area`, `input.timeframe`,
`input.symbol`, `input.source`, `input.session`, `input.time`,
`input.price`, `input.enum`.

Common parameters: `defval`, `title`, `tooltip`, `inline` (same line),
`group` (heading), `display` (status line / data window / none), `active`
(enable or disable based on another input, added July 2025), `options`
(dropdown), `minval` / `maxval` / `step`, `confirm` (ask on add).

Notable behavior:
- `input.source` can take **another script's plot** as its source, so
  indicators chain together.
- `input.time` + `input.price` with matching `group` and `inline` make a
  draggable point on the chart. `confirm=true` lets the user click the chart
  to set it.
- **LumiBot needs:** a typed input schema (JSON Schema style) with groups,
  inline rows, tooltips, enums, symbol and timeframe pickers, a "source" type
  that can point at another indicator's output, and chart-click inputs for
  time and price.

### 1.3 Plots [official, Plots and Limitations pages]

- `plot()` parameters: `series`, `title`, `color` (can change bar by bar),
  `linewidth`, `style`, `trackprice`, `histbase`, `offset`, `join`,
  `editable`, `show_last`, `display`, `format`, `precision`,
  `force_overlay`, `linestyle` (solid, dashed, dotted; Sept 2025).
- Styles: `line`, `linebr` (breaks on `na`), `stepline`,
  `stepline_diamond`, `area`, `areabr`, `columns`, `histogram`, `circles`,
  `cross`.
- Other plot functions: `plotshape` (shapes with text, location above/below
  bar, absolute), `plotchar` (any Unicode character), `plotarrow` (up or down
  arrows sized by value), `plotcandle` and `plotbar` (custom OHLC series with
  their own colors).
- `hline()` for fixed levels (does not count toward the plot limit).
- `bgcolor()` colors the background bar by bar. `barcolor()` recolors the
  chart's own candles.
- **Limit: 64 plot counts per script.** One call can use up to 7 counts
  (a `plotcandle` with series colors for body, wick and border).
  `alertcondition`, `bgcolor`, `barcolor` and a series-colored `fill` also
  count. Lines, labels, boxes and tables do not.
- **LumiBot needs:** every style above, per-point color, `na` gaps,
  offsets, show-last, status-line and data-window display flags, custom
  candles that recolor the main series, background color bands.

### 1.4 Fills [official, Fills page and Reference]

- `fill(plot1, plot2, color, ...)` and `fill(hline1, hline2, ...)`. Plot and
  hline IDs cannot mix. `fillgaps` controls `na` gaps.
- Gradient fill: a second `fill()` signature takes `top_value`,
  `bottom_value`, `top_color`, `bottom_color`. `color.from_gradient()`
  builds a color from a value inside a range. (From the Reference Manual;
  the Fills page fetched today only showed the two basic signatures.)
- `linefill.new(line1, line2, color)` fills between two drawn lines and
  follows them when they move.

### 1.5 Drawing objects [official, Lines and boxes; Text and shapes; Tables; Limitations]

| Object | What it is | Key details | Cap |
|---|---|---|---|
| `chart.point` | A point with `index`, `time`, `price` | `now()`, `from_index()`, `from_time()`; drawings copy values | n/a |
| `line` | Segment between two points | `xloc.bar_index` or `bar_time`; `extend` left/right/both; solid, dotted, dashed, arrows; full getters and setters; `copy`, `delete`, `line.all` | 500 (default about 50) |
| `box` | Rectangle | border style, bgcolor, text with size, color, alignment, wrap, font, formatting; `box.all` | 500 |
| `label` | Text bubble or shape at a point | many styles (label up/down/left/right, arrows, circles, flags), tooltip | 500 |
| `polyline` | Path through up to 10,000 points | `curved`, `closed`, fill color; no setters, delete and redraw | 100 |
| `linefill` | Fill between two lines | updates with the lines | tied to lines |
| `table` | Grid of cells pinned to one of 9 screen positions | cell text, colors, merge; Sept 2026 tables can move into a bottom-panel tab | 9 per script |

Other rules: bar-index x can go 10,000 bars back and 500 bars forward. When
the cap is hit, the oldest object is deleted. An object with `na`
coordinates still counts. (Limitations page.)

- **LumiBot needs:** a drawing object model with stable IDs that can be
  created, updated and deleted while the indicator runs, anchored by time
  (bar index is fragile across data sources), with caps and oldest-first
  eviction.

### 1.6 Data requests [official, Other timeframes and data; Limitations]

| Function | Gives you |
|---|---|
| `request.security()` | Any expression on another symbol, timeframe or chart type |
| `request.security_lower_tf()` | Array of intrabar values from a lower timeframe |
| `request.currency_rate()` | Daily FX conversion rate |
| `request.dividends()` / `splits()` / `earnings()` | Corporate events |
| `request.financial()` | FactSet fundamentals |
| `request.economic()` | Economic and industry series |
| `request.footprint()` | Volume footprint rows (Jan 2026; Premium and up; one call per script) |
| `request.seed()` | Data from a user-maintained GitHub repo |
| `request.quandl()` | Legacy Nasdaq Data Link |

Rules:
- `lookahead` defaults off. Higher-timeframe `lookahead_on` without a `[1]`
  offset leaks future data, and publishing such scripts is not allowed.
- `gaps` off fills with the last confirmed value; `gaps_on` returns `na`.
- Dynamic requests (series arguments, requests in loops and local scopes,
  nested requests) are on by default in v6. A realtime bar cannot request a
  context it did not request on history.
- 40 unique request calls per script (64 on Ultimate). Identical calls are
  reused and count once. 127 tuple elements across all requests. Up to
  100K intrabars (200K on Ultimate).
- **LumiBot needs:** multi-symbol and multi-timeframe reads inside an
  indicator with explicit no-lookahead alignment (LumiBot already enforces
  causality in `self.indicators`), corporate events, fundamentals, macro
  (LumiBot has `lumibot/macro` and `lumibot/fundamentals`), and lower
  timeframe arrays.

### 1.7 Built-in TA library [official + GitHub count]

- `ta.*` has about 70 distinct names (81 `def`s including overloads in
  PyneCore's Pine-compatible port, counted 2026-10-23): moving averages
  (sma, ema, rma, wma, hma, alma, swma, vwma, vwap), oscillators (rsi,
  stoch, cci, cmo, mfi, tsi, wpr, rci, roc, mom), bands and channels (bb,
  bbw, kc, kcw), trend (dmi, sar, supertrend, linreg), volume (obv, pvt, nvi,
  pvi, accdist, wad, wvad, iii), statistics (stdev, variance, dev,
  correlation, median, mode, percentile, percentrank), and helpers (cross,
  crossover, crossunder, barssince, valuewhen, highest/lowest(+bars),
  pivothigh/low, pivot_point_levels, rising, falling, change, cum, tr, atr).
- RSI uses RMA (Wilder) smoothing, which is why Python libraries often
  disagree with TradingView. [practitioner, r/TradingView, Feb 2025]
- **LumiBot needs:** bit-level parity with these definitions, especially
  `rma`, `rsi`, `atr`, `supertrend`, `pivothigh`, `vwap` session anchoring.
  pandas-ta-classic is wider but not guaranteed identical.

### 1.8 Language features [official]

- Types: int, float, bool, color, string, plus `line`, `label`, `box`,
  `table`, `polyline`, `linefill`, `chart.point`, `footprint`, `volume_row`.
- Qualifiers: const, input, simple, series. Library parameters can be pinned
  to `simple` so their results can feed functions that refuse series input.
- `var` (init once), `varip` (survives intrabar rollback), `once` block
  (Aug 2026, runs once on the first closed bar where its condition is true).
- Collections: arrays, matrices, maps. 100,000 elements each; maps 50,000
  pairs. Sort user-defined types by a field (Apr 2026).
- User-defined types (`type`), `enum`, `method`, exported constants (June
  2025), multiline strings (Apr 2026), strings up to 40,960 characters.
- Pine Logs (`log.info` etc.), Pine Profiler, `runtime.error()`.
- **LumiBot needs:** none of this as syntax, because Python already has it.
  What it needs is the *semantics*: per-bar persistent state, intrabar
  rollback, and "run once" hooks.

### 1.9 Time and sessions [official]

- `time`, `time_close`, `time()` / `time_close()` with session and timezone
  arguments and `timeframe_bars_back` (Oct 2025), `timestamp()`,
  `input.session`, `session.ismarket` / `ispremarket` / `ispostmarket`,
  `syminfo.session`, `timeframe.*` helpers, `bid` / `ask` (Feb 2025).
- **LumiBot needs:** session-aware helpers that read the broker or exchange
  calendar (LumiBot already has market calendars).

### 1.10 Bar states and the realtime model [official, Execution model]

- History: one run per closed bar. Realtime: one run per tick on the open
  bar, and only the last tick's values are committed.
- **Rollback:** before each tick, all `var` state is reset to the bar's
  opening state. `varip` is the escape hatch, and it can repaint.
- `barstate.isfirst`, `islast`, `ishistory`, `isrealtime`, `isnew`,
  `isconfirmed`, `islastconfirmedhistory`.
- Strategies run once per bar close by default; `calc_on_every_tick`,
  `calc_on_order_fills`, and (July 2026) `calc_on_every_history_tick`.
- Re-execution triggers: add, save, refresh, input change, symbol or
  timeframe change, visible range change if the script reads it, alert
  creation, screener scan. **Results from earlier runs with the same
  settings may be reused; the cache clears on refresh or code update.**
- **LumiBot needs:** a `confirmed` vs `forming` bar flag passed to every
  update, rollback of indicator state on forming bars, and an explicit
  per-tick persistence opt-in.

### 1.11 Alerts [official, Alerts page]

- `alertcondition()` (indicators only, constant message, global scope) and
  `alert()` (dynamic message, any scope, frequency `once_per_bar`,
  `once_per_bar_close`, `all`).
- Placeholders: `{{ticker}}`, `{{exchange}}`, `{{interval}}`, OHLCV,
  `{{plot_0}}`..`{{plot_19}}`, `{{plot("title")}}`, `{{time}}`,
  `{{timenow}}`, `{{strategy.order.alert_message}}`.
- "TradingView alerts run 24x7 on our servers and do not require users to be
  logged in." An alert stores **a copy of the script and its inputs**, so
  later edits do not change a running alert.
- Webhooks are configured in the alert dialog, not in Pine.
- **LumiBot needs:** indicator-level alert events with a frozen script
  version and inputs, server-side evaluation, and message templates. LumiBot
  can go further because the same process can trade.

### 1.12 Strategies [official, Strategies page]

- Orders: `strategy.entry`, `order`, `exit` (TP, SL, trailing),
  `close`, `close_all`, `cancel`, `cancel_all`; OCA groups; `pyramiding`;
  `strategy.risk.*`.
- Broker emulator assumes an intrabar path (open, nearer extreme, far
  extreme, close). Bar magnifier uses lower-timeframe data (Premium and up).
- 9,000 orders per backtest (1,000,000 with Deep Backtesting).
- Report: P&L, max drawdown, win rate, profit factor, Sharpe, Sortino, CAGR,
  margin calls, run-ups.
- **LumiBot already exceeds this** for execution (real brokers, options,
  futures, crypto). Parity work here is about the *chart view* of trades
  (entries, exits, stop and target lines on the price chart).

### 1.13 Libraries [official, Libraries page]

- `library("Name")` exports functions, methods, UDTs, enums and constants.
- Import pins a version: `import user/LibName/3 as alias`. A library can
  import other libraries, even older versions of itself.
- **All libraries are published open source.** Public scripts can only use
  public libraries. Private scripts can use private libraries.
- Exported functions cannot read inputs or non-const globals, must type
  their parameters, and their `request.*` calls count against the caller's
  40-call limit.
- **LumiBot needs:** a library unit that is versioned by integer, imported
  by `owner/name/version`, immutable once published, and resolvable inside
  the sandbox without pip.

### 1.14 Runtime limits [official, Limitations page]

| Limit | Value |
|---|---|
| Compile time | 2 minutes; 3 warnings in a row means a 1-hour compile ban |
| Full-history execution | 20 s (Basic), 40 s (paid) |
| One loop on one bar | 500 ms |
| Plot counts | 64 |
| Lines, boxes, labels | 500 each (default about 50) |
| Polylines | 100, 10,000 points each |
| Tables | 9 (one per screen position) |
| Unique `request.*` calls | 40 (64 Ultimate) |
| Intrabars | 100K (125K Expert, 200K Ultimate) |
| History buffer | 5,000 bars per series, 10,000 for OHLC and time |
| Collections | 100,000 elements, maps 50,000 pairs |
| Script size | 100,256 compiled tokens; 1M tokens across imported libraries; 5 MB request |
| Variables per scope | 1,000 |
| Backtest orders | 9,000 (1M Deep Backtesting) |
| Chart bars | 5K Basic, 10K Essential and Plus, 20K Premium, 25K Expert, 40K Ultimate |

Plan limits (TradingView pricing page, read 2026-10-23, CAD annual prices):
indicators per chart 2 / 5 / 10 / 25 / 50, historical bars 5K / 10K / 10K /
20K / 40K, charts per tab 1 / 2 / 4 / 8 / 16 for Basic / Essential / Plus /
Premium / Ultimate. Protected publishing needs a paid plan; invite-only
needs Premium or higher (Publishing page).

The docs also say there is "no means for Pine Script programmers to get data
on the resources consumed by their scripts." The Pine Profiler now gives
per-line timing, but not memory or quota use. [official]

---

## 2. Sharing and versioning

### 2.1 Publish settings [official, Publishing page]

- **Privacy:** Public (in the Community Scripts feed, moderated, must follow
  House Rules) or Private (link only, never moderated, cannot be linked from
  public content). **Privacy cannot be changed after publishing.**
- **Visibility:** Open (source visible, MPL 2.0 by default), Protected
  (closed source, anyone can use, paid plan), Invite-only (closed source,
  only invited users can add it, Premium plan, author becomes a "vendor"
  under Vendor Requirements). **Visibility cannot be changed either.**
- Public scripts can be edited or deleted only in the **first 15 minutes**.
  After that they are permanent. Private scripts can always be edited.
- Invite-only pages have a **Manage access** button where the author lists
  users. Vendors sell access outside TradingView (practitioner: LuxAlgo and
  Edgeful docs describe granting access by TradingView username). The page
  fetched today does not describe expiry dates; vendor tools add them.

### 2.2 Updates and version history [official + practitioner]

- To update, the author adds the new code to a chart and picks **Update
  existing script** in the publish dialog. Release notes are added as dated
  entries and are final once posted. "All updates are preserved in the
  script's version history." [official]
- The published chart image is replaced by the author's current chart unless
  they tick "Don't update the chart". [official, Help Center 43000482583]
- **Users' charts are not pinned to a version in a way the user controls.**
  When an update lands, a chart that already has the script shows a refresh
  icon on the indicator; clicking it (or reloading, or re-adding the script)
  loads the newest code. [practitioner: friendofthetrend.com guide; LuxAlgo
  and Edgeful support docs, 2025-2026] [inferred: there is no supported way
  for a user to stay on an old version of a published script except by
  copying open source code.]
- **Existing alerts keep the old version.** Alerts store a copy of the
  script and inputs, so after an update users must delete and recreate their
  alerts. [official, Alerts page; practitioner, Edgeful support]
- **Libraries are the only versioned dependency.** `import user/lib/3`
  pins version 3 forever. A new library version never changes an existing
  importer until the importer edits the number. [official]

### 2.3 How inputs are stored [official + inferred]

- An indicator instance on a chart stores its current input values and style
  overrides with the chart layout. Defaults come from the code. The Settings
  dialog has Inputs, Style and Visibility tabs; "Defaults" can save the
  current values as the user's new defaults. [inferred from Help Center
  layout articles; exact storage format is not public]
- Changing an input re-runs the script on all bars. [official]

### 2.4 Layouts, templates, favorites [official, Help Center]

- A **layout** is a saved workspace with a unique URL: charts, settings,
  drawings, indicators. Watchlists and alerts are not part of it.
  "Copy link" shares a read-only view; "Make a copy" clones it. Autosave is
  optional. 1 to 16 charts per layout depending on plan.
- An **indicator template** saves a set of indicators with current settings,
  optionally with symbol and interval. Applying one **replaces** the current
  indicators (and wipes drawings in indicator panes).
- "Add this indicator to the entire layout" and "Apply these indicators to
  the entire layout" copy indicators across charts.
- **Favorites:** a star icon in the Indicators dialog adds a script to the
  Favorites list for quick access; favorites can be shown as a toolbar.
- The Advanced Charts library (TradingView's embeddable product) exposes
  `createStudy(name, overlay, lock, inputs, overrides)` and
  `getStudyInputs(name)`, which shows how they model an indicator instance:
  name + inputs map + style overrides + pane/scale placement. [official,
  charting-library docs]

---

## 3. Runtime and efficiency

### 3.1 Where Pine runs

- **Server side.** The Limitations page: "each script uses computational
  resources in the cloud". The Help Center: scripts "are saved in
  TradingView's cloud servers". Alerts "run 24x7 on our servers". Binance
  Academy: "your script runs on TradingView's servers and the output appears
  directly on your chart." Reddit (2024): "Execution of pinescript happens on
  TV's backend servers. This includes strategy backtesting, alerts,
  everything. You are merely presented with the result." [official +
  practitioner]
- The compiler turns source into a tokenized intermediate language before
  running it; size limits are measured in IL tokens. [official, v5
  Limitations page]
- No public engineering blog, talk or job post found that describes the
  backend stack. HN has no architecture thread; HN results were third-party
  tools (PineCone bundler, Dec 2025; PineForge v6-to-C++, June 2026). X had
  no TradingView staff posts on this. [searched, nothing found]

### 3.2 How they keep cost down (stated and inferred)

| Lever | Evidence |
|---|---|
| Hard time caps per run (20 s / 40 s) and per loop (500 ms) | official |
| History depth capped by plan (5K to 40K bars) and per-series buffer (5K) | official |
| Indicators per chart capped by plan (2 to 50) | official |
| Request calls deduplicated: identical `request.*` calls run once | official |
| Results reused across runs with the same settings until refresh or code change | official (Execution model) |
| Compile cache; repeated slow compiles get a 1-hour ban | official |
| Drawings capped and evicted oldest-first | official |
| Realtime runs only re-execute the last bar, with rollback, not the whole history | official |
| Footprint limited to one call and paid plans | official |
| Shared computation across users with identical script + symbol + inputs | **[inferred, not documented]** The caching sentence suggests it, but no source says results are shared between users |

### 3.3 Third-party runtimes outside TradingView [GitHub, read 2026-10-23]

- **PyneCore** (PyneSys, Apache 2.0, 243 stars): Pine semantics in plain
  Python via import-time AST rewriting. `Series[float]` with `[1]` history,
  `Persistent[int]` for `var`, per-call function state, Pine `na` rules,
  every `ta.*` function. Claims 1,353 of 1,353 comparable outputs match
  TradingView across 1,000 published scripts, 99.771% of 132M plotted values
  bit-identical, 353,577 strategy trades matching (snapshot 2026-10-13).
  **This is the most important prior art for LumiBot.**
- **PineTS** (LuxAlgo, AGPL-3.0 / commercial, 893 stars, pushed 2026-10-22):
  runs native Pine v6 in Node or the browser, 60+ TA functions, streaming
  recalculation, `request.security`, libraries from GitHub or URLs.
- **pynescript** (Python Pine parser) used by the PineCone bundler. PineForge
  compiles Pine v6 to C++ "trade-for-trade with TradingView". [HN]

---

## 4. Multi-symbol charts [official, Help Center + Advanced Charts docs]

- **Compare:** add another symbol on the same chart, in percent mode or on
  its own scale or pane.
- **Spread syntax:** typing an expression like `AAPL/MSFT`, `BTCUSD-ETHUSD`
  or `2*ES1!-NQ1!` in symbol search builds a synthetic series. In the
  embeddable library the datafeed server must compute it ("the library does
  not handle the spread calculations").
- **Multi-chart layouts:** up to 16 charts; sync symbol, interval,
  crosshair, time and date range. Symbol and interval can be synced in
  groups (colored emoji tags). Drawings sync per symbol, within a layout or
  globally.
- **Indicators referencing other symbols** do it in code with
  `request.security("NASDAQ:MSFT", "D", close)` or with an `input.symbol`.
- **LumiBot needs:** indicators bound to a specific `Asset` (LumiBot already
  passes `asset=`), synthetic series from expressions, and a chart model
  with several panes and assets that can share a crosshair.

---

## 5. Competitors that use real languages

| Platform | Language | Indicator shape | Streaming model | Sandbox | Sharing | Price (checked 2026-10-23) |
|---|---|---|---|---|---|---|
| **QuantConnect LEAN** | Python, C# | Subclass `PythonIndicator`; `__init__` sets `name`, `warm_up_period`; `update(input) -> bool` returns ready; `is_ready`, `current`, `value`; `on_updated` events | `register_indicator(symbol, ind, resolution)` auto-feeds bars; `warm_up_indicator` | Cloud containers; Institution tier adds AES-256 code encryption | Projects, collaborators; no public indicator marketplace | Free; Researcher about $60/mo; Team about $120; Firm about $336; Institution about $1,080 (third-party price list) |
| **NinjaTrader 8** | C# | `OnStateChange()` with SetDefaults, Configure, DataLoaded; `AddPlot`, `AddDataSeries`; `OnBarUpdate()`; `Values[n][0]` | `Calculate.OnBarClose` / `OnEachTick` / `OnPriceChange`; history runs at bar close only; `IsFirstTickOfBar`; filter on `BarsInProgress` for multi-series | None; compiled DLLs run on the user's desktop | Exported assemblies, vendor licensing, large third-party ecosystem | Free; $99/mo; $1,499 lifetime (official) |
| **cTrader Algo** | C# and Python | `[Indicator(IsOverlay, AccessRights)]` class; `[Parameter]` properties; `[Output] IndicatorDataSeries`; `Initialize()`; `Calculate(int index)` | `Calculate` per historical index then per tick; referenced indicators lazy-load | `AccessRights.None` sandbox by default; `FullAccess` shows a warning dialog to the user | cTrader Store | Free with broker |
| **TradeStation EasyLanguage** | EasyLanguage | `Inputs:`, `Vars:`, `Plot1(...)`..; `MaxBarsBack` auto-detected | Left to right per bar; realtime on each tick or bar close; strategies can use Intrabar Order Generation and Look-Inside-Bar | Desktop; code stays on local machine | ELD files; protected ("encrypted") studies; TradeStation App Store | Free with funded account; tech fees otherwise |
| **TrendSpider** | JavaScript | Whole-array script: `close` is an array of all closes; helpers like `sma(close, 20)`; `paint()` and `color_cloud()`; `input()` | Whole series recomputed, not per bar [inferred from "get all candles at once"] | Runs in the browser and can be debugged with browser dev tools; same scripts power bots, alerts and scanners | Store for third-party add-ons | Standard $59/mo annual; $89 monthly; no free tier |
| **TakeProfit Indie** | Python subset | `@indicator` on `Main(MainContext)` with `__init__` and `calc()`; `@plot.line`, `@plot.fill`, `@level`, `@band`, `@param.*`; `@algorithm` for reusable series math; `@sec_context` + `calc_on` for other symbols | `calc()` per candle left to right, then per realtime update | "We run Indie code on our servers ... sandboxed"; no file or socket I/O; very limited imports | Platform library | Freemium |
| **Freqtrade** | Python | `populate_indicators(dataframe)` adds columns; `plot_config` dict with `main_plot` and `subplots`, each with `color`, `type` (scatter or bar), `fill_to` | Vectorized per candle close | Self-hosted, no sandbox | Strategy files on GitHub | Free, open source |

What to take from them:
- **The class shape everyone converges on:** declare metadata and inputs,
  one-time setup, a per-bar update, declared outputs with styles. LEAN,
  NinjaTrader, cTrader and Indie all look like this. TrendSpider and
  Freqtrade are vectorized instead.
- **Ready and warm-up are first class.** LEAN's `is_ready` and
  `warm_up_period`, EasyLanguage's `MaxBarsBack`.
- **Sandbox by default with a declared capability escape hatch.** cTrader's
  `AccessRights` is the cleanest public example: no network or files unless
  declared, and a visible warning to the user when it is declared.
- **TrendSpider and Indie both sell "use the same indicator in charts,
  alerts, scanners and bots"** as the headline. TrendSpider also ships an
  in-editor Claude model to write and port indicators from other languages.

---

## 6. What LumiBot must have for parity

Declaration and inputs
- [ ] An `Indicator` base class: `title`, `short_title`, `overlay` or pane,
      `scale`, `format`, `precision`, `max_bars_back`, drawing caps.
- [ ] Typed inputs: int, float, bool, color, string, text area, enum,
      symbol (Asset), timeframe, session, time, price, and **source**
      (another indicator's output). Groups, inline rows, tooltips,
      min/max/step, options, `active` dependency, `confirm`.
- [ ] Inputs serialize to a JSON schema so the BotSpot UI can render the
      settings dialog without running code.

Computation
- [ ] Per-bar `update(bar, state)` with a `confirmed` flag; full-history
      replay must equal streaming updates (same numbers).
- [ ] Intrabar rollback for forming bars plus an explicit per-tick
      persistence opt-in (the `varip` equivalent) and a `once` hook.
- [ ] Series history access with `[n]` and `na` semantics that match Pine.
- [ ] A `ta` module that is bit-identical to Pine for the ~70 `ta.*`
      functions (RMA-based RSI, ATR, Supertrend, pivots, session VWAP).
      PyneCore proves this is possible in Python.
- [ ] Multi-symbol and multi-timeframe requests with no-lookahead alignment
      by default, `gaps` control, lower-timeframe arrays, request dedupe.
- [ ] Corporate events, fundamentals and macro series available to
      indicators.

Visual output
- [ ] Plot styles: line, line-with-breaks, step, area, columns, histogram,
      circles, cross; per-point color; linewidth and linestyle; offset;
      show-last; track price; display flags.
- [ ] `plotshape`, `plotchar`, `plotarrow`, custom candles and bars,
      `hline`, `bgcolor`, `barcolor`.
- [ ] Fills between plots and between levels, gradient fills.
- [ ] Drawing objects with IDs: line, box, label, polyline, linefill, table;
      time-anchored; create, update, delete; caps with oldest-first eviction.
- [ ] `force_overlay` per output.

Alerts and strategies
- [ ] Indicator alert events with frequency and message templates, evaluated
      server side against a frozen indicator version and inputs.
- [ ] Trade markers, stop and target lines on the price chart from the
      strategy's real orders.

Sharing and versioning
- [ ] Publish an indicator as Open, Protected or Invite-only; private vs
      public listing.
- [ ] Immutable numbered versions with release notes.
- [ ] Libraries imported as `owner/name/version`.
- [ ] Saved instances (indicator + version + inputs + style), templates and
      layouts with share links and clone.

Runtime
- [ ] Server-side execution in a sandbox with time, memory, history and
      drawing quotas by plan, and result caching for identical
      (code hash, version, inputs, asset, timeframe, data window).

## 7. Ideas to exceed Pine

1. **Version pinning for users, not just libraries.** Pine silently moves
   charts to the newest code and leaves alerts on the old code. Let each
   chart, alert and live bot pin a version, show a "new version available"
   diff with release notes, and offer one-click upgrade with a backtest
   comparison before switching. [inferred gap from section 2.2]
2. **One indicator, every surface.** The same object runs in charts,
   backtests, live bots, alerts, scanners and the AI agent's tools, with
   identical numbers. TrendSpider and Indie advertise this; Pine splits
   indicators and strategies.
3. **Real Python, with a declared capability model.** numpy, pandas,
   pandas-ta, scikit-learn inside a sandbox that is closed by default and
   opened by declaration (cTrader `AccessRights` pattern) for things like
   HTTP data, with the user warned. Pine cannot import anything.
4. **Resource visibility.** Pine admits users cannot see what their script
   costs. Return per-indicator runtime, memory, bars processed and cache hit
   with every run.
5. **Transparent quotas instead of hidden ones.** Publish the numbers
   (history depth, run time, drawings) per plan and raise them for paying
   users. Bars beyond 40K are possible when data is local.
6. **No-lookahead by construction.** LumiBot's indicator accessor already
   refuses noncausal parameters. Make `request`-style calls causal by
   default and impossible to misuse, then show a "repaint check" that
   replays history as streaming and flags any value that changed.
7. **Pine import.** Accept Pine v6 source and convert it (PyneCore and
   PineTS show both conversion and native execution work at high
   fidelity). Licensing note: PyneCore is Apache 2.0, PineTS is AGPL-3.0,
   so PyneCore is the safer base to study or depend on. [inferred]
8. **AI-written indicators with tests.** TrendSpider ships Claude in its
   editor. LumiBot can go further: the agent writes the indicator, a parity
   test against a reference, and a repaint test, before it can be published.
9. **Data the chart does not have.** Broker positions, fills, options
   chains and Greeks, account equity and the AI agent's own research as
   indicator inputs.
10. **Unlimited plots and drawings in backtests.** Plot limits exist to
    protect a shared live chart; offline backtest charts can store more and
    downsample for display (LEAN's per-tier point caps show the trade-off).
11. **Invite-only with expiry and billing built in.** Pine vendors run access
    lists by hand outside TradingView. LumiBot/BotSpot can tie access to a
    subscription with an end date.

## 8. Open questions for Rob

- Should indicators execute only in the backtest and bot workers we already
  run, or also on a chart service that streams values to the website?
- Do we want Pine source import in scope, or only a Pine-shaped Python API?
- Which publish visibilities do we want at launch (Open only, or Protected
  and Invite-only too)?

## Sources checked

| Source type | What was checked | What it returned |
|---|---|---|
| TradingView Pine v6 User Manual | Limitations, Execution model, Inputs, Libraries, Publishing, Plots, Fills, Lines and boxes, Alerts, Strategies, Other timeframes and data, Release notes (fetched 2026-10-23) | Most capability and limit numbers in sections 1 to 3 |
| TradingView Reference Manual | Main page | JS-rendered, no body returned; ta count taken from PyneCore instead |
| TradingView Help Center | Update a published script (43000482583), Layouts and interaction (43000692404), Sync charts (43000629992), Layouts guide (43000746975), Multi-chart (43000629990), Supercharts guide (43000746464), What is Pine (43000561836) | Sharing, layouts, templates, favorites, sync |
| TradingView pricing page | /pricing (2026-10-23) | Plan limits table; yes/no rows not readable |
| TradingView Advanced Charts docs | Indicators page | `createStudy` model; spread operators computed by datafeed |
| GitHub | `gh search repos "pine script"`; PyneSys/pynecore README and `lib/ta.py`; LuxAlgo/PineTS README | PyneCore parity numbers, PineTS features, ta function count |
| X via Grok | 1 call, low effort, from 2025-01-01 | Dev posts on footprint, `request.security`, compile limits (@unclehk2000, 2026-10-14), TakeProfit pitch; no TradingView staff posts |
| Reddit | `reddit_search` r/TradingView (1 success, 1 rate-limited); `reddit_thread` "Are there alternatives to Pine Script?" (2024) | Server-side execution confirmed by users; RSI/RMA mismatch thread; JS-instead-of-Pine request |
| Hacker News | `hn_search` "Pine Script TradingView"; `hn_thread` PineForge | Third-party tools only (PineCone, PineForge); no comments, no architecture talk |
| YouTube (vidIQ + transcripts) | "Pine Script v6 Full course" by The Art of Trading (0eSP4LXP1V0, Apr 2025, 91K views), transcript 42:20 to 48:00 on namespaces and libraries; "Lines Function in Pine Script" by Pine Wizards (XZhQWvabR8M, 2023), full transcript. No audio played. | Libraries behave like built-in namespaces once imported; `line.new` needs a variable and manual `line.delete` to avoid hitting caps |
| Medium | Exa search: algobt (2023), Betashorts (Sept 2025, Mar 2026), "best coding language for a trader" (Sept 2024) | Repainting and `request.security` pitfalls, commission default zero, Indie runs on servers |
| Substack | Exa search with Medium | Nothing relevant returned |
| QuantConnect docs | Custom indicators, Charting, Pricing | Indicator class shape, chart quotas, tier limits |
| NinjaTrader docs | OnBarUpdate, Calculate, Pricing | Event model, Calculate modes, prices |
| cTrader docs | Create indicator, code samples, access rights, lifecycle | Class shape, `AccessRights` sandbox, Python support |
| TradeStation | EasyLanguage Essentials PDF, Learning EasyLanguage Strategies PDF | MaxBarsBack, per-bar model, intrabar order generation, protected code |
| TrendSpider | Help Center JS scripting, capabilities page, 2023 scripting update blog, pricing | JS whole-array model, browser debugging, alt data requests, prices. Docs site is JS-rendered; "Comparison to Pine" section could not be read |
| TakeProfit Indie docs | Code structure, differences from Python, syntactic sugar, quick start | Python-subset class shape, server sandbox |
| Freqtrade docs | Plotting | `plot_config` shape |
| Firecrawl | Not used | Exa and WebFetch covered every page needed |
| Our own data | LumiBot source (`indicators.py`, `strategy.py`), prior docs (`docs/ADD_OHLC_IMPLEMENTATION.md`) | Baseline section |
| Product reviews (G2, Trustpilot) | Not checked | Out of scope for an API design question; recommend if we price the feature |
