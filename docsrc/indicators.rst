Indicators
==========

``self.indicators`` is a per-strategy technical-indicator accessor. It computes
against history **at or before strategy time**, returning the current value.
Repeated calls with identical observed bars and parameters reuse their result.
It replaces the common per-iteration pattern::

    # slow: recomputes the full-history rolling mean every iteration
    df = bars.df.copy()
    df["sma200"] = df["close"].rolling(200).mean()
    latest = df.iloc[-1]

with::

    sma200 = self.indicators.sma(asset, length=200)

The same API works in backtest and live. The memo lives on the strategy
instance and dies with it — no disk cache, no cross-run persistence.

Why this matters
----------------

Calculating against future rows and trimming only the result is unsafe for
centered windows, negative offsets and custom functions. The accessor restricts
the input first. This trades the former full-backtest precomputation speedup for
temporal correctness. Repeated unchanged inputs still reuse computation, but
checking the observed-input fingerprint takes time proportional to history size.

Built-in pandas-ta-classic passthrough
--------------------------------------

Every indicator exposed by ``pandas-ta-classic`` (~130 indicators) is callable
as ``self.indicators.<name>(asset, timestep="day", **kwargs)``::

    sma20  = self.indicators.sma(asset, length=20)
    rsi14  = self.indicators.rsi(asset, length=14)
    ema50  = self.indicators.ema(asset, length=50)
    atr14  = self.indicators.atr(asset, length=14)
    macd   = self.indicators.macd(asset, fast=12, slow=26, signal=9)  # multi-column
    bb     = self.indicators.bbands(asset, length=20, std=2)           # multi-column

Single-column indicators (``sma``, ``rsi``, ``ema``, …) return a float — the
indicator value at the current bar.

Multi-column indicators (``bbands``, ``macd``, ``stoch``, …) return an
:py:class:`~lumibot.indicators.IndicatorRow` — an attribute-style read-only
view over the current-bar row::

    bb = self.indicators.bbands(asset, length=20, std=2)
    lower = bb.BBL_20_2_0
    upper = bb.BBU_20_2_0
    # Dots in column names are normalized to underscores; bracket access also works.
    lower_alt = bb["BBL_20_2.0"]

    if "BBL_20_2.0" in bb:
        ...

If the indicator has not yet accumulated enough bars to produce a value, the
return is ``NaN`` (scalar) or a row containing ``NaN`` (multi-column). If the
data source has no bars at all for the asset, the return is ``None``.

Fibonacci range retracements
----------------------------

``fibonacci`` returns observed range bounds and the standard 0, 23.6, 38.2,
50, 61.8, 78.6 and 100 percent retracement prices::

    levels = self.indicators.fibonacci(asset, direction="up", length=200)
    halfway = levels["retracement_0.5"]

``direction="up"`` measures down from the high; ``direction="down"`` measures
up from the low. Direction is explicit: this calculation does not identify a
trend, choose swing pivots, or recommend a trade. Missing warmup returns no value;
nonfinite or crossed high/low data and unsupported parameters fail visibly.

Agent ``get_indicator`` and ``get_indicators`` accept ``indicator="fibonacci"``.
Use independently named batch requests with explicit zoned ``start``/``end``
bounds for each completed month and for the annual window. Each calculation
uses only its own window; it cannot borrow prices from another month or future
bars. Combine these requests with independently parameterized RSI, SMA50,
SMA200 and intraday VWAP requests in the same batch.

Custom indicators
-----------------

For user-defined indicators use :py:meth:`~lumibot.indicators.Indicators.custom`::

    def squeeze_momentum(df, length=20, mult=2.0):
        # df is an isolated copy of as-of history. Return a Series or DataFrame.
        basis = df["close"].rolling(length).mean()
        dev   = df["close"].rolling(length).std(ddof=0)
        return pd.DataFrame({
            "basis": basis,
            "upper": basis + mult * dev,
            "lower": basis - mult * dev,
        }, index=df.index)

    row = self.indicators.custom(
        "sqz_mom", squeeze_momentum, asset, timestep="day", length=20, mult=2.0,
    )
    upper = row.upper

``fn`` receives an isolated **as-of history** DataFrame for ``(asset, timestep)`` and
must return a :class:`pandas.Series` (scalar-per-bar) or
:class:`pandas.DataFrame` (multi-column per-bar) indexed by the same
``DatetimeIndex``. ``**kwargs`` are forwarded to ``fn`` and folded into the
cache key, so distinct parameter sets produce distinct memo entries.

Cache key and staleness
-----------------------

Indicator results are keyed on
``(asset, timestep, name, sorted-kwargs)``. Different ``length``, ``std``,
``fast``, or any other keyword produces a distinct cache entry, so
``self.indicators.sma(asset, length=20)`` and
``self.indicators.sma(asset, length=50)`` each run and memoize independently.

The memo fingerprints observed values, timestamps, columns and custom function
identity. New bars, corrections to existing bars, and rewinding simulated time
invalidate the corresponding result. A custom function cannot mutate the source
cache through the DataFrame it receives.

Current-bar semantics
---------------------

Input and returned values are restricted to timestamps at or before
``self.get_datetime()``. A time before the first available bar returns ``None``.
History must have a unique, increasing ``DatetimeIndex`` with a timezone
compatible with strategy time; invalid history fails visibly.

Negative/fractional offsets, ``center=True`` and ``lookahead=True`` are rejected.
Nonnegative integer offsets remain supported. DPO and Ichimoku use
``lookahead=False`` by default. Missing warmup remains missing, not zero.

Bar timestamps and completion semantics remain the selected data source's
contract. This accessor does not infer an exchange session close from a daily
date label. Restricting future rows alone must not be treated as proof that a
provider's current bar is complete.

When a cached series declares its timestep, it must match the requested timestep.
Missing intraday data cannot silently use daily bars. Requests for another
timeframe use the data source's historical-price method, including its resampling
and availability rules.

API reference
-------------

.. currentmodule:: lumibot.indicators

.. autoclass:: Indicators
   :members: custom, invalidate, cache_size

.. autoclass:: IndicatorRow
   :members: as_dict

Migration guide
---------------

**Before** — per-iteration hand-roll inside ``on_trading_iteration``::

    bars = self.get_historical_prices(asset, length=300, timestep="day")
    df = bars.df.copy()
    df["sma200"] = df["close"].rolling(200).mean()
    df["rsi14"]  = ta.rsi(df["close"], length=14)
    latest = df.iloc[-1]
    sma200 = latest["sma200"]
    rsi14  = latest["rsi14"]

**After**::

    sma200 = self.indicators.sma(asset, length=200)
    rsi14  = self.indicators.rsi(asset, length=14)

**Before** — custom indicator factored into ``compute_indicators(df)``::

    def compute_indicators(self, df):
        df["basis"] = df["close"].rolling(20).mean()
        df["sqz"]   = (df["basis"] > df["basis"].shift(1)).astype(int)
        return df

    def on_trading_iteration(self):
        bars = self.get_historical_prices(asset, length=300, timestep="day")
        df = self.compute_indicators(bars.df.copy())
        latest = df.iloc[-1]
        ...

**After** — pass the same function to ``custom``, keep the ``latest`` row::

    def on_trading_iteration(self):
        latest = self.indicators.custom(
            "sqz_mom", self.compute_indicators, asset, timestep="day",
        )
        if latest is None:
            return
        ...

``compute_indicators`` runs exactly once per asset/timestep; every subsequent
iteration returns the current-bar row in O(log N) without re-running the
rolling-window math.

See :doc:`standalone_components` for use in scripts and notebooks.
