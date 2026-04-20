Indicators
==========

``self.indicators`` is a per-strategy technical-indicator accessor that computes
any indicator **once** over the full bar series for an asset, then hands back
the value at the current bar in **O(1)** on every subsequent call. It replaces
the common per-iteration pattern::

    # slow: recomputes the full-history rolling mean every iteration
    df = bars.df.copy()
    df["sma200"] = df["close"].rolling(200).mean()
    latest = df.iloc[-1]

with::

    sma200 = self.indicators.sma(asset, length=200)  # O(log N) per call, compute-once

The same API works in backtest and live. The memo lives on the strategy
instance and dies with it — no disk cache, no cross-run persistence.

Why this matters
----------------

A 13-year daily backtest with a 300-bar indicator recomputes the rolling
window ~3,200 times in the hand-rolled pattern — that is ~1M redundant
window-ops on the simulated data. The indicator subsystem collapses that to a
single full-series compute plus one O(log N) positional lookup per iteration.

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

Custom indicators
-----------------

For user-defined indicators use :py:meth:`~lumibot.indicators.Indicators.custom`::

    def squeeze_momentum(df, length=20, mult=2.0):
        # df is the full-history DataFrame. Return a Series or DataFrame.
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

``fn`` receives the **full-history** DataFrame for ``(asset, timestep)`` and
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

The memo entry also stores a data-tag of
``(len(df), df.index[-1])``. When the underlying bar series grows — common in
routed/live modes where the data store is populated lazily — the indicator
transparently recomputes on the next call. Strategies never see stale output.

Current-bar semantics
---------------------

The indicator is computed once against the full series, but the **returned
value** is always sliced to the most-recent bar at-or-before
``self.get_datetime()``. This means:

- In backtest mode the strategy sees bar ``t`` values on iteration ``t`` —
  no lookahead, despite the compute running over the whole simulated
  dataset.
- As backtest time advances, the same cached result is re-indexed at the
  new current bar in O(log N) via ``Index.searchsorted``.
- Querying a time before the first bar returns ``NaN`` (scalar) or
  ``None`` (row).

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
