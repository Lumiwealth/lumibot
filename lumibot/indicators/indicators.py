"""Per-strategy technical indicator accessor.

Usage inside a Strategy::

    # Built-in pandas-ta-classic passthrough (~130 indicators available)
    sma200 = self.indicators.sma(asset, length=200)           # float
    rsi14  = self.indicators.rsi(asset, length=14)            # float
    bb     = self.indicators.bbands(asset, length=20, std=2)  # IndicatorRow
    print(bb.BBL_20_2_0, bb.BBM_20_2_0, bb.BBU_20_2_0)

    # Custom user-defined indicator (takes df, returns Series or DataFrame)
    latest = self.indicators.custom(
        "my_signal", my_signal_fn, asset, timestep="day", length=50,
    )

Behavior
--------
- The first call for a given ``(asset, timestep, indicator_name, kwargs)`` runs
  the indicator over the **full known bar series** for that asset and
  memoizes the result on the ``Indicators`` instance.
- Every subsequent call with the same key returns the value at the current
  strategy datetime in O(1) (constant-time pandas slice + iloc).
- The memo lives on the strategy's ``self.indicators`` and dies with the
  strategy instance. No disk cache, no cross-run persistence.

Why
---
Traditional strategies call something like::

    df = bars.df.copy()
    df["sma"] = df["close"].rolling(200).mean()
    df["rsi"] = df["close"].rolling(14).apply(my_rsi)
    latest = df.iloc[-1]

That recomputes the full indicator over the full lookback every iteration
(O(N * W) total, where N is iteration count and W is window length). For a
13-year daily backtest with a 300-bar lookback that is ~1M redundant ops.

Computing the indicator **once** over the whole series and then indexing by
current datetime collapses that to O(N + W) total — the exact speedup users
are missing when they hand-roll indicator code inside ``on_trading_iteration``.
"""
from __future__ import annotations

import logging
from importlib import import_module
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

_TA_MODULE: Any = None


class _LazyModule:
    __slots__ = ("_module_name", "_module")

    def __init__(self, module_name: str):
        object.__setattr__(self, "_module_name", module_name)
        object.__setattr__(self, "_module", None)

    def _load(self):
        module = object.__getattribute__(self, "_module")
        if module is None:
            module = import_module(object.__getattribute__(self, "_module_name"))
            object.__setattr__(self, "_module", module)
        return module

    def __getattr__(self, name):
        return getattr(self._load(), name)

    def __setattr__(self, name, value):
        setattr(self._load(), name, value)

    def __delattr__(self, name):
        if name in {"_module_name", "_module"}:
            object.__delattr__(self, name)
        else:
            delattr(self._load(), name)


np = _LazyModule("numpy")
pd = _LazyModule("pandas")


def _get_ta_module():
    """Lazy import pandas-ta-classic so the indicators module is importable
    even in environments where the indicator backend is missing (e.g. slim
    installs that never call ``self.indicators``).
    """
    global _TA_MODULE
    if _TA_MODULE is None:
        if not hasattr(np, "NaN"):
            np.NaN = np.nan
        import pandas_ta_classic as ta

        _TA_MODULE = ta
    return _TA_MODULE


class IndicatorRow:
    """Attribute-style read-only view over a single pandas Series (one row of
    a multi-column indicator output).

    Given a DataFrame indicator result like pandas-ta's ``bbands``
    (columns ``BBL_20_2.0``, ``BBM_20_2.0``, ``BBU_20_2.0`` …), this wrapper
    lets the strategy write ``bb.BBL_20_2_0`` or ``bb["BBL_20_2.0"]``.
    """

    __slots__ = ("_data",)

    def __init__(self, data: pd.Series):
        self._data = data

    def __getattr__(self, name: str):
        data = object.__getattribute__(self, "_data")
        if name in data.index:
            return data[name]
        normalized = {str(col).replace(".", "_").replace("-", "_"): col for col in data.index}
        if name in normalized:
            return data[normalized[name]]
        raise AttributeError(name)

    def __getitem__(self, key):
        return self._data[key]

    def __contains__(self, key):
        return key in self._data.index

    def as_dict(self) -> dict:
        return dict(self._data)

    def __repr__(self) -> str:
        return f"IndicatorRow({dict(self._data)!r})"


class Indicators:
    """Per-strategy indicator accessor. See module docstring for usage."""

    def __init__(self, strategy):
        self._strategy = strategy
        self._cache: dict = {}
        self._fallback_length: int = 10_000

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        ta = _get_ta_module()
        if not hasattr(ta, name):
            raise AttributeError(
                f"pandas_ta_classic has no indicator named {name!r}. "
                f"For user-defined indicators use self.indicators.custom(name, fn, asset, ...)."
            )

        def _call(asset, timestep: str = "day", **kwargs):
            return self._dispatch(asset, timestep, name, kwargs, custom_fn=None)

        _call.__name__ = f"indicators.{name}"
        _call.__doc__ = (
            f"Precomputed pandas-ta-classic ``{name}`` indicator for the given "
            f"asset+timestep. First call runs the full-history compute; every "
            f"subsequent call is an O(1) lookup at the current strategy bar."
        )
        return _call

    def custom(
        self,
        name: str,
        fn: Callable[..., Any],
        asset,
        timestep: str = "day",
        **kwargs,
    ):
        """Register-and-evaluate a user-defined indicator.

        Parameters
        ----------
        name : str
            Arbitrary label used in the cache key. Give each distinct user
            indicator a stable label so repeat calls hit the memo.
        fn : callable
            ``fn(df, **kwargs) -> pandas.Series | pandas.DataFrame``.
            Function to run once over the full history DataFrame.
        asset : Asset
            Underlying asset.
        timestep : str
            ``"day"``, ``"minute"``, etc. — matched against the data source.
        **kwargs
            Forwarded to ``fn`` and included in the cache key.
        """
        if not callable(fn):
            raise TypeError(
                f"custom indicator fn must be callable, got {type(fn).__name__}"
            )
        return self._dispatch(asset, timestep, name, kwargs, custom_fn=fn)

    def invalidate(self, asset=None) -> None:
        """Drop memoized indicator results.

        With no argument, clears everything. With an asset, drops only that
        asset's entries (useful if a user ever needs to force a recompute —
        should be rare since the memo is per-strategy-instance).
        """
        if asset is None:
            self._cache.clear()
            return
        asset_key = self._asset_key(asset)
        for key in [k for k in self._cache if k[0] == asset_key]:
            self._cache.pop(key, None)

    @property
    def cache_size(self) -> int:
        """Number of memoized indicator results currently held."""
        return len(self._cache)

    def _dispatch(self, asset, timestep, name, kwargs, custom_fn):
        key = self._cache_key(asset, timestep, name, kwargs)
        df = self._full_history(asset, timestep)
        if df is None or df.empty:
            return None
        data_tag = (len(df), df.index[-1])
        cached = self._cache.get(key)
        if cached is None or cached[0] != data_tag:
            result = self._compute(df, name, kwargs, custom_fn)
            self._cache[key] = (data_tag, result)
        return self._at_current_bar(self._cache[key][1])

    def _asset_key(self, asset):
        if hasattr(asset, "symbol"):
            return (asset.symbol, getattr(asset, "asset_type", None))
        return (str(asset), None)

    def _cache_key(self, asset, timestep, name, kwargs):
        kw_items = []
        for k, v in sorted(kwargs.items()):
            try:
                hash(v)
                kw_items.append((k, v))
            except TypeError:
                kw_items.append((k, repr(v)))
        return (self._asset_key(asset), timestep, name, tuple(kw_items))

    def _full_history(self, asset, timestep) -> Optional[pd.DataFrame]:
        """Return the full known bar series DataFrame for ``asset``.

        In backtest mode (PANDAS-style data source) this returns the entire
        simulated dataset — the indicator output is sliced to current-bar
        in ``_at_current_bar``, so the strategy never sees future values.

        In routed/live modes ``_data_store`` is populated lazily. If it is
        empty we call ``get_historical_prices`` with a large length to force
        the adapter to prefetch, then re-read the resulting full series.
        """
        broker = getattr(self._strategy, "broker", None)
        data_source = getattr(broker, "data_source", None) if broker is not None else None

        df = self._read_store_df(data_source, asset, timestep)
        if df is not None:
            return df

        try:
            bars = self._strategy.get_historical_prices(
                asset, length=self._fallback_length, timestep=timestep
            )
        except Exception as exc:
            logger.debug("indicators: get_historical_prices fallback failed for %s: %s", asset, exc)
            bars = None

        df = self._read_store_df(data_source, asset, timestep)
        if df is not None:
            return df

        if bars is None:
            return None
        return getattr(bars, "df", None)

    def _read_store_df(self, data_source, asset, timestep) -> Optional[pd.DataFrame]:
        if data_source is None or getattr(data_source, "_data_store", None) is None:
            return None
        data_obj = self._find_in_store(data_source, asset, timestep)
        if data_obj is None or not hasattr(data_obj, "df"):
            return None
        df = data_obj.df
        if df is None or df.empty:
            return None
        return df

    def _find_in_store(self, data_source, asset, timestep=None):
        store = data_source._data_store
        if hasattr(data_source, "find_asset_in_data_store"):
            for ts_arg in (timestep, None):
                try:
                    key = data_source.find_asset_in_data_store(asset, timestep=ts_arg) if ts_arg else data_source.find_asset_in_data_store(asset)
                except TypeError:
                    try:
                        key = data_source.find_asset_in_data_store(asset)
                    except Exception:
                        key = None
                except Exception:
                    key = None
                if key is not None and key in store:
                    return store[key]
        for stored_key, data in store.items():
            stored_asset = stored_key[0] if isinstance(stored_key, tuple) else stored_key
            if stored_asset == asset:
                return data
        return None

    def _compute(self, df, name, kwargs, custom_fn):
        if custom_fn is not None:
            return custom_fn(df, **kwargs)
        ta = _get_ta_module()
        fn = getattr(ta, name)
        call_args = {}
        for col in ("open", "high", "low", "close", "volume"):
            if col in df.columns:
                call_args[col] = df[col]
        call_args.update(kwargs)
        return fn(**call_args)

    def _at_current_bar(self, result):
        now = self._strategy.get_datetime()
        if isinstance(result, pd.Series):
            return self._latest_scalar(result, now)
        if isinstance(result, pd.DataFrame):
            row = self._latest_row(result, now)
            if row is None:
                return None
            return IndicatorRow(row)
        return result

    @staticmethod
    def _position_at(index: pd.Index, now) -> int:
        """Return the integer position of the most recent bar at-or-before ``now``.

        Uses ``searchsorted`` for O(log N) lookup without allocating a slice.
        Returns -1 if no bar is on-or-before ``now``.
        """
        try:
            pos = index.searchsorted(now, side="right") - 1
        except TypeError:
            idx = index.get_indexer([now], method="pad")
            return int(idx[0]) if len(idx) else -1
        return int(pos)

    @staticmethod
    def _latest_scalar(series: pd.Series, now):
        if series.empty:
            return np.nan
        pos = Indicators._position_at(series.index, now)
        if pos < 0:
            return np.nan
        return series.iloc[pos]

    @staticmethod
    def _latest_row(df: pd.DataFrame, now):
        if df.empty:
            return None
        pos = Indicators._position_at(df.index, now)
        if pos < 0:
            return None
        return df.iloc[pos]
