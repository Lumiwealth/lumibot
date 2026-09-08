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

Only rows at or before the strategy datetime enter a calculation. Results are
memoized for identical observed input and parameters, not for the whole future
backtest dataset. Negative offsets and explicitly noncausal parameters fail
visibly. Source adapters retain ownership of bar timestamp/completion semantics.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any, Callable

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_TA_MODULE: Any = None


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
            f"As-of pandas-ta-classic ``{name}`` indicator for the given "
            f"asset+timestep. Identical observed input reuses the cached result."
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
            Function to run over a copy of history available as of strategy time.
        asset : Asset
            Underlying asset.
        timestep : str
            ``"day"``, ``"minute"``, etc. — matched against the data source.
        **kwargs
            Forwarded to ``fn`` and included in the cache key.
        """
        if not callable(fn):
            raise TypeError(f"custom indicator fn must be callable, got {type(fn).__name__}")
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
        self._validate_causal_parameters(kwargs)
        key = self._cache_key(asset, timestep, name, kwargs)
        df = self._full_history(asset, timestep)
        if df is None or df.empty:
            return None
        if not isinstance(df.index, pd.DatetimeIndex) or not df.index.is_monotonic_increasing or not df.index.is_unique:
            raise ValueError("Indicator history requires a unique, increasing DatetimeIndex.")
        # Slicing OUTPUT cannot make an arbitrary calculation causal: negative
        # shifts, centered windows and custom functions can use later rows.
        # Copy also prevents a custom function from mutating the provider cache.
        now = self._strategy.get_datetime()
        end = self._position_at(df.index, now) + 1
        df = df.iloc[:end].copy()
        if df.empty:
            return None
        digest = hashlib.sha256(pd.util.hash_pandas_object(df, index=True).values.tobytes()).hexdigest()
        data_tag = (tuple(df.columns), digest, custom_fn)
        cached = self._cache.get(key)
        if cached is None or cached[0] != data_tag:
            result = self._compute(df, name, kwargs, custom_fn)
            self._cache[key] = (data_tag, result)
        return self._at_current_bar(self._cache[key][1])

    @staticmethod
    def _validate_causal_parameters(kwargs):
        offset = kwargs.get("offset", 0)
        if offset is not None:
            try:
                valid_offset = np.isfinite(float(offset)) and float(offset) >= 0 and float(offset).is_integer()
            except (TypeError, ValueError, OverflowError):
                valid_offset = False
            if not valid_offset:
                raise ValueError("Only causal indicators are supported: offset must be a nonnegative integer.")
        if kwargs.get("center") or kwargs.get("lookahead"):
            raise ValueError("Only causal indicators are supported: center and lookahead must be false.")

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

    def _full_history(self, asset, timestep) -> pd.DataFrame | None:
        """Return the full known bar series DataFrame for ``asset``.

        In backtest mode this may contain the entire simulated dataset.
        ``_dispatch`` restricts the INPUT before computing, never just output.

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
            bars = self._strategy.get_historical_prices(asset, length=self._fallback_length, timestep=timestep)
        except Exception as exc:
            logger.debug("indicators: get_historical_prices fallback failed for %s: %s", asset, exc)
            bars = None

        df = self._read_store_df(data_source, asset, timestep)
        if df is not None:
            return df

        if bars is None:
            return None
        return getattr(bars, "df", None)

    def _read_store_df(self, data_source, asset, timestep) -> pd.DataFrame | None:
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

        def matches_timeframe(data):
            stored_timestep = getattr(data, "timestep", None)
            # Older/custom stores can omit timeframe metadata. When it is
            # present, never relabel daily bars as intraday (or vice versa).
            # Resampling belongs to the selected data source's history method.
            return stored_timestep is None or timestep is None or stored_timestep == timestep

        if hasattr(data_source, "find_asset_in_data_store"):
            for ts_arg in (timestep, None):
                try:
                    key = (
                        data_source.find_asset_in_data_store(asset, timestep=ts_arg)
                        if ts_arg
                        else data_source.find_asset_in_data_store(asset)
                    )
                except TypeError:
                    try:
                        key = data_source.find_asset_in_data_store(asset)
                    except Exception:
                        key = None
                except Exception:
                    key = None
                if key is not None and key in store and matches_timeframe(store[key]):
                    return store[key]
        for stored_key, data in store.items():
            stored_asset = stored_key[0] if isinstance(stored_key, tuple) else stored_key
            if stored_asset == asset and matches_timeframe(data):
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
        # These pandas-ta indicators otherwise enable noncausal components by
        # default. An explicit true is rejected before reaching this boundary.
        if name in {"dpo", "ichimoku"}:
            call_args["lookahead"] = False
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
