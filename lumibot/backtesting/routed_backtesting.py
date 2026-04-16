from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import pandas as pd

from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
from lumibot.constants import LUMIBOT_DEFAULT_PYTZ
from lumibot.credentials import ALPACA_CONFIG, COINBASE_CONFIG, KRAKEN_CONFIG, POLYGON_API_KEY
from lumibot.entities import Asset, Data
from lumibot.tools import ibkr_helper
from lumibot.tools import polygon_helper
from lumibot.tools.helpers import parse_timestep_qty_and_unit

logger = logging.getLogger(__name__)

_DEFAULT_QUOTE_ASSET = Asset("USD", "forex")


class RoutingProviderError(ValueError):
    pass


@dataclass(frozen=True)
class ProviderSpec:
    provider: str
    ccxt_exchange_id: str | None = None
    raw: str | None = None


def _normalize_token(value: str) -> str:
    return re.sub(r"[\s_\-]+", "", (value or "").strip().lower())


def _normalize_asset_type(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if "." in raw:
        raw = raw.split(".")[-1]
    return raw


def _ibkr_include_after_hours(asset_type: str, timestep_unit: str) -> bool:
    """Return IBKR outsideRth policy for routed backtests.

    For stock/index daily bars we explicitly use regular-session bars (outsideRth=false)
    to match ThetaData/Yahoo daily close semantics and avoid after-hours-driven signal drift.
    """
    return not (asset_type in {"stock", "index"} and timestep_unit == "day")


def _ccxt_exchange_id_from_token(token: str) -> str | None:
    """Resolve a user token to a CCXT exchange id (case/sep-insensitive).

    This is intentionally lazy (imports ccxt only when needed) and does not perform any network I/O.
    """
    token_norm = _normalize_token(token)
    if not token_norm:
        return None
    try:
        import ccxt  # type: ignore
    except Exception:
        return None

    exchange_by_norm = {_normalize_token(e): e for e in getattr(ccxt, "exchanges", []) or []}
    return exchange_by_norm.get(token_norm)


def _infer_default_ccxt_exchange_id() -> str:
    """Infer a CCXT exchange id from existing environment/credentials.

    This avoids introducing a new env var just for routing. When nothing is configured,
    we fall back to CCXT's common default used in this codebase: binance.
    """
    env_hint = (os.environ.get("DATA_SOURCE") or os.environ.get("TRADING_BROKER") or "").strip()
    if env_hint:
        resolved = _ccxt_exchange_id_from_token(env_hint)
        if resolved:
            return resolved

    if (COINBASE_CONFIG.get("apiKey") or "").strip():
        return "coinbase"
    if (KRAKEN_CONFIG.get("apiKey") or "").strip():
        return "kraken"
    return "binance"


def _align_timestamp_to_index_tz(ts_value: datetime, ref_index_ts: pd.Timestamp) -> pd.Timestamp:
    """Return a comparable timestamp aligned to the timezone mode of `ref_index_ts`."""
    ts = pd.Timestamp(ts_value)
    ref = pd.Timestamp(ref_index_ts)
    if ref.tzinfo is not None and ts.tzinfo is None:
        ts = ts.tz_localize(ref.tzinfo)
    elif ref.tzinfo is None and ts.tzinfo is not None:
        ts = ts.tz_localize(None)
    elif ref.tzinfo is not None and ts.tzinfo is not None and ts.tzinfo != ref.tzinfo:
        ts = ts.tz_convert(ref.tzinfo)
    return ts


def _is_day_like_timestep(value: str) -> bool:
    """Return True when a timestep string represents daily cadence.

    Accepts aliases like `day`, `1d`, `1day`, and normalized quantities returned by
    `parse_timestep_qty_and_unit`.
    """
    raw = str(value or "").strip().lower()
    if not raw:
        return False
    try:
        _qty, unit = parse_timestep_qty_and_unit(raw)
        return str(unit).strip().lower() == "day"
    except Exception:
        return raw in {"day", "1d", "1day", "d"}


class _RoutingAdapter:
    provider_key: str

    def __init__(self, router: "RoutedBacktestingPandas"):
        self._router = router

    def update_pandas_data(
        self,
        *,
        asset: Asset,
        quote_asset: Asset,
        length: int,
        timestep: str,
        start_dt: datetime | None,
        require_quote_data: bool,
        require_ohlc_data: bool,
        snapshot_only: bool,
        provider_spec: ProviderSpec,
    ):
        raise NotImplementedError


class _ThetaDataRoutingAdapter(_RoutingAdapter):
    provider_key = "thetadata"

    def update_pandas_data(
        self,
        *,
        asset: Asset,
        quote_asset: Asset,
        length: int,
        timestep: str,
        start_dt: datetime | None,
        require_quote_data: bool,
        require_ohlc_data: bool,
        snapshot_only: bool,
        provider_spec: ProviderSpec,
    ):
        if snapshot_only:
            return None
        return ThetaDataBacktestingPandas._update_pandas_data(
            self._router,
            asset,
            quote_asset,
            length,
            timestep,
            start_dt=start_dt,
            require_quote_data=require_quote_data,
            require_ohlc_data=require_ohlc_data,
            snapshot_only=snapshot_only,
        )


class _DataFrameRoutingAdapter(_RoutingAdapter):
    """Base adapter for providers that return a pandas DataFrame and share the router data store."""

    _default_start_buffer = timedelta(days=5)

    def __init__(self, router: "RoutedBacktestingPandas"):
        super().__init__(router)
        self._fully_loaded_series: set[Any] = set()

    def _start_buffer(self, asset: Asset, provider_spec: ProviderSpec) -> timedelta:
        return self._default_start_buffer

    def _fetch_df(
        self,
        *,
        asset: Asset,
        quote_asset: Asset,
        ts_unit: str,
        start_datetime: datetime,
        end_dt: datetime,
        length: int,
        canonical_key: Any,
        provider_spec: ProviderSpec,
        require_quote_data: bool,
        require_ohlc_data: bool,
    ) -> pd.DataFrame | None:
        raise NotImplementedError

    def update_pandas_data(
        self,
        *,
        asset: Asset,
        quote_asset: Asset,
        length: int,
        timestep: str,
        start_dt: datetime | None,
        require_quote_data: bool,
        require_ohlc_data: bool,
        snapshot_only: bool,
        provider_spec: ProviderSpec,
    ):
        if snapshot_only:
            return None

        end_dt = start_dt if isinstance(start_dt, datetime) else self._router.get_datetime()
        ts = timestep or self._router.get_timestep()

        start_datetime, ts_unit = self._router.get_start_datetime_and_ts_unit(
            length,
            ts,
            start_dt=end_dt,
            start_buffer=self._start_buffer(asset, provider_spec),
        )

        if ts_unit == "day":
            try:
                self._router._effective_day_mode = True
            except Exception:
                pass

        canonical_key, legacy_key = self._router._build_dataset_keys(asset, quote_asset, ts_unit)
        existing = self._router._data_store.get(canonical_key)
        existing_df = getattr(existing, "df", None) if existing is not None else None

        if existing_df is not None and isinstance(existing_df, pd.DataFrame) and not existing_df.empty:
            try:
                existing_start = existing_df.index.min()
                existing_end = existing_df.index.max()
                if existing_start is not None and existing_end is not None:
                    start_cmp = _align_timestamp_to_index_tz(start_datetime, existing_start)
                    end_cmp = _align_timestamp_to_index_tz(end_dt, existing_end)
                    if start_cmp >= existing_start and end_cmp <= existing_end:
                        return None
            except Exception:
                pass

        df = self._fetch_df(
            asset=asset,
            quote_asset=quote_asset,
            ts_unit=ts_unit,
            start_datetime=start_datetime,
            end_dt=end_dt,
            length=length,
            canonical_key=canonical_key,
            provider_spec=provider_spec,
            require_quote_data=require_quote_data,
            require_ohlc_data=require_ohlc_data,
        )

        if df is None or df.empty:
            return None

        if isinstance(df.index, pd.DatetimeIndex):
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC")
            df.index = df.index.tz_convert(LUMIBOT_DEFAULT_PYTZ)
            df = df.sort_index()

        if existing_df is not None and isinstance(existing_df, pd.DataFrame) and not existing_df.empty:
            merged = pd.concat([existing_df, df], axis=0).sort_index()
            merged = merged[~merged.index.duplicated(keep="last")]
        else:
            merged = df

        data = Data(asset, merged, timestep=ts_unit, quote=quote_asset)
        self._router._data_store[canonical_key] = data
        if legacy_key not in self._router._data_store:
            self._router._data_store[legacy_key] = data
        return None


class _IbkrRoutingAdapter(_DataFrameRoutingAdapter):
    provider_key = "ibkr"
    _default_start_buffer = timedelta(0)

    @staticmethod
    def _normalize_timestep_key(timestep: str) -> str:
        """Normalize a user-facing timestep into a stable IBKR series key.

        Mirrors InteractiveBrokersRESTBacktesting normalization so router and IBKR-only backtests
        share cache keys and can leverage native multi-minute datasets (e.g., "60m" -> "60minute").
        """
        qty, unit = parse_timestep_qty_and_unit(timestep)
        qty = int(qty)
        unit = str(unit)
        return unit if qty == 1 else f"{qty}{unit}"

    def update_pandas_data(
        self,
        *,
        asset: Asset,
        quote_asset: Asset,
        length: int,
        timestep: str,
        start_dt: datetime | None,
        require_quote_data: bool,
        require_ohlc_data: bool,
        snapshot_only: bool,
        provider_spec: ProviderSpec,
    ):
        """IBKR routing adapter with native multi-minute support.

        Key differences vs the generic DataFrame adapter:
        - Preserve the full timestep multiplier in the dataset key ("60m" -> "60minute") so it
          doesn't collide with "minute".
        - Pass the normalized key through to `ibkr_helper.get_price_data()` so IBKR can return
          native bars at that cadence (avoids per-iteration resampling).
        - Store `Data` under the normalized key and annotate `_native_timestep_*` so `Data.get_bars()`
          can fast-path slices without resampling.
        """
        if snapshot_only:
            return None

        end_dt = start_dt if isinstance(start_dt, datetime) else self._router.get_datetime()
        ts = timestep or self._router.get_timestep()

        start_datetime, ts_unit = self._router.get_start_datetime_and_ts_unit(
            length,
            ts,
            start_dt=end_dt,
            start_buffer=self._start_buffer(asset, provider_spec),
        )

        if ts_unit == "day":
            try:
                self._router._effective_day_mode = True
            except Exception:
                pass

        dataset_key = self._normalize_timestep_key(ts)
        qty, unit = parse_timestep_qty_and_unit(dataset_key)
        qty = int(qty)
        unit = str(unit)

        canonical_key, legacy_key = self._router._build_dataset_keys(asset, quote_asset, dataset_key)
        if canonical_key in self._fully_loaded_series and canonical_key in self._router._data_store:
            return None
        existing = self._router._data_store.get(canonical_key)
        existing_df = getattr(existing, "df", None) if existing is not None else None

        if existing_df is not None and isinstance(existing_df, pd.DataFrame) and not existing_df.empty:
            try:
                existing_start = existing_df.index.min()
                existing_end = existing_df.index.max()
                if existing_start is not None and existing_end is not None:
                    start_cmp = _align_timestamp_to_index_tz(start_datetime, existing_start)
                    end_cmp = _align_timestamp_to_index_tz(end_dt, existing_end)
                    if start_cmp >= existing_start and end_cmp <= existing_end:
                        return None
            except Exception:
                pass

        asset_type = _normalize_asset_type(getattr(asset, "asset_type", ""))
        include_after_hours = _ibkr_include_after_hours(asset_type, unit)
        df = None

        # PERF: warm-cache minute strategies can call `get_historical_prices()` tens of thousands of
        # times. In the router data source, IBKR history fetches must be amortized by prefetching
        # the full backtest window once, then slicing in-memory thereafter (same principle as the
        # IBKR-only backtesting data source).
        if asset_type in {"stock", "index"} and unit in {"minute", "hour"} and canonical_key not in self._fully_loaded_series:
            # Perf: routed minute/index strategies (for example SPX intraday options) can call
            # `get_historical_prices()` every loop iteration. Prefetch the full window once, then
            # serve slices from in-memory Data to avoid one downloader roundtrip per minute.
            try:
                prefetch_start = min(start_datetime, self._router.datetime_start)
            except Exception:
                prefetch_start = start_datetime
            prefetch_end = self._router.datetime_end or end_dt
            df = ibkr_helper.get_price_data(
                asset=asset,
                quote=quote_asset,
                timestep=dataset_key,
                start_dt=prefetch_start,
                end_dt=prefetch_end,
                exchange=None,
                include_after_hours=include_after_hours,
            )
            if df is None or df.empty:
                return None
            if ibkr_helper.frame_covers_requested_window(
                df,
                asset=asset,
                timestep=dataset_key,
                start_dt=prefetch_start,
                end_dt=prefetch_end,
            ):
                self._fully_loaded_series.add(canonical_key)
        elif asset_type in {"future", "cont_future"} and unit in {"minute", "hour", "day"} and canonical_key not in self._fully_loaded_series:
            try:
                from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting

                prev_open = InteractiveBrokersRESTBacktesting._previous_us_futures_session_open(self._router.datetime_start)
            except Exception:
                prev_open = None

            try:
                if prev_open is not None:
                    prefetch_start = min(start_datetime, prev_open)
                else:
                    prefetch_start = min(start_datetime, self._router.datetime_start - timedelta(days=1))
            except Exception:
                prefetch_start = start_datetime

            prefetch_end = self._router.datetime_end or end_dt
            df = ibkr_helper.get_price_data(
                asset=asset,
                quote=quote_asset,
                timestep=dataset_key,
                start_dt=prefetch_start,
                end_dt=prefetch_end,
                exchange=None,
                include_after_hours=include_after_hours,
            )
            if df is None or df.empty:
                return None
            if ibkr_helper.frame_covers_requested_window(
                df,
                asset=asset,
                timestep=dataset_key,
                start_dt=prefetch_start,
                end_dt=prefetch_end,
            ):
                self._fully_loaded_series.add(canonical_key)
        elif asset_type == "crypto" and unit in {"minute", "hour"} and canonical_key not in self._fully_loaded_series:
            try:
                prefetch_start = min(start_datetime, self._router.datetime_start)
            except Exception:
                prefetch_start = start_datetime
            prefetch_end = self._router.datetime_end or end_dt
            df = ibkr_helper.get_price_data(
                asset=asset,
                quote=quote_asset,
                timestep=dataset_key,
                start_dt=prefetch_start,
                end_dt=prefetch_end,
                exchange=None,
                include_after_hours=include_after_hours,
            )
            if df is None or df.empty:
                return None
            if ibkr_helper.frame_covers_requested_window(
                df,
                asset=asset,
                timestep=dataset_key,
                start_dt=prefetch_start,
                end_dt=prefetch_end,
            ):
                self._fully_loaded_series.add(canonical_key)
        elif asset_type in {"stock", "index"} and unit == "day" and canonical_key not in self._fully_loaded_series:
            # Daily lookback requests are in trading bars, so rely on the provider-agnostic
            # `start_datetime` computed by `get_start_datetime_and_ts_unit()`.
            # Capping prefetch to a short calendar window from backtest start can underfill warmup
            # bars and shift first signals by weeks/months.
            prefetch_start = start_datetime
            prefetch_end = self._router.datetime_end or end_dt
            df = ibkr_helper.get_price_data(
                asset=asset,
                quote=quote_asset,
                timestep=dataset_key,
                start_dt=prefetch_start,
                end_dt=prefetch_end,
                exchange=None,
                include_after_hours=include_after_hours,
            )
            if df is None or df.empty:
                return None
            if ibkr_helper.frame_covers_requested_window(
                df,
                asset=asset,
                timestep=dataset_key,
                start_dt=prefetch_start,
                end_dt=prefetch_end,
            ):
                self._fully_loaded_series.add(canonical_key)
        elif asset_type == "crypto" and unit == "day" and canonical_key not in self._fully_loaded_series:
            try:
                lookback_days = max(7, int(length) + 5)
            except Exception:
                lookback_days = 7
            prefetch_start = min(start_datetime, self._router.datetime_start - timedelta(days=lookback_days))
            prefetch_end = self._router.datetime_end or end_dt
            df = ibkr_helper.get_price_data(
                asset=asset,
                quote=quote_asset,
                timestep=dataset_key,
                start_dt=prefetch_start,
                end_dt=prefetch_end,
                exchange=None,
                include_after_hours=include_after_hours,
            )
            if df is None or df.empty:
                return None
            if ibkr_helper.frame_covers_requested_window(
                df,
                asset=asset,
                timestep=dataset_key,
                start_dt=prefetch_start,
                end_dt=prefetch_end,
            ):
                self._fully_loaded_series.add(canonical_key)
        else:
            df = ibkr_helper.get_price_data(
                asset=asset,
                quote=quote_asset,
                timestep=dataset_key,
                start_dt=start_datetime,
                end_dt=end_dt,
                exchange=None,
                include_after_hours=include_after_hours,
            )

        if df is None or df.empty:
            return None

        if isinstance(df.index, pd.DatetimeIndex):
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC")
            df.index = df.index.tz_convert(LUMIBOT_DEFAULT_PYTZ)
            df = df.sort_index()

        if existing_df is not None and isinstance(existing_df, pd.DataFrame) and not existing_df.empty:
            merged = pd.concat([existing_df, df], axis=0).sort_index()
            merged = merged[~merged.index.duplicated(keep="last")]
        else:
            merged = df

        # `Data` supports base units, but IBKR can return multi-minute datasets. Store them under
        # a separate key (e.g., "60minute") and annotate the instance so `Data.get_bars()` can
        # slice directly without resampling each iteration.
        data_timestep = unit if unit in {"minute", "hour", "day"} else "minute"
        data = Data(asset, merged, timestep=data_timestep, quote=quote_asset)
        data._native_timestep_quantity = int(qty)  # type: ignore[attr-defined]
        data._native_timestep_unit = unit  # type: ignore[attr-defined]
        try:
            if isinstance(merged.index, pd.DatetimeIndex) and len(merged.index) > 0:
                data.repair_times_and_fill(merged.index)
        except Exception:
            pass

        self._router._data_store[canonical_key] = data
        # Only expose the (asset, quote) legacy key for true minute data to avoid collisions with
        # multi-minute datasets (which would otherwise satisfy minute requests incorrectly).
        if dataset_key == "minute" and legacy_key not in self._router._data_store:
            self._router._data_store[legacy_key] = data
        return None

    def _fetch_df(
        self,
        *,
        asset: Asset,
        quote_asset: Asset,
        ts_unit: str,
        start_datetime: datetime,
        end_dt: datetime,
        length: int,
        canonical_key: Any,
        provider_spec: ProviderSpec,
        require_quote_data: bool,
        require_ohlc_data: bool,
    ) -> pd.DataFrame | None:
        asset_type = _normalize_asset_type(getattr(asset, "asset_type", ""))
        include_after_hours = _ibkr_include_after_hours(asset_type, ts_unit)

        # PERF: warm-cache minute strategies can call `get_historical_prices()` tens of thousands of
        # times. In the router data source, IBKR history fetches must be amortized by prefetching
        # the full backtest window once, then slicing in-memory thereafter (same principle as the
        # IBKR-only backtesting data source).

        if (
            asset_type in {"future", "cont_future"}
            and ts_unit in {"minute", "hour", "day"}
            and canonical_key not in self._fully_loaded_series
        ):
            try:
                from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting

                prev_open = InteractiveBrokersRESTBacktesting._previous_us_futures_session_open(self._router.datetime_start)
            except Exception:
                prev_open = None

            try:
                if prev_open is not None:
                    prefetch_start = min(start_datetime, prev_open)
                else:
                    prefetch_start = min(start_datetime, self._router.datetime_start - timedelta(days=1))
            except Exception:
                prefetch_start = start_datetime

            prefetch_end = self._router.datetime_end or end_dt

            df = ibkr_helper.get_price_data(
                asset=asset,
                quote=quote_asset,
                timestep=ts_unit,
                start_dt=prefetch_start,
                end_dt=prefetch_end,
                exchange=None,
                include_after_hours=include_after_hours,
            )
            if df is None or df.empty:
                return None
            self._fully_loaded_series.add(canonical_key)
            return df

        if asset_type == "crypto" and ts_unit in {"minute", "hour"} and canonical_key not in self._fully_loaded_series:
            try:
                prefetch_start = min(start_datetime, self._router.datetime_start)
            except Exception:
                prefetch_start = start_datetime
            prefetch_end = self._router.datetime_end or end_dt

            df = ibkr_helper.get_price_data(
                asset=asset,
                quote=quote_asset,
                timestep=ts_unit,
                start_dt=prefetch_start,
                end_dt=prefetch_end,
                exchange=None,
                include_after_hours=include_after_hours,
            )
            if df is None or df.empty:
                return None
            self._fully_loaded_series.add(canonical_key)
            return df

        if asset_type == "crypto" and ts_unit == "day" and canonical_key not in self._fully_loaded_series:
            try:
                lookback_days = max(7, int(length) + 5)
            except Exception:
                lookback_days = 7
            prefetch_start = min(start_datetime, self._router.datetime_start - timedelta(days=lookback_days))
            prefetch_end = self._router.datetime_end or end_dt

            df = ibkr_helper.get_price_data(
                asset=asset,
                quote=quote_asset,
                timestep=ts_unit,
                start_dt=prefetch_start,
                end_dt=prefetch_end,
                exchange=None,
                include_after_hours=include_after_hours,
            )
            if df is None or df.empty:
                return None
            self._fully_loaded_series.add(canonical_key)
            return df

        return ibkr_helper.get_price_data(
            asset=asset,
            quote=quote_asset,
            timestep=ts_unit,
            start_dt=start_datetime,
            end_dt=end_dt,
            exchange=None,
            include_after_hours=include_after_hours,
        )


class _PolygonRoutingAdapter(_DataFrameRoutingAdapter):
    provider_key = "polygon"

    def _fetch_df(
        self,
        *,
        asset: Asset,
        quote_asset: Asset,
        ts_unit: str,
        start_datetime: datetime,
        end_dt: datetime,
        length: int,
        canonical_key: Any,
        provider_spec: ProviderSpec,
        require_quote_data: bool,
        require_ohlc_data: bool,
    ) -> pd.DataFrame | None:
        polygon_key = (os.environ.get("POLYGON_API_KEY") or POLYGON_API_KEY or "").strip()
        if not polygon_key:
            raise RoutingProviderError("Routing selected Polygon but POLYGON_API_KEY is not configured.")

        ts_lower = str(ts_unit or "").lower()
        if ts_lower.endswith("day"):
            timespan = "day"
        elif ts_lower.endswith("hour"):
            timespan = "hour"
        else:
            timespan = "minute"

        asset_type = _normalize_asset_type(getattr(asset, "asset_type", ""))
        if asset_type == "crypto" and ts_unit == "day" and canonical_key not in self._fully_loaded_series:
            try:
                lookback_days = max(7, int(length) + 5)
            except Exception:
                lookback_days = 7
            prefetch_start = min(start_datetime, self._router.datetime_start - timedelta(days=lookback_days))
            prefetch_end = self._router.datetime_end
            df = polygon_helper.get_price_data_from_polygon(
                api_key=polygon_key,
                asset=asset,
                quote_asset=quote_asset,
                start=prefetch_start,
                end=prefetch_end,
                timespan=timespan,
                force_cache_update=False,
                max_workers=4,
            )
            self._fully_loaded_series.add(canonical_key)
        else:
            df = polygon_helper.get_price_data_from_polygon(
                api_key=polygon_key,
                asset=asset,
                quote_asset=quote_asset,
                start=start_datetime,
                end=end_dt,
                timespan=timespan,
                force_cache_update=False,
                max_workers=4,
            )

        if df is None or df.empty:
            return None

        if "close" in df.columns:
            if "bid" not in df.columns:
                df["bid"] = pd.to_numeric(df["close"], errors="coerce")
            if "ask" not in df.columns:
                df["ask"] = pd.to_numeric(df["close"], errors="coerce")

        return df


class _AlpacaRoutingAdapter(_DataFrameRoutingAdapter):
    provider_key = "alpaca"

    def __init__(self, router: "RoutedBacktestingPandas"):
        super().__init__(router)
        self._source = None

    def _fetch_df(
        self,
        *,
        asset: Asset,
        quote_asset: Asset,
        ts_unit: str,
        start_datetime: datetime,
        end_dt: datetime,
        length: int,
        canonical_key: Any,
        provider_spec: ProviderSpec,
        require_quote_data: bool,
        require_ohlc_data: bool,
    ) -> pd.DataFrame | None:
        if self._source is None:
            if not (
                ALPACA_CONFIG.get("OAUTH_TOKEN")
                or (ALPACA_CONFIG.get("API_KEY") and ALPACA_CONFIG.get("API_SECRET"))
            ):
                raise RoutingProviderError(
                    "Routing selected Alpaca but Alpaca credentials are not configured. "
                    "Set ALPACA_API_KEY/ALPACA_API_SECRET or ALPACA_OAUTH_TOKEN."
                )
            from lumibot.backtesting.alpaca_backtesting import AlpacaBacktesting

            self._source = AlpacaBacktesting(
                datetime_start=self._router.datetime_start,
                datetime_end=self._router.datetime_end,
                config=ALPACA_CONFIG,
                show_progress_bar=False,
            )

        df = self._source.get_historical_prices_between_dates(
            base_asset=asset,
            quote_asset=quote_asset,
            timestep=ts_unit,
            data_datetime_start=start_datetime,
            data_datetime_end=end_dt,
        )
        if df is None or df.empty:
            return None

        if isinstance(df.index, pd.DatetimeIndex):
            return df[(df.index >= start_datetime) & (df.index <= end_dt)]
        return df


class _CcxtRoutingAdapter(_DataFrameRoutingAdapter):
    provider_key = "ccxt"
    _default_start_buffer = timedelta(0)

    def __init__(self, router: "RoutedBacktestingPandas"):
        super().__init__(router)
        self._cache_by_exchange: dict[str, Any] = {}

    def _fetch_df(
        self,
        *,
        asset: Asset,
        quote_asset: Asset,
        ts_unit: str,
        start_datetime: datetime,
        end_dt: datetime,
        length: int,
        canonical_key: Any,
        provider_spec: ProviderSpec,
        require_quote_data: bool,
        require_ohlc_data: bool,
    ) -> pd.DataFrame | None:
        exchange_id = provider_spec.ccxt_exchange_id or _infer_default_ccxt_exchange_id()

        try:
            from lumibot.tools.ccxt_data_store import CcxtCacheDB
        except Exception as e:
            raise RoutingProviderError(
                f"Routing selected CCXT ({exchange_id}) but CCXT dependencies are not available: {e}"
            ) from e

        if exchange_id not in self._cache_by_exchange:
            self._cache_by_exchange[exchange_id] = CcxtCacheDB(exchange_id)
        cache_db = self._cache_by_exchange[exchange_id]

        if ts_unit == "minute":
            timeframe = "1m"
        elif ts_unit == "day":
            timeframe = "1d"
        else:
            raise RoutingProviderError(f"CCXT routing only supports minute/day timesteps, got {ts_unit!r}.")

        symbol = f"{asset.symbol.upper()}/{quote_asset.symbol.upper()}"
        df = cache_db.download_ohlcv(symbol, timeframe, start_datetime, end_dt)
        if df is None or df.empty:
            return None

        df.index = df.index.tz_localize("UTC").tz_convert(LUMIBOT_DEFAULT_PYTZ)
        return df.sort_index()


class _ProviderRegistry:
    def __init__(self, router: "RoutedBacktestingPandas"):
        self._router = router
        self._adapters: dict[str, _RoutingAdapter] = {
            "thetadata": _ThetaDataRoutingAdapter(router),
            "ibkr": _IbkrRoutingAdapter(router),
            "polygon": _PolygonRoutingAdapter(router),
            "alpaca": _AlpacaRoutingAdapter(router),
            "ccxt": _CcxtRoutingAdapter(router),
        }

    def resolve_provider_spec(self, provider: Any) -> ProviderSpec:
        raw = "" if provider is None else str(provider).strip()
        token = _normalize_token(raw)
        if not token:
            return ProviderSpec(provider="thetadata", raw=raw)

        aliases: dict[str, str] = {
            "theta": "thetadata",
            "thetadata": "thetadata",
            "interactivebrokers": "ibkr",
            "interactivebrokersrest": "ibkr",
            "interactivebrokersclientportal": "ibkr",
            "interactivebrokersclientportalrest": "ibkr",
            "interactivebrokersrestbacktesting": "ibkr",
            "ib": "ibkr",
            "ibkr": "ibkr",
            "poly": "polygon",
            "polygon": "polygon",
            "alpaca": "alpaca",
            "ccxt": "ccxt",
        }
        if token in aliases:
            if aliases[token] == "ccxt":
                return ProviderSpec(provider="ccxt", ccxt_exchange_id=_infer_default_ccxt_exchange_id(), raw=raw)
            return ProviderSpec(provider=aliases[token], raw=raw)

        exchange_id = _ccxt_exchange_id_from_token(raw)
        if exchange_id:
            return ProviderSpec(provider="ccxt", ccxt_exchange_id=exchange_id, raw=raw)

        raise RoutingProviderError(
            f"Unknown backtesting routing provider {provider!r}. "
            "Expected one of: thetadata, ibkr, polygon, alpaca, ccxt, or a CCXT exchange id (e.g., binance, kraken)."
        )

    def adapter_for_spec(self, spec: ProviderSpec) -> _RoutingAdapter:
        if spec.provider not in self._adapters:
            raise RoutingProviderError(f"No routing adapter registered for provider={spec.provider!r}.")
        return self._adapters[spec.provider]

    def validate_routing(self, routing: Dict[str, str]) -> None:
        for _, provider in routing.items():
            self.resolve_provider_spec(provider)


class RoutedBacktestingPandas(ThetaDataBacktestingPandas):
    """Backtesting data source that routes requests to multiple providers by asset type.

    Supported providers (routing values are case/whitespace/_/- insensitive):
    - ThetaData (default): stocks/options/indexes (and anything not explicitly routed)
    - IBKR Client Portal (REST) via the shared Data Downloader: futures + spot crypto
    - Polygon: optional crypto parity checks (API key required)
    - Alpaca: optional stocks/crypto (API key/token required)
    - CCXT: optional crypto via ccxt:
        - use "ccxt" to auto-select exchange from existing env/credentials
        - or specify an exchange id directly (e.g., "coinbase", "kraken", "binance", "kucoin")

    Routing is configured via `config["backtesting_data_routing"]` (a dict mapping asset_type -> provider).
    """

    _CONFIG_KEY = "backtesting_data_routing"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._registry = _ProviderRegistry(self)
        self._routing = self._normalize_routing(self._extract_routing_config(getattr(self, "_config", None)))
        self._registry.validate_routing(self._routing)

    @staticmethod
    def _extract_routing_config(config: Any) -> Optional[Dict[str, str]]:
        if config is None:
            return None
        if isinstance(config, dict):
            raw = config.get(RoutedBacktestingPandas._CONFIG_KEY)
            return raw if isinstance(raw, dict) else None
        raw = getattr(config, RoutedBacktestingPandas._CONFIG_KEY, None)
        return raw if isinstance(raw, dict) else None

    @staticmethod
    def _normalize_routing(routing: Optional[Dict[str, str]]) -> Dict[str, str]:
        if not routing:
            return {
                "default": "thetadata",
                "future": "ibkr",
                "cont_future": "ibkr",
                "crypto": "ibkr",
            }

        normalized: Dict[str, str] = {}
        for key, value in routing.items():
            if key is None:
                continue
            asset_type = str(key).strip().lower()
            normalized[asset_type] = "" if value is None else str(value).strip()

        # Convenience aliases for common user typos/plurals.
        # Keep the canonical keys in singular form: "future", "cont_future".
        if "futures" in normalized and "future" not in normalized:
            normalized["future"] = normalized["futures"]
        if "cont_futures" in normalized and "cont_future" not in normalized:
            normalized["cont_future"] = normalized["cont_futures"]

        # If a futures provider is configured but continuous futures is not, route cont_future to the
        # same provider. This matches user expectations that "futures" covers both FUTURE and CONT_FUTURE.
        future_provider = normalized.get("future")
        if future_provider and "cont_future" not in normalized:
            normalized["cont_future"] = future_provider

        normalized.setdefault("default", "thetadata")
        return normalized

    def _provider_spec_for_asset(self, asset: Asset) -> ProviderSpec:
        if getattr(self, "_registry", None) is None:
            # Defensive: some unit tests construct the router via __new__ without running __init__.
            self._registry = _ProviderRegistry(self)
        asset_type = _normalize_asset_type(getattr(asset, "asset_type", ""))
        raw = self._routing.get(asset_type) or self._routing.get("default") or "thetadata"
        return self._registry.resolve_provider_spec(raw)

    def _update_pandas_data(
        self,
        asset,
        quote,
        length,
        timestep,
        start_dt=None,
        require_quote_data: bool = False,
        require_ohlc_data: bool = True,
        snapshot_only: bool = False,
    ):
        asset_separated = asset
        quote_asset = quote if quote is not None else _DEFAULT_QUOTE_ASSET
        if isinstance(asset_separated, tuple):
            asset_separated, quote_asset = asset_separated

        provider_spec = self._provider_spec_for_asset(asset_separated)
        adapter = self._registry.adapter_for_spec(provider_spec)
        return adapter.update_pandas_data(
            asset=asset_separated,
            quote_asset=quote_asset,
            length=length,
            timestep=timestep,
            start_dt=start_dt,
            require_quote_data=require_quote_data,
            require_ohlc_data=require_ohlc_data,
            snapshot_only=snapshot_only,
            provider_spec=provider_spec,
        )

    def get_last_price(self, asset, timestep="minute", quote=None, exchange=None, **kwargs):
        """Align routed daily backtests away from minute bars for performance.

        ThetaDataBacktestingPandas already aligns get_last_price() to day bars when the data source
        is running in daily cadence. For non-Theta routed providers, infer "safe to align" using the
        same guardrail: only when we have not observed intraday cadence.
        """
        try:
            dt = self.get_datetime()
            self._update_cadence_from_dt(dt)
        except Exception:
            pass

        try:
            spec = self._provider_spec_for_asset(asset if not isinstance(asset, tuple) else asset[0])
        except Exception:
            spec = ProviderSpec(provider="thetadata")

        if spec.provider != "thetadata" and timestep == "minute":
            source_timestep = str(getattr(self, "_timestep", "") or "").strip().lower()
            if _is_day_like_timestep(source_timestep):
                timestep = "day"
            elif not bool(getattr(self, "_observed_intraday_cadence", False)) and bool(
                getattr(self, "_effective_day_mode", False)
            ):
                timestep = "day"

        return super().get_last_price(asset, timestep=timestep, quote=quote, exchange=exchange, **kwargs)

    def get_quote(self, asset, quote=None, exchange=None, timestep="minute", **kwargs):
        """Align routed quote lookups away from minute bars in daily non-Theta runs.

        In daily stock/index backtests routed to IBKR, market-order fills can call get_quote()
        and trigger expensive minute history downloads. When we have day-cadence evidence and
        no intraday cadence observed, align quote requests to day bars for non-Theta providers.
        """
        try:
            dt = self.get_datetime()
            self._update_cadence_from_dt(dt)
        except Exception:
            pass

        try:
            spec = self._provider_spec_for_asset(asset if not isinstance(asset, tuple) else asset[0])
        except Exception:
            spec = ProviderSpec(provider="thetadata")

        if spec.provider != "thetadata" and timestep == "minute":
            source_timestep = str(getattr(self, "_timestep", "") or "").strip().lower()
            if _is_day_like_timestep(source_timestep):
                timestep = "day"
            elif not bool(getattr(self, "_observed_intraday_cadence", False)) and bool(
                getattr(self, "_effective_day_mode", False)
            ):
                timestep = "day"

        return super().get_quote(asset, quote=quote, exchange=exchange, timestep=timestep, **kwargs)
