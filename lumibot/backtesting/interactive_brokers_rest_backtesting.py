from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta
from functools import lru_cache
from importlib import import_module
from typing import Optional

from lumibot.data_sources import PandasData
from lumibot.entities import Asset

logger = logging.getLogger(__name__)

_USD_FOREX = Asset("USD", "forex")


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


pd = _LazyModule("pandas")
ibkr_helper = _LazyModule("lumibot.tools.ibkr_helper")
_DATA_CLASS = None
_BARS_CLASS = None
_PARSE_TIMESTEP_QTY_AND_UNIT = None


def _data_class():
    global _DATA_CLASS
    if _DATA_CLASS is None:
        from lumibot.entities import Data

        _DATA_CLASS = Data
    return _DATA_CLASS


def _bars_class():
    global _BARS_CLASS
    if _BARS_CLASS is None:
        from lumibot.entities.bars import Bars

        _BARS_CLASS = Bars
    return _BARS_CLASS


def _parse_timestep_qty_and_unit(*args, **kwargs):
    global _PARSE_TIMESTEP_QTY_AND_UNIT
    if _PARSE_TIMESTEP_QTY_AND_UNIT is None:
        from lumibot.tools.helpers import parse_timestep_qty_and_unit

        _PARSE_TIMESTEP_QTY_AND_UNIT = parse_timestep_qty_and_unit
    return _PARSE_TIMESTEP_QTY_AND_UNIT(*args, **kwargs)


class InteractiveBrokersRESTBacktesting(PandasData):
    """Backtesting data source that fetches historical data from IBKR via the Data Downloader.

    IMPORTANT:
    - Uses the Client Portal Gateway (REST) style via the shared Data Downloader.
    - Implements local parquet caching under `LUMIBOT_CACHE_FOLDER/ibkr/...` with optional S3 mirroring.
    - Focuses on 1-minute+ bars (seconds are intentionally out of scope for now).
    """

    MIN_TIMESTEP = "minute"
    ALLOW_DAILY_TIMESTEP = True
    SOURCE = "InteractiveBrokersREST"
    PREFER_NATIVE_DAY_BARS_FOR_STOCK_INDEX = True

    def __init__(
        self,
        datetime_start: datetime,
        datetime_end: datetime,
        pandas_data=None,
        *,
        exchange: Optional[str] = None,
        history_source: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(datetime_start=datetime_start, datetime_end=datetime_end, pandas_data=pandas_data, **kwargs)
        self._timestep = self.MIN_TIMESTEP
        self.exchange = exchange
        self.history_source = history_source
        self.market = kwargs.get("market")

        unique_id = uuid.uuid4().hex[:8]
        strategy_name = kwargs.get("name", "Backtest")
        client_id = f"{strategy_name}_{unique_id}"
        from lumibot.tools.data_downloader_queue_client import set_queue_client_id

        set_queue_client_id(client_id)
        logger.info("[IBKR][QUEUE] Set client_id for queue fairness: %s", client_id)

        # Set data_source to self since this class acts as its own DataSource.
        self.data_source = self
        # Track which (asset, quote, timestep) series have been fully loaded for the backtest window.
        # Without this, PandasData's default behavior can end up seeding only a couple of bars
        # (e.g., `length=2` in strategy code) and then portfolio mark-to-market gets "stuck"
        # forward-filling the last available price for the rest of the run.
        self._fully_loaded_series: set[tuple] = set()

    @staticmethod
    def _normalize_exchange_key(exchange: Optional[str]) -> str:
        exch = (exchange or "").strip().upper()
        return exch or "AUTO"

    @staticmethod
    def _normalize_asset_type(value: object) -> str:
        raw = str(value or "").strip().lower()
        if "." in raw:
            raw = raw.split(".")[-1]
        return raw

    @staticmethod
    def _ibkr_include_after_hours(asset_type: str, timestep_unit: str) -> bool:
        """Return IBKR outsideRth policy for backtests.

        Stock/index day bars should be regular-session only so they line up with
        ThetaData/Yahoo daily semantics.
        """
        return not (asset_type in {"stock", "index"} and timestep_unit == "day")

    def _build_dataset_keys(
        self,
        asset: Asset,
        quote: Optional[Asset],
        dataset_key: str,
        exchange: Optional[str],
    ) -> tuple[tuple, tuple]:
        quote_asset = quote if quote is not None else _USD_FOREX
        exch = self._normalize_exchange_key(exchange)
        canonical_key = (asset, quote_asset, dataset_key, exch)
        legacy_key = (asset, quote_asset, exch)
        return canonical_key, legacy_key

    @staticmethod
    def _normalize_timestep_key(timestep: str) -> str:
        """Normalize a user-facing timestep into a stable series key.

        - Preserves multi-minute multipliers (e.g., "15min" -> "15minute") so cached datasets do
          not collide with "minute".
        - Keeps the `Data.timestep` base unit as "minute"/"day" for compatibility.
        """
        if timestep in {"minute", "day", "hour"}:
            return timestep
        qty, unit = _parse_timestep_qty_and_unit(timestep)
        qty = int(qty)
        unit = str(unit)
        if qty == 1:
            return unit
        return f"{qty}{unit}"

    def get_historical_prices(
        self,
        asset: Asset,
        length: int,
        timestep: str = None,
        timeshift: int = None,
        quote: Asset = None,
        exchange: str = None,
        include_after_hours: bool = True,
        return_polars: bool = False,
    ):
        """Get bars for a given asset, with a hot path for fully prefetched IBKR series."""
        if isinstance(asset, str):
            asset = Asset(symbol=asset)

        if not timestep:
            timestep = self.get_timestep()

        asset_separated = asset
        quote_asset = quote
        if isinstance(asset_separated, tuple):
            asset_separated, quote_asset = asset_separated

        quote_key = quote_asset if quote_asset is not None else _USD_FOREX
        effective_exchange = exchange if exchange is not None else self.exchange
        timestep_key = timestep if isinstance(timestep, str) else str(timestep)
        hot_cache = getattr(self, "_fully_loaded_hot_data_cache", None)
        if hot_cache is not None:
            data = hot_cache.get((id(asset_separated), id(quote_key), timestep_key, effective_exchange))
            if data is not None:
                try:
                    if timeshift is None and timestep in {"minute", "day"}:
                        response = data.get_native_bars_fast(self._datetime, length=length, timestep=timestep)
                    else:
                        response = data.get_bars(self._datetime, length=length, timestep=timestep, timeshift=timeshift)
                except ValueError:
                    return None
                if isinstance(response, float) or response is None:
                    return response
                if (
                    not return_polars
                    and isinstance(response, pd.DataFrame)
                    and response.attrs.get("_lumibot_skip_timezone", False)
                    and "return" in response.columns
                    and "dividend" not in response.columns
                ):
                    return _bars_class().from_pandas_fast(
                        response,
                        self.SOURCE,
                        asset_separated,
                        quote=quote_asset,
                        raw=response,
                    )
                return self._parse_source_symbol_bars(
                    response,
                    asset,
                    quote=quote,
                    length=length,
                    return_polars=return_polars,
                )

        return super().get_historical_prices(
            asset,
            length,
            timestep=timestep,
            timeshift=timeshift,
            quote=quote,
            exchange=exchange,
            include_after_hours=include_after_hours,
            return_polars=return_polars,
        )

    @staticmethod
    @lru_cache(maxsize=256)
    def _previous_us_futures_session_open(dt_value: datetime) -> Optional[datetime]:
        """Return the most recent `us_futures` session open at or before `dt_value`.

        Futures backtests frequently start at midnight timestamps (e.g. Monday 00:00 ET), but the
        `us_futures` session opens the prior day at ~18:00 ET and is closed for long stretches
        (weekends/holidays). Prefetching from a naive `dt_value - 1 day` can land in a closed
        interval and trigger unnecessary (and sometimes flaky) downloader fetch attempts.
        """
        try:
            import pandas_market_calendars as mcal
        except Exception:
            return None

        try:
            ref = pd.Timestamp(dt_value)
            if ref.tzinfo is None:
                ref = ref.tz_localize("UTC")
            ref = ref.tz_convert("UTC")

            cal = mcal.get_calendar("us_futures")
            schedule = cal.schedule(
                start_date=ref.date() - timedelta(days=10),
                end_date=ref.date() + timedelta(days=1),
            )
            if schedule is None or schedule.empty:
                return None

            opens = pd.to_datetime(schedule["market_open"], utc=True, errors="coerce").dropna()
            opens = opens.loc[opens <= ref]
            if opens.empty:
                return None
            return opens.max().to_pydatetime()
        except Exception:
            return None

    def get_last_price(self, asset, quote=None, exchange=None):
        """Return the best available last price for mark-to-market during IBKR backtests.

        For IBKR crypto daily backtests, prefetching the full-minute series for the entire backtest
        window is prohibitively slow (and unnecessary). Prefer the daily series for crypto when
        available, which is derived from intraday history and aligned to LumiBot's day cadence.

        For non-crypto, keep the existing minute-first behavior to preserve prior semantics.
        """
        base_asset = asset
        quote_asset = quote
        if isinstance(base_asset, tuple):
            base_asset, quote_asset = base_asset
        quote_asset = quote_asset if quote_asset is not None else Asset("USD", "forex")

        effective_exchange = exchange if exchange is not None else self.exchange

        raw_asset_type = getattr(base_asset, "asset_type", "")
        if raw_asset_type in {"crypto", "future", "cont_future", "stock", "index"}:
            asset_type = raw_asset_type
        else:
            asset_type = self._normalize_asset_type(raw_asset_type)
        now = self._datetime
        # Futures backtests should not look ahead into the current (incomplete) bar. Interpret
        # "last price at dt" as the last completed bar's close by nudging dt slightly earlier.
        #
        # NOTE: Continuous futures stitching is responsible for ensuring the bar immediately
        # preceding a roll boundary is present (so the last-completed-bar semantics remain valid
        # across contract transitions).
        if asset_type in {"future", "cont_future"}:
            try:
                now = now - timedelta(microseconds=1)
            except Exception:
                pass
        if asset_type == "crypto" and now.hour == 0 and now.minute == 0 and now.second == 0 and now.microsecond == 0:
            day_key = (base_asset, quote_asset, "day", self._normalize_exchange_key(effective_exchange))
            if day_key not in self._fully_loaded_series:
                try:
                    self._update_pandas_data(
                        base_asset,
                        quote_asset,
                        "day",
                        start_dt=self.datetime_start - timedelta(days=7),
                        end_dt=self.datetime_end,
                        exchange=effective_exchange,
                        include_after_hours=True,
                    )
                except Exception:
                    pass
                self._fully_loaded_series.add(day_key)
            day_data = self._data_store.get(day_key)
            if day_data is not None:
                try:
                    get_last_price_fast = getattr(day_data, "get_last_price_fast", None)
                    if callable(get_last_price_fast):
                        return get_last_price_fast(now)
                    return day_data.get_last_price(now)
                except Exception:
                    pass

        # If a native daily stock/index series is already loaded, prefer it over triggering a
        # separate minute fetch. This preserves daily-cadence semantics and avoids unnecessary
        # intraday index requests such as VIX/USD midpoint history during daily backtests.
        if asset_type in {"stock", "index"}:
            day_key = (base_asset, quote_asset, "day", self._normalize_exchange_key(effective_exchange))
            day_data = self._data_store.get(day_key)
            if day_data is not None:
                try:
                    get_last_price_fast = getattr(day_data, "get_last_price_fast", None)
                    if callable(get_last_price_fast):
                        return get_last_price_fast(now)
                    return day_data.get_last_price(now)
                except Exception:
                    pass

        minute_key = (base_asset, quote_asset, "minute", self._normalize_exchange_key(effective_exchange))
        if minute_key not in self._fully_loaded_series:
            try:
                self._update_pandas_data(
                    base_asset,
                    quote_asset,
                    "minute",
                    start_dt=self.datetime_start,
                    end_dt=self.datetime_end,
                    exchange=effective_exchange,
                    include_after_hours=True,
                )
            except Exception:
                pass
            self._fully_loaded_series.add(minute_key)
        data = self._data_store.get(minute_key)
        if data is not None:
            hot_cache = getattr(self, "_fully_loaded_hot_data_cache", None)
            if hot_cache is None:
                hot_cache = {}
                self._fully_loaded_hot_data_cache = hot_cache
            hot_cache[(id(base_asset), id(quote_asset), "minute", effective_exchange)] = data
            try:
                return data.get_last_price_fast(now)
            except Exception:
                pass
        return None

    def get_quote(self, asset, quote=None, exchange=None, **kwargs):
        """Return the best available quote snapshot for IBKR backtests.

        Performance trade-off:
        - Daily-cadence crypto strategies frequently execute at midnight timestamps.
        - Fetching 1-minute history just to support quote-based fills can be extremely slow.

        For crypto at midnight, prefer the derived daily series (fast, stable) as the quote
        source; otherwise fall back to PandasData's default minute-based quote path.
        """
        from lumibot.entities import Quote

        base_asset = asset
        quote_asset = quote
        if isinstance(base_asset, tuple):
            base_asset, quote_asset = base_asset
        quote_asset = quote_asset if quote_asset is not None else Asset("USD", "forex")

        effective_exchange = exchange if exchange is not None else self.exchange

        asset_type = self._normalize_asset_type(getattr(base_asset, "asset_type", ""))
        now = self.get_datetime()
        if asset_type == "crypto" and now.hour == 0 and now.minute == 0 and now.second == 0 and now.microsecond == 0:
            day_key = (base_asset, quote_asset, "day", self._normalize_exchange_key(effective_exchange))
            if day_key not in self._fully_loaded_series:
                try:
                    self._update_pandas_data(
                        base_asset,
                        quote_asset,
                        "day",
                        start_dt=self.datetime_start - timedelta(days=7),
                        end_dt=self.datetime_end,
                        exchange=effective_exchange,
                        include_after_hours=True,
                    )
                except Exception:
                    pass
                self._fully_loaded_series.add(day_key)

            day_data = self._data_store.get(day_key)
            if day_data is not None:
                try:
                    ohlcv_bid_ask_dict = day_data.get_quote(now)
                    return Quote(
                        asset=base_asset,
                        price=ohlcv_bid_ask_dict.get("close"),
                        bid=ohlcv_bid_ask_dict.get("bid"),
                        ask=ohlcv_bid_ask_dict.get("ask"),
                        volume=ohlcv_bid_ask_dict.get("volume"),
                        timestamp=now,
                        bid_size=ohlcv_bid_ask_dict.get("bid_size"),
                        ask_size=ohlcv_bid_ask_dict.get("ask_size"),
                        raw_data=ohlcv_bid_ask_dict,
                    )
                except Exception:
                    pass

        if asset_type in {"stock", "index"}:
            day_key = (base_asset, quote_asset, "day", self._normalize_exchange_key(effective_exchange))
            day_data = self._data_store.get(day_key)
            if day_data is not None:
                try:
                    ohlcv_bid_ask_dict = day_data.get_quote(now)
                    return Quote(
                        asset=base_asset,
                        price=ohlcv_bid_ask_dict.get("close"),
                        bid=ohlcv_bid_ask_dict.get("bid"),
                        ask=ohlcv_bid_ask_dict.get("ask"),
                        volume=ohlcv_bid_ask_dict.get("volume"),
                        timestamp=now,
                        bid_size=ohlcv_bid_ask_dict.get("bid_size"),
                        ask_size=ohlcv_bid_ask_dict.get("ask_size"),
                        raw_data=ohlcv_bid_ask_dict,
                    )
                except Exception:
                    pass

        minute_key = (base_asset, quote_asset, "minute", self._normalize_exchange_key(effective_exchange))
        if minute_key not in self._fully_loaded_series:
            try:
                self._update_pandas_data(
                    base_asset,
                    quote_asset,
                    "minute",
                    start_dt=self.datetime_start,
                    end_dt=self.datetime_end,
                    exchange=effective_exchange,
                    include_after_hours=True,
                )
            except Exception:
                pass
            self._fully_loaded_series.add(minute_key)

        minute_data = self._data_store.get(minute_key)
        if minute_data is None:
            return Quote(asset=base_asset)
        try:
            ohlcv_bid_ask_dict = minute_data.get_quote(now)
        except Exception:
            return Quote(asset=base_asset)
        return Quote(
            asset=base_asset,
            price=ohlcv_bid_ask_dict.get("close"),
            bid=ohlcv_bid_ask_dict.get("bid"),
            ask=ohlcv_bid_ask_dict.get("ask"),
            volume=ohlcv_bid_ask_dict.get("volume"),
            timestamp=now,
            bid_size=ohlcv_bid_ask_dict.get("bid_size"),
            ask_size=ohlcv_bid_ask_dict.get("ask_size"),
            raw_data=ohlcv_bid_ask_dict,
        )

    def _update_pandas_data(
        self,
        asset: Asset,
        quote: Optional[Asset],
        dataset_key: str | None = None,
        start_dt: datetime | None = None,
        end_dt: datetime | None = None,
        *,
        # Backwards-compatible alias for older call sites/tests that use `timestep=...`.
        timestep: str | None = None,
        exchange: Optional[str],
        include_after_hours: bool,
    ) -> None:
        if timestep is not None:
            dataset_key = timestep
        if dataset_key is None:
            raise TypeError("InteractiveBrokersRESTBacktesting._update_pandas_data missing required 'timestep'/'dataset_key'")
        if start_dt is None or end_dt is None:
            raise TypeError("InteractiveBrokersRESTBacktesting._update_pandas_data requires 'start_dt' and 'end_dt'")
        canonical_key, legacy_key = self._build_dataset_keys(asset, quote, dataset_key, exchange)
        existing = self._data_store.get(canonical_key)
        existing_df = getattr(existing, "df", None) if existing is not None else None

        df = ibkr_helper.get_price_data(
            asset=asset,
            quote=quote,
            timestep=dataset_key,
            start_dt=start_dt,
            end_dt=end_dt,
            exchange=exchange,
            include_after_hours=include_after_hours,
            source=self.history_source,
        )

        if df is None or df.empty:
            return

        if existing_df is not None and isinstance(existing_df, pd.DataFrame) and not existing_df.empty:
            merged = pd.concat([existing_df, df], axis=0).sort_index()
            merged = merged[~merged.index.duplicated(keep="last")]
        else:
            merged = df

        # `Data` supports only base units ("minute"/"day"). Store multi-minute datasets under a
        # separate key (e.g., "15minute") and annotate the instance so it can fast-path slices
        # without resampling on every call.
        qty, unit = _parse_timestep_qty_and_unit(dataset_key)
        unit = str(unit)
        data_timestep = unit if unit in {"minute", "day"} else "minute"
        assume_clean = bool(
            isinstance(merged, pd.DataFrame)
            and isinstance(merged.index, pd.DatetimeIndex)
            and merged.index.tz is not None
            and merged.index.is_monotonic_increasing
            and all(col in merged.columns for col in ("open", "high", "low", "close", "volume"))
        )
        data = _data_class()(asset, merged, timestep=data_timestep, quote=quote, assume_clean=assume_clean)
        if dataset_key in {"minute", "day"}:
            data._defer_clean_returns = True
        if dataset_key == "day":
            data._skip_clean_datalines = True
        data._native_timestep_quantity = int(qty)  # type: ignore[attr-defined]
        data._native_timestep_unit = unit  # type: ignore[attr-defined]
        # CRITICAL: Pandas backtesting expects each Data object to have `iter_index`/datalines
        # built so prices advance as the backtest clock advances. Normally this is done via
        # PandasData.load_data() -> Data.repair_times_and_fill(...), but IBKR loads data lazily.
        #
        # IMPORTANT (data gaps vs synthetic bars):
        # IBKR futures/crypto history can contain *real* timestamp gaps (maintenance windows,
        # holiday early closes, weekend gaps). We must not "repair" those gaps by expanding a
        # minute-by-minute index and forward-filling, because that would create synthetic bars
        # and enable fills at timestamps where the market was closed.
        #
        # See: docs/BACKTESTING_SESSION_GAPS_AND_DATA_GAPS.md
        try:
            if isinstance(merged.index, pd.DatetimeIndex) and len(merged.index) > 0:
                if not data._initialize_clean_repaired_state():
                    data.repair_times_and_fill(merged.index)
        except Exception:
            # Fallback: if repair fails, leave data as-is (callers will treat as missing).
            pass
        self._data_store[canonical_key] = data
        # Only write the legacy key for minute data to avoid collisions with daily bars.
        if dataset_key == "minute":
            self._data_store[legacy_key] = data

    def _pull_source_symbol_bars(
        self,
        asset,
        length,
        timestep=None,
        timeshift=None,
        quote=None,
        exchange=None,
        include_after_hours=True,
    ):
        asset_separated = asset
        quote_asset = quote
        if isinstance(asset_separated, tuple):
            asset_separated, quote_asset = asset_separated

        if isinstance(asset_separated, str):
            asset_separated = Asset(symbol=asset_separated)
        if timestep is None:
            timestep = self.get_timestep()

        end_dt = self._datetime

        # PERF: warm-cache minute backtests call `_pull_source_symbol_bars()` in tight loops. Once a
        # (asset, quote, timestep) series has been prefetched for the full backtest window, we can
        # skip start/end datetime calculations and downloader bookkeeping and slice immediately.
        quote_key = quote_asset if quote_asset is not None else _USD_FOREX
        effective_exchange = exchange if exchange is not None else self.exchange
        timestep_key = timestep if isinstance(timestep, str) else str(timestep)
        hot_cache_key = (id(asset_separated), id(quote_key), timestep_key, effective_exchange)
        hot_cache = getattr(self, "_fully_loaded_hot_data_cache", None)
        if hot_cache is not None:
            data = hot_cache.get(hot_cache_key)
            if data is not None:
                try:
                    return data.get_bars(end_dt, length=length, timestep=timestep, timeshift=timeshift)
                except ValueError:
                    return None
        dataset_key = self._normalize_timestep_key(timestep)
        exchange_key = self._normalize_exchange_key(effective_exchange)
        fully_loaded_key = (asset_separated, quote_key, dataset_key, exchange_key)
        if fully_loaded_key in self._fully_loaded_series:
            data_cache = getattr(self, "_fully_loaded_data_cache", None)
            if data_cache is None:
                data_cache = {}
                self._fully_loaded_data_cache = data_cache

            data = data_cache.get(fully_loaded_key)
            if data is None:
                canonical_key, legacy_key = self._build_dataset_keys(
                    asset_separated,
                    quote_asset,
                    dataset_key,
                    effective_exchange,
                )
                data = self._data_store.get(canonical_key)
                if data is None and dataset_key == "minute":
                    data = self._data_store.get(legacy_key)
                if data is not None:
                    data_cache[fully_loaded_key] = data
                    hot_cache = getattr(self, "_fully_loaded_hot_data_cache", None)
                    if hot_cache is None:
                        hot_cache = {}
                        self._fully_loaded_hot_data_cache = hot_cache
                    hot_cache[hot_cache_key] = data
            if data is None:
                return None
            try:
                return data.get_bars(end_dt, length=length, timestep=timestep, timeshift=timeshift)
            except ValueError:
                return None

        # IBKR crypto/futures trade outside equity calendars; do not add the default 5-day padding.
        start_dt, ts_unit = self.get_start_datetime_and_ts_unit(
            length, timestep, start_dt=end_dt, start_buffer=timedelta(0)
        )
        ts_unit = str(ts_unit or "").strip().lower()
        asset_type = self._normalize_asset_type(getattr(asset_separated, "asset_type", ""))
        include_after_hours = self._ibkr_include_after_hours(asset_type, ts_unit)
        if asset_type in {"future", "cont_future"} and ts_unit in {"minute", "hour", "day"}:
            # Futures strategies frequently request very small slices (e.g., `length=2`) at the
            # beginning of the backtest window. If we only fetch the tiny requested slice, IBKR's
            # history endpoint can return slightly-stale bars and leave the Data object underfilled,
            # causing strategies to see "no bars available" and skip trading entirely.
            #
            # Fix: on first access, prefetch the full backtest window for the series and reuse it.
            quote_key = quote_asset if quote_asset is not None else _USD_FOREX
            key = (asset_separated, quote_key, dataset_key, exchange_key)
            if key not in self._fully_loaded_series:
                prev_open = None if self.market == "24/7" else self._previous_us_futures_session_open(self.datetime_start)
                if prev_open is not None:
                    prefetch_start = min(start_dt, prev_open)
                else:
                    prefetch_start = min(start_dt, self.datetime_start - timedelta(days=1))
                prefetch_end = self.datetime_end
                self._update_pandas_data(
                    asset_separated,
                    quote_asset,
                    dataset_key,
                    start_dt=prefetch_start,
                    end_dt=prefetch_end,
                    exchange=effective_exchange,
                    include_after_hours=True,
                )
                data_prefetched = self._data_store.get(key)
                df_prefetched = getattr(data_prefetched, "df", None) if data_prefetched is not None else None
                if ibkr_helper.frame_covers_requested_window(
                    df_prefetched,
                    asset=asset_separated,
                    timestep=dataset_key,
                    start_dt=prefetch_start,
                    end_dt=prefetch_end,
                ):
                    self._fully_loaded_series.add(key)
        elif asset_type in {"stock", "index"} and ts_unit == "day":
            # Equity/index daily strategies repeatedly request overlapping windows.
            # Prefetch the full backtest range once and reuse in-memory slices.
            key = (asset_separated, quote_asset if quote_asset is not None else _USD_FOREX, dataset_key, exchange_key)
            if key not in self._fully_loaded_series:
                try:
                    lookback_days = max(7, int(length) + 5)
                except Exception:
                    lookback_days = 7
                req_start = pd.Timestamp(start_dt)
                base_start = pd.Timestamp(self.datetime_start - timedelta(days=lookback_days))
                if req_start.tzinfo is None and base_start.tzinfo is not None:
                    req_start = req_start.tz_localize(base_start.tzinfo)
                elif req_start.tzinfo is not None and base_start.tzinfo is None:
                    base_start = base_start.tz_localize(req_start.tzinfo)
                prefetch_start = min(req_start, base_start).to_pydatetime()
                prefetch_end = self.datetime_end
                self._update_pandas_data(
                    asset_separated,
                    quote_asset,
                    dataset_key,
                    start_dt=prefetch_start,
                    end_dt=prefetch_end,
                    exchange=effective_exchange,
                    include_after_hours=include_after_hours,
                )
                data_prefetched = self._data_store.get(key)
                df_prefetched = getattr(data_prefetched, "df", None) if data_prefetched is not None else None
                if ibkr_helper.frame_covers_requested_window(
                    df_prefetched,
                    asset=asset_separated,
                    timestep=dataset_key,
                    start_dt=prefetch_start,
                    end_dt=prefetch_end,
                ):
                    self._fully_loaded_series.add(key)
        elif asset_type == "crypto" and ts_unit == "day":
            # Prefetch daily series for the full backtest window on first access so we do not
            # hammer the downloader once per simulated day.
            key = (asset_separated, quote_asset if quote_asset is not None else _USD_FOREX, dataset_key, exchange_key)
            if key not in self._fully_loaded_series:
                prefetch_start = min(start_dt, self.datetime_start - timedelta(days=max(7, int(length) + 5)))
                prefetch_end = self.datetime_end
                self._update_pandas_data(
                    asset_separated,
                    quote_asset,
                    dataset_key,
                    start_dt=prefetch_start,
                    end_dt=prefetch_end,
                    exchange=effective_exchange,
                    include_after_hours=True,
                )
                data_prefetched = self._data_store.get(key)
                df_prefetched = getattr(data_prefetched, "df", None) if data_prefetched is not None else None
                if ibkr_helper.frame_covers_requested_window(
                    df_prefetched,
                    asset=asset_separated,
                    timestep=dataset_key,
                    start_dt=prefetch_start,
                    end_dt=prefetch_end,
                ):
                    self._fully_loaded_series.add(key)
        elif asset_type == "crypto" and ts_unit == "minute":
            # Crypto is 24/7 but IBKR history calls are still expensive. Intraday strategies can call
            # `get_historical_prices()` tens of thousands of times; prefetch the full window once
            # and then slice in-memory for warm-cache speed.
            key = (asset_separated, quote_asset if quote_asset is not None else _USD_FOREX, dataset_key, exchange_key)
            if key not in self._fully_loaded_series:
                self._update_pandas_data(
                    asset_separated,
                    quote_asset,
                    dataset_key,
                    start_dt=self.datetime_start,
                    end_dt=self.datetime_end,
                    exchange=effective_exchange,
                    include_after_hours=True,
                )
                data_prefetched = self._data_store.get(key)
                df_prefetched = getattr(data_prefetched, "df", None) if data_prefetched is not None else None
                if ibkr_helper.frame_covers_requested_window(
                    df_prefetched,
                    asset=asset_separated,
                    timestep=dataset_key,
                    start_dt=self.datetime_start,
                    end_dt=self.datetime_end,
                ):
                    self._fully_loaded_series.add(key)
        else:
                self._update_pandas_data(
                    asset_separated,
                    quote_asset,
                    dataset_key,
                    start_dt=start_dt,
                    end_dt=end_dt,
                    exchange=effective_exchange,
                    include_after_hours=include_after_hours,
                )
        # PERF: avoid `PandasData.find_asset_in_data_store()` candidate generation on every call.
        # IBKR uses stable canonical keys; slice directly from the cached `Data` object.
        canonical_key, legacy_key = self._build_dataset_keys(asset_separated, quote_asset, dataset_key, effective_exchange)
        data = self._data_store.get(canonical_key)
        if data is None and dataset_key == "minute":
            data = self._data_store.get(legacy_key)
        if data is None:
            return None
        if fully_loaded_key in self._fully_loaded_series:
            hot_cache = getattr(self, "_fully_loaded_hot_data_cache", None)
            if hot_cache is None:
                hot_cache = {}
                self._fully_loaded_hot_data_cache = hot_cache
            hot_cache[hot_cache_key] = data

        now = self.get_datetime()
        try:
            return data.get_bars(now, length=length, timestep=timestep, timeshift=timeshift)
        except ValueError:
            return None

    def get_historical_prices_between_dates(
        self,
        asset,
        timestep="minute",
        quote=None,
        exchange=None,
        include_after_hours=True,
        start_date=None,
        end_date=None,
    ):
        asset_separated = asset
        quote_asset = quote
        if isinstance(asset_separated, tuple):
            asset_separated, quote_asset = asset_separated

        if isinstance(asset_separated, str):
            asset_separated = Asset(symbol=asset_separated)
        if start_date is None or end_date is None:
            return None

        dataset_key = self._normalize_timestep_key(timestep)
        qty, unit = _parse_timestep_qty_and_unit(dataset_key)
        unit = str(unit or "").strip().lower()
        asset_type = self._normalize_asset_type(getattr(asset_separated, "asset_type", ""))
        include_after_hours = self._ibkr_include_after_hours(asset_type, unit)
        effective_exchange = exchange if exchange is not None else self.exchange
        self._update_pandas_data(
            asset_separated,
            quote_asset,
            dataset_key,
            start_dt=start_date,
            end_dt=end_date,
            exchange=effective_exchange,
            include_after_hours=include_after_hours,
        )

        canonical_key, legacy_key = self._build_dataset_keys(asset_separated, quote_asset, dataset_key, effective_exchange)
        data = self._data_store.get(canonical_key)
        if data is None and dataset_key == "minute":
            data = self._data_store.get(legacy_key)
        if data is None:
            return None

        try:
            response = data.get_bars_between_dates(start_date=start_date, end_date=end_date, timestep=timestep)
        except ValueError:
            response = None
        if response is None:
            return None
        return self._parse_source_symbol_bars(response, asset_separated, quote=quote_asset)
