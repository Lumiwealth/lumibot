from __future__ import annotations

import os
import math
import time
from abc import ABC, abstractmethod

from lumibot._lazy_imports import LazyLogger, LazyModule, LazyPytzTimezoneRef, lazy_class

from .exceptions import UnavailabeTimestep

logger = LazyLogger(__name__)
TYPE_CHECKING = False

date = lazy_class("datetime", "date")
datetime = lazy_class("datetime", "datetime")
timedelta = lazy_class("datetime", "timedelta")
Asset = lazy_class("lumibot.entities", "Asset")
AssetsMapping = lazy_class("lumibot.entities", "AssetsMapping")
pd = LazyModule("pandas")
LUMIBOT_DEFAULT_TIMEZONE = "America/New_York"

if TYPE_CHECKING:
    from lumibot.entities import Bars, Quote


def _create_options_symbol(*args, **kwargs):
    from lumibot.tools import create_options_symbol

    return create_options_symbol(*args, **kwargs)


def _black_scholes():
    from lumibot.tools import black_scholes

    return black_scholes


class MultiAssetBarsResult(dict):
    """Asset-to-bars mapping with sanitized per-asset error metadata."""

    def __init__(self, *args, errors=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.errors = errors or {}


def _normalized_data_error(error):
    """Return a provider-neutral error for the generic multi-asset boundary.

    Provider adapters may translate their SDK failures before they reach this
    boundary. The generic data source deliberately does not inspect provider
    response objects, status codes, exception names, or response bodies.
    """
    if isinstance(error, NotImplementedError):
        category = "unsupported"
    else:
        category = "unavailable"
    return {
        "category": category,
        "errorType": "unsupported_operation" if category == "unsupported" else "data_unavailable",
        "retryable": category == "unavailable",
    }


class DataSource(ABC):
    SOURCE = ""
    IS_BACKTESTING_DATA_SOURCE = False
    APPLY_BACKTEST_POSITION_SPLITS = True
    AUTO_ADJUST_IMPLIES_SPLIT_ADJUSTED_PRICES = False
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = []
    DEFAULT_TIMEZONE = LUMIBOT_DEFAULT_TIMEZONE
    DEFAULT_PYTZ = LazyPytzTimezoneRef(LUMIBOT_DEFAULT_TIMEZONE)
    option_quote_fallback_allowed = False

    def __init__(
            self,
            api_key: str | None = None,
            delay: int | None = None,
            tzinfo=None,
            **kwargs
    ):
        """

        Parameters
        ----------
        api_key : str
            The API key to use for the data source
        delay : int
            The number of minutes to delay the data by. This is useful for paper trading data sources that
            provide delayed data (i.e. 15m delayed data).
        """
        self.name = "data_source"
        self._timestep = None
        self._api_key = api_key

        # Use DATA_SOURCE_DELAY environment variable if it exists and delay is not explicitly provided
        if delay is None:
            env_delay = os.environ.get("DATA_SOURCE_DELAY")
            if env_delay is not None:
                try:
                    delay = int(env_delay)
                except ValueError:
                    # If the environment variable is not a valid integer, ignore it
                    pass
            else:
                # Default to 0 if no environment variable is set
                delay = 0

        self._delay_minutes = delay
        self._delay_value = None
        self._tzinfo = tzinfo

        # Initialize caches centrally (avoid ad-hoc hasattr checks in methods)
        self._greeks_cache = {}

        # Thread pool for parallel operations - reuse to avoid creation/destruction overhead
        self._thread_pool = None
        self._thread_pool_max_workers = kwargs.get('max_workers', 10)

        # Dividend cache for backtest performance
        self._dividend_cache = {}  # {asset: {date: dividend_value}}
        self._dividend_cache_enabled = kwargs.get('cache_dividends', True)
        self._stock_split_cache = {}  # {asset: {date: split_ratio}}

        # Ensure the instance has an explicit attribute for fallback behaviour
        if not hasattr(self, "option_quote_fallback_allowed"):
            self.option_quote_fallback_allowed = False

    @property
    def _delay(self):
        if self._delay_minutes is None:
            return self._delay_value
        if self._delay_value is None:
            self._delay_value = timedelta(minutes=self._delay_minutes)
        return self._delay_value

    @_delay.setter
    def _delay(self, value):
        self._delay_minutes = None
        self._delay_value = value

    @property
    def tzinfo(self):
        if self._tzinfo is None:
            default_tz = type(self).DEFAULT_PYTZ
            self._tzinfo = default_tz._load() if hasattr(default_tz, "_load") else default_tz
        return self._tzinfo

    @tzinfo.setter
    def tzinfo(self, value):
        self._tzinfo = value

    def _get_or_create_thread_pool(self):
        """Get or create the thread pool for parallel operations"""
        if self._thread_pool is None:
            from concurrent.futures import ThreadPoolExecutor
            self._thread_pool = ThreadPoolExecutor(max_workers=self._thread_pool_max_workers)
        return self._thread_pool

    def shutdown(self):
        """Cleanup thread pool resources"""
        if self._thread_pool is not None:
            self._thread_pool.shutdown(wait=True)
            self._thread_pool = None

    # ========Required Implementations ======================
    @abstractmethod
    def get_chains(self, asset: Asset, quote: Asset = None) -> dict:
        """
        Obtains option chain information for the asset (stock) from each
        of the exchanges the options trade on and returns a dictionary
        for each exchange.

        Parameters
        ----------
        asset : Asset
            The asset to get the option chains for
        quote : Asset | None
            The quote asset to get the option chains for

        Returns
        -------
        dict
            Mapping with keys such as ``Multiplier`` (e.g. ``"100"``) and ``Chains``.
            ``Chains`` is a nested dictionary where expiration dates map to strike lists,
            e.g. ``chains['Chains']['CALL']['2023-07-31'] = [strike1, strike2, ...]``.
        """
        pass

    @abstractmethod
    def get_historical_prices(
        self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True, **kwargs
    ) -> Bars:
        """
        Get bars for a given asset, going back in time from now, getting length number of bars by timestep.
        For example, with a length of 10 and a timestep of "day", and no timeshift, this
        would return the last 10 daily bars.

        - Higher-level method that returns a `Bars` object
        - Handles timezone conversions automatically
        - Includes additional metadata and processing
        - Preferred for strategy development and backtesting
        - Returns normalized data with consistent format across data sources

        Parameters
        ----------
        asset : Asset
            The asset to get the bars for.
        length : int
            The number of bars to get.
        timestep : str
            The timestep to get the bars at. Accepts "day" "hour" or "minute".
        timeshift : datetime.timedelta
            The amount of time to shift the bars by. For example, if you want the bars from 1 hour ago to now,
            you would set timeshift to 1 hour.
        quote : Asset
            The quote asset to get the bars for.
        exchange : str
            The exchange to get the bars for.
        include_after_hours : bool
            Whether to include after hours data.
        return_polars : bool (deprecated)
            Deprecated. Do not use in strategy code. This keyword will be removed in a future release.
            Strategy logic should use pandas operations on ``bars.pandas_df`` and should not depend on
            the underlying DataFrame backend.

        Returns
        -------
        Bars
            The bars for the asset. For strategy code, prefer ``bars.pandas_df`` for a pandas DataFrame.
        """
        pass

    @abstractmethod
    def get_last_price(self, asset, quote=None, exchange=None) -> Union[float, Decimal, None]:
        """
        Takes an asset and returns the last known price

        Parameters
        ----------
        asset : Asset
            The asset to get the price of.
        quote : Asset
            The quote asset to get the price of.
        exchange : str
            The exchange to get the price of.

        Returns
        -------
        float or Decimal or None
            The last known price of the asset.
        """
        pass

    # ========Python datetime helpers======================

    def get_datetime(self, adjust_for_delay=False):
        """
        Returns the current datetime in the default timezone

        Parameters
        ----------
        adjust_for_delay : bool
            Whether to adjust the current time for the delay. This is useful for paper trading data sources that
            provide delayed data.

        Returns
        -------
        datetime
        """
        current_time = self.to_default_timezone(datetime.now())
        if adjust_for_delay and self._delay:
            current_time -= self._delay
        return current_time

    def get_timestamp(self):
        """
        Returns the current timestamp in the default timezone
        Returns
        -------
        float
        """
        return self.get_datetime().timestamp()

    def get_round_minute(self, timeshift=0):
        """
        Returns the current datetime rounded to the minute and applies a timeshift in minutes
        Parameters
        ----------
        timeshift: int
            The number of minutes to shift the datetime by

        Returns
        -------
        datetime
            Rounded datetime with the timeshift applied
        """
        current = self.get_datetime().replace(second=0, microsecond=0)
        return current - timedelta(minutes=timeshift)

    def get_last_minute(self):
        return self.get_round_minute(timeshift=1)

    def get_round_day(self, timeshift=0):
        """
        Returns the current datetime rounded to the day and applies a timeshift in days
        Parameters
        ----------
        timeshift: int
            The number of days to shift the datetime by

        Returns
        -------
        datetime
            Rounded datetime with the timeshift applied
        """
        current = self.get_datetime().replace(hour=0, minute=0, second=0, microsecond=0)
        return current - timedelta(days=timeshift)

    def get_last_day(self):
        return self.get_round_day(timeshift=1)

    def get_datetime_range(self, length, timestep="minute", timeshift=None):
        if timestep == "minute":
            period_length = length * timedelta(minutes=1)
            end_date = self.get_last_minute()
        else:
            period_length = length * timedelta(days=1)
            end_date = self.get_last_day()

        if timeshift:
            end_date -= timeshift

        start_date = end_date - period_length
        return start_date, end_date

    def localize_datetime(self, dt):
        if dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None:
            return self.to_default_timezone(dt)
        else:
            return self.tzinfo.localize(dt, is_dst=None)

    def to_default_timezone(self, dt):
        return dt.astimezone(self.tzinfo)

    def get_timestep(self):
        return self._timestep if self._timestep else self.MIN_TIMESTEP

    @staticmethod
    def convert_timestep_str_to_timedelta(timestep):
        """
        Convert a timestep string to a timedelta object. For example, "1minute" will be converted to a
        timedelta of 1 minute.

        Parameters
        ----------
        timestep : str
            The timestep string to convert. For example, "1minute" or "1hour" or "1day".

        Returns
        -------
        timedelta
            A timedelta object representing the timestep.
        unit : str
            The unit of the timestep. For example, "minute" or "hour" or "day".
        """
        timestep = timestep.lower()

        # Define mapping from timestep units to equivalent minutes
        time_unit_map = {
            "minute": 1,
            "min": 1,  # Common shorthand (e.g., "15min")
            "hour": 60,
            "day": 24 * 60,
            "m": 1,  # "M" is for minutes
            "h": 60,  # "H" is for hours
            "d": 24 * 60,  # "D" is for days
        }

        # Define default values
        quantity = 1
        unit = ""

        # Check if timestep string has a number at the beginning
        if timestep[0].isdigit():
            for i, char in enumerate(timestep):
                if not char.isdigit():
                    # Get the quantity (number of units)
                    quantity = int(timestep[:i])
                    # Get the unit (minute, hour, or day)
                    # IBRK uses "minutes" instead of "minute" when 'quantity' > 1, for some reason, so handle
                    # that behavior here so backtest is comptiable with IBRK
                    unit = timestep[i:].strip().rstrip("s")  # Remove extra whitespace and IBKR's extra pluralization
                    break
        else:
            unit = timestep

        # Check if the unit is valid
        if unit in time_unit_map:
            # Convert quantity to minutes
            quantity_in_minutes = quantity * time_unit_map[unit]
            # Convert minutes to timedelta
            delta = timedelta(minutes=quantity_in_minutes)
            canonical_unit = {
                "m": "minute",
                "min": "minute",
                "minute": "minute",
                "h": "hour",
                "hour": "hour",
                "d": "day",
                "day": "day",
            }.get(unit, unit)
            return delta, canonical_unit
        else:
            raise ValueError(f"Unknown unit: {unit}. Valid units are minute, hour, day, M, H, D")

    # ========Internal Market Data Methods===================

    def _parse_source_timestep(self, timestep, reverse=False):
        """transform the data source timestep variable
        into lumibot representation. set reverse to True
        for opposite direction"""
        for item in self.TIMESTEP_MAPPING:
            if reverse:
                if timestep == item["timestep"]:
                    return item["representations"][0]
            else:
                if timestep in item["representations"]:
                    return item["timestep"]

        raise UnavailabeTimestep(self.SOURCE, timestep)

    def _parse_source_bars(self, response, quote=None):
        result = {}
        for asset, data in response.items():
            if data is None or isinstance(data, float):
                result[asset] = data
                continue
            result[asset] = self._parse_source_symbol_bars(data, asset, quote=quote)
        return result

    # =================Public Market Data Methods==================

    def get_bars(
        self,
        assets,
        length,
        timestep="minute",
        timeshift=None,
        chunk_size=2,
        max_workers=2,
        quote=None,
        exchange=None,
        include_after_hours=True,
        sleep_time: float | None = None,
    ):
        """Get bars for the list of assets"""
        if not isinstance(assets, list):
            assets = [assets]

        effective_sleep_time = sleep_time
        if effective_sleep_time is None:
            effective_sleep_time = 0.0 if getattr(self, "IS_BACKTESTING_DATA_SOURCE", False) else 0.1

        def process_chunk(chunk):
            chunk_result = {}
            chunk_errors = {}
            for asset in chunk:
                if isinstance(asset, tuple):
                    base_asset = asset[0]
                    quote_asset = asset[1]
                else:
                    base_asset = asset
                    quote_asset = quote
                try:
                    chunk_result[asset] = self.get_historical_prices(
                        asset=base_asset,
                        length=length,
                        timestep=timestep,
                        timeshift=timeshift,
                        quote=quote_asset,
                        exchange=exchange,
                        include_after_hours=include_after_hours,
                    )

                    # Sleep to prevent rate limiting
                    if effective_sleep_time:
                        time.sleep(effective_sleep_time)
                except Exception as e:
                    normalized_error = _normalized_data_error(e)
                    asset_label = (
                        getattr(base_asset, "symbol", None)
                        or type(base_asset).__name__
                    )
                    logger.warning(
                        "Error retrieving data for %s: category=%s error_type=%s retryable=%s",
                        asset_label,
                        normalized_error["category"],
                        normalized_error["errorType"],
                        normalized_error["retryable"],
                    )
                    chunk_result[asset] = None
                    chunk_errors[asset] = normalized_error
            return chunk_result, chunk_errors

        # Convert strings to Asset objects
        assets = [Asset(symbol=a) if isinstance(a, str) else a for a in assets]

        # Chunk the assets
        chunks = [assets[i : i + chunk_size] for i in range(0, len(assets), chunk_size)]

        results = {}
        errors = {}
        # Reuse thread pool to avoid creation/destruction overhead
        from concurrent.futures import as_completed

        executor = self._get_or_create_thread_pool()
        futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
        for future in as_completed(futures):
            chunk_result, chunk_errors = future.result()
            results.update(chunk_result)
            errors.update(chunk_errors)

        return MultiAssetBarsResult(results, errors=errors)

    def get_last_prices(self, assets, quote=None, exchange=None):
        """Takes a list of assets and returns the last known prices"""

        result = {}
        for asset in assets:
            result[asset] = self.get_last_price(asset, quote=quote, exchange=exchange)

        if self.SOURCE == "CCXT":
            return result
        else:
            return AssetsMapping(result)

    # ========Prediction-market metadata defaults======================
    #
    # These methods are intentionally safe no-ops on the base class. Prediction
    # market data sources such as Polymarket can override them with provider
    # implementations, while existing stock/options/crypto brokers continue to
    # behave normally if strategy code probes for prediction-market metadata.

    def search_markets(self, query: str | None = None, limit: int = 20, **kwargs) -> list:
        """Search provider markets/events.

        The default implementation returns an empty list because most data
        sources do not expose prediction-market discovery.
        """
        return []

    def get_event(self, event_id: str | int | None = None, slug: str | None = None, **kwargs) -> dict:
        """Return event metadata if the provider supports event-level data."""
        return {}

    def get_market_metadata(self, market=None, **kwargs) -> dict:
        """Return normalized market metadata if the provider supports it."""
        return {}

    def get_market_rules(self, market=None, **kwargs) -> dict:
        """Return trading/resolution rules if the provider supports them."""
        return {}

    def get_resolution_status(self, market=None, **kwargs) -> dict:
        """Return resolution state for a prediction contract.

        Providers that do not support this surface return a stable unsupported
        shape instead of raising.
        """
        return {"status": "unsupported", "resolved": None, "winner": None, "raw": None}

    def get_spread(self, asset, quote=None, exchange=None) -> Union[float, Decimal, None]:
        """Return bid/ask spread when available.

        Generic fallback uses ``get_quote`` if the concrete data source has one.
        """
        get_quote = getattr(self, "get_quote", None)
        if get_quote is None:
            return None
        try:
            quote_obj = get_quote(asset, quote=quote, exchange=exchange)
        except Exception:
            return None
        bid = getattr(quote_obj, "bid", None)
        ask = getattr(quote_obj, "ask", None)
        if bid is None or ask is None:
            return None
        return ask - bid

    def get_midpoint(self, asset, quote=None, exchange=None) -> Union[float, Decimal, None]:
        """Return midpoint/mark when available.

        Generic fallback uses ``Quote.mid_price`` or bid/ask from ``get_quote``.
        """
        get_quote = getattr(self, "get_quote", None)
        if get_quote is None:
            return None
        try:
            quote_obj = get_quote(asset, quote=quote, exchange=exchange)
        except Exception:
            return None
        mid_price = getattr(quote_obj, "mid_price", None)
        if mid_price is not None:
            return mid_price
        bid = getattr(quote_obj, "bid", None)
        ask = getattr(quote_obj, "ask", None)
        if bid is not None and ask is not None:
            return (bid + ask) / 2
        return getattr(quote_obj, "price", None)

    def get_recent_trades(self, market=None, limit: int = 100, **kwargs) -> list:
        """Return recent trades/fills if the provider supports them."""
        return []

    def get_open_interest(self, market=None, **kwargs) -> Union[float, Decimal, None]:
        """Return open interest if the provider supports it."""
        return None

    def get_holders(self, market=None, limit: int = 20, **kwargs) -> list:
        """Return holder rows if the provider supports it."""
        return []

    def get_strikes(self, asset) -> list:
        """Return a set of strikes for a given asset"""
        chains = self.get_chains(asset)
        strikes = set()
        for right in chains["Chains"]:
            for exp_date, strikes in chains["Chains"][right].items():
                strikes |= set(strikes)

        return sorted(strikes)

    def get_yesterday_dividend(self, asset, quote=None):
        """Return dividend per share for a given
        asset for the day before"""
        bars = self.get_historical_prices(asset, 1, timestep="day")
        return bars.get_last_dividend()

    def get_yesterday_stock_split(self, asset, quote=None):
        """Return the stock split ratio for a given asset on the current backtest day."""
        return self.get_yesterday_stock_splits([asset], quote=quote).get(asset, 0)

    def _backtest_daily_corporate_action_length(self):
        length = 2000
        if (
            hasattr(self, "datetime_start")
            and hasattr(self, "datetime_end")
            and getattr(self, "datetime_start", None) is not None
            and getattr(self, "datetime_end", None) is not None
        ):
            try:
                span_days = (self.datetime_end.date() - self.datetime_start.date()).days + 1
                length = max(span_days + 10, 30)
            except Exception:
                length = 2000
        return length

    @staticmethod
    def _normalize_stock_split_ratio(value):
        try:
            ratio = float(value)
        except (TypeError, ValueError):
            return 0.0
        if not math.isfinite(ratio) or ratio <= 0:
            return 0.0
        if abs(ratio - 1.0) < 1e-12:
            return 0.0
        return ratio

    @staticmethod
    def _asset_values_match(left, right):
        if left == right:
            return True
        try:
            return (
                str(getattr(left, "symbol", "")).upper() == str(getattr(right, "symbol", "")).upper()
                and str(getattr(left, "asset_type", "")).lower() == str(getattr(right, "asset_type", "")).lower()
            )
        except Exception:
            return False

    @staticmethod
    def _quote_values_match(data_quote, requested_quote):
        if requested_quote is None:
            if data_quote is None:
                return True
            try:
                return (
                    str(getattr(data_quote, "symbol", "")).upper() == "USD"
                    and "forex" in str(getattr(data_quote, "asset_type", "")).lower()
                )
            except Exception:
                return False
        return DataSource._asset_values_match(data_quote, requested_quote)

    def _get_backtest_daily_corporate_action_frame(self, asset, quote=None):
        """Return the full preloaded daily frame for backtest corporate actions, when available.

        Backtest data sources such as routed IBKR prefetch the complete native daily series. Split
        events need that full daily frame because a split is effective before the market opens even
        when the provider timestamps the daily row at the close.
        """
        store = getattr(self, "_data_store", None)
        if not store:
            return None

        candidates = []
        finder = getattr(self, "find_asset_in_data_store", None)
        if callable(finder):
            try:
                key = finder(asset, quote, "day")
            except TypeError:
                try:
                    key = finder(asset, quote)
                except Exception:
                    key = None
            except Exception:
                key = None
            if key is not None:
                candidates.append(key)

        for key in candidates:
            data = store.get(key) if hasattr(store, "get") else None
            frame = self._daily_stock_split_frame_from_data(data, asset, quote)
            if frame is not None:
                return frame

        try:
            values = store.values()
        except Exception:
            values = []
        for data in values:
            frame = self._daily_stock_split_frame_from_data(data, asset, quote)
            if frame is not None:
                return frame
        return None

    def _daily_stock_split_frame_from_data(self, data, asset, quote=None):
        if data is None:
            return None
        if str(getattr(data, "timestep", "") or "").strip().lower() != "day":
            return None
        data_asset = getattr(data, "asset", None)
        data_quote = getattr(data, "quote", None)
        if isinstance(data_asset, tuple):
            if len(data_asset) >= 1:
                data_quote = data_quote if data_quote is not None else (data_asset[1] if len(data_asset) > 1 else None)
                data_asset = data_asset[0]
        if not self._asset_values_match(data_asset, asset):
            return None
        if not self._quote_values_match(data_quote, quote):
            return None
        frame = getattr(data, "df", None)
        if frame is None or not hasattr(frame, "columns") or "stock_splits" not in frame.columns:
            return None
        return frame

    @staticmethod
    def _frame_prices_are_split_adjusted(frame):
        if frame is None or not hasattr(frame, "columns") or "_split_adjusted" not in frame.columns:
            return False
        try:
            marker = frame["_split_adjusted"]
            if hasattr(marker, "fillna"):
                return bool(marker.fillna(False).astype(bool).any())
            return bool(marker)
        except Exception:
            return False

    def should_apply_stock_splits_to_positions(self, asset, quote=None):
        """Return whether split events should update held share quantities.

        Raw/unadjusted daily bars need position ledger adjustments on split dates. Split-adjusted
        providers already express historical fills in current share units, so applying the ledger
        split again would double-count the corporate action.
        """
        if not bool(getattr(self, "APPLY_BACKTEST_POSITION_SPLITS", True)):
            return False

        if bool(getattr(self, "AUTO_ADJUST_IMPLIES_SPLIT_ADJUSTED_PRICES", False)) and (
            bool(getattr(self, "auto_adjust", False)) or bool(getattr(self, "_auto_adjust", False))
        ):
            return False

        frame = self._get_backtest_daily_corporate_action_frame(asset, quote=quote)
        if self._frame_prices_are_split_adjusted(frame):
            return False

        return True

    def _stock_split_cache_from_frame(self, frame):
        asset_splits = {}
        if frame is None or not hasattr(frame, "iterrows") or "stock_splits" not in frame.columns:
            return asset_splits
        for idx, row in frame.iterrows():
            date_key = idx.date() if hasattr(idx, 'date') else idx
            ratio = self._normalize_stock_split_ratio(row.get('stock_splits', 0))
            if ratio:
                asset_splits[date_key] = ratio
        return asset_splits

    def _adjust_stale_daily_price_for_stock_split(self, data, price, dt):
        """Adjust a previous daily price when the current date has a split before the daily row.

        Native daily stock bars are commonly timestamped at the market close. On split effective
        dates, the position quantity changes before the market opens, but a pre-close mark may
        still resolve to the prior day's pre-split close. Dividing that stale price by the split
        ratio keeps portfolio value continuous until the split-date daily bar is available.
        """
        if price is None:
            return price
        if data is None or str(getattr(data, "timestep", "") or "").strip().lower() != "day":
            return price
        frame = getattr(data, "df", None)
        if frame is None or not hasattr(frame, "columns") or "stock_splits" not in frame.columns:
            return price
        if self._frame_prices_are_split_adjusted(frame):
            return price
        if bool(getattr(self, "AUTO_ADJUST_IMPLIES_SPLIT_ADJUSTED_PRICES", False)) and (
            bool(getattr(self, "auto_adjust", False)) or bool(getattr(self, "_auto_adjust", False))
        ):
            return price
        try:
            dt_ts = pd.Timestamp(dt)
            current_date = dt_ts.date()
        except Exception:
            return price

        try:
            matching_rows = frame.loc[[idx.date() == current_date for idx in frame.index]]
        except Exception:
            return price
        if matching_rows.empty:
            return price

        for idx, row in matching_rows.iterrows():
            ratio = self._normalize_stock_split_ratio(row.get("stock_splits", 0))
            if not ratio:
                continue
            try:
                event_ts = pd.Timestamp(idx)
                compare_dt = dt_ts
                if event_ts.tzinfo is not None:
                    if compare_dt.tzinfo is None:
                        compare_dt = compare_dt.tz_localize(event_ts.tzinfo)
                    else:
                        compare_dt = compare_dt.tz_convert(event_ts.tzinfo)
                elif compare_dt.tzinfo is not None:
                    compare_dt = compare_dt.tz_localize(None)
                if compare_dt >= event_ts:
                    continue
                return float(price) / ratio
            except Exception:
                continue
        return price

    def get_yesterday_dividends(self, assets, quote=None):
        """Return dividend per share for a list of assets for the day before.

        For backtesting, this method caches all dividend data to avoid repeated API calls.
        On the first call for an asset, it fetches ALL historical dividend data and caches it.
        Subsequent calls use the cache.
        """
        result = {}

        # For backtesting with dividends, use an efficient caching strategy
        if hasattr(self, '_datetime') and self._datetime:
            current_date = self._datetime.date() if hasattr(self._datetime, 'date') else self._datetime

            # Process each asset
            for asset in assets:
                # Check if we've already cached ALL dividends for this asset
                if asset not in self._dividend_cache:
                    # First time seeing this asset - fetch ALL its historical data and cache dividends
                    # Get enough bars to cover the entire backtest period
                    # Most backtests are < 1000 days; limit the fetch to what's needed for this backtest
                    # to avoid slow API calls (especially Polygon) and keep CI fast.
                    try:
                        # Default to a conservative upper bound, but shrink for short backtests.
                        length = 2000
                        if (
                            hasattr(self, "datetime_start")
                            and hasattr(self, "datetime_end")
                            and getattr(self, "datetime_start", None) is not None
                            and getattr(self, "datetime_end", None) is not None
                        ):
                            try:
                                span_days = (self.datetime_end.date() - self.datetime_start.date()).days + 1
                                # Add a small cushion for weekends/holidays; ensure at least ~1 month.
                                length = min(2000, max(span_days + 10, 30))
                            except Exception:
                                length = 2000

                        bars = self.get_bars([asset], length, timestep="day", quote=quote).get(asset)

                        # Extract all dividends from the bars and store by date
                        asset_dividends = {}
                        if bars is not None and hasattr(bars, 'df') and 'dividend' in bars.df.columns:
                            # Store dividend for each date
                            for idx, row in bars.df.iterrows():
                                date = idx.date() if hasattr(idx, 'date') else idx
                                dividend_val = row.get('dividend', 0)
                                if dividend_val and dividend_val > 0:
                                    asset_dividends[date] = dividend_val

                        # Cache the dividend dict for this asset
                        self._dividend_cache[asset] = asset_dividends
                        if asset_dividends:
                            logger.debug(
                                "[DIVIDEND][CACHE] Cached %d entries for %s (%s -> %s)",
                                len(asset_dividends),
                                getattr(asset, "symbol", asset),
                                min(asset_dividends.keys()),
                                max(asset_dividends.keys()),
                            )
                        else:
                            logger.debug(
                                "[DIVIDEND][CACHE] No dividend entries available for %s",
                                getattr(asset, "symbol", asset),
                            )
                    except Exception as e:
                        # If fetching fails, cache empty dict to avoid repeated failures
                        self._dividend_cache[asset] = {}

                # Now look up the dividend for the current trading date. Daily bars already align
                # dividends with the ex-date, so there's no need to subtract a day here.
                asset_dividends = self._dividend_cache.get(asset, {})
                dividend = asset_dividends.get(current_date, 0)
                if dividend:
                    logger.debug(
                        "[DIVIDEND][APPLY] %s -> %s pays %.4f on %s",
                        getattr(asset, "symbol", asset),
                        getattr(self, "_name", "strategy"),
                        dividend,
                        current_date,
                    )
                result[asset] = dividend

            return AssetsMapping(result)

        # Fallback to normal flow for non-backtesting
        assets_bars = self.get_bars(assets, 1, timestep="day", quote=quote)
        for asset, bars in assets_bars.items():
            if bars is not None:
                result[asset] = bars.get_last_dividend()

        return AssetsMapping(result)

    def get_yesterday_stock_splits(self, assets, quote=None):
        """Return stock split ratios for a list of assets on the current backtest day.

        Ratios use the data-source convention: 2.0 for a 2-for-1 split, 7.0 for
        a 7-for-1 split, and 0.1 for a 1-for-10 reverse split. Missing, zero,
        one, negative, and non-finite values are treated as no split.
        """
        result = {}

        if hasattr(self, '_datetime') and self._datetime:
            current_date = self._datetime.date() if hasattr(self._datetime, 'date') else self._datetime

            for asset in assets:
                if asset not in self._stock_split_cache:
                    try:
                        frame = self._get_backtest_daily_corporate_action_frame(asset, quote=quote)
                        asset_splits = self._stock_split_cache_from_frame(frame)

                        if not asset_splits:
                            length = self._backtest_daily_corporate_action_length()
                            bars = self.get_bars([asset], length, timestep="day", quote=quote).get(asset)
                            if bars is not None and hasattr(bars, 'df'):
                                asset_splits = self._stock_split_cache_from_frame(bars.df)

                        self._stock_split_cache[asset] = asset_splits
                        if asset_splits:
                            logger.debug(
                                "[SPLIT][CACHE] Cached %d entries for %s (%s -> %s)",
                                len(asset_splits),
                                getattr(asset, "symbol", asset),
                                min(asset_splits.keys()),
                                max(asset_splits.keys()),
                            )
                        else:
                            logger.debug(
                                "[SPLIT][CACHE] No split entries available for %s",
                                getattr(asset, "symbol", asset),
                            )
                    except Exception:
                        self._stock_split_cache[asset] = {}

                asset_splits = self._stock_split_cache.get(asset, {})
                split_ratio = asset_splits.get(current_date, 0)
                if split_ratio:
                    logger.debug(
                        "[SPLIT][APPLY] %s -> %s split ratio %.6f on %s",
                        getattr(asset, "symbol", asset),
                        getattr(self, "_name", "strategy"),
                        split_ratio,
                        current_date,
                    )
                result[asset] = split_ratio

            return AssetsMapping(result)

        assets_bars = self.get_bars(assets, 1, timestep="day", quote=quote)
        for asset, bars in assets_bars.items():
            if bars is None:
                continue
            get_last_stock_split = getattr(bars, "get_last_stock_split", None)
            if callable(get_last_stock_split):
                result[asset] = self._normalize_stock_split_ratio(get_last_stock_split())
            elif hasattr(bars, "df") and "stock_splits" in bars.df.columns:
                result[asset] = self._normalize_stock_split_ratio(bars.df["stock_splits"].iloc[-1])

        return AssetsMapping(result)

    def get_chain_full_info(self, asset: Asset, expiry: date | datetime, chains=None, underlying_price=float, risk_free_rate=float,
                            strike_min=None, strike_max=None) -> pd.DataFrame:
        """
        Get the full chain information for an option asset, including: greeks, bid/ask, open_interest, etc. For
        brokers that do not support this, greeks will be calculated locally. For brokers like Tradier this function
        is much faster as only a single API call can be done to return the data for all options simultaneously.

        Parameters
        ----------
        asset : Asset
            The option asset to get the chain information for.
        expiry : datetime.date | datetime.datetime
            The expiry date of the option chain.
        chains : dict
            The chains dictionary created by `get_chains` method. This is used
            to get the list of strikes needed to calculate the greeks.
        underlying_price : float
            Price of the underlying asset.
        risk_free_rate : float
            The risk-free rate used in interest calculations.
        strike_min : float
            The minimum strike price to return in the chain. If None, will return all strikes.
            Providing this will speed up execution by limiting the number of strikes queried.
        strike_max : float
            The maximum strike price to return in the chain. If None, will return all strikes.
            Providing this will speed up execution by limiting the number of strikes queried.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the full chain information for the option asset. Greeks columns will be named as
            'greeks.delta', 'greeks.theta', etc.
        """
        start_t = time.perf_counter()
        # Base level DataSource assumes that the data source does not support this and the greeks will be calculated
        # locally. Subclasses can override this method to provide a more efficient implementation.
        if isinstance(expiry, datetime):
            expiry_dt = expiry.date()
        elif isinstance(expiry, date):
            expiry_dt = expiry
        else:
            raise TypeError("expiry must be a datetime.date or datetime.datetime instance")

        expiry_str = expiry_dt.strftime("%Y-%m-%d")
        if chains is None:
            chains = self.get_chains(asset)

        rows = []
        query_total = 0
        for right in chains["Chains"]:
            expirations_map = chains["Chains"].get(right, {})
            if expiry_str not in expirations_map:
                raise KeyError(f"Expiry {expiry_str} not available for option type {right}")
            for strike in expirations_map[expiry_str]:
                # Skip strikes outside the requested range. Saves querying time.
                if strike_min and strike < strike_min or strike_max and strike > strike_max:
                    continue

                # Build the option asset and query for the price
                opt_asset = Asset(
                    asset.symbol,
                    asset_type="option",
                    expiration=expiry_dt,
                    strike=strike,
                    right=right,
                )
                query_t = time.perf_counter()
                option_symbol = _create_options_symbol(opt_asset.symbol, expiry_dt, right, strike)
                try:
                    opt_price = self.get_last_price(opt_asset, allow_stale_option_last=False)
                except TypeError:
                    opt_price = self.get_last_price(opt_asset)
                try:
                    opt_price_float = float(opt_price)
                except (TypeError, ValueError):
                    continue
                if not math.isfinite(opt_price_float) or opt_price_float <= 0:
                    continue
                greeks = self.calculate_greeks(opt_asset, opt_price, underlying_price, risk_free_rate)
                if not isinstance(greeks, dict):
                    continue
                query_total += time.perf_counter() - query_t

                # Build the row. Match the Tradier column naming conventions.
                row = {
                    "symbol": option_symbol,
                    "last": opt_price_float,
                    "expiration_date": expiry_dt,
                    "strike": strike,
                    "option_type": right,
                    "underlying": opt_asset.symbol,
                    "open_interest": 0,
                    "bid": 0.0,
                    "ask": 0.0,
                    "bidsize": 0,
                    "asksize": 0,
                    "volume": 0,
                    "last_volume": 0,
                    "average_volume": 0,
                    "type": 'option',
                }
                # Add in the greeks. Format: greeks.delta, greeks.theta, etc.
                row.update({f"greeks.{col}": val for col, val in greeks.items()})
                rows.append(row)

        logger.info(f"Chain Full Info Query Total: {query_total:.2f}s. "
                     f"Total Time: {time.perf_counter() - start_t:.2f}s, "
                     f"Rows: {len(rows)}")
        return pd.DataFrame(rows).sort_values("strike") if rows else pd.DataFrame()

    def calculate_greeks(
        self,
        asset,
        # API Querying for prices and rates are expensive, so we'll pass them in as arguments most of the time
        asset_price: float,
        underlying_price: float,
        risk_free_rate: float,
    ):
        """Returns Greeks in backtesting."""
        # Handle None values - don't cache or calculate if inputs are invalid
        if asset_price is None or underlying_price is None or risk_free_rate is None:
            return None

        # Optimization: Cache Greeks calculations based on key parameters
        # Round prices to 2 decimal places for cache key to handle minor price fluctuations
        current_date = self.get_datetime()
        cache_key = (
            asset.symbol,
            asset.strike,
            asset.right,
            asset.expiration,
            round(asset_price, 2),
            round(underlying_price, 2),
            round(risk_free_rate, 4),
            current_date.date() if hasattr(current_date, 'date') else current_date  # Cache per day to handle time decay
        )

        # Check cache
        if cache_key in self._greeks_cache:
            return self._greeks_cache[cache_key]

        # Keep cache size limited to prevent memory issues
        if len(self._greeks_cache) > 10000:
            # Clear oldest half of cache
            keys_to_remove = list(self._greeks_cache.keys())[:5000]
            for key in keys_to_remove:
                del self._greeks_cache[key]

        opt_price = asset_price
        und_price = underlying_price
        interest = risk_free_rate * 100
        black_scholes = _black_scholes()

        # If asset expiration is a datetime object, convert it to date
        expiration = asset.expiration
        if isinstance(expiration, datetime):
            expiration = expiration.date()

        # Convert the expiration to be a datetime with 4pm New York time
        expiration = datetime.combine(expiration, datetime.min.time())
        expiration = self.tzinfo.localize(expiration)
        expiration = expiration.astimezone(self.tzinfo)
        expiration = expiration.replace(hour=16, minute=0, second=0, microsecond=0)

        # Calculate the days to expiration, but allow for fractional days
        days_to_expiration = (expiration - current_date).total_seconds() / (60 * 60 * 24)

        if asset.right.upper() == "CALL":
            is_call = True
            iv = black_scholes.BS(
                [und_price, float(asset.strike), interest, days_to_expiration],
                callPrice=opt_price,
            )
        elif asset.right.upper() == "PUT":
            is_call = False
            iv = black_scholes.BS(
                [und_price, float(asset.strike), interest, days_to_expiration],
                putPrice=opt_price,
            )
        else:
            raise ValueError(f"Invalid option type {asset.right}, cannot get option greeks")

        c = black_scholes.BS(
            [und_price, float(asset.strike), interest, days_to_expiration],
            volatility=iv.impliedVolatility,
        )

        greeks = dict(
            implied_volatility=iv.impliedVolatility,
            delta=c.callDelta if is_call else c.putDelta,
            option_price=c.callPrice if is_call else c.putPrice,
            pv_dividend=None,  # (No equiv )
            gamma=c.gamma,
            vega=c.vega,
            theta=c.callTheta if is_call else c.putTheta,
            underlying_price=und_price,
        )

        # Cache the result
        self._greeks_cache[cache_key] = greeks

        return greeks

    def query_greeks(self, asset):
        """Query for the Greeks as it can be more accurate than calculating locally."""
        logger.info(f"Querying Options Greeks for {asset.symbol} is not supported for this "
                     f"data source {self.__class__}.")
        return {}

    def get_quote(self, asset: Asset, quote: Asset = None, exchange: str = None) -> Quote:
        """
        Get the latest quote for an asset (stock, option, or crypto).
        Returns a Quote object with bid, ask, last, and other fields if available.

        Parameters
        ----------
        asset : Asset object
            The asset for which the quote is needed.
        quote : Asset object, optional
            The quote asset for cryptocurrency pairs.
        exchange : str, optional
            The exchange to get the quote from.

        Returns
        -------
        Quote
            A Quote object with the quote information, eg. bid, ask, etc.
        """
        raise NotImplementedError("get_quote method not implemented")
