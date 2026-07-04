from collections import OrderedDict, defaultdict
from datetime import timedelta
from decimal import Decimal
from typing import Union

import pandas as pd

from lumibot.data_sources import DataSourceBacktesting
from lumibot.entities import Asset, Bars, Quote
from lumibot.tools.helpers import parse_timestep_qty_and_unit
from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)

# PERF: `find_asset_in_data_store()` is called in tight loops (often 200K+ times per backtest). It
# constructs `Asset("USD", "forex")` as the default quote on every call, which shows up as ~3s of
# pure `Asset.__init__` overhead in yappi profiles. Cache a module-level singleton so repeated
# lookups reuse the same object (Asset is effectively value-typed for USD/forex).
_USD_FOREX = Asset("USD", "forex")


class PandasData(DataSourceBacktesting):
    """
    PandasData is a Backtesting-only DataSource that uses a Pandas DataFrame (read from CSV) as the source of
    data for a backtest run. It is not possible to use this class to run a live trading strategy.
    """

    SOURCE = "PANDAS"
    # Provider-specific day-bar policy:
    #
    # Some providers (IBKR stock/index) require native day bars for correctness because provider-
    # specific day normalization/repairs are applied before storage. Others (e.g., Polygon) rely on
    # minute->day resampling and should continue to allow minute datasets to satisfy day requests.
    PREFER_NATIVE_DAY_BARS_FOR_STOCK_INDEX = False
    TIMESTEP_MAPPING = [
        {"timestep": "day", "representations": ["1D", "day"]},
        {"timestep": "minute", "representations": ["1M", "minute"]},
    ]

    def __init__(self, *args, pandas_data=None, auto_adjust=True, allow_option_quote_fallback: bool = False, **kwargs):
        super().__init__(*args, **kwargs)
        self.option_quote_fallback_allowed = allow_option_quote_fallback
        self.name = "pandas"
        self.pandas_data = self._set_pandas_data_keys(pandas_data)
        self.auto_adjust = auto_adjust
        self._data_store = self.pandas_data
        self._date_index = None
        self._date_supply = None
        self._timestep = "minute"
        # PERF: `find_asset_in_data_store()` is called in tight loops (quotes + history). Cache the
        # resolved key for repeated `(asset, quote, timestep)` lookups within a backtest run.
        self._find_asset_in_data_store_cache = {}
        # PERF: When a lookup misses, the missing-asset warning fires once per iteration. On a 1-min
        # futures backtest that's ~65K warnings and ~4s of LumibotLogger overhead. Gate the warning
        # to first-encounter per (asset, timestep) so it still surfaces the problem once.
        self._missing_asset_warned = set()

    @staticmethod
    def _set_pandas_data_keys(pandas_data):
        # OrderedDict tracks the LRU dataframes for when it comes time to do evictions.
        new_pandas_data = OrderedDict()

        def _get_new_pandas_data_key(data):
            # Always save the asset as a tuple of Asset and quote
            if isinstance(data.asset, tuple):
                return data.asset
            elif isinstance(data.asset, Asset):
                # If quote is not specified, use USD as the quote
                if data.quote is None:
                    # Warn that USD is being used as the quote
                    logger.warning(f"No quote specified for {data.asset}. Using USD as the quote.")
                    return data.asset, _USD_FOREX
                return data.asset, data.quote
            else:
                raise ValueError("Asset must be an Asset or a tuple of Asset and quote")

        # Check if pandas_data is a dictionary
        if isinstance(pandas_data, dict):
            for k, data in pandas_data.items():
                key = _get_new_pandas_data_key(data)
                new_pandas_data[key] = data

        # Check if pandas_data is a list
        elif isinstance(pandas_data, list):
            for data in pandas_data:
                key = _get_new_pandas_data_key(data)
                new_pandas_data[key] = data

        return new_pandas_data

    def load_data(self):
        self._data_store = self.pandas_data
        self._date_index = self.update_date_index()
        # Invalidate lookup cache whenever we reload/replace the underlying data store.
        self._find_asset_in_data_store_cache.clear()

        if len(self._data_store.values()) > 0:
            self._timestep = list(self._data_store.values())[0].timestep

        pcal = self.get_trading_days_pandas()
        self._date_index = self.clean_trading_times(self._date_index, pcal)
        for _, data in self._data_store.items():
            data.repair_times_and_fill(self._date_index)
        return pcal

    def clean_trading_times(self, dt_index, pcal):
        """Fill gaps within trading days using the supplied market calendar.

        Parameters
        ----------
        dt_index : pandas.DatetimeIndex
            Original datetime index.
        pcal : pandas.DataFrame
            Calendar with ``market_open`` and ``market_close`` columns indexed by date.

        Returns
        -------
        pandas.DatetimeIndex
            Cleaned index with one-minute frequency during market hours.
        """
        # Ensure the datetime index is in datetime format and drop duplicate timestamps
        dt_index = pd.to_datetime(dt_index).drop_duplicates()

        # Create a DataFrame with dt_index as the index and sort it
        df = pd.DataFrame(range(len(dt_index)), index=dt_index)
        df = df.sort_index()

        # Create a column for the date portion only (normalize to date, keeping as datetime64 type)
        df["dates"] = df.index.normalize()

        # Merge with the trading calendar on the 'dates' column to get market open/close times.
        # Use a left join to keep all rows from the original index.
        df = df.merge(
            pcal[["market_open", "market_close"]],
            left_on="dates",
            right_index=True,
            how="left"
        )

        if self._timestep == "minute":
            # Resample to a 1-minute frequency, using pad to fill missing times.
            # At this point, the index is unique so asfreq will work correctly.
            df = df.asfreq("1min", method="pad")

            # Filter to include only the rows that fall within market open and close times.
            result_index = df.loc[
                (df.index >= df["market_open"]) & (df.index <= df["market_close"])
            ].index
        else:
            result_index = df.index

        # Backtests should never iterate beyond the requested end bound. Keep any pre-start buffer
        # (needed for lookbacks), but drop dates strictly after `datetime_end` so daily pandas
        # backtests stop at the correct last trading day (see tests/test_momentum.py).
        if self.datetime_end is not None:
            result_index = result_index[result_index <= self.datetime_end]

        return result_index

    def get_trading_days_pandas(self):
        pcal = pd.DataFrame(self._date_index)

        if pcal.empty:
            # Create a dummy dataframe that spans the entire date range with market_open and market_close
            # set to 00:00:00 and 23:59:59 respectively.
            result = pd.DataFrame(
                index=pd.date_range(start=self.datetime_start, end=self.datetime_end, freq="D"),
                columns=["market_open", "market_close"],
            )
            result["market_open"] = result.index.floor("D")
            result["market_close"] = result.index.ceil("D") - pd.Timedelta("1s")
            return result

        else:
            pcal.columns = ["datetime"]
            # Normalize to date but keep as datetime64 type (not date objects)
            pcal["date"] = pcal["datetime"].dt.normalize()
            result = pcal.groupby("date").agg(
                market_open=(
                    "datetime",
                    "first",
                ),
                market_close=(
                    "datetime",
                    "last",
                ),
            )
            return result

    def get_assets(self):
        return list(self._data_store.keys())

    def get_asset_by_name(self, name):
        return [asset for asset in self.get_assets() if asset.name == name]

    def get_asset_by_symbol(self, symbol, asset_type=None):
        """Finds the assets that match the symbol. If type is specified
        finds the assets matching symbol and type.

        Parameters
        ----------
        symbol : str
            The symbol of the asset.
        asset_type : str
            Asset type. One of:
            - stock
            - future
            - option
            - forex

        Returns
        -------
        list of Asset
        """
        store_assets = self.get_assets()
        if asset_type is None:
            return [asset for asset in store_assets if asset.symbol == symbol]
        else:
            return [asset for asset in store_assets if (asset.symbol == symbol and asset.asset_type == asset_type)]

    def update_date_index(self):
        dt_index = None
        for asset, data in self._data_store.items():
            if dt_index is None:
                df = data.df
                dt_index = df.index
            else:
                dt_index = dt_index.join(data.df.index, how="outer")

        if dt_index is None:
            # Build a dummy index
            freq = "1min" if self._timestep == "minute" else "1D"
            dt_index = pd.date_range(start=self.datetime_start, end=self.datetime_end, freq=freq)

        else:
            if self.datetime_end < dt_index[0]:
                raise ValueError(
                    f"The ending date for the backtest was set for {self.datetime_end}. "
                    f"The earliest data entered is {dt_index[0]}. \nNo backtest can "
                    f"be run since there is no data before the backtest end date."
                )
            elif self.datetime_start > dt_index[-1]:
                raise ValueError(
                    f"The starting date for the backtest was set for {self.datetime_start}. "
                    f"The latest data entered is {dt_index[-1]}. \nNo backtest can "
                    f"be run since there is no data after the backtest start date."
                )

        return dt_index

    def get_last_price(self, asset, quote=None, exchange=None) -> Union[float, Decimal, None]:
        # Takes an asset and returns the last known price
        tuple_to_find = self.find_asset_in_data_store(asset, quote)

        # If the asset is not yet cached, try a quick fetch using the current timestep
        # so daily-cadence strategies do not trigger minute downloads by default.
        if tuple_to_find not in self._data_store:
            try:
                target_ts = self._timestep or self.MIN_TIMESTEP
                # Fetch a single bar to seed the cache
                self.get_historical_prices(asset, length=1, timestep=target_ts, quote=quote, exchange=exchange)
            except Exception:
                pass
            tuple_to_find = self.find_asset_in_data_store(asset, quote)

        if tuple_to_find in self._data_store:
            data = self._data_store[tuple_to_find]
            try:
                dt = self.get_datetime()
                price = data.get_last_price(dt)
                price = self._adjust_stale_daily_price_for_stock_split(data, price, dt)

                # Check if price is NaN
                if pd.isna(price):
                    # Provide more specific error message for index assets
                    if hasattr(asset, 'asset_type') and asset.asset_type == Asset.AssetType.INDEX:
                        logger.warning(
                            "Index asset `%s` returned a NaN price. This data source did not provide usable "
                            "index data for the requested window. Verify that index coverage and any required "
                            "subscriptions are enabled for the active provider.",
                            asset.symbol,
                        )
                    else:
                        logger.info(f"Error getting last price for {tuple_to_find}: price is NaN")
                    return None

                if price is None:
                    return None

                # Treat non-positive prices as missing data.
                try:
                    numeric_price = float(price)
                except (TypeError, ValueError):
                    numeric_price = None

                if numeric_price is not None and numeric_price <= 0:
                    logger.warning(
                        "Ignoring non-positive price %.4f for %s; treating as missing data.",
                        numeric_price,
                        tuple_to_find,
                    )
                    return None

                return price
            except Exception as e:
                logger.info(f"Error getting last price for {tuple_to_find}: {e}")
                return None
        else:
            # Provide more specific error message when asset not found in data store
            if hasattr(asset, 'asset_type') and asset.asset_type == Asset.AssetType.INDEX:
                logger.warning(
                    "The index asset `%s` does not exist or has no data in this data source. Verify that the "
                    "active provider supports index data for this symbol and that any required subscriptions "
                    "are enabled.",
                    asset.symbol,
                )
            return None

    def get_quote(self, asset, quote=None, exchange=None) -> Quote:
        """
        Get the latest quote for an asset.
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
            A Quote object with the quote information.
        """
        from lumibot.entities import Quote

        # Takes an asset and returns the last known price
        tuple_to_find = self.find_asset_in_data_store(asset, quote)

        if tuple_to_find in self._data_store:
            data = self._data_store[tuple_to_find]
            dt = self.get_datetime()
            ohlcv_bid_ask_dict = data.get_quote(dt)
            if isinstance(ohlcv_bid_ask_dict, dict):
                for price_key in ("open", "high", "low", "close", "bid", "ask"):
                    if price_key in ohlcv_bid_ask_dict:
                        ohlcv_bid_ask_dict[price_key] = self._adjust_stale_daily_price_for_stock_split(
                            data,
                            ohlcv_bid_ask_dict.get(price_key),
                            dt,
                        )

            # Check if ohlcv_bid_ask_dict is NaN
            if pd.isna(ohlcv_bid_ask_dict):
                logger.info(f"Error getting ohlcv_bid_ask for {tuple_to_find}: ohlcv_bid_ask_dict is NaN")
                return Quote(asset=asset)

            for side_key in ("bid", "ask"):
                value = ohlcv_bid_ask_dict.get(side_key)
                if value is not None:
                    try:
                        numeric_value = float(value)
                    except (TypeError, ValueError):
                        numeric_value = value
                    if isinstance(numeric_value, (int, float)) and numeric_value <= 0:
                        ohlcv_bid_ask_dict[side_key] = None

            # Convert dictionary to Quote object
            return Quote(
                asset=asset,
                price=ohlcv_bid_ask_dict.get('close'),
                bid=ohlcv_bid_ask_dict.get('bid'),
                ask=ohlcv_bid_ask_dict.get('ask'),
                volume=ohlcv_bid_ask_dict.get('volume'),
                timestamp=dt,
                bid_size=ohlcv_bid_ask_dict.get('bid_size'),
                ask_size=ohlcv_bid_ask_dict.get('ask_size'),
                raw_data=ohlcv_bid_ask_dict
            )
        else:
            return Quote(asset=asset)

    def get_last_prices(self, assets, quote=None, exchange=None, **kwargs):
        result = {}
        for asset in assets:
            result[asset] = self.get_last_price(asset, quote=quote, exchange=exchange)
        return result

    def find_asset_in_data_store(self, asset, quote=None, timestep=None):
        # PERF: Avoid rebuilding candidate lists and repeatedly probing `_data_store` when the same
        # `(asset, quote, timestep)` is requested every iteration (common in backtests).
        #
        # IMPORTANT: Do not treat a cached `None` as authoritative.
        #
        # Some backtesting data sources (notably ThetaDataBacktestingPandas) *mutate* `_data_store`
        # during the run by fetching/warming new datasets on-demand. If we cache "misses" (None),
        # a lookup performed *before* the dataset is fetched will poison all future lookups for that
        # `(asset, quote, timestep)` and make the freshly loaded data unreachable.
        try:
            cache_key = (asset, quote, timestep)
            cached = self._find_asset_in_data_store_cache.get(cache_key)
            if cached is not None:
                return cached
        except Exception:
            cache_key = None

        requested_unit = None
        normalized_key = None
        asset_for_type = asset[0] if isinstance(asset, tuple) else asset
        asset_lookup = asset_for_type
        quote_lookup = quote
        if isinstance(asset, tuple) and len(asset) >= 2 and quote_lookup is None:
            tuple_quote = asset[1]
            if isinstance(tuple_quote, Asset):
                quote_lookup = tuple_quote
        requested_asset_type = str(getattr(asset_for_type, "asset_type", "") or "").strip().lower()
        if "." in requested_asset_type:
            requested_asset_type = requested_asset_type.split(".")[-1]
        if timestep is not None:
            try:
                qty, requested_unit = parse_timestep_qty_and_unit(str(timestep))
            except Exception:
                qty, requested_unit = 1, str(timestep)
                requested_unit = str(timestep)
            requested_unit = (requested_unit or "").strip().lower()
            if requested_unit in {"m", "min", "mins"}:
                requested_unit = "minute"
            elif requested_unit in {"minutes"}:
                requested_unit = "minute"
            elif requested_unit in {"h", "hr", "hrs", "hour", "hours"}:
                requested_unit = "hour"
            elif requested_unit in {"d", "day", "days"}:
                requested_unit = "day"

            # Normalize timestep key so "15min" can match cached datasets stored as "15minute".
            try:
                qty = int(qty)
            except Exception:
                qty = 1
            if requested_unit and requested_unit in {"minute", "hour", "day"}:
                normalized_key = requested_unit if qty == 1 else f"{qty}{requested_unit}"
            else:
                normalized_key = str(timestep)

        def _accepts_timestep(data_obj) -> bool:
            if requested_unit is None:
                return True
            data_ts = str(getattr(data_obj, "timestep", "") or "").strip().lower()
            if requested_unit == "minute":
                return data_ts == "minute"
            if requested_unit == "hour":
                # Hour requests can be satisfied by either hour data or minute data (resample).
                return data_ts in {"hour", "minute"}
            if requested_unit == "day":
                # IMPORTANT:
                # Keep day requests pinned to native day datasets only for providers that opt into
                # this behavior (IBKR stock/index correctness path).
                if (
                    requested_asset_type in {"stock", "index"}
                    and bool(getattr(self, "PREFER_NATIVE_DAY_BARS_FOR_STOCK_INDEX", False))
                ):
                    return data_ts == "day"
                return data_ts in {"day", "minute"}
            # Fallback: require exact match for other timesteps.
            return data_ts == requested_unit

        candidates = []

        if timestep is not None:
            base_quote = quote_lookup if quote_lookup is not None else _USD_FOREX
            candidates.append((asset_lookup, base_quote, timestep))
            if normalized_key is not None and str(normalized_key) != str(timestep):
                candidates.append((asset_lookup, base_quote, normalized_key))
            if quote_lookup is not None:
                candidates.append((asset_lookup, _USD_FOREX, timestep))
                if normalized_key is not None and str(normalized_key) != str(timestep):
                    candidates.append((asset_lookup, _USD_FOREX, normalized_key))

        if quote_lookup is not None:
            candidates.append((asset_lookup, quote_lookup))

        if isinstance(asset_lookup, Asset) and asset_lookup.asset_type in ["option", "future", "cont_future", "stock", "index"]:
            candidates.append((asset_lookup, _USD_FOREX))

        candidates.append(asset_lookup)
        if asset_lookup is not asset:
            candidates.append(asset)

        for key in candidates:
            if key in self._data_store:
                data_obj = self._data_store.get(key)
                if data_obj is None:
                    continue
                if _accepts_timestep(data_obj):
                    if cache_key is not None:
                        self._find_asset_in_data_store_cache[cache_key] = key
                    return key
        return None

    def _pull_source_symbol_bars(
        self,
        asset,
        length,
        timestep="",
        timeshift=0,
        quote=None,
        exchange=None,
        include_after_hours=True,
    ):
        timestep = timestep if timestep else self.MIN_TIMESTEP
        if exchange is not None:
            logger.warning(
                f"the exchange parameter is not implemented for PandasData, but {exchange} was passed as the exchange"
            )

        if not timeshift:
            timeshift = 0

        asset_to_find = self.find_asset_in_data_store(asset, quote, timestep)

        if asset_to_find in self._data_store:
            data = self._data_store[asset_to_find]
        else:
            # PERF: Gate to first-encounter per (asset, timestep). In 1-min backtests this warning
            # fired ~65K times/run with no new info, costing ~4s of logger overhead.
            warn_key = (asset, str(timestep))
            if warn_key not in self._missing_asset_warned:
                self._missing_asset_warned.add(warn_key)
                if hasattr(asset, 'asset_type') and asset.asset_type == Asset.AssetType.INDEX:
                    logger.warning(
                        "The index asset `%s` does not exist or has no data in this data source. Verify that the "
                        "active provider supports index data for this symbol and that any required subscriptions "
                        "are enabled.",
                        asset.symbol,
                    )
                else:
                    logger.warning(f"The asset: `{asset}` does not exist or does not have data.")
            return

        now = self.get_datetime()
        try:
            res = data.get_bars(now, length=length, timestep=timestep, timeshift=timeshift)
        # Return None if data.get_bars returns a ValueError
        except ValueError as e:
            logger.info(f"Error getting bars for {asset}: {e}")
            return None

        return res

    def _pull_source_symbol_bars_between_dates(
        self,
        asset,
        timestep="",
        quote=None,
        exchange=None,
        include_after_hours=True,
        start_date=None,
        end_date=None,
    ):
        """Pull all bars for an asset"""
        timestep = timestep if timestep else self.MIN_TIMESTEP
        asset_to_find = self.find_asset_in_data_store(asset, quote, timestep)

        if asset_to_find in self._data_store:
            data = self._data_store[asset_to_find]
        else:
            # PERF: Gate to first-encounter per (asset, timestep). See _pull_source_symbol_bars.
            warn_key = (asset, str(timestep))
            if warn_key not in self._missing_asset_warned:
                self._missing_asset_warned.add(warn_key)
                if hasattr(asset, 'asset_type') and asset.asset_type == Asset.AssetType.INDEX:
                    logger.warning(
                        "The index asset `%s` does not exist or has no data in this data source. Verify that the "
                        "active provider supports index data for this symbol and that any required subscriptions "
                        "are enabled.",
                        asset.symbol,
                    )
                else:
                    logger.warning(f"The asset: `{asset}` does not exist or does not have data.")
            return

        try:
            res = data.get_bars_between_dates(start_date=start_date, end_date=end_date, timestep=timestep)
        # Return None if data.get_bars returns a ValueError
        except ValueError as e:
            logger.info(f"Error getting bars for {asset}: {e}")
            res = None
        return res

    def _pull_source_bars(
        self,
        assets,
        length,
        timestep="",
        timeshift=None,
        quote=None,
        include_after_hours=True,
    ):
        """pull broker bars for a list assets"""
        timestep = timestep if timestep else self.MIN_TIMESTEP
        self._parse_source_timestep(timestep, reverse=True)

        result = {}
        for asset in assets:
            result[asset] = self._pull_source_symbol_bars(
                asset, length, timestep=timestep, timeshift=timeshift, quote=quote
            )
            # remove assets that have no data from the result
            if result[asset] is None:
                result.pop(asset)

        return result

    def _parse_source_symbol_bars(self, response, asset, quote=None, length=None, return_polars: bool = False):
        """parse broker response for a single asset

        CRITICAL: return_polars defaults to False for backwards compatibility.
        PandasData always returns pandas-backed Bars for consistency.
        """
        asset1 = asset
        asset2 = quote
        if isinstance(asset, tuple):
            asset1, asset2 = asset
        bars = Bars(response, self.SOURCE, asset1, quote=asset2, raw=response, return_polars=return_polars)
        return bars

    def get_yesterday_dividend(self, asset, quote=None):
        return super().get_yesterday_dividend(asset, quote=quote)

    def get_yesterday_dividends(self, assets, quote=None):
        return super().get_yesterday_dividends(assets, quote=quote)

    # =======Options methods.=================
    def get_chains(self, asset: Asset, quote: Asset = None, exchange: str = None):
        """Returns option chains.

        Obtains option chain information for the asset (stock) from each
        of the exchanges the options trade on and returns a dictionary
        for each exchange.

        Parameters
        ----------
        asset : Asset object
            The stock whose option chain is being fetched. Represented
            as an asset object.
        quote : Asset object, optional
            The quote asset. Default is None.
        exchange : str, optional
            The exchange to fetch the option chains from. For PandasData, will only use "SMART".

        Returns
        -------
        dict
            Mapping with keys such as ``Multiplier`` (e.g. ``"100"``) and ``Chains``.
            ``Chains`` is a nested dictionary where expiration dates map to strike lists,
            e.g. ``chains['Chains']['CALL']['2023-07-31'] = [strike1, strike2, ...]``.
        """
        chains = dict(
            Multiplier=100,
            Exchange="SMART",
            Chains={"CALL": defaultdict(list), "PUT": defaultdict(list)},
        )

        for store_item, data in self._data_store.items():
            store_asset = store_item[0]
            if store_asset.asset_type != "option":
                continue
            if store_asset.symbol != asset.symbol:
                continue
            chains["Chains"][store_asset.right][store_asset.expiration].append(store_asset.strike)

        return chains

    def get_start_datetime_and_ts_unit(self, length, timestep, start_dt=None, start_buffer=timedelta(days=5)):
        """
        Get the start datetime for the data.

        Parameters
        ----------
        length : int
            The number of data points to get.
        timestep : str
            The timestep to use. For example, "1minute" or "1hour" or "1day".


        Returns
        -------
        datetime
            The start datetime.
        str
            The timestep unit.
        """
        # Convert timestep string to timedelta and get start datetime
        td, ts_unit = self.convert_timestep_str_to_timedelta(timestep)

        if ts_unit == "day":
            weeks_requested = length // 5  # Full trading week is 5 days
            extra_padding_days = weeks_requested * 3  # to account for 3day weekends
            td = timedelta(days=length + extra_padding_days)
        else:
            td *= length

        if start_dt is not None:
            start_datetime = start_dt - td
        else:
            start_datetime = self.datetime_start - td

        # Subtract an extra 5 days to the start datetime to make sure we have enough
        # data when it's a sparsely traded asset, especially over weekends
        start_datetime = start_datetime - start_buffer

        return start_datetime, ts_unit

    def get_historical_prices(
        self,
        asset: Asset,
        length: int,
        timestep: str = None,
        timeshift: int = None,
        quote: Asset = None,
        exchange: str = None,
        include_after_hours: bool = True,
        # Accept `return_polars` for API compatibility with other data sources.
        # PandasData always returns pandas-backed Bars, so this flag is ignored.
        return_polars: bool = False,
    ):
        """Get bars for a given asset"""
        if isinstance(asset, str):
            asset = Asset(symbol=asset)

        if not timestep:
            timestep = self.get_timestep()
        response = self._pull_source_symbol_bars(
            asset,
            length,
            timestep=timestep,
            timeshift=timeshift,
            quote=quote,
            exchange=exchange,
            include_after_hours=include_after_hours,
        )
        if isinstance(response, float):
            return response
        elif response is None:
            return None

        bars = self._parse_source_symbol_bars(response, asset, quote=quote, length=length, return_polars=return_polars)
        return bars
