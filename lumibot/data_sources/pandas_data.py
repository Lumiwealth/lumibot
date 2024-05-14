import logging
from collections import defaultdict, OrderedDict
from datetime import date, timedelta

import pandas as pd
from lumibot.data_sources import DataSourceBacktesting
from lumibot.entities import Asset, AssetsMapping, Bars


class PandasData(DataSourceBacktesting):
    """
    PandasData is a Backtesting-only DataSource that uses a Pandas DataFrame (read from CSV) as the source of
    data for a backtest run. It is not possible to use this class to run a live trading strategy.
    """

    SOURCE = "PANDAS"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {"timestep": "day", "representations": ["1D", "day"]},
        {"timestep": "minute", "representations": ["1M", "minute"]},
    ]

    def __init__(self, *args, pandas_data=None, auto_adjust=True, timestep="minute", **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "pandas"
        self.pandas_data = self._set_pandas_data_keys(OrderedDict(), pandas_data)
        self.auto_adjust = auto_adjust
        self._data_store = self.pandas_data
        self._date_index = None
        self._date_supply = None
        self._timestep = timestep

    @staticmethod
    def _set_pandas_data_keys(existing_pandas_data, new_pandas_data):
        """
        Update existing_pandas_data with new_pandas_data, respecting existing timesteps.

        Parameters
        ----------
        existing_pandas_data : OrderedDict
            The existing pandas data to update.
        new_pandas_data : list
            The new pandas data to add to the existing data.

        Returns
        -------
        OrderedDict
            The updated pandas data with new timesteps added without overwriting existing ones.
        """
        if existing_pandas_data is None:
            existing_pandas_data = OrderedDict()

        def _get_new_pandas_data_key(data):
            # Always save the asset as a tuple of Asset and quote
            if isinstance(data.asset, tuple):
                return data.asset
            elif isinstance(data.asset, Asset):
                # If quote is not specified, use USD as the quote
                if data.quote is None:
                    # Warn that USD is being used as the quote
                    logging.warning(f"No quote specified for {data.asset}. Using USD as the quote.")
                    return data.asset, Asset(symbol="USD", asset_type="forex")
                return data.asset, data.quote
            else:
                raise ValueError("Asset must be an Asset or a tuple of Asset and quote")

        if new_pandas_data is None:
            return existing_pandas_data

        # Convert new_pandas_data to nested dict with timesteps
        for data in new_pandas_data:
            # Ensure the DataFrame has a DateTimeIndex
            if 'datetime' in data.df.columns:
                data.df = data.df.set_index(pd.DatetimeIndex(data.df['datetime']))

            key = _get_new_pandas_data_key(data)
            if key not in existing_pandas_data:
                existing_pandas_data[key] = {}
            existing_pandas_data[key][data.timestep] = data
    
    def load_data(self):
        self._data_store = self.pandas_data
        self._date_index = self.update_date_index()

        if len(self._data_store.values()) > 0:
            self._timestep = list(self._data_store.values())[0].timestep

        pcal = self.get_trading_days_pandas()
        self._date_index = self.clean_trading_times(self._date_index, pcal)
        for _, data in self._data_store.items():
            data.repair_times_and_fill(self._date_index)
        return pcal

    def clean_trading_times(self, dt_index, pcal):
        # Used to fill in blanks in the data, on trading days, within market trading hours.
        df = pd.DataFrame(range(len(dt_index)), index=dt_index)
        df = df.sort_index()
        df["dates"] = df.index.date
        df = df.merge(pcal[["market_open", "market_close"]], left_on="dates", right_index=True)
        if self._timestep == "minute":
            df = df.asfreq("1min", method="pad")
            result_index = df.loc[(df.index >= df["market_open"]) & (df.index <= df["market_close"]), :].index
        else:
            result_index = df.index
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
            pcal["date"] = pcal["datetime"].dt.date
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
            # Determine the frequency and adjust the end date accordingly
            if self._timestep == "minute":
                freq = "1min"
                adjusted_end = self.datetime_end
            else:  # Assumes the only other option is daily
                freq = "1D"
                adjusted_end = self.datetime_end + pd.Timedelta(days=1)

            # Generate date range with the determined frequency and adjusted end date
            dt_index = pd.date_range(start=self.datetime_start, end=adjusted_end, freq=freq)

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

    def get_last_price(self, asset, quote=None, timestep="minute", exchange=None):
        """
        Takes an asset and returns the last known price for the specified timestep.
        """
        tuple_to_find = self.find_asset_in_data_store(asset, quote)

        if tuple_to_find in self._data_store:
            data_store = self._data_store[tuple_to_find]

            if timestep in data_store:
                data = data_store[timestep]
                try:
                    dt = self.get_datetime()
                    price = data.get_last_price(dt)

                    # Check if price is NaN
                    if pd.isna(price):
                        logging.info(f"Error getting last price for {tuple_to_find} with timestep {timestep}: price is NaN")
                        return None

                    return price
                except Exception as e:
                    logging.info(f"Error getting last price for {tuple_to_find} with timestep {timestep}: {e}")
                    return None
            else:
                logging.info(f"Error: No data for {tuple_to_find} with timestep {timestep}")
                return None
        else:
            logging.info(f"Error: Asset {asset} with quote {quote} not found in data store")
            return None


    def get_last_prices(self, assets, quote=None, exchange=None, **kwargs):
        result = {}
        for asset in assets:
            result[asset] = self.get_last_price(asset, quote=quote, exchange=exchange)
        return result

    def find_asset_in_data_store(self, asset, quote=None):
        """
        Find the asset in the data store, handling quotes if necessary.

        Parameters
        ----------
        asset : Asset or tuple
            The asset to find in the data store. Can be an Asset object or a tuple (symbol, quote).
        quote : Asset, optional
            The quote asset to use, if applicable.

        Returns
        -------
        tuple
            A tuple representing the asset and quote combination found in the data store.
        """
        if isinstance(asset, tuple):
            search_asset = asset
        else:
            if quote is None:
                quote = Asset("USD", "forex")  # Asumimos USD como la cotización predeterminada si no se proporciona ninguna
            search_asset = (asset, quote)
        
        if search_asset in self._data_store:
            return search_asset
        else:
            logging.warning(f"Asset {asset} with quote {quote} not found in data store.")
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
        """Pull all bars for an asset"""
        timestep = timestep if timestep else self.MIN_TIMESTEP
        if exchange is not None:
            logging.warning(
                f"the exchange parameter is not implemented for PandasData, but {exchange} was passed as the exchange"
            )

        if not timeshift:
            timeshift = 0

        asset_to_find = self.find_asset_in_data_store(asset, quote)

        if asset_to_find is None:
            logging.warning(f"The asset: `{asset}` does not exist or does not have data.")
            return None

        # Verify that we have data for the specific timestep
        if timestep in self._data_store[asset_to_find]:
            data = self._data_store[asset_to_find][timestep]
        else:
            logging.warning(f"The asset: `{asset}` does not have data for the timestep `{timestep}`.")
            return None

        now = self.get_datetime()
        try:
            res = data.get_bars(now, length=length, timestep=timestep, timeshift=timeshift)
        except ValueError as e:
            logging.info(f"Error getting bars for {asset}: {e}")
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
        asset_to_find = self.find_asset_in_data_store(asset, quote)

        if asset_to_find in self._data_store and timestep in self._data_store[asset_to_find]:
            data = self._data_store[asset_to_find][timestep]
        else:
            logging.warning(f"The asset: `{asset}` with timestep `{timestep}` does not exist or does not have data.")
            return

        try:
            res = self.get_bars_between_dates(asset,start_date=start_date, end_date=end_date, timestep=timestep)
        # Return None if data.get_bars returns a ValueError
        except ValueError as e:
            logging.info(f"Error getting bars for {asset}: {e}")
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

    def _parse_source_symbol_bars(self, response, asset, quote=None, length=None):
        """parse broker response for a single asset"""
        asset1 = asset
        asset2 = quote
        if isinstance(asset, tuple):
            asset1, asset2 = asset
        bars = Bars(response, self.SOURCE, asset1, quote=asset2, raw=response)
        return bars

    def get_yesterday_dividend(self, asset, quote=None):
        pass

    def get_yesterday_dividends(self, assets, quote=None):
        pass

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
        dictionary of dictionary
            Format:
            - `Multiplier` (str) eg: `100`
            - 'Chains' - paired Expiration/Strke info to guarentee that the stikes are valid for the specific
                         expiration date.
                         Format:
                           chains['Chains']['CALL'][exp_date] = [strike1, strike2, ...]
                         Expiration Date Format: 2023-07-31
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

        # Multiply td by length to get the end datetime
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
        self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True
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

        bars = self._parse_source_symbol_bars(response, asset, quote=quote, length=length)
        return bars
    
    def get_bars_between_dates(self, asset, timestep="minute", exchange=None, start_date=None, end_date=None):
        """Returns a dataframe of all the data available between the start and end dates.

        Parameters
        ----------
        asset : Asset
            The asset to get data for.
        timestep : str
            The frequency of the data to get the data. Only minute and day are supported.
        exchange : str
            The exchange to get the data for.
        start_date : datetime.datetime
            The start date to get the data for.
        end_date : datetime.datetime
            The end date to get the data for.

        Returns
        -------
        pandas.DataFrame
        """

        if timestep != "minute" and timestep != "day":
            raise ValueError(f"Only minute and day are supported for timestep. You provided: {timestep}")

        # Assuming USD as default quote
        asset_to_find = (asset, Asset("USD", "forex"))

        if asset_to_find not in self._data_store or timestep not in self._data_store[asset_to_find]:
            logging.warning(f"No data available for asset `{asset}` with timestep `{timestep}`.")
            return None

        data = self._data_store[asset_to_find][timestep]
        df = data.df

        if start_date and end_date:
            df = df[(df.index >= start_date) & (df.index <= end_date)]
        elif start_date:
            df = df[df.index >= start_date]
        elif end_date:
            df = df[df.index <= end_date]

        return df
