import logging
import traceback
from collections import OrderedDict, defaultdict
from datetime import date, timedelta

from polygon.exceptions import BadResponse
from termcolor import colored

from lumibot.data_sources import PandasData
from lumibot.entities import Asset, Data
from lumibot.tools import polygon_helper
from lumibot.tools.polygon_helper import PolygonClient

START_BUFFER = timedelta(days=5)


class PolygonDataBacktesting(PandasData):
    """
    Backtesting implementation of Polygon
    """

    # Size limit for the pandas_data and _data_store (dicts of Pandas DataFrames) in bytes.
    # Set to None to disable the limit.
    MAX_STORAGE_BYTES = None

    def __init__(
        self,
        datetime_start,
        datetime_end,
        pandas_data=None,
        api_key=None,
        **kwargs,
    ):
        super().__init__(
            datetime_start=datetime_start, datetime_end=datetime_end, pandas_data=pandas_data, api_key=api_key, **kwargs
        )

        # RESTClient API for Polygon.io polygon-api-client
        self.polygon_client = PolygonClient.create(api_key=api_key)

    @staticmethod
    def _enforce_storage_limit(pandas_data: OrderedDict):
        storage_used = sum(data.df.memory_usage().sum() for data in pandas_data.values())
        logging.info(f"{storage_used = :,} bytes for {len(pandas_data)} items")
        while storage_used > PolygonDataBacktesting.MAX_STORAGE_BYTES:
            k, d = pandas_data.popitem(last=False)
            mu = d.df.memory_usage().sum()
            storage_used -= mu
            logging.info(f"Storage limit exceeded. Evicted LRU data: {k} used {mu:,} bytes")

    def _update_pandas_data(self, asset, quote, length, timestep, start_dt=None):
        """
        Get asset data and update the self.pandas_data dictionary.

        Parameters
        ----------
        asset : Asset
            The asset to get data for.
        quote : Asset
            The quote asset to use. For example, if asset is "SPY" and quote is "USD", the data will be for "SPY/USD".
        length : int
            The number of data points to get.
        timestep : str
            The timestep to use. For example, "1minute" or "1hour" or "1day".
        start_dt : datetime
            The start datetime to use. If None, the current self.start_datetime will be used.
        """
        search_asset = asset
        asset_separated = asset
        quote_asset = quote if quote is not None else Asset("USD", "forex")

        if isinstance(search_asset, tuple):
            asset_separated, quote_asset = search_asset
        else:
            search_asset = (search_asset, quote_asset)

        # Get the start datetime and timestep unit
        start_datetime, ts_unit = self.get_start_datetime_and_ts_unit(
            length, timestep, start_dt, start_buffer=START_BUFFER
        )

        # Check if we have data for this asset
        if search_asset in self.pandas_data:
            asset_data = self.pandas_data[search_asset]
            asset_data_df = asset_data.df
            data_start_datetime = asset_data_df.index[0]

            # Get the timestep of the data
            data_timestep = asset_data.timestep

            # If the timestep is the same, we don't need to update the data
            if data_timestep == ts_unit:
                # Check if we have enough data (5 days is the buffer we subtracted from the start datetime)
                if (data_start_datetime - start_datetime) < START_BUFFER:
                    return

            # Always try to get the lowest timestep possible because we can always resample
            # If day is requested then make sure we at least have data that's less than a day
            if ts_unit == "day":
                if data_timestep == "minute":
                    # Check if we have enough data (5 days is the buffer we subtracted from the start datetime)
                    if (data_start_datetime - start_datetime) < START_BUFFER:
                        return
                    else:
                        # We don't have enough data, so we need to get more (but in minutes)
                        ts_unit = "minute"
                elif data_timestep == "hour":
                    # Check if we have enough data (5 days is the buffer we subtracted from the start datetime)
                    if (data_start_datetime - start_datetime) < START_BUFFER:
                        return
                    else:
                        # We don't have enough data, so we need to get more (but in hours)
                        ts_unit = "hour"

            # If hour is requested then make sure we at least have data that's less than an hour
            if ts_unit == "hour":
                if data_timestep == "minute":
                    # Check if we have enough data (5 days is the buffer we subtracted from the start datetime)
                    if (data_start_datetime - start_datetime) < START_BUFFER:
                        return
                    else:
                        # We don't have enough data, so we need to get more (but in minutes)
                        ts_unit = "minute"

        # Download data from Polygon
        try:
            # Get data from Polygon
            df = polygon_helper.get_price_data_from_polygon(
                self._api_key,
                asset_separated,
                start_datetime,
                self.datetime_end,
                timespan=ts_unit,
                quote_asset=quote_asset,
            )
        except BadResponse as e:
            # Assuming e.message or similar attribute contains the error message
            formatted_start_datetime = start_datetime.strftime("%Y-%m-%d")
            formatted_end_datetime = self.datetime_end.strftime("%Y-%m-%d")
            if "Your plan doesn't include this data timeframe" in str(e):
                error_message = colored(
                                "Polygon Access Denied: Your subscription does not allow you to backtest that far back in time. "
                                f"You requested data for {asset_separated} {ts_unit} bars "
                                f"from {formatted_start_datetime} to {formatted_end_datetime}. "
                                "Please consider either changing your backtesting timeframe to start later since your "
                                "subscription does not allow you to backtest that far back or upgrade your Polygon "
                                "subscription."
                                "You can upgrade your Polygon subscription at at https://polygon.io/?utm_source=affiliate&utm_campaign=lumi10 "
                                "Please use the full link to give us credit for the sale, it helps support this project. "
                                "You can use the coupon code 'LUMI10' for 10% off. ",
                                color="red")
                raise Exception(error_message) from e
            elif "Unknown API Key" in str(e):
                error_message = colored(
                                "Polygon Access Denied: Your API key is invalid. "
                                "Please check your API key and try again. "
                                "You can get an API key at https://polygon.io/?utm_source=affiliate&utm_campaign=lumi10 "
                                "Please use the full link to give us credit for the sale, it helps support this project. "
                                "You can use the coupon code 'LUMI10' for 10% off. ",
                                color="red")
                raise Exception(error_message) from e
            else:
                # Handle other BadResponse exceptions not related to plan limitations
                logging.error(traceback.format_exc())
                raise
        except Exception as e:
            # Handle all other exceptions
            logging.error(traceback.format_exc())
            raise Exception("Error getting data from Polygon") from e

        if (df is None) or df.empty:
            return

        data = Data(asset_separated, df, timestep=ts_unit, quote=quote_asset)
        pandas_data_update = self._set_pandas_data_keys([data])

        # Add the keys to the self.pandas_data dictionary
        self.pandas_data.update(pandas_data_update)
        if PolygonDataBacktesting.MAX_STORAGE_BYTES:
            self._enforce_storage_limit(self.pandas_data)

    def _pull_source_symbol_bars(
        self,
        asset: Asset,
        length: int,
        timestep: str = "day",
        timeshift: int = None,
        quote: Asset = None,
        exchange: str = None,
        include_after_hours: bool = True,
    ):
        # Get the current datetime and calculate the start datetime
        current_dt = self.get_datetime()

        # Get data from Polygon
        self._update_pandas_data(asset, quote, length, timestep, current_dt)

        return super()._pull_source_symbol_bars(
            asset, length, timestep, timeshift, quote, exchange, include_after_hours
        )

    # Get pricing data for an asset for the entire backtesting period
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
        self._update_pandas_data(asset, quote, 1, timestep)

        response = super()._pull_source_symbol_bars_between_dates(
            asset, timestep, quote, exchange, include_after_hours, start_date, end_date
        )

        if response is None:
            return None

        bars = self._parse_source_symbol_bars(response, asset, quote=quote)
        return bars

    def get_last_price(self, asset, timestep="minute", quote=None, exchange=None, **kwargs):
        try:
            dt = self.get_datetime()
            self._update_pandas_data(asset, quote, 1, timestep, dt)
        except Exception as e:
            print(f"Error get_last_price from Polygon: {e}")
            print(f"Error get_last_price from Polygon: {asset=} {quote=} {timestep=} {dt=} {e}")

        return super().get_last_price(asset=asset, quote=quote, exchange=exchange)

    def get_chains(self, asset: Asset, quote: Asset = None, exchange: str = None):
        """
        Integrates the Polygon client library into the LumiBot backtest for Options Data in the same
        structure as Interactive Brokers options chain data

        Parameters
        ----------
        asset : Asset
            The underlying asset to get data for.
        quote : Asset
            The quote asset to use. For example, if asset is "SPY" and quote is "USD", the data will be for "SPY/USD".
        exchange : str
            The exchange to get the data from. Example: "SMART"

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

        # All Option Contracts | get_chains matching IBKR |
        # {'Multiplier': 100, 'Exchange': "NYSE",
        #      'Chains': {'CALL': {<date1>: [100.00, 101.00]}}, 'PUT': defaultdict(list)}}
        option_contracts = {
            "Multiplier": None,
            "Exchange": None,
            "Chains": {"CALL": defaultdict(list), "PUT": defaultdict(list)},
        }
        today = self.get_datetime().date()
        real_today = date.today()

        # All Contracts | to match lumitbot, more inputs required from get_chains()
        # If the strategy is using a recent backtest date, some contracts might not be expired yet, query those too
        expired_list = [True, False] if real_today - today <= timedelta(days=31) else [True]
        polygon_contracts = []
        for expired in expired_list:
            polygon_contracts.extend(
                list(
                    self.polygon_client.list_options_contracts(
                        underlying_ticker=asset.symbol,
                        expiration_date_gte=today,
                        expired=expired,  # Needed so BackTest can look at old contracts to find the expirations/strikes
                        limit=1000,
                    )
                )
            )

        for polygon_contract in polygon_contracts:
            # Return to Loop and Skip if Multipler is not 100 because non-standard contracts are not supported
            if polygon_contract.shares_per_contract != 100:
                continue

            # Contract Data | Attributes
            exchange = polygon_contract.primary_exchange
            right = polygon_contract.contract_type.upper()
            exp_date = polygon_contract.expiration_date  # Format: '2023-08-04'
            strike = polygon_contract.strike_price
            option_contracts["Multiplier"] = polygon_contract.shares_per_contract
            option_contracts["Exchange"] = exchange
            option_contracts["Chains"][right][exp_date].append(strike)

        return option_contracts
