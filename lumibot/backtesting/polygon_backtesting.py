import logging
import traceback
from collections import defaultdict
from datetime import date, timedelta

from polygon import RESTClient
from polygon.exceptions import BadResponse
from termcolor import colored
from urllib3.exceptions import MaxRetryError

from lumibot.data_sources import PandasData
from lumibot.entities import Asset, Data
from lumibot.tools import polygon_helper

START_BUFFER = timedelta(days=5)


class PolygonDataBacktesting(PandasData):
    """
    Backtesting implementation of Polygon
    """

    def __init__(
        self,
        datetime_start,
        datetime_end,
        pandas_data=None,
        api_key=None,
        has_paid_subscription=False,
        **kwargs,
    ):
        super().__init__(
            datetime_start=datetime_start, datetime_end=datetime_end, pandas_data=pandas_data, api_key=api_key, **kwargs
        )
        self.has_paid_subscription = has_paid_subscription

        # RESTClient API for Polygon.io polygon-api-client
        self.polygon_client = RESTClient(self._api_key)

    def update_pandas_data(self, asset, quote, length, timestep, start_dt=None):
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

        Returns
        -------
        dict
            A dictionary with the keys being the asset and the values being the PandasData objects.
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
                    return None

            # Always try to get the lowest timestep possible because we can always resample
            # If day is requested then make sure we at least have data that's less than a day
            if ts_unit == "day":
                if data_timestep == "minute":
                    # Check if we have enough data (5 days is the buffer we subtracted from the start datetime)
                    if (data_start_datetime - start_datetime) < START_BUFFER:
                        return None
                    else:
                        # We don't have enough data, so we need to get more (but in minutes)
                        ts_unit = "minute"
                elif data_timestep == "hour":
                    # Check if we have enough data (5 days is the buffer we subtracted from the start datetime)
                    if (data_start_datetime - start_datetime) < START_BUFFER:
                        return None
                    else:
                        # We don't have enough data, so we need to get more (but in hours)
                        ts_unit = "hour"

            # If hour is requested then make sure we at least have data that's less than an hour
            if ts_unit == "hour":
                if data_timestep == "minute":
                    # Check if we have enough data (5 days is the buffer we subtracted from the start datetime)
                    if (data_start_datetime - start_datetime) < START_BUFFER:
                        return None
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
                has_paid_subscription=self.has_paid_subscription,
            )
        except BadResponse as e:
            # Assuming e.message or similar attribute contains the error message
            formatted_start_datetime = start_datetime.strftime("%Y-%m-%d")
            formatted_end_datetime = self.datetime_end.strftime("%Y-%m-%d")
            if "Your plan doesn't include this data timeframe" in str(e):
                error_message = colored(
                                "Polygon Access Denied: Your current plan does not support the requested data timeframe "
                                f"from {formatted_start_datetime} to {formatted_end_datetime}. "
                                "Please consider either changing your backtesting timeframe to start later since your "
                                "subscription does not allow you to backtest that far back, or upgrade your subscription "
                                "so that you can backtest further back in time. Generally speaking, the more you pay for "
                                "your subscription, the further back in time you can backtest and the faster you can get "
                                "data. "
                                "You can upgrade your Polygon subscription at https://polygon.io/pricing ", 
                                color="red")
                logging.error(error_message)
                # Optionally, inform the user through the application's UI or a notification system
                # For CLI or logs, re-raise the exception with a clearer message
                raise #Exception("Polygon Access Denied: Upgrade required for requested data timeframe.") from e
            else:
                # Handle other BadResponse exceptions not related to plan limitations
                logging.error(traceback.format_exc())
                raise
        except MaxRetryError as e:
            # TODO: Make this just sleep for a bit and retry (there's no need for people to set
            # polygon_has_paid_subscription to False)

            # Handle MaxRetriesError
            error_message = colored(
                            "Polygon Max Retries Error: The maximum number of retries has been reached. "
                            "This is probably because you do not have a paid subscription to Polygon. "
                            "The free version of Polygon has a limit on the number of requests you can make "
                            "per minute. If you are using the free version of Polygon, you should set "
                            "polygon_has_paid_subscription to False when you run the backtest() function. eg. \n"
                            "result = OptionsButterflyCondor.backtest( \n"
                            "    PolygonDataBacktesting, \n"
                            "    backtesting_start, \n"
                            "    backtesting_end, \n"
                            "    polygon_api_key=polygon_api_key, \n"
                            "    polygon_has_paid_subscription=False, # Make sure this is False! \n"
                            " ) \n"
                            "Otherwise, you should consider upgrading your subscription to Polygon to avoid this error. "
                            "You can upgrade your Polygon subscription at https://polygon.io/pricing",
                            color="red")
            logging.error(error_message)
            raise
        except Exception as e:
            # Handle all other exceptions
            logging.error(traceback.format_exc())
            raise Exception("Error getting data from Polygon") from e

        if df is None:
            return None

        pandas_data = []
        data = Data(asset_separated, df, timestep=ts_unit, quote=quote_asset)
        pandas_data.append(data)
        pandas_data_updated = self._set_pandas_data_keys(pandas_data)

        return pandas_data_updated

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
        start_dt, ts_unit = self.get_start_datetime_and_ts_unit(length, timestep, current_dt, start_buffer=START_BUFFER)

        # Get data from Polygon
        pandas_data_update = self.update_pandas_data(asset, quote, length, timestep, start_dt)

        if pandas_data_update is not None:
            # Add the keys to the self.pandas_data dictionary
            self.pandas_data.update(pandas_data_update)

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
        pandas_data_update = self.update_pandas_data(asset, quote, 1, timestep)
        if pandas_data_update is not None:
            # Add the keys to the self.pandas_data dictionary
            self.pandas_data.update(pandas_data_update)

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
            pandas_data_update = self.update_pandas_data(asset, quote, 1, timestep, dt)
            if pandas_data_update is not None:
                # Add the keys to the self.pandas_data dictionary
                self.pandas_data.update(pandas_data_update)
                self._data_store.update(pandas_data_update)
        except Exception as e:
            print(f"Error get_last_price from Polygon: {e}")

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
