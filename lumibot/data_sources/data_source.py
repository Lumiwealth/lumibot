import logging
import os
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import traceback
import time
from decimal import Decimal
from typing import Union
import pytz

import pandas as pd

from lumibot import LUMIBOT_DEFAULT_PYTZ, LUMIBOT_DEFAULT_TIMEZONE
from lumibot.entities import Asset, AssetsMapping, Bars, Quote
from lumibot.tools import black_scholes, create_options_symbol

from .exceptions import UnavailabeTimestep


class DataSource(ABC):
    SOURCE = ""
    IS_BACKTESTING_DATA_SOURCE = False
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = []
    DEFAULT_TIMEZONE = LUMIBOT_DEFAULT_TIMEZONE
    DEFAULT_PYTZ = LUMIBOT_DEFAULT_PYTZ

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

        self._delay = timedelta(minutes=delay) if delay is not None else None

        if tzinfo is None:
            tzinfo = pytz.timezone(self.DEFAULT_TIMEZONE)
        self.tzinfo = tzinfo

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
        dictionary of dictionary
            Format:
            - `Multiplier` (str) eg: `100`
            - 'Chains' - paired Expiration/Strike info to guarentee that the strikes are valid for the specific
                         expiration date.
                         Format:
                           chains['Chains']['CALL'][exp_date] = [strike1, strike2, ...]
                         Expiration Date Format: 2023-07-31
        """
        pass

    @abstractmethod
    def get_historical_prices(
        self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True
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

        Returns
        -------
        Bars
            The bars for the asset.
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
            return delta, unit
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
    ):
        """Get bars for the list of assets"""
        if not isinstance(assets, list):
            assets = [assets]

        def process_chunk(chunk):
            chunk_result = {}
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
                    time.sleep(0.1)
                except Exception as e:
                    # Log once per asset to avoid spamming with a huge traceback
                    logging.warning(f"Error retrieving data for {base_asset.symbol}: {e}")
                    tb = traceback.format_exc()
                    logging.warning(tb)  # This prints the traceback
                    chunk_result[asset] = None
            return chunk_result

        # Convert strings to Asset objects
        assets = [Asset(symbol=a) if isinstance(a, str) else a for a in assets]

        # Chunk the assets
        chunks = [assets[i : i + chunk_size] for i in range(0, len(assets), chunk_size)]

        results = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
            for future in as_completed(futures):
                results.update(future.result())

        return results

    def get_last_prices(self, assets, quote=None, exchange=None):
        """Takes a list of assets and returns the last known prices"""

        result = {}
        for asset in assets:
            result[asset] = self.get_last_price(asset, quote=quote, exchange=exchange)

        if self.SOURCE == "CCXT":
            return result
        else:
            return AssetsMapping(result)

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

    def get_yesterday_dividends(self, assets, quote=None):
        """Return dividend per share for a list of
        assets for the day before"""
        result = {}
        assets_bars = self.get_bars(assets, 1, timestep="day", quote=quote)
        for asset, bars in assets_bars.items():
            if bars is not None:
                result[asset] = bars.get_last_dividend()

        return AssetsMapping(result)

    def get_chain_full_info(self, asset: Asset, expiry: str, chains=None, underlying_price=float, risk_free_rate=float,
                            strike_min=None, strike_max=None) -> pd.DataFrame:
        """
        Get the full chain information for an option asset, including: greeks, bid/ask, open_interest, etc. For
        brokers that do not support this, greeks will be calculated locally. For brokers like Tradier this function
        is much faster as only a single API call can be done to return the data for all options simultaneously.

        Parameters
        ----------
        asset : Asset
            The option asset to get the chain information for.
        expiry : str | datetime.datetime | datetime.date
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
        expiry_dt = datetime.strptime(expiry, "%Y-%m-%d") if isinstance(expiry, str) else expiry
        expiry_str = expiry_dt.strftime("%Y-%m-%d")
        if chains is None:
            chains = self.get_chains(asset)

        rows = []
        query_total = 0
        for right in chains["Chains"]:
            for strike in chains["Chains"][right][expiry_str]:
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
                option_symbol = create_options_symbol(opt_asset.symbol, expiry_dt, right, strike)
                opt_price = self.get_last_price(opt_asset)
                greeks = self.calculate_greeks(opt_asset, opt_price, underlying_price, risk_free_rate)
                query_total += time.perf_counter() - query_t

                # Build the row. Match the Tradier column naming conventions.
                row = {
                    "symbol": option_symbol,
                    "last": opt_price,
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

        logging.info(f"Chain Full Info Query Total: {query_total:.2f}s. "
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
        opt_price = asset_price
        und_price = underlying_price
        interest = risk_free_rate * 100
        current_date = self.get_datetime()

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

        return greeks

    def query_greeks(self, asset):
        """Query for the Greeks as it can be more accurate than calculating locally."""
        logging.info(f"Querying Options Greeks for {asset.symbol} is not supported for this "
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
