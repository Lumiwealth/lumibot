from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from lumibot import LUMIBOT_DEFAULT_PYTZ, LUMIBOT_DEFAULT_TIMEZONE
from lumibot.entities import Asset, AssetsMapping
from lumibot.tools import black_scholes, get_chunks

from .exceptions import UnavailabeTimestep


class DataSource(ABC):
    SOURCE = ""
    IS_BACKTESTING_DATA_SOURCE = False
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = []
    DEFAULT_TIMEZONE = LUMIBOT_DEFAULT_TIMEZONE
    DEFAULT_PYTZ = LUMIBOT_DEFAULT_PYTZ

    def __init__(self, api_key=None):
        self.name = "data_source"
        self._timestep = None
        self._api_key = api_key

    # ========Required Implementations ======================
    @abstractmethod
    def _pull_source_symbol_bars(
            self,
            asset,
            length,
            timestep=MIN_TIMESTEP,
            timeshift=None,
            quote=None,
            exchange=None,
            include_after_hours=True
    ):
        """pull source bars for a given asset"""
        pass

    @abstractmethod
    def _pull_source_bars(
            self, assets, length, timestep=MIN_TIMESTEP, timeshift=None, quote=None, include_after_hours=True
    ):
        pass

    @abstractmethod
    def _parse_source_symbol_bars(self, response, asset, quote=None, length=None):
        pass

    @abstractmethod
    def get_last_price(self, asset, quote=None, exchange=None):
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
        float
            The last known price of the asset.
        """
        pass

    # ========Python datetime helpers======================

    def get_datetime(self):
        """
        Returns the current datetime in the default timezone

        Returns
        -------
        datetime
        """
        return self.to_default_timezone(datetime.now())

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

    @classmethod
    def localize_datetime(cls, dt):
        if dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None:
            return cls.to_default_timezone(dt)
        else:
            return cls.DEFAULT_PYTZ.localize(dt, is_dst=None)

    @classmethod
    def to_default_timezone(cls, dt):
        return dt.astimezone(cls.DEFAULT_PYTZ)

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
                    unit = timestep[i:].strip().rstrip('s')  # Remove extra whitespace and IBKR's extra pluralization
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
            raise ValueError(
                f"Unknown unit: {unit}. Valid units are minute, hour, day, M, H, D"
            )

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
            include_after_hours=include_after_hours
        )
        if isinstance(response, float):
            return response
        elif response is None:
            return None

        bars = self._parse_source_symbol_bars(response, asset, quote=quote, length=length)
        return bars

    def get_bars(
        self,
        assets,
        length,
        timestep="minute",
        timeshift=None,
        chunk_size=100,
        max_workers=200,
        quote=None,
        exchange=None,
        include_after_hours=True
    ):
        """Get bars for the list of assets"""
        assets = [Asset(symbol=a) if isinstance(a, str) else a for a in assets]

        chunks = get_chunks(assets, chunk_size)
        with ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix=f"{self.name}_requesting_data"
        ) as executor:
            tasks = []
            func = lambda args, kwargs: self._pull_source_bars(*args, **kwargs)
            kwargs = dict(
                timestep=timestep, timeshift=timeshift, quote=quote, exchange=exchange, include_after_hours=include_after_hours
            )
            kwargs = {k: v for k, v in kwargs.items() if v is not None}
            for chunk in chunks:
                tasks.append(executor.submit(func, (chunk, length), kwargs))

            result = {}
            for task in as_completed(tasks):
                response = task.result()
                parsed = self._parse_source_bars(response, quote=quote)
                result = {**result, **parsed}

        return result

    def get_last_prices(self, assets, quote=None, exchange=None):
        """Takes a list of assets and returns the last known prices"""

        result = {}
        for asset in assets:
            result[asset] = self.get_last_price(
                asset, quote=quote, exchange=exchange
            )

        if self.SOURCE == "CCXT":
            return result
        else:
            return AssetsMapping(result)

    def get_yesterday_dividend(self, asset, quote=None):
        """Return dividend per share for a given
        asset for the day before"""
        bars = self.get_historical_prices(
            asset, 1, timestep="day"
        )
        return bars.get_last_dividend()

    def get_yesterday_dividends(self, assets, quote=None):
        """Return dividend per share for a list of
        assets for the day before"""
        result = {}
        assets_bars = self.get_bars(
            assets, 1, timestep="day", quote=quote
        )
        for asset, bars in assets_bars.items():
            if bars is not None:
                result[asset] = bars.get_last_dividend()

        return AssetsMapping(result)

    def get_greeks(
        self,
        asset,

        # API Querying for prices and rates are expensive, so we'll pass them in as arguments most of the time
        asset_price: float,
        underlying_price: float,
        risk_free_rate: float,
    ):
        """Returns Greeks in backtesting. """
        opt_price = asset_price
        und_price = underlying_price
        interest = risk_free_rate * 100
        current_date = self.get_datetime().date()
        
        # If asset expiration is a datetime object, convert it to date
        expiration = asset.expiration
        if isinstance(expiration, datetime):
            expiration = expiration.date()
        
        days_to_expiration = (expiration - current_date).days
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
