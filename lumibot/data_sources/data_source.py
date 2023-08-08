from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from lumibot import LUMIBOT_DEFAULT_PYTZ, LUMIBOT_DEFAULT_TIMEZONE
from lumibot.entities import Asset, AssetsMapping
from lumibot.tools import get_chunks, get_risk_free_rate
from lumibot.tools.black_scholes import BS

from .exceptions import UnavailabeTimestep


class DataSource:
    SOURCE = ""
    IS_BACKTESTING_DATA_SOURCE = False
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = []
    DEFAULT_TIMEZONE = LUMIBOT_DEFAULT_TIMEZONE
    DEFAULT_PYTZ = LUMIBOT_DEFAULT_PYTZ

    # ========Python datetime helpers======================

    def get_datetime(self):
        return self.to_default_timezone(datetime.now())

    def get_timestamp(self):
        return self.get_datetime().timestamp()

    def get_round_minute(self, timeshift=0):
        current = self.get_datetime().replace(second=0, microsecond=0)
        return current - timedelta(minutes=timeshift)

    def get_last_minute(self):
        return self.get_round_minute(timeshift=1)

    def get_round_day(self, timeshift=0):
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
        return (start_date, end_date)

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
        if self.IS_BACKTESTING_DATA_SOURCE and self.SOURCE == "PANDAS":
            return self._timestep
        else:
            return self.MIN_TIMESTEP

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

    def _pull_source_bars(
        self, assets, length, timestep=MIN_TIMESTEP, timeshift=None, quote=None,  include_after_hours=True
    ):
        pass

    def _parse_source_symbol_bars(self, response, asset, quote=None, length=None):
        pass

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

    def get_last_price(self, asset, timestep=None, quote=None, exchange=None, **kwargs):
        """Takes an asset and returns the last known price"""
        pass

    def get_last_prices(
        self, assets, timestep=None, quote=None, exchange=None, **kwargs
    ):
        """Takes a list of assets and returns the last known prices"""
        if timestep is None:
            timestep = self.MIN_TIMESTEP

        result = {}
        for asset in assets:
            result[asset] = self.get_last_price(
                asset, timestep=timestep, quote=quote, exchange=exchange, **kwargs
            )

        if self.SOURCE == "CCXT":
            return result
        else:
            return AssetsMapping(result)

    def is_tradable(self, asset, dt, length=1, timestep="minute", timeshift=0):
        # Check if an asset is tradable at this moment.
        raise NotImplementedError(self.__class__.__name__ + ".is_tradable")

    def get_tradable_assets(self, dt, length=1, timestep="minute", timeshift=0):
        # Return a list of tradable assets.
        raise NotImplementedError(self.__class__.__name__ + ".get_tradable_assets")

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
        implied_volatility=False,
        delta=False,
        option_price=False,
        pv_dividend=False,
        gamma=False,
        vega=False,
        theta=False,
        underlying_price=False,
    ):
        """Returns Greeks in backtesting. """
        underlying_asset = Asset(symbol=asset.symbol, asset_type="stock")
        und_price = self.get_last_price(underlying_asset)

        opt_price = self.get_last_price(asset)

        interest = get_risk_free_rate() * 100
        current_date = self.get_datetime().date()
        
        # If asset expiration is a datetime object, convert it to date
        expiration = asset.expiration
        if type(expiration) == datetime:
            expiration = expiration.date()
        
        days_to_expiration = (expiration - current_date).days
        if asset.right == "CALL":
            iv = BS(
                [und_price, float(asset.strike), interest, days_to_expiration],
                callPrice=opt_price,
            )
        elif asset.right == "PUT":
            iv = BS(
                [und_price, float(asset.strike), interest, days_to_expiration],
                putPrice=opt_price,
            )

        c = BS(
            [und_price, float(asset.strike), interest, days_to_expiration],
            volatility=iv.impliedVolatility,
        )

        is_call = True if asset.right == "CALL" else False

        result = dict(
            implied_volatility=iv.impliedVolatility,
            delta=c.callDelta if is_call else c.putDelta,
            option_price=c.callPrice if is_call else c.putPrice,
            pv_dividend=None,  # (No equiv )
            gamma=c.gamma,
            vega=c.vega,
            theta=c.callTheta if is_call else c.putTheta,
            underlying_price=und_price,
        )

        greeks = dict()
        for greek, value in result.items():
            if eval(greek):
                greeks[greek] = value

        if len(greeks) == 0:
            greeks = result

        return greeks
