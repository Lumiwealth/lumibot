import datetime
import math

import pandas as pd

from lumibot.entities import Asset, Bars

from .data_source import DataSource


class InteractiveBrokersData(DataSource):
    """Make Interactive Brokers connection and gets data.

    Create connection to Interactive Brokers market through either Gateway or TWS
    which must be running locally for connection to be made.
    """

    SOURCE = "InteractiveBrokers"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {
            "timestep": "second",
            "representations": [
                "1 secs",
            ],
        },
        {
            "timestep": "minute",
            "representations": [
                "1 min",
            ],
        },
        {
            "timestep": "5 minutes",
            "representations": [
                "5 mins",
            ],
        },
        {
            "timestep": "10 minutes",
            "representations": [
                "10 mins",
            ],
        },
        {
            "timestep": "15 minutes",
            "representations": [
                "15 mins",
            ],
        },
        {
            "timestep": "30 minutes",
            "representations": [
                "30 mins",
            ],
        },
        {
            "timestep": "1 hour",
            "representations": [
                "1 hour",
            ],
        },
        {
            "timestep": "2 hours",
            "representations": [
                "2 hours",
            ],
        },
        {
            "timestep": "4 hours",
            "representations": [
                "4 hours",
            ],
        },
        {
            "timestep": "day",
            "representations": [
                "1 day",
            ],
        },
    ]

    def __init__(self, config, max_workers=20, chunk_size=100, **kwargs):
        super().__init__(**kwargs)
        self.name = "interactivebrokers"
        self.max_workers = min(max_workers, 200)
        self.chunk_size = min(chunk_size, 100)
        self.ib = None

    @staticmethod
    def _format_datetime(dt):
        return pd.Timestamp(dt).isoformat()

    @staticmethod
    def _format_ib_datetime(dt):
        return pd.Timestamp(dt).strftime("%Y%m%d %H:%M:%S")

    def _parse_duration(self, length, timestep):
        # If the timestemp includes a number, then separate it from the unit.
        if timestep[0].isdigit():
            x = timestep.split(" ")
            digit = int(x[0])
            freq = x[1].lower()
        else:
            digit = 1
            freq = timestep.lower()

        # Converts length and timestep into IB `durationStr`
        if "second" in freq:
            # IB has a max for seconds of 86400.
            return f"{str(min(length, 86400))} S"
        elif "minute" in freq:
            # IB does not allow minutes to be used as a duration.
            divisor = 24 * 60 / digit
            num_days = length / divisor
            # Add a buffer to the number of days because of market hours
            num_days = num_days * 2
            return f"{str(math.ceil(num_days))} D"
        elif "hour" in freq:
            # IB does not allow hours to be used as a duration.
            divisor = 24 / digit
            num_days = length / divisor
            # Add a buffer to the number of days because of market hours
            num_days = num_days * 2
            return f"{str(math.ceil(num_days))} D"
        elif "day" in freq:
            return f"{str(length)} D"
        else:
            raise ValueError(f"Timestep must be `day` or `minute`, you entered: {timestep}")

    def _pull_source_symbol_bars(
        self,
        asset,
        length,
        timestep=MIN_TIMESTEP,
        timeshift=None,
        quote=None,
        exchange=None,
        include_after_hours=True,
    ):
        """pull broker bars for a given asset"""
        response = self._pull_source_bars(
            [asset],
            length,
            timestep=timestep,
            timeshift=timeshift,
            quote=quote,
            exchange=exchange,
            include_after_hours=include_after_hours,
        )
        return response.get(asset)

    def _pull_source_bars(
        self,
        assets,
        length,
        timestep=MIN_TIMESTEP,
        timeshift=None,
        quote=None,
        exchange=None,
        include_after_hours=True,
    ):
        """pull broker bars for a list assets"""

        if exchange is None:
            exchange = "SMART"

        response = dict()

        parsed_timestep = self._parse_source_timestep(timestep, reverse=True)

        if timeshift:
            end = datetime.datetime.now() - timeshift
            end = self.to_default_timezone(end)
            end_date_time = self._format_ib_datetime(end)
            type = "TRADES"
        else:
            end_date_time = ""
            type = "TRADES"

        # Call data.
        reqId = 0
        for asset in assets:
            get_data_attempt = 0
            max_attempts = 2
            # Two attempts to retreive data are possible, one short, then one longer,
            # If no data is returned, than a dataframe with `0` in each row is returned.
            while get_data_attempt < max_attempts:
                reqId += 1
                result = self.ib.get_historical_data(
                    reqId,
                    asset,
                    end_date_time,
                    self._parse_duration(length, timestep),
                    parsed_timestep,
                    type,
                    0 if include_after_hours else 1,  # useRTH
                    2,
                    False,
                    [],
                    exchange=exchange,
                )

                df = pd.DataFrame(result)
                cols = [
                    "date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "barCount",
                    "average",
                ]
                try:
                    df = df[cols]
                    get_data_attempt = max_attempts
                except:
                    get_data_attempt += 1
                    # Add one day in minutes.
                    length += 1339
                    continue

                # Return dataframe with zeros if no historical data.
                if df.empty:
                    response[asset] = pd.DataFrame(
                        data=[[0, 0, 0, 0, 0, 0, 0, 0]],
                        columns=cols,
                    )
                    continue

                if "min" in parsed_timestep or "hour" in parsed_timestep:
                    df["date"] = (
                        pd.to_datetime(df["date"].astype(int), unit="s", origin="unix")
                        .dt.tz_localize("UTC")
                        .dt.tz_convert(self.DEFAULT_TIMEZONE)
                    )
                    df = df.iloc[-int(length) :, :]
                elif "day" in parsed_timestep:
                    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
                    df["date"] = df["date"].dt.tz_localize(self.DEFAULT_TIMEZONE)

                    df = df.iloc[-int(length) :, :]

                response[asset] = df
        return response

    def _parse_source_symbol_bars(self, response, asset, quote=None, length=None):
        # Catch empty dataframe.
        if isinstance(response, float) or response.empty:
            bars = Bars(response, self.SOURCE, asset, raw=response)
            return bars
        df = response.copy()
        # df["date"] = pd.to_datetime(df["date"], unit='s')
        df = df.rename(columns={"date": "datetime"})  # Renaming to match other data sources like BackTest and Alpaca.
        df = df.set_index("datetime")
        df["price_change"] = df["close"].pct_change()
        df["dividend"] = 0
        df["stock_splits"] = 0
        df["dividend_yield"] = df["dividend"] / df["close"]
        df["return"] = df["dividend_yield"] + df["price_change"]
        # df = df.tail(length)
        df = df[
            [
                "open",
                "high",
                "low",
                "close",
                "volume",
                "price_change",
                "dividend",
                "stock_splits",
                "dividend_yield",
                "return",
            ]
        ]

        bars = Bars(df, self.SOURCE, asset, raw=response, quote=quote)
        return bars

    def _start_realtime_bars(self, asset, keep_bars=12):
        return self.ib.start_realtime_bars(asset=asset, keep_bars=keep_bars)

    def _get_realtime_bars(self, asset):
        rtb = self.ib.realtime_bars[asset]
        if len(rtb) == 0:
            return None
        else:
            return rtb

    def _cancel_realtime_bars(self, asset):
        self.ib.cancel_realtime_bars(asset)
        return 0

    def get_chains(self, asset: Asset, quote: Asset = None, exchange: str = None):
        """
        For InteractiveBrokers, this function is defined in the broker because the broker object has access
        to additional API calls that are not available in the data source object because of the way IBClient and
        IBWrapper are designed.
        """
        raise NotImplementedError(
            "Lumibot InteractiveBrokersData does not support get_chains options data. However, this is defined in"
            "the InteractiveBroker broker class as it has access to IBClient and IBWrapper functionality not available"
            "here. If you need this feature, please use call the broker object method directly"
        )

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

    def get_last_price(self, asset, timestep=None, quote=None, exchange=None, **kwargs):
        if exchange is None:
            exchange = "SMART"

        response = dict()
        get_data_attempt = 0
        max_attempts = 2
        while get_data_attempt < max_attempts:
            try:
                if "should_use_last_close" in kwargs:
                    should_use_last_close = kwargs["should_use_last_close"]
                else:
                    should_use_last_close = True
                result = self.ib.get_tick(
                    asset,
                    exchange=exchange,
                    should_use_last_close=should_use_last_close,
                )
                if result:
                    response[asset] = result[0]
                    break
                get_data_attempt += 1
            except:
                get_data_attempt += 1
        if asset not in response:
            response[asset] = None
        return response[asset]

    def _get_tick(self, asset):
        result = self.ib.get_tick(asset, greek=False)
        return result

    def get_yesterday_dividend(self, asset, quote=None):
        """Unavailable"""
        return 0

    def get_yesterday_dividends(self, asset, quote=None):
        """Unavailable"""
        return None
