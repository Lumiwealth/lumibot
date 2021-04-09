from datetime import datetime
import asyncio
import nest_asyncio
from ib_insync import *
import pandas as pd

from lumibot.entities import Bars

from .data_source import DataSource

nest_asyncio.apply()

class InteractiveBrokersData(DataSource):
    """Make Interactive Brokers connection and gets data.

    Create connection to Interactive Brokers market through either Gateway or TWS
    which must be running locally for connection to be made.
    """
    SOURCE = "InteractiveBrokers"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {
            "timestep": "minute",
            "representations": [
                "1 min",
            ],
        },
        {
            "timestep": "day",
            "representations": [
                "1 day",
            ],
        },
    ]

    @staticmethod
    def _format_datetime(dt):
        return pd.Timestamp(dt).isoformat()

    def __init__(self, config, max_workers=20, chunk_size=100, **kwargs):
        self.name = "interactivebrokers"
        self.max_workers = min(max_workers, 200)

        # When requesting data for assets for example,
        # if there is too many assets, the best thing to do would
        # be to split it into chunks and request data for each chunk
        self.chunk_size = min(chunk_size, 100)

        # Connection to interactive brokers
        self.config = config
        self.socket_port = config.SOCKET_PORT
        self.client_id = config.CLIENT_ID
        self.ip = config.IP
        self.api = IB()



    def _pull_source_symbol_bars(
        self, symbol, length, timestep=MIN_TIMESTEP, timeshift=None
    ):
        """pull broker bars for a given symbol"""
        response = asyncio.run(
            self._pull_source_bars(
                [symbol],
                length=length,
                timestep=timestep,
                timeshift=timeshift,
            )
        )

        return response[symbol]

    async def _pull_source_bars(
            self,
            symbols,
            length,
            timestep=MIN_TIMESTEP,
            timeshift=None,
    ):
        """pull broker bars for a list symbols"""
        parsed_timestep = self._parse_source_timestep(timestep, reverse=True)
        parsed_duration = self._parse_duration(length, timestep)
        kwargs = dict(limit=length)

        if timeshift:
            end = datetime.now() - timeshift
            end = self.to_default_timezone(end)
            kwargs["end"] = self._format_datetime(end)
        else:
            end = ""

        with await self.api.connectAsync(
                self.ip, self.socket_port, clientId=self.client_id
        ):
            contracts = [Stock(symbol, "SMART", "USD") for symbol in symbols]
            bars_dict = {}
            all_bars = await asyncio.gather(
                *[
                    self.api.reqHistoricalDataAsync(
                        contract,
                        endDateTime="",
                        durationStr=parsed_duration,
                        barSizeSetting=parsed_timestep,
                        whatToShow="ADJUSTED_LAST",
                        useRTH=True,
                    )
                    for contract in contracts
                ]
            )
            for contract, bars in zip(contracts, all_bars):
                # Convert to dataframes.
                bars_dict[contract.symbol] = util.df(bars)

            return bars_dict

    def _parse_source_symbol_bars(self, response, symbol):
        df = response.copy()
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        df["price_change"] = df["close"].pct_change()
        df["dividend"] = 0
        df["stock_splits"] = 0
        df["dividend_yield"] = df["dividend"] / df["close"]
        df["return"] = df["dividend_yield"] + df["price_change"]

        df = df[['open', 'high', 'low', 'close', 'volume', "price_change", "dividend",
                 "stock_splits","dividend_yield","return",]]
        bars = Bars(df, self.SOURCE, symbol, raw=response)
        return bars

    def _parse_duration(self, length, timestep):
        # Converts length and timestep into IB `durationStr`
        if timestep == "minute":
            # IB has a max for seconds of 86400.
            return f"{str(min(length * 60, 86400))} S"
        elif timestep == "day":
            return f"{str(length)} D"

        raise ValueError(f"Timestep must be `day` or `minute`, you entered: {timestep}")


    def get_bars(self, symbols, length, timestep="", timeshift=None, **kwargs):
        """Get bars for the list of symbols"""

        if not timestep:
            timestep = self.MIN_TIMESTEP

        result = asyncio.run(
            self._pull_source_bars(
                symbols,
                length=length,
                timestep=timestep,
                timeshift=timeshift,
            )
        )

        parsed = self._parse_source_bars(result)
        result = {**result, **parsed}

        return result

