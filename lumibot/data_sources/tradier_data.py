import logging
from datetime import datetime, timedelta

from lumibot.entities import Bars
from lumibot.tools.helpers import create_options_symbol, parse_timestep_qty_and_unit
from lumiwealth_tradier import Tradier

from .data_source import DataSource


class TradierAPIError(Exception):
    pass


class TradierData(DataSource):
    MIN_TIMESTEP = "minute"
    SOURCE = "Tradier"
    TIMESTEP_MAPPING = [
        {
            "timestep": "tick",
            "representations": [
                "tick",
            ],
        },
        {
            "timestep": "minute",
            "representations": [
                "minute",
            ],
        },
        {
            "timestep": "day",
            "representations": [
                "daily",
            ],
        },
        {
            "timestep": "week",
            "representations": [
                "weekly",
            ],
        },
        {
            "timestep": "month",
            "representations": [
                "monthly",
            ],
        },
    ]

    def __init__(self, account_number, access_token, paper=True, max_workers=20):
        super().__init__(api_key=access_token)
        self._account_number = account_number
        self._paper = paper
        self.max_workers = min(max_workers, 50)
        self.tradier = Tradier(account_number, access_token, paper)

    def get_historical_prices(
        self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True
    ):
        """
        Get bars for a given asset

        Parameters
        ----------
        asset : Asset
            The asset to get the bars for.
        length : int
            The number of bars to get.
        timestep : str
            The timestep to get the bars at. For example, "minute" or "day".
        timeshift : datetime.timedelta
            The amount of time to shift the bars by. For example, if you want the bars from 1 hour ago to now,
            you would set timeshift to 1 hour.
        quote : Asset
            The quote asset to get the bars for.
        exchange : str
            The exchange to get the bars for.
        include_after_hours : bool
            Whether to include after hours data.
        """

        timestep = timestep if timestep else self.MIN_TIMESTEP

        # Parse the timestep
        timestep_qty, timestep_unit = parse_timestep_qty_and_unit(timestep)

        parsed_timestep_unit = self._parse_source_timestep(timestep_unit, reverse=True)

        if asset.asset_type == "option":
            symbol = create_options_symbol(
                asset.symbol,
                asset.expiration,
                asset.right,
                asset.strike,
            )
        else:
            symbol = asset.symbol

        # Calculate the end date
        end_date = datetime.now()
        if timeshift:
            end_date = end_date - timeshift

        # Calculate the start date
        td, _ = self.convert_timestep_str_to_timedelta(timestep)
        start_date = end_date - (td * length)

        # Check what timestep we are using, different endpoints are required for different timesteps
        try:
            if parsed_timestep_unit == "minute":
                df = self.tradier.market.get_timesales(
                    symbol,
                    interval=timestep_qty,
                    start_date=start_date,
                    end_date=end_date,
                    session_filter="all" if include_after_hours else "open",
                )
            else:
                df = self.tradier.market.get_historical_quotes(
                    symbol,
                    interval=parsed_timestep_unit,
                    start_date=start_date,
                    end_date=end_date,
                    session_filter="all" if include_after_hours else "open",
                )
        except Exception as e:
            logging.error(f"Error getting historical prices for {symbol}: {e}")
            return None

        # Drop the "time" and "timestamp" columns if they exist
        if "time" in df.columns:
            df = df.drop(columns=["time"])
        if "timestamp" in df.columns:
            df = df.drop(columns=["timestamp"])

        # Convert the dataframe to a Bars object
        bars = Bars(df, self.SOURCE, asset, raw=df, quote=quote)

        return bars

    def get_last_price(self, asset, quote=None, exchange=None):
        """
        This function returns the last price of an asset.
        Parameters
        ----------
        asset
        quote
        exchange

        Returns
        -------
        float
           Price of the asset
        """

        if asset.asset_type == "option":
            symbol = create_options_symbol(
                asset.symbol,
                asset.expiration,
                asset.right,
                asset.strike,
            )
        else:
            symbol = asset.symbol

        price = self.tradier.market.get_last_price(symbol)
        return price
