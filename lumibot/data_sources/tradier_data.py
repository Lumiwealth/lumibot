import logging
from collections import defaultdict
import datetime as dt
from decimal import Decimal
from typing import Union
import pytz

import pandas as pd

from lumibot import LUMIBOT_DEFAULT_PYTZ, LUMIBOT_DEFAULT_TIMEZONE
from lumibot.entities import Asset, Bars, Quote
from lumibot.tools.helpers import (
    create_options_symbol,
    parse_timestep_qty_and_unit,
    get_trading_days,
    date_n_trading_days_from_date
)
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

    def __init__(
            self,
            account_number: str,
            access_token: str,
            paper: bool = True,
            max_workers: int = 20,
            delay: int = None,
            tzinfo: pytz.timezone = pytz.timezone(LUMIBOT_DEFAULT_TIMEZONE),
            remove_incomplete_current_bar: bool = False,
            **kwargs
    ) -> None:
        """
        Initializes the trading account with the specified parameters.

        Parameters:
        - account_number (str): The account number used for accessing the trading account.
        - access_token (str): The access token for authenticating requests.
        - paper (bool, optional): Indicates whether to use the paper trading environment.
          Defaults to True.
        - max_workers (int, optional): The maximum number of workers for parallel processing.
          Defaults to 20.
        - delay (int, optional): A delay parameter to control how many minutes to delay non-crypto data for.
          If not specified, uses DATA_SOURCE_DELAY environment variable or defaults to 0.
        - tzinfo (pytz.timezone, optional): Timezone for data adjustments. Determines how datetime objects
          are adjusted when retrieving historical data. Defaults to the `LUMIBOT_DEFAULT_TIMEZONE`.
        - remove_incomplete_current_bar (bool, optional): Default False.
          Whether to remove the incomplete current bar from the data.
          Tradier includes incomplete bars for the current bar (ie: it gives you a daily bar for the current day even if
          the day isn't over yet). Some Lumibot users night not expect that, so this option will remove the incomplete
          bar from the data.

        Returns:
        - None
        """

        super().__init__(api_key=access_token, delay=delay, tzinfo=tzinfo)
        self._account_number = account_number
        self._paper = paper
        self.max_workers = min(max_workers, 50)
        self.tradier = Tradier(account_number, access_token, paper)
        self._remove_incomplete_current_bar = remove_incomplete_current_bar

    def _sanitize_base_and_quote_asset(self, base_asset, quote_asset) -> tuple[Asset, Asset]:
        if isinstance(base_asset, tuple):
            quote = base_asset[1]
            asset = base_asset[0]
        else:
            asset = base_asset
            quote = quote_asset

        if isinstance(asset, str):
            raise NotImplementedError(f"TradierData doesn't support string assets like: {asset} yet.")

        return asset, quote

    def get_chains(self, asset: Asset, quote: Asset = None, exchange: str = None):
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
        exchange: str | None
            The exchange to get the option chains for

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
        df_chains = self.tradier.market.get_option_expirations(asset.symbol)
        if not isinstance(df_chains, pd.DataFrame) or df_chains.empty:
            raise LookupError(f"Could not find Tradier option chains for {asset.symbol}")

        # Tradier doesn't report multiple exchanges, just use SMART
        multiplier = int(df_chains.contract_size.mode()[0])  # Use most common, should always be 100
        chains = {"Multiplier": multiplier, "Exchange": "unknown",
                  "Chains": {"CALL": defaultdict(list), "PUT": defaultdict(list)}}
        for row in df_chains.reset_index().to_dict("records"):
            exp_date = row["date"].strftime('%Y-%m-%d')
            chains["Chains"]["CALL"][exp_date] = row["strikes"]
            chains["Chains"]["PUT"][exp_date] = row["strikes"]

        return chains

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
        expiry : str | dt.datetime | dt.date
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
            This is not necessary for Tradier as all option data is returned in a single query.
        strike_max : float
            The maximum strike price to return in the chain. If None, will return all strikes.
            This is not necessary for Tradier as all option data is returned in a single query.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the full chain information for the option asset. Greeks columns will be named as
            'greeks.delta', 'greeks.theta', etc.
        """
        df = self.tradier.market.get_option_chains(asset.symbol, expiry, greeks=True)

        # Filter the dataframe by strike_min and strike_max
        if strike_min is not None:
            df = df[df.strike >= strike_min]
        if strike_max is not None:
            df = df[df.strike <= strike_max]

        return df

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
            The timestep to get the bars at. Accepts "day" or "minute".
        timeshift : dt.timedelta
            The amount of time to shift the bars by. For example, if you want the bars from 1 hour ago to now,
            you would set timeshift to 1 hour.
        quote : Asset
            The quote asset to get the bars for.
        exchange : str
            The exchange to get the bars for.
        include_after_hours : bool
            Whether to include after hours data.
        """
        asset, quote = self._sanitize_base_and_quote_asset(asset, quote)
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

        # Create end time
        now = dt.datetime.now(self.tzinfo)
        if self._delay:
            end_dt = now - self._delay
        else:
            end_dt = now

        if timeshift is not None:
            if not isinstance(timeshift, dt.timedelta):
                raise TypeError("timeshift must be a timedelta")
            end_dt = end_dt - timeshift

        if timestep == 'day':
            days_needed = length
        else:
            # For minute bars, calculate additional days needed accounting for weekends/holidays
            minutes_per_day = 390  # ~6.5 hours of trading per day
            days_needed = (length // minutes_per_day) + 1

        start_date = date_n_trading_days_from_date(
            n_days=days_needed,
            start_datetime=end_dt,
            # TODO: pass market into DataSource
            # This works for now. Crypto gets more bars but throws them out.
            market='NYSE'
        )
        start_dt = self.tzinfo.localize(dt.datetime.combine(start_date, dt.datetime.min.time()))

        # Check what timestep we are using, different endpoints are required for different timesteps
        try:
            if parsed_timestep_unit == "minute":
                df = self.tradier.market.get_timesales(
                    symbol,
                    interval=timestep_qty,
                    start_date=start_dt,
                    end_date=end_dt,
                    session_filter="all" if include_after_hours else "open",
                )
            else:
                df = self.tradier.market.get_historical_quotes(
                    symbol,
                    interval=parsed_timestep_unit,
                    start_date=start_dt,
                    end_date=end_dt,
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

        # If the index contains date objects, convert and handle timezone
        if isinstance(df.index[0], dt.date):  # Check if the index contains date objects
            df.index = pd.to_datetime(df.index)  # Always ensure it's a DatetimeIndex

            # Check if the index is timezone-naive or already timezone-aware
            if df.index.tz is None:  # Naive index, localize to data source timezone
                df.index = df.index.tz_localize(self.tzinfo)
            else:  # Already timezone-aware, convert to data source timezone
                df.index = df.index.tz_convert(self.tzinfo)

        # Check for incomplete bars
        if self._remove_incomplete_current_bar:
            if timestep == "minute":
                # For minute bars, remove the current minute
                current_minute = now.replace(second=0, microsecond=0)
                df = df[df.index < current_minute]
            else:
                # For daily bars, remove today's bar if market is open
                current_date = now.date()
                df = df[df.index.date < current_date]

        # Ensure df only contains the last N bars
        if len(df) > length:
            df = df.iloc[-length:]

        # Convert the dataframe to a Bars object
        bars = Bars(df, self.SOURCE, asset, raw=df, quote=quote)

        return bars

    def get_last_price(self, asset, quote=None, exchange=None) -> Union[float, Decimal, None]:
        """
        This function returns the last price of an asset.
        Parameters
        ----------
        asset: Asset
            The asset to get the last price for
        quote: Asset
            The quote asset to get the last price for (currently not used for Tradier)
        exchange: str
            The exchange to get the last price for (currently not used for Tradier)

        Returns
        -------
        float or Decimal or none
           Price of the asset
        """
        asset, quote = self._sanitize_base_and_quote_asset(asset, quote)

        symbol = None
        try:
            if asset.asset_type == "option":
                symbol = create_options_symbol(
                    asset.symbol,
                    asset.expiration,
                    asset.right,
                    asset.strike,
                )
            elif asset.asset_type == "index":
                symbol = f"I:{asset.symbol}"
            else:
                symbol = asset.symbol

            price = self.tradier.market.get_last_price(symbol)
            return price

        except Exception as e:
            logging.error(f"Error getting last price for {symbol or asset.symbol}: {e}")
            return None

    def get_quote(self, asset, quote=None, exchange=None) -> Quote:
        """
        This function returns the quote of an asset.
        Parameters
        ----------
        asset: Asset
            The asset to get the quote for
        quote: Asset
            The quote asset to get the quote for (currently not used for Tradier)
        exchange: str
            The exchange to get the quote for (currently not used for Tradier)

        Returns
        -------
        Quote
           Quote object containing bid, ask, last price and other information
        """

        asset, quote = self._sanitize_base_and_quote_asset(asset, quote)

        if asset.asset_type == "option":
            symbol = create_options_symbol(
                asset.symbol,
                asset.expiration,
                asset.right,
                asset.strike,
            )
        else:
            symbol = asset.symbol

        quotes_df = self.tradier.market.get_quotes([symbol])

        # If the dataframe is empty, return an empty Quote
        if quotes_df is None or quotes_df.empty:
            return Quote(asset=asset)

        # Get the quote from the dataframe
        quote_dict = quotes_df.iloc[0].to_dict()

        # Extract relevant fields for the Quote object
        return Quote(
            asset=asset,
            price=quote_dict.get('last'),
            bid=quote_dict.get('bid'),
            ask=quote_dict.get('ask'),
            volume=quote_dict.get('volume'),
            timestamp=dt.datetime.now(pytz.UTC),
            bid_size=quote_dict.get('bidsize'),
            ask_size=quote_dict.get('asksize'),
            change=quote_dict.get('change'),
            percent_change=quote_dict.get('change_percentage'),
            raw_data=quote_dict
        )

    def query_greeks(self, asset: Asset):
        """
        This function returns the greeks of an option as reported by the Tradier API.

        Parameters
        ----------
        asset : Asset
            The option asset to get the greeks for.

        Returns
        -------
        dict
            A dictionary containing the greeks of the option.
        """
        greeks = {}
        stock_symbol = asset.symbol
        expiration = asset.expiration
        option_symbol = create_options_symbol(stock_symbol, expiration, asset.right, asset.strike)
        df_chains = self.tradier.market.get_option_chains(stock_symbol, expiration, greeks=True)
        df = df_chains[df_chains["symbol"] == option_symbol]
        if df.empty:
            return {}

        for col in [x for x in df.columns if 'greeks' in x]:
            greek_name = col.replace('greeks.', '')
            greeks[greek_name] = df[col].iloc[0]
        return greeks
