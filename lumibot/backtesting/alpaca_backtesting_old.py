import logging
from datetime import datetime

import pandas as pd
from alpaca.data.historical import CryptoHistoricalDataClient, StockHistoricalDataClient
from alpaca.data.requests import (
    CryptoBarsRequest,
    StockBarsRequest,
)
from alpaca.data.timeframe import TimeFrame

from lumibot.entities import Asset
from lumibot.backtesting.base_pandas_backtesting import BasePandasBacktesting

logger = logging.getLogger(__name__)


class AlpacaBacktesting(BasePandasBacktesting):
    """
    Backtesting implementation for the Alpaca data source.
    Inherits common functionality from BasePandasBacktesting and implements
    custom logic for Alpaca-specific integration.
    """

    def __init__(
            self,
            datetime_start,
            datetime_end,
            max_memory=None,
            config=None,
            **kwargs
    ):
        super().__init__(datetime_start=datetime_start, datetime_end=datetime_end, max_memory=max_memory, **kwargs)

        if isinstance(config, dict) and "API_KEY" in config:
            api_key = config["API_KEY"]
        elif hasattr(config, "API_KEY"):
            api_key = config.API_KEY
        else:
            raise ValueError("API_KEY not found in config")

        if isinstance(config, dict) and "API_SECRET" in config:
            api_secret = config["API_SECRET"]
        elif hasattr(config, "API_SECRET"):
            api_secret = config.API_SECRET
        else:
            raise ValueError("API_SECRET not found in config")

        self._stock_client = StockHistoricalDataClient(api_key, api_secret)
        self._crypto_client = CryptoHistoricalDataClient(api_key, api_secret)

    # noinspection PyMethodMayBeStatic
    def alpaca_timeframe_from_timestep(self, timestep: str) -> TimeFrame:
        """Convert a timestep string to an Alpaca TimeFrame."""
        if timestep == 'day':
            return TimeFrame.Day
        elif timestep == 'minute':
            return TimeFrame.Minute
        else:
            raise ValueError(f"Unsupported timestep: {timestep}")

    def _fetch_data_from_source(
            self,
            *,
            base_asset: Asset,
            quote_asset: Asset,
            start_datetime: datetime,
            end_datetime: datetime,
            timestep: str = "minute",
            **kwargs
    ) -> pd.DataFrame:
        """
        Fetches historical market data based on the provided asset details and parameters.

        This function retrieves data for either stocks or cryptocurrencies, utilizing the
        Alpaca client APIs. The data retrieval is based on the asset type, symbol, and time
        range provided. It formats the resulting data into a pandas DataFrame. If no data is
        available for the requested parameters, an empty DataFrame is returned.

        Notes: Alpaca end dates are inclusive.

        Args:
            base_asset (Asset): The primary asset for which data is being requested. Must include
                the asset type and symbol.
            quote_asset (Asset): The secondary asset used in conjunction with `base_asset` for
                cryptocurrencies. Ignored for stocks.
            start_datetime (datetime): The starting datetime for the data retrieval period.
            end_datetime (datetime): The ending datetime for the data retrieval period.
            timestep (str): The time interval for the data points. Defaults to "minute". Must
                align with Alpaca-supported timeframes.
            **kwargs: Additional keyword arguments that may be passed to the data extraction
                clients.

        Returns:
            pd.DataFrame: A DataFrame containing the historical market data. The DataFrame will
            include the following columns depending on the returned data from Alpaca:
            - timestamp: Timestamps for each data point.
            - open: Opening prices.
            - high: High prices.
            - low: Low prices.
            - close: Closing prices.
            - volume: Trading volume.

        Raises:
            ValueError: If the `base_asset.asset_type` is unsupported. The supported types are
            "stock" and "crypto".
        """

        if base_asset.asset_type.lower() == "stock":
            ticker = base_asset.symbol
        elif base_asset.asset_type.lower() == "crypto":
            ticker = f"{base_asset.symbol}/{quote_asset.symbol}"
        else:
            raise ValueError(f"Unsupported asset type: {base_asset.asset_type}")

        if '/' in ticker:
            logger.debug(f"Downloading crypto data for {ticker} from Alpaca.")

            # noinspection PyArgumentList
            request_params = CryptoBarsRequest(
                symbol_or_symbols=ticker,
                timeframe=self.alpaca_timeframe_from_timestep(timestep),
                start=start_datetime,
                end=end_datetime,
            )
            bars = self._crypto_client.get_crypto_bars(request_params)
        else:
            logger.debug(f"Downloading stock data for {ticker} from Alpaca.")
            # noinspection PyArgumentList
            request_params = StockBarsRequest(
                symbol_or_symbols=ticker,
                timeframe=self.alpaca_timeframe_from_timestep(timestep),
                start=start_datetime,
                end=end_datetime,
            )
            bars = self._stock_client.get_stock_bars(request_params)

        df = bars.df.reset_index()
        if df.empty:
            logger.debug(f"No data found for {request_params.symbol_or_symbols}.")
            return pd.DataFrame()
        else:
            return df

    def _handle_api_errors(self, exception):
        """
        Handle any errors specific to Alpaca's API.
        Placeholder for error handling.
        """
        logging.error(f"Error while fetching data: {exception}")
        # Add logic to handle specific errors such as rate limits, missing permissions, etc.
