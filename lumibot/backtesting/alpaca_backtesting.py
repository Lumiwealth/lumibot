import logging
from datetime import datetime

import pandas as pd

from lumibot.entities import Asset
from lumibot.backtesting.base_pandas_backtesting import BasePandasBacktesting


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
        Abstract method to fetch data from the specific source.
        Subclasses must implement this.
        """
        raise NotImplementedError("Subclasses must implement `_fetch_data_from_source`.")

    def _handle_api_errors(self, exception):
        """
        Handle any errors specific to Alpaca's API.
        Placeholder for error handling.
        """
        logging.error(f"Error while fetching data: {exception}")
        # Add logic to handle specific errors such as rate limits, missing permissions, etc.
