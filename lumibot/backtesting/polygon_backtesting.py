from datetime import timedelta

from lumibot.data_sources import PandasData
from lumibot.entities import Asset, Data
from lumibot.tools import polygon_helper

from .data_source_backtesting import DataSourceBacktesting


class PolygonDataBacktesting(DataSourceBacktesting, PandasData):
    """
    Backtesting implementation of Polygon

    Parameters
    ----------
    data_source : PandasData
        The data source to use for backtesting.
    """

    def __init__(
        self,
        datetime_start,
        datetime_end,
        pandas_data=None,
        polygon_api_key=None,
        polygon_has_paid_subscription=False,
        **kwargs,
    ):
        self.LIVE_DATA_SOURCE = PandasData
        self.polygon_api_key = polygon_api_key
        self.has_paid_subscription = polygon_has_paid_subscription
        PandasData.__init__(self, pandas_data, **kwargs)
        DataSourceBacktesting.__init__(self, datetime_start, datetime_end)

    def convert_timestep_str_to_timedelta(self, timestep):
        """
        Convert a timestep string to a timedelta object. For example, "1minute" will be converted to a timedelta of 1 minute.

        Parameters
        ----------
        timestep : str
            The timestep string to convert. For example, "1minute" or "1hour" or "1day".

        Returns
        -------
        timedelta
            A timedelta object representing the timestep.
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
                    unit = timestep[i:]
                    break
        else:
            unit = timestep

        # Check if the unit is valid
        if unit in time_unit_map:
            # Convert quantity to minutes
            quantity_in_minutes = quantity * time_unit_map[unit]
            # Convert minutes to timedelta
            delta = timedelta(minutes=quantity_in_minutes)
            return delta
        else:
            raise ValueError(
                f"Unknown unit: {unit}. Valid units are minute, hour, day, M, H, D"
            )

    def update_pandas_data(self, asset, quote, length, timestep, polygon_helper):
        """
        Get asset data and update the self.pandas_data dictionary.

        Parameters
        ----------
        asset : Asset
            The asset to get data for.
        length : int
            The number of data points to get.
        timestep : str
            The timestep to use. For example, "1minute" or "1hour" or "1day".
        polygon_helper : PolygonHelper
            The PolygonHelper object to use to get data from Polygon.

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

        # Check if we have data for this asset
        if search_asset not in self.pandas_data:
            # Download data from Polygon
            try:
                # Convert timestep string to timedelta and get start datetime
                td = self.convert_timestep_str_to_timedelta(timestep) * length
                # Multiply td by length to get the end datetime
                start_datetime = self.datetime_start - td

                # Subtract an extra 5 days to the start datetime to make sure we have enough
                # data when it's a sparsely traded asset, especially over weekends
                start_datetime = start_datetime - timedelta(days=5)

                # Get data from Polygon
                df = polygon_helper.get_price_data_from_polygon(
                    self.polygon_api_key,
                    asset_separated,
                    start_datetime,
                    self.datetime_end,
                    timespan=timestep,
                    quote_asset=quote_asset,
                    has_paid_subscription=self.has_paid_subscription,
                )
            except Exception as e:
                raise Exception(f"Error getting data from Polygon: {e}")

            if df is None:
                return None

            pandas_data = []
            data = Data(asset_separated, df, timestep=timestep, quote=quote_asset)
            pandas_data.append(data)
            pandas_data_updated = self._set_pandas_data_keys(pandas_data)

            return pandas_data_updated

        return None

    def _pull_source_symbol_bars(
        self,
        asset,
        length,
        timestep=None,
        timeshift=None,
        quote=None,
        exchange=None,
        include_after_hours=True,
    ):
        pandas_data_update = self.update_pandas_data(
            asset, quote, length, timestep, polygon_helper
        )

        if pandas_data_update is not None:
            # Add the keys to the self.pandas_data dictionary
            self.pandas_data.update(pandas_data_update)

        return super()._pull_source_symbol_bars(
            asset, length, timestep, timeshift, quote, exchange, include_after_hours
        )

    def get_last_price(
        self, asset, timestep="minute", quote=None, exchange=None, **kwargs
    ):
        try:
            pandas_data_update = self.update_pandas_data(
                asset, quote, 1, timestep, polygon_helper
            )
            if pandas_data_update is not None:
                # Add the keys to the self.pandas_data dictionary
                self.pandas_data.update(pandas_data_update)
        except Exception as e:
            print(f"Error get_last_price from Polygon: {e}")

        return super().get_last_price(asset, timestep, quote, exchange, **kwargs)
