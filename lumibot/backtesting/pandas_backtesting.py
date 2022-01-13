from lumibot.data_sources import PandasData

from .data_source_backtesting import DataSourceBacktesting


class PandasDataBacktesting(DataSourceBacktesting, PandasData):
    """
    Backtesting implementation of the PandasData class.

    Parameters
    ----------
    data_source : PandasData
        The data source to use for backtesting.
    """
    def __init__(self, datetime_start, datetime_end, pandas_data=None, **kwargs):
        self.LIVE_DATA_SOURCE = PandasData
        PandasData.__init__(self, pandas_data, **kwargs)
        DataSourceBacktesting.__init__(self, datetime_start, datetime_end)

    def get_data(self, start_date, end_date, **kwargs):
        """
        Get the data from the data source.

        Parameters
        ----------
        start_date : datetime.datetime
            The start date of the data to get.
        end_date : datetime.datetime
            The end date of the data to get.
        kwargs : dict
            Additional arguments to pass to the data source.

        Returns
        -------
        pandas.DataFrame
            The data.
        """
        return self.data_source.get_data(start_date, end_date, **kwargs)
