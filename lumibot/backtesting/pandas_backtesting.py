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

    def __init__(self, data_source):
        DataSourceBacktesting.__init__(self, data_source)
        PandasData.__init__(self, data_source.data_source)

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
