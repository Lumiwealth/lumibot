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
