from lumibot.data_sources import YahooData

from .data_source_backtesting import DataSourceBacktesting


class YahooDataBacktesting(DataSourceBacktesting, YahooData):
    """
    YahooDataBacktesting is a DataSourceBacktesting that uses YahooData as a
    backtesting data source.
    """

    def __init__(self, datetime_start, datetime_end, **kwargs):
        self.LIVE_DATA_SOURCE = YahooData
        YahooData.__init__(self, **kwargs)
        DataSourceBacktesting.__init__(self, datetime_start, datetime_end)
