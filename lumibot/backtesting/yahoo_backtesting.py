from lumibot.data_sources import YahooData


class YahooDataBacktesting(YahooData):
    """
    YahooDataBacktesting is a DataSourceBacktesting that uses YahooData as a
    backtesting data source.
    """

    def __init__(self, datetime_start, datetime_end, **kwargs):
        YahooData.__init__(self, datetime_start, datetime_end, **kwargs)
