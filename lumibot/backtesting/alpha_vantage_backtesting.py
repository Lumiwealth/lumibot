from lumibot.data_sources import AlphaVantageData

from .data_source_backtesting import DataSourceBacktesting


class AlphaVantageBacktesting(DataSourceBacktesting, AlphaVantageData):
    def __init__(self, datetime_start, datetime_end, **kwargs):
        raise Exception("AlphaVantageBacktesting is not currently operational")

        self.LIVE_DATA_SOURCE = AlphaVantageData
        AlphaVantageData.__init__(self, **kwargs)
        DataSourceBacktesting.__init__(self, datetime_start, datetime_end)
