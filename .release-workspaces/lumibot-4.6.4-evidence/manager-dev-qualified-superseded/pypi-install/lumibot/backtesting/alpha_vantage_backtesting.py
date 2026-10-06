from lumibot.data_sources import AlphaVantageData, DataSourceBacktesting


class AlphaVantageBacktesting(DataSourceBacktesting, AlphaVantageData):
    def __init__(self, datetime_start, datetime_end, **kwargs):
        raise Exception("AlphaVantageBacktesting is not currently operational")

        AlphaVantageData.__init__(self, **kwargs)
        DataSourceBacktesting.__init__(self, datetime_start, datetime_end)
