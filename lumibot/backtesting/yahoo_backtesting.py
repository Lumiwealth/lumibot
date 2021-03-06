from lumibot.data_sources import YahooData

from .data_source_backtesting import DataSourceBacktesting

YahooDataBacktesting = DataSourceBacktesting.factory(YahooData)
