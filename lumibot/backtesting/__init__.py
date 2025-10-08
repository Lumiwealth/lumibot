from .alpaca_backtesting import AlpacaBacktesting
from .alpha_vantage_backtesting import AlphaVantageBacktesting
from .backtesting_broker import BacktestingBroker
from .ccxt_backtesting import CcxtBacktesting
from .interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting
from .pandas_backtesting import PandasDataBacktesting
from .polygon_backtesting import PolygonDataBacktesting
from .thetadata_backtesting import ThetaDataBacktesting
from .yahoo_backtesting import YahooDataBacktesting

# Import DataBento backtesting
# Polars version (NEW DEFAULT - faster performance)
from lumibot.data_sources.databento_data_polars_backtesting import DataBentoDataPolarsBacktesting as DataBentoDataBacktesting

# Pandas version (stable fallback, kept for compatibility)
from .databento_backtesting import DataBentoDataBacktesting as DataBentoDataBacktestingPandas