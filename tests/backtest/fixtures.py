import pytest
from unittest.mock import patch, MagicMock
import datetime
from lumibot.backtesting import BacktestingBroker, PolygonDataBacktesting
from lumibot.tools.polygon_helper import build_cache_filename
from lumibot.entities import Asset, TradingFee
from lumibot.traders import Trader
from lumibot.strategies import Strategy
import pandas as pd
from datetime import datetime, timedelta

current_test_params = {}

@pytest.fixture
def mock_pd_read_feather(request):
    asset = request.param.get('asset', Asset(symbol="BTC", asset_type="crypto"))
    timestep = request.param.get('timestep', 'day')
    start_date = request.param.get('start', datetime(2023, 1, 1))
    end_date = request.param.get('end', datetime(2023, 4, 1))

    def custom_read_feather(_):
        return generate_test_data(start_date, end_date, timestep)

    with patch('pandas.read_feather', side_effect=custom_read_feather) as mock:
        yield mock

@pytest.fixture
def mock_polygon_client():
    with patch('polygon.RESTClient') as MockClient:

        def get_aggs_side_effect(*args, **kwargs):
            raise Exception("Bad request error")
        
        mock_instance = MockClient.return_value
        mock_instance.get_aggs.return_value = {}
        mock_instance.get_aggs.side_effect = get_aggs_side_effect

        mock_instance.list_splits.return_value = iter([])
        mock_instance._get.return_value = MagicMock()
        yield mock_instance

@pytest.fixture
def mock_validate_cache():
    with patch('lumibot.tools.polygon_helper.validate_cache', return_value=False) as mock_method:
        yield mock_method


# TODO: Parameters!
@pytest.fixture
def polygon_data_backtesting():
    datetime_start = datetime(2023, 1, 1)
    datetime_end = datetime(2023, 2, 1)
    api_key = "fake_api_key"
    pandas_data = {}
    
    polygon_data_instance = PolygonDataBacktesting(
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        pandas_data=pandas_data,
        api_key=api_key,
        has_paid_subscription=False
    )
    
    return polygon_data_instance

@pytest.fixture
def backtest_environment(request):
    config_params = request.param
    sleeptime = config_params['sleeptime']
    timestep = config_params['timestep']
    asset_type = config_params['asset_type']
    start = config_params['start']
    end = config_params['end']

    backtesting_start = start
    backtesting_end = end
    risk_free_rate = 0.0532
    trading_fee = TradingFee(percent_fee=0.0033, flat_fee=0.0)


    if asset_type == "stock":
        benchmark_asset = Asset(symbol="SPY", asset_type="stock")
        quote_asset = Asset(symbol="USD", asset_type="forex")
        
    else:
        benchmark_asset = Asset(symbol="BTC", asset_type="crypto")
        quote_asset = Asset(symbol="USD", asset_type="forex")

    asset = benchmark_asset

    # For now only support sleeptime = "1D"
    strategy = DailyStrategy

    data_source = PolygonDataBacktesting(
        datetime_start=backtesting_start,
        datetime_end=backtesting_end,
        api_key="your_polygon_api_key",
        has_paid_subscription=True,
        timestep=timestep
    )

    broker = BacktestingBroker(data_source)

    strategy = strategy(
        asset=asset,
        sleeptime=sleeptime,
        broker=broker,
        risk_free_rate=risk_free_rate,
        benchmark_asset=benchmark_asset,
        quote_asset=quote_asset,
        buy_trading_fees=[trading_fee],
        sell_trading_fees=[trading_fee],

    )

    trader = Trader(backtest=True)
    trader.add_strategy(strategy)

    return trader


#### helper functions ####

def generate_test_data(start_date, end_date, timestep):
    freq = {'minute': 'min', 'hour': 'H', 'day': 'D', 'week': 'W', 'month': 'M'}.get(timestep, 'D')
    date_range = pd.date_range(start=start_date, end=end_date, freq=freq)
    return pd.DataFrame({
        "datetime": date_range,
        "open": [100 + i for i in range(len(date_range))],
        "high": [110 + i for i in range(len(date_range))],
        "low": [90 + i for i in range(len(date_range))],
        "close": [100 + i for i in range(len(date_range))],
        "volume": [1000 + 100 * i for i in range(len(date_range))],
    })

def cache_needs_update(cache_file):
    """Check if the cache file needs to be updated."""
    last_modified = datetime.fromtimestamp(cache_file.stat().st_mtime)
    return datetime.now() - last_modified > timedelta(days=1)  # Example: Update if older than 1 day



### Test Strategies ###

class DailyStrategy(Strategy):
    def initialize(self, asset):
        self.asset = asset
        self.sleeptime = "1D"

    def on_trading_iteration(self):
        self.order = self.create_order(self.asset, 1, "buy")
        self.submit_order(self.order)