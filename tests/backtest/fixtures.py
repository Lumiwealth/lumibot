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
        return generate_test_data(asset, start_date, end_date, timestep)

    with patch('pandas.read_feather', side_effect=custom_read_feather) as mock:
        yield mock

@pytest.fixture
def mock_polygon_client():
    with patch('lumibot.tools.polygon_helper.RESTClient') as MockClient:
        mock_instance = MockClient.return_value
        mock_instance.get_aggs.return_value = {}
        mock_instance.get_aggs.side_effect = Exception("get_aggs polygon REST API was called")

        mock_instance.list_splits.return_value = iter([])
        mock_instance._get.return_value = MagicMock()
        yield mock_instance


@pytest.fixture
def mock_should_load_from_cache():
    with patch('lumibot.tools.polygon_helper.should_load_from_cache', return_value=True) as mock:
        yield mock

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
    asset_type = config_params['asset'].asset_type
    start = config_params['start']
    end = config_params['end']

    backtesting_start = start
    backtesting_end = end
    risk_free_rate = 0.0532
    trading_fee = TradingFee(percent_fee=0.0033, flat_fee=0.0)
    market = None

    if asset_type == "stock":
        benchmark_asset = Asset(symbol="SPY", asset_type="stock")
        quote_asset = Asset(symbol="USD", asset_type="forex")
        market = "NYSE"
        
    elif asset_type == "crypto":
        benchmark_asset = Asset(symbol="BTC", asset_type="crypto")
        quote_asset = Asset(symbol="USD", asset_type="forex")
        market = "24/7"

    else:
        benchmark_asset = Asset(symbol="BTC", asset_type="crypto")
        quote_asset = Asset(symbol="USD", asset_type="forex")
        market = "24/7"


    asset = benchmark_asset

    data_source = PolygonDataBacktesting(
        datetime_start=backtesting_start,
        datetime_end=backtesting_end,
        api_key="your_polygon_api_key",
        has_paid_subscription=True,
        timestep=timestep
    )

    broker = BacktestingBroker(data_source)

    strategy = BuyEachIterationStrategy(
        asset=asset,
        market=market,
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

# TODO: Implement calendar option for removing data when market is closed
def generate_test_data(asset, start_date, end_date, timestep, market=None):
    freq = {'minute': 'min', 'hour': 'H', 'day': 'D', 'week': 'W', 'month': 'M'}.get(timestep, 'D')
    
    date_range = pd.date_range(start=start_date, end=end_date, freq=freq)
    
    total_points = len(date_range)
    increment = 100.0 / total_points
    
    return pd.DataFrame({
        "datetime": date_range,
        "open": [10000.0 + increment * i for i in range(total_points)],
        "high": [11000.0 + increment * i for i in range(total_points)],
        "low": [9000.0 + increment * i for i in range(total_points)],
        "close": [10000.0 + increment * i for i in range(total_points)],
        "volume": [100000 + 100 * increment * i for i in range(total_points)],
    })
def cache_needs_update(cache_file):
    """Check if the cache file needs to be updated."""
    last_modified = datetime.fromtimestamp(cache_file.stat().st_mtime)
    return datetime.now() - last_modified > timedelta(days=1)  # Example: Update if older than 1 day



### Test Strategies ###

class BuyEachIterationStrategy(Strategy):
    def __init__(self, *args, asset=None, market=None, sleeptime=None, **kwargs):
        super().__init__(*args, **kwargs)

        if not asset:
            raise Exception("No asset in BuyEachIterationStrategy")
        if not market:
            raise Exception("no market in BuyEachIterationStrategy")
        if not sleeptime:
            raise Exception("no sleeptime in BuyEachIterationStrategy")
        
        self.sleeptime = sleeptime
        self.asset = asset
        self.set_market(market)
        
        

    def on_trading_iteration(self):
        historical_prices = self.get_historical_prices(self.asset, length=10)
        self.order = self.create_order(self.asset, 1, "buy")
        self.submit_order(self.order)