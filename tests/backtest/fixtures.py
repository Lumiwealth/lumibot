import pytest
import datetime
from lumibot.backtesting import PolygonDataBacktesting

@pytest.fixture
def polygon_data_backtesting():
    datetime_start = datetime.datetime(2023, 1, 1)
    datetime_end = datetime.datetime(2023, 2, 1)
    api_key = "fake_api_key"
    pandas_data = {}
    
    polygon_data_instance = PolygonDataBacktesting(
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        pandas_data=pandas_data,
        api_key=api_key,
    )
    
    return polygon_data_instance
