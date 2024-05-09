from datetime import datetime, timedelta
from lumibot.entities import Asset
from unittest.mock import MagicMock, patch
import pytest
from .fixtures import *

@pytest.mark.parametrize("backtest_environment", [
    {'sleeptime': "1D", 
     'timestep': "day",
     'asset_type': "crypto",
     'start': datetime(2023, 1, 1),
     'end': datetime(2023, 4, 1),
     'asset': Asset(symbol="BTC", asset_type="crypto")  # Agregar el asset aquí si necesitas pasarlo al fixture
    }
], indirect=True)
@pytest.mark.parametrize('mock_pd_read_feather', [
    {'asset': Asset(symbol="BTC", asset_type="crypto"),
     'timestep': 'day',
     'start': datetime(2023, 1, 1) - timedelta(days=6),
     'end': datetime(2023, 4, 1)}
], indirect=True)
@pytest.mark.filterwarnings("error")
def test_polygon_1D_day_crypto(backtest_environment, mock_polygon_client, mock_validate_cache, mock_pd_read_feather):
    results = None
    try:
        results = backtest_environment.run_all(show_plot=False, show_tearsheet=False, save_tearsheet=False)
        assert results is not None, "Results should not be None"
    except Exception as e:
        pytest.fail(f"An unexpected exception was raised: {e}")

@pytest.mark.parametrize("backtest_environment", [
    {'sleeptime': "1D", 
     'timestep': "minute",
     'asset_type': "crypto",
     'start': datetime(2023, 1, 1),
     'end': datetime(2023, 4, 1),
     'asset': Asset(symbol="BTC", asset_type="crypto")  # Agregar el asset aquí si necesitas pasarlo al fixture
    }
], indirect=True)
@pytest.mark.parametrize('mock_pd_read_feather', [
    {'asset': Asset(symbol="BTC", asset_type="crypto"),
     'timestep': 'minute',
     'start': datetime(2023, 1, 1) - timedelta(days=6),
     'end': datetime(2023, 4, 1)}
], indirect=True)
@pytest.mark.filterwarnings("error")
def test_polygon_1D_minute_crypto(backtest_environment, mock_polygon_client, mock_validate_cache, mock_pd_read_feather):
    results = None
    try:
        results = backtest_environment.run_all(show_plot=False, show_tearsheet=False, save_tearsheet=False)
        assert results is not None, "Results should not be None"
    except Exception as e:
        pytest.fail(f"An unexpected exception was raised: {e}")


def test_pull_source_symbol_bars_with_api_call(polygon_data_backtesting, mocker):
    """Test that polygon_helper.get_price_data_from_polygon() is called with the right parameters"""
    
    # Only simulate first date
    mocker.patch.object(
        polygon_data_backtesting,
        'get_datetime',
        return_value=polygon_data_backtesting.datetime_start
    )

    mocked_get_price_data = mocker.patch(
        'lumibot.tools.polygon_helper.get_price_data_from_polygon',
        return_value=MagicMock()
    )
    
    asset = Asset(symbol="AAPL", asset_type="stock")
    quote = Asset(symbol="USD", asset_type="forex")
    length = 10
    timestep = "day"
    START_BUFFER = timedelta(days=5)

    with patch('lumibot.backtesting.polygon_backtesting.START_BUFFER', new=START_BUFFER):
        polygon_data_backtesting._pull_source_symbol_bars(
            asset=asset,
            length=length,
            timestep=timestep,
            quote=quote
        )

        mocked_get_price_data.assert_called_once()
        call_args = mocked_get_price_data.call_args
        
        expected_start_date = polygon_data_backtesting.datetime_start - timedelta(days=length) - START_BUFFER
        
        assert call_args[0][0] == polygon_data_backtesting._api_key
        assert call_args[0][1] == asset
        assert call_args[0][2] == expected_start_date
        assert call_args[0][3] == polygon_data_backtesting.datetime_end
        assert call_args[1]["timespan"] == timestep
        assert call_args[1]["quote_asset"] == quote
        assert call_args[1]["has_paid_subscription"] == polygon_data_backtesting.has_paid_subscription