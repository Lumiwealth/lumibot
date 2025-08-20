import pandas as pd
from types import SimpleNamespace

from lumibot.data_sources.projectx_data import ProjectXData
from lumibot.entities.asset import Asset

class DummyClient:
    def history_retrieve_bars(self, contract_id, start_datetime, end_datetime, unit, unit_number, limit, include_partial_bar, live, is_est):
        # Return small dummy DataFrame similar to ProjectX output
        data = {
            'date': ['2025-08-19','2025-08-19','2025-08-19'],
            'time': ['15:32:19','15:32:20','15:32:21'],
            'open': [1.0,1.1,1.2],
            'high': [1.1,1.2,1.3],
            'low': [0.9,1.0,1.1],
            'close': [1.05,1.15,1.25],
            'volume': [10,20,30],
        }
        return pd.DataFrame(data)

    def get_contracts(self):
        return []

class DummyProjectX(ProjectXData):
    def __init__(self):
        self.client = DummyClient()
        self.logger = SimpleNamespace(debug=lambda *a,**k: None, error=lambda *a,**k: None, warning=lambda *a,**k: None)

    def _get_contract_id_from_asset(self, asset):
        return 'TEST'

    def _refresh_contract_metadata(self):
        pass


def test_projectx_datetime_index():
    ds = DummyProjectX()
    asset = Asset(symbol='ES', asset_type='future')
    bars = ds.get_historical_prices(asset, length=3, timestep='minute')
    df = bars.df
    assert isinstance(df.index, pd.DatetimeIndex)
    assert df.index.name == 'datetime'
    # Ensure sorted and timezone aware
    assert df.index.is_monotonic_increasing
    assert df.index.tz is not None
