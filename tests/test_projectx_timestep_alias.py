import types
import pandas as pd
import pytest
from datetime import datetime, timedelta

from lumibot.entities import Asset
from lumibot.data_sources.projectx_data import ProjectXData

class DummyClient:
    def history_retrieve_bars(self, contract_id, start_datetime, end_datetime, unit, unit_number, limit, include_partial_bar, live, is_est):
        # Return a minimal DataFrame resembling expected structure
        idx = pd.date_range(end=end_datetime, periods=unit_number, freq='min')
        data = {
            'open': [1.0]*len(idx),
            'high': [1.1]*len(idx),
            'low': [0.9]*len(idx),
            'close': [1.05]*len(idx),
            'volume': [100]*len(idx),
        }
        df = pd.DataFrame(data, index=idx)
        df.index.name = 'datetime'
        return df.reset_index()

@pytest.fixture
def projectx(monkeypatch):
    cfg = {
        'firm': 'TEST',
        'api_key': 'KEY',
        'username': 'USER',
        'base_url': 'http://example'
    }
    # Monkeypatch ProjectXClient before instantiation to avoid real auth/network
    from lumibot.data_sources import projectx_data as px_module
    monkeypatch.setattr(px_module, 'ProjectXClient', lambda _cfg: types.SimpleNamespace())
    px = ProjectXData(config=cfg)
    # Inject dummy client with history retrieval
    px.client = DummyClient()
    # Stub contract id resolver
    px._get_contract_id_from_asset = lambda asset: 'CONTRACT1'
    return px


def test_projectx_get_bars_accepts_timestep_alias(projectx):
    asset = Asset(symbol='ES', asset_type='future')
    # Base DataSource.get_bars returns a mapping asset->Bars
    bars_map = projectx.get_bars(asset, 1, timestep='minute')
    assert isinstance(bars_map, dict)
    bars = list(bars_map.values())[0]
    assert bars is not None and hasattr(bars, 'df') and not bars.df.empty
    # Repeat call to ensure idempotent alias handling
    bars_map2 = projectx.get_bars(asset, 1, timestep='minute')
    bars2 = list(bars_map2.values())[0]
    assert bars2 is not None and not bars2.df.empty
