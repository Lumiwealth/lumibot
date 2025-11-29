import pandas as pd
import types
import pytest
from lumibot.entities import Asset
from lumibot.data_sources.projectx_data import ProjectXData
import logging

class DummyClientDT:
    def history_retrieve_bars(self, contract_id, start_datetime, end_datetime, unit, unit_number, limit, include_partial_bar, live, is_est):
        # Return dataframe missing a single combined datetime column but having separate date/time columns
        data = {
            'date': ['2025-08-19','2025-08-19','2025-08-19'],
            'time': ['15:15:01','15:15:02','15:15:03'],
            'open': [1.0,1.1,1.2],
            'high': [1.05,1.15,1.25],
            'low': [0.95,1.05,1.15],
            'close': [1.02,1.12,1.22],
            'volume': [10,11,12],
        }
        return pd.DataFrame(data)

@pytest.fixture
def projectx_dt(monkeypatch):
    cfg = {'firm':'TEST','api_key':'KEY','username':'USER','base_url':'http://example'}
    from lumibot.data_sources import projectx_data as px_module
    monkeypatch.setattr(px_module, 'ProjectXClient', lambda _cfg: types.SimpleNamespace())
    px = ProjectXData(config=cfg)
    px.client = DummyClientDT()
    px._get_contract_id_from_asset = lambda asset: 'CONTRACT1'
    return px


def test_projectx_fetch_bars_datetime_debug(projectx_dt, caplog):
    asset = Asset(symbol='MES', asset_type='future')
    # Explicitly set logger levels to DEBUG because get_logger may default to INFO
    projectx_dt.logger.setLevel(logging.DEBUG)
    logging.getLogger('lumibot.data_sources.projectx_data').setLevel(logging.DEBUG)
    with caplog.at_level(logging.DEBUG):
        bars = projectx_dt._fetch_bars(asset=asset, length=3, timestep='minute')
    assert bars is not None
    # Ensure debugging log about datetime-related columns present
    debug_entries = [r for r in caplog.records if 'Retrieved ' in r.message]
    assert debug_entries, 'Expected debug log about retrieved bars'
    # Confirm synthesized datetime column exists
    assert bars.df.index.name == 'datetime'
    # Check datetime index type
    assert pd.api.types.is_datetime64_any_dtype(bars.df.index)

