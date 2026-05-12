from datetime import datetime

import pytz

from lumibot.entities import Asset
from lumibot.tools import thetadata_helper


def test_get_trading_dates_inverted_window_returns_empty():
    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    start = datetime(2025, 1, 2, tzinfo=pytz.UTC)
    end = datetime(2025, 1, 1, tzinfo=pytz.UTC)

    assert thetadata_helper.get_trading_dates(asset, start, end) == []
