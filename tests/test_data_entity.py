"""
Tests for the `Data` entity pricing semantics.

Contract:
- `Data.get_last_price()` is trade/bar based only (open/close from bars).
- It must NEVER fall back to bid/ask midpoint (quote/mark pricing is accessed via `get_quote()`
  / `get_price_snapshot()`).
"""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytz

from lumibot.entities import Asset
from lumibot.entities.data import Data


class TestDataGetLastPriceTradeOnly:
    def _create_data_with_prices(
        self,
        asset: Asset,
        close_prices,
        open_prices=None,
        bid_prices=None,
        ask_prices=None,
        timestep: str = "day",
    ) -> Data:
        if open_prices is None:
            open_prices = close_prices

        n = len(close_prices)
        tz = pytz.timezone("America/New_York")
        base_dt = tz.localize(datetime(2024, 1, 1, 9, 30))
        dates = [base_dt + timedelta(days=i) for i in range(n)]

        df_data = {
            "datetime": dates,
            "open": open_prices,
            "high": [
                max(o, c) if o is not None and c is not None else (o or c)
                for o, c in zip(open_prices, close_prices)
            ],
            "low": [
                min(o, c) if o is not None and c is not None else (o or c)
                for o, c in zip(open_prices, close_prices)
            ],
            "close": close_prices,
            "volume": [1000] * n,
        }

        if bid_prices is not None:
            df_data["bid"] = bid_prices
        if ask_prices is not None:
            df_data["ask"] = ask_prices

        df = pd.DataFrame(df_data).set_index("datetime")
        return Data(asset, df, timestep=timestep)

    def test_day_bars_returns_close(self):
        asset = Asset("SPY")
        close_prices = [100.0, 101.0, 102.0]
        data = self._create_data_with_prices(asset, close_prices)

        tz = pytz.timezone("America/New_York")
        dt = tz.localize(datetime(2024, 1, 3, 9, 30))
        assert data.get_last_price(dt) == 102.0

    def test_intraday_returns_open_before_bar_completion(self):
        asset = Asset("SPY")
        tz = pytz.timezone("America/New_York")
        base_dt = tz.localize(datetime(2024, 1, 2, 9, 30))
        df = (
            pd.DataFrame(
                {
                    "datetime": [base_dt, base_dt + timedelta(minutes=1)],
                    "open": [100.0, 200.0],
                    "high": [110.0, 210.0],
                    "low": [90.0, 190.0],
                    "close": [110.0, 210.0],
                    "volume": [1000, 1000],
                }
            )
            .set_index("datetime")
        )

        data = Data(asset, df, timestep="minute")
        dt = base_dt + timedelta(minutes=1)
        assert data.get_last_price(dt) == 200.0

    def test_returns_none_when_close_missing_even_with_bid_ask(self):
        asset = Asset(
            "SPY",
            asset_type="option",
            expiration=datetime(2024, 2, 1).date(),
            strike=400,
            right="CALL",
        )

        close_prices = [None, None, None]
        bid_prices = [10.0, 11.0, 12.0]
        ask_prices = [11.0, 12.0, 13.0]
        data = self._create_data_with_prices(
            asset,
            close_prices,
            open_prices=[None, None, None],
            bid_prices=bid_prices,
            ask_prices=ask_prices,
        )

        tz = pytz.timezone("America/New_York")
        dt = tz.localize(datetime(2024, 1, 3, 9, 30))
        assert data.get_last_price(dt) is None

    def test_returns_none_when_close_nan_even_with_bid_ask(self):
        asset = Asset(
            "SPY",
            asset_type="option",
            expiration=datetime(2024, 2, 1).date(),
            strike=400,
            right="CALL",
        )

        close_prices = [np.nan, np.nan, np.nan]
        bid_prices = [10.0, 11.0, 12.0]
        ask_prices = [11.0, 12.0, 13.0]
        data = self._create_data_with_prices(
            asset,
            close_prices,
            open_prices=[np.nan, np.nan, np.nan],
            bid_prices=bid_prices,
            ask_prices=ask_prices,
        )

        tz = pytz.timezone("America/New_York")
        dt = tz.localize(datetime(2024, 1, 3, 9, 30))
        assert data.get_last_price(dt) is None

    def test_prefers_close_over_bid_ask(self):
        asset = Asset(
            "SPY",
            asset_type="option",
            expiration=datetime(2024, 2, 1).date(),
            strike=400,
            right="CALL",
        )

        close_prices = [5.0, 5.0, 5.0]
        bid_prices = [10.0, 11.0, 12.0]
        ask_prices = [11.0, 12.0, 13.0]
        data = self._create_data_with_prices(
            asset,
            close_prices,
            bid_prices=bid_prices,
            ask_prices=ask_prices,
        )

        tz = pytz.timezone("America/New_York")
        dt = tz.localize(datetime(2024, 1, 3, 9, 30))
        assert data.get_last_price(dt) == 5.0
