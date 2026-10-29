"""A real historical API -> standard strategy backtest; trades are simulated."""

import pandas as pd
import pytest

from lumibot.backtesting import KalshiBacktesting
from lumibot.data_sources import KalshiData
from lumibot.entities import Asset
from lumibot.strategies import Strategy
from lumibot.tools.kalshi_client import KalshiClient

pytestmark = [pytest.mark.apitest, pytest.mark.kalshi, pytest.mark.public_http]


def test_real_archived_candles_run_through_normal_backtest(monkeypatch):
    monkeypatch.setenv("BACKTESTING_DATA_SOURCE", "kalshi")
    with KalshiClient(
        {
            "IS_DEMO": False,
            "BASE_URL": KalshiClient.PRODUCTION_URL,
            "API_KEY_ID": None,
            "PRIVATE_KEY": None,
            "PRIVATE_KEY_PATH": None,
        }
    ) as client:
        markets = client.request(
            "GET", "/historical/markets", params={"series_ticker": "KXHIGHNY", "limit": 10}, authenticated=False
        )["markets"]
        data = KalshiData(client=client)
        selected = None
        for row in markets[:5]:
            yes = Asset(row["ticker"], "prediction_contract", outcome="yes")
            end = pd.Timestamp(row["close_time"])
            frame = data.get_historical_prices(yes, None, "hour", start=end - pd.Timedelta(days=7), end=end).df
            usable = frame[(frame["ask"] > 0) & (frame["ask"] < 1)]
            if not usable.empty:
                selected = yes, usable.index[0], end
                break
        assert selected is not None, "No populated executable archived quote in the bounded market sample"
        yes, start, end = selected

        class HistoricalContractCheck(Strategy):
            def initialize(self):
                self.sleeptime = "1H"
                self.set_market("24/7")
                self.vars.submitted = False

            def on_trading_iteration(self):
                if not self.vars.submitted:
                    quote = self.get_quote(yes)
                    if quote.ask is not None and 0 < quote.ask < 1:
                        self.submit_order(self.create_order(yes, 0.25, "buy", time_in_force="ioc"))
                        self.vars.submitted = True

        try:
            result, strategy = HistoricalContractCheck.run_backtest(
                KalshiBacktesting,
                start.to_pydatetime(),
                min(end, start + pd.Timedelta(hours=4)).to_pydatetime(),
                assets=[yes],
                kalshi_data=data,
                timestep="hour",
                market="24/7",
                budget=100,
                benchmark_asset=None,
                show_plot=False,
                show_tearsheet=False,
                save_tearsheet=False,
                save_stats_file=False,
                show_progress_bar=False,
            )
            assert result is not None
            fills = [order for order in strategy.broker.get_all_orders() if order.is_filled()]
            assert len(fills) >= 1
            assert fills[0].asset == yes and fills[0].quantity == 0.25
            assert 0 < fills[0].avg_fill_price < 1
            assert fills[0].trade_cost > 0
        finally:
            data.shutdown()
