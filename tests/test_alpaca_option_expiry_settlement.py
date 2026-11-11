"""Expired options settle in Alpaca backtests the same way they do on pandas-based sources.

Before 2026-11-11 `process_expired_option_contracts` returned unless the data source was
named "PANDAS", so on Alpaca an expired contract stayed in the portfolio forever at its
last print, and orders on it stayed open. Settlement prices off the underlying's real
close, so trade-only option data does not block it.
"""

from datetime import date, datetime

import lumibot.backtesting.alpaca_backtesting as ab
from lumibot.entities import Asset
from lumibot.strategies import Strategy
from tests.test_alpaca_backtesting_multitimeframe_unit import (
    _NY_TZ,
    _FakeOptionClient,
    _FakeStockClient,
    _FakeTradingClient,
    _bars,
    _spy_contract_pages,
    _spy_minute_bars,
)

_DAYS = ["2026-08-03", "2026-08-04", "2026-08-05", "2026-08-06", "2026-08-07", "2026-08-10", "2026-08-11", "2026-08-12"]
# SPY minute bars run 640.00 -> 643.89 each day, so the 08-07 close is 643.89.
_OPTION_BARS = _bars("SPY260807C00645000", [("2026-08-03T13:31:00Z", 0.50, 0.55, 0.45, 0.52, 3)])
_ITM_BARS = _bars("SPY260807C00640000", [("2026-08-03T13:31:00Z", 2.00, 2.05, 1.95, 2.02, 3)])


def _offline(option_bars):
    class Offline(ab.AlpacaBacktesting):
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            self._stock_client = _FakeStockClient(_spy_minute_bars(_DAYS))
            self._option_client = _FakeOptionClient(option_bars)
            self._trading_client = _FakeTradingClient(_spy_contract_pages())
            self._sleep = lambda s: None

    return Offline


def _run(strike, option_bars, *, working_exit, timestep, monkeypatch, tmp_path):
    monkeypatch.setattr(ab, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    contract = Asset("SPY", asset_type="option", expiration=date(2026, 8, 7), strike=strike, right="CALL")

    class BuyAndHold(Strategy):
        def initialize(self):
            self.sleeptime = "1M" if timestep == "minute" else "1D"
            self.vars.stage = 0
            self.vars.exit_order = None
            self.vars.after = None

        def on_trading_iteration(self):
            now = self.get_datetime()
            if self.vars.stage == 0:
                self.submit_order(self.create_order(contract, 1, "buy"))
                self.vars.stage = 1
            elif self.vars.stage == 1 and working_exit and self.get_position(contract) is not None:
                # A limit no print will ever reach: it can only end by expiry handling.
                self.vars.exit_order = self.submit_order(self.create_order(contract, 1, "sell", limit_price=99.0))
                self.vars.stage = 2
            if now.date() == date(2026, 8, 10) and self.vars.after is None:
                self.vars.after = {
                    "options": [(str(p.asset), float(p.quantity)) for p in self.get_positions()
                                if p.asset.asset_type == "option"],
                    "spy": float(self.get_position(Asset("SPY")).quantity) if self.get_position(Asset("SPY")) else 0.0,
                    "cash": float(self.get_cash()),
                    "exit_status": str(self.vars.exit_order.status) if self.vars.exit_order is not None else None,
                }

    _results, strategy = BuyAndHold.run_backtest(
        _offline(option_bars),
        backtesting_start=_NY_TZ.localize(datetime(2026, 8, 3)),
        backtesting_end=_NY_TZ.localize(datetime(2026, 8, 13)),
        benchmark_asset=None, analyze_backtest=False, show_plot=False, save_tearsheet=False,
        show_tearsheet=False, show_progress_bar=False, budget=100_000, timestep=timestep, market="NYSE",
        config={"API_KEY": "test-key", "API_SECRET": "test-secret", "PAPER": True},
    )
    return strategy.vars.after


def test_out_of_the_money_option_is_removed_after_expiry_minute(monkeypatch, tmp_path, disable_datasource_override):
    after = _run(645, _OPTION_BARS, working_exit=False, timestep="minute", monkeypatch=monkeypatch, tmp_path=tmp_path)
    assert after["options"] == []
    assert after["spy"] == 0.0


def test_out_of_the_money_option_is_removed_after_expiry_daily(monkeypatch, tmp_path, disable_datasource_override):
    after = _run(645, _OPTION_BARS, working_exit=False, timestep="day", monkeypatch=monkeypatch, tmp_path=tmp_path)
    assert after["options"] == []


def test_working_order_on_an_expired_option_is_cancelled(monkeypatch, tmp_path, disable_datasource_override):
    after = _run(645, _OPTION_BARS, working_exit=True, timestep="minute", monkeypatch=monkeypatch, tmp_path=tmp_path)
    assert after["options"] == []
    assert after["exit_status"] in ("canceled", "cancelled")


def test_in_the_money_long_call_is_exercised_at_the_real_underlying_close(monkeypatch, tmp_path, disable_datasource_override):
    after = _run(640, _ITM_BARS, working_exit=False, timestep="minute", monkeypatch=monkeypatch, tmp_path=tmp_path)
    assert after["options"] == []
    assert after["spy"] == 100.0  # exercised into shares at the 640 strike
