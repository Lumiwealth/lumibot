"""Result parity gate: a release must not silently change backtest results.

A small fixed set of deterministic backtests runs on the CSV files bundled in
``data/`` (no network, no data provider, no credentials). Each run records
every fill (time, symbol, side, quantity, price) and the portfolio value at
every iteration, and compares them with the stored results in
``tests/backtest_parity/baselines.json``.

Why: speed work is only acceptable when results stay identical. Version to
version, the only proof so far was a manual rerun of a few hosted benchmarks.
This gate makes a changed trade, fill price or portfolio value fail in CI
before a release.

If a change is a deliberate, proven accuracy fix, regenerate the baselines
with ``LUMIBOT_UPDATE_PARITY_BASELINES=1 pytest tests/test_backtest_result_parity_gate.py``
and explain the result change in the commit message. Never regenerate them to
make a speed change pass.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from lumibot import LUMIBOT_SOURCE_PATH
from lumibot.backtesting import PandasDataBacktesting
from lumibot.entities import Asset, Data
from lumibot.strategies import Strategy

BASELINE_PATH = Path(__file__).parent / "backtest_parity" / "baselines.json"
DATA_DIR = Path(LUMIBOT_SOURCE_PATH).parent / "data"
QUOTE = Asset(symbol="USD", asset_type="forex")
UPDATE_ENV = "LUMIBOT_UPDATE_PARITY_BASELINES"


def _daily_data(symbols: list[str]) -> list[Data]:
    out = []
    for symbol in symbols:
        df = pd.read_csv(DATA_DIR / f"{symbol}.csv", parse_dates=True, index_col=0)
        df = df.rename(
            columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}
        )[["open", "high", "low", "close", "volume"]]
        df.index.name = "datetime"
        out.append(
            Data(
                Asset(symbol=symbol, asset_type="stock"),
                df,
                date_start=datetime.datetime(2019, 1, 2),
                date_end=datetime.datetime(2019, 12, 31),
                timestep="day",
                quote=QUOTE,
            )
        )
    return out


def _minute_data(asset: Asset) -> list[Data]:
    df = pd.read_csv(DATA_DIR / "XYZ_1Min.csv", parse_dates=True, index_col=0)
    df.index.name = "datetime"
    df.index = df.index.tz_localize("America/New_York")
    return [Data(asset, df, timestep="minute", quote=QUOTE)]


class _Recorder(Strategy):
    """Records fills and per-iteration portfolio value for the parity gate."""

    def _setup_recording(self) -> None:
        self.vars.fills = []
        self.vars.values = []

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.vars.fills.append(
            [
                self.get_datetime().isoformat(),
                order.asset.symbol,
                str(order.side),
                float(quantity),
                round(float(price), 6),
            ]
        )

    def _record_value(self) -> None:
        self.vars.values.append(round(float(self.get_portfolio_value()), 6))


class DailySmaCross(_Recorder):
    parameters = {"symbol": "SPY", "fast": 5, "slow": 20}

    def initialize(self, parameters: Any = None) -> None:
        self.sleeptime = "1D"
        self._setup_recording()

    def on_trading_iteration(self):
        symbol = self.parameters["symbol"]
        bars = self.get_historical_prices(symbol, self.parameters["slow"] + 1, "day")
        if bars is not None and len(bars.df) >= self.parameters["slow"]:
            closes = bars.df["close"]
            fast = closes.iloc[-self.parameters["fast"]:].mean()
            slow = closes.iloc[-self.parameters["slow"]:].mean()
            position = self.get_position(symbol)
            held = position.quantity if position is not None else 0
            if fast > slow and held == 0:
                qty = int(self.get_cash() // self.get_last_price(symbol))
                if qty > 0:
                    self.submit_order(self.create_order(symbol, qty, "buy"))
            elif fast < slow and held > 0:
                self.submit_order(self.create_order(symbol, held, "sell"))
        self._record_value()


class MonthlyRebalance(_Recorder):
    parameters = {"weights": {"SPY": 0.4, "TLT": 0.3, "GLD": 0.15, "IEF": 0.1, "DJP": 0.05}}

    def initialize(self, parameters: Any = None) -> None:
        self.sleeptime = "1D"
        self._setup_recording()
        self.vars.last_month = None

    def on_trading_iteration(self):
        month = self.get_datetime().month
        if month != self.vars.last_month:
            self.vars.last_month = month
            total = self.get_portfolio_value()
            sells, buys = [], []
            for symbol, weight in self.parameters["weights"].items():
                price = self.get_last_price(symbol)
                position = self.get_position(symbol)
                held = position.quantity if position is not None else 0
                target = int((total * weight) // price)
                if target < held:
                    sells.append(self.create_order(symbol, held - target, "sell"))
                elif target > held:
                    buys.append(self.create_order(symbol, target - held, "buy"))
            for order in sells + buys:
                self.submit_order(order)
        self._record_value()


class MinuteLimitStop(_Recorder):
    """Resting limit entry, then stop loss and take profit, on minute bars."""

    parameters = {"asset": None, "qty": 1, "offset": 2.0, "stop": 6.0, "target": 8.0}

    def initialize(self, parameters: Any = None) -> None:
        self.sleeptime = "15M"
        self._setup_recording()

    def on_trading_iteration(self):
        asset = self.parameters["asset"]
        position = self.get_position(asset)
        held = position.quantity if position is not None else 0
        open_orders = [o for o in self.get_orders() if o.is_active()]
        price = self.get_last_price(asset)
        if held == 0 and price is not None:
            # Re-price a stale entry each iteration (exercises cancel + resubmit).
            if open_orders:
                self.cancel_open_orders()
            self.submit_order(
                self.create_order(
                    asset, self.parameters["qty"], "buy", limit_price=round(price - self.parameters["offset"], 2)
                )
            )
        elif held > 0 and not open_orders and price is not None:
            entry = float(position.avg_fill_price or price)
            self.submit_order(
                self.create_order(
                    asset,
                    held,
                    "sell",
                    order_class="oco",
                    limit_price=round(entry + self.parameters["target"], 2),
                    stop_price=round(entry - self.parameters["stop"], 2),
                )
            )
        self._record_value()


def _run(strategy_class, pandas_data, start, end, parameters, budget=100_000):
    _, strat = strategy_class.run_backtest(
        datasource_class=PandasDataBacktesting,
        backtesting_start=start,
        backtesting_end=end,
        pandas_data=pandas_data,
        parameters=parameters,
        budget=budget,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=False,
        quiet_logs=True,
    )
    fills = list(strat.vars.fills)
    values = list(strat.vars.values)
    return {
        "fill_count": len(fills),
        "fills_sha256": hashlib.sha256(json.dumps(fills).encode()).hexdigest(),
        "first_fills": fills[:5],
        "iterations": len(values),
        "values_sha256": hashlib.sha256(json.dumps(values).encode()).hexdigest(),
        "final_value": values[-1] if values else None,
    }


def _cases():
    stock = Asset("XYZ", asset_type="stock")
    future = Asset("XYZ", asset_type="future", multiplier=5)
    return {
        "daily_sma_cross_spy_2019": lambda: _run(
            DailySmaCross, _daily_data(["SPY"]), datetime.datetime(2019, 2, 1), datetime.datetime(2019, 12, 30), None
        ),
        "monthly_rebalance_five_etfs_2019": lambda: _run(
            MonthlyRebalance,
            _daily_data(["SPY", "TLT", "GLD", "IEF", "DJP"]),
            datetime.datetime(2019, 1, 3),
            datetime.datetime(2019, 12, 30),
            None,
        ),
        "minute_limit_oco_stock_xyz": lambda: _run(
            MinuteLimitStop,
            _minute_data(stock),
            datetime.datetime(2020, 2, 3),
            datetime.datetime(2020, 2, 8),
            {"asset": stock, "qty": 10},
        ),
        "minute_limit_oco_future_xyz": lambda: _run(
            MinuteLimitStop,
            _minute_data(future),
            datetime.datetime(2020, 3, 2),
            datetime.datetime(2020, 3, 7),
            {"asset": future, "qty": 1},
        ),
    }


CASES = _cases()


def _load_baselines() -> dict:
    if BASELINE_PATH.exists():
        return json.loads(BASELINE_PATH.read_text())
    return {}


@pytest.mark.parametrize("case", sorted(CASES))
def test_backtest_results_match_stored_baseline(case):
    result = CASES[case]()
    assert result["fill_count"] > 0, f"{case}: the fixture must trade to guard anything"

    if os.environ.get(UPDATE_ENV) == "1":
        baselines = _load_baselines()
        baselines[case] = result
        BASELINE_PATH.parent.mkdir(parents=True, exist_ok=True)
        BASELINE_PATH.write_text(json.dumps(baselines, indent=2, sort_keys=True) + "\n")
        return

    baselines = _load_baselines()
    assert case in baselines, f"{case}: no stored baseline; generate it with {UPDATE_ENV}=1"
    expected = baselines[case]
    assert result["fill_count"] == expected["fill_count"], (case, result["first_fills"], expected["first_fills"])
    assert result["first_fills"] == expected["first_fills"], case
    assert result["fills_sha256"] == expected["fills_sha256"], f"{case}: fills changed"
    assert result["iterations"] == expected["iterations"], f"{case}: iteration count changed"
    assert result["final_value"] == expected["final_value"], f"{case}: final portfolio value changed"
    assert result["values_sha256"] == expected["values_sha256"], f"{case}: per-iteration portfolio values changed"
