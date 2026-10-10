"""Actual frozen event windows through real fills, dividends and valuation.

These fixtures prove repeatable accounting, not independent vendor price accuracy.
IBKR prices and Yahoo dividends are already in split-adjusted share units: a
split event must not multiply held shares or debit/credit the dividend twice.
"""
from decimal import Decimal
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from tests.backtest.ibkr_replay_support import ReplayMomentum, run_engine_replay

FIXTURE = Path(__file__).parents[1] / "fixtures" / "ibkr_corporate_events_20261010.json"
EXPECTED_ACTIONS = {"NVDA": (10., .01), "SQQQ": (.2, 3.725), "TSLA": (3., 0.),
                    "AAPL": (4., .205), "SPY": (0., 1.904)}


def _frame(symbol):
    corpus = json.loads(FIXTURE.read_text())
    record = next(r for r in corpus["records"] if r["symbol"] == symbol)
    encoded = json.dumps(record["rows"], sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    assert hashlib.sha256(encoded).hexdigest() == record["rows_sha256"]
    assert len(record["source_object_sha256"]) == 64
    frame = pd.DataFrame(record["rows"])
    frame.index = pd.to_datetime(frame.pop("time"), utc=True).dt.tz_convert("America/New_York")
    assert frame["_split_adjusted"].all()
    split, dividend = EXPECTED_ACTIONS[symbol]
    assert sum(frame.stock_splits) == pytest.approx(split, abs=0, rel=0)
    assert sum(frame.dividend) == pytest.approx(dividend, abs=0, rel=0)
    return frame


def _signed(fill):
    side = str(fill["side"])
    return Decimal(str(fill["filled_quantity"])) * (1 if side.startswith("buy") else -1)


def _ledger(frame, fills, when, *, include_same_time=False):
    """Independent decimal ledger; ex-date entitlement precedes today's fills."""
    cash, quantity = Decimal("100000"), Decimal(0)
    for fill in fills:
        stamp = pd.Timestamp(fill["time"])
        if stamp < when or (include_same_time and stamp == when):
            signed = _signed(fill)
            cash -= signed * Decimal(str(fill["price"]))
            quantity += signed
    for stamp, row in frame.loc[frame.dividend != 0].iterrows():
        if stamp.date() > when.date():
            continue
        held_on_ex_date = sum((_signed(f) for f in fills if pd.Timestamp(f["time"]).date() < stamp.date()), Decimal(0))
        cash += held_on_ex_date * Decimal(str(row.dividend))
    return cash, quantity


@pytest.mark.acceptance_backtest
@pytest.mark.usefixtures("disable_datasource_override")
@pytest.mark.parametrize("symbol", list(EXPECTED_ACTIONS))
@pytest.mark.parametrize("routed", [False, True], ids=["direct", "routed"])
@pytest.mark.parametrize("direction", [1, -1], ids=["long", "short"])
def test_real_event_hold_matches_independent_ledger_and_fresh_repeat(monkeypatch, symbol, routed, direction):
    monkeypatch.setenv("LUMIBOT_CACHE_BACKEND", "local")
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    frame = _frame(symbol)
    close_date = frame.index[-1].date()

    def hold(self):
        now = self.get_datetime()
        position = self.get_position(self.asset)
        held = float(position.quantity) if position is not None else 0.
        self.trace.append({"time": now.isoformat(), "cash": float(self.cash),
                           "quantity": held, "equity": float(self.portfolio_value)})
        if not getattr(self, "entered", False):
            self.submit_order(self.create_order(self.asset, 7, "buy" if direction > 0 else "sell_short"))
            self.entered = True
        elif now.date() == close_date and held:
            self.submit_order(self.create_order(self.asset, abs(held), "sell" if direction > 0 else "buy_to_cover"))

    monkeypatch.setattr(ReplayMomentum, "on_trading_iteration", hold)
    results = [run_engine_replay(frame.copy(deep=True), symbol=symbol, lookback=3,
                                 start=frame.index[3].normalize().to_pydatetime(),
                                 end=(frame.index[-1].normalize() + pd.Timedelta(days=1)).to_pydatetime(),
                                 routed=routed) for _ in range(2)]
    first, repeat = results
    for key in ("bars", "signals", "fills", "cash", "quantity", "equity", "frame_sha256"):
        assert first[key] == repeat[key], f"Fresh replay changed {symbol} {key}"
    assert first["data_health"]["required_complete"] is True
    fills = first["fills"]
    assert len(fills) == 2
    assert [float(f["filled_quantity"]) for f in fills] == [7., 7.], "Adjusted holdings must never split twice"
    for fill in fills:
        rows = frame.loc[frame.index.date == pd.Timestamp(fill["time"]).date()]
        assert len(rows) == 1
        assert Decimal(str(fill["price"])) == Decimal(str(rows.open.iloc[0]))
    assert first["quantity"] == 0.
    final_time = frame.index[-1] + pd.Timedelta(days=1)
    cash, quantity = _ledger(frame, fills, final_time)
    assert quantity == 0
    assert first["cash"] == pytest.approx(float(cash), rel=0, abs=1e-8)
    assert first["equity"] == pytest.approx(float(cash), rel=0, abs=1e-8)
    for observation in first["signals"]:
        when = pd.Timestamp(observation["time"])
        cash, quantity = _ledger(frame, fills, when)
        mark = Decimal(str(frame.loc[frame.index < when].close.iloc[-1]))
        expected_equity = cash + quantity * mark
        assert observation["quantity"] == float(quantity)
        assert observation["cash"] == pytest.approx(float(cash), rel=0, abs=1e-8)
        assert observation["equity"] == pytest.approx(float(expected_equity), rel=0, abs=1e-8)
