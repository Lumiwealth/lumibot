"""Release-blocking fixed-input comparisons, independent of mutable vendor data."""
from datetime import date, datetime

import pandas as pd
import pytest

from tests.backtest.ibkr_replay_support import run_engine_replay


def _prices(index, opens):
    return pd.DataFrame({"open": opens, "high": [x + 2 for x in opens],
                         "low": [x - 2 for x in opens], "close": [x + .5 for x in opens],
                         "volume": 1000, "missing": False, "dividend": 0.0, "stock_splits": 0.0}, index=index)


@pytest.mark.acceptance_backtest
@pytest.mark.usefixtures("disable_datasource_override")
def test_missing_warmup_is_distinct_from_valid_zero_trade_run(monkeypatch):
    from tests.backtest.ibkr_replay_support import ReplayMomentum

    monkeypatch.setenv("LUMIBOT_CACHE_BACKEND", "local")
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    frame = _prices(pd.date_range("2026-02-02 16:00", periods=10, freq="B", tz="America/New_York"), [100.] * 10)
    valid = run_engine_replay(frame, symbol="SPY", start=datetime(2026, 2, 9), end=datetime(2026, 2, 13))
    assert not valid["fills"]
    assert valid["data_health"]["required_complete"] is True

    def hold_if_insufficient(self):
        # A strategy may intentionally hold cash instead of crashing on missing history.
        # That must not turn unavailable warmup into a qualified zero-trade result.
        self.get_historical_prices(self.asset, 51, timestep="day")

    monkeypatch.setattr(ReplayMomentum, "on_trading_iteration", hold_if_insufficient)
    incomplete = run_engine_replay(frame, symbol="SPY", start=datetime(2026, 2, 9), end=datetime(2026, 2, 13))
    assert not incomplete["fills"]
    assert incomplete["data_health"]["required_complete"] is False
    assert incomplete["data_health"]["required_failures"][0]["requested_bars"] == 51


@pytest.mark.acceptance_backtest
@pytest.mark.usefixtures("disable_datasource_override")
def test_missing_futures_position_mark_invalidates_equity(monkeypatch):
    monkeypatch.setenv("LUMIBOT_CACHE_BACKEND", "local")
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    index = pd.date_range("2026-02-02 18:00", periods=10, freq="B", tz="America/New_York")
    frame = _prices(index, [100. + i % 4 for i in range(10)])
    empty_minute = frame.iloc[:0]
    result = run_engine_replay(frame, symbol="MES", start=datetime(2026, 2, 9), end=datetime(2026, 2, 14),
                               asset_type="future", expiration=date(2026, 3, 20), multiplier=5,
                               market="us_futures", auxiliary_frames={"minute": empty_minute})
    assert result["fills"]
    assert result["data_health"]["required_complete"] is False
    assert any(failure["reason"] == "missing_valuation_price" for failure in result["data_health"]["required_failures"])


def _assert_stock_oracle(result, frame, lookback):
    cash, quantity = 100_000., 0.
    pending_fills = list(result["fills"])
    for signal in result["signals"]:
        decision = pd.Timestamp(signal["time"])
        while pending_fills and pd.Timestamp(pending_fills[0]["time"]) < decision:
            fill = pending_fills.pop(0)
            signed = float(fill["filled_quantity"]) * (1 if fill["side"] == "buy" else -1)
            cash -= signed * float(fill["price"])
            quantity += signed
        completed = frame.loc[frame.index < decision].tail(lookback)
        assert len(completed) == lookback
        assert signal["last_close"] == float(completed.close.iloc[-1])
        assert signal["signal"] == int(completed.close.iloc[-1] > completed.close.iloc[:-1].mean())
        assert signal["cash_before"] == pytest.approx(cash, abs=1e-9, rel=0)
        assert signal["held_before"] == quantity
        assert signal["equity_before"] == pytest.approx(cash + quantity * completed.close.iloc[-1], abs=1e-9, rel=0)
        assert [bar["close"] for bar in signal["input_bars"]] == completed.close.tolist()
    for fill in result["fills"]:
        when = pd.Timestamp(fill["time"])
        row = frame.loc[frame.index.date == when.date()]
        assert len(row) == 1
        assert float(fill["price"]) == float(row.open.iloc[0]), f"Market fill must use this session's open: {fill} expected={row.open.iloc[0]}"


@pytest.mark.acceptance_backtest
@pytest.mark.usefixtures("disable_datasource_override")
@pytest.mark.parametrize("symbol,lookback", [("SPY", 3), ("AAPL", 20), ("TQQQ", 200)])
@pytest.mark.parametrize("routed", [False, True], ids=["direct", "botspot-auto"])
def test_250_session_daily_replay_matches_independent_signal_and_ledger(monkeypatch, symbol, lookback, routed):
    import pandas_market_calendars as mcal

    monkeypatch.setenv("LUMIBOT_CACHE_BACKEND", "local")
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    schedule = mcal.get_calendar("NYSE").schedule("2024-01-01", "2026-02-13")
    index = pd.DatetimeIndex(schedule.market_close).tz_convert("America/New_York")
    opens = [100. + (i % 41) * .25 for i in range(len(index))]
    frame = _prices(index, opens).tail(lookback + 250)
    # Supply exactly the required warmup, not an unbounded fixture that masks
    # repeated requests for unnecessary calendar padding before the corpus.
    first = index[-250].normalize().to_pydatetime()
    end = (index[-1].normalize() + pd.Timedelta(days=1)).to_pydatetime()
    result = run_engine_replay(frame, symbol=symbol, start=first, end=end, lookback=lookback, routed=routed)
    assert len(result["signals"]) == 250
    assert len(result["fills"]) >= 10
    assert len(result["history_requests"]) == 1, "Complete frozen history should load once"
    _assert_stock_oracle(result, frame, lookback)


@pytest.mark.acceptance_backtest
@pytest.mark.usefixtures("disable_datasource_override")
def test_daily_replay_has_real_fills_and_independently_reconciled_cash(monkeypatch):
    monkeypatch.setenv("LUMIBOT_DISABLE_DOTENV", "1")
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("LUMIBOT_CACHE_BACKEND", "local")
    idx = pd.date_range("2026-02-02 16:00", periods=12, freq="B", tz="America/New_York")
    opens = [100, 101, 102, 103, 101, 99, 98, 100, 103, 104, 102, 101]
    frame = pd.DataFrame({"open": opens, "high": [x + 2 for x in opens],
                          "low": [x - 2 for x in opens], "close": [x + .5 for x in opens],
                          "volume": 1000, "missing": False}, index=idx)
    result = run_engine_replay(frame, symbol="SPY", start=datetime(2026, 2, 9), end=datetime(2026, 2, 13))
    assert result["signals"]
    assert result["signals"][0]["last_close"] == 101.5, result["signals"]
    assert result["fills"], "This deterministic price pattern must exercise broker accounting"
    cash = 100_000.0
    quantity = 0.0
    for fill in result["fills"]:
        signed = float(fill["filled_quantity"]) * (1 if fill["side"] == "buy" else -1)
        cash -= signed * float(fill["price"])
        quantity += signed
    assert result["cash"] == pytest.approx(cash, abs=1e-9)
    assert result["quantity"] == quantity
    for signal in result["signals"]:
        assert pd.Timestamp(signal["last_input_time"]) < pd.Timestamp(signal["time"]), "future-close leakage"
        completed = frame.loc[frame.index < pd.Timestamp(signal["time"])]
        assert signal["last_close"] == float(completed["close"].iloc[-1]), "unclosed session leaked into history"


def test_replay_refuses_missing_prices():
    frame = pd.DataFrame({"open": [1.], "high": [1.], "low": [1.], "close": [None], "volume": [1]},
                         index=pd.DatetimeIndex(["2026-02-09T16:00:00-05:00"]))
    with pytest.raises(ValueError, match="missing prices"):
        run_engine_replay(frame, symbol="SPY", start=datetime(2026, 2, 9), end=datetime(2026, 2, 10))


@pytest.mark.acceptance_backtest
@pytest.mark.usefixtures("disable_datasource_override")
def test_intraday_replay_never_uses_forming_close_and_fills_at_bar_open(monkeypatch):
    monkeypatch.setenv("LUMIBOT_CACHE_BACKEND", "local")
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    index = pd.DatetimeIndex([])
    for session_date in ["2026-02-06", "2026-02-09", "2026-02-10"]:
        session = pd.date_range(f"{session_date} 09:30", periods=390, freq="min", tz="America/New_York")
        index = session if index.empty else index.append(session)
    frame = _prices(index, [100. + (i % 47) * .1 for i in range(len(index))])
    daily = _prices(pd.date_range("2026-02-02 16:00", periods=7, freq="B", tz="America/New_York"), [100.] * 7)
    result = run_engine_replay(frame, symbol="SPY", start=datetime(2026, 2, 9), end=datetime(2026, 2, 11),
                               timestep="minute", auxiliary_frames={"day": daily})
    assert len(result["signals"]) >= 20
    assert len(result["fills"]) >= 2
    minute_requests = [request for request in result["history_requests"] if request["timestep"] == "minute"]
    assert len(minute_requests) == 1, "A complete historical series must be reused across strategy iterations"
    for signal in result["signals"]:
        when = pd.Timestamp(signal["time"])
        completed = frame.loc[frame.index + pd.Timedelta(minutes=1) <= when].tail(3)
        assert [bar["close"] for bar in signal["input_bars"]] == completed.close.tolist()
        assert signal["signal"] == int(completed.close.iloc[-1] > completed.close.iloc[:-1].mean())
    for fill in result["fills"]:
        assert float(fill["price"]) == float(frame.loc[pd.Timestamp(fill["time"]), "open"])


@pytest.mark.acceptance_backtest
@pytest.mark.usefixtures("disable_datasource_override")
@pytest.mark.parametrize("symbol,multiplier", [("MES", 5), ("GC", 100), ("MGC", 10), ("NG", 10000)])
@pytest.mark.parametrize("routed", [False, True], ids=["direct", "botspot-auto"])
def test_futures_daily_replay_uses_completed_sessions(monkeypatch, symbol, multiplier, routed):
    import pandas_market_calendars as mcal

    monkeypatch.setenv("LUMIBOT_CACHE_BACKEND", "local")
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    sessions = mcal.get_calendar("us_futures").schedule("2026-02-02", "2026-02-13")
    index = pd.DatetimeIndex(sessions.market_close).tz_convert("America/New_York")
    frame = _prices(index, [100. + i % 4 for i in range(len(index))])
    minute_frames = []
    for i, (_, session) in enumerate(sessions.iterrows()):
        minutes = pd.date_range(session.market_open, session.market_close, freq="min", inclusive="left")
        minutes = minutes[minutes.tz_convert("America/New_York").hour != 17]
        minute_frames.append(_prices(minutes, [100. + i % 4] * len(minutes)))
    minute = pd.concat(minute_frames)
    result = run_engine_replay(frame, symbol=symbol, start=datetime(2026, 2, 9), end=datetime(2026, 2, 14),
                               asset_type="future", expiration=date(2026, 3, 20) if symbol == "MES" else date(2026, 2, 25),
                               multiplier=multiplier, market="us_futures", auxiliary_frames={"minute": minute}, routed=routed)
    assert len(result["signals"]) >= 4
    assert result["fills"]
    margin = {"MES": 1300., "GC": 10000., "MGC": 1200., "NG": 3000.}[symbol]
    realized, held, entry = 0., 0., 0.
    fills = list(result["fills"])
    for signal in result["signals"]:
        when = pd.Timestamp(signal["time"])
        while fills and pd.Timestamp(fills[0]["time"]) < when:
            fill = fills.pop(0)
            if fill["side"] == "buy":
                held, entry = float(fill["filled_quantity"]), float(fill["price"])
            else:
                realized += (float(fill["price"]) - entry) * float(fill["filled_quantity"]) * multiplier
                held -= float(fill["filled_quantity"])
        completed = frame.loc[frame.index <= when].tail(3)
        assert [bar["close"] for bar in signal["input_bars"]] == completed.close.tolist()
        assert signal["signal"] == int(completed.close.iloc[-1] > completed.close.iloc[:-1].mean())
        mark = minute.loc[when, "open"] if when in minute.index else minute.loc[minute.index < when, "close"].iloc[-1]
        assert signal["held_before"] == held
        assert signal["cash_before"] == pytest.approx(100_000 + realized - held * margin, abs=1e-9, rel=0)
        assert signal["equity_before"] == pytest.approx(100_000 + realized + held * (mark - entry) * multiplier,
                                                       abs=1e-9, rel=0)


@pytest.mark.acceptance_backtest
@pytest.mark.usefixtures("disable_datasource_override")
@pytest.mark.parametrize("side,expected_quantity", [("buy", 1), ("sell_short", -1)])
@pytest.mark.parametrize("action", ["hold", "close", "roll"])
@pytest.mark.parametrize("fee", [0, 1])
@pytest.mark.parametrize("routed", [False, True], ids=["direct", "botspot-auto"])
def test_held_contract_does_not_earn_continuous_series_roll_gap(monkeypatch, side, expected_quantity, action, fee, routed):
    """Two flat tradable contracts cannot generate profit when a chart changes contract."""
    import pandas_market_calendars as mcal
    from tests.backtest.ibkr_replay_support import ReplayMomentum
    from lumibot.entities import TradingFee

    monkeypatch.setenv("LUMIBOT_CACHE_BACKEND", "local")
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    sessions = mcal.get_calendar("us_futures").schedule("2026-09-14", "2026-09-24")
    daily_index = pd.DatetimeIndex(sessions.market_close)
    minutes = pd.DatetimeIndex([])
    for _, session in sessions.iterrows():
        part = pd.date_range(session.market_open, session.market_close, freq="min", inclusive="left")
        part = part[part.tz_convert("America/New_York").hour != 17]
        minutes = part if minutes.empty else minutes.append(part)
    cut = pd.Timestamp("2026-09-21 04:05", tz="UTC")

    def flat(index, values):
        frame = _prices(index, values)
        frame["close"] = frame["open"]
        return frame

    continuous_day = flat(daily_index, [100. if stamp < cut else 110. for stamp in daily_index])
    continuous_minute = flat(minutes, [100. if stamp < cut else 110. for stamp in minutes])
    old_day, old_minute = flat(daily_index, [100.] * len(daily_index)), flat(minutes, [100.] * len(minutes))
    new_day, new_minute = flat(daily_index, [110.] * len(daily_index)), flat(minutes, [110.] * len(minutes))

    def hold(self):
        if not self.get_position(self.asset) and not self.get_orders():
            self.submit_order(self.create_order(self.asset, 1, side))
        if action != "hold" and self.get_datetime() >= cut and not getattr(self, "closed_original", False):
            self.submit_order(self.create_order(self.asset, 1, "sell" if side == "buy" else "buy_to_cover"))
            if action == "roll":
                self.submit_order(self.create_order(self.asset, 1, "buy_to_open" if side == "buy" else "sell_short"))
            self.closed_original = True
        self.trace.append({"time": self.get_datetime().isoformat(), "equity": float(self.portfolio_value)})

    monkeypatch.setattr(ReplayMomentum, "on_trading_iteration", hold)
    result = run_engine_replay(continuous_day, symbol="MGC", start=datetime(2026, 9, 17),
                               end=datetime(2026, 9, 24), asset_type="cont_future", multiplier=10,
                               market="us_futures", auxiliary_frames={"minute": continuous_minute},
                               contract_frames={date(2026, 10, 28): {"day": old_day, "minute": old_minute},
                                                date(2026, 12, 29): {"day": new_day, "minute": new_minute}},
                               trading_fees=[TradingFee(flat_fee=fee)], routed=routed)
    assert len(result["fills"]) == {"hold": 1, "close": 2, "roll": 3}[action]
    assert [fill["price"] for fill in result["fills"]] == {"hold": [100.], "close": [100., 100.],
                                                         "roll": [100., 100., 110.]}[action]
    assert result["quantity"] == (0 if action == "close" else expected_quantity)
    expected_equity = 100_000. - fee * len(result["fills"])
    assert result["equity"] == pytest.approx(expected_equity, abs=1e-9, rel=0)
    assert all(expected_equity <= row["equity"] <= 100_000. for row in result["signals"])
    assert result["cash"] == pytest.approx(expected_equity - (0 if action == "close" else 1200.), abs=1e-9, rel=0)
    assert any(request["expiration"] == "2026-10-28" for request in result["history_requests"])
    assert result["data_health"]["required_complete"] is True, str(result["data_health"]["required_failures"])


@pytest.mark.acceptance_backtest
@pytest.mark.usefixtures("disable_datasource_override")
def test_open_market_missing_mark_is_invalid_even_with_previous_good_price(monkeypatch):
    from tests.backtest.ibkr_replay_support import ReplayMomentum

    monkeypatch.setenv("LUMIBOT_CACHE_BACKEND", "local")
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    days = pd.date_range("2026-02-02 18:00", periods=10, freq="B", tz="America/New_York")
    daily = _prices(days, [100.] * len(days))
    minutes = pd.date_range("2026-02-08 18:00", "2026-02-10 16:59", freq="min", tz="America/New_York")
    available = _prices(minutes, [100.] * len(minutes))

    def hold(self):
        if not self.get_position(self.asset) and not self.get_orders():
            self.submit_order(self.create_order(self.asset, 1, "buy"))
        self.trace.append({"time": self.get_datetime().isoformat(), "equity": float(self.portfolio_value)})

    monkeypatch.setattr(ReplayMomentum, "on_trading_iteration", hold)
    result = run_engine_replay(daily, symbol="MES", start=datetime(2026, 2, 9), end=datetime(2026, 2, 12),
                               asset_type="future", expiration=date(2026, 3, 20), multiplier=5,
                               market="us_futures", auxiliary_frames={"minute": available})
    assert result["fills"] and result["signals"]
    assert result["data_health"]["required_complete"] is False
    assert any(f["reason"] == "missing_valuation_price" for f in result["data_health"]["required_failures"])


@pytest.mark.acceptance_backtest
@pytest.mark.usefixtures("disable_datasource_override")
@pytest.mark.parametrize("routed", [False, True], ids=["direct", "botspot-auto"])
def test_intraday_trade_derived_quotes_cannot_fill_at_forming_close(monkeypatch, routed):
    monkeypatch.setenv("LUMIBOT_CACHE_BACKEND", "local")
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    index = pd.DatetimeIndex([])
    for session_date in ["2026-02-06", "2026-02-09", "2026-02-10"]:
        session = pd.date_range(f"{session_date} 04:00", periods=960, freq="min", tz="America/New_York")
        index = session if index.empty else index.append(session)
    frame = _prices(index, [100. + i % 17 for i in range(len(index))])
    # Actual Trades cache objects carry bid/ask synthesized from the bar close.
    # These are not NBBO known at the start of the minute.
    frame["bid"] = frame["close"]
    frame["ask"] = frame["close"]
    daily = _prices(pd.date_range("2026-02-02 16:00", periods=7, freq="B", tz="America/New_York"), [100.] * 7)
    result = run_engine_replay(frame, symbol="SPY", start=datetime(2026, 2, 9), end=datetime(2026, 2, 11),
                               timestep="minute", auxiliary_frames={"day": daily}, routed=routed)
    assert len(result["signals"]) >= 20
    assert len(result["fills"]) >= 2
    assert len([r for r in result["history_requests"] if r["timestep"] == "minute"]) == 1
    for fill in result["fills"]:
        assert float(fill["price"]) == float(frame.loc[pd.Timestamp(fill["time"]), "open"])
    cash, quantity = 100_000., 0.
    pending = list(result["fills"])
    for signal in result["signals"]:
        when = pd.Timestamp(signal["time"])
        while pending and pd.Timestamp(pending[0]["time"]) < when:
            fill = pending.pop(0)
            signed = float(fill["filled_quantity"]) * (1 if fill["side"] == "buy" else -1)
            cash -= signed * float(frame.loc[pd.Timestamp(fill["time"]), "open"])
            quantity += signed
        assert signal["cash_before"] == pytest.approx(cash, rel=0, abs=1e-9)
        assert signal["equity_before"] == pytest.approx(cash + quantity * float(frame.loc[when, "open"]), rel=0, abs=1e-9)
