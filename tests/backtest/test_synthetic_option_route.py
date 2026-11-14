"""End-to-end backtests on the synthetic option route.

The underlying's bars are made up here (no network, no market data). Options are routed to
``synthetic``: chains, prices and bid/ask are computed from those bars. The tests check that
multi-leg option strategies fill at the synthetic bid/ask, settle at expiry for the right cash,
that every fill is labeled ``synthetic`` and counted in the provenance report, and that a large
gap in the underlying moves prices and widens spreads without breaking anything.
"""

from datetime import date, datetime

import numpy as np
import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker
from lumibot.backtesting.data_provenance import build_backtest_data_provenance
from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas
from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
from lumibot.entities import Asset
from lumibot.entities.order import Order
from lumibot.strategies.strategy import Strategy
from lumibot.tools import synthetic_options as model
from lumibot.traders import Trader

NY = "America/New_York"
STOCK = Asset("XYZ", asset_type=Asset.AssetType.STOCK)
EXPIRY = date(2025, 1, 17)  # Friday
START, END = datetime(2025, 1, 13), datetime(2025, 1, 18)
SESSIONS = [date(2025, 1, d) for d in (13, 14, 15, 16, 17)]
BASE = 400.0


def _minute_bars(gap_on=None, gap=0.0):
    """Minute bars for the week: a slow drift around 400, optionally one overnight gap."""
    frames, level = [], BASE
    for day in SESSIONS:
        if gap_on == day:
            level *= 1.0 + gap
        index = pd.date_range(f"{day} 09:30", periods=390, freq="min", tz=NY)
        steps = np.arange(390)
        opens = level + 0.4 * np.sin(steps / 40.0)
        frames.append(pd.DataFrame(
            {"open": opens, "high": opens + 0.05, "low": opens - 0.05, "close": opens + 0.01, "volume": 1000.0},
            index=index,
        ))
    return pd.concat(frames)


def _daily_bars(minute):
    """Daily bars: 200 quiet sessions before the test week (about 16% volatility), then the week."""
    history = pd.bdate_range(end="2025-01-10", periods=200)
    rng = np.random.default_rng(7)
    closes = BASE * np.exp(np.cumsum(rng.normal(0.0, 0.01, len(history))))
    closes = closes * BASE / closes[-1]
    rows = [(pd.Timestamp(d).tz_localize(NY), c) for d, c in zip(history, closes)]
    for day, part in minute.groupby(minute.index.date):
        rows.append((pd.Timestamp(day).tz_localize(NY), float(part["close"].iloc[-1])))
    index = pd.DatetimeIndex([r[0] for r in rows])
    close = np.array([r[1] for r in rows])
    return pd.DataFrame({"open": close, "high": close, "low": close, "close": close, "volume": 1e6}, index=index)


@pytest.fixture
def run_backtest(monkeypatch, tmp_path):
    import lumibot.backtesting.synthetic_option_routing as routing
    import lumibot.tools.ibkr_helper as ibkr_helper
    import lumibot.tools.thetadata_helper as thetadata_helper

    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *a, **k: None)
    monkeypatch.setattr(thetadata_helper, "reset_theta_terminal_tracking", lambda *a, **k: None)
    monkeypatch.setattr(routing, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
    monkeypatch.setenv("LUMIBOT_SYNTHETIC_OPTION_VOL_INPUT", "realized")

    def run(strategy_class, gap_on=None, gap=0.0):
        minute = _minute_bars(gap_on, gap)
        daily = _daily_bars(minute)

        def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, **kwargs):
            assert asset.symbol == "XYZ", "only the underlying may be fetched: options are synthetic"
            frame = daily if str(timestep).endswith("day") else minute
            return frame.copy()

        monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)
        data_source = RoutedBacktestingPandas(
            datetime_start=START, datetime_end=END,
            config={"backtesting_data_routing": {"default": "ibkr", "option": "synthetic"}},
            username="dev", password="dev", use_quote_data=False, show_progress_bar=False,
            log_backtest_progress_to_file=False,
        )
        broker = BacktestingBroker(data_source=data_source)
        strategy = strategy_class(broker=broker, budget=100_000.0, analyze_backtest=False, benchmark_asset=None)
        trader = Trader(logfile="", backtest=True)
        trader.add_strategy(strategy)
        trader.run_all(show_plot=False, show_tearsheet=False, show_indicators=False, save_tearsheet=False)
        log = broker._trade_event_log_df
        fills = log[log["status"] == "fill"]
        provenance = build_backtest_data_provenance(data_source, getattr(broker, "_option_fill_price_counts", None))
        return strategy, fills, provenance, minute

    return run


def _option(strike, right):
    return Asset("XYZ", asset_type=Asset.AssetType.OPTION, expiration=EXPIRY, strike=strike, right=right,
                 underlying_asset=STOCK)


class _Base(Strategy):
    legs = ()
    entry = datetime(2025, 1, 13, 10, 0)

    def initialize(self):
        self.sleeptime = "1M"
        self.set_market("NYSE")
        self.done = False
        self.quotes = {}
        self.cash_before = None
        self.chain = None

    def before_market_closes(self):
        pass

    def on_trading_iteration(self):
        now = self.get_datetime().replace(tzinfo=None)
        if self.done or now < self.entry:
            return
        self.done = True
        self.chain = self.get_chains(STOCK)
        self.cash_before = self.get_cash()
        orders = []
        for strike, right, side in self.legs:
            asset = _option(strike, right)
            quote = self.get_quote(asset)
            self.quotes[(strike, right)] = (quote.bid, quote.ask)
            orders.append(self.create_order(asset, 1, side))
        self.place(orders)

    def place(self, orders):
        for order in orders:
            self.submit_order(order)


class PutCreditSpread(_Base):
    legs = ((395.0, "put", Order.OrderSide.SELL), (390.0, "put", Order.OrderSide.BUY))


class IronCondor(_Base):
    legs = (
        (390.0, "put", Order.OrderSide.SELL), (385.0, "put", Order.OrderSide.BUY),
        (410.0, "call", Order.OrderSide.SELL), (415.0, "call", Order.OrderSide.BUY),
    )

    def place(self, orders):
        self.submit_order(orders)


def _entry_fills(fills):
    entry = fills[fills["time"].astype(str).str.startswith("2025-01-13")]
    # leg rows only (a package also logs a parent row with strike 0)
    entry = entry[entry["asset.strike"].fillna(0).astype(float) > 0]
    return {(float(r["asset.strike"]), str(r["asset.right"]).lower()): r for _, r in entry.iterrows()}


def _check_entry(strategy, fills):
    """Every leg filled at the synthetic quote the strategy saw: buys at the ask, sells at the bid."""
    entry = _entry_fills(fills)
    assert len(entry) == len(strategy.legs), sorted((k, v['type'], v['price']) for k, v in entry.items())
    credit = 0.0
    for strike, right, side in strategy.legs:
        row = entry[(strike, right)]
        bid, ask = strategy.quotes[(strike, right)]
        assert bid is not None and ask is not None and 0 < bid < ask
        expected = ask if side == Order.OrderSide.BUY else bid
        assert float(row["price"]) == pytest.approx(expected, abs=1e-9)
        assert row["price_source"] == "synthetic"
        credit += -expected if side == Order.OrderSide.BUY else expected
    return credit


def test_put_credit_spread_fills_at_synthetic_quotes_and_settles_at_expiry(run_backtest):
    strategy, fills, provenance, minute = run_backtest(PutCreditSpread)
    credit = _check_entry(strategy, fills)
    assert credit > 0
    # the underlying closes near 400, above both strikes: both puts expire worthless
    assert not strategy.get_positions() or all(p.asset.asset_type != "option" for p in strategy.get_positions())
    assert strategy.get_cash() == pytest.approx(100_000.0 + credit * 100.0, abs=0.5)
    assert provenance["optionFillPrices"]["synthetic"] >= 2
    assert provenance["optionFillPriceShares"]["synthetic"] == pytest.approx(1.0)
    assert provenance["syntheticOptions"]["volatilityInput"] == ["realized_volatility"]
    assert "synthetic" in provenance["syntheticOptions"]["notice"]
    assert any(route.get("vendor") == "synthetic" for route in provenance["observedRoutes"])


def test_chain_is_rule_based_and_labeled(run_backtest):
    strategy, _fills, _provenance, _minute = run_backtest(PutCreditSpread)
    chain = strategy.chain
    assert chain["Exchange"] == "SYNTHETIC"
    expirations = sorted(chain["Chains"]["CALL"])
    assert "2025-01-17" in expirations and "2025-02-21" in expirations  # weekly and monthly
    assert "2025-01-14" not in expirations  # an ordinary stock has no daily expirations
    strikes = chain["Chains"]["PUT"]["2025-01-17"]
    assert 395.0 in strikes and 400.0 in strikes and all(k % 5 == 0 for k in strikes)


def test_iron_condor_package_fills_together_and_expires_worthless(run_backtest):
    strategy, fills, provenance, _minute = run_backtest(IronCondor)
    credit = _check_entry(strategy, fills)
    entry = _entry_fills(fills)
    assert len({str(row["time"]) for row in entry.values()}) == 1, "all four legs fill on the same bar"
    assert credit > 0
    assert strategy.get_cash() == pytest.approx(100_000.0 + credit * 100.0, abs=0.5)
    assert provenance["optionFillPrices"] == {"synthetic": provenance["optionFillPrices"]["synthetic"]}


def test_short_put_assigned_value_after_a_gap_down(run_backtest):
    """An 8% overnight gap down: the spread ends fully in the money and loses its width minus the credit."""
    strategy, fills, _provenance, _minute = run_backtest(PutCreditSpread, gap_on=date(2025, 1, 15), gap=-0.08)
    credit = _check_entry(strategy, fills)
    assert strategy.get_cash() == pytest.approx(100_000.0 + (credit - 5.0) * 100.0, abs=0.5)


class SmartLimitPut(_Base):
    legs = ((395.0, "put", Order.OrderSide.BUY),)

    def place(self, orders):
        from lumibot.entities import SmartLimitConfig, SmartLimitPreset

        order = self.create_order(
            orders[0].asset, 1, Order.OrderSide.BUY, order_type=Order.OrderType.SMART_LIMIT,
            smart_limit=SmartLimitConfig(preset=SmartLimitPreset.FAST),
        )
        self.submit_order(order)


def test_smart_limit_fills_inside_the_synthetic_spread(run_backtest):
    strategy, fills, provenance, _minute = run_backtest(SmartLimitPut)
    entry = _entry_fills(fills)
    row = entry[(395.0, "put")]
    assert row["price_source"] == "synthetic"
    # compare with the synthetic quote of the bar the order filled on
    store = strategy.broker.data_source._data_store
    frame = store[(_option(395.0, "put"), Asset("USD", "forex"), "minute")].df
    bar = frame.loc[pd.Timestamp(row["time"])]
    assert bar["bid"] <= float(row["price"]) <= bar["ask"]
    assert float(row["price"]) < bar["ask"], "a smart limit buy should not pay the full ask"
    assert provenance["optionFillPrices"]["synthetic"] >= 1


def test_gap_day_moves_prices_and_widens_spreads():
    """Model-level stress: the underlying opens 8% lower and trades in wide bars."""
    minute = _minute_bars(gap_on=date(2025, 1, 15), gap=-0.08)
    day = minute.index.date == date(2025, 1, 15)
    minute.loc[day, "high"] = minute.loc[day, "open"] * 1.004
    minute.loc[day, "low"] = minute.loc[day, "open"] * 0.996
    daily = _daily_bars(minute)
    closes = pd.Series(daily["close"].to_numpy(), index=daily.index.date)
    surface = model.build_surface(minute, closes, symbol="XYZ", mode="realized")
    frame = model.contract_frame(surface, 390.0, False, EXPIRY)
    assert np.isfinite(frame[["open", "high", "low", "close", "bid", "ask"]].to_numpy()).all()
    before = frame[frame.index.date == date(2025, 1, 14)].iloc[-1]
    after = frame[frame.index.date == date(2025, 1, 15)].iloc[0]
    assert after["open"] > 5 * before["open"]  # the put is now deep in the money
    assert after["volatility"] > before["volatility"]  # volatility rises when the underlying falls
    assert (after["ask"] - after["bid"]) > 2 * (before["ask"] - before["bid"])
    assert (frame["ask"] > frame["bid"]).all() and (frame["bid"] >= 0).all()
