"""Normal strategy execution on recorded-shape candles and synthetic event timing."""

from decimal import Decimal

import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker, KalshiBacktesting
from lumibot.entities import Asset, Data, Order
from lumibot.strategies import Strategy


def asset(side="yes"):
    return Asset("SYNTHETIC-EVENT", "prediction_contract", outcome=side)


def make_source(*, settlement="2026-01-01T12:04:00Z", result="yes"):
    index = pd.date_range("2026-01-01T12:00:00Z", periods=4, freq="min")
    yes = pd.DataFrame(
        {
            "open": [0.4, 0.45, 0.5, 0.55],
            "high": [0.4, 0.45, 0.5, 0.55],
            "low": [0.4, 0.45, 0.5, 0.55],
            "close": [0.4, 0.45, 0.5, 0.55],
            "bid": [0.39, 0.44, 0.49, 0.54],
            "ask": [0.41, 0.46, 0.51, 0.56],
            "volume": [10, 10, 10, 10],
        },
        index=index,
    )
    no = yes.copy()
    for name in ("open", "high", "low", "close"):
        no[name] = 1 - yes[name]
    no["bid"], no["ask"] = 1 - yes["ask"], 1 - yes["bid"]
    quote = Asset("USD", "forex")
    metadata = {
        "SYNTHETIC-EVENT": {
            "ticker": "SYNTHETIC-EVENT",
            "open_time": "2026-01-01T11:00:00Z",
            "close_time": "2026-01-01T12:03:00Z",
            "settlement_ts": settlement,
            "result": result,
            "settlement_value_dollars": "1.0000" if result == "yes" else "0.0000",
        }
    }
    return KalshiBacktesting(
        index[0].to_pydatetime(),
        pd.Timestamp("2026-01-01T12:05Z").to_pydatetime(),
        pandas_data=[Data(asset(), yes, quote=quote), Data(asset("no"), no, quote=quote)],
        market_metadata=metadata,
        fee_rate=0,
        market="24/7",
        show_progress_bar=False,
    )


def make_strategy(source):
    broker = BacktestingBroker(source)
    broker.initialize_market_calendars(source.get_trading_days_pandas())
    broker._first_iteration = False

    class ContractStrategy(Strategy):
        def on_trading_iteration(self):
            pass

    strategy = ContractStrategy(broker=broker, budget=100, analyze_backtest=False)
    strategy._first_iteration = False
    return strategy, broker


def execute(strategy, broker, side, count, action):
    order = strategy.create_order(asset(side), count, action)
    strategy.submit_order(order)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue(skip_first_iteration_events=False)
    assert order.is_filled()
    return order


def test_completed_end_history_and_quote_do_not_see_next_bar():
    source = make_source()
    source._update_datetime(pd.Timestamp("2026-01-01T12:00:30Z").to_pydatetime())
    assert source.get_last_price(asset()) == 0.4
    bars = source.get_historical_prices(asset(), 100)
    assert list(bars.df["close"]) == [0.4]
    assert source.get_quote(asset()).ask == 0.41
    assert source.get_resolution_status(asset())["resolved"] is False
    assert not source.get_market_metadata(asset()).get("result")


def test_standard_orders_net_opposite_holdings_and_cash():
    source = make_source()
    strategy, broker = make_strategy(source)
    execute(strategy, broker, "yes", 3, "buy")
    assert strategy.get_position(asset()).quantity == 3
    assert strategy.cash == pytest.approx(98.77)
    execute(strategy, broker, "no", 1, "buy")
    assert strategy.get_position(asset()).quantity == 2
    assert strategy.get_position(asset("no")) is None
    assert strategy.cash == pytest.approx(99.16)
    execute(strategy, broker, "no", 3, "buy")
    assert strategy.get_position(asset()) is None
    assert strategy.get_position(asset("no")).quantity == 1
    assert strategy.cash == pytest.approx(99.33)
    execute(strategy, broker, "no", 1, "sell")
    assert not [p for p in strategy.get_positions() if p.asset.asset_type == "prediction_contract"]
    assert strategy.cash == pytest.approx(99.92)


@pytest.mark.parametrize("result,payout", [("yes", 1), ("no", 0)])
def test_settlement_uses_actual_time_including_zero_and_runs_once(result, payout):
    source = make_source(result=result)
    strategy, broker = make_strategy(source)
    execute(strategy, broker, "yes", 2, "buy")
    cash = strategy.cash
    source._update_datetime(pd.Timestamp("2026-01-01T12:03:30Z").to_pydatetime())
    broker.process_pending_orders(strategy)
    assert strategy.get_position(asset()).quantity == 2
    source._update_datetime(pd.Timestamp("2026-01-01T12:04Z").to_pydatetime())
    broker.process_pending_orders(strategy)
    assert strategy.get_position(asset()) is None
    assert strategy.cash == pytest.approx(cash + 2 * payout)
    broker.process_pending_orders(strategy)
    assert strategy.cash == pytest.approx(cash + 2 * payout)
    events = broker._trade_event_log_df
    cash_events = events.loc[events["event_kind"] == "cash_event"]
    assert len(cash_events) == (1 if payout else 0)
    if payout:
        event = cash_events.iloc[0]
        assert event["cash_event_type"] == "other_cash"
        assert event["cash_event_amount"] == 2
        assert event["cash_event_raw_subtype"] == "prediction_settlement"
        assert not event["is_external_cash_flow"]
    settled = events.loc[events["status"] == broker.CASH_SETTLED]
    assert len(settled) == 1 and settled.iloc[0]["price"] == payout


def test_default_quadratic_and_balance_rounding_match_recorded_native_fills():
    source = make_source()
    source.fee_rate = Decimal(".07")
    order = Order("neutral", asset(), 3, "buy")
    assert source.calculate_backtest_trade_cost(order, 0.59) == Decimal(".0508")
    order = Order("neutral", asset("no"), 0.25, "sell")
    assert source.calculate_backtest_trade_cost(order, 0.41) == Decimal(".0043")


def test_stale_or_missing_quote_never_fabricates_an_execution():
    source = make_source()
    strategy, broker = make_strategy(source)
    source._update_datetime(pd.Timestamp("2026-01-01T11:59Z").to_pydatetime())
    order = strategy.create_order(asset(), 1, "buy", time_in_force="ioc")
    strategy.submit_order(order)
    broker.process_pending_orders(strategy)
    assert order.is_canceled() and strategy.cash == 100


def test_sell_from_flat_posts_collateral_and_opens_the_opposite_contract():
    source = make_source()
    strategy, broker = make_strategy(source)
    execute(strategy, broker, "yes", 1, "sell")
    assert strategy.get_position(asset()) is None
    assert strategy.get_position(asset("no")).quantity == 1
    assert strategy.get_position(asset("no")).avg_fill_price == pytest.approx(0.61)
    assert strategy.cash == pytest.approx(99.39)


def test_end_of_backtest_does_not_resolve_a_future_settlement():
    source = make_source(settlement="2026-01-01T12:06Z")
    strategy, broker = make_strategy(source)
    execute(strategy, broker, "no", 0.25, "buy")
    cash = strategy.cash
    source._update_datetime(pd.Timestamp("2026-01-01T12:05Z").to_pydatetime())
    broker.process_pending_orders(strategy)
    assert strategy.get_position(asset("no")).quantity == 0.25
    assert strategy.cash == cash
    assert source.get_resolution_status(asset("no"))["resolved"] is False


def test_netting_callback_sees_the_actual_portfolio(monkeypatch):
    source = make_source()
    strategy, broker = make_strategy(source)
    execute(strategy, broker, "yes", 3, "buy")
    observed = []

    def callback(position, order, *args):
        observed.append(
            (
                order.asset.outcome,
                position.quantity,
                [
                    (p.asset.outcome, p.quantity)
                    for p in broker.get_tracked_positions(strategy.name)
                    if p.asset.asset_type == "prediction_contract"
                ],
            )
        )

    monkeypatch.setattr(broker, "_on_filled_order", callback)
    execute(strategy, broker, "no", 1, "buy")
    assert observed == [("no", 0, [("yes", 2)])]


class NormalContractBacktest(Strategy):
    def initialize(self):
        self.sleeptime = "1M"
        self.set_market("24/7")
        self.vars.phase = 0

    def on_trading_iteration(self):
        phase = self.vars.phase
        if phase < 3:
            side, count, action = [("yes", 1, "buy"), ("no", 2, "buy"), ("no", 1, "sell")][phase]
            self.submit_order(self.create_order(asset(side), count, action))
        self.vars.phase += 1


def test_normal_backtest_entrypoint_and_environment_routing(monkeypatch):
    monkeypatch.setenv("BACKTESTING_DATA_SOURCE", "kalshi")
    prepared = make_source()
    result, strategy = NormalContractBacktest.run_backtest(
        KalshiBacktesting,
        prepared.datetime_start,
        prepared.datetime_end,
        pandas_data=prepared.pandas_data,
        market_metadata=prepared._market_metadata,
        fee_rate=0,
        market="24/7",
        timestep="minute",
        budget=100,
        benchmark_asset=None,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=False,
        save_stats_file=False,
        show_progress_bar=False,
    )
    assert result is not None
    filled = [order for order in strategy.broker.get_all_orders() if order.is_filled()]
    assert len(filled) == 3
    assert not [p for p in strategy.get_positions() if p.asset.asset_type == "prediction_contract"]


def test_history_aggregation_keeps_absent_candles_missing():
    source = make_source()
    frame = source._frames[asset()].iloc[[0, -1]].copy()
    frame.index = pd.to_datetime(["2026-01-01T12:00Z", "2026-01-01T12:04Z"])
    source._frames[asset()] = frame
    source._update_datetime(pd.Timestamp("2026-01-01T12:04Z").to_pydatetime())
    result = source.get_historical_prices(asset(), None, timestep="2min").df
    assert pd.Timestamp("2026-01-01T12:02Z") not in result.index


def test_gtd_expiry_precedes_a_crossing_at_its_deadline():
    source = make_source()
    strategy, broker = make_strategy(source)
    order = strategy.create_order(
        asset(),
        1,
        "buy",
        limit_price=0.44,
        time_in_force="gtd",
        good_till_date=pd.Timestamp("2026-01-01T12:01Z").to_pydatetime(),
    )
    strategy.submit_order(order)
    source._frames[asset()].loc[pd.Timestamp("2026-01-01T12:01Z"), "ask"] = 0.43
    source._update_datetime(pd.Timestamp("2026-01-01T12:01Z").to_pydatetime())
    broker.process_pending_orders(strategy)
    assert order.is_canceled() and strategy.cash == 100


def test_a_market_without_candles_remains_missing_instead_of_failing_other_assets():
    from unittest.mock import Mock

    from lumibot.data_sources import KalshiData
    from lumibot.entities import Bars

    data = Mock(spec=KalshiData)
    data._validate_asset.side_effect = KalshiData._validate_asset
    data.get_historical_prices.return_value = Bars(
        pd.DataFrame(columns=["open", "high", "low", "close", "volume"], index=pd.DatetimeIndex([], tz="UTC")),
        "KALSHI",
        asset(),
    )
    data.get_market_metadata.return_value = {"ticker": "SYNTHETIC-EVENT"}
    source = KalshiBacktesting(
        pd.Timestamp("2026-01-01T12:00Z").to_pydatetime(),
        pd.Timestamp("2026-01-01T12:05Z").to_pydatetime(),
        assets=[asset()],
        kalshi_data=data,
    )
    assert source.get_last_price(asset()) is None
    assert source.get_historical_prices(asset(), 2).df.empty


def test_reduce_only_ioc_caps_fill_without_losing_requested_quantity():
    source = make_source()
    strategy, broker = make_strategy(source)
    execute(strategy, broker, "yes", 2, "buy")
    order = strategy.create_order(asset("no"), 5, "buy", time_in_force="ioc", custom_params={"reduce_only": True})
    strategy.submit_order(order)
    broker.process_pending_orders(strategy)
    assert order.quantity == 5
    assert sum(t.quantity for t in order.transactions) == 2
    assert order.is_canceled()
    assert strategy.get_position(asset()) is None and strategy.get_position(asset("no")) is None


@pytest.mark.parametrize("action,limit", [("buy", 0.5), ("sell", 0.3), ("buy", None), ("sell", None)])
def test_post_only_crossing_is_not_replayed_as_a_taker_fill(action, limit):
    source = make_source()
    strategy, _ = make_strategy(source)
    order = strategy.create_order(asset(), 1, action, limit_price=limit, custom_params={"post_only": True})
    with pytest.raises(ValueError, match="post_only"):
        strategy.submit_order(order)


def test_normal_runner_records_settlement_without_an_external_cash_flow(monkeypatch):
    monkeypatch.setenv("BACKTESTING_DATA_SOURCE", "kalshi")
    prepared = make_source()

    class HoldContract(Strategy):
        def initialize(self):
            self.sleeptime = "1M"
            self.set_market("24/7")
            self.vars.submitted = False

        def on_trading_iteration(self):
            if not self.vars.submitted:
                self.submit_order(self.create_order(asset(), 1, "buy"))
                self.vars.submitted = True

    result, strategy = HoldContract.run_backtest(
        KalshiBacktesting,
        prepared.datetime_start,
        # Use the requested bound, not the already end-clipped source bound.
        pd.Timestamp("2026-01-01T12:06Z").to_pydatetime(),
        pandas_data=prepared.pandas_data,
        market_metadata=prepared._market_metadata,
        fee_rate=0,
        market="24/7",
        timestep="minute",
        budget=100,
        benchmark_asset=None,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=False,
        save_stats_file=False,
        show_progress_bar=False,
    )
    assert result is not None
    assert strategy.get_position(asset()) is None
    events = strategy.broker._trade_event_log_df
    settled = events.loc[events["status"] == strategy.broker.CASH_SETTLED]
    assert len(settled) == 1
    assert pd.Timestamp(settled.iloc[0]["time"]) == pd.Timestamp("2026-01-01T12:04Z")
    fills = [o for o in strategy.broker.get_all_orders() if o.is_filled() and o.order_type == "market"]
    assert len(fills) == 1
    assert strategy.cash == pytest.approx(101 - fills[0].avg_fill_price)
    cash_events = events.loc[events["event_kind"] == "cash_event"]
    assert len(cash_events) == 1 and not cash_events.iloc[0]["is_external_cash_flow"]


def test_negative_history_timeshift_cannot_expose_a_future_completed_candle():
    source = make_source()
    now = pd.Timestamp("2026-01-01T12:00:30Z")
    source._update_datetime(now.to_pydatetime())
    for shift in (-1, pd.Timedelta(minutes=-2)):
        frame = source.get_historical_prices(asset(), 100, timeshift=shift).df
        assert frame.index.max() <= now
        assert list(frame["close"]) == [0.4]
