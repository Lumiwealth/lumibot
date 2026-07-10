import time

import pytest

from lumibot.brokers.alpaca import Alpaca
from lumibot.brokers.tradier import Tradier
from lumibot.credentials import ALPACA_TEST_CONFIG, TRADIER_TEST_CONFIG
from lumibot.entities import Asset, Order
from lumibot.strategies.strategy import Strategy
from lumibot.traders.trader import Trader

pytestmark = [pytest.mark.apitest, pytest.mark.broker_strategy_live]


_CANCELLED_STATUSES = {"canceled", "cancelled"}
_TERMINAL_STATUSES = _CANCELLED_STATUSES | {"error", "expired", "fill", "filled", "rejected"}
_LIVE_ACTIVE_STATUSES = {
    "accepted",
    "accepted_for_bidding",
    "held",
    "new",
    "open",
    "partially_filled",
    "pending",
    "pending_cancel",
    "pending_new",
    "queued",
    "submitted",
}


def _normalized_order_status(record):
    if record is None:
        return None
    if isinstance(record, str):
        raw_status = record
    elif isinstance(record, dict):
        raw_status = record.get("status")
    else:
        raw_status = getattr(record, "status", None)
    if hasattr(raw_status, "value"):
        raw_status = raw_status.value
    if raw_status is None:
        return None
    status = str(raw_status).lower()
    if "." in status:
        status = status.rsplit(".", 1)[-1]
    return status


def _pull_order_status(broker, identifier):
    return _normalized_order_status(broker._pull_broker_order(identifier))


def _wait_for_broker_status(broker, identifier, *, timeout=30, expected_statuses=None):
    deadline = time.time() + timeout
    last_status = None
    while time.time() < deadline:
        last_status = _pull_order_status(broker, identifier)
        if last_status and (expected_statuses is None or last_status in expected_statuses):
            return last_status
        time.sleep(0.5)
    return last_status


def _cancel_order_until_terminal(broker, order, *, timeout=30, retry_interval=2):
    identifier = getattr(order, "identifier", None)
    if not identifier:
        raise AssertionError("Cannot clean up a broker order without an identifier")

    deadline = time.time() + timeout
    last_status = None
    last_error = None
    while time.time() < deadline:
        try:
            last_status = _pull_order_status(broker, identifier)
            last_error = None
        except Exception as exc:
            last_error = exc

        if last_status in _TERMINAL_STATUSES:
            return last_status

        try:
            broker.cancel_order(order)
        except Exception as exc:
            last_error = exc

        time.sleep(min(retry_interval, max(0, deadline - time.time())))

    try:
        last_status = _pull_order_status(broker, identifier)
        last_error = None
    except Exception as exc:
        last_error = exc
    if last_status in _TERMINAL_STATUSES:
        return last_status

    message = (
        f"Broker order {identifier} did not reach a terminal status within {timeout} seconds; "
        f"last status: {last_status!r}"
    )
    if last_error is not None:
        raise AssertionError(message) from last_error
    raise AssertionError(message)


def _alpaca() -> Alpaca:
    if not ALPACA_TEST_CONFIG.get("API_KEY") or not ALPACA_TEST_CONFIG.get("API_SECRET"):
        pytest.skip("Missing ALPACA_TEST_API_KEY / ALPACA_TEST_API_SECRET in .env")

    return Alpaca(
        dict(ALPACA_TEST_CONFIG),
        max_workers=1,
        connect_stream=False,
        start_orders_thread=False,
    )


def _tradier() -> Tradier:
    if not TRADIER_TEST_CONFIG.get("ACCOUNT_NUMBER") or not TRADIER_TEST_CONFIG.get("ACCESS_TOKEN"):
        pytest.skip("Missing TRADIER_TEST_ACCOUNT_NUMBER / TRADIER_TEST_ACCESS_TOKEN in .env")

    return Tradier(
        config=dict(TRADIER_TEST_CONFIG),
        max_workers=1,
        connect_stream=False,
    )


def _stock_limit_order(strategy_name):
    return Order(
        strategy_name,
        Asset("AAPL", asset_type=Asset.AssetType.STOCK),
        quantity=1,
        side=Order.OrderSide.BUY,
        limit_price=0.01,
        time_in_force="gtc",
        order_type=Order.OrderType.LIMIT,
    )


def _assert_direct_broker_order_lifecycle(broker, *, strategy_name):
    submitted = None
    try:
        submitted = broker._submit_order(_stock_limit_order(strategy_name))
        assert submitted is not None
        assert submitted.identifier, "Broker _submit_order() did not return an order id"

        status_before_cancel = _wait_for_broker_status(broker, submitted.identifier, timeout=30)
        assert status_before_cancel in _LIVE_ACTIVE_STATUSES, (
            f"Unexpected broker status before cancel: {status_before_cancel!r}"
        )

        status_after_cancel = _cancel_order_until_terminal(broker, submitted)
        assert status_after_cancel in _CANCELLED_STATUSES, (
            f"Broker order was not canceled: {status_after_cancel!r}"
        )
    finally:
        if submitted is not None and submitted.identifier:
            _cancel_order_until_terminal(broker, submitted)


class _GtcLimitSubmitCancelStrategy(Strategy):
    def initialize(self, parameters=None):
        self.sleeptime = "1S"
        self.submitted_order = None
        self.submitted_identifier = None
        self.status_before_cancel = None
        self.status_after_cancel = None
        self.cancel_requested = False
        self.cancel_error = None
        self.cash_seen = False
        self.positions_seen = False
        self.iteration_ran = False
        self.strategy_end_ran = False

    def before_starting_trading(self):
        self.cash_seen = self.get_cash() is not None
        self.positions_seen = self.get_positions() is not None

    def on_trading_iteration(self):
        asset = Asset(self.parameters["symbol"], asset_type=Asset.AssetType.STOCK)
        order = self.create_order(
            asset,
            self.parameters["quantity"],
            Order.OrderSide.BUY,
            limit_price=self.parameters["limit_price"],
            order_type=Order.OrderType.LIMIT,
            time_in_force="gtc",
        )

        self.submitted_order = self.submit_order(order)
        self.submitted_identifier = getattr(self.submitted_order, "identifier", None)
        if self.submitted_identifier:
            self.status_before_cancel = _wait_for_broker_status(
                self.broker,
                self.submitted_identifier,
                timeout=30,
            )
        self.iteration_ran = True

    def on_strategy_end(self):
        self.strategy_end_ran = True
        self._cancel_submitted_order()

    def on_bot_crash(self, error):
        self._cancel_submitted_order()

    def _cancel_submitted_order(self):
        if self.submitted_order is None or not self.submitted_identifier:
            return
        self.cancel_requested = True
        try:
            self.status_after_cancel = _cancel_order_until_terminal(
                self.broker,
                self.submitted_order,
                timeout=30,
            )
            self.cancel_error = None
        except Exception as exc:
            self.cancel_error = repr(exc)


def _run_live_strategy(broker, *, name):
    broker.is_market_open = lambda: True
    strategy = _GtcLimitSubmitCancelStrategy(
        broker=broker,
        name=name,
        benchmark_asset=None,
        analyze_backtest=False,
        should_backup_variables_to_database=False,
        should_send_summary_to_discord=False,
        parameters={
            "symbol": "AAPL",
            "quantity": 1,
            "limit_price": 0.01,
        },
    )
    # This CI test validates the order lifecycle inside a real strategy run. It bypasses only the scheduler's
    # market-hours gate so pull-request CI is not tied to US equity session timing. Alpaca run_once has an early
    # UTC-hours precheck, so patch the executor gate directly instead of relying on broker.is_market_open.
    strategy._executor._initialize_live_market_calendars_for_run_once = (
        lambda: setattr(strategy._executor, "_run_once_market_open_override", True)
    )
    trader = Trader(logfile="", backtest=False)
    trader.add_strategy(strategy)
    try:
        result = trader.run_all(run_once=True)
    finally:
        _ensure_strategy_order_terminal(strategy, broker)
    return strategy, result


def _ensure_strategy_order_terminal(strategy, broker):
    if strategy.submitted_order is None or not strategy.submitted_identifier:
        return
    if _normalized_order_status(strategy.status_after_cancel) in _TERMINAL_STATUSES:
        return

    strategy.cancel_requested = True
    try:
        strategy.status_after_cancel = _cancel_order_until_terminal(
            broker,
            strategy.submitted_order,
        )
    except Exception as exc:
        if strategy.cancel_error is None:
            strategy.cancel_error = repr(exc)


def _assert_strategy_order_lifecycle(strategy, result):
    assert result is not None
    assert strategy.cash_seen
    assert strategy.positions_seen
    assert strategy.iteration_ran
    assert strategy.strategy_end_ran
    assert strategy.submitted_order is not None
    assert strategy.submitted_identifier, "Strategy submit_order() did not return a broker order id"
    assert strategy.status_before_cancel in _LIVE_ACTIVE_STATUSES, (
        f"Unexpected broker status before cancel: {strategy.status_before_cancel!r}"
    )
    assert strategy.cancel_requested
    assert strategy.cancel_error is None
    assert strategy.status_after_cancel in _CANCELLED_STATUSES, (
        f"Broker order was not canceled: {strategy.status_after_cancel!r}"
    )


def test_alpaca_paper_broker_submits_polls_and_cancels_gtc_limit_order():
    broker = _alpaca()
    try:
        _assert_direct_broker_order_lifecycle(broker, strategy_name="alpaca-paper-broker-live-ci")
    finally:
        broker.cleanup_streams()


def test_tradier_paper_broker_submits_polls_and_cancels_gtc_limit_order():
    broker = _tradier()
    try:
        _assert_direct_broker_order_lifecycle(broker, strategy_name="tradier-paper-broker-live-ci")
    finally:
        broker.cleanup_streams()


def test_alpaca_paper_strategy_run_submits_polls_and_cancels_gtc_limit_order():
    broker = _alpaca()
    try:
        strategy, result = _run_live_strategy(broker, name="alpaca-paper-strategy-live-ci")
        _assert_strategy_order_lifecycle(strategy, result)
    finally:
        broker.cleanup_streams()


def test_tradier_paper_strategy_run_submits_polls_and_cancels_gtc_limit_order():
    broker = _tradier()
    try:
        strategy, result = _run_live_strategy(broker, name="tradier-paper-strategy-live-ci")
        _assert_strategy_order_lifecycle(strategy, result)
    finally:
        broker.cleanup_streams()
