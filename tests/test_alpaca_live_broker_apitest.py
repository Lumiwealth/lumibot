import time
from datetime import datetime, timedelta

import pytest

from lumibot.brokers.alpaca import Alpaca
from lumibot.components.options_helper import OptionsHelper
from lumibot.credentials import ALPACA_TEST_CONFIG
from lumibot.entities import Asset, Order
from lumibot.strategies.strategy import Strategy

pytestmark = pytest.mark.apitest


def _require_alpaca() -> Alpaca:
    api_key = ALPACA_TEST_CONFIG.get("API_KEY")
    api_secret = ALPACA_TEST_CONFIG.get("API_SECRET")
    if not api_key or not api_secret or api_key == "<your key here>" or api_secret == "<your key here>":
        pytest.skip("Missing ALPACA_TEST_API_KEY / ALPACA_TEST_API_SECRET")

    broker = Alpaca(ALPACA_TEST_CONFIG, max_workers=1, connect_stream=False)
    try:
        account = broker.api.get_account()
    except Exception as exc:
        broker.cleanup_streams()
        raise RuntimeError(f"Alpaca paper account authentication failed: {exc}") from exc

    if getattr(account, "trading_blocked", False):
        broker.cleanup_streams()
        pytest.skip("Alpaca paper account is trading-blocked")

    return broker


def _status_text(raw_order) -> str:
    status = getattr(raw_order, "status", "")
    if hasattr(status, "value"):
        status = status.value
    return str(status).lower()


def _wait_for_terminal_cancel(broker: Alpaca, order_id: str, *, timeout: float = 30.0) -> str:
    deadline = time.time() + timeout
    last_status = ""
    while time.time() < deadline:
        raw = broker.api.get_order_by_id(order_id)
        last_status = _status_text(raw)
        if last_status in {"canceled", "cancelled"} or last_status.endswith(".canceled"):
            return last_status
        if last_status in {"filled", "rejected", "expired"}:
            return last_status
        time.sleep(0.25)
    return last_status


class _LiveOrderDataStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1S"
        self.vars.iterations = 0
        self.vars.submitted_order_id = None
        self.vars.stock_price = None
        self.vars.bar_count = 0

    def on_trading_iteration(self):
        self.vars.iterations += 1

        asset = Asset("AAPL", asset_type=Asset.AssetType.STOCK)
        price = self.get_last_price(asset)
        assert price is not None and float(price) > 0
        self.vars.stock_price = float(price)

        bars = self.get_historical_prices(asset, 3, "day")
        assert bars is not None and bars.df is not None and not bars.df.empty
        self.vars.bar_count = len(bars.df)

        # Non-marketable by design: CI must prove real submit/cancel wiring without taking a fill.
        order = self.create_order(
            asset,
            1,
            Order.OrderSide.BUY,
            order_type=Order.OrderType.LIMIT,
            limit_price=0.01,
            time_in_force="day",
        )
        submitted = self.submit_order(order)
        assert submitted is not None and submitted.identifier
        self.vars.submitted_order_id = submitted.identifier
        self.broker.cancel_order(submitted)


class _LiveOptionsChainStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1S"
        self.options_helper = OptionsHelper(self)
        self.vars.iterations = 0
        self.vars.call_expirations = 0
        self.vars.put_expirations = 0
        self.vars.option_symbol = None

    def on_trading_iteration(self):
        self.vars.iterations += 1

        underlying = Asset("SPY", asset_type=Asset.AssetType.STOCK)
        underlying_price = self.get_last_price(underlying)
        assert underlying_price is not None and float(underlying_price) > 0

        chains = self.get_chains(underlying)
        chain_root = chains.get("Chains", {}) if isinstance(chains, dict) else {}
        call_chains = chain_root.get("CALL", {})
        put_chains = chain_root.get("PUT", {})
        assert call_chains, "Alpaca returned no SPY call chains"
        assert put_chains, "Alpaca returned no SPY put chains"
        self.vars.call_expirations = len(call_chains)
        self.vars.put_expirations = len(put_chains)

        target_date = datetime.now().astimezone().date() + timedelta(days=7)
        expiry = self.options_helper.get_expiration_on_or_after_date(
            target_date,
            chains,
            "call",
            underlying_asset=underlying,
        )
        assert expiry is not None

        expiry_key = expiry.strftime("%Y-%m-%d") if hasattr(expiry, "strftime") else str(expiry)
        strikes = call_chains.get(expiry_key)
        assert strikes, f"Alpaca returned no SPY call strikes for {expiry_key}"
        strike = min(strikes, key=lambda value: abs(float(value) - float(underlying_price)))

        option = self.options_helper.find_next_valid_option(underlying, strike, expiry, put_or_call="call")
        assert option is not None
        self.vars.option_symbol = str(option)


def test_alpaca_run_once_strategy_reads_data_submits_and_cancels_real_order():
    broker = _require_alpaca()
    strategy = _LiveOrderDataStrategy(
        broker=broker,
        name="alpaca-live-broker-order-ci",
        benchmark_asset=None,
        should_backup_variables_to_database=False,
        should_send_summary_to_discord=False,
        save_logfile=False,
    )

    try:
        strategy.run_live(run_once=True)
        assert strategy.vars.iterations == 1
        assert strategy.vars.stock_price > 0
        assert strategy.vars.bar_count >= 1
        assert strategy.vars.submitted_order_id

        status = _wait_for_terminal_cancel(broker, strategy.vars.submitted_order_id)
        assert status in {"canceled", "cancelled"} or status.endswith(".canceled")

        all_orders = broker._pull_broker_all_orders()
        assert all_orders is not None
    finally:
        try:
            strategy.cancel_open_orders()
        except Exception:
            pass
        broker.cleanup_streams()


def test_alpaca_run_once_strategy_reads_options_chain_through_broker_data_source():
    broker = _require_alpaca()
    strategy = _LiveOptionsChainStrategy(
        broker=broker,
        name="alpaca-live-broker-options-ci",
        benchmark_asset=None,
        should_backup_variables_to_database=False,
        should_send_summary_to_discord=False,
        save_logfile=False,
    )

    try:
        strategy.run_live(run_once=True)
        assert strategy.vars.iterations == 1
        assert strategy.vars.call_expirations > 0
        assert strategy.vars.put_expirations > 0
        assert strategy.vars.option_symbol
    finally:
        broker.cleanup_streams()
