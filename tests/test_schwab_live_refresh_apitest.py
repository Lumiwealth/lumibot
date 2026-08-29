import asyncio
import os
import time
from datetime import date
from pathlib import Path

import pytest

from lumibot.brokers.schwab import Schwab
from lumibot.entities import Asset, Order

pytestmark = pytest.mark.apitest


def _schwab_broker(monkeypatch):
    token_path = Path("schwab_token.json").resolve()
    if not token_path.exists():
        pytest.skip("Missing local schwab_token.json token file.")

    account_number = (os.environ.get("SCHWAB_ACCOUNT_NUMBER") or "").strip()
    if not account_number:
        pytest.skip(
            "Missing existing SCHWAB_ACCOUNT_NUMBER credential. "
            "This live apitest may place and cancel a real Schwab order."
        )

    monkeypatch.setattr(Schwab, "_launch_stream", lambda self: None)
    return Schwab(
        config={
            "SCHWAB_TOKEN_PATH": str(token_path),
            "SCHWAB_ACCOUNT_NUMBER": account_number,
            "MARKET": "NASDAQ",
        }
    )


def _pull_all_for_order(broker, order_id, strategy_name):
    raw_orders = broker._pull_broker_all_orders()
    raw = next((row for row in raw_orders if str(row.get("orderId")) == str(order_id)), None)
    parsed = broker._parse_broker_order(raw, strategy_name) if raw else None
    return raw, parsed


def _poll_all_for_order(broker, order_id, strategy_name, *, timeout_seconds=4.0):
    deadline = time.perf_counter() + timeout_seconds
    while True:
        raw, parsed = _pull_all_for_order(broker, order_id, strategy_name)
        if raw or time.perf_counter() >= deadline:
            return raw, parsed
        time.sleep(0.25)


def _account_activity_message_type(message):
    content = message.get("content") if isinstance(message, dict) else None
    first = content[0] if isinstance(content, list) and content else {}
    if not isinstance(first, dict):
        return "unknown"
    return str(first.get("MESSAGE_TYPE") or first.get("message_type") or "unknown")


def _select_tsll_call_asset(broker):
    underlying = Asset("TSLL", asset_type=Asset.AssetType.STOCK)
    chains = broker.data_source.get_chains(underlying, strike_count=20)
    call_chains = (chains or {}).get("Chains", {}).get("CALL", {})
    if not call_chains:
        pytest.skip("No TSLL call option chain available from Schwab.")

    today = date.today()
    expirations = sorted(
        expiration
        for expiration in call_chains
        if date.fromisoformat(expiration) >= today
    )
    if not expirations:
        pytest.skip("No future TSLL call expiration available from Schwab.")

    expiration = expirations[0]
    strikes = call_chains.get(expiration) or []
    if not strikes:
        pytest.skip("No TSLL call strikes available from Schwab.")

    quote = broker.data_source.get_quote(underlying)
    last_price = float(getattr(quote, "price", None) or broker.data_source.get_last_price(underlying))
    strike = min(strikes, key=lambda value: abs(float(value) - last_price))

    return Asset(
        "TSLL",
        asset_type=Asset.AssetType.OPTION,
        expiration=date.fromisoformat(expiration),
        strike=float(strike),
        right="CALL",
    )


def test_schwab_live_submit_read_cancel_refresh(monkeypatch):
    """Places one TSLL limit order, verifies fresh reads, and cancels it.

    This is intentionally marked apitest because it uses the real Schwab API.
    It skips during market hours to avoid an immediate fill from the at-price
    limit order.
    """

    broker = _schwab_broker(monkeypatch)
    if broker.is_market_open():
        pytest.skip("Market is open; not placing an at-price TSLL limit order.")

    strategy_name = "schwab-live-refresh-apitest"
    asset = Asset("TSLL", asset_type=Asset.AssetType.STOCK)
    quote = broker.data_source.get_quote(asset)
    last_price = float(getattr(quote, "price", None) or broker.data_source.get_last_price(asset))
    assert last_price > 0

    order = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.LIMIT,
        limit_price=round(last_price, 2),
        time_in_force="day",
        tag=strategy_name,
    )

    submitted = None
    try:
        submitted = broker._submit_order(order)
        assert submitted is not None
        assert submitted.identifier

        raw_single = broker._pull_broker_order(submitted.identifier)
        parsed_single = broker._parse_broker_order(raw_single, strategy_name)
        assert raw_single is not None
        assert parsed_single is not None
        assert str(parsed_single.identifier) == str(submitted.identifier)

        raw_all, parsed_all = _poll_all_for_order(broker, submitted.identifier, strategy_name)
        assert raw_all is not None
        assert parsed_all is not None
        assert str(parsed_all.identifier) == str(submitted.identifier)

        broker.cancel_order(submitted)

        post_cancel_single = broker._pull_broker_order(submitted.identifier)
        post_cancel_parsed = broker._parse_broker_order(post_cancel_single, strategy_name)
        assert post_cancel_single["status"] in {"CANCELED", "CANCELLED"}
        assert post_cancel_parsed.is_canceled()

        post_cancel_all, post_cancel_all_parsed = _poll_all_for_order(
            broker, submitted.identifier, strategy_name
        )
        assert post_cancel_all["status"] in {"CANCELED", "CANCELLED"}
        assert post_cancel_all_parsed.is_canceled()
    finally:
        if submitted and submitted.identifier:
            try:
                raw = broker._pull_broker_order(submitted.identifier)
                status = str(raw.get("status", "") if raw else "").upper()
                if status not in {"CANCELED", "CANCELLED", "FILLED", "REJECTED", "EXPIRED"}:
                    broker.cancel_order(submitted)
            except Exception:
                pass


def test_schwab_live_account_activity_stream_observes_order_change(monkeypatch):
    """Connects the real account stream and verifies an order change wakes it.

    Raw provider messages are deliberately not written to test output or disk.
    The product adapter treats them as opaque wake-ups and reconciles through an
    exact REST order read.
    """

    broker = _schwab_broker(monkeypatch)
    strategy_name = "schwab-live-account-activity-apitest"
    submitted = None
    observed_message_types = []

    order = Order(
        strategy=strategy_name,
        asset=Asset("TSLL", asset_type=Asset.AssetType.STOCK),
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.LIMIT,
        limit_price=0.01,
        time_in_force="day",
        tag=strategy_name,
    )

    async def exercise_stream():
        nonlocal submitted

        def record_activity(message):
            observed_message_types.append(_account_activity_message_type(message))

        stream_client = broker.stream_client
        stream_client.add_account_activity_handler(record_activity)
        await broker._configure_schwab_account_activity_stream(stream_client)
        try:
            submitted = await asyncio.to_thread(broker._submit_order, order)
            assert submitted is not None
            assert submitted.identifier
            try:
                await asyncio.to_thread(broker.cancel_order, submitted)
            except Exception:
                # The order may transition to rejected before after-hours cancel;
                # that broker transition should still produce account activity.
                pass

            deadline = time.monotonic() + 10
            while not observed_message_types and time.monotonic() < deadline:
                await asyncio.wait_for(stream_client.handle_message(), timeout=5)
        finally:
            try:
                await stream_client.account_activity_unsubs()
            finally:
                await stream_client.logout()

    try:
        asyncio.run(exercise_stream())
        assert observed_message_types
    finally:
        if submitted and submitted.identifier:
            try:
                raw = broker._pull_broker_order(submitted.identifier)
                status = str(raw.get("status", "") if raw else "").upper()
                if status not in {"CANCELED", "CANCELLED", "FILLED", "REJECTED", "EXPIRED"}:
                    broker.cancel_order(submitted)
            except Exception:
                pass


def test_schwab_live_option_modify_read_cancel(monkeypatch):
    """Places, replaces, reads, and cancels one real TSLL call limit order.

    This is intentionally marked apitest because it uses the real Schwab API.
    The limit is deliberately far below the selected option's expected market
    value so the order should rest, then be replaced, then be canceled.
    """

    broker = _schwab_broker(monkeypatch)
    strategy_name = "schwab-live-option-modify-apitest"
    asset = _select_tsll_call_asset(broker)
    submitted = None

    order = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.BUY_TO_OPEN,
        order_type=Order.OrderType.LIMIT,
        limit_price=0.05,
        time_in_force="day",
        tag=strategy_name,
    )

    try:
        submitted = broker._submit_order(order)
        assert submitted is not None
        assert submitted.identifier

        raw_single = broker._pull_broker_order(submitted.identifier)
        parsed_single = broker._parse_broker_order(raw_single, strategy_name)
        assert raw_single is not None
        assert parsed_single is not None
        assert str(parsed_single.identifier) == str(submitted.identifier)

        raw_all, parsed_all = _poll_all_for_order(broker, submitted.identifier, strategy_name)
        assert raw_all is not None
        assert parsed_all is not None
        assert str(parsed_all.identifier) == str(submitted.identifier)

        original_id = submitted.identifier
        broker._modify_order(submitted, limit_price=0.10)
        assert submitted.identifier
        assert submitted.identifier != original_id
        assert original_id in getattr(submitted, "previous_identifiers", [])
        assert submitted.limit_price == 0.10

        replaced_single = broker._pull_broker_order(submitted.identifier)
        replaced_parsed = broker._parse_broker_order(replaced_single, strategy_name)
        assert replaced_single is not None
        assert replaced_parsed is not None
        assert str(replaced_parsed.identifier) == str(submitted.identifier)

        broker.cancel_order(submitted)

        post_cancel_single = broker._pull_broker_order(submitted.identifier)
        post_cancel_parsed = broker._parse_broker_order(post_cancel_single, strategy_name)
        assert post_cancel_single["status"] in {"CANCELED", "CANCELLED"}
        assert post_cancel_parsed.is_canceled()
    finally:
        if submitted and submitted.identifier:
            try:
                raw = broker._pull_broker_order(submitted.identifier)
                status = str(raw.get("status", "") if raw else "").upper()
                if status not in {"CANCELED", "CANCELLED", "FILLED", "REJECTED", "EXPIRED"}:
                    broker.cancel_order(submitted)
            except Exception:
                pass


def test_schwab_live_option_limit_wait_then_cancel_refresh(monkeypatch):
    """Places a safe option limit order, waits, cancels, and confirms Schwab state.

    This covers the Titus-style path where strategy code submits an option limit
    order, gives Schwab a short window to fill it, then cancels if it is still
    open. The low limit is intended to rest instead of filling.
    """

    broker = _schwab_broker(monkeypatch)
    strategy_name = "schwab-live-option-timeout-cancel-apitest"
    asset = _select_tsll_call_asset(broker)
    submitted = None

    order = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.BUY_TO_OPEN,
        order_type=Order.OrderType.LIMIT,
        limit_price=0.05,
        time_in_force="day",
        tag=strategy_name,
    )

    try:
        submitted = broker._submit_order(order)
        assert submitted is not None
        assert submitted.identifier

        raw_single = broker._pull_broker_order(submitted.identifier)
        parsed_single = broker._parse_broker_order(raw_single, strategy_name)
        assert raw_single is not None
        assert parsed_single is not None
        assert str(parsed_single.identifier) == str(submitted.identifier)

        raw_all, parsed_all = _poll_all_for_order(broker, submitted.identifier, strategy_name)
        assert raw_all is not None
        assert parsed_all is not None
        assert str(parsed_all.identifier) == str(submitted.identifier)

        time.sleep(4)
        broker.cancel_order(submitted)

        deadline = time.perf_counter() + 8
        post_cancel_single = None
        post_cancel_parsed = None
        while time.perf_counter() < deadline:
            post_cancel_single = broker._pull_broker_order(submitted.identifier)
            post_cancel_parsed = broker._parse_broker_order(post_cancel_single, strategy_name)
            if post_cancel_parsed and post_cancel_parsed.is_canceled():
                break
            time.sleep(0.5)

        assert post_cancel_single is not None
        assert post_cancel_parsed is not None
        assert post_cancel_parsed.is_canceled()

        post_cancel_all, post_cancel_all_parsed = _poll_all_for_order(
            broker, submitted.identifier, strategy_name
        )
        assert post_cancel_all is not None
        assert post_cancel_all_parsed is not None
        assert post_cancel_all_parsed.is_canceled()
    finally:
        if submitted and submitted.identifier:
            try:
                raw = broker._pull_broker_order(submitted.identifier)
                status = str(raw.get("status", "") if raw else "").upper()
                if status not in {"CANCELED", "CANCELLED", "FILLED", "REJECTED", "EXPIRED"}:
                    broker.cancel_order(submitted)
            except Exception:
                pass


def test_schwab_live_oto_submit_read_cancel(monkeypatch):
    """Places a safe nonmarketable OTO, reads parent/child structure, and cancels.

    This verifies Schwab accepts the TRIGGER order payload. It intentionally
    uses nonmarketable stock limits and skips while the market is open so the
    parent should rest instead of filling and activating the child.
    """

    broker = _schwab_broker(monkeypatch)
    if broker.is_market_open():
        pytest.skip("Market is open; not placing a nonmarketable OTO smoke order.")

    strategy_name = "schwab-live-oto-apitest"
    asset = Asset("TSLL", asset_type=Asset.AssetType.STOCK)
    quote = broker.data_source.get_quote(asset)
    last_price = float(getattr(quote, "price", None) or broker.data_source.get_last_price(asset))
    assert last_price > 0

    parent_limit = max(round(last_price * 0.50, 2), 0.01)
    child_limit = round(last_price * 2.0, 2)
    child_order = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.SELL,
        order_type=Order.OrderType.LIMIT,
        limit_price=child_limit,
        time_in_force="day",
        tag=strategy_name,
    )
    order = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.LIMIT,
        limit_price=parent_limit,
        order_class=Order.OrderClass.OTO,
        child_orders=[child_order],
        time_in_force="day",
        tag=strategy_name,
    )

    submitted = None
    try:
        submitted = broker._submit_order(order)
        assert submitted is not None
        assert submitted.identifier

        raw_single = broker._pull_broker_order(submitted.identifier)
        parsed_single = broker._parse_broker_order(raw_single, strategy_name)
        assert raw_single is not None
        assert raw_single.get("orderStrategyType") == "TRIGGER"
        assert raw_single.get("childOrderStrategies")
        assert parsed_single is not None
        assert parsed_single.order_class == Order.OrderClass.OTO
        assert parsed_single.child_orders

        broker.cancel_order(submitted)

        post_cancel_single = broker._pull_broker_order(submitted.identifier)
        post_cancel_parsed = broker._parse_broker_order(post_cancel_single, strategy_name)
        assert post_cancel_single["status"] in {"CANCELED", "CANCELLED"}
        assert post_cancel_parsed.is_canceled()
    finally:
        if submitted and submitted.identifier:
            try:
                raw = broker._pull_broker_order(submitted.identifier)
                status = str(raw.get("status", "") if raw else "").upper()
                if status not in {"CANCELED", "CANCELLED", "FILLED", "REJECTED", "EXPIRED"}:
                    broker.cancel_order(submitted)
            except Exception:
                pass


def test_schwab_live_oco_submit_read_cancel(monkeypatch):
    """Places a safe nonmarketable OCO, reads child structure, and cancels."""

    broker = _schwab_broker(monkeypatch)
    if broker.is_market_open():
        pytest.skip("Market is open; not placing a nonmarketable OCO smoke order.")

    strategy_name = "schwab-live-oco-apitest"
    asset = Asset("TSLL", asset_type=Asset.AssetType.STOCK)
    quote = broker.data_source.get_quote(asset)
    last_price = float(getattr(quote, "price", None) or broker.data_source.get_last_price(asset))
    assert last_price > 0

    limit_order = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.LIMIT,
        limit_price=max(round(last_price * 0.50, 2), 0.01),
        time_in_force="day",
        tag=strategy_name,
    )
    stop_order = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.STOP,
        stop_price=round(last_price * 2.0, 2),
        time_in_force="day",
        tag=strategy_name,
    )
    order = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.BUY,
        order_class=Order.OrderClass.OCO,
        child_orders=[limit_order, stop_order],
        time_in_force="day",
        tag=strategy_name,
    )

    submitted = None
    try:
        submitted = broker._submit_order(order)
        assert submitted is not None
        assert submitted.identifier

        raw_single = broker._pull_broker_order(submitted.identifier)
        parsed_single = broker._parse_broker_order(raw_single, strategy_name)
        assert raw_single is not None
        assert raw_single.get("orderStrategyType") == "OCO"
        assert len(raw_single.get("childOrderStrategies") or []) == 2
        assert parsed_single is not None
        assert parsed_single.order_class == Order.OrderClass.OCO
        assert len(parsed_single.child_orders) == 2

        broker.cancel_order(submitted)

        post_cancel_single = broker._pull_broker_order(submitted.identifier)
        post_cancel_parsed = broker._parse_broker_order(post_cancel_single, strategy_name)
        assert post_cancel_single["status"] in {"CANCELED", "CANCELLED"}
        assert post_cancel_parsed.is_canceled()
    finally:
        if submitted and submitted.identifier:
            try:
                raw = broker._pull_broker_order(submitted.identifier)
                status = str(raw.get("status", "") if raw else "").upper()
                if status not in {"CANCELED", "CANCELLED", "FILLED", "REJECTED", "EXPIRED"}:
                    broker.cancel_order(submitted)
            except Exception:
                pass


def test_schwab_live_bracket_submit_read_cancel(monkeypatch):
    """Places a safe nonmarketable bracket, reads trigger/OCO structure, and cancels."""

    broker = _schwab_broker(monkeypatch)
    if broker.is_market_open():
        pytest.skip("Market is open; not placing a nonmarketable bracket smoke order.")

    strategy_name = "schwab-live-bracket-apitest"
    asset = Asset("TSLL", asset_type=Asset.AssetType.STOCK)
    quote = broker.data_source.get_quote(asset)
    last_price = float(getattr(quote, "price", None) or broker.data_source.get_last_price(asset))
    assert last_price > 0

    take_profit = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.SELL,
        order_type=Order.OrderType.LIMIT,
        limit_price=round(last_price * 2.0, 2),
        time_in_force="day",
        tag=strategy_name,
    )
    stop_loss = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.SELL,
        order_type=Order.OrderType.STOP,
        stop_price=max(round(last_price * 0.25, 2), 0.01),
        time_in_force="day",
        tag=strategy_name,
    )
    order = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.LIMIT,
        limit_price=max(round(last_price * 0.50, 2), 0.01),
        order_class=Order.OrderClass.BRACKET,
        child_orders=[take_profit, stop_loss],
        time_in_force="day",
        tag=strategy_name,
    )

    submitted = None
    try:
        submitted = broker._submit_order(order)
        assert submitted is not None
        assert submitted.identifier

        raw_single = broker._pull_broker_order(submitted.identifier)
        parsed_single = broker._parse_broker_order(raw_single, strategy_name)
        assert raw_single is not None
        assert raw_single.get("orderStrategyType") == "TRIGGER"
        child_strategies = raw_single.get("childOrderStrategies") or []
        assert len(child_strategies) == 1
        assert child_strategies[0].get("orderStrategyType") == "OCO"
        assert parsed_single is not None
        assert parsed_single.order_class == Order.OrderClass.BRACKET
        assert len(parsed_single.child_orders) == 2

        broker.cancel_order(submitted)

        post_cancel_single = broker._pull_broker_order(submitted.identifier)
        post_cancel_parsed = broker._parse_broker_order(post_cancel_single, strategy_name)
        assert post_cancel_single["status"] in {"CANCELED", "CANCELLED"}
        assert post_cancel_parsed.is_canceled()
    finally:
        if submitted and submitted.identifier:
            try:
                raw = broker._pull_broker_order(submitted.identifier)
                status = str(raw.get("status", "") if raw else "").upper()
                if status not in {"CANCELED", "CANCELLED", "FILLED", "REJECTED", "EXPIRED"}:
                    broker.cancel_order(submitted)
            except Exception:
                pass


def test_schwab_live_option_oto_submit_read_cancel(monkeypatch):
    """Places a safe nonmarketable option OTO, reads structure, and cancels."""

    broker = _schwab_broker(monkeypatch)
    strategy_name = "schwab-live-option-oto-apitest"
    asset = _select_tsll_call_asset(broker)
    child_order = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.SELL_TO_CLOSE,
        order_type=Order.OrderType.LIMIT,
        limit_price=0.25,
        time_in_force="day",
        tag=strategy_name,
    )
    order = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.BUY_TO_OPEN,
        order_type=Order.OrderType.LIMIT,
        limit_price=0.05,
        order_class=Order.OrderClass.OTO,
        child_orders=[child_order],
        time_in_force="day",
        tag=strategy_name,
    )

    submitted = None
    try:
        submitted = broker._submit_order(order)
        assert submitted is not None
        assert submitted.identifier

        raw_single = broker._pull_broker_order(submitted.identifier)
        parsed_single = broker._parse_broker_order(raw_single, strategy_name)
        assert raw_single is not None
        assert raw_single.get("orderStrategyType") == "TRIGGER"
        assert raw_single.get("childOrderStrategies")
        assert parsed_single is not None
        assert parsed_single.order_class == Order.OrderClass.OTO
        assert parsed_single.child_orders

        broker.cancel_order(submitted)

        post_cancel_single = broker._pull_broker_order(submitted.identifier)
        post_cancel_parsed = broker._parse_broker_order(post_cancel_single, strategy_name)
        assert post_cancel_single["status"] in {"CANCELED", "CANCELLED"}
        assert post_cancel_parsed.is_canceled()
    finally:
        if submitted and submitted.identifier:
            try:
                raw = broker._pull_broker_order(submitted.identifier)
                status = str(raw.get("status", "") if raw else "").upper()
                if status not in {"CANCELED", "CANCELLED", "FILLED", "REJECTED", "EXPIRED"}:
                    broker.cancel_order(submitted)
            except Exception:
                pass


def test_schwab_live_option_oco_submit_read_cancel(monkeypatch):
    """Places a safe nonmarketable option OCO, reads child structure, and cancels."""

    broker = _schwab_broker(monkeypatch)
    strategy_name = "schwab-live-option-oco-apitest"
    asset = _select_tsll_call_asset(broker)
    first_order = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.BUY_TO_OPEN,
        order_type=Order.OrderType.LIMIT,
        limit_price=0.05,
        time_in_force="day",
        tag=strategy_name,
    )
    second_order = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.BUY_TO_OPEN,
        order_type=Order.OrderType.LIMIT,
        limit_price=0.10,
        time_in_force="day",
        tag=strategy_name,
    )
    order = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.BUY_TO_OPEN,
        order_class=Order.OrderClass.OCO,
        child_orders=[first_order, second_order],
        time_in_force="day",
        tag=strategy_name,
    )

    submitted = None
    try:
        submitted = broker._submit_order(order)
        assert submitted is not None
        assert submitted.identifier

        raw_single = broker._pull_broker_order(submitted.identifier)
        parsed_single = broker._parse_broker_order(raw_single, strategy_name)
        assert raw_single is not None
        assert raw_single.get("orderStrategyType") == "OCO"
        assert len(raw_single.get("childOrderStrategies") or []) == 2
        assert parsed_single is not None
        assert parsed_single.order_class == Order.OrderClass.OCO
        assert len(parsed_single.child_orders) == 2

        broker.cancel_order(submitted)

        post_cancel_single = broker._pull_broker_order(submitted.identifier)
        post_cancel_parsed = broker._parse_broker_order(post_cancel_single, strategy_name)
        assert post_cancel_single["status"] in {"CANCELED", "CANCELLED"}
        assert post_cancel_parsed.is_canceled()
    finally:
        if submitted and submitted.identifier:
            try:
                raw = broker._pull_broker_order(submitted.identifier)
                status = str(raw.get("status", "") if raw else "").upper()
                if status not in {"CANCELED", "CANCELLED", "FILLED", "REJECTED", "EXPIRED"}:
                    broker.cancel_order(submitted)
            except Exception:
                pass


def test_schwab_live_option_bracket_submit_read_cancel(monkeypatch):
    """Places a safe nonmarketable option bracket, reads trigger/OCO structure, and cancels."""

    broker = _schwab_broker(monkeypatch)
    strategy_name = "schwab-live-option-bracket-apitest"
    asset = _select_tsll_call_asset(broker)
    take_profit = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.SELL_TO_CLOSE,
        order_type=Order.OrderType.LIMIT,
        limit_price=0.25,
        time_in_force="day",
        tag=strategy_name,
    )
    stop_loss = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.SELL_TO_CLOSE,
        order_type=Order.OrderType.STOP,
        stop_price=0.01,
        time_in_force="day",
        tag=strategy_name,
    )
    order = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.BUY_TO_OPEN,
        order_type=Order.OrderType.LIMIT,
        limit_price=0.05,
        order_class=Order.OrderClass.BRACKET,
        child_orders=[take_profit, stop_loss],
        time_in_force="day",
        tag=strategy_name,
    )

    submitted = None
    try:
        submitted = broker._submit_order(order)
        assert submitted is not None
        assert submitted.identifier

        raw_single = broker._pull_broker_order(submitted.identifier)
        parsed_single = broker._parse_broker_order(raw_single, strategy_name)
        assert raw_single is not None
        assert raw_single.get("orderStrategyType") == "TRIGGER"
        child_strategies = raw_single.get("childOrderStrategies") or []
        assert len(child_strategies) == 1
        assert child_strategies[0].get("orderStrategyType") == "OCO"
        assert parsed_single is not None
        assert parsed_single.order_class == Order.OrderClass.BRACKET
        assert len(parsed_single.child_orders) == 2

        broker.cancel_order(submitted)

        post_cancel_single = broker._pull_broker_order(submitted.identifier)
        post_cancel_parsed = broker._parse_broker_order(post_cancel_single, strategy_name)
        assert post_cancel_single["status"] in {"CANCELED", "CANCELLED"}
        assert post_cancel_parsed.is_canceled()
    finally:
        if submitted and submitted.identifier:
            try:
                raw = broker._pull_broker_order(submitted.identifier)
                status = str(raw.get("status", "") if raw else "").upper()
                if status not in {"CANCELED", "CANCELLED", "FILLED", "REJECTED", "EXPIRED"}:
                    broker.cancel_order(submitted)
            except Exception:
                pass
