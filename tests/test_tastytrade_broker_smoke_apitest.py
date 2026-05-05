"""
Smoke tests for the Tastytrade broker.

The offline tests in this file are pure unit tests (no network, no
credentials) and run on every CI invocation. The single ``apitest``-marked
test at the bottom hits the Tastytrade sandbox and is gated on real
credentials being present in the environment.
"""

from __future__ import annotations

import asyncio
import datetime
import os
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest


def _make_broker(monkeypatch):
    """Build a Tastytrade broker wired against mocked SDK classes."""
    from lumibot.brokers import tastytrade as tt_mod

    fake_session = MagicMock(name="Session")
    monkeypatch.setattr(tt_mod, "_TTSession", MagicMock(return_value=fake_session))

    fake_account = MagicMock(name="Account")

    async def _get(_session, _account_number):
        return fake_account

    fake_account_cls = MagicMock()
    fake_account_cls.get.side_effect = _get
    monkeypatch.setattr(tt_mod, "_TTAccount", fake_account_cls)

    broker = tt_mod.Tastytrade(
        client_secret="cs",
        refresh_token="rt",
        account_number="ACC123",
        is_test=True,
        connect_stream=False,
    )
    return broker, fake_account, fake_session


# ---------------------------------------------------------------------------
# Offline tests (no credentials, no network)
# ---------------------------------------------------------------------------

def test_missing_credentials_raises():
    """Missing client_secret / refresh_token / account_number must raise ValueError."""
    from lumibot.brokers.tastytrade import Tastytrade

    # Strip any env vars that might leak in from the dev shell.
    env_keys = (
        "TASTYTRADE_CLIENT_SECRET",
        "TASTYTRADE_REFRESH_TOKEN",
        "TASTYTRADE_ACCOUNT_NUMBER",
        "TASTYTRADE_SANDBOX",
    )
    with patch.dict(os.environ, {k: "" for k in env_keys}, clear=False):
        with pytest.raises(ValueError) as excinfo:
            Tastytrade()
    assert "client_secret" in str(excinfo.value)
    assert "refresh_token" in str(excinfo.value)
    assert "account_number" in str(excinfo.value)


def test_async_bridge_runs_and_returns_value():
    """The asyncio bridge must execute coroutines and return their result."""
    from lumibot.brokers.tastytrade import _AsyncBridge

    bridge = _AsyncBridge()
    try:
        async def _add():
            await asyncio.sleep(0)
            return 42

        assert bridge.run(_add()) == 42
    finally:
        bridge.close()


def test_async_bridge_cancels_future_on_timeout():
    """If run() times out the underlying coroutine must be cancelled,
    not left running on the background loop."""
    import concurrent.futures
    import threading
    import time

    from lumibot.brokers.tastytrade import _AsyncBridge

    bridge = _AsyncBridge()
    try:
        # threading.Event (not asyncio.Event) — the signal flows from the
        # background asyncio thread to the main test thread, which is exactly
        # what threading.Event is for. asyncio.Event is bound to a single
        # loop and isn't safe across threads.
        cancelled = threading.Event()

        async def _slow():
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                cancelled.set()
                raise

        with pytest.raises(concurrent.futures.TimeoutError):
            bridge.run(_slow(), timeout=0.05)

        # Give cancellation a moment to propagate to the loop thread.
        for _ in range(20):
            if cancelled.is_set():
                break
            time.sleep(0.05)
        assert cancelled.is_set(), "coroutine was not cancelled after timeout"
    finally:
        bridge.close()


def test_parse_truthy_handles_string_false():
    """bool('false') is True; _parse_truthy must not be that naive."""
    from lumibot.brokers.tastytrade import Tastytrade

    # Truthy
    assert Tastytrade._parse_truthy("true") is True
    assert Tastytrade._parse_truthy("True") is True
    assert Tastytrade._parse_truthy("1") is True
    assert Tastytrade._parse_truthy("yes") is True
    assert Tastytrade._parse_truthy("on") is True
    assert Tastytrade._parse_truthy(True) is True
    # Falsy
    assert Tastytrade._parse_truthy("false") is False
    assert Tastytrade._parse_truthy("0") is False
    assert Tastytrade._parse_truthy("no") is False
    assert Tastytrade._parse_truthy("off") is False
    assert Tastytrade._parse_truthy("") is False
    assert Tastytrade._parse_truthy(None) is False
    assert Tastytrade._parse_truthy(False) is False


@patch("lumibot.brokers.tastytrade._TTAccount")
@patch("lumibot.brokers.tastytrade._TTSession")
def test_init_with_kwargs_resolves_account(mock_session_cls, mock_account_cls):
    """Constructor builds a Session, fetches the Account, and stores both."""
    from lumibot.brokers.tastytrade import Tastytrade

    fake_session = MagicMock(name="Session")
    mock_session_cls.return_value = fake_session

    fake_account = MagicMock(name="Account")

    async def _get(_session, _account_number):
        assert _account_number == "ACC123"
        return fake_account

    mock_account_cls.get.side_effect = _get

    broker = Tastytrade(
        client_secret="cs",
        refresh_token="rt",
        account_number="ACC123",
        is_test=True,
        connect_stream=False,
    )
    try:
        mock_session_cls.assert_called_once_with(
            provider_secret="cs",
            refresh_token="rt",
            is_test=True,
        )
        assert broker._session is fake_session
        assert broker._account is fake_account
        assert broker._tt_account_number == "ACC123"
        assert broker._tt_is_test is True
    finally:
        broker._async_bridge.close()


# ---------------------------------------------------------------------------
# Mapping helpers (pure functions, no broker required)
# ---------------------------------------------------------------------------

def test_to_occ_symbol_pads_root_and_strikes():
    from lumibot.brokers.tastytrade import Tastytrade
    from lumibot.entities import Asset

    asset = Asset(
        symbol="AAPL",
        asset_type=Asset.AssetType.OPTION,
        expiration=datetime.date(2026, 7, 17),
        strike=230,
        right=Asset.OptionRight.CALL,
    )
    assert Tastytrade._to_occ_symbol(asset) == "AAPL  260717C00230000"

    spx_put = Asset(
        symbol="SPX",
        asset_type=Asset.AssetType.OPTION,
        expiration=datetime.date(2026, 5, 16),
        strike=4500.5,
        right=Asset.OptionRight.PUT,
    )
    assert Tastytrade._to_occ_symbol(spx_put) == "SPX   260516P04500500"


def test_occ_to_asset_round_trip():
    from lumibot.brokers.tastytrade import Tastytrade
    from lumibot.entities import Asset

    asset = Tastytrade._occ_to_asset("AAPL  260717C00230000")
    assert asset is not None
    assert asset.symbol == "AAPL"
    assert asset.asset_type == Asset.AssetType.OPTION
    assert asset.expiration == datetime.date(2026, 7, 17)
    assert float(asset.strike) == 230.0
    assert asset.right == Asset.OptionRight.CALL


def test_side_mapping_equity_vs_option():
    from lumibot.brokers.tastytrade import Tastytrade
    from tastytrade.order import OrderAction

    # Equity: plain buy -> BUY_TO_OPEN (Tastytrade rejects plain Buy/Sell on
    # the standard order endpoint).
    assert Tastytrade._lumi_side_to_tt_action(
        "buy", is_option=False
    ) == OrderAction.BUY_TO_OPEN
    assert Tastytrade._lumi_side_to_tt_action(
        "sell", is_option=False
    ) == OrderAction.SELL_TO_CLOSE
    assert Tastytrade._lumi_side_to_tt_action(
        "sell_short", is_option=False
    ) == OrderAction.SELL_TO_OPEN
    assert Tastytrade._lumi_side_to_tt_action(
        "buy_to_cover", is_option=False
    ) == OrderAction.BUY_TO_CLOSE
    # Options:
    assert Tastytrade._lumi_side_to_tt_action(
        "buy_to_open", is_option=True
    ) == OrderAction.BUY_TO_OPEN
    assert Tastytrade._lumi_side_to_tt_action(
        "sell_to_close", is_option=True
    ) == OrderAction.SELL_TO_CLOSE
    # Plain buy on an option defaults to BUY_TO_OPEN.
    assert Tastytrade._lumi_side_to_tt_action(
        "buy", is_option=True
    ) == OrderAction.BUY_TO_OPEN


# ---------------------------------------------------------------------------
# Order submission (mocked SDK)
# ---------------------------------------------------------------------------

def _stub_place_order(captured: list):
    """Return an async place_order that captures the NewOrder and returns a fake response."""

    async def _place(_session, new_order, dry_run=False):
        captured.append(new_order)
        resp = MagicMock(name="PlacedOrderResponse")
        resp.order = MagicMock(name="PlacedOrder")
        resp.order.id = 4242
        return resp

    return _place


def test_submit_order_equity_limit(monkeypatch):
    from lumibot.entities import Asset, Order
    from tastytrade.order import OrderAction, OrderType, OrderTimeInForce

    broker, fake_account, _ = _make_broker(monkeypatch)
    try:
        captured: list = []
        fake_account.place_order.side_effect = _stub_place_order(captured)

        order = Order(
            strategy="s",
            asset=Asset(symbol="AAPL", asset_type=Asset.AssetType.STOCK),
            quantity=10,
            side=Order.OrderSide.BUY,
            order_type=Order.OrderType.LIMIT,
            limit_price=150.25,
            time_in_force="day",
        )
        result = broker._submit_order(order)

        assert result is order
        assert order.identifier == "4242"
        assert order.status == Order.OrderStatus.SUBMITTED

        assert len(captured) == 1
        new_order = captured[0]
        assert new_order.order_type == OrderType.LIMIT
        assert new_order.time_in_force == OrderTimeInForce.DAY
        # BUY -> debit -> negative price (Tastytrade convention; serializer
        # strips abs() and emits price-effect on the wire).
        assert new_order.price == Decimal("-150.25")
        assert new_order.price_effect.value == "Debit"
        assert len(new_order.legs) == 1
        leg = new_order.legs[0]
        assert leg.symbol == "AAPL"
        # Equity buy maps to BUY_TO_OPEN (Tastytrade rejects plain Buy).
        assert leg.action == OrderAction.BUY_TO_OPEN
        assert Decimal(str(leg.quantity)) == Decimal("10")
    finally:
        broker._async_bridge.close()


def test_submit_order_option_limit_uses_occ(monkeypatch):
    from lumibot.entities import Asset, Order
    from tastytrade.order import InstrumentType, OrderAction

    broker, fake_account, _ = _make_broker(monkeypatch)
    try:
        captured: list = []
        fake_account.place_order.side_effect = _stub_place_order(captured)

        opt = Asset(
            symbol="AAPL",
            asset_type=Asset.AssetType.OPTION,
            expiration=datetime.date(2026, 7, 17),
            strike=230,
            right=Asset.OptionRight.CALL,
        )
        order = Order(
            strategy="s",
            asset=opt,
            quantity=1,
            side=Order.OrderSide.BUY_TO_OPEN,
            order_type=Order.OrderType.LIMIT,
            limit_price=4.20,
        )
        broker._submit_order(order)

        assert len(captured) == 1
        new_order = captured[0]
        leg = new_order.legs[0]
        assert leg.instrument_type == InstrumentType.EQUITY_OPTION
        assert leg.symbol == "AAPL  260717C00230000"
        assert leg.action == OrderAction.BUY_TO_OPEN
        # BUY_TO_OPEN -> debit -> negative price.
        assert new_order.price == Decimal("-4.20")
    finally:
        broker._async_bridge.close()


def test_submit_orders_multileg_credit_spread(monkeypatch):
    """A credit put spread: short 4500P / long 4400P. Price should be positive."""
    from lumibot.entities import Asset, Order
    from tastytrade.order import OrderAction, OrderType

    broker, fake_account, _ = _make_broker(monkeypatch)
    try:
        captured: list = []
        fake_account.place_order.side_effect = _stub_place_order(captured)

        short_leg = Order(
            strategy="s",
            asset=Asset(
                symbol="SPX",
                asset_type=Asset.AssetType.OPTION,
                expiration=datetime.date(2026, 5, 16),
                strike=4500,
                right=Asset.OptionRight.PUT,
            ),
            quantity=1,
            side=Order.OrderSide.SELL_TO_OPEN,
        )
        long_leg = Order(
            strategy="s",
            asset=Asset(
                symbol="SPX",
                asset_type=Asset.AssetType.OPTION,
                expiration=datetime.date(2026, 5, 16),
                strike=4400,
                right=Asset.OptionRight.PUT,
            ),
            quantity=1,
            side=Order.OrderSide.BUY_TO_OPEN,
        )
        result = broker._submit_orders(
            [short_leg, long_leg],
            is_multileg=True,
            order_type="credit",
            duration="day",
            price=2.50,
        )

        assert isinstance(result, list) and len(result) == 1
        parent = result[0]
        assert parent.order_class == Order.OrderClass.MULTILEG
        assert parent.identifier == "4242"

        assert len(captured) == 1
        new_order = captured[0]
        assert new_order.order_type == OrderType.LIMIT
        # Credit spread -> positive price (you receive the premium).
        assert new_order.price == Decimal("2.50")
        assert new_order.price_effect.value == "Credit"
        assert len(new_order.legs) == 2
        actions = [leg.action for leg in new_order.legs]
        assert OrderAction.SELL_TO_OPEN in actions
        assert OrderAction.BUY_TO_OPEN in actions
    finally:
        broker._async_bridge.close()


def test_submit_orders_multileg_rejects_mixed_underlyings(monkeypatch):
    from lumibot.entities import Asset, Order

    broker, fake_account, _ = _make_broker(monkeypatch)
    try:
        a = Order(strategy="s", asset=Asset(symbol="SPY",
                  asset_type=Asset.AssetType.OPTION,
                  expiration=datetime.date(2026, 5, 16), strike=400,
                  right=Asset.OptionRight.PUT),
                  quantity=1, side=Order.OrderSide.SELL_TO_OPEN)
        b = Order(strategy="s", asset=Asset(symbol="QQQ",
                  asset_type=Asset.AssetType.OPTION,
                  expiration=datetime.date(2026, 5, 16), strike=400,
                  right=Asset.OptionRight.PUT),
                  quantity=1, side=Order.OrderSide.BUY_TO_OPEN)
        with pytest.raises(ValueError, match="share an underlying"):
            broker._submit_orders([a, b], is_multileg=True,
                                  order_type="credit", price=1.0)
    finally:
        broker._async_bridge.close()


# ---------------------------------------------------------------------------
# Order parsing
# ---------------------------------------------------------------------------

def test_parse_broker_order_single_leg_equity():
    from lumibot.brokers.tastytrade import Tastytrade
    from lumibot.entities import Asset, Order

    leg = MagicMock()
    leg.instrument_type = MagicMock(value="Equity")
    leg.symbol = "AAPL"
    leg.action = MagicMock(value="Buy")
    leg.quantity = Decimal("10")

    placed = MagicMock()
    placed.id = 99
    placed.legs = [leg]
    placed.status = MagicMock(value="Live")
    placed.order_type = MagicMock(value="Limit")
    placed.time_in_force = MagicMock(value="Day")
    placed.price = Decimal("150.25")
    placed.stop_trigger = None
    placed.underlying_symbol = "AAPL"

    parsed = Tastytrade._parse_broker_order(
        Tastytrade.__new__(Tastytrade), placed, "s",
    )
    assert parsed is not None
    assert parsed.identifier == "99"
    assert parsed.asset.symbol == "AAPL"
    assert parsed.asset.asset_type == Asset.AssetType.STOCK
    assert parsed.side == Order.OrderSide.BUY
    assert parsed.status == Order.OrderStatus.OPEN
    assert parsed.order_type == "limit"


def test_parse_broker_order_multileg_attaches_children():
    from lumibot.brokers.tastytrade import Tastytrade
    from lumibot.entities import Order

    short_leg = MagicMock()
    short_leg.instrument_type = MagicMock(value="Equity Option")
    short_leg.symbol = "SPX   260516P04500000"
    short_leg.action = MagicMock(value="Sell to Open")
    short_leg.quantity = Decimal("1")

    long_leg = MagicMock()
    long_leg.instrument_type = MagicMock(value="Equity Option")
    long_leg.symbol = "SPX   260516P04400000"
    long_leg.action = MagicMock(value="Buy to Open")
    long_leg.quantity = Decimal("1")

    placed = MagicMock()
    placed.id = 1234
    placed.legs = [short_leg, long_leg]
    placed.status = MagicMock(value="Filled")
    placed.order_type = MagicMock(value="Limit")
    placed.time_in_force = MagicMock(value="Day")
    placed.price = Decimal("2.50")
    placed.stop_trigger = None
    placed.underlying_symbol = "SPX"

    parsed = Tastytrade._parse_broker_order(
        Tastytrade.__new__(Tastytrade), placed, "s",
    )
    assert parsed is not None
    assert parsed.order_class == Order.OrderClass.MULTILEG
    assert parsed.status == Order.OrderStatus.FILLED
    assert len(parsed.child_orders) == 2
    sides = {c.side for c in parsed.child_orders}
    assert Order.OrderSide.SELL_TO_OPEN in sides
    assert Order.OrderSide.BUY_TO_OPEN in sides


# ---------------------------------------------------------------------------
# Order modification + polling stream
# ---------------------------------------------------------------------------

def test_get_stream_object_returns_polling_stream(monkeypatch):
    from lumibot.trading_builtins import PollingStream

    broker, _, _ = _make_broker(monkeypatch)
    try:
        stream = broker._get_stream_object()
        assert isinstance(stream, PollingStream)
        assert stream.polling_interval == broker.polling_interval
    finally:
        broker._async_bridge.close()


def test_modify_order_calls_replace(monkeypatch):
    from lumibot.entities import Asset, Order

    broker, fake_account, _ = _make_broker(monkeypatch)
    try:
        captured: list = []

        async def _replace(_session, identifier, new_order):
            captured.append((identifier, new_order))
            resp = MagicMock()
            resp.order = MagicMock()
            resp.order.id = 9999
            return resp

        fake_account.replace_order.side_effect = _replace

        order = Order(
            strategy="s",
            asset=Asset(symbol="AAPL", asset_type=Asset.AssetType.STOCK),
            quantity=10,
            side=Order.OrderSide.BUY,
            order_type=Order.OrderType.LIMIT,
            limit_price=150.00,
        )
        order.identifier = "1234"

        broker._modify_order(order, limit_price=151.50)

        assert len(captured) == 1
        identifier, new_order = captured[0]
        assert identifier == "1234"
        # BUY -> debit -> negative.
        assert new_order.price == Decimal("-151.50")
        assert order.identifier == "9999"  # broker assigns new id on replace
        assert order.limit_price == 151.50
    finally:
        broker._async_bridge.close()


def test_avg_fill_from_legs_weighted_average():
    from lumibot.brokers.tastytrade import Tastytrade

    fill1 = MagicMock()
    fill1.quantity = Decimal("3")
    fill1.fill_price = Decimal("100")
    fill2 = MagicMock()
    fill2.quantity = Decimal("7")
    fill2.fill_price = Decimal("110")
    leg = MagicMock()
    leg.fills = [fill1, fill2]
    placed = MagicMock()
    placed.legs = [leg]

    avg = Tastytrade._avg_fill_from_legs(placed)
    # (3*100 + 7*110) / 10 = 107
    assert avg == Decimal("107")


def test_modify_order_rejects_multileg(monkeypatch):
    """_modify_order must reject multileg orders early — it only knows how
    to rebuild a single leg from the parent and would silently break a spread."""
    from lumibot.entities import Asset, Order

    broker, fake_account, _ = _make_broker(monkeypatch)
    try:
        replaced = []

        async def _replace(*args, **kwargs):
            replaced.append((args, kwargs))
            return MagicMock()

        fake_account.replace_order.side_effect = _replace

        parent = Order(
            strategy="s",
            asset=Asset(symbol="SPY", asset_type=Asset.AssetType.STOCK),
            quantity=1,
            side=Order.OrderSide.SELL_TO_OPEN,
            order_class=Order.OrderClass.MULTILEG,
            order_type=Order.OrderType.LIMIT,
            limit_price=2.50,
        )
        parent.identifier = "12345"

        result = broker._modify_order(parent, limit_price=2.75)
        assert result is None
        assert replaced == [], (
            "Multileg modify must NOT call replace_order with a single-leg payload"
        )
    finally:
        broker._async_bridge.close()


def test_leg_to_lumi_side_equity_explicit_actions():
    """Equity legs read back from Tastytrade arrive as 'Buy to Open' etc.
    because that's what the wire requires. Parser must map them correctly."""
    from lumibot.brokers.tastytrade import Tastytrade
    from lumibot.entities import Order

    def _leg(action_value):
        leg = MagicMock()
        leg.action = MagicMock(value=action_value)
        return leg

    assert Tastytrade._leg_to_lumi_side(
        _leg("Buy to Open"), is_option=False
    ) == Order.OrderSide.BUY_TO_OPEN
    assert Tastytrade._leg_to_lumi_side(
        _leg("Sell to Close"), is_option=False
    ) == Order.OrderSide.SELL_TO_CLOSE
    assert Tastytrade._leg_to_lumi_side(
        _leg("Sell to Open"), is_option=False
    ) == Order.OrderSide.SELL_TO_OPEN
    assert Tastytrade._leg_to_lumi_side(
        _leg("Buy to Close"), is_option=False
    ) == Order.OrderSide.BUY_TO_CLOSE
    # Plain Buy/Sell still round-trip.
    assert Tastytrade._leg_to_lumi_side(
        _leg("Buy"), is_option=False
    ) == Order.OrderSide.BUY
    assert Tastytrade._leg_to_lumi_side(
        _leg("Sell"), is_option=False
    ) == Order.OrderSide.SELL


def test_avg_fill_from_legs_returns_none_when_unfilled():
    from lumibot.brokers.tastytrade import Tastytrade

    leg = MagicMock()
    leg.fills = []
    placed = MagicMock()
    placed.legs = [leg]
    assert Tastytrade._avg_fill_from_legs(placed) is None


# ---------------------------------------------------------------------------
# TastytradeData (market data)
# ---------------------------------------------------------------------------

def test_get_last_price_prefers_mid(monkeypatch):
    from lumibot.data_sources.tastytrade_data import TastytradeData
    from lumibot.entities import Asset
    from lumibot.data_sources import tastytrade_data as td_mod

    fake_md = MagicMock()
    fake_md.mid = Decimal("100.10")
    fake_md.last = Decimal("100.05")
    fake_md.mark = Decimal("100.07")
    fake_md.bid = Decimal("100.05")
    fake_md.ask = Decimal("100.15")

    async def _get(_session, _symbol, _instrument_type):
        return fake_md

    monkeypatch.setattr(td_mod, "_tt_get_market_data", _get)

    ds = TastytradeData(session=MagicMock(), runner=None)

    asset = Asset(symbol="AAPL", asset_type=Asset.AssetType.STOCK)
    price = ds.get_last_price(asset)
    assert price == 100.10  # picked mid


def test_get_last_price_falls_back_to_bid_ask_avg(monkeypatch):
    from lumibot.data_sources.tastytrade_data import TastytradeData
    from lumibot.entities import Asset
    from lumibot.data_sources import tastytrade_data as td_mod

    fake_md = MagicMock()
    fake_md.mid = None
    fake_md.last = None
    fake_md.mark = None
    fake_md.bid = Decimal("99.90")
    fake_md.ask = Decimal("100.10")

    async def _get(_session, _symbol, _instrument_type):
        return fake_md

    monkeypatch.setattr(td_mod, "_tt_get_market_data", _get)

    ds = TastytradeData(session=MagicMock(), runner=None)
    asset = Asset(symbol="AAPL", asset_type=Asset.AssetType.STOCK)
    assert ds.get_last_price(asset) == 100.0


def test_get_quote_populates_bid_ask_mid(monkeypatch):
    from lumibot.data_sources.tastytrade_data import TastytradeData
    from lumibot.entities import Asset
    from lumibot.data_sources import tastytrade_data as td_mod

    fake_md = MagicMock()
    fake_md.bid = Decimal("99.90")
    fake_md.ask = Decimal("100.10")
    fake_md.mid = Decimal("100.00")
    fake_md.last = Decimal("100.05")
    fake_md.mark = None
    fake_md.bid_size = Decimal("100")
    fake_md.ask_size = Decimal("200")
    fake_md.volume = Decimal("500000")
    fake_md.updated_at = datetime.datetime(2026, 5, 4, 14, 30)

    async def _get(_session, _symbol, _instrument_type):
        return fake_md

    monkeypatch.setattr(td_mod, "_tt_get_market_data", _get)

    ds = TastytradeData(session=MagicMock(), runner=None)
    quote = ds.get_quote(Asset(symbol="AAPL", asset_type=Asset.AssetType.STOCK))
    assert quote.bid == 99.90
    assert quote.ask == 100.10
    assert quote.mid_price == 100.00
    assert quote.price == 100.05
    assert quote.bid_size == 100
    assert quote.ask_size == 200


def test_get_chains_pivots_to_lumibot_shape(monkeypatch):
    from lumibot.data_sources.tastytrade_data import TastytradeData
    from lumibot.entities import Asset
    from lumibot.data_sources import tastytrade_data as td_mod

    def _opt(strike, opt_type):
        m = MagicMock()
        m.strike_price = Decimal(str(strike))
        m.option_type = MagicMock(value=opt_type)
        return m

    chain = {
        datetime.date(2026, 5, 16): [
            _opt(450, "C"), _opt(440, "C"),
            _opt(450, "P"), _opt(440, "P"),
        ],
        datetime.date(2026, 6, 20): [
            _opt(460, "Call"), _opt(450, "Put"),
        ],
    }

    async def _get(_session, _symbol):
        return chain

    monkeypatch.setattr(td_mod, "_tt_get_option_chain", _get)

    ds = TastytradeData(session=MagicMock(), runner=None)
    out = ds.get_chains(Asset(symbol="SPY", asset_type=Asset.AssetType.STOCK))

    assert out["Multiplier"] == 100
    assert out["Chains"]["CALL"]["2026-05-16"] == [440.0, 450.0]
    assert out["Chains"]["PUT"]["2026-05-16"] == [440.0, 450.0]
    assert out["Chains"]["CALL"]["2026-06-20"] == [460.0]
    assert out["Chains"]["PUT"]["2026-06-20"] == [450.0]


# ---------------------------------------------------------------------------
# Live sandbox smoke (only runs when sandbox credentials are present)
# ---------------------------------------------------------------------------

@pytest.mark.apitest
@pytest.mark.tastytrade
@pytest.mark.skipif(
    not all(os.environ.get(k) for k in (
        "TASTYTRADE_CLIENT_SECRET",
        "TASTYTRADE_REFRESH_TOKEN",
        "TASTYTRADE_ACCOUNT_NUMBER",
    )),
    reason="Tastytrade credentials not configured.",
)
def test_live_sandbox_balances_and_positions():
    """Hit the sandbox API for balances + positions; expects no exceptions.

    Forces ``is_test=True`` so the test is guaranteed to hit Tastytrade's
    certification (sandbox) environment regardless of how the user set
    ``TASTYTRADE_SANDBOX`` — preventing accidental prod-account runs in CI.
    Users running this locally need sandbox credentials, not prod.
    """
    from lumibot.brokers.tastytrade import Tastytrade

    broker = Tastytrade(connect_stream=False, is_test=True)
    try:
        cash, positions_value, nlv = broker._get_balances_at_broker(
            quote_asset=None, strategy=None,
        )
        assert isinstance(cash, float)
        assert isinstance(nlv, float)
        positions = broker._pull_positions(strategy=None)
        assert isinstance(positions, list)
    finally:
        broker._async_bridge.close()
