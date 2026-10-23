"""Managed CCXT Pro stream lifecycle, using an injected transport."""

import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from lumibot.trading_builtins.ccxt_stream import CcxtStream
from lumibot.entities import Asset, Order
from tests.test_ccxt_unified_spot import make_broker
from tests.test_ccxt_derivatives import derivatives


class Client:
    def __init__(self):
        self.closed = False
        self.calls = []

    def set_sandbox_mode(self, enabled):
        self.sandbox = enabled

    def set_markets(self, markets, currencies):
        self.markets = markets

    async def watch_ticker(self, **kwargs):
        self.calls.append(kwargs)
        if len(self.calls) > 1:
            await asyncio.Event().wait()
        return {"last": 123, "symbol": kwargs["symbol"]}

    async def close(self):
        self.closed = True


def test_stream_starts_lazily_reuses_subscription_and_closes_client():
    client = Client()
    factory = Mock(return_value=client)
    stream = CcxtStream(SimpleNamespace(id="phemex", markets={}, currencies={}),
                        {"sandbox": True}, client_factory=factory)
    assert not factory.called
    try:
        first = stream.snapshot("watch_ticker", symbol="BTC/USDT", timeout=2)
        first["last"] = 0
        assert stream.snapshot("watch_ticker", symbol="BTC/USDT", timeout=2)["last"] == 123
        assert client.sandbox
        assert len(stream._tasks) == 1
    finally:
        stream.stop()
    assert client.closed
    assert not stream._thread.is_alive()


def test_stream_recovers_only_failed_subscription():
    client = Client()
    normal_watch = client.watch_ticker
    failures = [True]

    async def transient(**kwargs):
        if failures:
            failures.pop()
            raise ConnectionError("synthetic disconnect")
        return await normal_watch(**kwargs)
    client.watch_ticker = transient
    stream = CcxtStream(SimpleNamespace(id="phemex", markets={}, currencies={}),
                        {"sandbox": True}, client_factory=lambda: client)
    try:
        assert stream.snapshot("watch_ticker", symbol="BTC/USDT", timeout=2)["last"] == 123
    finally:
        stream.stop()


def test_phemex_v2_public_channels_follow_product_metadata():
    market = {"symbol": "BTC/USDC:USDC", "info": {"type": "PerpetualV2"}}
    api = SimpleNamespace(id="phemex", markets={market["symbol"]: market}, currencies={})
    stream = CcxtStream(api, {})
    assert stream.watch_params("watch_ticker", market["symbol"])["method"] == "perp_market24h_pack_p.subscribe"
    assert stream.watch_params("watch_order_book", market["symbol"])["method"] == "orderbook_p.subscribe"
    assert stream.watch_params("watch_trades", market["symbol"])["method"] == "trade_p.subscribe"
    assert stream.watch_params("watch_ohlcv", market["symbol"])["method"] == "kline_p.subscribe"


def test_phemex_empty_snapshot_and_usdc_order_use_v2_scope():
    observed = Mock()
    api = SimpleNamespace(id="phemex", markets={}, currencies={})
    stream = CcxtStream(api, {}, on_orders=observed)
    client = SimpleNamespace(parse_ws_swap_order=lambda value: value)
    stream._phemex_message(client, {"orders_p": [], "type": "snapshot"})
    observed.assert_called_once_with([], {"type": "swap", "settle": "USDT"}, True)
    stream._phemex_message(client, {"orders_p": [{"symbol": "BTC/USDC:USDC"}], "type": "incremental"})
    assert observed.call_args.args[0][0]["symbol"] == "BTC/USDC:USDC"


def test_live_quote_uses_streamed_ticker_and_book(make_broker):
    broker = make_broker()
    source = broker.data_source
    source._stream = Mock()
    source._stream.snapshot.side_effect = [
        {"last": 100, "timestamp": 1700000000000},
        {"bids": [[99, 2]], "asks": [[101, 3]], "timestamp": 1700000000000},
    ]

    quote = source.get_quote(Asset("BTC", "crypto"), Asset("USDT", "crypto"))

    assert (quote.price, quote.bid, quote.ask, quote.bid_size, quote.ask_size) == (100, 99, 101, 2, 3)
    assert quote.quote_time.timestamp() == 1700000000


def test_stream_timeout_keeps_rest_price_available(make_broker):
    broker = make_broker()
    source = broker.data_source
    source.get_last_price = type(source).get_last_price.__get__(source)
    source._stream = Mock()
    source._stream.snapshot.side_effect = TimeoutError("synthetic timeout")
    source.api.fetch_ticker = Mock(return_value={"last": 100})

    assert source.get_last_price(Asset("BTC", "crypto"), Asset("USDT", "crypto")) == 100
    source.api.fetch_ticker.assert_called_once_with("BTC/USDT")


def test_order_updates_apply_only_new_fill_quantity_and_ignore_replays(make_broker):
    broker = make_broker()
    order = Order("test-strategy", Asset("BTC", "crypto"), 3, "buy", quote=Asset("USDT", "crypto"))
    order.identifier = "streamed-order"
    broker._process_new_order(order)

    def observation(filled, cost, status="open"):
        return {"id": order.identifier, "symbol": "BTC/USDT", "type": "market", "amount": 3,
                "side": "buy", "filled": filled, "cost": cost, "average": cost / filled,
                "status": status}
    broker._on_ccxt_orders([observation(1, 100)], {}, False)
    broker._on_ccxt_orders([observation(1, 100)], {}, False)
    broker._on_ccxt_orders([observation(2, 202)], {}, False)
    broker._on_ccxt_orders([observation(3, 309, "closed")], {}, False)
    broker._on_ccxt_orders([observation(1, 100)], {}, False)

    assert [float(tx.quantity) for tx in order.transactions] == [1, 1, 1]
    assert [float(tx.price) for tx in order.transactions] == [100, 102, 107]
    assert order.is_filled()
    assert order.avg_fill_price == 103


def test_private_snapshot_clears_only_its_own_family(make_broker):
    broker = make_broker()
    broker._on_ccxt_orders([{"id": "spot", "status": "open"}], {"type": "spot"}, True)
    broker._on_ccxt_orders([{"id": "swap", "status": "open"}], {"type": "swap"}, True)
    broker._on_ccxt_orders([], {"type": "spot"}, True)
    assert set(broker._stream_orders) == {"swap"}


@pytest.mark.parametrize("inverse,size,costs,prices", [
    (False, 0.001, ["20", "45"], [20000, 25000]),
    (True, 1, ["0.00005", "0.00009"], [20000, 25000]),
])
def test_contract_fill_cost_is_converted_using_contract_units(derivatives, inverse, size, costs, prices):
    symbol = "BTC/USD:BTC" if inverse else "BTC/USDT:USDT"
    derivatives.api.markets[symbol]["contractSize"] = size
    order = Order("test-strategy", derivatives.get_asset(symbol), 2, "buy")
    order.pair = symbol
    order.identifier = "contract-fill"
    derivatives._process_new_order(order)

    for filled, cost in enumerate(costs, 1):
        derivatives._on_ccxt_orders([{"id": order.identifier, "filled": filled, "cost": cost,
                                      "status": "closed" if filled == 2 else "open"}], {}, False)

    assert [float(tx.price) for tx in order.transactions] == prices


def test_immediate_fill_acknowledgement_registers_and_fills_the_order(make_broker):
    broker = make_broker()
    order = Order("test-strategy", Asset("BTC", "crypto"), 3, "buy", quote=Asset("USDT", "crypto"))
    broker.api.create_order.return_value = {"id": "instant-fill", "status": "closed", "filled": 3,
                                          "cost": 300, "average": 100}

    broker.submit_order(order)

    assert order.is_filled()
    assert len(order.transactions) == 1
    assert broker.get_tracked_order("instant-fill") is order


def test_unknown_submission_is_reconciled_by_client_id_without_retry(make_broker):
    import ccxt

    broker = make_broker()
    order = Order("test-strategy", Asset("BTC", "crypto"), 3, "buy", quote=Asset("USDT", "crypto"))
    client_id = order.identifier
    broker.api.create_order.side_effect = ccxt.RequestTimeout("synthetic timeout after send")

    broker.submit_order(order)

    assert order.status == "unknown"
    broker._on_ccxt_orders([{"id": "actual-order", "clientOrderId": client_id, "status": "closed",
                             "filled": 3, "cost": 300, "average": 100}], {}, False)
    assert order.identifier == "actual-order"
    assert order.is_filled()
    broker.api.create_order.assert_called_once()


@pytest.mark.parametrize("event", ["fill", "partial_fill"])
def test_contract_execution_callbacks_do_not_debit_spot_notional(event):
    from lumibot.strategies.strategy_executor import StrategyExecutor

    executor = StrategyExecutor.__new__(StrategyExecutor)
    executor.strategy = SimpleNamespace(logger=Mock(), _update_cash=Mock())
    executor._on_filled_order = Mock()
    executor._on_partially_filled_order = Mock()
    order = Order("test-strategy", Asset("BTC/USDC:USDC", "crypto_future"), 2, "buy")
    payload = {"position": None, "order": order, "price": 20000, "quantity": 1, "multiplier": 1}

    executor.process_event(event, payload, skip_first_iteration_events=False)

    executor.strategy._update_cash.assert_not_called()
    callback = executor._on_filled_order if event == "fill" else executor._on_partially_filled_order
    callback.assert_called_once_with(**payload)

