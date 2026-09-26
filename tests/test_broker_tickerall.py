"""Unit tests for the TickerAll hosted MT5 API broker + data source.

The hosted-API client is mocked, so these run in CI with no network or
credentials. The whole module is skipped if the optional ``tickerall`` package
is not installed.
"""
import os
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("tickerall")

from lumibot.brokers import TickerAll  # noqa: E402
from lumibot.data_sources import TickerAllData  # noqa: E402
from lumibot.entities import Asset, Order  # noqa: E402


def _account_detail(balance=1000.0, equity=1050.0, positions=None, is_demo=True, has_account=True):
    account = None
    if has_account:
        account = SimpleNamespace(
            name="Demo", account_type="demo", leverage=100, balance=balance,
            broker_name="TestBroker", currency="USD", equity=equity, margin=10.0,
            free_margin=equity - 10.0, margin_level=200.0,
        )
    return SimpleNamespace(
        id="acc1", broker="mt5", server="Test-MT5", account_number="12345678",
        is_demo=is_demo, status="CONNECTED", account=account, positions=positions or [], hint=None,
    )


def _position(ticket=1, symbol="EURUSDm", side="BUY", volume=0.1, entry=1.10, current=1.11, profit=1.0):
    return SimpleNamespace(
        ticket=ticket, symbol=symbol, side=side, volume=volume, stop_loss=0.0, take_profit=0.0,
        magic=0, comment="", swap=0.0, commission=0.0, open_time=None,
        entry_price=entry, current_price=current, profit=profit, last_update=None,
    )


class TestTickerAllBroker(unittest.TestCase):
    def setUp(self):
        self.patcher = patch("tickerall.Tickerall")
        self.MockTA = self.patcher.start()
        self.client = MagicMock()
        self.MockTA.return_value = self.client
        self.client.accounts.symbols.return_value = ["EURUSDm", "BTCUSDm", "XAUUSDm"]
        self.cfg = {"API_KEY": "test_key", "ACCOUNT_ID": "acc1"}

    def tearDown(self):
        self.patcher.stop()

    def _broker(self, connect_stream=False):
        ds = TickerAllData(self.cfg)
        broker = TickerAll(self.cfg, data_source=ds, connect_stream=connect_stream)
        broker.stream = MagicMock()  # capture dispatched events without a real thread
        return broker

    # ── construction ─────────────────────────────────────────────────────────
    def test_initialization(self):
        broker = self._broker()
        self.assertEqual(broker.NAME, "TickerAll")
        self.assertEqual(broker.api, self.client)
        self.assertEqual(broker.account_id, "acc1")
        # MT5 default market is always-tradeable unless a calendar is configured.
        self.assertEqual(broker.market, "24/7")

    def test_requires_tickerall_data_source(self):
        with self.assertRaises(ValueError):
            TickerAll(self.cfg, data_source=object(), connect_stream=False)

    # ── symbol resolution (case-sensitive MT5 symbols) ────────────────────────
    def test_symbol_resolution_case_insensitive(self):
        broker = self._broker()
        # Lumibot upper-cases the symbol; it must resolve back to the real casing.
        self.assertEqual(broker._resolve_symbol(Asset("EURUSDM", asset_type="forex")), "EURUSDm")
        self.assertEqual(broker._resolve_symbol("btcusdm"), "BTCUSDm")
        # Unknown symbol falls back to the requested value.
        self.assertEqual(broker._resolve_symbol("UNKNOWNm"), "UNKNOWNm")

    # ── balances ──────────────────────────────────────────────────────────────
    def test_balances(self):
        broker = self._broker()
        self.client.accounts.get.return_value = _account_detail(balance=1000.0, equity=1050.0)
        cash, positions_value, portfolio = broker._get_balances_at_broker(Asset("USD", asset_type="forex"), "s")
        self.assertEqual(cash, 1000.0)
        self.assertAlmostEqual(positions_value, 50.0)
        self.assertEqual(portfolio, 1050.0)

    def test_balance_of_zero_is_valid(self):
        # A balance of 0.0 is a real, valid account state (never treat 0 as missing).
        broker = self._broker()
        self.client.accounts.get.return_value = _account_detail(balance=0.0, equity=0.0)
        cash, positions_value, portfolio = broker._get_balances_at_broker(Asset("USD", asset_type="forex"), "s")
        self.assertEqual((cash, positions_value, portfolio), (0.0, 0.0, 0.0))

    def test_balances_no_account_snapshot(self):
        broker = self._broker()
        self.client.accounts.get.return_value = _account_detail(has_account=False)
        self.assertEqual(broker._get_balances_at_broker(Asset("USD", asset_type="forex"), "s"), (0.0, 0.0, 0.0))

    # ── positions ──────────────────────────────────────────────────────────────
    def test_short_position_is_negative_quantity(self):
        broker = self._broker()
        self.client.accounts.get.return_value = _account_detail(
            positions=[_position(side="SELL", volume=0.2, symbol="EURUSDm")]
        )
        positions = broker._pull_positions("s")
        self.assertEqual(len(positions), 1)
        self.assertEqual(float(positions[0].quantity), -0.2)  # SELL -> negative
        self.assertEqual(positions[0].broker_ticket, 1)

    def test_long_position_is_positive_quantity(self):
        broker = self._broker()
        self.client.accounts.get.return_value = _account_detail(
            positions=[_position(side="BUY", volume=0.15)]
        )
        positions = broker._pull_positions("s")
        self.assertEqual(float(positions[0].quantity), 0.15)

    def test_hedging_positions_aggregate_to_net(self):
        # A hedging account can hold several positions per symbol; they must
        # aggregate into one net Lumibot position (BUY +, SELL -), tracking
        # every underlying ticket for closing.
        broker = self._broker()
        self.client.accounts.get.return_value = _account_detail(positions=[
            _position(ticket=1, symbol="EURUSDm", side="BUY", volume=0.2),
            _position(ticket=2, symbol="EURUSDm", side="SELL", volume=0.05),
        ])
        positions = broker._pull_positions("s")
        self.assertEqual(len(positions), 1)  # one net position, not two colliding
        self.assertAlmostEqual(float(positions[0].quantity), 0.15)  # 0.20 - 0.05
        self.assertEqual(sorted(positions[0].broker_tickets), [1, 2])

    # ── order type mapping ──────────────────────────────────────────────────────
    def test_submit_market_order_dispatches_fill(self):
        broker = self._broker()
        self.client.orders.place.return_value = SimpleNamespace(
            ticket=999, symbol="EURUSDm", side="BUY", type="market", volume=0.1,
            status="DONE", timestamp="", price=1.105, stop_loss=None, take_profit=None, comment="",
        )
        order = Order("s", Asset("EURUSDm", asset_type="forex"), 0.1, "buy", order_type=Order.OrderType.MARKET)
        broker._submit_order(order)

        self.client.orders.place.assert_called_once()
        _, kwargs = self.client.orders.place.call_args
        self.assertEqual(kwargs["type"], "market")
        self.assertEqual(kwargs["side"], "BUY")
        self.assertEqual(kwargs["volume"], 0.1)
        self.assertEqual(order.identifier, "999")
        # A market fill dispatches NEW then FILLED on the stream.
        events = [c.args[0] for c in broker.stream.dispatch.call_args_list]
        self.assertIn(broker.NEW_ORDER, events)
        self.assertIn(broker.FILLED_ORDER, events)

    def test_submit_limit_order_is_pending(self):
        broker = self._broker()
        self.client.orders.place.return_value = SimpleNamespace(
            ticket=1001, symbol="EURUSDm", side="BUY", type="limit", volume=0.1,
            status="PLACED", timestamp="", price=1.08, stop_loss=None, take_profit=None, comment="",
        )
        order = Order("s", Asset("EURUSDm", asset_type="forex"), 0.1, "buy",
                      order_type=Order.OrderType.LIMIT, limit_price=1.08)
        broker._submit_order(order)
        _, kwargs = self.client.orders.place.call_args
        self.assertEqual(kwargs["type"], "limit")
        self.assertEqual(kwargs["price"], 1.08)
        self.assertEqual(order.status, "new")  # resting pending order

    def test_submit_rejects_unsupported_order_type(self):
        broker = self._broker()
        order = Order("s", Asset("EURUSDm", asset_type="forex"), 0.1, "buy",
                      order_type=Order.OrderType.STOP_LIMIT, limit_price=1.1, stop_price=1.09)
        result = broker._submit_order(order)
        self.assertEqual(result.status, "error")
        self.client.orders.place.assert_not_called()

    def test_submit_rejects_zero_quantity(self):
        broker = self._broker()
        order = Order("s", Asset("EURUSDm", asset_type="forex"), 0.0, "buy", order_type=Order.OrderType.MARKET)
        result = broker._submit_order(order)
        self.assertEqual(result.status, "error")
        self.client.orders.place.assert_not_called()

    def test_market_order_forwards_sl_tp(self):
        broker = self._broker()
        self.client.orders.place.return_value = SimpleNamespace(
            ticket=1, symbol="EURUSDm", side="BUY", type="market", volume=0.1,
            status="DONE", timestamp="", price=1.1, stop_loss=1.08, take_profit=1.12, comment="",
        )
        order = Order("s", Asset("EURUSDm", asset_type="forex"), 0.1, "buy",
                      order_type=Order.OrderType.MARKET,
                      secondary_stop_price=1.08, secondary_limit_price=1.12)
        broker._submit_order(order)
        _, kwargs = self.client.orders.place.call_args
        self.assertEqual(kwargs["stop_loss"], 1.08)
        self.assertEqual(kwargs["take_profit"], 1.12)

    # ── cancel ──────────────────────────────────────────────────────────────────
    def test_cancel_order(self):
        broker = self._broker()
        order = Order("s", Asset("EURUSDm", asset_type="forex"), 0.1, "buy", order_type=Order.OrderType.LIMIT,
                      limit_price=1.05)
        order.set_identifier("777")
        order.status = "new"
        broker.cancel_order(order)
        self.client.orders.cancel_pending.assert_called_once_with("acc1", 777)

    def test_cancel_order_proceeds_on_cancelling_status(self):
        # Strategy.cancel_order sets status to "cancelling" right before calling
        # the broker; is_canceled() treats "cancelling" as canceled, so the guard
        # must NOT skip on it (otherwise the broker cancel never fires).
        broker = self._broker()
        order = Order("s", Asset("EURUSDm", asset_type="forex"), 0.1, "buy", order_type=Order.OrderType.LIMIT,
                      limit_price=1.05)
        order.set_identifier("778")
        order.status = "cancelling"
        broker.cancel_order(order)
        self.client.orders.cancel_pending.assert_called_once_with("acc1", 778)

    def test_cancel_order_skips_terminal(self):
        broker = self._broker()
        order = Order("s", Asset("EURUSDm", asset_type="forex"), 0.1, "buy", order_type=Order.OrderType.LIMIT,
                      limit_price=1.05)
        order.set_identifier("779")
        order.status = "canceled"
        broker.cancel_order(order)
        self.client.orders.cancel_pending.assert_not_called()

    def _tracked_pending(self, broker, identifier="555"):
        """Track a pending limit order as active (as after a real submit)."""
        broker.stream = None  # do_polling processes events inline
        self.client.history.orders.return_value = []  # default: no history match
        o = Order("s", Asset("EURUSDm", asset_type="forex"), 0.1, "buy",
                  order_type=Order.OrderType.LIMIT, limit_price=1.0)
        o.set_identifier(identifier)
        o.update_raw({})
        broker._process_trade_event(o, broker.NEW_ORDER)
        return o

    def test_do_polling_history_reports_fill_when_position_netted_to_zero(self):
        # A pending order that fills and nets a netting position to exactly zero
        # leaves NO position in the snapshot; history (deal_count > 0) must still
        # report it as FILLED, not CANCELED.
        broker = self._broker()
        o = self._tracked_pending(broker, identifier="600")
        self.client.orders.list_pending.return_value = []
        self.client.accounts.get.return_value = _account_detail(positions=[])  # net zero -> no position
        self.client.history.orders.return_value = [
            SimpleNamespace(order_ticket="600", symbol="EURUSDm", side="BUY", volume=0.1,
                            price=1.0, time="", position_id="0", state="filled", deal_count=1),
        ]
        broker.do_polling()
        self.assertTrue(o.is_filled())

    def test_do_polling_history_deal_count_zero_is_cancel(self):
        broker = self._broker()
        o = self._tracked_pending(broker, identifier="601")
        self.client.orders.list_pending.return_value = []
        self.client.accounts.get.return_value = _account_detail(positions=[])
        self.client.history.orders.return_value = [
            SimpleNamespace(order_ticket="601", symbol="EURUSDm", side="BUY", volume=0.1,
                            price=1.0, time="", position_id="0", state="canceled", deal_count=0),
        ]
        broker.do_polling()
        self.assertTrue(o.is_canceled())

    def test_do_polling_fills_pending_when_position_appears(self):
        # A tracked pending order that left the broker list AND has a matching
        # position -> reconciled as FILLED.
        broker = self._broker()
        o = self._tracked_pending(broker)
        self.client.orders.list_pending.return_value = []
        self.client.accounts.get.return_value = _account_detail(
            positions=[_position(symbol="EURUSDm", side="BUY", volume=0.1)]
        )
        broker.do_polling()
        self.assertTrue(o.is_filled())

    def test_do_polling_cancels_pending_when_no_position(self):
        # A tracked pending order that vanished with no matching position -> CANCELED.
        broker = self._broker()
        o = self._tracked_pending(broker, identifier="556")
        self.client.orders.list_pending.return_value = []
        self.client.accounts.get.return_value = _account_detail(positions=[])
        broker.do_polling()
        self.assertTrue(o.is_canceled())


class TestTickerAllData(unittest.TestCase):
    def setUp(self):
        self.patcher = patch("tickerall.Tickerall")
        self.MockTA = self.patcher.start()
        self.client = MagicMock()
        self.MockTA.return_value = self.client
        self.client.accounts.symbols.return_value = ["EURUSDm", "BTCUSDm"]
        self.ds = TickerAllData({"API_KEY": "k", "ACCOUNT_ID": "acc1"})

    def tearDown(self):
        self.patcher.stop()

    def test_timeframe_mapping(self):
        self.assertEqual(self.ds._to_timeframe("day"), "D1")
        self.assertEqual(self.ds._to_timeframe("minute"), "M1")
        self.assertEqual(self.ds._to_timeframe("hour"), "H1")
        # Raw MT5 timeframes pass through.
        self.assertEqual(self.ds._to_timeframe("M5"), "M5")
        self.assertEqual(self.ds._to_timeframe("H4"), "H4")

    def test_get_historical_prices_builds_bars(self):
        self.client.candles.get.return_value = [
            SimpleNamespace(timestamp=1_700_000_000 + i * 86400, open=1.1, high=1.2, low=1.0,
                            close=1.15, bid=1.149, tick_volume=100, spread=2)
            for i in range(5)
        ]
        bars = self.ds.get_historical_prices(Asset("EURUSDm", asset_type="forex"), 5, "day")
        self.assertIsNotNone(bars)
        self.assertEqual(len(bars.df), 5)
        for col in ("open", "high", "low", "close", "volume"):
            self.assertIn(col, bars.df.columns)
        self.assertIsNotNone(bars.df.index.tz)  # tz-aware index

    def test_get_historical_prices_empty_returns_none(self):
        # RULE #1: no fabricated data - absence is surfaced honestly.
        self.client.candles.get.return_value = []
        self.assertIsNone(self.ds.get_historical_prices(Asset("EURUSDm", asset_type="forex"), 5, "day"))

    def test_get_last_price_candle_fallback(self):
        # No live tick available -> fall back to the latest candle close.
        self.client.stream.connect.side_effect = Exception("no stream")
        self.client.candles.get.return_value = [
            SimpleNamespace(timestamp=1_700_000_000, open=1.1, high=1.2, low=1.0,
                            close=1.2345, bid=1.234, tick_volume=1, spread=2)
        ]
        self.assertEqual(self.ds.get_last_price(Asset("EURUSDm", asset_type="forex")), 1.2345)

    def test_get_chains_empty(self):
        self.assertEqual(self.ds.get_chains(Asset("EURUSDm", asset_type="forex")), {})

    def test_env_var_credentials(self):
        # The documented TICKERALL_* environment variables must authenticate and
        # select an account with no config dict passed.
        with patch.dict(os.environ, {"TICKERALL_API_KEY": "envkey", "TICKERALL_ACCOUNT_ID": "envacct"}, clear=False):
            ds = TickerAllData({})
        self.MockTA.assert_called_with(api_key="envkey")
        self.assertEqual(ds._configured_account_id, "envacct")

    def test_symbol_fetch_failure_not_cached(self):
        # A transient symbol-fetch failure must not be cached permanently.
        self.client.accounts.symbols.side_effect = [Exception("hiccup"), ["EURUSDm", "BTCUSDm"]]
        self.assertEqual(self.ds._ensure_symbols(), [])  # first call fails, returns []
        self.assertEqual(self.ds._ensure_symbols(), ["EURUSDm", "BTCUSDm"])  # retries, succeeds


if __name__ == "__main__":
    unittest.main()
