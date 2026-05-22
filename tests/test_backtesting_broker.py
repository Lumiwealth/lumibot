import datetime
import os
import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime as dt # Renamed datetime to dt to avoid conflict
import pytz


# Assuming the BacktestingBroker class is importable like this
# Adjust the import path if necessary based on your project structure
try:
    from lumibot.backtesting.backtesting_broker import BacktestingBroker
    from lumibot.data_sources import PandasData
    from lumibot.entities import Asset, Order, Position, Quote # Import Asset if needed by mocked methods
    from lumibot.trading_builtins import SafeList, SafeOrderDict
except ImportError:
    # Add path modification if running tests directly and lumibot is not installed
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
    from lumibot.backtesting.backtesting_broker import BacktestingBroker
    from lumibot.data_sources import PandasData
    from lumibot.entities import Asset, Order, Position, Quote
    from lumibot.trading_builtins import SafeList, SafeOrderDict


class _OptionSettlementStrategyStub:
    def __init__(self, broker, cash=100_000.0, name="option_settlement_test"):
        self.broker = broker
        self.name = name
        self._name = name
        self.cash = float(cash)
        self.parameters = {}
        self.minutes_before_closing = 0
        self.buy_trading_fees = []
        self.sell_trading_fees = []
        self.vars = type("Vars", (), {})()

    def get_cash(self):
        return self.cash

    def _set_cash_position(self, cash):
        self.cash = float(cash)

    def create_order(self, asset, quantity, side):
        return Order(
            asset=asset,
            quantity=quantity,
            side=side,
            strategy=self.name,
        )


class TestBacktestingBroker:
    def test_limit_fills(self):
        start = dt(2023, 8, 1) # Use dt alias
        end = dt(2023, 8, 2) # Use dt alias
        data_source = PandasData(datetime_start=start, datetime_end=end, pandas_data={})
        broker = BacktestingBroker(data_source=data_source)

        # Limit triggered by candle body
        limit_price = 105
        assert broker.limit_order(limit_price, 'sell', open_=100, high=110, low=90) == limit_price

        # Limit triggered by candle wick
        limit_price = 109
        assert broker.limit_order(limit_price, 'sell', open_=100, high=110, low=90) == limit_price

        # Limit Sell Triggered by a gap up candle
        limit_price = 85
        assert broker.limit_order(limit_price, 'sell', open_=100, high=110, low=90) == 100

        # Limit Buy Triggered by a gap down candle
        limit_price = 115
        assert broker.limit_order(limit_price, 'buy', open_=100, high=110, low=90) == 100

        # Limit not triggered
        limit_price = 120
        assert not broker.limit_order(limit_price, 'sell', open_=100, high=110, low=90)

    def test_stop_fills(self):
        start = dt(2023, 8, 1) # Use dt alias
        end = dt(2023, 8, 2) # Use dt alias
        data_source = PandasData(datetime_start=start, datetime_end=end, pandas_data={})
        broker = BacktestingBroker(data_source=data_source)

        # Stop triggered by candle body
        stop_price = 95
        assert broker.stop_order(stop_price, 'sell', open_=100, high=110, low=90) == stop_price

        # Stop triggered by candle wick
        stop_price = 91
        assert broker.stop_order(stop_price, 'sell', open_=100, high=110, low=90) == stop_price

        # Stop Sell Triggered by a gap down candle
        stop_price = 115
        assert broker.stop_order(stop_price, 'sell', open_=100, high=110, low=90) == 100

        # Stop Buy Triggered by a gap up candle
        stop_price = 85
        assert broker.stop_order(stop_price, 'buy', open_=100, high=110, low=90) == 100

        # Stop not triggered
        stop_price = 80
        assert not broker.stop_order(stop_price, 'sell', open_=100, high=110, low=90)

    def test_submit_order_calls_conform_order(self):
        start = dt(2023, 8, 1) # Use dt alias
        end = dt(2023, 8, 2) # Use dt alias
        data_source = PandasData(datetime_start=start, datetime_end=end, pandas_data={})
        broker = BacktestingBroker(data_source=data_source)

        # mock _conform_order method
        broker._conform_order = MagicMock()
        Order(asset=Asset("SPY"), quantity=10, side="buy", strategy='abc')
        broker.submit_order(Order(asset=Asset("SPY"), quantity=10, side="buy", strategy='abc'))
        broker._conform_order.assert_called_once()

    def test_process_new_order_does_not_repromote_terminal_order(self):
        broker = BacktestingBroker.__new__(BacktestingBroker)
        broker._new_orders = SafeOrderDict(None)
        broker._unprocessed_orders = SafeOrderDict(None)
        broker._partially_filled_orders = SafeOrderDict(None)
        broker._placeholder_orders = SafeOrderDict(None)
        broker._filled_orders = SafeList(None)
        broker._canceled_orders = SafeList(None)
        broker._error_orders = SafeList(None)
        broker.logger = MagicMock()
        broker.logger.isEnabledFor.return_value = False
        broker.name = "backtesting"
        broker._hold_trade_events = False
        broker._on_new_order = MagicMock()

        filled_order = Order(asset=Asset("SPY"), quantity=1, side="buy", strategy="abc", identifier="same-id")
        filled_order.status = broker.FILLED_ORDER
        broker._filled_orders.append(filled_order)
        replayed_order = Order(asset=Asset("SPY"), quantity=1, side="buy", strategy="abc", identifier="same-id")

        result = broker._process_new_order(replayed_order)

        assert result is broker._REPLAYED_TERMINAL_ORDER
        assert len(broker._new_orders) == 0
        broker._process_trade_event(replayed_order, broker.NEW_ORDER)
        broker._on_new_order.assert_not_called()

    def test_market_order_prefers_quote_when_missing_ohlc(self):
        broker = BacktestingBroker.__new__(BacktestingBroker)
        broker.logger = MagicMock()
        broker.data_source = type("StubSource", (), {"_timestep": "minute"})()
        broker.get_quote = MagicMock(
            return_value=Quote(
                asset=Asset("SPY"),
                bid=99.0,
                ask=101.0,
            )
        )

        order = Order(
            asset=Asset("SPY"),
            quantity=10,
            side="buy",
            order_type=Order.OrderType.MARKET,
            strategy="test",
        )
        order.quote = Asset("USD", asset_type="forex")

        price = broker._try_fill_with_quote(order, strategy=None, open_=None, high_=None, low_=None)

        assert price == 101.0

    def test_quote_fill_attaches_audit_fields_when_enabled(self):
        broker = BacktestingBroker.__new__(BacktestingBroker)
        broker.logger = MagicMock()
        broker.data_source = type("StubSource", (), {"_timestep": "minute"})()
        broker.get_quote = MagicMock(
            return_value=Quote(
                asset=Asset("SPY"),
                bid=99.0,
                ask=101.0,
            )
        )

        order = Order(
            asset=Asset("SPY"),
            quantity=10,
            side="buy",
            order_type=Order.OrderType.MARKET,
            strategy="test",
        )
        order.quote = Asset("USD", asset_type="forex")

        with patch.dict(os.environ, {"LUMIBOT_BACKTEST_AUDIT": "1"}):
            price = broker._try_fill_with_quote(order, strategy=None, open_=None, high_=None, low_=None)

        assert price == 101.0
        assert hasattr(order, "_audit")
        assert order._audit.get("fill.model") == "quote_fallback"
        assert order._audit.get("asset_quote.bid") == 99.0
        assert order._audit.get("asset_quote.ask") == 101.0

    def test_resolve_provider_key_for_asset_prefers_ibkr_class(self):
        broker = BacktestingBroker.__new__(BacktestingBroker)
        broker.data_source = type("InteractiveBrokersRESTBacktesting", (), {})()
        provider = broker._resolve_provider_key_for_asset(Asset("SPY", asset_type="stock"))
        assert provider == "ibkr"

    def test_should_force_day_fill_timestep_true_for_routed_ibkr_daily_stock(self):
        broker = BacktestingBroker.__new__(BacktestingBroker)

        class _StubDataSource:
            _effective_day_mode = True
            _observed_intraday_cadence = False

            @staticmethod
            def _provider_spec_for_asset(_asset):
                return SimpleNamespace(provider="ibkr")

        broker.data_source = _StubDataSource()
        order = Order(
            asset=Asset("SPY", asset_type="stock"),
            quantity=1,
            side="buy",
            order_type=Order.OrderType.MARKET,
            strategy="test",
        )

        assert broker._should_force_day_fill_timestep(order) is True

    def test_should_force_day_fill_timestep_false_when_intraday_seen(self):
        broker = BacktestingBroker.__new__(BacktestingBroker)

        class _StubDataSource:
            _effective_day_mode = True
            _observed_intraday_cadence = True

            @staticmethod
            def _provider_spec_for_asset(_asset):
                return SimpleNamespace(provider="ibkr")

        broker.data_source = _StubDataSource()
        order = Order(
            asset=Asset("SPY", asset_type="stock"),
            quantity=1,
            side="buy",
            order_type=Order.OrderType.MARKET,
            strategy="test",
        )

        assert broker._should_force_day_fill_timestep(order) is False

    def test_get_active_tracked_orders_fallback_respects_asset_filter(self):
        broker = BacktestingBroker.__new__(BacktestingBroker)
        asset = Asset("SPY")
        other_asset = Asset("QQQ")
        matching = MagicMock()
        matching.asset = asset
        matching.is_active.return_value = True
        other = MagicMock()
        other.asset = other_asset
        other.is_active.return_value = True
        broker.get_tracked_orders = MagicMock(return_value=[matching, other])
        broker._unprocessed_orders = MagicMock()
        broker._unprocessed_orders.get_list.side_effect = RuntimeError("no bucket access")
        broker._new_orders = MagicMock()
        broker._partially_filled_orders = MagicMock()

        orders = broker.get_active_tracked_orders(strategy="test", asset=asset)

        assert orders == [matching]

    def test_get_active_tracked_orders_normalizes_strategy_object_for_simple_pending(self):
        broker = BacktestingBroker.__new__(BacktestingBroker)
        asset = Asset("SPY")
        strategy = SimpleNamespace(name="test")
        matching = MagicMock()
        matching.asset = asset
        matching.strategy = "test"
        matching.is_active.return_value = True
        broker._simple_new_orders_by_strategy = {"test": [matching]}
        broker._unprocessed_orders = MagicMock()
        broker._unprocessed_orders.get_list.return_value = []
        broker._new_orders = MagicMock()
        broker._new_orders.get_list.return_value = []
        broker._partially_filled_orders = MagicMock()
        broker._partially_filled_orders.get_list.return_value = []

        orders = broker.get_active_tracked_orders(strategy=strategy, asset=asset)

        assert orders == [matching]

    def test_get_time_to_close_marks_end_of_trading_days(self):
        broker = BacktestingBroker.__new__(BacktestingBroker)
        broker.data_source = SimpleNamespace(
            get_datetime=lambda: pd.Timestamp("2025-01-03 16:00:00", tz="America/New_York")
        )
        broker._trading_days = pd.DataFrame(
            {"market_open": [pd.Timestamp("2025-01-02 09:30:00", tz="America/New_York")]},
            index=pd.DatetimeIndex([pd.Timestamp("2025-01-02 16:00:00", tz="America/New_York")]),
        )
        broker._mark_end_of_trading_days = MagicMock()

        result = broker.get_time_to_close()

        assert result is None
        broker._mark_end_of_trading_days.assert_called_once_with(broker.datetime)

    def test_should_force_day_fill_timestep_false_for_options(self):
        broker = BacktestingBroker.__new__(BacktestingBroker)

        class _StubDataSource:
            _effective_day_mode = True
            _observed_intraday_cadence = False

            @staticmethod
            def _provider_spec_for_asset(_asset):
                return SimpleNamespace(provider="ibkr")

        broker.data_source = _StubDataSource()
        order = Order(
            asset=Asset(
                symbol="SPY",
                asset_type="option",
                expiration=dt(2025, 1, 17),
                strike=500,
                right=Asset.OptionRight.CALL,
            ),
            quantity=1,
            side="buy",
            order_type=Order.OrderType.MARKET,
            strategy="test",
        )

        assert broker._should_force_day_fill_timestep(order) is False

    def test_trade_event_log_includes_audit_columns_when_enabled(self):
        broker = BacktestingBroker.__new__(BacktestingBroker)
        broker.logger = MagicMock()
        broker.logger.isEnabledFor.return_value = False
        broker.name = "backtesting"
        broker._hold_trade_events = False
        broker._held_trades = []
        broker._trade_event_log_enabled = True
        broker._trade_event_log_rows = []
        broker._get_subscriber = MagicMock(return_value=None)
        broker._process_filled_order = MagicMock(return_value=None)
        broker.data_source = type(
            "StubSource",
            (),
            {"get_datetime": lambda self: pytz.UTC.localize(dt(2025, 1, 2, 10, 0))},
        )()

        order = Order(
            asset=Asset("SPY"),
            quantity=1,
            side="buy",
            order_type=Order.OrderType.MARKET,
            strategy="test",
        )
        order._audit = {"hello": "world"}

        with patch.dict(os.environ, {"LUMIBOT_BACKTEST_AUDIT": "1"}):
            broker._process_trade_event(order, broker.FILLED_ORDER, price=101.0, filled_quantity=1)

        row = broker._trade_event_log_rows[-1]
        assert row["audit.hello"] == "world"
        assert not hasattr(order, "_audit")

    def test_direct_filled_order_failure_skips_post_fill_side_effects(self):
        broker = BacktestingBroker.__new__(BacktestingBroker)
        default_action = object()
        broker.stream = SimpleNamespace(_actions_mapping={broker.FILLED_ORDER: default_action})
        broker._default_filled_order_stream_action = default_action
        broker._backtest_audit_enabled = False
        broker._process_filled_order = MagicMock(side_effect=RuntimeError("boom"))
        broker._process_futures_fill = MagicMock()
        broker._on_filled_order = MagicMock()
        broker._record_fast_backtest_trade_event = MagicMock()
        broker.calculate_trade_cost = MagicMock(return_value=Decimal("1.25"))
        broker._apply_trade_cost = MagicMock()
        broker.get_tracked_order = MagicMock()
        broker._futures_lot_ledgers = {}

        order = Order(
            asset=Asset("SPY"),
            quantity=1,
            side="buy",
            order_type=Order.OrderType.MARKET,
            strategy="test",
        )
        strategy = SimpleNamespace(broker=broker)

        broker._execute_filled_order(order, 101.0, Decimal("1"), strategy)

        broker._record_fast_backtest_trade_event.assert_not_called()
        broker._apply_trade_cost.assert_not_called()
        broker.get_tracked_order.assert_not_called()

    def test_get_next_trading_day_marks_end_of_trading_days(self):
        """Regression: reaching end of trading calendar should stop backtest (no infinite loop)."""
        broker = BacktestingBroker.__new__(BacktestingBroker)
        broker.option_source = None
        broker._end_of_trading_days_reached = False

        tz = pytz.timezone("America/New_York")
        now = tz.localize(dt(2025, 1, 3, 12, 0))

        # Simulate a datasource whose configured end extends beyond available trading days.
        broker.data_source = type(
            "StubSource",
            (),
            {
                "datetime_end": tz.localize(dt(2025, 2, 1)),
                "get_datetime": lambda self: now,
            },
        )()

        open_1 = tz.localize(dt(2025, 1, 2, 9, 30))
        close_1 = tz.localize(dt(2025, 1, 2, 16, 0))
        broker._trading_days = pd.DataFrame({"market_open": [open_1]}, index=[close_1])

        assert broker._get_next_trading_day() is None
        assert broker._end_of_trading_days_reached is True
        assert broker.data_source.datetime_end == now
        assert broker.should_continue() is False

    def test_export_trade_events_to_csv_emits_parquet(self, tmp_path):
        broker = BacktestingBroker.__new__(BacktestingBroker)
        broker._trade_event_log_df = pd.DataFrame(
            [
                {"time": dt(2025, 1, 1, 10, 0), "status": "new", "symbol": "SPY", "price": 100.0},
                {"time": dt(2025, 1, 1, 10, 1), "status": "fill", "symbol": "SPY", "price": 101.0},
            ]
        )

        out_csv = tmp_path / "trade_events.csv"
        broker.export_trade_events_to_csv(out_csv.as_posix())

        assert out_csv.exists()
        assert out_csv.with_suffix(".parquet").exists()

        parquet_df = pd.read_parquet(out_csv.with_suffix(".parquet"))
        assert "time" in parquet_df.columns
        assert "status" in parquet_df.columns

    def test_option_expiry_short_put_assignment_delivers_stock(self):
        start = dt(2023, 8, 1)
        end = dt(2023, 8, 2)
        data_source = PandasData(datetime_start=start, datetime_end=end, pandas_data={})
        broker = BacktestingBroker(data_source=data_source)
        strategy = _OptionSettlementStrategyStub(broker=broker, cash=50_000.0)

        underlying = Asset(symbol="AAPL", asset_type="stock")
        option = Asset(
            symbol="AAPL",
            asset_type="option",
            expiration=datetime.date(2023, 8, 1),
            strike=100,
            right=Asset.OptionRight.PUT,
            multiplier=100,
            underlying_asset=underlying,
        )
        broker._filled_positions.append(Position(strategy.name, option, quantity=-1))
        broker.get_last_price = MagicMock(return_value=95.0)

        broker.settle_expired_option_contract(
            broker.get_tracked_position(strategy.name, option),
            strategy,
        )

        events = broker._trade_event_log_df
        option_events = events[(events["symbol"] == "AAPL") & (events["asset.asset_type"] == "option")]
        stock_events = events[(events["symbol"] == "AAPL") & (events["asset.asset_type"] == "stock")]

        assert "assigned" in option_events["status"].tolist()
        assert "assigned" in option_events["type"].tolist()
        assert not stock_events.empty
        assert "fill" in stock_events["status"].tolist()
        assert "assigned" in stock_events["type"].tolist()

        stock_position = broker.get_tracked_position(strategy.name, underlying)
        assert stock_position is not None
        assert stock_position.quantity == 100.0
        assert broker.get_tracked_position(strategy.name, option) is None

    def test_option_expiry_long_call_exercise_delivers_stock_when_supported(self):
        start = dt(2023, 8, 1)
        end = dt(2023, 8, 2)
        data_source = PandasData(datetime_start=start, datetime_end=end, pandas_data={})
        broker = BacktestingBroker(data_source=data_source)
        strategy = _OptionSettlementStrategyStub(broker=broker, cash=10_000.0)

        underlying = Asset(symbol="MSFT", asset_type="stock")
        option = Asset(
            symbol="MSFT",
            asset_type="option",
            expiration=datetime.date(2023, 8, 1),
            strike=50,
            right=Asset.OptionRight.CALL,
            multiplier=100,
            underlying_asset=underlying,
        )
        broker._filled_positions.append(Position(strategy.name, option, quantity=1))
        broker.get_last_price = MagicMock(return_value=55.0)

        broker.settle_expired_option_contract(
            broker.get_tracked_position(strategy.name, option),
            strategy,
        )

        events = broker._trade_event_log_df
        option_events = events[(events["symbol"] == "MSFT") & (events["asset.asset_type"] == "option")]
        stock_events = events[(events["symbol"] == "MSFT") & (events["asset.asset_type"] == "stock")]

        assert "exercised" in option_events["status"].tolist()
        assert "exercised" in option_events["type"].tolist()
        assert not stock_events.empty
        assert "fill" in stock_events["status"].tolist()
        assert "exercised" in stock_events["type"].tolist()
        assert broker.get_tracked_position(strategy.name, underlying).quantity == 100.0
        assert broker.get_tracked_position(strategy.name, option) is None

    def test_option_expiry_long_call_itm_with_insufficient_cash_cash_settles(self):
        start = dt(2023, 8, 1)
        end = dt(2023, 8, 2)
        data_source = PandasData(datetime_start=start, datetime_end=end, pandas_data={})
        broker = BacktestingBroker(data_source=data_source)
        strategy = _OptionSettlementStrategyStub(broker=broker, cash=100.0)

        underlying = Asset(symbol="NVDA", asset_type="stock")
        option = Asset(
            symbol="NVDA",
            asset_type="option",
            expiration=datetime.date(2023, 8, 1),
            strike=300,
            right=Asset.OptionRight.CALL,
            multiplier=100,
            underlying_asset=underlying,
        )
        broker._filled_positions.append(Position(strategy.name, option, quantity=1))
        broker.get_last_price = MagicMock(return_value=350.0)

        broker.settle_expired_option_contract(
            broker.get_tracked_position(strategy.name, option),
            strategy,
        )

        events = broker._trade_event_log_df
        option_events = events[(events["symbol"] == "NVDA") & (events["asset.asset_type"] == "option")]
        stock_events = events[(events["symbol"] == "NVDA") & (events["asset.asset_type"] == "stock")]

        assert "cash_settled" in option_events["status"].tolist()
        assert "cash_settled" in option_events["type"].tolist()
        assert stock_events.empty
        assert broker.get_tracked_position(strategy.name, underlying) is None
        assert strategy.cash == 5_100.0

    def test_option_expiry_index_option_itm_cash_settles(self):
        start = dt(2023, 8, 1)
        end = dt(2023, 8, 2)
        data_source = PandasData(datetime_start=start, datetime_end=end, pandas_data={})
        broker = BacktestingBroker(data_source=data_source)
        strategy = _OptionSettlementStrategyStub(broker=broker, cash=1_000.0)

        underlying = Asset(symbol="SPX", asset_type="index")
        option = Asset(
            symbol="SPX",
            asset_type="option",
            expiration=datetime.date(2023, 8, 1),
            strike=5000,
            right=Asset.OptionRight.CALL,
            multiplier=100,
            underlying_asset=underlying,
        )
        broker._filled_positions.append(Position(strategy.name, option, quantity=1))
        broker.get_last_price = MagicMock(return_value=5100.0)

        broker.settle_expired_option_contract(
            broker.get_tracked_position(strategy.name, option),
            strategy,
        )

        events = broker._trade_event_log_df
        option_events = events[(events["symbol"] == "SPX") & (events["asset.asset_type"] == "option")]
        stock_events = events[(events["symbol"] == "SPX") & (events["asset.asset_type"] == "stock")]

        assert "cash_settled" in option_events["status"].tolist()
        assert "cash_settled" in option_events["type"].tolist()
        assert stock_events.empty
        assert strategy.cash == 11_000.0

    def test_option_early_assignment_short_call_delivers_stock_before_expiry(self):
        start = dt(2023, 8, 1)
        end = dt(2023, 8, 2)
        data_source = PandasData(datetime_start=start, datetime_end=end, pandas_data={})
        broker = BacktestingBroker(data_source=data_source)
        strategy = _OptionSettlementStrategyStub(broker=broker, cash=50_000.0)
        strategy.parameters.update(
            {
                "option_early_assignment_enabled": True,
                "option_early_assignment_max_dte_days": 30,
                "option_early_assignment_max_extrinsic": 0.05,
            }
        )

        underlying = Asset(symbol="AAPL", asset_type="stock")
        option = Asset(
            symbol="AAPL",
            asset_type="option",
            expiration=datetime.date(2023, 8, 15),
            strike=100,
            right=Asset.OptionRight.CALL,
            multiplier=100,
            underlying_asset=underlying,
        )
        broker._filled_positions.append(Position(strategy.name, option, quantity=-1))

        def _mock_last_price(asset):
            if asset.asset_type == Asset.AssetType.OPTION:
                return 10.02  # intrinsic=10, extrinsic=0.02 -> should assign
            return 110.0

        broker.get_last_price = MagicMock(side_effect=_mock_last_price)
        broker.process_early_assignment_contracts(strategy, force=True)

        events = broker._trade_event_log_df
        option_events = events[(events["symbol"] == "AAPL") & (events["asset.asset_type"] == "option")]
        stock_events = events[(events["symbol"] == "AAPL") & (events["asset.asset_type"] == "stock")]

        assert "assigned" in option_events["status"].tolist()
        assert "assigned" in option_events["type"].tolist()
        assert "fill" in stock_events["status"].tolist()
        assert "assigned" in stock_events["type"].tolist()
        assert broker.get_tracked_position(strategy.name, option) is None
        assert broker.get_tracked_position(strategy.name, underlying).quantity == -100.0

    def test_option_early_assignment_skips_when_extrinsic_is_high(self):
        start = dt(2023, 8, 1)
        end = dt(2023, 8, 2)
        data_source = PandasData(datetime_start=start, datetime_end=end, pandas_data={})
        broker = BacktestingBroker(data_source=data_source)
        strategy = _OptionSettlementStrategyStub(broker=broker, cash=50_000.0)
        strategy.parameters.update(
            {
                "option_early_assignment_enabled": True,
                "option_early_assignment_max_dte_days": 30,
                "option_early_assignment_max_extrinsic": 0.05,
            }
        )

        underlying = Asset(symbol="MSFT", asset_type="stock")
        option = Asset(
            symbol="MSFT",
            asset_type="option",
            expiration=datetime.date(2023, 8, 15),
            strike=100,
            right=Asset.OptionRight.CALL,
            multiplier=100,
            underlying_asset=underlying,
        )
        broker._filled_positions.append(Position(strategy.name, option, quantity=-1))

        def _mock_last_price(asset):
            if asset.asset_type == Asset.AssetType.OPTION:
                return 12.5  # intrinsic=10, extrinsic=2.5 -> should not assign
            return 110.0

        broker.get_last_price = MagicMock(side_effect=_mock_last_price)
        broker.process_early_assignment_contracts(strategy, force=True)

        events = getattr(broker, "_trade_event_log_df", pd.DataFrame())
        if not events.empty:
            option_events = events[(events["symbol"] == "MSFT") & (events["asset.asset_type"] == "option")]
            assert "assigned" not in option_events["status"].tolist()

        assert broker.get_tracked_position(strategy.name, option) is not None
        assert broker.get_tracked_position(strategy.name, underlying) is None


# New Test Class for Time Advancement Logic
class TestBacktestingBrokerTimeAdvance(unittest.TestCase):

    def setUp(self):
        """Set up a mock BacktestingBroker instance for testing."""
        # Use patch to mock the data_source during instantiation or assign afterwards
        with patch('lumibot.backtesting.backtesting_broker.DataSourceBacktesting') as MockDataSource:
            # Prevent __init__ from running fully if it causes issues
            self.broker = BacktestingBroker.__new__(BacktestingBroker)
            # Mock necessary attributes that would normally be set in __init__
            self.broker._trading_days = pd.DataFrame() # Initialize attribute
            self.broker.data_source = MockDataSource() # Assign mock data_source

        self.broker.logger = MagicMock()
        # Mock the get_datetime method on the data_source
        self.mock_datetime = pd.Timestamp('2023-01-01 10:00:00', tz='America/New_York')
        self.broker.data_source.get_datetime = MagicMock(return_value=self.mock_datetime)

        # Setup trading days directly on the broker instance for testing internal logic
        # Ensure _trading_days is set correctly after __new__
        self.broker._trading_days = pd.DataFrame({
            'market_open': [pd.Timestamp('2023-01-01 09:30:00', tz='America/New_York')],
            'market_close': [pd.Timestamp('2023-01-01 16:00:00', tz='America/New_York')]
        }, index=[pd.Timestamp('2023-01-01 16:00:00', tz='America/New_York')]) # Index is market_close_time

        # Mock other methods used by the tested logic
        self.broker.get_time_to_close = MagicMock()
        self.broker.get_time_to_open = MagicMock() # Mock get_time_to_open
        self.broker._update_datetime = MagicMock()
        self.broker.process_pending_orders = MagicMock()
        self.mock_strategy = MagicMock() # Mock strategy object

    def _set_current_time(self, timestamp_str):
        """Helper to set the mock time."""
        self.mock_datetime = pd.Timestamp(timestamp_str, tz='America/New_York')
        self.broker.data_source.get_datetime.return_value = self.mock_datetime

    def test_await_close_during_market_hours_no_buffer(self):
        """Test _await_market_to_close during market hours without buffer."""
        self._set_current_time('2023-01-01 15:30:00')
        market_close_time = pd.Timestamp('2023-01-01 16:00:00', tz='America/New_York')
        expected_time_to_close_seconds = (market_close_time - self.mock_datetime).total_seconds() # 1800

        # Mock get_time_to_close to return the calculated value
        self.broker.get_time_to_close.return_value = expected_time_to_close_seconds

        # Call the method under test
        self.broker._await_market_to_close(strategy=self.mock_strategy)

        # Assertions
        self.broker.process_pending_orders.assert_called_once_with(strategy=self.mock_strategy)
        self.broker.get_time_to_close.assert_called_once()
        self.broker._update_datetime.assert_called_once_with(expected_time_to_close_seconds)

    def test_await_close_get_time_to_close_returns_none(self):
        """Test _await_market_to_close when get_time_to_close returns None (e.g., market closed)."""
        # Simulate a time when market might be considered closed or get_time_to_close fails
        self._set_current_time('2023-01-01 17:00:00')

        # Mock get_time_to_close returning None
        self.broker.get_time_to_close.return_value = None # Simulate market closed or error

        # Call the method under test
        self.broker._await_market_to_close(strategy=self.mock_strategy)

        # Assertions
        self.broker.process_pending_orders.assert_called_once_with(strategy=self.mock_strategy)
        self.broker.get_time_to_close.assert_called_once()
        # _update_datetime should NOT be called if time_to_close is None or <= 0
        self.broker._update_datetime.assert_not_called()

    def test_await_close_with_buffer(self):
        """Test _await_market_to_close with a timedelta buffer."""
        self._set_current_time('2023-01-01 15:00:00')
        market_close_time = pd.Timestamp('2023-01-01 16:00:00', tz='America/New_York')
        base_time_to_close = (market_close_time - self.mock_datetime).total_seconds() # 3600 seconds
        buffer_minutes = 5
        expected_update_time = base_time_to_close - (buffer_minutes * 60) # 3300 seconds

        # Mock get_time_to_close returning the base value
        self.broker.get_time_to_close.return_value = base_time_to_close

        # Call the method under test with the buffer
        self.broker._await_market_to_close(timedelta=buffer_minutes, strategy=self.mock_strategy)

        # Assertions
        self.broker.process_pending_orders.assert_called_once_with(strategy=self.mock_strategy)
        self.broker.get_time_to_close.assert_called_once()
        self.broker._update_datetime.assert_called_once_with(expected_update_time)

    def test_await_close_when_already_past_close_no_buffer(self):
        """Test _await_market_to_close when current time is past market close (no buffer)."""
        self._set_current_time('2023-01-01 16:01:00')

        # Mock get_time_to_close returning 0 or negative
        self.broker.get_time_to_close.return_value = -60

        # Call the method under test
        self.broker._await_market_to_close(strategy=self.mock_strategy)

        # Assertions
        self.broker.process_pending_orders.assert_called_once_with(strategy=self.mock_strategy)
        self.broker.get_time_to_close.assert_called_once()
        # _update_datetime should NOT be called
        self.broker._update_datetime.assert_not_called()

    # ===== Tests for _await_market_to_open =====

    def test_await_open_before_market_opens_no_buffer(self):
        """Test _await_market_to_open before market opens without buffer."""
        self._set_current_time('2023-01-01 09:00:00')
        market_open_time = pd.Timestamp('2023-01-01 09:30:00', tz='America/New_York')
        expected_time_to_open_seconds = (market_open_time - self.mock_datetime).total_seconds() # 1800

        # Mock get_time_to_open to return the calculated value
        self.broker.get_time_to_open.return_value = expected_time_to_open_seconds

        # Call the method under test
        self.broker._await_market_to_open(strategy=self.mock_strategy)

        # Assertions
        self.broker.process_pending_orders.assert_called_once_with(strategy=self.mock_strategy)
        self.broker.get_time_to_open.assert_called_once()
        self.broker._update_datetime.assert_called_once_with(expected_time_to_open_seconds)

    def test_await_open_with_buffer(self):
        """Test _await_market_to_open with a timedelta buffer."""
        self._set_current_time('2023-01-01 08:00:00')
        market_open_time = pd.Timestamp('2023-01-01 09:30:00', tz='America/New_York')
        base_time_to_open = (market_open_time - self.mock_datetime).total_seconds() # 5400 seconds
        buffer_minutes = 5
        expected_update_time = base_time_to_open - (buffer_minutes * 60) # 5100 seconds

        # Mock get_time_to_open returning the base value
        self.broker.get_time_to_open.return_value = base_time_to_open

        # Call the method under test with the buffer
        self.broker._await_market_to_open(timedelta=buffer_minutes, strategy=self.mock_strategy)

        # Assertions
        self.broker.process_pending_orders.assert_called_once_with(strategy=self.mock_strategy)
        self.broker.get_time_to_open.assert_called_once()
        self.broker._update_datetime.assert_called_once_with(expected_update_time)

    def test_await_open_when_market_already_open(self):
        """Test _await_market_to_open when the market is already open (time_to_open is 0)."""
        self._set_current_time('2023-01-01 10:00:00') # Time is during market hours

        # Mock get_time_to_open returning 0
        self.broker.get_time_to_open.return_value = 0

        # Call the method under test
        self.broker._await_market_to_open(strategy=self.mock_strategy)

        # Assertions
        self.broker.process_pending_orders.assert_called_once_with(strategy=self.mock_strategy)
        self.broker.get_time_to_open.assert_called_once()
        # _update_datetime should NOT be called because time_to_open is 0
        self.broker._update_datetime.assert_not_called()

    def test_await_open_with_buffer_making_time_negative(self):
        """Test _await_market_to_open when buffer makes time_to_open non-positive."""
        self._set_current_time('2023-01-01 09:28:00') # 2 minutes before open
        market_open_time = pd.Timestamp('2023-01-01 09:30:00', tz='America/New_York')
        base_time_to_open = (market_open_time - self.mock_datetime).total_seconds() # 120 seconds
        buffer_minutes = 3 # 180 seconds buffer

        # Mock get_time_to_open returning the base value
        self.broker.get_time_to_open.return_value = base_time_to_open

        # Call the method under test with the buffer
        self.broker._await_market_to_open(timedelta=buffer_minutes, strategy=self.mock_strategy)

        # Assertions
        self.broker.process_pending_orders.assert_called_once_with(strategy=self.mock_strategy)
        self.broker.get_time_to_open.assert_called_once()
        # _update_datetime should NOT be called because calculated time_to_open is <= 0
        self.broker._update_datetime.assert_not_called()


if __name__ == '__main__':
    unittest.main()
