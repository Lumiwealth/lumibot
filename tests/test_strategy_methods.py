from datetime import date, datetime, timedelta
import logging
import uuid
from types import SimpleNamespace
from unittest.mock import patch, MagicMock
import pytest
import pandas as pd

from lumibot.backtesting import BacktestingBroker, YahooDataBacktesting
from lumibot.example_strategies.stock_buy_and_hold import BuyAndHold
from lumibot.entities import Asset, Order, Position
from lumibot.strategies.strategy import Strategy
from apscheduler.triggers.cron import CronTrigger
from lumibot.constants import LUMIBOT_DEFAULT_PYTZ, LUMIBOT_DEFAULT_TIMEZONE


class FakeSnapshotSource:
    def __init__(self):
        self.snapshot = None
        self.last_price_calls = 0

    def get_price_snapshot(self, asset, *args, **kwargs):
        return self.snapshot

    def get_last_price(self, asset, *args, **kwargs):
        self.last_price_calls += 1
        return None


# LEGACY TEST CLASS (created Aug 2023)
# These tests explicitly test YahooDataBacktesting and must not be overridden
# by the BACKTESTING_DATA_SOURCE environment variable.
@pytest.mark.usefixtures("disable_datasource_override")
class TestStrategyMethods:
    def _make_strategy_stub(self):
        strat = Strategy.__new__(Strategy)
        strat.logger = logging.getLogger(__name__)
        return strat

    def test_get_option_expiration_after_date(self):
        """
        Test the get_option_expiration_after_date method by checking that the correct expiration date is returned
        """
        date_start = datetime(2021, 7, 10)
        date_end = datetime(2021, 7, 13)
        data_source = YahooDataBacktesting(date_start, date_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = BuyAndHold(
            backtesting_broker,
            backtesting_start=date_start,
            backtesting_end=date_end,
        )

        # Get the expiration date
        expiry_date = strategy.get_option_expiration_after_date(
            datetime(2023, 4, 2)
        )

        # Check that the expiration date is correct
        assert expiry_date == date(2023, 4, 21)

        # Get the expiration date
        expiry_date = strategy.get_option_expiration_after_date(
            datetime(2023, 7, 12)
        )

        # Check that the expiration date is correct
        assert expiry_date == date(2023, 7, 21)

        # Get the expiration date
        expiry_date = strategy.get_option_expiration_after_date(
            datetime(2023, 6, 29)
        )

        # Check that the expiration date is correct
        assert expiry_date == date(2023, 7, 21)

    def test_validate_order_with_none_quantity(self):
        """
        Test that _validate_order rejects orders with None quantity
        """
        date_start = datetime(2021, 7, 10)
        date_end = datetime(2021, 7, 13)
        data_source = YahooDataBacktesting(date_start, date_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = BuyAndHold(
            backtesting_broker,
            backtesting_start=date_start,
            backtesting_end=date_end,
        )

        # Create an order with None quantity
        order = Order(
            asset=Asset("SPY"), 
            quantity=None, 
            side=Order.OrderSide.BUY, 
            strategy='test_strategy'
        )

        # Test that validation fails
        is_valid = strategy._validate_order(order)
        assert is_valid == False

    def test_validate_order_with_zero_quantity(self):
        """
        Test that _validate_order rejects orders with zero quantity
        """
        date_start = datetime(2021, 7, 10)
        date_end = datetime(2021, 7, 13)
        data_source = YahooDataBacktesting(date_start, date_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = BuyAndHold(
            backtesting_broker,
            backtesting_start=date_start,
            backtesting_end=date_end,
        )

        # Create an order with zero quantity
        order = Order(
            asset=Asset("SPY"), 
            quantity=0, 
            side=Order.OrderSide.BUY, 
            strategy='test_strategy'
        )

        # Test that validation fails
        is_valid = strategy._validate_order(order)
        assert is_valid == False

    def test_validate_order_with_valid_quantity(self):
        """
        Test that _validate_order accepts orders with valid quantity
        """
        date_start = datetime(2021, 7, 10)
        date_end = datetime(2021, 7, 13)
        data_source = YahooDataBacktesting(date_start, date_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = BuyAndHold(
            backtesting_broker,
            backtesting_start=date_start,
            backtesting_end=date_end,
        )

        # Create an order with valid quantity
        order = Order(
            asset=Asset("SPY"), 
            quantity=100, 
            side=Order.OrderSide.BUY, 
            strategy='test_strategy'
        )

        # Test that validation passes
        is_valid = strategy._validate_order(order)
        assert is_valid == True

    def test_validate_order_with_none_order(self):
        """
        Test that _validate_order rejects None orders
        """
        date_start = datetime(2021, 7, 10)
        date_end = datetime(2021, 7, 13)
        data_source = YahooDataBacktesting(date_start, date_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = BuyAndHold(
            backtesting_broker,
            backtesting_start=date_start,
            backtesting_end=date_end,
        )

        # Test that validation fails for None order
        is_valid = strategy._validate_order(None)
        assert is_valid == False

    def test_create_order_none_order_type_uses_simple_market_backtest_fast_path(self):
        strat = self._make_strategy_stub()
        quote = Asset("USD", Asset.AssetType.FOREX)
        strat._name = "test_strategy"
        strat._quote_asset = quote
        strat.broker = SimpleNamespace(
            IS_BACKTESTING_BROKER=True,
            data_source=SimpleNamespace(_datetime=datetime(2024, 1, 2, 9, 30)),
            datetime=datetime(2024, 1, 2, 9, 30),
        )

        order = Strategy.create_order(strat, Asset("SPY"), 1, "buy", order_type=None)

        assert getattr(order, "_simple_backtest_order", False) is True
        assert order.order_type is Order.OrderType.MARKET
        assert order.quote == quote

    def test_simple_backtest_submit_order_uses_strategy_validation(self):
        strat = self._make_strategy_stub()
        strat._validate_order = MagicMock(return_value=False)
        strat.broker = SimpleNamespace(
            IS_BACKTESTING_BROKER=True,
            _submit_simple_backtest_order=MagicMock(),
            _submit_order=MagicMock(),
        )
        order = Order.simple_market_backtest("test_strategy", Asset("SPY"), 1, Order.OrderSide.BUY)

        result = Strategy.submit_order(strat, order)

        assert result is None
        strat._validate_order.assert_called_once_with(order)
        strat.broker._submit_simple_backtest_order.assert_not_called()
        strat.broker._submit_order.assert_not_called()

    def test_get_price_from_source_snapshot_fallback(self):
        strat = self._make_strategy_stub()
        strat._should_use_daily_last_price = MagicMock(return_value=False)
        strat.get_last_price = MagicMock(return_value=321)
        strat._pick_snapshot_price = MagicMock(return_value=42.0)

        dummy_source = MagicMock()
        dummy_source.get_price_snapshot.return_value = {"fake": "snapshot"}

        asset = Asset("QQQ", Asset.AssetType.STOCK)
        result = Strategy._get_price_from_source(strat, dummy_source, asset)

        assert result == 42.0
        strat._pick_snapshot_price.assert_called_once()
        strat.get_last_price.assert_not_called()

    def test_get_last_price_prefers_day_bars_for_routed_backtesting_daily_cadence(self):
        """The routed/IBKR/ThetaData daily shortcut must request exactly ONE
        bar at or before sim_time.

        IMPORTANT — bug history (2026-04-17 Alpha Picks incident):
            The earlier version of this test mocked `get_historical_prices`
            to return a TWO-row DataFrame (`[100.0, 101.0]`) and asserted the
            shortcut returned 101.0 (the second row). That assertion pinned
            the *bug*: it proved the shortcut could read a bar AFTER
            sim_time when its call was `length=2, timeshift=-1`, and the
            frame's second row was look-ahead. In production that second row
            turned out to be real-now's market close (via a polluted cache
            frame) and polluted position sizing on every IBKR stock backtest.

            This test now asserts the correct sim-time-safe contract:
              1. The shortcut MUST call `get_historical_prices` with
                 `length=1` and either `timeshift=0` or omitted (no negative
                 shift). See `tests/test_get_last_price_sim_time_safety.py`
                 for the whitebox regression on the call signature.
              2. The shortcut MUST return the close of the single bar it
                 asked for — so when `get_historical_prices` returns a
                 DataFrame with one row `[100.0]`, the answer is `100.0`.
        """
        strat = self._make_strategy_stub()
        strat._quote_asset = Asset("USD", Asset.AssetType.FOREX)
        strat._should_use_daily_last_price = MagicMock(return_value=True)
        strat._sanitize_user_asset = lambda asset: asset

        class RoutedBacktestingPandas:
            pass

        broker = MagicMock()
        broker.IS_BACKTESTING_BROKER = True
        broker.data_source = RoutedBacktestingPandas()
        broker.get_last_price = MagicMock(return_value=999.0)
        strat.broker = broker

        # Record exactly how the shortcut calls get_historical_prices so the
        # test fails if a future edit restores `length=2, timeshift=-1`.
        captured_calls: list[dict] = []

        def _capture(*args, **kwargs):
            captured_calls.append(
                {
                    "length": kwargs.get("length"),
                    "timestep": kwargs.get("timestep"),
                    "timeshift": kwargs.get("timeshift"),
                }
            )
            return SimpleNamespace(df=pd.DataFrame({"close": [100.0]}))

        strat.get_historical_prices = MagicMock(side_effect=_capture)

        price = Strategy.get_last_price(strat, Asset("SPY", Asset.AssetType.STOCK))

        # Single bar at sim_time → shortcut returns that bar's close.
        assert price == 100.0, (
            "Shortcut must return the single pre-sim bar's close. Any other "
            "value means the shortcut is reading a forbidden slice."
        )
        strat.get_historical_prices.assert_called_once()
        broker.get_last_price.assert_not_called()

        # The specific param combo is the crux of the fix — pin it.
        assert len(captured_calls) == 1
        call = captured_calls[0]
        assert call["length"] == 1, (
            f"Shortcut called length={call['length']} — must be 1. length>=2 "
            "reintroduces the Alpha Picks 2026-04-17 look-ahead bug."
        )
        assert call["timestep"] == "day"
        assert call["timeshift"] in (None, 0), (
            f"Shortcut called timeshift={call['timeshift']} — must be 0 or "
            "omitted. Any negative timeshift walks forward past sim_time."
        )

    def test_validate_order_with_invalid_order_type(self):
        """
        Test that _validate_order rejects non-Order objects
        """
        date_start = datetime(2021, 7, 10)
        date_end = datetime(2021, 7, 13)
        data_source = YahooDataBacktesting(date_start, date_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = BuyAndHold(
            backtesting_broker,
            backtesting_start=date_start,
            backtesting_end=date_end,
        )

        # Test that validation fails for non-Order object
        is_valid = strategy._validate_order("not an order")
        assert is_valid == False

    @patch('uuid.uuid4')
    def test_register_cron_callback_returns_job_id(self, mock_uuid4):
        """
        Test that register_cron_callback returns a job ID
        """
        # Mock uuid4 to return a predictable value
        mock_uuid = MagicMock()
        mock_uuid.hex = "test-uuid"
        mock_uuid4.return_value = mock_uuid

        date_start = datetime(2021, 7, 10)
        date_end = datetime(2021, 7, 13)
        data_source = YahooDataBacktesting(date_start, date_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = BuyAndHold(
            backtesting_broker,
            backtesting_start=date_start,
            backtesting_end=date_end,
        )

        # Set is_backtesting to False for this test
        strategy.is_backtesting = False

        # Mock the scheduler's add_job method
        strategy._executor.scheduler.add_job = MagicMock(return_value=None)

        # Define a callback function
        def test_callback():
            pass

        # Register the callback
        job_id = strategy.register_cron_callback("0 9 * * 1-5", test_callback)

        # Check that the job ID is correct
        assert job_id == "cron_callback_test-uuid"

    def test_update_portfolio_value_with_missing_price(self):
        """_update_portfolio_value should skip assets whose prices are missing instead of raising."""

        date_start = datetime(2021, 7, 10)
        date_end = datetime(2021, 7, 13)
        data_source = YahooDataBacktesting(date_start, date_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = BuyAndHold(
            backtesting_broker,
            backtesting_start=date_start,
            backtesting_end=date_end,
        )

        asset = Asset("SPY")
        position = Position(strategy._name, asset, quantity=1, avg_fill_price=430.0)
        strategy.broker._filled_positions.append(position)

        with patch.object(strategy.broker.data_source, "get_last_price", return_value=None):
            original_value = strategy.get_portfolio_value()
            updated_value = strategy._update_portfolio_value()

        assert updated_value == original_value

    def _setup_strategy_with_option_position(self):
        date_start = datetime(2024, 1, 1)
        date_end = datetime(2024, 1, 10)
        data_source = YahooDataBacktesting(date_start, date_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = BuyAndHold(
            backtesting_broker,
            backtesting_start=date_start,
            backtesting_end=date_end,
        )
        option_asset = Asset(
            "CVNA",
            asset_type="option",
            expiration=date(2026, 1, 16),
            strike=180.0,
            right="CALL",
        )
        option_asset.multiplier = 100
        position = Position(strategy._name, option_asset, quantity=2, avg_fill_price=60.0)
        strategy.broker.get_tracked_positions = MagicMock(return_value=[position])
        strategy._quote_asset = Asset("USD", asset_type="forex")
        source = FakeSnapshotSource()
        strategy.broker.option_source = source
        strategy.broker.data_source = MagicMock()
        strategy.broker.data_source.get_last_price = MagicMock(return_value=None)
        return strategy, position, option_asset, source

    def test_update_portfolio_value_prefers_fresh_trade_snapshot(self):
        strategy, position, option_asset, source = self._setup_strategy_with_option_position()
        now = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2025, 4, 7, 10, 30))
        strategy.broker.data_source.get_datetime = MagicMock(return_value=now)
        source.snapshot = {
            "open": 60.0,
            "high": 66.0,
            "low": 58.0,
            "close": 65.0,
            "bid": 64.5,
            "ask": 65.5,
            "last_trade_time": now - timedelta(seconds=30),
            "last_bid_time": now - timedelta(seconds=20),
            "last_ask_time": now - timedelta(seconds=10),
        }
        starting_cash = strategy.cash

        value = strategy._update_portfolio_value()
        expected_price = 65.0
        assert value == pytest.approx(starting_cash + position.quantity * option_asset.multiplier * expected_price)
        assert source.last_price_calls == 0

    def test_update_portfolio_value_uses_mid_when_trade_stale(self):
        strategy, position, option_asset, source = self._setup_strategy_with_option_position()
        now = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2025, 4, 7, 10, 30))
        strategy.broker.data_source.get_datetime = MagicMock(return_value=now)
        source.snapshot = {
            "open": 60.0,
            "high": 66.0,
            "low": 58.0,
            "close": 65.0,
            "bid": 70.0,
            "ask": 74.0,
            "last_trade_time": now - timedelta(minutes=10),
            "last_bid_time": now - timedelta(seconds=20),
            "last_ask_time": now - timedelta(seconds=10),
        }
        starting_cash = strategy.cash

        with patch.object(strategy.logger, "warning") as warning_mock:
            value = strategy._update_portfolio_value()

        expected_price = (70.0 + 74.0) / 2.0
        assert value == pytest.approx(starting_cash + position.quantity * option_asset.multiplier * expected_price)
        warning_mock.assert_not_called()
        assert source.last_price_calls == 0

    def test_update_portfolio_value_logs_debug_when_all_snapshot_data_stale(self):
        """Test that stale snapshot data triggers debug log and uses close price."""
        strategy, position, option_asset, source = self._setup_strategy_with_option_position()
        now = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2025, 4, 7, 10, 30))
        strategy.broker.data_source.get_datetime = MagicMock(return_value=now)
        stale_dt = now - timedelta(minutes=10)
        source.snapshot = {
            "open": 60.0,
            "high": 66.0,
            "low": 58.0,
            "close": 65.0,
            "bid": 70.0,
            "ask": 74.0,
            "last_trade_time": stale_dt,
            "last_bid_time": stale_dt,
            "last_ask_time": stale_dt,
        }
        starting_cash = strategy.cash

        with patch.object(strategy.logger, "debug") as debug_mock:
            value = strategy._update_portfolio_value()

        assert value == pytest.approx(starting_cash + position.quantity * option_asset.multiplier * 65.0)
        # Debug is called for stale data - expected behavior in backtesting
        debug_mock.assert_called_once()
        assert source.last_price_calls == 0

    @patch('uuid.uuid4')
    def test_register_cron_callback_adds_job_to_scheduler(self, mock_uuid4):
        """
        Test that register_cron_callback adds the job to the scheduler with the correct parameters
        """
        # Mock uuid4 to return a predictable value
        mock_uuid = MagicMock()
        mock_uuid.hex = "test-uuid"
        mock_uuid4.return_value = mock_uuid

        date_start = datetime(2021, 7, 10)
        date_end = datetime(2021, 7, 13)
        data_source = YahooDataBacktesting(date_start, date_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = BuyAndHold(
            backtesting_broker,
            backtesting_start=date_start,
            backtesting_end=date_end,
        )

        # Set is_backtesting to False for this test
        strategy.is_backtesting = False

        # Mock the scheduler's add_job method
        strategy._executor.scheduler.add_job = MagicMock(return_value=None)

        # Define a callback function
        def test_callback():
            pass

        # Register the callback
        strategy.register_cron_callback("0 9 * * 1-5", test_callback)

        # Check that add_job was called with the correct parameters
        strategy._executor.scheduler.add_job.assert_called_once()
        args, kwargs = strategy._executor.scheduler.add_job.call_args

        assert args[0] == test_callback
        assert isinstance(args[1], CronTrigger)
        assert kwargs['id'] == "cron_callback_test-uuid"
        assert kwargs['name'] == "Cron Callback: test_callback"
        assert kwargs['jobstore'] == "default"

    @patch('uuid.uuid4')
    def test_register_cron_callback_uses_broker_timezone(self, mock_uuid4):
        """
        Test that register_cron_callback uses the broker's timezone when creating the CronTrigger
        """
        # Mock uuid4 to return a predictable value
        mock_uuid = MagicMock()
        mock_uuid.hex = "test-uuid"
        mock_uuid4.return_value = mock_uuid

        date_start = datetime(2021, 7, 10)
        date_end = datetime(2021, 7, 13)
        data_source = YahooDataBacktesting(date_start, date_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = BuyAndHold(
            backtesting_broker,
            backtesting_start=date_start,
            backtesting_end=date_end,
        )

        # Set is_backtesting to False for this test
        strategy.is_backtesting = False

        # Mock the CronTrigger.from_crontab method
        with patch('apscheduler.triggers.cron.CronTrigger.from_crontab') as mock_from_crontab:
            mock_trigger = MagicMock()
            mock_from_crontab.return_value = mock_trigger

            # Mock the scheduler's add_job method
            strategy._executor.scheduler.add_job = MagicMock(return_value=None)

            # Define a callback function
            def test_callback():
                pass

            # Register the callback
            strategy.register_cron_callback("0 9 * * 1-5", test_callback)

            # Check that from_crontab was called with the broker's timezone
            mock_from_crontab.assert_called_once_with("0 9 * * 1-5", timezone=strategy.pytz)

    @patch('uuid.uuid4')
    def test_register_cron_callback_does_nothing_in_backtesting(self, mock_uuid4):
        """
        Test that register_cron_callback does nothing in backtesting mode
        """
        # Mock uuid4 to return a predictable value
        mock_uuid = MagicMock()
        mock_uuid.hex = "test-uuid"
        mock_uuid4.return_value = mock_uuid

        date_start = datetime(2021, 7, 10)
        date_end = datetime(2021, 7, 13)
        data_source = YahooDataBacktesting(date_start, date_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = BuyAndHold(
            backtesting_broker,
            backtesting_start=date_start,
            backtesting_end=date_end,
        )

        # Ensure is_backtesting is True
        assert strategy.is_backtesting == True

        # Mock the scheduler's add_job method
        strategy._executor.scheduler.add_job = MagicMock(return_value=None)

        # Mock the log_message method to verify it's called
        strategy.log_message = MagicMock()

        # Define a callback function
        def test_callback():
            pass

        # Register the callback
        job_id = strategy.register_cron_callback("0 9 * * 1-5", test_callback)

        # Check that the job ID is correct
        assert job_id == "cron_callback_test-uuid"

        # Check that add_job was not called
        strategy._executor.scheduler.add_job.assert_not_called()

        # Check that log_message was called with the expected message
        strategy.log_message.assert_called_once_with(
            f"Skipping registration of cron callback test_callback in backtesting mode"
        )


def test_timezone_defaults_when_tzinfo_missing():
    """Ensure Strategy.timezone and Strategy.pytz default when data source tzinfo is missing."""
    date_start = datetime(2021, 7, 10)
    date_end = datetime(2021, 7, 13)

    # Create a data source and explicitly remove tzinfo
    data_source = YahooDataBacktesting(date_start, date_end)
    # Simulate a broker/data source that does not provide tzinfo
    data_source.tzinfo = None

    backtesting_broker = BacktestingBroker(data_source)
    strategy = BuyAndHold(
        backtesting_broker,
        backtesting_start=date_start,
        backtesting_end=date_end,
    )

    # timezone should fall back to the documented default string
    assert strategy.timezone == LUMIBOT_DEFAULT_TIMEZONE

    # pytz should fall back to the default tzinfo (pytz timezone)
    # Compare by canonical name to avoid object identity assumptions
    assert getattr(strategy.pytz, "zone", None) == LUMIBOT_DEFAULT_TIMEZONE
