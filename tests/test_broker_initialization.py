"""
Simple test cases for broker initialization error handling.
"""
import json
import os
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pandas as pd
import pytest
from unittest.mock import patch, MagicMock


class _CalendarTestBroker:
    @staticmethod
    def create():
        from lumibot.brokers.broker import Broker

        class TestBroker(Broker):
            IS_BACKTESTING_BROKER = True

            def cancel_order(self, order): pass
            def _modify_order(self, order, limit_price=None, stop_price=None): pass
            def _submit_order(self, order): return order
            def _get_balances_at_broker(self, quote_asset, strategy): return (0, 0, 0)
            def get_historical_account_value(self): return {}
            def _get_stream_object(self): return None
            def _register_stream_events(self): pass
            def _run_stream(self): pass
            def _pull_positions(self, strategy): return []
            def _pull_position(self, strategy, asset): return None
            def _parse_broker_order(self, response, strategy_name, strategy_object=None): return response
            def _pull_broker_order(self, identifier): return None
            def _pull_broker_all_orders(self): return []

        return TestBroker(name="test", connect_stream=False, data_source=object())


class TestBrokerInitializationSimple:
    """Test cases for broker initialization and error handling."""
    
    def test_strategy_with_none_broker_raises_helpful_error(self):
        """
        Test that when broker is None, a helpful error message is provided
        that explains how to set up environment variables.
        """
        # Mock both the credentials imports in the strategy module
        from lumibot.strategies import Strategy

        with patch('lumibot.strategies._strategy.BROKER', None):
            with patch('lumibot.credentials.IS_BACKTESTING', False):
                # Create a minimal strategy class for testing
                class TestStrategy(Strategy):
                    def on_trading_iteration(self):
                        pass
                
                # Attempt to initialize the strategy with None broker
                with pytest.raises(ValueError) as exc_info:
                    TestStrategy(broker=None)
                
                # Check that the error message is helpful and contains key information
                error_message = str(exc_info.value)
                
                # Verify the error message contains helpful guidance
                assert "No broker is set" in error_message
                assert "IS_BACKTESTING" in error_message
                assert ".env file" in error_message
                assert "ALPACA_API_KEY" in error_message
                assert "lumibot.lumiwealth.com" in error_message
                assert "backtesting" in error_message.lower()
                assert "live trading" in error_message.lower()
    
    def test_strategy_with_valid_broker_does_not_raise_broker_error(self):
        """
        Test that when a valid broker is provided, the broker None error is not raised.
        """
        # Create a mock broker with required attributes
        mock_broker = MagicMock()
        mock_broker.name = "test_broker"
        mock_broker.quote_assets = set()
        mock_broker.IS_BACKTESTING_BROKER = True  # Set to True to avoid broker balance updates
        mock_broker.data_source = MagicMock()
        mock_broker.data_source.datetime_start = None
        mock_broker.data_source.datetime_end = None

        from lumibot.strategies import Strategy
        
        # Create a minimal strategy class for testing
        class TestStrategy(Strategy):
            def on_trading_iteration(self):
                pass
        
        # This should not raise the broker None error
        # (though it might raise other errors, we're only testing the broker None case)
        try:
            strategy = TestStrategy(broker=mock_broker)
            # If we get here, the broker None error was not raised
            assert strategy.broker == mock_broker
        except ValueError as e:
            # If a ValueError is raised, it should NOT be the broker None error
            error_message = str(e)
            assert "No broker is set" not in error_message, f"Unexpected broker None error: {error_message}"
            # If it's a different ValueError, we can let it pass for this test
        except Exception as e:
            # Other exceptions are acceptable for this test since we're only testing the broker None case
            pass

    def test_is_market_open_uses_initialized_calendar(self):
        import pandas as pd
        from lumibot.brokers.broker import Broker

        class TestBroker(Broker):
            IS_BACKTESTING_BROKER = True

            def cancel_order(self, order): pass
            def _modify_order(self, order, limit_price=None, stop_price=None): pass
            def _submit_order(self, order): return order
            def _get_balances_at_broker(self, quote_asset, strategy): return (0, 0, 0)
            def get_historical_account_value(self): return {}
            def _get_stream_object(self): return None
            def _register_stream_events(self): pass
            def _run_stream(self): pass
            def _pull_positions(self, strategy): return []
            def _pull_position(self, strategy, asset): return None
            def _parse_broker_order(self, response, strategy_name, strategy_object=None): return response
            def _pull_broker_order(self, identifier): return None
            def _pull_broker_all_orders(self): return []

        broker = TestBroker(name="test", connect_stream=False, data_source=object())
        broker.initialize_market_calendars(
            pd.DataFrame(
                {
                    "market_open": [datetime(2026, 5, 11, 13, 30, tzinfo=timezone.utc)],
                    "market_close": [datetime(2026, 5, 11, 20, 0, tzinfo=timezone.utc)],
                }
            )
        )

        assert broker._is_market_open_from_initialized_calendar(
            datetime(2026, 5, 11, 14, 0, tzinfo=timezone.utc)
        ) is True
        assert broker._is_market_open_from_initialized_calendar(
            datetime(2026, 5, 11, 21, 0, tzinfo=timezone.utc)
        ) is False

    def test_initialized_calendar_returns_none_when_current_date_not_covered(self):
        broker = _CalendarTestBroker.create()
        broker.initialize_market_calendars(
            pd.DataFrame(
                {
                    "market_open": [datetime(2026, 7, 7, 13, 30, tzinfo=timezone.utc)],
                    "market_close": [datetime(2026, 7, 7, 20, 0, tzinfo=timezone.utc)],
                }
            )
        )

        assert broker._is_market_open_from_initialized_calendar(
            datetime(2026, 7, 8, 14, 7, tzinfo=timezone.utc)
        ) is None

    def test_initialized_calendar_returns_none_when_current_date_before_calendar_window(self):
        broker = _CalendarTestBroker.create()
        broker.initialize_market_calendars(
            pd.DataFrame(
                {
                    "market_open": [datetime(2026, 7, 9, 13, 30, tzinfo=timezone.utc)],
                    "market_close": [datetime(2026, 7, 9, 20, 0, tzinfo=timezone.utc)],
                }
            )
        )

        assert broker._is_market_open_from_initialized_calendar(
            datetime(2026, 7, 8, 14, 7, tzinfo=timezone.utc)
        ) is None

    def test_initialized_calendar_returns_false_for_covered_closed_session(self):
        broker = _CalendarTestBroker.create()
        broker.initialize_market_calendars(
            pd.DataFrame(
                {
                    "market_open": [datetime(2026, 7, 8, 13, 30, tzinfo=timezone.utc)],
                    "market_close": [datetime(2026, 7, 8, 20, 0, tzinfo=timezone.utc)],
                }
            )
        )

        assert broker._is_market_open_from_initialized_calendar(
            datetime(2026, 7, 8, 12, 0, tzinfo=timezone.utc)
        ) is False

    def test_initialized_calendar_returns_false_for_weekend_inside_calendar_window(self):
        broker = _CalendarTestBroker.create()
        broker.initialize_market_calendars(
            pd.DataFrame(
                {
                    "market_open": [
                        datetime(2026, 7, 10, 13, 30, tzinfo=timezone.utc),
                        datetime(2026, 7, 13, 13, 30, tzinfo=timezone.utc),
                    ],
                    "market_close": [
                        datetime(2026, 7, 10, 20, 0, tzinfo=timezone.utc),
                        datetime(2026, 7, 13, 20, 0, tzinfo=timezone.utc),
                    ],
                }
            )
        )

        assert broker._is_market_open_from_initialized_calendar(
            datetime(2026, 7, 11, 14, 0, tzinfo=timezone.utc)
        ) is False

    def test_initialized_calendar_handles_overnight_sessions(self):
        broker = _CalendarTestBroker.create()
        broker.initialize_market_calendars(
            pd.DataFrame(
                {
                    "market_open": [datetime(2026, 7, 8, 22, 0, tzinfo=timezone.utc)],
                    "market_close": [datetime(2026, 7, 9, 21, 0, tzinfo=timezone.utc)],
                }
            )
        )

        assert broker._is_market_open_from_initialized_calendar(
            datetime(2026, 7, 9, 2, 0, tzinfo=timezone.utc)
        ) is True
        assert broker._is_market_open_from_initialized_calendar(
            datetime(2026, 7, 9, 21, 30, tzinfo=timezone.utc)
        ) is False

    def test_initialized_calendar_applies_extended_trading_minutes(self):
        broker = _CalendarTestBroker.create()
        broker.extended_trading_minutes = 15
        broker.initialize_market_calendars(
            pd.DataFrame(
                {
                    "market_open": [datetime(2026, 7, 8, 13, 30, tzinfo=timezone.utc)],
                    "market_close": [datetime(2026, 7, 8, 20, 0, tzinfo=timezone.utc)],
                }
            )
        )

        assert broker._is_market_open_from_initialized_calendar(
            datetime(2026, 7, 8, 20, 10, tzinfo=timezone.utc)
        ) is True

    def test_is_market_open_falls_back_when_initialized_calendar_is_stale(self, mocker):
        broker = _CalendarTestBroker.create()
        broker.market = "24/5"
        broker.initialize_market_calendars(
            pd.DataFrame(
                {
                    "market_open": [datetime(2026, 7, 7, 13, 30, tzinfo=timezone.utc)],
                    "market_close": [datetime(2026, 7, 7, 20, 0, tzinfo=timezone.utc)],
                }
            )
        )
        mocker.patch.object(broker, "_is_continuous_market", return_value=True)

        assert broker.is_market_open() is True

    def test_is_market_open_does_not_fall_back_for_weekend_inside_calendar_window(self, mocker):
        broker = _CalendarTestBroker.create()
        broker.market = "24/5"
        broker.initialize_market_calendars(
            pd.DataFrame(
                {
                    "market_open": [
                        datetime(2026, 7, 10, 0, 0, tzinfo=timezone.utc),
                        datetime(2026, 7, 13, 0, 0, tzinfo=timezone.utc),
                    ],
                    "market_close": [
                        datetime(2026, 7, 10, 21, 0, tzinfo=timezone.utc),
                        datetime(2026, 7, 13, 21, 0, tzinfo=timezone.utc),
                    ],
                }
            )
        )
        mocker.patch.object(broker, "_is_continuous_market", side_effect=AssertionError("should not fall back"))

        assert broker._is_market_open_from_initialized_calendar(
            datetime(2026, 7, 11, 14, 0, tzinfo=timezone.utc)
        ) is False

    def test_base_broker_continuous_market_detection_respects_weekend_gaps(self):
        broker = _CalendarTestBroker.create()

        assert broker._is_continuous_market("24/7") is True
        assert broker._is_continuous_market("24/5") is False
        assert broker._is_continuous_market("us_futures") is False

    @pytest.mark.parametrize(
        ("market", "open_dt", "other_dt", "expected_other_open"),
        [
            (
                "NASDAQ",
                datetime(2026, 7, 8, 14, 7, 54, tzinfo=timezone.utc),
                datetime(2026, 7, 11, 14, 0, tzinfo=timezone.utc),
                False,
            ),
            (
                "NYSE",
                datetime(2026, 7, 8, 14, 7, 54, tzinfo=timezone.utc),
                datetime(2026, 7, 11, 14, 0, tzinfo=timezone.utc),
                False,
            ),
            (
                "24/5",
                datetime(2026, 7, 8, 14, 7, 54, tzinfo=timezone.utc),
                datetime(2026, 7, 11, 14, 0, tzinfo=timezone.utc),
                False,
            ),
            (
                "us_futures",
                datetime(2026, 7, 8, 14, 7, 54, tzinfo=timezone.utc),
                datetime(2026, 7, 11, 14, 0, tzinfo=timezone.utc),
                False,
            ),
            (
                "24/7",
                datetime(2026, 7, 8, 14, 7, 54, tzinfo=timezone.utc),
                datetime(2026, 7, 11, 14, 0, tzinfo=timezone.utc),
                True,
            ),
        ],
    )
    def test_real_market_calendars_cover_open_and_other_times(
        self,
        market,
        open_dt,
        other_dt,
        expected_other_open,
    ):
        from lumibot.tools import get_trading_days

        broker = _CalendarTestBroker.create()
        broker.initialize_market_calendars(
            get_trading_days(
                market=market,
                start_date=open_dt - timedelta(days=14),
                end_date=open_dt + timedelta(days=15),
            )
        )

        assert broker._is_market_open_from_initialized_calendar(open_dt) is True
        assert broker._is_market_open_from_initialized_calendar(other_dt) is expected_other_open

    def test_us_futures_real_calendar_weekend_closed_and_monday_night_open(self):
        from lumibot.tools import get_trading_days

        broker = _CalendarTestBroker.create()
        broker.initialize_market_calendars(
            get_trading_days(
                market="us_futures",
                start_date=datetime(2026, 7, 1, tzinfo=timezone.utc),
                end_date=datetime(2026, 7, 22, tzinfo=timezone.utc),
            )
        )

        # Saturday night in New York is still the CME weekend gap.
        assert broker._is_market_open_from_initialized_calendar(
            datetime(2026, 7, 12, 2, 0, tzinfo=timezone.utc)
        ) is False
        # Monday night in New York is inside the regular Globex session.
        assert broker._is_market_open_from_initialized_calendar(
            datetime(2026, 7, 14, 2, 0, tzinfo=timezone.utc)
        ) is True

    def test_24_5_real_calendar_weekend_closed_and_weeknight_open(self):
        from lumibot.tools import get_trading_days

        broker = _CalendarTestBroker.create()
        broker.initialize_market_calendars(
            get_trading_days(
                market="24/5",
                start_date=datetime(2026, 7, 1, tzinfo=timezone.utc),
                end_date=datetime(2026, 7, 22, tzinfo=timezone.utc),
            )
        )

        assert broker._is_market_open_from_initialized_calendar(
            datetime(2026, 7, 12, 2, 0, tzinfo=timezone.utc)
        ) is False
        assert broker._is_market_open_from_initialized_calendar(
            datetime(2026, 7, 14, 2, 0, tzinfo=timezone.utc)
        ) is True

    @pytest.mark.parametrize("market", ["NASDAQ", "NYSE"])
    def test_equity_real_calendar_market_hours_weekend_and_holiday(self, market):
        from lumibot.tools import get_trading_days

        broker = _CalendarTestBroker.create()
        broker.initialize_market_calendars(
            get_trading_days(
                market=market,
                start_date=datetime(2026, 7, 1, tzinfo=timezone.utc),
                end_date=datetime(2026, 7, 22, tzinfo=timezone.utc),
            )
        )

        assert broker._is_market_open_from_initialized_calendar(
            datetime(2026, 7, 8, 14, 7, 54, tzinfo=timezone.utc)
        ) is True
        assert broker._is_market_open_from_initialized_calendar(
            datetime(2026, 7, 8, 12, 0, tzinfo=timezone.utc)
        ) is False
        assert broker._is_market_open_from_initialized_calendar(
            datetime(2026, 7, 8, 21, 0, tzinfo=timezone.utc)
        ) is False
        assert broker._is_market_open_from_initialized_calendar(
            datetime(2026, 7, 11, 14, 0, tzinfo=timezone.utc)
        ) is False
        # Independence Day is observed on Friday July 3 in 2026.
        assert broker._is_market_open_from_initialized_calendar(
            datetime(2026, 7, 3, 14, 0, tzinfo=timezone.utc)
        ) is False

    @pytest.mark.parametrize(
        "timestamp",
        [
            datetime(2026, 1, 1, 14, 0, tzinfo=timezone.utc),
            datetime(2026, 7, 4, 14, 0, tzinfo=timezone.utc),
            datetime(2026, 7, 11, 14, 0, tzinfo=timezone.utc),
            datetime(2026, 12, 25, 14, 0, tzinfo=timezone.utc),
        ],
    )
    def test_24_7_real_calendar_ignores_weekends_and_holidays(self, timestamp):
        from lumibot.tools import get_trading_days

        broker = _CalendarTestBroker.create()
        broker.initialize_market_calendars(
            get_trading_days(
                market="24/7",
                start_date=timestamp - timedelta(days=14),
                end_date=timestamp + timedelta(days=15),
            )
        )

        assert broker._is_market_open_from_initialized_calendar(timestamp) is True

    def test_utc_to_local_converts_aware_datetime_before_localizing(self):
        from dateutil import tz
        from lumibot.brokers.broker import Broker

        class TestBroker(Broker):
            def cancel_order(self, order): pass
            def _modify_order(self, order, limit_price=None, stop_price=None): pass
            def _submit_order(self, order): return order
            def _get_balances_at_broker(self, quote_asset, strategy): return (0, 0, 0)
            def get_historical_account_value(self): return {}
            def _get_stream_object(self): return None
            def _register_stream_events(self): pass
            def _run_stream(self): pass
            def _pull_positions(self, strategy): return []
            def _pull_position(self, strategy, asset): return None
            def _parse_broker_order(self, response, strategy_name, strategy_object=None): return response
            def _pull_broker_order(self, identifier): return None
            def _pull_broker_all_orders(self): return []

        broker = TestBroker.__new__(TestBroker)
        source = datetime(2026, 5, 11, 16, 30, tzinfo=timezone(timedelta(hours=3)))

        converted = broker.utc_to_local(source)

        expected = datetime(2026, 5, 11, 13, 30, tzinfo=timezone.utc).astimezone(tz.tzlocal())
        assert converted == expected


def test_ibkr_rest_submit_order_without_stream_does_not_crash(monkeypatch):
    from lumibot.brokers import broker as broker_module
    from lumibot.brokers.interactive_brokers_rest import InteractiveBrokersREST
    from lumibot.entities import Asset, Order

    monkeypatch.setattr(broker_module.Broker, "_start_orders_thread", lambda self: None)
    data_source = SimpleNamespace(execute_order=lambda order_data: [{"order_id": "ib-1"}])
    broker = InteractiveBrokersREST(config={"MARKET": "NYSE"}, data_source=data_source, connect_stream=False)
    broker.get_order_data_from_orders = lambda orders: {"orders": []}
    broker._log_order_status = lambda *args, **kwargs: None
    order = Order("unit-test", Asset("AAPL"), 1, Order.OrderSide.BUY)

    submitted = broker._submit_order(order)

    assert submitted is order
    assert order.identifier == "ib-1"
    assert order.status == Order.OrderStatus.SUBMITTED


def test_ibkr_rest_pull_broker_order_reads_client_portal_mapping():
    from lumibot.brokers.interactive_brokers_rest import InteractiveBrokersREST

    raw_order = {"orderId": 1234567890, "status": "Submitted"}
    broker = InteractiveBrokersREST.__new__(InteractiveBrokersREST)
    broker.data_source = SimpleNamespace(get_broker_all_orders=lambda: [raw_order])

    # Client Portal JSON uses a numeric orderId while strategy state can hold a string.
    assert broker._pull_broker_order("1234567890") is raw_order


def test_ibkr_rest_pull_broker_order_returns_none_when_missing():
    from lumibot.brokers.interactive_brokers_rest import InteractiveBrokersREST

    broker = InteractiveBrokersREST.__new__(InteractiveBrokersREST)
    broker.data_source = SimpleNamespace(
        get_broker_all_orders=lambda: [{"orderId": 111, "status": "Submitted"}]
    )

    # Missing broker data must not fabricate a truthy placeholder order.
    assert broker._pull_broker_order("222") is None


def test_interactive_brokers_keeps_required_orders_thread_enabled(monkeypatch):
    from lumibot.brokers import broker as broker_module
    from lumibot.brokers.interactive_brokers import InteractiveBrokers

    started = []
    monkeypatch.setattr(broker_module.Broker, "_start_orders_thread", lambda self: started.append(self.name))
    monkeypatch.setattr(InteractiveBrokers, "start_ib", lambda self: None)

    InteractiveBrokers(
        config={"IP": "127.0.0.1", "SOCKET_PORT": 4002, "CLIENT_ID": 1},
        data_source=object(),
        connect_stream=False,
        start_orders_thread=False,
    )

    assert started == ["interactive_brokers"]


def test_schwab_data_can_skip_constructor_client_creation(monkeypatch):
    from lumibot.data_sources.schwab_data import SchwabData

    def _create_client(*args, **kwargs):
        pytest.fail("Broker-owned SchwabData must not create its own Schwab client")

    monkeypatch.setattr(SchwabData, "create_schwab_client", staticmethod(_create_client))

    data_source = SchwabData(auto_create_client=False)

    assert data_source.client is None


def test_schwab_base_init_does_not_launch_stream_before_client_setup(monkeypatch, tmp_path):
    from lumibot.brokers import broker as broker_module
    from lumibot.brokers import schwab as schwab_module
    import requests_oauthlib

    token_path = tmp_path / "schwab_token.json"
    token_path.write_text(
        json.dumps(
            {
                "creation_timestamp": 1,
                "token": {
                    "access_token": "access",
                    "refresh_token": "refresh",
                    "issued_at": int(time.time() * 1000),
                    "expires_in": 1800,
                    "refresh_token_issued_at": int(time.time() * 1000),
                    "refresh_token_expires_in": 7776000,
                    "token_type": "Bearer",
                    "scope": "api",
                },
            }
        ),
        encoding="utf-8",
    )

    class _OAuth2Session:
        def __init__(self, *, client_id, token, **kwargs):
            self.client_id = client_id
            self.token = token

        def register_compliance_hook(self, hook_type, hook):
            return None

    class _AccountResponse:
        status_code = 200

        def json(self):
            return [{"accountNumber": "12345678", "hashValue": "hash-123"}]

    class _Client:
        def __init__(self, *, api_key, session, token_metadata=None):
            self.api_key = api_key
            self.session = session

        def get_account_numbers(self):
            return _AccountResponse()

    def _fail_stream(*args, **kwargs):
        pytest.fail("Schwab base Broker init must not launch stream before client setup")

    monkeypatch.setenv("LUMIBOT_DISABLE_DOTENV", "1")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "secret")
    monkeypatch.setattr(requests_oauthlib, "OAuth2Session", _OAuth2Session)
    monkeypatch.setattr(schwab_module, "Client", _Client)
    monkeypatch.setattr(broker_module.Broker, "_start_orders_thread", lambda self: None)
    monkeypatch.setattr(broker_module.Broker, "_launch_stream", _fail_stream)
    monkeypatch.setattr(schwab_module.Schwab, "_get_stream_object", _fail_stream)
    monkeypatch.setattr(schwab_module.Schwab, "_finish_initialization", lambda self, *args, **kwargs: None)

    schwab_module.Schwab(
        config={
            "SCHWAB_ACCOUNT_NUMBER": "5678",
            "SCHWAB_APP_KEY": "app-key",
            "SCHWAB_APP_SECRET": "secret",
            "SCHWAB_TOKEN_PATH": str(token_path),
        }
    )


def test_schwab_manual_client_exposes_token_metadata_for_account_activity_stream(monkeypatch, tmp_path):
    """The REST client must retain schwab-py token metadata used by stream login."""
    from lumibot.brokers import broker as broker_module
    from lumibot.brokers import schwab as schwab_module
    import requests_oauthlib

    issued_at = int(time.time() * 1000)
    token_path = tmp_path / "schwab_token.json"
    token_path.write_text(
        json.dumps(
            {
                "creation_timestamp": 123,
                "token": {
                    "access_token": "access",
                    "refresh_token": "refresh",
                    "issued_at": issued_at,
                    "expires_in": 1800,
                    "refresh_token_issued_at": issued_at,
                    "refresh_token_expires_in": 7776000,
                    "token_type": "Bearer",
                    "scope": "api",
                },
            }
        ),
        encoding="utf-8",
    )

    class _OAuth2Session:
        def __init__(self, *, client_id, token, **kwargs):
            self.client_id = client_id
            self.token = token

        def register_compliance_hook(self, hook_type, hook):
            return None

    class _AccountResponse:
        status_code = 200

        def json(self):
            return [{"accountNumber": "12345678", "hashValue": "hash-123"}]

    class _Client:
        def __init__(self, *, api_key, session, token_metadata=None):
            self.api_key = api_key
            self.session = session
            self.token_metadata = token_metadata

        def get_account_numbers(self):
            return _AccountResponse()

    monkeypatch.setenv("LUMIBOT_DISABLE_DOTENV", "1")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "secret")
    monkeypatch.setattr(requests_oauthlib, "OAuth2Session", _OAuth2Session)
    monkeypatch.setattr(schwab_module, "Client", _Client)
    monkeypatch.setattr(broker_module.Broker, "_start_orders_thread", lambda self: None)
    monkeypatch.setattr(schwab_module.Schwab, "_finish_initialization", lambda self, *args, **kwargs: None)

    broker = schwab_module.Schwab(
        config={
            "SCHWAB_ACCOUNT_NUMBER": "5678",
            "SCHWAB_APP_KEY": "app-key",
            "SCHWAB_APP_SECRET": "secret",
            "SCHWAB_TOKEN_PATH": str(token_path),
        },
        connect_stream=False,
    )

    assert broker.client.token_metadata is not None
    assert broker.client.token_metadata.creation_timestamp == 123
    assert broker.client.token_metadata.token is broker.client.session.token
    assert broker.client.token_metadata.token["access_token"] == "access"


def test_schwab_force_refresh_on_startup_rewrites_token(monkeypatch, tmp_path):
    from lumibot.brokers import broker as broker_module
    from lumibot.brokers import schwab as schwab_module
    import requests_oauthlib

    old_issued_at = 1_700_000_000_000
    refreshed_now = 1_782_800_000.123
    token_path = tmp_path / "schwab_token.json"
    token_path.write_text(
        json.dumps(
            {
                "creation_timestamp": 1,
                "token": {
                    "access_token": "old-access",
                    "refresh_token": "old-refresh",
                    "issued_at": old_issued_at,
                    "expires_in": 1800,
                    "refresh_token_issued_at": old_issued_at,
                    "refresh_token_expires_in": 7776000,
                    "token_type": "Bearer",
                    "scope": "api",
                },
            }
        ),
        encoding="utf-8",
    )

    class _OAuth2Session:
        instances = []

        def __init__(
            self,
            *,
            client_id,
            token,
            auto_refresh_url,
            auto_refresh_kwargs,
            token_updater,
        ):
            self.client_id = client_id
            self.token = token
            self.auto_refresh_url = auto_refresh_url
            self.auto_refresh_kwargs = auto_refresh_kwargs
            self.token_updater = token_updater
            self.refresh_calls = []
            self.hooks = []
            self.instances.append(self)

        def register_compliance_hook(self, hook_type, hook):
            self.hooks.append((hook_type, hook))

        def refresh_token(self, token_url, *, refresh_token, **kwargs):
            self.refresh_calls.append(
                {
                    "token_url": token_url,
                    "refresh_token": refresh_token,
                    "kwargs": kwargs,
                }
            )
            return {
                "access_token": "new-access",
                "refresh_token": "new-refresh",
                "expires_in": 1800,
            }

    class _AccountResponse:
        status_code = 200

        def json(self):
            return [{"accountNumber": "12345678", "hashValue": "hash-123"}]

    class _Client:
        def __init__(self, *, api_key, session, token_metadata=None):
            self.api_key = api_key
            self.session = session

        def get_account_numbers(self):
            return _AccountResponse()

    monkeypatch.setenv("BOTSPOT_FORCE_BROKER_TOKEN_REFRESH", "true")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "secret")
    monkeypatch.setattr(requests_oauthlib, "OAuth2Session", _OAuth2Session)
    monkeypatch.setattr(schwab_module, "Client", _Client)
    monkeypatch.setattr(broker_module.Broker, "_start_orders_thread", lambda self: None)
    monkeypatch.setattr(schwab_module.Schwab, "_finish_initialization", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(schwab_module.Schwab, "_get_stream_object", lambda self: None)
    monkeypatch.setattr(schwab_module.time, "time", lambda: refreshed_now)

    broker = schwab_module.Schwab(
        config={
            "SCHWAB_ACCOUNT_NUMBER": "5678",
            "SCHWAB_APP_KEY": "app-key",
            "SCHWAB_APP_SECRET": "secret",
            "SCHWAB_TOKEN_PATH": str(token_path),
        }
    )

    assert broker.client.session.refresh_calls == [
        {
            "token_url": "https://api.schwabapi.com/v1/oauth/token",
            "refresh_token": "old-refresh",
            "kwargs": {"client_id": "app-key", "client_secret": "secret"},
        }
    ]
    rewritten = json.loads(token_path.read_text(encoding="utf-8"))
    assert rewritten["creation_timestamp"] == 1
    assert rewritten["token"]["access_token"] == "new-access"
    assert rewritten["token"]["refresh_token"] == "new-refresh"
    assert rewritten["token"]["issued_at"] == int(refreshed_now * 1000)


def test_schwab_force_refresh_fails_if_token_file_cannot_be_rewritten(monkeypatch, tmp_path):
    from lumibot.brokers import broker as broker_module
    from lumibot.brokers import schwab as schwab_module
    import requests_oauthlib

    token_path = tmp_path / "schwab_token.json"
    token_path.write_text(
        json.dumps(
            {
                "creation_timestamp": 1,
                "token": {
                    "access_token": "old-access",
                    "refresh_token": "old-refresh",
                    "issued_at": int(time.time() * 1000),
                    "expires_in": 1800,
                    "refresh_token_issued_at": int(time.time() * 1000),
                    "refresh_token_expires_in": 7776000,
                    "token_type": "Bearer",
                    "scope": "api",
                },
            }
        ),
        encoding="utf-8",
    )

    class _OAuth2Session:
        def __init__(
            self,
            *,
            client_id,
            token,
            auto_refresh_url,
            auto_refresh_kwargs,
            token_updater,
        ):
            self.token_updater = token_updater
            self.auto_refresh_url = auto_refresh_url

        def register_compliance_hook(self, hook_type, hook):
            return None

        def refresh_token(self, token_url, *, refresh_token, **kwargs):
            return {
                "access_token": "new-access",
                "refresh_token": "new-refresh",
                "expires_in": 1800,
                "issued_at": int(time.time() * 1000),
            }

    monkeypatch.setenv("BOTSPOT_FORCE_BROKER_TOKEN_REFRESH", "true")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "secret")
    monkeypatch.setattr(requests_oauthlib, "OAuth2Session", _OAuth2Session)
    monkeypatch.setattr(broker_module.Broker, "_start_orders_thread", lambda self: None)
    monkeypatch.setattr(schwab_module.Schwab, "_finish_initialization", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(schwab_module.Schwab, "_get_stream_object", lambda self: None)
    monkeypatch.setattr(
        schwab_module.os,
        "replace",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("disk full")),
    )

    with pytest.raises(ConnectionError, match="Failed to initialize Schwab client"):
        schwab_module.Schwab(
            config={
                "SCHWAB_ACCOUNT_NUMBER": "5678",
                "SCHWAB_APP_KEY": "app-key",
                "SCHWAB_APP_SECRET": "secret",
                "SCHWAB_TOKEN_PATH": str(token_path),
            }
        )

    preserved = json.loads(token_path.read_text(encoding="utf-8"))
    assert preserved["token"]["access_token"] == "old-access"
    assert preserved["token"]["refresh_token"] == "old-refresh"


def test_schwab_external_oauth_refresh_mode_skips_forced_refresh_and_uses_external_file(monkeypatch, tmp_path):
    from lumibot.brokers import broker as broker_module
    from lumibot.brokers import schwab as schwab_module
    import requests_oauthlib

    token_path = tmp_path / "schwab_token.json"
    token_path.write_text(
        json.dumps(
            {
                "creation_timestamp": 1,
                "token": {
                    "access_token": "old-access",
                    "issued_at": int(time.time() * 1000),
                    "expires_in": 1800,
                    "token_type": "Bearer",
                    "scope": "api",
                },
            }
        ),
        encoding="utf-8",
    )

    class _OAuth2Session:
        def __init__(self, *, client_id, token, **kwargs):
            self.client_id = client_id
            self.token = token
            self.kwargs = kwargs
            self.request_calls = 0

        def register_compliance_hook(self, hook_type, hook):
            pytest.fail("External OAuth refresh mode must not install provider refresh hooks")

        def refresh_token(self, *args, **kwargs):
            pytest.fail("External OAuth refresh mode must not call Schwab refresh_token")

        def request(self, *args, **kwargs):
            self.request_calls += 1
            return type("Response", (), {"status_code": 200})()

    class _AccountResponse:
        status_code = 200

        def json(self):
            return [{"accountNumber": "12345678", "hashValue": "hash-123"}]

    class _Client:
        def __init__(self, *, api_key, session, token_metadata=None):
            self.api_key = api_key
            self.session = session

        def get_account_numbers(self):
            return _AccountResponse()

    monkeypatch.setenv("BOTSPOT_FORCE_BROKER_TOKEN_REFRESH", "true")
    monkeypatch.setenv("LUMIBOT_OAUTH_REFRESH_MODE", "external")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "secret")
    monkeypatch.setattr(requests_oauthlib, "OAuth2Session", _OAuth2Session)
    monkeypatch.setattr(schwab_module, "Client", _Client)
    monkeypatch.setattr(broker_module.Broker, "_start_orders_thread", lambda self: None)
    monkeypatch.setattr(schwab_module.Schwab, "_finish_initialization", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(schwab_module.Schwab, "_get_stream_object", lambda self: None)

    broker = schwab_module.Schwab(
        config={
            "SCHWAB_ACCOUNT_NUMBER": "5678",
            "SCHWAB_APP_KEY": "app-key",
            "SCHWAB_APP_SECRET": "secret",
            "SCHWAB_TOKEN_PATH": str(token_path),
        }
    )

    assert broker.client.session.kwargs == {}
    assert broker.client.session.token["access_token"] == "old-access"
    assert "refresh_token" not in broker.client.session.token
    rewritten = json.loads(token_path.read_text(encoding="utf-8"))
    assert rewritten["token"]["access_token"] == "old-access"
    assert "refresh_token" not in rewritten["token"]


def test_schwab_external_oauth_refresh_mode_reloads_access_only_file_without_refresh_token(monkeypatch, tmp_path):
    from lumibot.brokers import broker as broker_module
    from lumibot.brokers import schwab as schwab_module
    import requests_oauthlib

    token_path = tmp_path / "schwab_token.json"
    token_path.write_text(
        json.dumps(
            {
                "creation_timestamp": 1,
                "token": {
                    "access_token": "old-access",
                    "refresh_token": "stale-refresh-must-not-survive",
                    "issued_at": int(time.time() * 1000),
                    "expires_in": 1800,
                    "refresh_token_issued_at": int(time.time() * 1000),
                    "refresh_token_expires_in": 7776000,
                    "token_type": "Bearer",
                    "scope": "api",
                },
            }
        ),
        encoding="utf-8",
    )

    class TokenExpiredError(Exception):
        pass

    class _OAuth2Session:
        def __init__(self, *, client_id, token, **kwargs):
            self.client_id = client_id
            self.token = token
            self.kwargs = kwargs
            self.request_calls = 0

        def register_compliance_hook(self, hook_type, hook):
            pytest.fail("External OAuth refresh mode must not install provider refresh hooks")

        def refresh_token(self, *args, **kwargs):
            pytest.fail("External OAuth refresh mode must not call Schwab refresh_token")

        def request(self, *args, **kwargs):
            self.request_calls += 1
            if self.request_calls == 1:
                token_path.write_text(
                    json.dumps(
                        {
                            "creation_timestamp": 1,
                            "token": {
                                "access_token": "new-access",
                                "issued_at": int(time.time() * 1000),
                                "expires_in": 1800,
                                "token_type": "Bearer",
                                "scope": "api",
                            },
                        }
                    ),
                    encoding="utf-8",
                )
                raise TokenExpiredError("expired access token")
            assert self.token["access_token"] == "new-access"
            assert "refresh_token" not in self.token
            return type("Response", (), {"status_code": 200})()

    class _AccountResponse:
        status_code = 200

        def json(self):
            return [{"accountNumber": "12345678", "hashValue": "hash-123"}]

    class _Client:
        def __init__(self, *, api_key, session, token_metadata=None):
            self.api_key = api_key
            self.session = session

        def get_account_numbers(self):
            return _AccountResponse()

    monkeypatch.setenv("LUMIBOT_OAUTH_REFRESH_MODE", "external")
    monkeypatch.setattr(requests_oauthlib, "OAuth2Session", _OAuth2Session)
    monkeypatch.setattr(schwab_module, "Client", _Client)
    monkeypatch.setattr(broker_module.Broker, "_start_orders_thread", lambda self: None)
    monkeypatch.setattr(schwab_module.Schwab, "_finish_initialization", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(schwab_module.Schwab, "_get_stream_object", lambda self: None)

    broker = schwab_module.Schwab(
        config={
            "SCHWAB_ACCOUNT_NUMBER": "5678",
            "SCHWAB_APP_KEY": "app-key",
            "SCHWAB_TOKEN_PATH": str(token_path),
        }
    )

    assert broker.client.session.token["access_token"] == "old-access"
    assert "refresh_token" not in broker.client.session.token
    assert broker.client.session.request("https://api.schwab.test/accounts").status_code == 200
    assert broker.client.session.request_calls == 2
    assert broker.client.session.token["access_token"] == "new-access"
    assert "refresh_token" not in broker.client.session.token


def test_schwab_external_oauth_refresh_mode_force_reapplies_fresh_file_to_stale_client(monkeypatch, tmp_path):
    from lumibot.brokers import broker as broker_module
    from lumibot.brokers import schwab as schwab_module
    import requests_oauthlib

    token_path = tmp_path / "schwab_token.json"
    issued_at = int(time.time() * 1000)
    token_path.write_text(
        json.dumps(
            {
                "creation_timestamp": 1,
                "token": {
                    "access_token": "fresh-access",
                    "issued_at": issued_at,
                    "expires_in": 1800,
                    "token_type": "Bearer",
                    "scope": "api",
                },
            }
        ),
        encoding="utf-8",
    )

    class TokenExpiredError(Exception):
        pass

    class _OAuth2Session:
        def __init__(self, *, client_id, token, **kwargs):
            self.client_id = client_id
            self.token = token
            self.kwargs = kwargs
            self.request_calls = 0
            self._client = SimpleNamespace(access_token=token.get("access_token"), expires_at=0)

        def register_compliance_hook(self, hook_type, hook):
            pytest.fail("External OAuth refresh mode must not install provider refresh hooks")

        def refresh_token(self, *args, **kwargs):
            pytest.fail("External OAuth refresh mode must not call Schwab refresh_token")

        def request(self, *args, **kwargs):
            self.request_calls += 1
            if self.request_calls == 1:
                self._client.expires_at = 0
                raise TokenExpiredError("oauthlib stale client state")
            assert self.token["access_token"] == "fresh-access"
            assert "refresh_token" not in self.token
            assert self._client.access_token == "fresh-access"
            assert self._client.expires_at > time.time()
            return type("Response", (), {"status_code": 200})()

    class _AccountResponse:
        status_code = 200

        def json(self):
            return [{"accountNumber": "12345678", "hashValue": "hash-123"}]

    class _Client:
        def __init__(self, *, api_key, session, token_metadata=None):
            self.api_key = api_key
            self.session = session

        def get_account_numbers(self):
            return _AccountResponse()

    monkeypatch.setenv("LUMIBOT_OAUTH_REFRESH_MODE", "external")
    monkeypatch.setattr(requests_oauthlib, "OAuth2Session", _OAuth2Session)
    monkeypatch.setattr(schwab_module, "Client", _Client)
    monkeypatch.setattr(broker_module.Broker, "_start_orders_thread", lambda self: None)
    monkeypatch.setattr(schwab_module.Schwab, "_finish_initialization", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(schwab_module.Schwab, "_get_stream_object", lambda self: None)

    broker = schwab_module.Schwab(
        config={
            "SCHWAB_ACCOUNT_NUMBER": "5678",
            "SCHWAB_APP_KEY": "app-key",
            "SCHWAB_TOKEN_PATH": str(token_path),
        }
    )

    assert broker.client.session.request("https://api.schwab.test/accounts").status_code == 200
    assert broker.client.session.request_calls == 2
    rewritten = json.loads(token_path.read_text(encoding="utf-8"))
    assert rewritten["token"]["access_token"] == "fresh-access"
    assert "refresh_token" not in rewritten["token"]


def test_schwab_external_oauth_refresh_mode_multiple_brokers_reload_atomic_replacements(monkeypatch, tmp_path):
    from lumibot.brokers import broker as broker_module
    from lumibot.brokers import schwab as schwab_module
    import requests_oauthlib

    token_path = tmp_path / "schwab_token.json"

    def write_token(access_token):
        payload = {
            "creation_timestamp": 1,
            "token": {
                "access_token": access_token,
                "issued_at": int(time.time() * 1000),
                "expires_in": 1800,
                "token_type": "Bearer",
                "scope": "api",
            },
        }
        tmp_file = token_path.with_name(f".{token_path.name}.{access_token}.tmp")
        tmp_file.write_text(json.dumps(payload), encoding="utf-8")
        os.replace(tmp_file, token_path)

    write_token("access-0")

    class _OAuth2Session:
        def __init__(self, *, client_id, token, **kwargs):
            self.client_id = client_id
            self.token = token
            self.kwargs = kwargs
            self.request_calls = 0

        def register_compliance_hook(self, hook_type, hook):
            pytest.fail("External OAuth refresh mode must not install provider refresh hooks")

        def refresh_token(self, *args, **kwargs):
            pytest.fail("External OAuth refresh mode must not call Schwab refresh_token")

        def request(self, *args, **kwargs):
            self.request_calls += 1
            assert "refresh_token" not in self.token
            return type("Response", (), {"status_code": 200})()

    class _AccountResponse:
        status_code = 200

        def json(self):
            return [{"accountNumber": "12345678", "hashValue": "hash-123"}]

    class _Client:
        def __init__(self, *, api_key, session, token_metadata=None):
            self.api_key = api_key
            self.session = session

        def get_account_numbers(self):
            return _AccountResponse()

    monkeypatch.setenv("LUMIBOT_OAUTH_REFRESH_MODE", "external")
    monkeypatch.setattr(requests_oauthlib, "OAuth2Session", _OAuth2Session)
    monkeypatch.setattr(schwab_module, "Client", _Client)
    monkeypatch.setattr(broker_module.Broker, "_start_orders_thread", lambda self: None)
    monkeypatch.setattr(schwab_module.Schwab, "_finish_initialization", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(schwab_module.Schwab, "_get_stream_object", lambda self: None)

    brokers = [
        schwab_module.Schwab(
            config={
                "SCHWAB_ACCOUNT_NUMBER": "5678",
                "SCHWAB_APP_KEY": "app-key",
                "SCHWAB_TOKEN_PATH": str(token_path),
            }
        )
        for _ in range(3)
    ]

    for broker in brokers:
        assert broker.client.session.token["access_token"] == "access-0"
        assert "refresh_token" not in broker.client.session.token

    for cycle in range(1, 4):
        write_token(f"access-{cycle}")
        for broker in brokers:
            assert broker.client.session.request("https://api.schwab.test/accounts").status_code == 200
            assert broker.client.session.token["access_token"] == f"access-{cycle}"
            assert "refresh_token" not in broker.client.session.token


def test_schwab_rejects_invalid_oauth_refresh_mode(monkeypatch, tmp_path):
    from lumibot.brokers import broker as broker_module
    from lumibot.brokers import schwab as schwab_module
    import requests_oauthlib

    token_path = tmp_path / "schwab_token.json"
    token_path.write_text(
        json.dumps(
            {
                "creation_timestamp": 1,
                "token": {
                    "access_token": "old-access",
                    "refresh_token": "old-refresh",
                    "issued_at": int(time.time() * 1000),
                    "expires_in": 1800,
                    "refresh_token_issued_at": int(time.time() * 1000),
                    "refresh_token_expires_in": 7776000,
                    "token_type": "Bearer",
                    "scope": "api",
                },
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("LUMIBOT_OAUTH_REFRESH_MODE", "disabled")
    monkeypatch.setattr(requests_oauthlib, "OAuth2Session", lambda *args, **kwargs: None)
    monkeypatch.setattr(broker_module.Broker, "_start_orders_thread", lambda self: None)

    with pytest.raises(ValueError, match="LUMIBOT_OAUTH_REFRESH_MODE"):
        schwab_module.Schwab(
            config={
                "SCHWAB_ACCOUNT_NUMBER": "5678",
                "SCHWAB_APP_KEY": "app-key",
                "SCHWAB_TOKEN_PATH": str(token_path),
            }
        )
