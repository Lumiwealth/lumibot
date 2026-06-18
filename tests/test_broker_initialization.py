"""
Simple test cases for broker initialization error handling.
"""
import json
import time

import pytest
from unittest.mock import patch, MagicMock

from lumibot.strategies import Strategy
from lumibot.entities import Asset


class TestBrokerInitializationSimple:
    """Test cases for broker initialization and error handling."""
    
    def test_strategy_with_none_broker_raises_helpful_error(self):
        """
        Test that when broker is None, a helpful error message is provided
        that explains how to set up environment variables.
        """
        # Mock both the credentials imports in the strategy module
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


def test_schwab_force_refresh_on_startup_rewrites_token(monkeypatch, tmp_path):
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
                "issued_at": int(time.time() * 1000),
            }

    class _AccountResponse:
        status_code = 200

        def json(self):
            return [{"accountNumber": "12345678", "hashValue": "hash-123"}]

    class _Client:
        def __init__(self, *, api_key, session):
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
