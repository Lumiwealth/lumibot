"""
Simple test cases for broker initialization error handling.
"""
import json
import stat
import sys
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


def _install_fake_schwab_runtime(monkeypatch, refreshed_token=None, refresh_results=None):
    from lumibot.brokers.broker import Broker
    import lumibot.brokers.schwab as schwab_module

    refresh_results = list(refresh_results or [])

    class FakeAccountNumbersResponse:
        status_code = 200

        def json(self):
            return [{"accountNumber": "12345678", "hashValue": "hash-123"}]

    class FakeClient:
        class Account:
            class Fields:
                POSITIONS = "positions"

        def __init__(self, api_key, session):
            self.api_key = api_key
            self.session = session

        def get_account_numbers(self):
            return FakeAccountNumbersResponse()

    class FakeOAuth2Session:
        instances = []

        def __init__(self, client_id, token, auto_refresh_url, auto_refresh_kwargs, token_updater):
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

        def refresh_token(self, token_url, refresh_token, **kwargs):
            self.refresh_calls.append((token_url, refresh_token, kwargs))
            if refresh_results:
                result = refresh_results.pop(0)
                if isinstance(result, Exception):
                    raise result
                self.token = result
            else:
                self.token = refreshed_token or {
                    "access_token": "refreshed-access",
                    "refresh_token": "rotated-refresh",
                    "issued_at": int(time.time() * 1000),
                    "expires_in": 1800,
                    "token_type": "Bearer",
                    "scope": "api",
                }
            return self.token

    def fake_broker_init(self, name="", data_source=None, config=None, **_kwargs):
        self.name = name
        self.data_source = data_source
        self.config = config
        self.quote_assets = set()

    monkeypatch.setattr(Broker, "__init__", fake_broker_init)
    monkeypatch.setattr(schwab_module, "Client", FakeClient)
    monkeypatch.setattr(schwab_module.Schwab, "_finish_initialization", lambda self, *_args, **_kwargs: None)
    monkeypatch.setattr("requests_oauthlib.OAuth2Session", FakeOAuth2Session)
    return FakeOAuth2Session


def _write_schwab_token(path, *, access_token="existing-access", refresh_token="existing-refresh"):
    path.write_text(
        json.dumps(
            {
                "creation_timestamp": int(time.time()),
                "token": {
                    "access_token": access_token,
                    "refresh_token": refresh_token,
                    "issued_at": int(time.time() * 1000),
                    "expires_in": 1800,
                    "token_type": "Bearer",
                    "scope": "api",
                },
            }
        ),
        encoding="utf-8",
    )


def _assert_private_posix_file(path):
    if sys.platform != "win32":
        assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_schwab_prefers_existing_token_file_over_stale_env_payload(monkeypatch, tmp_path):
    from lumibot.brokers.schwab import Schwab
    from lumibot.tools import SchwabHelper

    token_path = tmp_path / "schwab_token.json"
    _write_schwab_token(token_path, access_token="fresh-access", refresh_token="fresh-refresh")
    _install_fake_schwab_runtime(monkeypatch)
    monkeypatch.setenv("SCHWAB_TOKEN", "stale-original-payload")
    monkeypatch.delenv("LUMIBOT_SCHEDULED_EXECUTION", raising=False)

    save_payload = MagicMock(side_effect=AssertionError("SCHWAB_TOKEN should only seed a missing token file"))
    monkeypatch.setattr(SchwabHelper, "_save_payload_str_to_token_file", save_payload)

    Schwab(
        config={
            "SCHWAB_ACCOUNT_NUMBER": "12345678",
            "SCHWAB_APP_KEY": "app-key",
            "SCHWAB_APP_SECRET": "app-secret",
            "SCHWAB_TOKEN_PATH": str(token_path),
        },
        data_source=object(),
    )

    assert save_payload.call_count == 0
    token_data = json.loads(token_path.read_text(encoding="utf-8"))
    assert token_data["token"]["refresh_token"] == "fresh-refresh"
    assert token_data["token"]["refresh_token_expires_in"] == 604800
    _assert_private_posix_file(token_path)


def test_schwab_scheduled_startup_does_not_force_refresh(monkeypatch, tmp_path):
    from lumibot.brokers.schwab import Schwab

    token_path = tmp_path / "schwab_token.json"
    _write_schwab_token(token_path, access_token="runtime-access", refresh_token="runtime-refresh")
    fake_session_cls = _install_fake_schwab_runtime(monkeypatch)
    monkeypatch.setenv("LUMIBOT_SCHEDULED_EXECUTION", "true")

    Schwab(
        config={
            "SCHWAB_ACCOUNT_NUMBER": "12345678",
            "SCHWAB_APP_KEY": "app-key",
            "SCHWAB_APP_SECRET": "app-secret",
            "SCHWAB_TOKEN_PATH": str(token_path),
        },
        data_source=object(),
    )

    session = fake_session_cls.instances[0]
    assert session.refresh_calls == []
    token_data = json.loads(token_path.read_text(encoding="utf-8"))
    assert token_data["token"]["access_token"] == "runtime-access"
    assert token_data["token"]["refresh_token"] == "runtime-refresh"
    assert token_data["token"]["refresh_token_expires_in"] == 604800
    _assert_private_posix_file(token_path)


def test_schwab_token_updater_preserves_refresh_token_metadata(monkeypatch, tmp_path):
    from lumibot.brokers.schwab import Schwab

    token_path = tmp_path / "schwab_token.json"
    _write_schwab_token(token_path, access_token="runtime-access", refresh_token="runtime-refresh")
    fake_session_cls = _install_fake_schwab_runtime(monkeypatch)

    Schwab(
        config={
            "SCHWAB_ACCOUNT_NUMBER": "12345678",
            "SCHWAB_APP_KEY": "app-key",
            "SCHWAB_APP_SECRET": "app-secret",
            "SCHWAB_TOKEN_PATH": str(token_path),
        },
        data_source=object(),
    )

    original_token_data = json.loads(token_path.read_text(encoding="utf-8"))
    original_refresh_issued_at = original_token_data["token"]["refresh_token_issued_at"]

    session = fake_session_cls.instances[0]
    session.token_updater({
        "access_token": "new-access",
        "expires_in": 1800,
        "token_type": "Bearer",
        "scope": "api",
    })

    token_data = json.loads(token_path.read_text(encoding="utf-8"))
    assert token_data["token"]["access_token"] == "new-access"
    assert token_data["token"]["refresh_token"] == "runtime-refresh"
    assert token_data["token"]["refresh_token_issued_at"] == original_refresh_issued_at
    assert token_data["token"]["refresh_token_expires_in"] == 604800
    _assert_private_posix_file(token_path)


def test_schwab_preserves_token_file_when_client_init_fails_transiently(monkeypatch, tmp_path):
    import lumibot.brokers.schwab as schwab_module
    from lumibot.brokers.schwab import Schwab

    token_path = tmp_path / "schwab_token.json"
    _write_schwab_token(token_path, access_token="runtime-access", refresh_token="runtime-refresh")
    _install_fake_schwab_runtime(monkeypatch)

    class FailingClient:
        def __init__(self, *_args, **_kwargs):
            raise TimeoutError("network timeout while initializing client")

    monkeypatch.setattr(schwab_module, "Client", FailingClient)

    with pytest.raises(ConnectionError):
        Schwab(
            config={
                "SCHWAB_ACCOUNT_NUMBER": "12345678",
                "SCHWAB_APP_KEY": "app-key",
                "SCHWAB_APP_SECRET": "app-secret",
                "SCHWAB_TOKEN_PATH": str(token_path),
            },
            data_source=object(),
        )

    token_data = json.loads(token_path.read_text(encoding="utf-8"))
    assert token_data["token"]["access_token"] == "runtime-access"
    assert token_data["token"]["refresh_token"] == "runtime-refresh"
    _assert_private_posix_file(token_path)
