"""Tests for BACKTESTING_PARAMETERS environment variable support."""

import importlib
import json


class TestBacktestingParametersEnvVar:
    """Test parsing of BACKTESTING_PARAMETERS in credentials.py."""

    def _reload_credentials(self):
        """Force reimport of credentials module to pick up env var changes."""
        import lumibot.credentials

        importlib.reload(lumibot.credentials)
        return lumibot.credentials

    def test_valid_json_dict(self, monkeypatch):
        """Valid JSON dict should be parsed correctly."""
        params = {"symbol": "AAPL", "quantity": 10, "spread_width": 5.0}
        monkeypatch.setenv("BACKTESTING_PARAMETERS", json.dumps(params))
        creds = self._reload_credentials()
        assert creds.BACKTESTING_PARAMETERS == params

    def test_nested_dict(self, monkeypatch):
        """Nested dicts (like ALLOCATION) should work."""
        params = {"ALLOCATION": {"SPY": 0.50, "IWM": 0.50}, "max_utilization": 0.40}
        monkeypatch.setenv("BACKTESTING_PARAMETERS", json.dumps(params))
        creds = self._reload_credentials()
        assert creds.BACKTESTING_PARAMETERS == params
        assert creds.BACKTESTING_PARAMETERS["ALLOCATION"]["SPY"] == 0.50

    def test_empty_string_ignored(self, monkeypatch):
        """Empty string should result in None."""
        monkeypatch.setenv("BACKTESTING_PARAMETERS", "")
        creds = self._reload_credentials()
        assert creds.BACKTESTING_PARAMETERS is None

    def test_none_string_ignored(self, monkeypatch):
        """String 'none' should result in None."""
        monkeypatch.setenv("BACKTESTING_PARAMETERS", "none")
        creds = self._reload_credentials()
        assert creds.BACKTESTING_PARAMETERS is None

    def test_null_string_ignored(self, monkeypatch):
        """String 'null' should result in None."""
        monkeypatch.setenv("BACKTESTING_PARAMETERS", "null")
        creds = self._reload_credentials()
        assert creds.BACKTESTING_PARAMETERS is None

    def test_empty_dict_ignored(self, monkeypatch):
        """Empty dict '{}' should result in None (no-op)."""
        monkeypatch.setenv("BACKTESTING_PARAMETERS", "{}")
        creds = self._reload_credentials()
        assert creds.BACKTESTING_PARAMETERS is None

    def test_invalid_json_ignored(self, monkeypatch):
        """Invalid JSON should be ignored with a warning, not raise."""
        monkeypatch.setenv("BACKTESTING_PARAMETERS", "not-valid-json{")
        creds = self._reload_credentials()
        assert creds.BACKTESTING_PARAMETERS is None

    def test_json_list_ignored(self, monkeypatch):
        """JSON list (not dict) should be ignored with a warning."""
        monkeypatch.setenv("BACKTESTING_PARAMETERS", "[1, 2, 3]")
        creds = self._reload_credentials()
        assert creds.BACKTESTING_PARAMETERS is None

    def test_unset_env_is_none(self, monkeypatch):
        """When env var is not set, BACKTESTING_PARAMETERS should be None."""
        monkeypatch.delenv("BACKTESTING_PARAMETERS", raising=False)
        creds = self._reload_credentials()
        assert creds.BACKTESTING_PARAMETERS is None

    def test_whitespace_trimmed(self, monkeypatch):
        """Whitespace around JSON should be trimmed."""
        params = {"key": "value"}
        monkeypatch.setenv("BACKTESTING_PARAMETERS", f"  {json.dumps(params)}  ")
        creds = self._reload_credentials()
        assert creds.BACKTESTING_PARAMETERS == params
