import importlib.util
import sys
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "learn_lumibot" / "simple_buy_bot.py"


def _load_simple_buy_bot_module():
    spec = importlib.util.spec_from_file_location("simple_buy_bot_test_module", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_get_alpaca_config_uses_standard_env_names(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "standard-key")
    monkeypatch.setenv("ALPACA_API_SECRET", "standard-secret")
    monkeypatch.delenv("ALPACA_SECRET_KEY", raising=False)
    monkeypatch.delenv("APCA_API_KEY_ID", raising=False)
    monkeypatch.delenv("APCA_API_SECRET_KEY", raising=False)

    module = _load_simple_buy_bot_module()

    assert module.ALPACA_CONFIG["API_KEY"] == "standard-key"
    assert module.ALPACA_CONFIG["API_SECRET"] == "standard-secret"
    assert module.ALPACA_CONFIG["PAPER"] is True


def test_get_alpaca_config_accepts_legacy_secret_alias(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "legacy-key")
    monkeypatch.delenv("ALPACA_API_SECRET", raising=False)
    monkeypatch.setenv("ALPACA_SECRET_KEY", "legacy-secret")
    monkeypatch.delenv("APCA_API_KEY_ID", raising=False)
    monkeypatch.delenv("APCA_API_SECRET_KEY", raising=False)

    module = _load_simple_buy_bot_module()

    assert module.ALPACA_CONFIG["API_KEY"] == "legacy-key"
    assert module.ALPACA_CONFIG["API_SECRET"] == "legacy-secret"


def test_credentials_module_accepts_legacy_alpaca_secret_alias(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "cred-key")
    monkeypatch.delenv("ALPACA_API_SECRET", raising=False)
    monkeypatch.setenv("ALPACA_SECRET_KEY", "cred-secret")
    monkeypatch.delenv("APCA_API_KEY_ID", raising=False)
    monkeypatch.delenv("APCA_API_SECRET_KEY", raising=False)

    sys_modules = __import__("sys").modules
    sys_modules.pop("lumibot.credentials", None)
    credentials = __import__("lumibot.credentials", fromlist=["ALPACA_CONFIG"]).ALPACA_CONFIG

    assert credentials["API_KEY"] == "cred-key"
    assert credentials["API_SECRET"] == "cred-secret"
