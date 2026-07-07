import inspect
import os
import subprocess
import sys


def test_disable_dotenv_skips_recursive_scan(monkeypatch):
    """When LUMIBOT_DISABLE_DOTENV is set, importing credentials must not walk directories."""

    monkeypatch.setenv("LUMIBOT_DISABLE_DOTENV", "1")

    def _boom(*_args, **_kwargs):
        raise AssertionError("os.walk should not be called when LUMIBOT_DISABLE_DOTENV=1")

    monkeypatch.setattr(os, "walk", _boom)

    # credentials.py runs at import-time; force a clean import for this test.
    sys.modules.pop("lumibot.credentials", None)
    __import__("lumibot.credentials")


def test_find_and_load_dotenv_walks_upward_and_loads_local_override(monkeypatch, tmp_path):
    """Verify upward .env discovery and .env.local override behavior from nested paths."""
    monkeypatch.setenv("LUMIBOT_DISABLE_DOTENV", "1")
    monkeypatch.delenv("LUMIBOT_TEST_DOTENV_VALUE", raising=False)
    monkeypatch.delenv("LUMIBOT_DISABLE_DOTENV_LOCAL", raising=False)

    root = tmp_path / "repo"
    child = root / "nested" / "tests"
    child.mkdir(parents=True)
    (root / ".env").write_text("LUMIBOT_TEST_DOTENV_VALUE=base\n", encoding="utf-8")
    (root / ".env.local").write_text("LUMIBOT_TEST_DOTENV_VALUE=local\n", encoding="utf-8")

    sys.modules.pop("lumibot.credentials", None)
    credentials = __import__("lumibot.credentials").credentials

    assert credentials.find_and_load_dotenv(child)
    assert os.environ["LUMIBOT_TEST_DOTENV_VALUE"] == "local"


def test_find_and_load_dotenv_can_skip_local_override(monkeypatch, tmp_path):
    """Verify LUMIBOT_DISABLE_DOTENV_LOCAL leaves values from .env intact."""
    monkeypatch.setenv("LUMIBOT_DISABLE_DOTENV", "1")
    monkeypatch.setenv("LUMIBOT_DISABLE_DOTENV_LOCAL", "1")
    monkeypatch.delenv("LUMIBOT_TEST_DOTENV_VALUE", raising=False)

    root = tmp_path / "repo"
    child = root / "nested" / "tests"
    child.mkdir(parents=True)
    (root / ".env").write_text("LUMIBOT_TEST_DOTENV_VALUE=base\n", encoding="utf-8")
    (root / ".env.local").write_text("LUMIBOT_TEST_DOTENV_VALUE=local\n", encoding="utf-8")

    sys.modules.pop("lumibot.credentials", None)
    credentials = __import__("lumibot.credentials").credentials

    assert credentials.find_and_load_dotenv(child)
    assert os.environ["LUMIBOT_TEST_DOTENV_VALUE"] == "base"


def test_find_and_load_dotenv_ignores_dotenv_directory(monkeypatch, tmp_path):
    """Verify a directory named .env is ignored during upward dotenv discovery."""
    monkeypatch.setenv("LUMIBOT_DISABLE_DOTENV", "1")
    root = tmp_path / "repo"
    child = root / "nested"
    child.mkdir(parents=True)
    (root / ".env").mkdir()

    sys.modules.pop("lumibot.credentials", None)
    credentials = __import__("lumibot.credentials").credentials

    assert not credentials.find_and_load_dotenv(child)


def test_credentials_broker_exports_are_real_classes(monkeypatch):
    """Verify credentials broker exports resolve to the real broker classes."""
    monkeypatch.setenv("LUMIBOT_DISABLE_DOTENV", "1")
    sys.modules.pop("lumibot.credentials", None)

    import lumibot.brokers as brokers
    import lumibot.credentials as credentials

    for name in sorted(credentials._BROKER_CLASS_NAMES):
        credential_class = getattr(credentials, name)
        broker_class = getattr(brokers, name)

        assert inspect.isclass(credential_class)
        assert credential_class is broker_class


def test_lazy_credentials_polymarket_trading_broker_builds_broker(monkeypatch):
    monkeypatch.setenv("LUMIBOT_DISABLE_DOTENV", "1")
    monkeypatch.setenv("LUMIBOT_DISABLE_DOTENV_LOCAL", "1")
    monkeypatch.setenv("LUMIBOT_LAZY_CREDENTIALS", "1")
    monkeypatch.setenv("LUMIBOT_CONNECT_STREAM", "0")
    monkeypatch.setenv("IS_BACKTESTING", "false")
    monkeypatch.setenv("TRADING_BROKER", "polymarket")
    sys.modules.pop("lumibot.credentials", None)

    import lumibot.credentials as credentials
    import lumibot.data_sources as data_sources

    class FakePolymarketData:
        def __init__(self, config):
            self.config = config

    class Polymarket:
        def __init__(self, config, data_source=None, connect_stream=True):
            self.config = config
            self.data_source = data_source
            self.connect_stream = connect_stream
            self.name = "Polymarket"

    monkeypatch.setitem(data_sources.__dict__, "PolymarketData", FakePolymarketData)
    monkeypatch.setattr(credentials, "_broker_class", lambda name: Polymarket)

    broker = credentials.BROKER

    assert broker is not None
    assert isinstance(broker, Polymarket)
    assert broker.name == "Polymarket"
    assert isinstance(broker.data_source, FakePolymarketData)
    assert broker.data_source.config is credentials.POLYMARKET_CONFIG
    assert broker.connect_stream is False


def test_lazy_credentials_polymarket_data_source_builds_data_source(monkeypatch):
    monkeypatch.setenv("LUMIBOT_DISABLE_DOTENV", "1")
    monkeypatch.setenv("LUMIBOT_DISABLE_DOTENV_LOCAL", "1")
    monkeypatch.setenv("LUMIBOT_LAZY_CREDENTIALS", "1")
    monkeypatch.setenv("IS_BACKTESTING", "false")
    monkeypatch.setenv("DATA_SOURCE", "polymarket")
    monkeypatch.delenv("TRADING_BROKER", raising=False)
    sys.modules.pop("lumibot.credentials", None)

    import lumibot.credentials as credentials
    import lumibot.data_sources as data_sources

    class FakePolymarketData:
        def __init__(self, config):
            self.config = config

    monkeypatch.setitem(data_sources.__dict__, "PolymarketData", FakePolymarketData)

    data_source = credentials.DATA_SOURCE

    assert isinstance(data_source, FakePolymarketData)
    assert data_source.config is credentials.POLYMARKET_CONFIG


def test_eager_credentials_polymarket_respects_connect_stream_env():
    env = os.environ.copy()
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env["LUMIBOT_CONNECT_STREAM"] = "0"
    env["LUMIBOT_LOG_LEVEL"] = "ERROR"
    env["IS_BACKTESTING"] = "false"
    env["TRADING_BROKER"] = "polymarket"
    env.pop("LUMIBOT_LAZY_CREDENTIALS", None)
    env.pop("LUMIBOT_SCHEDULED_EXECUTION", None)

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import lumibot.credentials as credentials; "
                "print('broker_global=' + str('BROKER' in credentials.__dict__)); "
                "broker = credentials.BROKER; "
                "print('broker_class=' + broker.__class__.__name__); "
                "print('broker_name=' + broker.name); "
                "print('stream_exists=' + str(hasattr(broker, 'stream'))); "
                "broker.cleanup_streams()"
            ),
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    assert "broker_global=True" in result.stdout
    assert "broker_class=Polymarket" in result.stdout
    assert "broker_name=Polymarket" in result.stdout
    assert "stream_exists=False" in result.stdout
