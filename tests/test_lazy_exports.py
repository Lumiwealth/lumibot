import importlib
import inspect
import os
import subprocess
import sys
from unittest.mock import patch


_RUNTIME_ENV_PREFIXES = (
    "ALPACA_",
    "BACKTESTING_",
    "DATABENTO_",
    "DATADOWNLOADER_",
    "LUMIBOT_",
    "LUMIWEALTH_",
    "POLYGON_",
    "PROJECTX_",
    "THETADATA_",
    "TRADIER_",
)


def _clean_subprocess_env() -> dict[str, str]:
    """Keep lazy-import subprocesses independent of local runtime credentials."""
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in {"BROKER", "DATA_SOURCE", "IS_BACKTESTING", "TRADING_BROKER"}
        and not key.startswith(_RUNTIME_ENV_PREFIXES)
    }
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env["LUMIBOT_LOG_LEVEL"] = "ERROR"
    return env


def test_lazy_package_all_exports_resolve():
    modules = [
        "lumibot",
        "lumibot.backtesting",
        "lumibot.brokers",
        "lumibot.components",
        "lumibot.components.agents",
        "lumibot.data_sources",
        "lumibot.entities",
        "lumibot.strategies",
        "lumibot.tools",
        "lumibot.traders",
    ]

    for module_name in modules:
        module = importlib.import_module(module_name)
        for export_name in getattr(module, "__all__", ()):
            getattr(module, export_name)


def test_lazy_package_exports_defer_heavy_submodule_imports(monkeypatch):
    # Invariant: importing package namespaces must not import heavy optional
    # backends until their public export is first accessed.
    cases = [
        ("lumibot.backtesting", "BacktestingBroker", "lumibot.backtesting.backtesting_broker"),
        ("lumibot.brokers", "Alpaca", "lumibot.brokers.alpaca"),
        ("lumibot.components", "BuiltinTools", "lumibot.components.agents"),
        ("lumibot.data_sources", "YahooData", "lumibot.data_sources.yahoo_data"),
        ("lumibot.tools", "YahooHelper", "lumibot.tools.yahoo_helper"),
    ]

    for module_name, export_name, target_module in cases:
        parent_name, child_name = target_module.rsplit(".", 1)
        parent_module = importlib.import_module(parent_name)
        monkeypatch.delitem(parent_module.__dict__, child_name, raising=False)
        monkeypatch.delitem(sys.modules, target_module, raising=False)
        module = importlib.import_module(module_name)
        monkeypatch.delitem(module.__dict__, export_name, raising=False)

        assert target_module not in sys.modules
        getattr(module, export_name)
        assert target_module in sys.modules


def test_trading_builtins_safelist_defer_stream_import():
    env = os.environ.copy()
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env["LUMIBOT_LOG_LEVEL"] = "ERROR"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from lumibot.trading_builtins import SafeList, SafeOrderDict; "
                "import sys; "
                "print('safelist=' + SafeList.__name__); "
                "print('stream_loaded=' + str('lumibot.trading_builtins.custom_stream' in sys.modules))"
            ),
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    assert "safelist=SafeList" in result.stdout
    assert "stream_loaded=False" in result.stdout


def test_startup_class_exports_defer_heavy_dependencies():
    env = os.environ.copy()
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env["LUMIBOT_LAZY_CREDENTIALS"] = "1"
    env["LUMIBOT_LOG_LEVEL"] = "ERROR"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from lumibot.strategies import Strategy; "
                "from lumibot.strategies.strategy_executor import StrategyExecutor; "
                "from lumibot.brokers import Alpaca, Bitunix, Broker, Ccxt, InteractiveBrokersREST, ProjectX, Schwab, Tradier, Tradovate; "
                "from lumibot.data_sources import AlpacaData, BitunixData, CcxtData, DataSource, InteractiveBrokersRESTData, ProjectXData, SchwabData, TradierData, TradovateData; "
                "import sys; "
                "heavy = ('alpaca', 'apscheduler', 'authlib', 'ccxt', 'concurrent.futures', 'datetime', "
                "'decimal', 'httpx', 'importlib.resources', 'inspect', 'json', 'lumibot.constants', 'lumibot.entities.asset', 'lumibot.entities.cash_event', "
                "'lumibot.entities.order', 'lumibot.entities.position', 'lumibot.tools.alpaca_helpers', "
                "'lumibot.tools.bitunix_helpers', 'lumibot.tools.projectx_helpers', "
                "'lumibot.tools.lumibot_logger', 'lumibot.tools.smart_limit_utils', "
                "'lumibot.tools.symbol_normalization', "
                "'lumibot.trading_builtins.custom_stream', "
                "'logging', 'lumiwealth_tradier', 'matplotlib', 'numpy', 'pandas', "
                "'pandas_market_calendars', 'polars', 'requests', 'schwab', 'scipy', "
                "'random', 'subprocess', 'tempfile', 'termcolor', 'traceback', 'typing', 'yfinance'); "
                "loaded = sorted(name for name in heavy "
                "if name in sys.modules or any(module.startswith(name + '.') for module in sys.modules)); "
                "print(','.join(loaded))"
            ),
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.stdout.strip() == ""


def test_scheduled_alpaca_credentials_defer_stream_dependencies():
    env = os.environ.copy()
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env["LUMIBOT_LOG_LEVEL"] = "ERROR"
    env["LUMIBOT_SCHEDULED_EXECUTION"] = "true"
    env["IS_BACKTESTING"] = "false"
    env["TRADING_BROKER"] = "alpaca"
    env["ALPACA_API_KEY"] = "fake"
    env["ALPACA_API_SECRET"] = "fake"
    env["ALPACA_IS_PAPER"] = "true"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import lumibot.credentials as credentials; "
                "import sys, threading; "
                "print('broker=' + credentials.BROKER.name); "
                "heavy = ('alpaca', 'asyncio', 'concurrent.futures', 'dateutil', 'datetime', 'decimal', 'json', "
                "'inspect', 'logging', 'random', 'string', 'traceback', 'uuid', "
                "'lumibot.data_sources.alpaca_data', 'lumibot.data_sources.data_source', "
                "'lumibot.entities.asset', 'lumibot.entities.cash_event', 'lumibot.entities.order', 'lumibot.entities.position', "
                "'lumibot.entities.quote', 'lumibot.entities.smart_limit', "
                "'lumibot.entities.trading_slippage', 'lumibot.tools.lumibot_logger', "
                "'lumibot.tools.runtime_telemetry', 'lumibot.tools.smart_limit_utils', 'lumibot.tools.symbol_parser', "
                "'lumibot.trading_builtins.custom_stream', 'numpy', 'pandas', "
                "'pytz', 'requests', 'typing', 'websockets'); "
                "loaded = sorted(name for name in heavy "
                "if name in sys.modules or any(module.startswith(name + '.') for module in sys.modules)); "
                "print('loaded=' + ','.join(loaded)); "
                "print('threads=' + ','.join(sorted(thread.name for thread in threading.enumerate())))"
            ),
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    assert "broker=alpaca" in result.stdout
    assert "loaded=\n" in result.stdout
    assert "alpaca_orders_thread" not in result.stdout


def test_scheduled_legacy_broker_alias_resolves_cached_default_without_workers():
    env = os.environ.copy()
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env["LUMIBOT_LOG_LEVEL"] = "ERROR"
    env["LUMIBOT_SCHEDULED_EXECUTION"] = "true"
    env["IS_BACKTESTING"] = "false"
    env["TRADING_BROKER"] = "alpaca"
    env["ALPACA_API_KEY"] = "fake"
    env["ALPACA_API_SECRET"] = "fake"
    env["ALPACA_IS_PAPER"] = "true"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from lumibot.credentials import broker; "
                "import lumibot.credentials as credentials; "
                "import threading; "
                "print('broker=' + broker.name); "
                "print('lower_is_upper=' + str(broker is credentials.BROKER)); "
                "print('lower_is_getter=' + str(broker is credentials.get_default_broker())); "
                "print('stream_exists=' + str(hasattr(broker, 'stream'))); "
                "print('threads=' + ','.join(sorted(thread.name for thread in threading.enumerate())))"
            ),
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    assert "broker=alpaca" in result.stdout
    assert "lower_is_upper=True" in result.stdout
    assert "lower_is_getter=True" in result.stdout
    assert "stream_exists=False" in result.stdout
    assert "alpaca_orders_thread" not in result.stdout


def test_scheduled_legacy_data_source_alias_uses_same_lazy_getter(monkeypatch):
    import lumibot.credentials as credentials

    expected = object()
    monkeypatch.setattr(credentials, "get_default_data_source", lambda: expected)
    for name in ("data_source", "DATA_SOURCE"):
        monkeypatch.delitem(credentials.__dict__, name, raising=False)

    assert credentials.data_source is expected
    assert credentials.DATA_SOURCE is expected


def test_scheduled_strategy_import_defers_default_broker_resolution():
    env = os.environ.copy()
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env["LUMIBOT_LOG_LEVEL"] = "ERROR"
    env["LUMIBOT_SCHEDULED_EXECUTION"] = "true"
    env["IS_BACKTESTING"] = "false"
    env["TRADING_BROKER"] = "alpaca"
    env["ALPACA_API_KEY"] = "fake"
    env["ALPACA_API_SECRET"] = "fake"
    env["ALPACA_IS_PAPER"] = "true"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from lumibot.strategies import Strategy; "
                "import sys; "
                "heavy = ('alpaca', 'asyncio', 'concurrent.futures', 'datetime', 'decimal', 'inspect', 'json', "
                "'logging', 'random', 'string', 'traceback', 'uuid', 'lumibot.credentials', 'lumibot.entities.asset', 'lumibot.entities.cash_event', "
                "'lumibot.entities.order', 'lumibot.entities.position', 'lumibot.entities.quote', "
                "'lumibot.entities.smart_limit', 'lumibot.entities.trading_slippage', "
                "'lumibot.tools.lumibot_logger', 'lumibot.tools.smart_limit_utils', 'lumibot.tools.symbol_parser', "
                "'lumibot.traders.trader', 'numpy', 'pandas', 'pytz', 'requests', 'typing', 'websockets'); "
                "loaded = sorted(name for name in heavy "
                "if name in sys.modules or any(module.startswith(name + '.') for module in sys.modules)); "
                "print('strategy=' + Strategy.__name__); "
                "print('loaded=' + ','.join(loaded))"
            ),
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    assert "strategy=Strategy" in result.stdout
    assert "loaded=\n" in result.stdout


def test_strategy_executor_import_defers_datetime_and_asset():
    env = os.environ.copy()
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env["LUMIBOT_LOG_LEVEL"] = "ERROR"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from lumibot.strategies.strategy_executor import StrategyExecutor; "
                "import sys; "
                "heavy = ('datetime', 'decimal', 'lumibot.entities.asset', 'lumibot.entities.order', "
                "'lumibot.strategies.scheduled_timing', 'lumibot.tools.decorators', "
                "'lumibot.tools.smart_limit_utils', 'pandas', 'pandas_market_calendars', 'typing'); "
                "loaded = sorted(name for name in heavy "
                "if name in sys.modules or any(module.startswith(name + '.') for module in sys.modules)); "
                "print('executor=' + StrategyExecutor.__name__); "
                "print('loaded=' + ','.join(loaded))"
            ),
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    assert "executor=StrategyExecutor" in result.stdout
    assert "loaded=\n" in result.stdout


def test_trader_import_defers_logging_and_typing():
    env = os.environ.copy()
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env["LUMIBOT_LOG_LEVEL"] = "ERROR"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "baseline = set(sys.modules); "
                "from lumibot.traders import Trader; "
                "heavy = ('dataclasses', 'importlib.util', 'inspect', 'logging', 'lumibot.tools.lumibot_logger', "
                "'signal', 'threading', 'typing'); "
                "loaded = sorted(name for name in heavy "
                "if name not in baseline and (name in sys.modules "
                "or any(module.startswith(name + '.') for module in sys.modules))); "
                "print('trader=' + Trader.__name__); "
                "print('loaded=' + ','.join(loaded))"
            ),
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    assert "trader=Trader" in result.stdout
    assert "loaded=\n" in result.stdout


def test_public_runtime_type_hints_resolve_after_lazy_imports():
    env = os.environ.copy()
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env["LUMIBOT_LOG_LEVEL"] = "ERROR"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import typing; "
                "from lumibot.entities import Asset, Order; "
                "from lumibot.strategies.strategy import Strategy; "
                "from lumibot.traders import Trader; "
                "from lumibot.data_sources.tradier_data import TradierData; "
                "checks = ("
                "Asset.__init__, "
                "Order.__init__, "
                "Strategy.create_order, "
                "Strategy.backtest, "
                "Strategy._collect_cash_events_for_cloud, "
                "Strategy._mark_cash_events_sent, "
                "Trader._get_backtest_profiling_config, "
                "TradierData.get_last_price, "
                "TradierData.get_quote"
                "); "
                "[typing.get_type_hints(check) for check in checks]; "
                "print('ok')"
            ),
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.stdout.strip() == "ok"


def test_datasource_default_timezone_loads_on_access_only():
    env = os.environ.copy()
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env["LUMIBOT_LOG_LEVEL"] = "ERROR"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from lumibot.data_sources import DataSource; "
                "import sys; "
                "print('datetime_before=' + str('datetime' in sys.modules)); "
                "import pytz; "
                "tz = DataSource.DEFAULT_PYTZ; "
                "real = pytz.timezone('America/New_York'); "
                "print('zone=' + tz.zone); "
                "print('str=' + str(tz)); "
                "print('eq=' + str(tz == real)); "
                "print('hash_eq=' + str(hash(tz) == hash(real))); "
                "print('datetime_after=' + str('datetime' in sys.modules))"
            ),
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    assert "datetime_before=False" in result.stdout
    assert "zone=America/New_York" in result.stdout
    assert "str=America/New_York" in result.stdout
    assert "eq=True" in result.stdout
    assert "hash_eq=True" in result.stdout
    assert "datetime_after=True" in result.stdout


def test_plain_asset_construction_defers_datetime():
    env = os.environ.copy()
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env["LUMIBOT_LOG_LEVEL"] = "ERROR"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from lumibot.entities import Asset; "
                "import sys; "
                "print('datetime_after_import=' + str('datetime' in sys.modules)); "
                "Asset('AAPL'); "
                "Asset('USD', asset_type='forex'); "
                "print('datetime_after_assets=' + str('datetime' in sys.modules))"
            ),
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    assert "datetime_after_import=False" in result.stdout
    assert "datetime_after_assets=False" in result.stdout


def test_datasource_tzinfo_materializes_pandas_compatible_timezone():
    env = os.environ.copy()
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env["LUMIBOT_LOG_LEVEL"] = "ERROR"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
from lumibot.data_sources.data_source import DataSource


class StubDataSource(DataSource):
    def get_chains(self, *args, **kwargs):
        return {}

    def get_historical_prices(self, *args, **kwargs):
        return None

    def get_last_price(self, *args, **kwargs):
        return None

    def get_quote(self, *args, **kwargs):
        return None


ds = StubDataSource()
import sys
print('datetime_before=' + str('datetime' in sys.modules))
tz = ds.tzinfo
import pandas as pd
idx = pd.DatetimeIndex(['2026-07-01 09:30']).tz_localize(tz).tz_convert(tz)
print('tz=' + str(tz))
print('idx_tz=' + str(idx.tz))
print('datetime_after=' + str('datetime' in sys.modules))
""",
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    assert "datetime_before=False" in result.stdout
    assert "tz=America/New_York" in result.stdout
    assert "idx_tz=America/New_York" in result.stdout
    assert "datetime_after=True" in result.stdout


def test_bitunix_data_default_timezone_is_utc(monkeypatch):
    bitunix_data = importlib.import_module("lumibot.data_sources.bitunix_data")

    class FakeBitUnixClient:
        def __init__(self, api_key, api_secret):
            self.api_key = api_key
            self.api_secret = api_secret

    monkeypatch.setattr(bitunix_data, "_get_bitunix_client_class", lambda: FakeBitUnixClient)

    data_source = bitunix_data.BitunixData({"API_KEY": "key", "API_SECRET": "secret"})

    assert data_source.tzinfo.zone == "UTC"


def test_diversified_leverage_import_defers_datetime():
    env = _clean_subprocess_env()

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from lumibot.example_strategies.stock_diversified_leverage import DiversifiedLeverage; "
                "import sys; "
                "heavy = ('datetime', 'lumibot.backtesting', 'lumibot.entities.trading_fee', "
                "'pandas', 'typing', 'yfinance'); "
                "loaded = sorted(name for name in heavy "
                "if name in sys.modules or any(module.startswith(name + '.') for module in sys.modules)); "
                "print('strategy=' + DiversifiedLeverage.__name__); "
                "print('loaded=' + ','.join(loaded))"
            ),
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    assert "strategy=DiversifiedLeverage" in result.stdout
    assert "loaded=\n" in result.stdout


def test_bitunix_helper_import_defers_requests():
    env = os.environ.copy()
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env["LUMIBOT_LOG_LEVEL"] = "ERROR"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from lumibot.tools.bitunix_helpers import BitUnixClient; "
                "import sys; "
                "print('client=' + BitUnixClient.__name__); "
                "print('requests_loaded=' + str('requests' in sys.modules)); "
                "print('logger_loaded=' + str('lumibot.tools.lumibot_logger' in sys.modules)); "
                "print('logging_loaded=' + str('logging' in sys.modules)); "
                "print('datetime_loaded=' + str('datetime' in sys.modules))"
            ),
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    assert "client=BitUnixClient" in result.stdout
    assert "requests_loaded=False" in result.stdout
    assert "logger_loaded=False" in result.stdout
    assert "logging_loaded=False" in result.stdout
    assert "datetime_loaded=False" in result.stdout


def test_projectx_constructors_defer_logger_in_scheduled_env():
    env = os.environ.copy()
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env["LUMIBOT_LOG_LEVEL"] = "ERROR"
    env["LUMIBOT_SCHEDULED_EXECUTION"] = "true"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
from lumibot.data_sources.projectx_data import ProjectXData
from lumibot.brokers.projectx import ProjectX
import sys


config = {
    'api_key': 'key',
    'username': 'user',
    'base_url': 'https://example.com',
    'firm': 'demo',
    'preferred_account_name': 'acct',
}
data_source = ProjectXData(config)
ProjectX(config, data_source=data_source, connect_stream=False)
deferred = (
    'logging',
    'lumibot.tools.lumibot_logger',
    'lumibot.tools.runtime_telemetry',
    'datetime',
)
loaded = sorted(
    name
    for name in deferred
    if name in sys.modules or any(module.startswith(name + '.') for module in sys.modules)
)
print('loaded=' + ','.join(loaded))
""",
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    assert "loaded=\n" in result.stdout


def test_order_import_defers_smart_limit_module():
    env = os.environ.copy()
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env["LUMIBOT_LOG_LEVEL"] = "ERROR"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from lumibot.entities import Order; "
                "import sys; "
                "print('order=' + Order.__name__); "
                "print('smart_limit_loaded=' + str('lumibot.entities.smart_limit' in sys.modules))"
            ),
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    assert "order=Order" in result.stdout
    assert "smart_limit_loaded=False" in result.stdout


def test_startup_order_lazy_class_proxy_defers_order_module():
    env = os.environ.copy()
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env["LUMIBOT_LOG_LEVEL"] = "ERROR"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from lumibot.strategies import Strategy; "
                "import lumibot.strategies.strategy as strategy_module; "
                "import sys; "
                "print('order_loaded_before=' + str('lumibot.entities.order' in sys.modules)); "
                "from lumibot.entities import Asset, Order; "
                "order = Order(asset=Asset('SPY'), quantity=1, side=Order.OrderSide.BUY, strategy='abc'); "
                "print('proxy_name=' + strategy_module.Order.__name__); "
                "print('proxy_instance=' + str(isinstance(order, strategy_module.Order))); "
                "print('enum=' + strategy_module.Order.OrderType.MARKET.value)"
            ),
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    assert "order_loaded_before=False" in result.stdout
    assert "proxy_name=Order" in result.stdout
    assert "proxy_instance=True" in result.stdout
    assert "enum=market" in result.stdout


def test_lazy_class_proxy_subclass_and_signature_compatibility():
    env = os.environ.copy()
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env["LUMIBOT_LOG_LEVEL"] = "ERROR"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import inspect
import lumibot.strategies.strategy as strategy_module
from lumibot.entities import Asset


class MyAsset(strategy_module.Asset):
    pass


print('subclass=' + str(issubclass(MyAsset, Asset)))
print('instance=' + str(isinstance(MyAsset('SPY'), Asset)))
print('signature=' + str(inspect.signature(strategy_module.Asset)))
""",
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    assert "subclass=True" in result.stdout
    assert "instance=True" in result.stdout
    assert "symbol: str" in result.stdout


def test_strategy_construction_defers_optional_components():
    env = _clean_subprocess_env()

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
from lumibot.strategies.strategy import Strategy
import sys


class DataSource:
    SOURCE = "stub"

    def __init__(self):
        self._data_store = {}
        self.datetime_start = None
        self.datetime_end = None


class Positions:
    def __init__(self):
        self.items = []

    def append(self, item):
        self.items.append(item)

    def get_list(self):
        return self.items


class Broker:
    IS_BACKTESTING_BROKER = False
    market = "NYSE"
    name = "stub"

    def __init__(self):
        self.data_source = DataSource()
        self._orders_queue = type("Queue", (), {"queue": []})()
        self._first_iteration = True

    def is_backtesting_broker(self):
        return False

    def set_strategy_name(self, name):
        self.strategy_name = name

    def _set_initial_positions(self, strategy):
        return None

    def _add_subscriber(self, subscriber):
        self.subscriber = subscriber

    def get_tracked_positions(self, strategy_name):
        return []


class TestStrategy(Strategy):
    def initialize(self):
        pass

    def on_trading_iteration(self):
        pass


TestStrategy(broker=Broker(), name="lazy-components")
deferred = (
    "lumibot.components.agents",
    "lumibot.components.memory",
    "lumibot.components.notifications",
    "lumibot.fundamentals",
    "lumibot.indicators",
    "lumibot.macro",
)
heavy = (
    "apscheduler",
    "duckdb",
    "matplotlib",
    "numpy",
    "pandas",
    "pandas_market_calendars",
    "polars",
    "scipy",
)
loaded = sorted(
    name
    for name in deferred + heavy
    if name in sys.modules or any(module.startswith(name + ".") for module in sys.modules)
)
print(",".join(loaded))
""",
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.stdout.strip() == ""


def test_scheduled_alpaca_strategy_construction_defers_logger_and_order():
    env = os.environ.copy()
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env["LUMIBOT_LOG_LEVEL"] = "ERROR"
    env["LUMIBOT_SCHEDULED_EXECUTION"] = "true"
    env["IS_BACKTESTING"] = "false"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
from lumibot.brokers import Alpaca
from lumibot.example_strategies.stock_diversified_leverage import DiversifiedLeverage
import sys


config = {"API_KEY": "test_api_key", "API_SECRET": "test_api_secret", "PAPER": True}
broker = Alpaca(config, connect_stream=False, start_orders_thread=False)
broker._get_balances_at_broker = lambda quote_asset, strategy: (100000.0, 0.0, 100000.0)
broker._set_initial_positions = lambda strategy: None
DiversifiedLeverage(broker=broker)

deferred = (
    "datetime",
    "json",
    "logging",
    "traceback",
    "lumibot.credentials",
    "lumibot.data_sources.alpaca_data",
    "lumibot.data_sources.data_source",
    "lumibot.entities.order",
    "lumibot.strategies.scheduled_timing",
    "lumibot.strategies.strategy_executor",
    "lumibot.tools.decorators",
    "lumibot.tools.lumibot_logger",
    "lumibot.tools.runtime_telemetry",
)
loaded = sorted(
    name
    for name in deferred
    if name in sys.modules or any(module.startswith(name + ".") for module in sys.modules)
)
print(",".join(loaded))
""",
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.stdout.strip() == ""


def test_scheduled_alpaca_constructor_defaults_defer_stream_and_orders():
    env = os.environ.copy()
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env["LUMIBOT_LOG_LEVEL"] = "ERROR"
    env["LUMIBOT_SCHEDULED_EXECUTION"] = "true"
    env["IS_BACKTESTING"] = "false"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
from lumibot.brokers import Alpaca
import sys
import threading


config = {"API_KEY": "test_api_key", "API_SECRET": "test_api_secret", "PAPER": True}
broker = Alpaca(config)
deferred = (
    "alpaca",
    "asyncio",
    "logging",
    "lumibot.data_sources.alpaca_data",
    "lumibot.data_sources.data_source",
    "lumibot.tools.lumibot_logger",
    "numpy",
    "pandas",
    "requests",
    "websockets",
)
loaded = sorted(
    name
    for name in deferred
    if name in sys.modules or any(module.startswith(name + ".") for module in sys.modules)
)
threads = sorted(thread.name for thread in threading.enumerate())
print("loaded=" + ",".join(loaded))
print("threads=" + ",".join(threads))
broker.data_source._timestep = "day"
print("timestep=" + broker.data_source.get_timestep())
print("data_source_loaded_after=" + str("lumibot.data_sources.alpaca_data" in sys.modules))
print("data_source_class_after=" + broker.data_source.__class__.__name__)
""",
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    assert "loaded=\n" in result.stdout
    assert "alpaca_orders_thread" not in result.stdout
    assert "broker_alpaca_thread" not in result.stdout
    assert "timestep=day" in result.stdout
    assert "data_source_loaded_after=True" in result.stdout
    assert "data_source_class_after=AlpacaData" in result.stdout


def test_scheduled_alpaca_constructor_preserves_missing_secret_error():
    env = os.environ.copy()
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env["LUMIBOT_LOG_LEVEL"] = "ERROR"
    env["LUMIBOT_SCHEDULED_EXECUTION"] = "true"
    env["IS_BACKTESTING"] = "false"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from lumibot.brokers import Alpaca; "
                "Alpaca({'API_KEY': 'key_without_secret', 'PAPER': True})"
            ),
        ],
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.returncode != 0
    assert "API_SECRET not found in config when API_KEY is provided" in result.stderr


def test_scheduled_strategy_construction_defers_executor_until_first_use():
    env = os.environ.copy()
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env["LUMIBOT_LOG_LEVEL"] = "ERROR"
    env["LUMIBOT_SCHEDULED_EXECUTION"] = "true"
    env["IS_BACKTESTING"] = "false"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
from lumibot.strategies.strategy import Strategy
import sys


class DataSource:
    SOURCE = "stub"

    def __init__(self):
        self._data_store = {}
        self.datetime_start = None
        self.datetime_end = None


class Positions:
    def __init__(self):
        self.items = []

    def append(self, item):
        self.items.append(item)

    def get_list(self):
        return self.items


class Broker:
    IS_BACKTESTING_BROKER = False
    market = "NYSE"
    name = "stub"

    def __init__(self):
        self.data_source = DataSource()
        self._orders_queue = type("Queue", (), {"queue": []})()
        self._orders_thread = None
        self._first_iteration = True
        self._subscribers = []
        self._filled_positions = Positions()

    def is_backtesting_broker(self):
        return False

    def set_strategy_name(self, name):
        self.strategy_name = name

    def _set_initial_positions(self, strategy):
        return None

    def _add_subscriber(self, subscriber):
        self._subscribers.append(subscriber)

    def _get_balances_at_broker(self, quote_asset, strategy):
        return (100000.0, 0.0, 100000.0)

    def get_tracked_positions(self, strategy_name):
        return []


class TestStrategy(Strategy):
    def initialize(self):
        pass

    def on_trading_iteration(self):
        pass


broker = Broker()
strategy = TestStrategy(broker=broker, name="lazy-executor")
print("loaded_before=" + str("lumibot.strategies.strategy_executor" in sys.modules))
print("subscribers_before=" + str(len(broker._subscribers)))
print("instance_before=" + str(strategy._executor_instance is None))
executor = strategy._executor
print("loaded_after=" + str("lumibot.strategies.strategy_executor" in sys.modules))
print("subscribers_after=" + str(len(broker._subscribers)))
print("same_executor=" + str(strategy._executor is executor))
""",
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    assert "loaded_before=False" in result.stdout
    assert "subscribers_before=0" in result.stdout
    assert "instance_before=True" in result.stdout
    assert "loaded_after=True" in result.stdout
    assert "subscribers_after=1" in result.stdout
    assert "same_executor=True" in result.stdout


def test_top_level_import_logs_version_without_loading_credentials():
    env = os.environ.copy()
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env.pop("LUMIBOT_LOG_LEVEL", None)

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "import lumibot; "
                "print('credentials_loaded=' + str('lumibot.credentials' in sys.modules))"
            ),
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    assert f"LumiBot v" in result.stdout
    assert "starting" in result.stdout
    assert "credentials_loaded=False" in result.stdout


def test_top_level_error_import_defers_logging_setup():
    env = os.environ.copy()
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env["LUMIBOT_LOG_LEVEL"] = "ERROR"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "import lumibot; "
                "print('logging_loaded=' + str('logging' in sys.modules)); "
                "print('credentials_loaded=' + str('lumibot.credentials' in sys.modules))"
            ),
        ],
        check=True,
        capture_output=True,
        env=env,
        text=True,
    )

    assert "LumiBot v" not in result.stdout
    assert "logging_loaded=False" in result.stdout
    assert "credentials_loaded=False" in result.stdout


def test_legacy_star_import_exports_resolve():
    tools_ns = {}
    exec("from lumibot.tools import *", tools_ns)
    for name in ("Decimal", "np", "pd", "parse_symbol", "time", "get_logger", "YahooHelper"):
        assert name in tools_ns
    assert tools_ns["pd"].__name__ == "pandas"
    assert tools_ns["np"].__name__ == "numpy"
    assert tools_ns["time"].__name__ == "time"

    entities_ns = {}
    exec("from lumibot.entities import *", entities_ns)
    for name in ("Asset", "Order", "Position", "Data"):
        assert name in entities_ns

    backtesting_ns = {}
    exec("from lumibot.backtesting import *", backtesting_ns)
    for name in ("BacktestingBroker", "PandasDataBacktesting", "YahooDataBacktesting"):
        assert name in backtesting_ns


def test_lazy_module_proxies_preserve_module_identity():
    from lumibot.backtesting import backtesting_broker
    from lumibot.entities import bars, data
    from lumibot.strategies import strategy, strategy_executor
    from lumibot.tools import helpers
    from lumibot.tools import ibkr_helper, thetadata_helper

    for value in (
        helpers.pd,
        ibkr_helper.pd,
        thetadata_helper.pd,
        thetadata_helper.mcal,
        thetadata_helper.requests,
        data.pd,
        data.np,
        bars.pd,
        bars.np,
        backtesting_broker.pd,
        backtesting_broker.np,
        strategy.pd,
        strategy.np,
        strategy_executor.pd,
    ):
        assert inspect.ismodule(value)


def test_lazy_module_patch_teardown_forwards_deletion():
    targets = [
        "lumibot.tools.thetadata_helper.requests._lumibot_patch_probe",
        "lumibot.brokers.tradier.requests._lumibot_patch_probe",
        "lumibot.tools.projectx_helpers.requests._lumibot_patch_probe",
        "lumibot.backtesting.routed_backtesting.pd._lumibot_patch_probe",
        "lumibot.backtesting.databento_backtesting_pandas.pd._lumibot_patch_probe",
        "lumibot.data_sources.databento_data_pandas.pd._lumibot_patch_probe",
        "lumibot.indicators.indicators.pd._lumibot_patch_probe",
    ]

    for target in targets:
        with patch(target, object(), create=True):
            pass


def test_lazy_module_patch_teardown_restores_existing_attributes():
    from lumibot.brokers import tradier
    from lumibot.tools import projectx_helpers, thetadata_helper

    target_pairs = [
        (thetadata_helper.requests, "lumibot.tools.thetadata_helper.requests.get"),
        (tradier.requests, "lumibot.brokers.tradier.requests.get"),
        (projectx_helpers.requests, "lumibot.tools.projectx_helpers.requests.get"),
    ]

    for proxy, target in target_pairs:
        original = proxy.get
        with patch(target) as mocked:
            assert proxy.get is mocked
        assert proxy.get is original


def test_lazy_agent_builtin_module_supports_string_patches(monkeypatch):
    import lumibot.components as components
    import lumibot.components.agents as agents

    monkeypatch.delitem(components.__dict__, "agents", raising=False)
    monkeypatch.delitem(agents.__dict__, "builtins", raising=False)
    monkeypatch.delitem(sys.modules, "lumibot.components.agents.builtins", raising=False)

    with patch("lumibot.components.agents.builtins.requests.get") as mocked:
        from lumibot.components.agents import builtins

        assert components.agents is agents
        assert builtins.requests.get is mocked


def test_legacy_entities_package_alias_resolves_submodules():
    import lumibot  # noqa: F401
    import entities
    from entities import asset as asset_module
    from entities.asset import Asset
    from entities.order import Order

    assert importlib.util.find_spec("entities.asset") is not None
    assert importlib.util.find_spec("entities.order") is not None
    assert importlib.reload(entities) is entities
    assert importlib.reload(asset_module).Asset is Asset
    assert entities.Asset is Asset
    assert entities.Order is Order
    assert Asset("SPY").symbol == "SPY"
    assert Order.OrderType.MARKET.value == "market"
