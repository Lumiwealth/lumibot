"""
Pytest configuration and fixtures for LumiBot tests.
Includes global cleanup for APScheduler instances to prevent CI hangs.
"""

import atexit
import gc
import json
import os
import threading
from collections import defaultdict
from pathlib import Path
from unittest import mock

import pytest
from dotenv import load_dotenv

_TEST_DURATIONS_SECONDS = defaultdict(float)

# Prevent Alpaca TradingStream from opening websocket connections during test collection.
# Under xdist this can inadvertently trigger many concurrent connections and rate limiting.
try:
    from alpaca.trading.stream import TradingStream as _AlpacaTradingStream
except Exception:
    _AlpacaTradingStream = None
    _ALPACA_TRADING_STREAM_ORIGINAL_RUN = None
    _ALPACA_TRADING_STREAM_ORIGINAL_CLOSE = None
    _ALPACA_TRADING_STREAM_ORIGINAL_RUN_FOREVER = None
else:
    _ALPACA_TRADING_STREAM_ORIGINAL_RUN = _AlpacaTradingStream.run
    _ALPACA_TRADING_STREAM_ORIGINAL_CLOSE = getattr(_AlpacaTradingStream, "close", None)
    _ALPACA_TRADING_STREAM_ORIGINAL_RUN_FOREVER = getattr(_AlpacaTradingStream, "_run_forever", None)

    async def _alpaca_trading_stream_run_noop(self, *args, **kwargs):
        return None

    async def _alpaca_trading_stream_run_forever_noop(self, *args, **kwargs):
        return None

    async def _alpaca_trading_stream_close_noop(self, *args, **kwargs):
        return None

    _AlpacaTradingStream.run = _alpaca_trading_stream_run_noop
    if _ALPACA_TRADING_STREAM_ORIGINAL_RUN_FOREVER is not None:
        _AlpacaTradingStream._run_forever = _alpaca_trading_stream_run_forever_noop
    if _ALPACA_TRADING_STREAM_ORIGINAL_CLOSE is not None:
        _AlpacaTradingStream.close = _alpaca_trading_stream_close_noop

# Load .env file at the very beginning, before any imports
# This ensures environment variables are available for all tests
project_root = Path(__file__).parent.parent
env_file = project_root / ".env"
if env_file.exists():
    load_dotenv(env_file)
    print(f"Loaded .env file from: {env_file}")
else:
    print(f"Warning: .env file not found at {env_file}")

# Tests should not be impacted by the user-facing BACKTESTING_DATA_SOURCE override.
# CI enforces this via workflow env; do the same for local runs.
os.environ["BACKTESTING_DATA_SOURCE"] = "none"

# Many tests flip `IS_BACKTESTING` on/off via direct `os.environ[...] = ...` assignment (not using
# pytest's monkeypatch), which can leak state across the suite and create order-dependent failures.
# Keep the environment stable by restoring `IS_BACKTESTING` after every test.
@pytest.fixture(autouse=True)
def _restore_is_backtesting_env():
    original = os.environ.get("IS_BACKTESTING")
    try:
        # Force a clean baseline for each test (prevents order-dependent leakage).
        os.environ.pop("IS_BACKTESTING", None)
        yield
    finally:
        if original is None:
            os.environ.pop("IS_BACKTESTING", None)
        else:
            os.environ["IS_BACKTESTING"] = original

# Ensure working directory is set to project root for tests
# This fixes issues with ConfigsHelper and other path-dependent code
original_cwd = os.getcwd()
if os.getcwd() != str(project_root):
    os.chdir(project_root)
    print(f"Changed working directory to: {project_root}")


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "ibkr: downloader-only IBKR tests that do not require Polygon or ThetaData credentials",
    )
    config.addinivalue_line(
        "markers",
        "alpaca: Alpaca paper API tests that require ALPACA_TEST_API_KEY / ALPACA_TEST_API_SECRET",
    )
    config.addinivalue_line(
        "markers",
        "polymarket: Polymarket CLOB tests that do not require Polygon or ThetaData credentials",
    )
    config.addinivalue_line(
        "markers",
        "polymarket_credentials: Polymarket CLOB tests that require authenticated credentials",
    )
    config.addinivalue_line(
        "markers",
        "polymarket_live_trading: Polymarket CLOB tests that can submit/cancel live orders",
    )


class _PatchProxy:
    def __init__(self, patches):
        self._patches = patches

    def __call__(self, *args, **kwargs):
        patcher = mock.patch(*args, **kwargs)
        patched = patcher.start()
        self._patches.append(patcher)
        return patched

    def object(self, *args, **kwargs):
        patcher = mock.patch.object(*args, **kwargs)
        patched = patcher.start()
        self._patches.append(patcher)
        return patched


class _SimpleMocker:
    Mock = mock.Mock
    MagicMock = mock.MagicMock
    call = mock.call

    def __init__(self):
        self._patches = []
        self.patch = _PatchProxy(self._patches)

    def stopall(self):
        while self._patches:
            self._patches.pop().stop()


@pytest.fixture
def mocker():
    """Small pytest-mock compatible fixture for tests that only need patch/Mock."""
    fixture = _SimpleMocker()
    try:
        yield fixture
    finally:
        fixture.stopall()


def cleanup_all_schedulers():
    """Emergency cleanup for any remaining scheduler instances"""
    try:
        # Force garbage collection to trigger __del__ methods
        gc.collect()

        # Try to find and shutdown any remaining APScheduler instances
        for obj in gc.get_objects():
            if hasattr(obj, '__class__') and 'scheduler' in str(obj.__class__).lower():
                if hasattr(obj, 'shutdown') and hasattr(obj, 'running'):
                    try:
                        if obj.running:
                            if hasattr(obj, 'remove_all_jobs'):
                                obj.remove_all_jobs()
                            obj.shutdown(wait=False)
                    except Exception:
                        pass
    except Exception:
        pass


def cleanup_all_threads():
    """Clean up any remaining threads that might be hanging"""
    try:
        # Get all active threads
        active_threads = threading.enumerate()
        main_thread = threading.main_thread()

        for thread in active_threads:
            if thread != main_thread and thread.is_alive():
                # Try to stop threads that have a stop method or event
                if hasattr(thread, 'stop'):
                    try:
                        thread.stop()
                    except Exception:
                        pass
                elif hasattr(thread, '_stop_event'):
                    try:
                        thread._stop_event.set()
                    except Exception:
                        pass
    except Exception:
        pass


def pytest_runtest_logreport(report):
    """Collect per-test runtimes (setup+call+teardown) for slow-test visibility."""
    if report.when not in {"setup", "call", "teardown"}:
        return
    try:
        _TEST_DURATIONS_SECONDS[report.nodeid] += float(report.duration or 0.0)
    except Exception:
        # Never let timing/reporting break the suite.
        return


def pytest_sessionfinish(session, exitstatus):
    """Emit a slow-test summary and persist durations for future reference.

    This is gated to CI by default to avoid noisy local output, but can be enabled locally via:
      LUMIBOT_PYTEST_REPORT_DURATIONS=1
    """
    should_report = os.environ.get("LUMIBOT_PYTEST_REPORT_DURATIONS") or os.environ.get("CI")
    if not should_report:
        return

    try:
        top_n = int(os.environ.get("LUMIBOT_PYTEST_REPORT_DURATIONS_TOP", "30"))
    except Exception:
        top_n = 30

    durations = sorted(_TEST_DURATIONS_SECONDS.items(), key=lambda kv: kv[1], reverse=True)
    if durations:
        print("\n=== Slowest tests (total setup+call+teardown) ===")
        for nodeid, seconds in durations[:top_n]:
            print(f"{seconds:8.2f}s  {nodeid}")

    output_path = os.environ.get(
        "LUMIBOT_PYTEST_DURATIONS_FILE",
        str(Path(session.config.rootpath) / ".pytest_cache" / "lumibot_pytest_durations.json"),
    )
    try:
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text(json.dumps({"exitstatus": exitstatus, "durations": durations}, indent=2))
    except Exception:
        # Never fail the suite on reporting issues.
        return


@pytest.fixture(scope="session", autouse=True)
def global_cleanup():
    """Global cleanup fixture that runs at session start and end"""

    # Cleanup before tests start
    cleanup_all_schedulers()
    cleanup_all_threads()

    yield

    # Cleanup after all tests complete
    cleanup_all_schedulers()
    cleanup_all_threads()

    # Force final garbage collection
    gc.collect()


@pytest.fixture(autouse=True)
def test_cleanup():
    """Per-test cleanup to prevent scheduler leaks between tests"""
    yield

    # Minimal cleanup to avoid CI deadlocks
    # Only force gc collection, don't do aggressive scheduler cleanup per-test
    gc.collect()


# Register cleanup functions to run on exit
atexit.register(cleanup_all_schedulers)
atexit.register(cleanup_all_threads)


@pytest.fixture
def disable_datasource_override(monkeypatch):
    """
    Fixture to disable the BACKTESTING_DATA_SOURCE environment variable override.

    Use this fixture in tests that need to test SPECIFIC data sources (Yahoo, Alpaca,
    Polygon, etc.) without being overridden by the CI environment.

    The BACKTESTING_DATA_SOURCE env var is designed to let users easily switch data sources,
    but it interferes with tests that explicitly test specific data source behavior.

    Usage:
        def test_yahoo_specific_behavior(disable_datasource_override):
            # This test will use YahooDataBacktesting as explicitly requested in code,
            # NOT whatever BACKTESTING_DATA_SOURCE is set to in the environment
            ...

    LEGACY TEST COMPATIBILITY (Aug 2023+):
    Many legacy tests were written before the BACKTESTING_DATA_SOURCE override existed.
    They expect specific data sources and will fail if overridden.
    """
    monkeypatch.setenv("BACKTESTING_DATA_SOURCE", "none")


@pytest.fixture(scope="session", autouse=True)
def _restore_alpaca_trading_stream():
    yield
    if _AlpacaTradingStream is not None and _ALPACA_TRADING_STREAM_ORIGINAL_RUN is not None:
        _AlpacaTradingStream.run = _ALPACA_TRADING_STREAM_ORIGINAL_RUN
    if _AlpacaTradingStream is not None and _ALPACA_TRADING_STREAM_ORIGINAL_CLOSE is not None:
        _AlpacaTradingStream.close = _ALPACA_TRADING_STREAM_ORIGINAL_CLOSE
    if _AlpacaTradingStream is not None and _ALPACA_TRADING_STREAM_ORIGINAL_RUN_FOREVER is not None:
        _AlpacaTradingStream._run_forever = _ALPACA_TRADING_STREAM_ORIGINAL_RUN_FOREVER


# Centralized credential validation and skipping for API-dependent tests
def _is_placeholder(value: str) -> bool:
    if value is None:
        return True
    v = str(value).strip().lower()
    if not v:
        return True
    placeholders = {
        "<your key here>",
        "<your api key>",
        "<api key>",
        "uname",
        "username",
        "password",
        "<username>",
        "<password>",
        "none",
        "null",
        "changeme",
    }
    return v in placeholders


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item: pytest.Item):
    """
    Skip API/data-provider tests only for the providers they actually require.

    Markers:
      - apitest: general external API usage
      - downloader: tests that hit remote/downloader services
      - polygon: requires Polygon credentials
      - thetadata: requires ThetaData credentials
      - ibkr: downloader-only IBKR tests; does not require Polygon/ThetaData creds
      - alpaca: Alpaca paper API tests; does not require Polygon/ThetaData creds
      - polymarket: Polymarket CLOB tests; does not require Polygon/ThetaData creds
      - polymarket_credentials: requires Polymarket wallet/CLOB credentials
      - polymarket_live_trading: requires explicit live-trading enablement

    Behavior:
      - If a test is marked with ibkr, require neither Polygon nor ThetaData.
      - If a test is marked with alpaca, require Alpaca paper credentials only.
      - If a test is marked with polygon and/or thetadata, only those
        provider credentials are required.
      - If a test has apitest/downloader but no provider-specific markers,
        require both providers (legacy behavior for mixed-provider tests).
    """
    has_apitest = item.get_closest_marker("apitest") is not None
    has_downloader = item.get_closest_marker("downloader") is not None
    if not (has_apitest or has_downloader):
        # Non-API tests are not gated
        return

    requires_polygon = item.get_closest_marker("polygon") is not None
    requires_theta = item.get_closest_marker("thetadata") is not None
    requires_ibkr = item.get_closest_marker("ibkr") is not None
    requires_alpaca = item.get_closest_marker("alpaca") is not None
    requires_polymarket = item.get_closest_marker("polymarket") is not None
    requires_polymarket_credentials = item.get_closest_marker("polymarket_credentials") is not None
    requires_polymarket_live_trading = item.get_closest_marker("polymarket_live_trading") is not None

    # Determine which providers are required
    if requires_ibkr or requires_alpaca or requires_polymarket:
        need_polygon = False
        need_theta = False
    elif requires_polygon or requires_theta:
        need_polygon = requires_polygon
        need_theta = requires_theta
    else:
        # No provider-specific markers: assume both may be used
        need_polygon = True
        need_theta = True

    missing = []

    # Validate only the required credentials
    if need_polygon:
        polygon_key = os.environ.get("POLYGON_API_KEY")
        if _is_placeholder(polygon_key):
            missing.append("POLYGON_API_KEY")

    if need_theta:
        theta_user = os.environ.get("THETADATA_USERNAME")
        theta_pass = os.environ.get("THETADATA_PASSWORD")
        if _is_placeholder(theta_user):
            missing.append("THETADATA_USERNAME")
        if _is_placeholder(theta_pass):
            missing.append("THETADATA_PASSWORD")

    if requires_alpaca:
        if _is_placeholder(os.environ.get("ALPACA_TEST_API_KEY")):
            missing.append("ALPACA_TEST_API_KEY")
        if _is_placeholder(os.environ.get("ALPACA_TEST_API_SECRET")):
            missing.append("ALPACA_TEST_API_SECRET")

    if requires_polymarket_credentials:
        if _is_placeholder(os.environ.get("POLYMARKET_PRIVATE_KEY")):
            missing.append("POLYMARKET_PRIVATE_KEY")
        if _is_placeholder(os.environ.get("POLYMARKET_WALLET_ADDRESS")):
            missing.append("POLYMARKET_WALLET_ADDRESS")

    if requires_polymarket_live_trading:
        if os.environ.get("POLYMARKET_LIVE_TRADING_ENABLED", "").strip().lower() not in {"1", "true", "yes", "on"}:
            missing.append("POLYMARKET_LIVE_TRADING_ENABLED=true")
        if _is_placeholder(os.environ.get("POLYMARKET_TEST_TOKEN_ID")):
            missing.append("POLYMARKET_TEST_TOKEN_ID")

    # Downloader-specific requirement: shared downloader API key
    # Only enforce when tests are explicitly marked with `downloader`.
    if has_downloader:
        downloader_key = os.environ.get("DATADOWNLOADER_API_KEY")
        if _is_placeholder(downloader_key):
            missing.append("DATADOWNLOADER_API_KEY")

        # Enforce the shared downloader endpoint is specified as well
        downloader_base = os.environ.get("DATADOWNLOADER_BASE_URL")
        if _is_placeholder(downloader_base):
            missing.append("DATADOWNLOADER_BASE_URL")

    if missing:
        reason = (
            "Skipping API test due to missing/placeholder credentials: "
            + ", ".join(missing)
        )
        pytest.skip(reason)
