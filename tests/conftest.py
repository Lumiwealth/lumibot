"""
Pytest configuration and fixtures for LumiBot tests.
Includes global cleanup for APScheduler instances to prevent CI hangs.
"""

import pytest
import gc
import atexit
import threading
import os
import json
from pathlib import Path
from dotenv import load_dotenv
from collections import defaultdict


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

# Ensure working directory is set to project root for tests
# This fixes issues with ConfigsHelper and other path-dependent code
original_cwd = os.getcwd()
if os.getcwd() != str(project_root):
    os.chdir(project_root)
    print(f"Changed working directory to: {project_root}")


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
