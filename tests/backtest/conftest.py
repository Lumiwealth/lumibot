"""
Pytest configuration for backtest tests.
Automatically tracks performance of all backtest tests.
"""
import time
import pytest
from pathlib import Path

# Import the performance tracker
from .performance_tracker import record_backtest_performance


@pytest.fixture(autouse=True)
def track_backtest_performance(request):
    """Automatically track execution time for all backtest tests"""
    # Only track tests in the backtest directory
    test_file = Path(request.node.fspath)
    if test_file.parent.name != "backtest":
        yield
        return

    # Skip if test is being skipped
    if hasattr(request.node, 'get_closest_marker'):
        skip_marker = request.node.get_closest_marker('skip')
        skipif_marker = request.node.get_closest_marker('skipif')
        if skip_marker or (skipif_marker and skipif_marker.args[0]):
            yield
            return

    # Record start time
    start_time = time.time()

    # Run the test
    yield

    # Record end time
    end_time = time.time()
    execution_time = end_time - start_time

    # Only record if test passed and took more than 0.1 seconds
    if execution_time > 0.1 and request.node.rep_call.passed:
        test_name = request.node.name
        test_module = test_file.stem  # e.g., "test_yahoo", "test_polygon"

        # Try to infer data source from test module name
        data_source = "unknown"
        if "yahoo" in test_module.lower():
            data_source = "Yahoo"
        elif "polygon" in test_module.lower():
            data_source = "Polygon"
        elif "databento" in test_module.lower() or "databento" in test_name.lower():
            data_source = "Databento"
        elif "thetadata" in test_module.lower():
            data_source = "ThetaData"

        # Record the performance
        try:
            record_backtest_performance(
                test_name=test_name,
                data_source=data_source,
                execution_time_seconds=execution_time,
                notes=f"Auto-tracked from {test_module}"
            )
        except Exception as e:
            # Don't fail tests if performance tracking fails
            print(f"Warning: Could not record performance: {e}")


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Hook to store test result for access in fixture"""
    outcome = yield
    rep = outcome.get_result()
    setattr(item, f"rep_{rep.when}", rep)
