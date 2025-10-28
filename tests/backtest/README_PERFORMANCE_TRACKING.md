# Backtest Performance Tracking

This directory includes an automatic performance tracking system that records the execution time of all backtest tests over time.

## How It Works

1. **Automatic Tracking**: The `conftest.py` file contains a pytest fixture that automatically tracks execution time for all tests in the `tests/backtest/` directory.

2. **CSV History**: Performance data is saved to `backtest_performance_history.csv` which is checked into git to track performance over time.

3. **Manual Tracking**: You can also manually record performance with more detailed information using the `performance_tracker` module.

## Data Collected

For each backtest test run, the following data is recorded:

- **timestamp**: When the test was run
- **test_name**: Name of the test function
- **data_source**: Data source used (Yahoo, Polygon, Databento, etc.)
- **trading_days**: Number of trading days in the backtest
- **execution_time_seconds**: How long the test took to run
- **git_commit**: Git commit hash (short)
- **lumibot_version**: Version of Lumibot
- **strategy_name**: Name of the strategy class
- **start_date**: Backtest start date
- **end_date**: Backtest end date
- **sleeptime**: Strategy sleep time (e.g., "1D", "1M")
- **notes**: Any additional notes

## Usage

### Automatic Tracking

Just run your tests normally. Performance will be automatically tracked:

```bash
pytest tests/backtest/test_yahoo.py
```

### Manual Tracking with Detailed Info

For more detailed tracking, you can manually record performance in your tests:

```python
import time
from tests.backtest.performance_tracker import record_backtest_performance

def test_my_backtest():
    start_time = time.time()

    backtesting_start = datetime.datetime(2023, 10, 1)
    backtesting_end = datetime.datetime(2023, 12, 31)

    # Run your backtest...
    results = run_backtest(...)

    execution_time = time.time() - start_time

    # Record with detailed information
    record_backtest_performance(
        test_name="test_my_backtest",
        data_source="Yahoo",
        execution_time_seconds=execution_time,
        trading_days=63,
        strategy_name="BuyAndHold",
        start_date=backtesting_start,
        end_date=backtesting_end,
        sleeptime="1D",
        notes="Testing with 3-month period"
    )
```

### Viewing Performance History

You can view the CSV file directly, or use the convenience functions:

```python
from tests.backtest.performance_tracker import get_recent_performance

# Get recent performance for all tests
recent = get_recent_performance(limit=20)

# Get recent performance for specific test
recent = get_recent_performance(test_name="test_yahoo_last_price", limit=10)
```

## Benefits

1. **Track Regressions**: See if tests are getting slower over time
2. **Compare Changes**: Compare performance before/after code changes
3. **Identify Bottlenecks**: Find which tests take the longest
4. **Historical Data**: Keep long-term history of test performance
5. **CI/CD Integration**: Track performance across different commits and PRs

## CSV File Location

The CSV file is located at:
```
tests/backtest/backtest_performance_history.csv
```

This file is **checked into git** so the performance history is preserved and shared across the team.
