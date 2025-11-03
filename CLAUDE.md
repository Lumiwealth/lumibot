# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Lumibot is a Python backtesting and live trading library for stocks, options, futures, crypto, and forex. The same strategy code works for both backtesting and live trading, enabling seamless transitions between the two modes.

**Key Feature**: Unified codebase - strategies written once can be backtested on historical data, then deployed for live trading without code changes.

## Architecture

### Core Components

1. **Strategies** (`lumibot/strategies/`)
   - Base class: `Strategy` (exported from `_strategy.py`)
   - User strategies inherit from `Strategy` and implement lifecycle methods: `initialize()`, `on_trading_iteration()`, `before_market_opens()`, etc.
   - `StrategyExecutor` handles strategy execution and coordination with brokers

2. **Brokers** (`lumibot/brokers/`)
   - Abstract base: `Broker` class
   - Live brokers: Alpaca, Interactive Brokers, Schwab, Tradier, CCXT (crypto), etc.
   - All inherit from base `Broker` and implement order submission, position tracking, account info
   - `BacktestingBroker` simulates broker behavior during backtests

3. **Data Sources** (`lumibot/data_sources/`)
   - Abstract base: `DataSource` class
   - Live sources: Polygon, Alpaca, Yahoo, DataBento, ThetaData, etc.
   - Backtesting sources: inherit from `DataSourceBacktesting`
   - Support both Pandas and Polars dataframes (Polars for performance)

4. **Backtesting** (`lumibot/backtesting/`)
   - `BacktestingBroker`: simulates order fills, position tracking, margin calculations
   - Data-source-specific backtesting classes (e.g., `PolygonDataBacktesting`, `DataBentoDataBacktestingPolars`)
   - Handles futures margin requirements, option pricing, crypto, forex

5. **Entities** (`lumibot/entities/`)
   - Core objects: `Asset`, `Order`, `Position`, `Bar`, `Bars`, `Data`, `Quote`
   - `Asset` represents any tradable (stock/option/future/crypto/forex) with unified interface
   - `Data` and `DataPolars` wrap price/market data in Pandas or Polars format

6. **Traders** (`lumibot/traders/`)
   - `Trader` orchestrates multiple strategies and broker connections
   - Handles multi-strategy execution and lifecycle management

### Dual-Mode Design Pattern

The library achieves backtesting/live trading duality by:
- Strategy code calls broker methods via unified interface (`self.submit_order()`, `self.get_positions()`, etc.)
- In backtesting: `BacktestingBroker` simulates fills using historical data
- In live trading: real broker implementations execute actual orders
- Data sources similarly abstract historical vs. real-time data fetching

### Backward Compatibility

`lumibot/__init__.py` maintains backward compatibility by aliasing `lumibot.entities` to legacy `entities` module name in `sys.modules` for older code/docs.

## Development Commands

### Setup
```bash
pip install -r requirements_dev.txt  # Install dev dependencies
pip install -e .                      # Install lumibot in editable mode
```

### Testing
```bash
pytest                                # Run all tests
pytest tests/test_asset.py            # Run specific test file
pytest tests/backtest/               # Run backtest tests only
pytest --cov                          # Run tests with coverage report
```

### Coverage Analysis
```bash
coverage run                          # Run tests with coverage tracking
coverage report                       # Show coverage in terminal
coverage html                         # Generate HTML coverage report (htmlcov/index.html)
```

Or combined:
```bash
coverage run; coverage report; coverage html
```

### Code Quality
```bash
ruff check .                          # Lint with ruff (Flake8 + isort combined)
ruff format .                         # Format code with ruff
```

Ruff configuration in `pyproject.toml`:
- Line length: 120
- Includes: pycodestyle, pyflakes, bugbear (security), pyupgrade, isort
- Target: Python 3.8+ (but project requires Python 3.10+)

### Running Example Strategies
```bash
python -m lumibot.example_strategies.stock_buy_and_hold    # Run backtest example
```

## Testing Philosophy

From `.github/copilot-instructions.md`:
- **Mission-critical code**: High test coverage required
- Always add unit tests for new functionality
- Tests must be pytest-compatible
- Tests should be well-documented and follow best practices

Test structure:
- `tests/` - Unit and integration tests
- `tests/backtest/` - Backtesting-specific tests
- `tests/performance/` - Performance profiling tests

## Environment Variables & Configuration

Configuration loaded via `lumibot/credentials.py` from environment:
- Data source API keys: `POLYGON_API_KEY`, etc.
- Broker credentials: varies by broker
- Backtesting params: `BACKTESTING_START`, `BACKTESTING_END`, `BACKTESTING_QUIET_LOGS`
- Features: `SHOW_PLOT`, `SHOW_TEARSHEET`, `HIDE_POSITIONS`, etc.

Use `.env` file (gitignored) for local development secrets.

## Data Source Notes

Different data sources have varying capabilities (from README):

| Data Source | Type  | OHLCV | Split Adjusted | Dividends | Returns | Dividend Adjusted Returns |
|-------------|-------|-------|----------------|-----------|---------|---------------------------|
| yahoo       | stock | Yes   | Yes            | Yes       | Yes     | Yes                       |
| alpaca      | stock | Yes   | Yes            | No        | Yes     | No                        |
| polygon     | stock | Yes   | Yes            | No        | Yes     | No                        |
| tradier     | stock | Yes   | Yes            | No        | Yes     | No                        |
| pandas*     | stock | Yes   | Yes            | Yes       | Yes     | Yes                       |

*Pandas can load CSV files in Yahoo dataframe format

## Key Implementation Details

### Futures Margin Simulation
`backtesting/backtesting_broker.py` contains `TYPICAL_FUTURES_MARGINS` dictionary with margin requirements for common futures (MES, ES, NQ, CL, GC, etc.) used in backtesting to simulate margin deduction/release.

### Polars Performance Optimization
Recent work focuses on Polars integration for performance:
- `data_sources/polars_mixin.py` - Polars utilities
- `databento_backtesting_polars.py` - High-performance DataBento backtesting
- `entities/data_polars.py` - Polars data wrapper

### Strategy Lifecycle Methods
When implementing strategies, override these methods in `Strategy` subclass:
- `initialize()` - Setup before trading starts
- `on_trading_iteration()` - Main trading logic (called each iteration)
- `before_market_opens()` - Pre-market preparation
- `before_market_closes()` - End-of-day actions
- `on_abrupt_closing()` - Cleanup on unexpected shutdown
- `trace_stats()` - Custom metrics tracking

### Asset Types
The `Asset` class represents all tradable instruments with `asset_type`:
- `'stock'` - Equities
- `'option'` - Options (with strike, expiration, right)
- `'future'` - Futures contracts (with expiration)
- `'forex'` - Foreign exchange pairs
- `'crypto'` - Cryptocurrencies

## Git Workflow

Main branch: `dev` (not `master`)

When creating pull requests, target the `dev` branch.

Branch workflow (from README):
```bash
# Create feature branch
git checkout -b my-feature
git fetch origin
git merge origin/dev

# Commit work
git add .
git commit -m "description"
git push -u origin my-feature

# Rebase if dev progressed
git checkout dev
git fetch origin
git merge origin/dev
git checkout my-feature
git rebase dev
git push --force-with-lease origin my-feature
```

## Important Notes

- Python 3.10+ required (enforced in `setup.py` and checked in `__init__.py`)
- NumPy 2.x compatible (requires numpy>=1.20.0, scipy>=1.14.0, pyarrow>=15.0.0)
- Pandas 2.2.0+ required
- Cache folder: Uses `LUMIBOT_CACHE_FOLDER` from constants for parquet caching
- Remote cache: AWS S3 mirroring available (see `docs/remote_cache.md`)
- Logging: Custom logger via `lumibot.tools.lumibot_logger.get_logger()`
- JSON serialization: `SafeJSONEncoder` handles Lumibot objects, dates, Decimals

## Dependencies

Core dependencies (from `setup.py`):
- Market data: polygon-api-client, alpaca-py, yfinance, databento
- Brokers: ibapi, ccxt, schwab-py, lumiwealth-tradier
- Data processing: pandas, polars, numpy, pyarrow
- Analysis: quantstats-lumi, pandas-ta-classic, scipy
- Visualization: matplotlib, plotly
- Scheduling: apscheduler
- Database: sqlalchemy, duckdb, psycopg2-binary
