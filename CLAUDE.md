# CLAUDE.md - AI Assistant Instructions for LumiBot

## Quick Start

**First, read these files:**
1. `BACKTESTING_ARCHITECTURE.md` - Understand the backtesting data flow
2. `AGENTS.md` - Critical rules for ThetaData (DO NOT SKIP)

## Project Overview

LumiBot is a trading and backtesting framework supporting multiple data sources (Yahoo, ThetaData, Polygon) and brokers (Alpaca, Interactive Brokers, Tradier, etc.).

## Key Locations

| What | Where |
|------|-------|
| LumiBot library | `/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/` |
| Strategy Library | `/Users/robertgrzesik/Documents/Development/Strategy Library/` |
| Demo strategies | `/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/` |
| Environment config | `Demos/.env` for strategies, `lumibot/.env` for library |
| Backtest logs | `/Users/robertgrzesik/Documents/Development/Strategy Library/logs/` |

## Critical Rules

### ThetaData Rules (MUST FOLLOW)

1. **NEVER run ThetaTerminal locally** - It will kill production connections
2. **Only use Data Downloader** at `http://44.192.43.146:8080`
3. **Always compare ThetaData vs Yahoo** - Yahoo is the gold standard for split-adjusted prices
4. See `AGENTS.md` for complete rules

### Data Source Selection

The `BACKTESTING_DATA_SOURCE` env var **OVERRIDES** explicit code:
```bash
# In .env file
BACKTESTING_DATA_SOURCE=thetadata  # Uses ThetaData regardless of code
BACKTESTING_DATA_SOURCE=yahoo      # Uses Yahoo regardless of code
BACKTESTING_DATA_SOURCE=none       # Uses whatever class the code specifies
```

### Cache Management

If seeing wrong/stale data:
1. Bump `LUMIBOT_CACHE_S3_VERSION` (e.g., v5 → v6)
2. Clear local cache: `rm -rf ~/Library/Caches/lumibot/`

## Common Tasks

### Run a Backtest

```bash
cd "/Users/robertgrzesik/Documents/Development/Strategy Library/Demos"
python3 "TQQQ 200-Day MA.py"
```

### Compare Yahoo vs ThetaData

1. Edit `Demos/.env`:
   - Set `BACKTESTING_DATA_SOURCE=yahoo`
2. Run backtest, note results
3. Edit `Demos/.env`:
   - Set `BACKTESTING_DATA_SOURCE=thetadata`
4. Run backtest, compare results
5. Results should match within ~1-2%

### Check Backtest Results

```bash
ls -la "/Users/robertgrzesik/Documents/Development/Strategy Library/logs/" | grep TQQQ | tail -10
```

Look at `*_tearsheet.csv` for CAGR and metrics.

## Known Issues & Fixes

### ✅ ThetaData Split Adjustment (FIXED - Nov 28, 2025)

**Status:** FIXED - Split handling now correct

**Root cause:** The `_apply_corporate_actions_to_frame()` function was being called 26+ times per backtest without any idempotency check, causing split adjustments to be applied multiple times.

**Fix applied:**
1. Added idempotency check at start of `_apply_corporate_actions_to_frame()` - checks for `_split_adjusted` column marker
2. Added marker at end of function after successful adjustment
3. Cache version bumped to v7

### ✅ ThetaData Dividend Split Adjustment (FIXED - Nov 28, 2025)

**Status:** FIXED - 17/21 dividends now match Yahoo within 5%

**Root causes found:**
1. `_update_cash_with_dividends()` was called 3 times per day without idempotency
2. ThetaData dividend amounts were UNADJUSTED for splits
3. ThetaData returned duplicate dividends for same ex_date (e.g., 2019-03-20 appeared 4x)
4. ThetaData returned special distributions with `less_amount > 0` (e.g., 2015-07-02)

**Fixes applied:**
1. Added `_dividends_applied_tracker` in `_strategy.py` to prevent multiple applications
2. Added split adjustment to `get_yesterday_dividends()` in `thetadata_backtesting_pandas.py`
3. Added deduplication by ex_date in `_normalize_dividend_events()`
4. Added filter for `less_amount > 0` to exclude special distributions

**Verified split adjustment:**
- ThetaData cumulative factor for 2014 dividends: 48x (2×3×2×2×2)
- After adjustment: $0.01182 raw → $0.000246 adjusted ≈ Yahoo's $0.000250 ✓

**Current results:** ~47% CAGR with ThetaData vs ~42% with Yahoo (gap due to phantom dividends)

### ⚠️ ThetaData Phantom Dividend (KNOWN ISSUE - Reported to ThetaData)

**Status:** KNOWN DATA QUALITY ISSUE - Reported to ThetaData support team

| Date | ThetaData | Yahoo | Status |
|------|-----------|-------|--------|
| 2014-09-18 | $0.41 raw | None | ⚠️ PHANTOM - main cause of CAGR gap |
| 2015-07-02 | $1.22 raw | None | ✅ FILTERED (less_amount=22.93) |
| 2020-12-23 | $0.000283 | None | ⚠️ PHANTOM |
| 2021-12-23 | $0.000119 | None | ⚠️ PHANTOM |

**Root cause:** ThetaData phantom dividends are DATA ERRORS in the SIP feed, not Return of Capital (ROC) distributions. Confirmed via Perplexity research - these amounts don't appear in any other financial database (Yahoo, Bloomberg, SEC filings).

**Workaround options:**
1. Use `BACKTESTING_DATA_SOURCE=yahoo` for dividend-sensitive strategies
2. Wait for ThetaData to fix the data quality issue
3. Accept ~5% CAGR gap as known ThetaData limitation

**Key files:**
- `lumibot/tools/thetadata_helper.py` - `_apply_corporate_actions_to_frame()`, `_normalize_dividend_events()`
- `lumibot/backtesting/thetadata_backtesting_pandas.py` - `get_yesterday_dividends()`
- `lumibot/strategies/_strategy.py` - `_update_cash_with_dividends()`

### Cache Version Mismatch

Always ensure `.env` files have matching cache versions:
- `lumibot/.env`
- `Demos/.env`

## Testing Checklist for Data Source Changes

1. Run TQQQ 200-Day MA with Yahoo (2013-2025) → expect ~30-45% CAGR
2. Run same strategy with ThetaData → should match Yahoo within ~5%
3. Check for anomalous daily returns (>50% gain/loss indicates split issue)
4. Compare specific prices around split dates (esp. Jan 13, 2022 2:1 forward split)

## Architecture Quick Reference

```
Strategy.backtest()
    │
    ▼
Data Source Selection (env var overrides code)
    │
    ├── Yahoo: yfinance → split-adjusted prices
    ├── ThetaData: Data Downloader → split-adjusted prices
    └── Polygon: Polygon API → handles splits in cache validation
    │
    ▼
BacktestingBroker (simulates trades)
    │
    ▼
Results (tearsheet, trades, logs)
```

See `BACKTESTING_ARCHITECTURE.md` for detailed data flow diagrams.
