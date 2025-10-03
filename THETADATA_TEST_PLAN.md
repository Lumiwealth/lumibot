# ThetaData Comprehensive Test Plan

## Critical Requirements
- **Price accuracy**: < 1¢ for stocks, < 5¢ for options, < 50¢ for indexes
- **Timestamp accuracy**: Exact to the second
- **Bar alignment**: Market open spike must be at 9:30:00 exactly
- **Data completeness**: No missing bars, no gaps
- **Volume accuracy**: Should match within 10% (different feeds may filter differently)

## Test Categories

### 1. Timestamp Verification (CRITICAL)
**Why**: The +1 minute bug we just fixed - need to verify it's fixed everywhere

#### Test 1.1: Full Day Timestamp Scan
- Load full trading day (9:30-16:00)
- Verify EVERY bar:
  - Timestamp is exactly 60 seconds apart
  - Bar labeled 9:30:00 contains market open spike (volume > 100k for liquid stocks)
  - Bar labeled 9:31:00 contains post-open data (not the spike)
  - No gaps in timestamps
  - No duplicate timestamps
- Compare bar count: ThetaData vs Polygon (should be identical)
- **Symbols**: AMZN, AAPL, SPY, TSLA (high liquidity)
- **Date**: Recent trading day

#### Test 1.2: Multiple Days Consistency
- Test 5 different trading days
- Verify timestamp pattern is consistent
- Check Monday vs Friday (different market dynamics)
- **Symbols**: SPY (most liquid)
- **Dates**: 5 random recent trading days

#### Test 1.3: DST Transition
- Test days before/after DST change
- Verify times stay in Eastern Time correctly
- **Dates**: March DST spring forward, November DST fall back

#### Test 1.4: Bar Boundary Precision
- Verify bar labeled 9:30:00 contains trades from [9:30:00.000, 9:30:59.999]
- Verify bar labeled 9:31:00 contains trades from [9:31:00.000, 9:31:59.999]
- Check first trade in bar matches open price
- Check last trade in bar matches close price

### 2. Price Accuracy Tests

#### Test 2.1: Full Day Price Comparison
- Compare EVERY bar throughout the day (9:30-16:00)
- Calculate statistics:
  - Mean difference
  - Max difference
  - Standard deviation
  - 95th percentile difference
- Plot difference over time (check for systematic drift)
- **Symbols**: AMZN, AAPL, MSFT, GOOGL, TSLA, SPY
- **Tolerance**: Mean < 0.5¢, Max < 1¢

#### Test 2.2: Different Price Ranges
- **Low-priced** ($5-$25): F, SOFI, NIO
- **Mid-priced** ($50-$200): AMD, NVDA, DIS
- **High-priced** ($500+): BRK.A, GOOG, AMZN (pre-split if available)
- **Penny stocks** (<$5): High risk for rounding errors
- Verify tolerance scales appropriately

#### Test 2.3: OHLC Consistency
For every bar, verify:
- High >= Open
- High >= Close
- High >= Low
- Low <= Open
- Low <= Close
- All prices > 0
- Prices are reasonable (no outliers)

#### Test 2.4: Different Market Conditions
- **High volatility day**: Check during major news event
- **Low volatility day**: Check during quiet summer trading
- **Gap day**: Stock gaps up/down at open
- **Trending day**: Strong uptrend or downtrend
- **Choppy day**: High intraday volatility

### 3. Options Testing

#### Test 3.1: Strike Ladder
For a single expiration, test:
- 5 strikes ITM (calls)
- 1 strike ATM (calls)
- 5 strikes OTM (calls)
- 5 strikes ITM (puts)
- 1 strike ATM (puts)
- 5 strikes OTM (puts)
- Verify bid/ask spreads are reasonable
- Verify volume data matches
- **Symbol**: SPY (most liquid options)

#### Test 3.2: Expiration Dates
- 0 DTE (day of expiration)
- 1-7 DTE (weekly options)
- 30-60 DTE (monthly options)
- 90+ DTE (quarterly options)
- Verify pricing accuracy for each
- **Symbol**: SPY, AAPL

#### Test 3.3: Options Chain Completeness
- Verify all expirations returned
- Verify all strikes returned for each expiration
- Compare chain completeness: ThetaData vs Polygon
- Check for missing contracts

#### Test 3.4: Zero Volume Options
- Find options with 0 volume
- Verify pricing still works
- Check bid/ask spread handling
- **Symbol**: Illiquid stock options

### 4. Index Testing

#### Test 4.1: Major Indexes
- SPX (S&P 500)
- NDX (NASDAQ 100)
- DJI (Dow Jones)
- RUT (Russell 2000)
- VIX (Volatility Index)
- Verify calculation methodology matches

#### Test 4.2: Index Options
- SPX options (cash-settled)
- Compare with SPY options (equity-settled)
- Verify settlement prices

### 5. Data Completeness Tests

#### Test 5.1: No Missing Bars
- Load full day
- Check for gaps in timestamps
- Verify 390 bars for RTH (9:30-16:00)
- Compare bar count between providers

#### Test 5.2: Extended Hours
- Pre-market data (4:00-9:30)
- After-hours data (16:00-20:00)
- Verify timestamps are correct
- **Note**: Test both with and without extended hours

#### Test 5.3: Historical Depth
- Test going back 1 month
- Test going back 6 months
- Test going back 1 year
- Verify data quality doesn't degrade
- Check for survivorship bias

### 6. Edge Cases

#### Test 6.1: Market Holidays
- Verify no data returned for:
  - New Year's Day
  - MLK Day
  - Presidents Day
  - Good Friday
  - Memorial Day
  - Independence Day
  - Labor Day
  - Thanksgiving
  - Christmas
- Verify error handling is graceful

#### Test 6.2: Weekends
- Verify no data returned for Saturdays
- Verify no data returned for Sundays
- Check Friday→Monday transition

#### Test 6.3: Early Close Days
- Test day before Thanksgiving (13:00 close)
- Test Christmas Eve (if early close)
- Verify last bar is at correct time

#### Test 6.4: Trading Halts
- Find day with known trading halt
- Verify data handling during halt
- Check for gaps or missing bars

#### Test 6.5: Stock Splits
- Find stock with recent split
- Verify prices are split-adjusted
- Check historical data consistency

### 7. Timeframe Tests

#### Test 7.1: Multiple Timeframes
For same symbol and date, test:
- 1-minute bars
- 5-minute bars (aggregate of 5x 1-min bars)
- 15-minute bars (aggregate of 15x 1-min bars)
- 1-hour bars (aggregate of 60x 1-min bars)
- Daily bars (aggregate of full day)

Verify aggregation:
- Open of period = open of first bar
- Close of period = close of last bar
- High of period = max of all bar highs
- Low of period = min of all bar lows
- Volume of period = sum of all bar volumes

### 8. Performance Tests

#### Test 8.1: Multi-Day Backtest
- Run backtest across 30 trading days
- Verify performance is acceptable (< 5 min)
- Check memory usage
- Verify cache is working

#### Test 8.2: Multi-Symbol
- Test loading 10 symbols simultaneously
- Check for race conditions
- Verify API rate limiting works

#### Test 8.3: Cache Consistency
- Load data
- Clear Python cache (not data cache)
- Load same data again
- Verify results are identical

### 9. Backtesting Integration Tests

#### Test 9.1: Order Fill Prices
- Place market orders at different times of day
- Verify fill price matches expected bar price
- Check fill logic (open vs close based on timestamp)

#### Test 9.2: Portfolio Valuation
- Run multi-day backtest
- Track portfolio value every minute
- Verify P&L calculations are correct
- Compare ThetaData vs Polygon backtest results

#### Test 9.3: Position Tracking
- Open positions
- Hold overnight
- Close positions
- Verify position values are correct throughout

### 10. Regression Tests

#### Test 10.1: Known Good Data
- Save known good dataset
- Run tests against it periodically
- Verify results don't change
- Catch regressions early

#### Test 10.2: Timestamp Bug Regression
- Specifically test the +1 minute bug fix
- Verify market open spike is at 9:30, not 9:31
- Run this test on every commit

## Test Execution Strategy

### Phase 1: Critical Tests (Must Pass)
1. Full day timestamp verification
2. Full day price comparison (all bars)
3. Market open spike verification
4. OHLC consistency checks
5. Basic options chain test
6. Basic index test

### Phase 2: Comprehensive Tests
7. Multiple days/symbols
8. All options strike ladder
9. All timeframe aggregations
10. Extended hours
11. Historical depth

### Phase 3: Edge Cases
12. Market holidays
13. Weekends
14. Early close days
15. Stock splits
16. Trading halts

### Phase 4: Performance & Integration
17. Multi-day backtests
18. Multi-symbol tests
19. Cache consistency
20. Memory/performance profiling

## Success Criteria

### Must Pass (Blocker):
- ✅ Market open spike at exactly 9:30:00
- ✅ All bars have correct timestamps (60s apart)
- ✅ No missing bars in RTH
- ✅ Price differences < 1¢ for 95% of bars
- ✅ No OHLC inconsistencies
- ✅ Options chains return data
- ✅ Index prices return data

### Should Pass (Important):
- ✅ Extended hours data available
- ✅ Multi-day backtests work
- ✅ Performance acceptable (< 5 min for 30 days)
- ✅ All timeframes aggregate correctly

### Nice to Have:
- ✅ Historical depth >1 year
- ✅ Illiquid options work
- ✅ Trading halt handling
- ✅ Stock split adjustments

## Test Implementation Priority

1. **HIGHEST PRIORITY**: Full day timestamp + price scan (catch systematic errors)
2. **HIGH**: Options strike ladder + chains (verify options work)
3. **HIGH**: Index data verification
4. **MEDIUM**: Multiple timeframes
5. **MEDIUM**: Extended hours
6. **LOW**: Edge cases (holidays, halts, splits)

## Estimated Testing Time

- Phase 1: 2-3 hours (critical path)
- Phase 2: 4-6 hours (comprehensive)
- Phase 3: 2-3 hours (edge cases)
- Phase 4: 1-2 hours (performance)

**Total**: 9-14 hours of thorough testing

## Current Status

- ✅ Basic stock test (1 bar, 1 symbol)
- ❌ Full day scan (needed)
- ❌ Multiple symbols (needed)
- ❌ Options comprehensive (needed)
- ❌ Index comprehensive (needed)
- ❌ Timeframes (needed)
- ❌ Edge cases (needed)
- ❌ Performance tests (needed)

**Completion**: ~5% of required testing done
