# ThetaData vs Polygon Price Discrepancy Investigation

## Summary
ThetaData and Polygon are returning **different OHLC prices and volumes** for the same stock at the same exact time. This is a fundamental data provider discrepancy that requires investigation.

## Test Case
- **Stock**: AMZN
- **Date**: 2024-08-01
- **Time**: 9:30 AM ET (market open)
- **Bar**: 1-minute

## Findings

### Price Discrepancy

| Data Source | Open Price | Close Price | Difference from Polygon |
|-------------|------------|-------------|------------------------|
| **ThetaData** | $189.29 | $189.60 | +$0.005 (0.0026%) |
| **Polygon** | $189.285 | $189.62 | baseline |

**Difference**: $0.005 (half a cent)

### Volume Discrepancy (CRITICAL)

| Data Source | Volume | Difference from Polygon |
|-------------|---------|------------------------|
| **ThetaData** | 1,517,215 | +498,756 (+49%) |
| **Polygon** | 1,018,459 | baseline |

**Difference**: 498,756 shares (49% MORE volume in ThetaData!)

### OHLC Comparison

| Data Source | Open | High | Low | Close | Volume |
|-------------|------|------|-----|-------|---------|
| **ThetaData** | 189.29 | 189.62 | 188.99 | 189.60 | 1,517,215 |
| **Polygon** | 189.285 | 189.62 | 189.00 | 189.62 | 1,018,459 |
| **Difference** | +0.005 | 0.00 | -0.01 | -0.02 | +498,756 |

## Root Cause Analysis

The massive volume discrepancy (49%!) suggests the data sources are fundamentally different. Possible causes:

### 1. **Exchange Coverage**
- **ThetaData** may use **consolidated tape** (all exchanges: NYSE, NASDAQ, ARCA, BATS, etc.)
- **Polygon** may use **primary exchange only** (NASDAQ for AMZN)
- This would explain both price and volume differences

### 2. **Trade Type Filters**
- Different handling of:
  - Odd lots (< 100 shares)
  - Block trades
  - Off-exchange trades
  - Dark pool trades
  - Extended hours trades (despite `rth=true`)

### 3. **Data Feed Type**
- **ThetaData** might be using **SIP (Securities Information Processor)** data
- **Polygon** might be using **direct exchange feeds**
- Or vice versa

### 4. **Aggregation Method**
- Different bar aggregation algorithms
- Different rounding precision
- Different handling of simultaneous trades

## API Configuration

### ThetaData Query
```
GET http://127.0.0.1:25510/hist/stock/ohlc
?root=AMZN
&start_date=20240801
&end_date=20240801
&ivl=60000
&rth=true
```

**Response**:
```json
{
  "open": 189.29,
  "high": 189.62,
  "low": 188.99,
  "close": 189.6,
  "volume": 1517215
}
```

### Polygon Query
```python
client.get_aggs(
    ticker='AMZN',
    multiplier=1,
    timespan='minute',
    from_='2024-08-01',
    to='2024-08-01'
)
```

**Response**:
```python
{
  "open": 189.285,
  "high": 189.62,
  "low": 189.0,
  "close": 189.62,
  "volume": 1018459.0
}
```

## Attempted Fixes

### 1. ✅ Regular Trading Hours (RTH)
- Added `rth=true` to ThetaData queries
- **Result**: No change - prices still differ

### 2. ❌ Extended Hours
- Not the issue (both using RTH now)

### 3. ❌ Timestamp Alignment
- Both queries use identical timestamps
- Both return 9:30 AM ET bar

## Next Steps

### Immediate Actions Required

1. **Contact ThetaData Support**
   - Ask what data feed they use (consolidated vs primary exchange)
   - Ask if there's a way to match Polygon's data source
   - Ask about volume calculation methodology
   - Request API parameters to control data source

2. **Contact Polygon Support**
   - Verify they're using primary exchange only
   - Ask if there's an option to use consolidated tape
   - Confirm their volume calculation method

3. **Check ThetaData API Parameters**
   - Research if there's an `exchange` parameter
   - Research if there's a `use_consolidate` parameter
   - Check if there's a `trade_type` filter

4. **Test Multiple Stocks**
   - Test 10+ different stocks (AAPL, MSFT, GOOGL, etc.)
   - Check if discrepancy is consistent
   - Check if discrepancy varies by stock liquidity

5. **Test Different Timeframes**
   - Test daily bars (should have less discrepancy)
   - Test hourly bars
   - Check if discrepancy is time-of-day dependent

### Long-term Solutions

#### Option A: Accept Small Discrepancies
- If discrepancies are < $0.01 and < 5% volume, accept them
- Document that different data providers have slight differences
- **User has rejected this** - requires ZERO tolerance

#### Option B: Force Data Source Alignment
- Figure out how to make both use identical data sources
- May require API parameter changes
- May require changing Polygon subscription tier

#### Option C: Use Single Data Source
- Pick one (Polygon is trusted baseline per user)
- Deprecate ThetaData integration
- **Not preferred** - goal is to support ThetaData

#### Option D: Use ThetaData's Polygon-Compatible Mode
- Check if ThetaData has a "Polygon-compatible" mode
- May require premium subscription

## Questions for Data Providers

### ThetaData
1. What data feed do you use for US equities? (SIP, direct exchange, consolidated?)
2. How is volume calculated? (all exchanges, primary only, with/without odd lots?)
3. What's the `rth` parameter default for stocks?
4. Is there a way to query specific exchanges only?
5. Can you provide NBBO data separately from trade data?

### Polygon
1. Do you use consolidated tape or primary exchange only?
2. How is volume calculated?
3. Is there a way to get consolidated tape data?
4. What subscription tier provides the most comprehensive data?

## Impact

### Backtesting Impact
- Different fill prices: affects P&L calculation
- Different volumes: affects liquidity assumptions
- Could cause strategy performance to differ between data sources

### Production Impact
- **CRITICAL**: Cannot deploy to production with mismatched data
- Legal/regulatory concerns if prices don't match NBBO
- Could cause incorrect trading decisions

## Conclusion

This is **not a bug in Lumibot** - this is a **fundamental data provider discrepancy**. The 49% volume difference is particularly concerning and suggests they're pulling from completely different data sources.

**Recommended Action**: Contact both data providers to understand the discrepancy and figure out how to align the data sources before proceeding with integration.
