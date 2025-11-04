# Proposed Fix: Add GC (Gold) Futures Support to futures_roll.py

## Problem Summary

GC (Gold) futures contracts are not properly supported in `lumibot/tools/futures_roll.py`, causing incorrect contract selection and severe data quality issues (61.4% of data is unusable).

**Root Cause**: GC trades bi-monthly (Feb, Apr, Jun, Aug, Oct, Dec) but lumibot's fallback logic assumes quarterly contracts (Mar, Jun, Sep, Dec).

**Impact**: Only 5 out of 12 months (41.7%) select the correct contract, resulting in trading non-existent or low-volume contracts.

## CME Official Specifications

From CME Group contract specs:
- **Trading Months**: Feb (G), Apr (J), Jun (M), Aug (Q), Oct (V), Dec (Z)
- **Last Trading Day**: 3rd last business day of contract month at 12:30 CT
- **Roll Convention**: Typically 3 business days before last trading day

## Proposed Solution

### Option 1: Add GC-Specific Logic (Recommended)

Add a dedicated function for bi-monthly contract determination and integrate it into `determine_contract_year_month()`.

#### Code Changes to `lumibot/tools/futures_roll.py`

**Step 1**: Add bi-monthly helper function after `_advance_quarter()` (after line 91):

```python
def _advance_bimonthly_gc(current_month: int, current_year: int) -> YearMonth:
    """Advance to next GC (Gold) bi-monthly contract: Feb, Apr, Jun, Aug, Oct, Dec."""
    gc_months = [2, 4, 6, 8, 10, 12]

    # Find next month in sequence
    next_months = [m for m in gc_months if m > current_month]

    if next_months:
        return current_year, next_months[0]
    else:
        # Wrap to next year's February
        return current_year + 1, 2


def _determine_gc_contract(reference_date: datetime) -> YearMonth:
    """
    Determine GC (Gold) futures contract for a given reference date.

    GC trades bi-monthly: Feb, Apr, Jun, Aug, Oct, Dec
    Last trading day: 3rd last business day of contract month
    Roll: ~3 business days before last trading day
    """
    gc_months = [2, 4, 6, 8, 10, 12]
    year = reference_date.year
    month = reference_date.month
    day = reference_date.day

    # Find the current or next bi-monthly contract month
    candidates = [m for m in gc_months if m >= month]

    if candidates:
        target_month = candidates[0]
        target_year = year
    else:
        # Wrap to next year
        target_month = gc_months[0]
        target_year = year + 1

    # Check if we need to roll to next contract
    # For simplicity, roll on or after the 20th of the month before expiration
    # More precise: should check 3rd last business day minus 3 business days
    if month == target_month and day >= 20:
        # Roll to next contract
        target_year, target_month = _advance_bimonthly_gc(target_month, target_year)

    return target_year, target_month
```

**Step 2**: Modify `determine_contract_year_month()` to handle GC (line 117-143):

```python
def determine_contract_year_month(symbol: str, reference_date: Optional[datetime] = None) -> YearMonth:
    ref = _normalize_reference_date(reference_date)
    symbol_upper = symbol.upper()
    rule = ROLL_RULES.get(symbol_upper)

    # Special handling for GC (Gold) - bi-monthly contracts
    if symbol_upper == "GC":
        return _determine_gc_contract(ref)

    quarter_months = [3, 6, 9, 12]
    year = ref.year
    month = ref.month

    if rule is None:
        return _legacy_mid_month(ref)

    # ... rest of existing logic for quarterly contracts
```

**Step 3**: Add GC to ROLL_RULES for proper roll trigger calculation (line 35-38):

```python
ROLL_RULES: Dict[str, RollRule] = {
    symbol: RollRule(offset_business_days=8, anchor="third_friday")
    for symbol in {"ES", "MES", "NQ", "MNQ", "YM", "MYM"}
}

# Gold futures use different roll schedule (bi-monthly, not quarterly)
# Roll ~3 business days before last trading day (3rd last business day of month)
# For implementation simplicity, we use mid-month anchor with appropriate offset
ROLL_RULES["GC"] = RollRule(offset_business_days=10, anchor="mid_month")
```

### Option 2: Enhanced RollRule Framework

Extend RollRule to support different contract cycles (quarterly, bi-monthly, monthly).

**Pros**: More scalable for future commodities
**Cons**: More complex, requires refactoring existing logic

### Option 3: Third-Last Business Day Calculation (Most Accurate)

Implement precise CME rule using actual last business day calculation:

```python
def _third_last_business_day(year: int, month: int) -> datetime:
    """Calculate 3rd last business day of month (CME GC last trading day)."""
    # Get last day of month
    if month == 12:
        last_day = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = datetime(year, month + 1, 1) - timedelta(days=1)

    last_day = _to_timezone(last_day)

    # Walk backwards to find last business day
    while last_day.weekday() >= 5:  # Saturday=5, Sunday=6
        last_day -= timedelta(days=1)

    # Count back 2 more business days
    count = 0
    while count < 2:
        last_day -= timedelta(days=1)
        if last_day.weekday() < 5:
            count += 1

    return last_day


def _calculate_roll_trigger(year: int, month: int, rule: RollRule) -> datetime:
    if rule.anchor == "third_friday":
        anchor = _third_friday(year, month)
    elif rule.anchor == "third_last_business_day":
        anchor = _third_last_business_day(year, month)
    else:
        anchor = _to_timezone(datetime(year, month, 15))

    if rule.offset_business_days <= 0:
        return anchor
    return _subtract_business_days(anchor, rule.offset_business_days)
```

Then set:
```python
ROLL_RULES["GC"] = RollRule(offset_business_days=3, anchor="third_last_business_day")
```

## Recommended Implementation

**Option 1** with **Option 3** combined:

1. Add `_third_last_business_day()` and modify `_calculate_roll_trigger()` to support it
2. Add `_determine_gc_contract()` for bi-monthly contract selection
3. Add special case for GC in `determine_contract_year_month()`
4. Add GC to ROLL_RULES with accurate anchor

This provides:
- ✅ Accurate CME-compliant roll dates
- ✅ Correct bi-monthly contract selection
- ✅ Minimal changes to existing code
- ✅ Framework for other commodities with special rules

## Testing Required

After implementing the fix:

1. Run `calculate_cme_gc_schedule.py` to verify correct schedule generation
2. Re-download GC data with corrected logic:
   ```bash
   python databento_to_buildalpha.py --symbol GC --start 20250101 --end 20251031
   ```
3. Run data quality analysis:
   ```bash
   python analyze_contract_rollover.py databento_exports/GC/.../GC_..._1m_EST.csv
   ```
4. Verify >95% of weeks now show GOOD quality (>800 bars/day)
5. Compare corrected data against known benchmarks

## Expected Results After Fix

- **Good weeks**: Should increase from 38.6% to >95%
- **Contract selection**: 12 out of 12 months correct (100%)
- **Data quality**: Average bars per day should be ~1140 consistently
- **Backtesting**: Reliable Gold futures strategies

## Files Modified

1. `/Users/marvin/repos/lumibot/lumibot/tools/futures_roll.py` - Add GC support
2. Test with `/Users/marvin/repos/lumibot/calculate_cme_gc_schedule.py` - Verify fix

## References

- CME Gold Futures Specs: https://www.cmegroup.com/markets/metals/precious/gold.contractSpecs.html
- Root Cause Report: `/Users/marvin/repos/lumibot/GC_DATA_QUALITY_ROOT_CAUSE_REPORT.md`
- Investigation Script: `/Users/marvin/repos/lumibot/investigate_gc_contracts.py`
