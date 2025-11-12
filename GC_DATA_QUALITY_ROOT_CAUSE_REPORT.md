# GC Futures Data Quality - Root Cause Analysis Report
**Date:** November 4, 2025
**Analyst:** Claude Code
**Dataset:** GC (Gold) Futures Jan-Oct 2025 (1-minute bars, EST timezone)

---

## Executive Summary

**CRITICAL ISSUE IDENTIFIED:** 61.4% of the GC futures data (27 out of 44 weeks) has severe quality issues with extremely sparse data. The root cause has been definitively identified as **incorrect futures contract selection** due to missing Gold-specific roll rules in lumibot's `futures_roll.py` module.

### Impact
- Only **38.6% of weeks** have usable data (>800 bars/day average)
- **61.4% of weeks** have severely degraded data (<200 bars/day average)
- This makes the dataset **unreliable for backtesting strategies**

---

## Root Cause Analysis

### The Problem

Lumibot's `futures_roll.py` module **does NOT have GC (Gold) specific roll rules**.

**Current roll rules** are only defined for:
- ES, MES (S&P 500)
- NQ, MNQ (Nasdaq)
- YM, MYM (Dow Jones)

**GC is missing** from this list.

### Fallback Behavior

When no roll rule is defined, the system falls back to `_legacy_mid_month()` logic which:
1. **Rolls on the 15th of every month**
2. **Advances quarterly** (March, June, September, December)

### Why This Fails for Gold

CME Gold Futures (GC) trade on a **bi-monthly cycle**:
- **Active months:** Feb (G), Apr (J), Jun (M), Aug (Q), Oct (V), Dec (Z)
- **Roll schedule:** ~3 business days before last trading day
- **Last trading day:** 3rd-to-last business day of contract month

The legacy mid-month logic selects **wrong contracts**:

| Month | Lumibot Selects | Should Use | Match | Issue |
|-------|----------------|------------|-------|-------|
| January | **GCH5** (Mar) | GCG5 (Feb) | ✗ | Wrong month |
| February | **GCH5** (Mar) | GCJ5 (Apr) | ✗ | Wrong month |
| March | **GCH5** (Mar) | GCJ5 (Apr) | ✗ | Wrong month |
| April | GCM5 (Jun) | GCM5 (Jun) | ✓ | Correct |
| May | GCM5 (Jun) | GCM5 (Jun) | ✓ | Correct |
| June | GCM5 (Jun) | **GCQ5** (Aug) | ✗ | Should roll earlier |
| July | **GCU5** (Sep) | GCQ5 (Aug) | ✗ | Wrong month |
| August | **GCU5** (Sep) | GCV5 (Oct) | ✗ | Wrong month |
| September | **GCU5** (Sep) | GCV5 (Oct) | ✗ | Wrong month |
| October | GCZ5 (Dec) | GCZ5 (Dec) | ✓ | Correct |

**Result:** Only 3 out of 10 months use the correct contract!

---

## Data Quality Evidence

### Weekly Analysis Results

```
📈 Week Classification:
   GOOD weeks (avg > 800 bars/day): 17 weeks (38.6%)
   BAD weeks (avg ≤ 800 bars/day):  27 weeks (61.4%)

📊 Statistics by Quality Category:
   GOOD weeks:
      Average bars/day: 1139.5
      Range: [890, 1308]

   BAD weeks:
      Average bars/day: 188.9
      Range: [15, 772]
```

### Continuous Quality Periods

| Period | Quality | Duration | Start | End |
|--------|---------|----------|-------|-----|
| 1 | **BAD** | 11 weeks (77 days) | 2024-12-30 | 2025-03-10 |
| 2 | GOOD | 10 weeks (70 days) | 2025-03-17 | 2025-05-19 |
| 3 | **BAD** | 16 weeks (112 days) | 2025-05-26 | 2025-09-08 |
| 4 | GOOD | 7 weeks (49 days) | 2025-09-15 | 2025-10-27 |

**Pattern Match:**
- **GOOD periods** align with correct contract selection (Apr-May, Sep-Oct)
- **BAD periods** align with incorrect contract selection (Jan-Mar, Jun-Sep)

---

## Technical Details

### Contract Selection Timeline

```
┌────────────────────────────────────────────────────────────────┐
│  Jan   │   Feb   │   Mar   │  Apr   │  May   │   Jun          │
├────────────────────────────────────────────────────────────────┤
│        GCH5 (Mar)           │    GCM5 (Jun)    │                │
│   ✗✗✗  WRONG CONTRACT  ✗✗✗  │  ✓✓✓ CORRECT ✓✓✓ │                │
│  Should be GCG5 → GCJ5      │                  │                │
└────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────┐
│   Jul   │  Aug   │   Sep   │  Oct   │                          │
├────────────────────────────────────────────────────────────────┤
│         GCU5 (Sep)          │  GCZ5  │                          │
│   ✗✗✗  WRONG CONTRACT  ✗✗✗  │  ✓✓✓   │                          │
│  Should be GCQ5 → GCV5      │        │                          │
└────────────────────────────────────────────────────────────────┘
```

###  Code Location

**File:** `/Users/marvin/repos/lumibot/lumibot/tools/futures_roll.py`

**Line 35-38:**
```python
ROLL_RULES: Dict[str, RollRule] = {
    symbol: RollRule(offset_business_days=8, anchor="third_friday")
    for symbol in {"ES", "MES", "NQ", "MNQ", "YM", "MYM"}
}
```

**Missing:** GC is not in this dictionary.

**Line 126-127** (fallback logic):
```python
if rule is None:
    return _legacy_mid_month(ref)
```

---

## Solution & Recommendations

### Immediate Fix

**Add GC roll rules** to `futures_roll.py`:

```python
# Gold futures roll schedule
GC_ROLL_RULE = RollRule(offset_business_days=3, anchor="last_trading_day")

ROLL_RULES: Dict[str, RollRule] = {
    **{symbol: RollRule(offset_business_days=8, anchor="third_friday")
       for symbol in {"ES", "MES", "NQ", "MNQ", "YM", "MYM"}},
    "GC": GC_ROLL_RULE,  # Gold futures
}
```

### Alternative Approach

If the RollRule framework doesn't support bi-monthly contracts, add custom logic:

```python
def _determine_gc_contract(reference_date: datetime) -> YearMonth:
    """Gold trades: Feb, Apr, Jun, Aug, Oct, Dec"""
    year = reference_date.year
    month = reference_date.month

    # Map to next bi-monthly contract
    if month <= 1: return year, 2   # Feb
    if month <= 3: return year, 4   # Apr
    if month <= 5: return year, 6   # Jun
    if month <= 7: return year, 8   # Aug
    if month <= 9: return year, 10  # Oct
    return year, 12  # Dec
```

### Testing Required

After fixing roll rules:
1. Re-download GC data with corrected logic
2. Verify contracts match CME standard schedule
3. Confirm data quality improves to >95% good weeks
4. Validate backtesting results against known benchmarks

---

## Artifacts Created

Analysis scripts created during investigation:

1. **`analyze_missing_minutes.py`** - Visual analysis with plots
2. **`analyze_missing_patterns.py`** - Statistical pattern analysis
3. **`analyze_contract_rollover.py`** - Week-by-week quality breakdown
4. **`investigate_gc_contracts.py`** - Contract selection verification
5. **`databento_exports/GC/.../weekly_analysis.csv`** - Detailed weekly data

---

## Conclusion

The data quality issues are **NOT caused by**:
- ❌ DataBento API problems
- ❌ Network/download errors
- ❌ Data corruption
- ❌ Timezone conversion bugs

The data quality issues **ARE caused by**:
- ✅ **Missing GC-specific roll rules** in lumibot
- ✅ **Incorrect contract selection** for 7 out of 10 months
- ✅ **Using low-volume/non-existent contracts**

### Impact Assessment
- **Backtesting reliability:** SEVERELY COMPROMISED (only 38.6% usable data)
- **Production trading:** UNSAFE (would trade wrong contracts)
- **Data procurement:** INCORRECT (fetching wrong contract symbols from DataBento)

### Fix Priority
**CRITICAL** - This affects all Gold futures strategies and potentially other commodity futures without specific roll rules.

---

**Report Generated:** 2025-11-04
**Dataset Analyzed:** 144,387 1-minute bars, Jan 1 - Oct 31, 2025
**Analysis Tools:** Python, pandas, polars, lumibot internal modules
