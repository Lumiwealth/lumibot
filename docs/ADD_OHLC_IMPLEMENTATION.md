# ADD_OHLC Implementation Specification

**Title:** Adding OHLC (Candlestick) Data Support to Lumibot Charts
**Last Updated:** 2026-01-28
**Status:** Specification Complete - Ready for Implementation
**Audience:** AI Agents / Developers implementing this feature

---

## Overview

This document specifies how to add `add_ohlc()` method support to Lumibot, enabling strategies to plot OHLC (Open, High, Low, Close) data as candlestick or bar charts instead of only single-value lines.

**Current State:** Lumibot only supports `add_line()` which plots a single value per timestamp.

**Desired State:** Add `add_ohlc()` to plot 4 values (open, high, low, close) per timestamp, rendered as candlesticks in TradingView.

---

## Background Research

### Industry Standards for OHLC Methods

| Library | Candlestick Method | Parameters |
|---------|-------------------|------------|
| **TradingView Pine Script** | `plotcandle()` | open, high, low, close, color, wickcolor |
| **TradingView Pine Script** | `plotbar()` | open, high, low, close, color |
| **Plotly** | `go.Candlestick()` | open, high, low, close |
| **mplfinance** | `candlestick_ohlc()` | open, high, low, close |

### Key Insight: Data Structure vs Visual Representation

The difference between "candlestick" and "bar" charts is purely **visual** (how TradingView renders the data), not a difference in **data structure**. Both use the same 4 values: open, high, low, close.

**Therefore:** We only need ONE method (`add_ohlc`) that provides the data. The visual representation (candlestick vs bar) is a TradingView display preference, not a data concern.

### What About Other Multi-Value Indicators?

Research across TradingView, MetaTrader, and other platforms shows only **two fundamental data structures** for indicators:

| Structure | Values per Timestamp | Examples |
|-----------|---------------------|----------|
| **Single value** | 1 | SMA, EMA, RSI, Bollinger upper/middle/lower |
| **OHLC** | 4 | Price bars, Heikin Ashi, custom OHLC overlays |

Multi-line indicators like Bollinger Bands are just **multiple single-value lines** (3 separate `add_line()` calls for upper, middle, lower).

More complex structures like footprint/order flow charts require tick-by-tick order book data that backtests don't have - not relevant for Lumibot.

---

## Implementation Specification

### 1. New Method: `add_ohlc()` in `strategy.py`

**Location:** `lumibot/strategies/strategy.py` (add after `add_line()` method around line 3710)

**Method Signature:**

```python
def add_ohlc(
    self,
    name: str,
    open: float,
    high: float,
    low: float,
    close: float,
    color: str = None,
    border_color: str = None,
    wick_color: str = None,
    detail_text: str = None,
    dt: Union[datetime.datetime, pd.Timestamp] = None,
    plot_name: str = "default_plot",
    asset: Asset = None
):
    """Adds an OHLC (candlestick) data point to the indicator chart.

    This can be used to plot price bars, Heikin Ashi candles, or any other
    OHLC data on the chart. The data will be rendered as candlesticks in
    TradingView.

    Parameters
    ----------
    name : str
        The name of the OHLC series. This is used to identify the series
        on the chart and group data points together.
    open : float
        The opening price for this bar.
    high : float
        The highest price for this bar.
    low : float
        The lowest price for this bar.
    close : float
        The closing price for this bar.
    color : str, optional
        The fill color of the candlestick body. If not specified, will use
        green for bullish (close > open) and red for bearish (close < open).
    border_color : str, optional
        The border color of the candlestick body. Defaults to same as color.
    wick_color : str, optional
        The color of the candlestick wicks. Defaults to same as color.
    detail_text : str, optional
        Additional text to display when the candle is hovered over.
    dt : datetime.datetime or pandas.Timestamp, optional
        The datetime for this bar. Defaults to current strategy datetime.
    plot_name : str, optional
        The name of the subplot. Default "default_plot" adds to main chart.
    asset : Asset, optional
        The Asset object associated with this OHLC data. Enables proper
        multi-symbol charting where OHLC data displays as overlay on its
        corresponding asset's price chart.

    Example
    -------
    >>> # Plot a custom OHLC bar
    >>> self.add_ohlc(
    ...     name="Heikin Ashi",
    ...     open=ha_open,
    ...     high=ha_high,
    ...     low=ha_low,
    ...     close=ha_close,
    ...     color="green" if ha_close > ha_open else "red"
    ... )

    >>> # Plot OHLC for a specific asset
    >>> self.add_ohlc(
    ...     name="SPY Price",
    ...     open=spy_open,
    ...     high=spy_high,
    ...     low=spy_low,
    ...     close=spy_close,
    ...     asset=spy_asset
    ... )
    """
```

**Implementation Details:**

1. **Validation:** Same pattern as `add_line()` - validate all parameters
2. **Data Storage:** Append to `self._chart_ohlc_list` (new list, similar to `_chart_lines_list`)
3. **OHLC Validation:** Ensure `high >= max(open, close)` and `low <= min(open, close)`

**Data Structure to Store:**

```python
{
    "datetime": dt,
    "name": name,
    "type": "ohlc",  # NEW: Distinguish from "line" type
    "open": open,
    "high": high,
    "low": low,
    "close": close,
    "color": color,
    "border_color": border_color,
    "wick_color": wick_color,
    "detail_text": detail_text,
    "plot_name": plot_name,
    # Asset fields (same as add_line)
    "asset_symbol": asset.symbol if asset else None,
    "asset_type": asset.asset_type if asset else None,
    "asset_expiration": str(asset.expiration) if asset and asset.expiration else None,
    "asset_strike": asset.strike if asset else None,
    "asset_right": asset.right if asset else None,
    "asset_multiplier": asset.multiplier if asset else None,
    "quote_symbol": asset._quote_asset.symbol if asset and hasattr(asset, '_quote_asset') and asset._quote_asset else None,
    "asset_display_name": str(asset) if asset else None,
}
```

### 2. New Method: `get_ohlc_df()` in `strategy.py`

**Location:** Add after `get_lines_df()` method

```python
def get_ohlc_df(self):
    """Returns a dataframe of the OHLC data on the indicator chart.

    Returns
    -------
    pandas.DataFrame
        The OHLC data on the indicator chart.
    """
    df = pd.DataFrame(self._chart_ohlc_list)
    return df
```

### 3. Initialize Storage in `_Strategy.__init__()`

**Location:** `lumibot/strategies/_strategy.py` - find where `_chart_lines_list` is initialized

Add:
```python
self._chart_ohlc_list = []
```

### 4. Update `indicators.py` to Handle OHLC Data

**Location:** `lumibot/tools/indicators.py`

**Changes to `plot_indicators()` function:**

1. **Add `chart_ohlc_df` parameter:**
```python
def plot_indicators(
    plot_file_html="indicators.html",
    chart_markers_df=None,
    chart_lines_df=None,
    chart_ohlc_df=None,  # NEW PARAMETER
    strategy_name=None,
    show_indicators=True,
):
```

2. **Process OHLC data similar to lines:**
```python
if chart_ohlc_df is not None and not chart_ohlc_df.empty:
    chart_ohlc_df = chart_ohlc_df.copy()
    if "plot_name" not in chart_ohlc_df.columns:
        chart_ohlc_df["plot_name"] = "default_plot"
    else:
        chart_ohlc_df["plot_name"] = chart_ohlc_df["plot_name"].fillna("default_plot")
```

3. **Add OHLC traces to Plotly chart:**
```python
if chart_ohlc_df is not None and not chart_ohlc_df.empty:
    for plot_name, plot_df in chart_ohlc_df.groupby("plot_name"):
        for ohlc_name, group_df in plot_df.groupby("name"):
            row = plot_names.index(plot_name) + 1

            fig.add_trace(
                go.Candlestick(
                    x=group_df["datetime"],
                    open=group_df["open"],
                    high=group_df["high"],
                    low=group_df["low"],
                    close=group_df["close"],
                    name=ohlc_name,
                    # Colors can be customized based on data
                ),
                row=row,
                col=1
            )
```

4. **Update CSV export to include OHLC data:**
```python
# When exporting to CSV, include OHLC data
if chart_ohlc_df is not None and not chart_ohlc_df.empty:
    chart_ohlc_df = chart_ohlc_df.copy()
    chart_ohlc_df["type"] = "ohlc"
    # Include in combined_df
```

### 5. CSV Format Changes

**Current CSV columns (lines):**
```
datetime,name,value,color,style,width,detail_text,plot_name,type,asset_symbol,...
```

**New CSV columns (OHLC):**
```
datetime,name,open,high,low,close,color,border_color,wick_color,detail_text,plot_name,type,asset_symbol,...
```

**Combined Format (both lines and OHLC in same CSV):**
- `type` column distinguishes: `"line"` vs `"ohlc"`
- For lines: `open`, `high`, `low`, `close` columns are empty/NA
- For OHLC: `value` column is empty/NA

### 6. Call `plot_indicators()` with OHLC Data

**Location:** Find where `plot_indicators()` is called (likely in backtesting result generation)

Update the call to include OHLC data:
```python
plot_indicators(
    plot_file_html=indicators_file,
    chart_markers_df=strategy.get_markers_df(),
    chart_lines_df=strategy.get_lines_df(),
    chart_ohlc_df=strategy.get_ohlc_df(),  # NEW
    strategy_name=strategy.name,
    show_indicators=show_indicators,
)
```

---

## Frontend Changes (botspot_react)

### 7. Update `BacktestChartPage.js` to Parse OHLC Data

**Location:** `src/pages/BacktestChartPage.js`

Currently, the CSV parsing only handles lines with a single `value` column. Update to detect and parse OHLC data:

```javascript
// When parsing indicators.csv, check for OHLC columns
const hasOhlcData = headers.includes('open') && headers.includes('high') &&
                    headers.includes('low') && headers.includes('close');

// Group by type
const lineData = rows.filter(r => r.type === 'line');
const ohlcData = rows.filter(r => r.type === 'ohlc');
```

### 8. Update `Chart.js` to Render OHLC Data

**Location:** `src/components/Chart.js`

The `IndicatorDatafeed` class needs to handle OHLC data:

1. **Store OHLC series separately from line series:**
```javascript
this.ohlcMap = {};  // NEW: Store OHLC series

this.indicatorData.forEach(ind => {
  if (ind.type === 'ohlc' && ind.name && ind.points?.length > 0) {
    this.ohlcMap[ind.name] = this.ensureUniqueTimestamps(ind.points);
    // Store metadata
  }
});
```

2. **Modify `getBars()` to return OHLC data:**

Currently `getBars()` creates fake OHLC where all values are the same:
```javascript
// CURRENT (wrong for OHLC)
const bar = {
  time: point.time,
  open: point.value,
  high: point.value,
  low: point.value,
  close: point.value,
};
```

For actual OHLC data:
```javascript
// NEW (for real OHLC)
const bar = {
  time: point.time,
  open: point.open,
  high: point.high,
  low: point.low,
  close: point.close,
};
```

3. **Update `resolveSymbol()` for OHLC symbols:**

When a symbol is OHLC type, set appropriate flags for TradingView to render candlesticks instead of lines.

---

## Testing Requirements

### Unit Tests for `add_ohlc()`

**Location:** `tests/test_add_ohlc.py` (new file)

1. **Test basic OHLC data storage:**
```python
def test_add_ohlc_basic():
    strategy = create_test_strategy()
    strategy.add_ohlc("Test", open=100, high=105, low=98, close=102)
    df = strategy.get_ohlc_df()
    assert len(df) == 1
    assert df.iloc[0]["open"] == 100
    assert df.iloc[0]["high"] == 105
    assert df.iloc[0]["low"] == 98
    assert df.iloc[0]["close"] == 102
```

2. **Test OHLC validation (high >= max, low <= min):**
```python
def test_add_ohlc_invalid_high_low():
    strategy = create_test_strategy()
    # high < close should warn or fix
    strategy.add_ohlc("Test", open=100, high=99, low=98, close=102)
    # Verify behavior (warning logged or values adjusted)
```

3. **Test color defaulting (green for bullish, red for bearish):**
```python
def test_add_ohlc_default_colors():
    strategy = create_test_strategy()
    strategy.add_ohlc("Bullish", open=100, high=105, low=98, close=104)  # close > open
    strategy.add_ohlc("Bearish", open=104, high=105, low=98, close=100)  # close < open
    df = strategy.get_ohlc_df()
    # Verify appropriate default colors assigned
```

4. **Test asset association:**
```python
def test_add_ohlc_with_asset():
    strategy = create_test_strategy()
    asset = Asset("SPY", asset_type="stock")
    strategy.add_ohlc("SPY", open=400, high=405, low=398, close=403, asset=asset)
    df = strategy.get_ohlc_df()
    assert df.iloc[0]["asset_symbol"] == "SPY"
```

5. **Test datetime handling:**
```python
def test_add_ohlc_custom_datetime():
    strategy = create_test_strategy()
    custom_dt = datetime.datetime(2024, 1, 15, 10, 30, 0)
    strategy.add_ohlc("Test", open=100, high=105, low=98, close=102, dt=custom_dt)
    df = strategy.get_ohlc_df()
    assert df.iloc[0]["datetime"] == custom_dt
```

### Integration Tests

1. **Test CSV export contains OHLC data correctly**
2. **Test mixed line + OHLC data in same CSV**
3. **Test Plotly chart generation with OHLC traces**

---

## Documentation Updates

### 1. Update `docsrc/strategy_methods.data.rst`

Add documentation for the new `add_ohlc()` method with examples.

### 2. Update `docsrc/getting_started.rst`

Add example showing how to plot custom OHLC data like Heikin Ashi.

### 3. Add Example Strategy

Create example in `examples/` showing OHLC usage:
```python
# Example: Plotting Heikin Ashi candles alongside regular price
def on_trading_iteration(self):
    bars = self.get_historical_prices(self.asset, 2)

    # Calculate Heikin Ashi
    ha_close = (bars.open + bars.high + bars.low + bars.close) / 4
    ha_open = (prev_ha_open + prev_ha_close) / 2
    ha_high = max(bars.high, ha_open, ha_close)
    ha_low = min(bars.low, ha_open, ha_close)

    # Plot as OHLC
    self.add_ohlc(
        name="Heikin Ashi",
        open=ha_open,
        high=ha_high,
        low=ha_low,
        close=ha_close,
        asset=self.asset
    )
```

---

## Implementation Checklist

- [ ] Add `_chart_ohlc_list = []` in `_Strategy.__init__()`
- [ ] Implement `add_ohlc()` method in `strategy.py`
- [ ] Implement `get_ohlc_df()` method in `strategy.py`
- [ ] Update `plot_indicators()` in `indicators.py` to accept OHLC data
- [ ] Add Plotly candlestick trace generation
- [ ] Update CSV export to include OHLC columns
- [ ] Update wherever `plot_indicators()` is called to pass OHLC data
- [ ] Write unit tests for `add_ohlc()`
- [ ] Write integration tests for CSV/chart generation
- [ ] Update Sphinx documentation in `docsrc/`
- [ ] Create example strategy using `add_ohlc()`
- [ ] (Future) Update botspot_react frontend to parse and display OHLC data

---

## Notes for Implementers

### Why `add_ohlc` vs `add_candle` or `add_bar`?

We chose `add_ohlc` because:
1. **It describes the data structure** (Open, High, Low, Close) not the visual style
2. **TradingView decides the visual** - users can toggle between candlestick and bar views
3. **Matches industry terminology** - "OHLC data" is universally understood
4. **Avoids confusion** - "candle" and "bar" both represent the same 4 data points

### Relationship to Existing `add_line()`

- `add_line()` = 1 value per timestamp (for SMA, EMA, RSI, etc.)
- `add_ohlc()` = 4 values per timestamp (for price bars, Heikin Ashi, etc.)

These are the only two data structures needed. Everything else is either:
- Multiple `add_line()` calls (Bollinger Bands = 3 lines)
- Visual styling of single values (histogram = line with different display)

### Frontend Priority

The Lumibot backend changes are the priority. Frontend (botspot_react) changes can be done in a follow-up phase since:
1. The CSV will be generated with OHLC data
2. The existing Chart.js can still function (it just won't render OHLC as candlesticks yet)
3. Frontend changes require separate testing and deployment
