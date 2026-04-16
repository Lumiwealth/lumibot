# Flat-Price Fabrication Bug: Root Cause Analysis

**Last Updated:** 2026-04-16
**Status:** Fix deployed (v4.4.62), verification in progress
**Audience:** Engineers, AI Agents
**Related:** docs/investigations/2026-03-05_vix-data-backtest-stuck-investigation.md

## Timeline

- **2026-03-31**: Commit `4d34024a` in botspot_node switched `botspot_auto` routing from IBKR to ThetaData for stocks/indexes/options. Commit message: "IBKR returns stale/constant data for INDEX assets (VIX always 14.2) and incorrect prices for some stocks (SPY showing $687 for 2018)." This was a WORKAROUND, not a root-cause fix.
- **2026-04-15**: Alpha Picks backtest reported 72 of 84 stocks with frozen fill prices. Investigation revealed the flat prices matched the `open` column of 2026-04-10 (penultimate bar in each symbol's S3 cache) for ALL 36 symbols checked. ThetaData was returning correct varied data; lumibot's broker fill path was fabricating prices.
- **2026-04-15**: Fix deployed in lumibot v4.4.62 (PR #991). Defensive guard in `backtesting_broker.py` prevents fills from bars far from simulation time.
- **2026-04-16**: Verification backtest completed: 0 frozen symbols, +86.82% total return (vs +36.64% with fabricated fills).

## Root Cause

### The bug: `backtesting_broker.py` lines 2729-2748

```python
df = df_original[df_original.index >= self.datetime]
if len(df) == 0:
    df = df_original.iloc[-1:]  # BUG: picks any row, including far-future bars
```

When `df_original` contains rows far in the future relative to `self.datetime` (simulation clock), the filter `index >= self.datetime` catches ALL of them (all are "at or after" the sim time). Then `df.iloc[0]` picks the FIRST one, which is the penultimate bar of the entire cache. For MARKET orders, `price = open` of that bar.

### Why it produced the same price for every fill

The penultimate cache row (2026-04-10) has a specific `open` value per symbol. Since every simulation step picked the same bar, every fill got the same `open`:
- AMR: open[2026-04-10] = $186.10
- SMCI: open[2026-04-10] = $23.64
- POWL: open[2026-04-10] = $230.81
- (etc., 36/36 symbols verified)

### Why it only affected "thin" symbols (not mega-caps)

The bug manifests when `get_historical_prices(length=2)` returns a frame containing bars far from the simulation time. For mega-caps (GOOGL, META, CRM), the ThetaData warm-cache path correctly returns bars near the simulation time. For thin tickers, some cache or data-loading interaction causes the returned frame to contain late bars. The exact upstream mechanism is still unidentified.

### The original IBKR symptoms were the SAME bug

- "VIX always 14.2": VIX IBKR cache last close IS $14.20 (at 2025-12-29). The broker picked that late bar for every historical fill.
- "SPY $687 for 2018": SPY IBKR cache last closes are ~$690 (late 2025). The broker picked that late bar for 2018 fills.

Cache data was NOT corrupted in either case. VIX IBKR cache has 1320 distinct closes; SPY has 3071 distinct closes. The data is real and varied.

## The fix (v4.4.62)

**File:** `lumibot/backtesting/backtesting_broker.py`

Two defensive guards:
1. **Future bar window**: Only accepts bars within 2 days (daily) / 2 hours (hourly) / 5 minutes (minute) of simulation time. Prefers the most recent bar AT OR BEFORE sim time when no future bar is in window.
2. **Max fill distance**: Hard-rejects any fill from a bar more than 7 days (daily) / 1 day (intraday) from simulation time with an error log.

Provider-agnostic: applies to every PandasData-derived source (ThetaData, IBKR, Polygon, etc.).

## Verification

- Alpha Picks rerun (backtest 186b4150, v4.4.62): **0 frozen symbols**, all 50+ symbols with 5+ fills show multiple distinct prices. Total return +86.82%.
- Previous broken run (backtest e58895f0, v4.4.61): 72 of 84 frozen at 1 price. Total return +36.64%.

## Still outstanding

1. **Root cause of WHY `df_original` contains late bars in prod** is unidentified. Local repro with cold cache returns correct bars. The fix prevents the symptom but not the upstream data delivery bug.
2. **botspot_node routing** still sends stocks/indexes to ThetaData instead of IBKR. Needs to be reverted (5 locations in botspot_node).
3. **VIX minute data** still unavailable on IBKR (separate from flat-price bug). VIX DAILY via TWS works (544 distinct closes over 2160 rows).
4. **Peter Hiebler's strategies** (Credit Spreads, Dual Sleeve, Wheel With Trap) need testing on IBKR after routing revert.

## Routing locations in botspot_node

The `botspot_auto` routing map is defined in 5 places (should be refactored to 1):
1. `src/services/providerMetadata.ts:24-25` (canonical definition)
2. `src/services/dataAccess.service.ts:75` (runtime fallback)
3. `src/services/dataAccess.service.ts:1132` (env var default: "ThetaData")
4. `src/services/dataAccess.service.ts:1208` (case-branch fallback)
5. `src/Mcp/handlers/dataProviders.ts:81` (MCP tool documentation)

Target: `{"default":"ibkr","stock":"ibkr","index":"ibkr","option":"thetadata","crypto":"ibkr","future":"ibkr","cont_future":"ibkr"}`

## Peter Hiebler's strategies (for VIX testing)

| Strategy | ID | VIX usage |
|---|---|---|
| Premium Selling Credit Spreads | `156a5362-0906-4013-b6d5-ca2691ee70e3` | Heavy: `_vix_overlay()`, crash controls, spread pricing |
| Dual Sleeve Portfolio | `03cbb2a0-f07f-4e23-99a8-6def370de896` | Unknown |
| Wheel With Trap | `d2285845-3bca-4e0d-81ee-07a6f10bb077` | Unknown |

The Credit Spreads strategy calls `self.get_last_price(Asset("VIX", asset_type=Asset.AssetType.INDEX))` directly. On IBKR, this routes to TWS for daily VIX data. The March investigation documented that IBKR has no MINUTE VIX bars, but DAILY bars work (verified: 751 rows, 544 distinct closes via TWS).
