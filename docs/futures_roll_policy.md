# Futures Roll Policy Unification

## Current State
- `Asset._determine_continuous_contract_components` applies a fixed mid-month roll: it advances to the next quarterly month when `day >= 15` within Mar/Jun/Sep/Dec. This logic feeds Polygon, Tradovate, and any code that calls `Asset.resolve_continuous_futures_contract_*`.
- `lumibot/tools/futures_roll.py` introduces a shared rule so DataBento backtests, Tradovate, and ProjectX all use the same contract schedule.
- ProjectX/Tradovate helpers duplicate the mid-month rule. Tests rely on whichever helper they touch, so mismatched schedules have gone unnoticed.
- Because asset-level logic does not consult actual expiration dates or exchange calendars, long backtests can lag the front month by weeks, while DataBento fetches hop ahead sooner. The divergence produces the pricing mismatch we observed (MESU5 vs MESZ5).

## Industry References
- CME equity index futures typically roll when the expiring contract’s volume falls below the next (around the Thursday before the 3rd Friday). NinjaTrader and Tradovate default their continuous contracts to roll 8 business days before expiration (configurable per instrument).
- IBKR’s default continuous contracts roll 7 calendar days prior to the last trade date, with overrides for energy and currency products.
- Energy and interest-rate products often use exchange-specific offsets (e.g., CL rolls 5 business days before the 25th of the month; ZB rolls 7 business days before the first business day of the delivery month).

## Proposed Direction
1. **Central Roll Registry**
   - Create `lumibot/futures_roll.py` that exposes:
     - `get_roll_rule(asset: Asset) -> RollRule`
     - `build_roll_schedule(asset, start, end, *, rule=None)` returning `(symbol, start_dt, end_dt)` tuples in UTC.
   - `RollRule` encapsulates:
     - `offset_type`: `business_days_before_expiry`, `calendar_days_before`, `volume_switch` (placeholder for future volume-based logic).
     - `offset_value`: integer days.
     - Optional exchange calendar (`pandas_market_calendars` identifier) for business-day math.
     - Optional hard-coded anchor (e.g., “Thursday before third Friday”).
2. **Rule Sources**
   - Base defaults per asset class + exchange:
     - CME equity index (ES, NQ, MES, MNQ): 8 business days before last trade date.
     - CME metals (GC, SI): 7 business days.
     - CME energy (CL, NG): follow exchange published schedule (e.g., 5 business days before the 25th) — store as callable rule.
     - Treasury futures (ZB, ZN): 7 business days before first business day of delivery month.
   - Allow overrides via config file or environment variable (e.g., `LUMIBOT_FUTURES_ROLL_OVERRIDES=MES:business,-8`).
3. **Expiration Data**
   - Prefer instrument definitions from the data source (DataBento, Tradovate, etc.). Fallback: maintain static expirations for common CME products or derive from `asset.contract_expirations` if provided.
   - Cache definitions per `(symbol, dataset)` only where a downstream adapter requires them; the shared module itself does not depend on provider metadata for CME equity index futures.
4. **Integration Plan**
   - Ensure `Asset.resolve_continuous_futures_contract*`, DataBento helpers, Tradovate, and ProjectX all call the shared module so the schedule stays aligned everywhere.
   - Remove duplicated mid-month logic once the shared rule is in place.
   - Provide a thin adapter for data sources that need symbol variants (e.g., single-digit year for DataBento).
5. **Testing Strategy**
   - Unit tests: build schedules around known roll dates (e.g., MES September 2025, CL December 2024) and assert the windows pivot on the expected dates.
   - Integration tests: run parity checks (like the MES demo) across backends after stubbing the rule to a deterministic offset.
   - Regression test to ensure `Asset.resolve_continuous_futures_contract` produces identical symbols to the data-source schedule for a range of dates.

## Deliverables
- `lumibot/futures_roll.py` module + data-driven rule registry.
- Migration patches for Asset, DataBento helpers, ProjectX, Tradovate.
- Documentation update (`docs/futures.md`) explaining default rules and how users override them.
- Expanded test coverage exercising the new rule set.

This approach gives us one authoritative roll policy, tuned per instrument, and keeps both brokers and data sources in sync while remaining extensible for future providers (Polygon futures, ThetaData, etc.).
