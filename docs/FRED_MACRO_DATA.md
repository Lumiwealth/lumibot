# FRED Macro Data

Lumibot includes native Federal Reserve Economic Data (FRED) macro tools for strategies and AI agents.

Use macro data for interest rates, inflation, employment, growth, liquidity, credit spreads, and market-risk context.

## Strategy API

```python
self.macro.list_series()
self.macro.get_series("DGS10")
self.macro.get_latest("UNRATE")
self.macro.get_snapshot(["FEDFUNDS", "DGS10", "CPIAUCSL", "UNRATE"])
```

## Agent Tools

Agents receive these built-ins automatically:

- `list_fred_series`
- `get_fred_series`
- `get_fred_latest`
- `get_fred_snapshot`

You do not need to manually attach these tools. They are included with the rest of the built-in agent tool surface.

## API Key Behavior

`FRED_API_KEY` is required for the official FRED/ALFRED API path.

Lumibot uses the official FRED/ALFRED API and passes `realtime_start` and `realtime_end` based on the strategy datetime. Vintage dates support conservative historical observations; they do not certify intraday release instants.

Lumibot does not use public CSV fallbacks for macro data. Built-in FRED agent tools are hidden during backtests unless `FRED_API_KEY` is configured. This prevents agents from accidentally using macro data without a point-in-time data contract in historical simulations.

## Backtest Date Safety

In a backtest, `as_of` defaults to `self.get_datetime()`.

FRED vintages identify calendar dates, not intraday release instants. During a
backtest, LumiBot caps both the vintage request and eligible observations at the
previous completed calendar day in America/Chicago, relative to the simulated
clock. An earlier explicit `as_of` remains earlier; a future override cannot
advance the effective vintage. Live requests retain their current-date behavior.

Results include `as_of_date`, `temporal_policy` and a timing limitation. This is a
conservative availability approximation: it prevents a morning decision from
seeing that day's closing observation, but can omit legitimate same-day releases.
It does not certify exact intraday release timing. Keep that limitation in research
handoffs rather than describing date-only vintages as exact historical replay.

## Cache

FRED data is cached under:

```text
~/.lumibot/cache/fred
```

Override with:

```bash
export LUMIBOT_FRED_CACHE_DIR=/path/to/cache
```

Backtests should fetch each series once and reuse the local cache instead of hitting FRED on every trading iteration.
