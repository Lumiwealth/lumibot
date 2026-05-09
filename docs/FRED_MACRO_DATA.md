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

`FRED_API_KEY` is required for the official FRED/ALFRED API path and for strict point-in-time macro backtests.

With `FRED_API_KEY`, Lumibot uses the official FRED/ALFRED API and passes `realtime_start` and `realtime_end` based on the strategy datetime. This is the strict point-in-time path for macro backtests.

Without `FRED_API_KEY`, Lumibot can fetch an allowlisted set of live public FRED graph CSV endpoints. The data is fetched live from FRED rather than bundled with Lumibot. It is still revised data, so use it for quick exploration or live context only, not for strict point-in-time backtests.

Built-in FRED agent tools are hidden during backtests unless `FRED_API_KEY` is configured. This prevents agents from accidentally using revised public CSV data in historical simulations.

## Backtest Date Safety

In a backtest, `as_of` defaults to `self.get_datetime()`.

Lumibot always filters observations to `observation_date <= as_of`. With `FRED_API_KEY`, it also requests the vintage data known as of that date.

Without a key, CSV mode is date-gated but not revision-safe. Tool results include:

- `point_in_time_safe: false`
- `uses_revised_data: true`

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
