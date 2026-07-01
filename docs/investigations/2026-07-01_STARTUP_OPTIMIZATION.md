# Startup Optimization

One-line description: Cold-start and scheduled run-once startup optimization evidence for LumiBot.

Last Updated: 2026-07-01

Status: In Progress

Audience: LumiBot maintainers and deployment operators

## Overview

Goal: reduce scheduled deployment cold/hot startup overhead without changing strategy behavior.

## Current Results

Benchmarks use subprocesses with:

```bash
LUMIBOT_DISABLE_DOTENV=1 LUMIBOT_DISABLE_DOTENV_LOCAL=1 LUMIBOT_LOG_LEVEL=ERROR
```

| Path | Earlier Baseline | Current Median | Notes |
| --- | ---: | ---: | --- |
| `from lumibot.example_strategies.stock_diversified_leverage import DiversifiedLeverage` | ~4.16s | ~0.0024s | Heavy backtesting/provider imports, runtime `typing`, and example-only `datetime` import deferred. |
| `from lumibot.strategies import Strategy` | ~4.45s initial / ~0.031s prior patch | ~0.0019s | `lumibot.constants`, logging, termcolor, `Asset`, `Order`, `datetime`, JSON, Decimal, runtime `typing`, smart-limit helpers, and broker/data-source defaults now deferred from strategy path. |
| `import lumibot.credentials` in scheduled Alpaca env | ~0.342s with Alpaca SDK stack | ~0.0008s | Scheduled credentials now skip import-time broker materialization, logger setup, colored debug formatting, and lazy-helper `datetime`. |
| `from lumibot.strategies import Strategy` in scheduled Alpaca env | ~0.046s after stream deferral / ~0.018s after lazy broker resolution | ~0.0019s | Scheduled `_strategy` import now defers `lumibot.credentials`; entity classes, logger, trader, parser, `Asset`, `Order`, `datetime`, JSON, Decimal, smart-limit helpers, and termcolor stay lazy. |
| first scheduled `credentials.BROKER.name` access with Alpaca fake creds | ~0.342s with stream startup / ~0.043s after first scheduled deferral / ~0.026s after stream/entity deferral | ~0.0033s | Broker materializes on demand with stream, telemetry, pytz, logger, cash-event/position/quote classes, AlpacaData/data-source modules, symbol parser, termcolor, `Order`, JSON, Decimal, runtime `typing`, `datetime`, and order thread deferred. |
| `from lumibot.strategies.strategy_executor import StrategyExecutor` | ~0.44s | ~0.0026s | APScheduler, pandas, calendars, inspect, traceback, `Asset`, `Order`, `datetime`, Decimal, and smart-limit helpers deferred. |
| startup provider export set with `StrategyExecutor`, core brokers, and core data sources | ~0.026s before final provider cleanup | ~0.0113s | No guarded heavy modules loaded: no logging, termcolor, `Asset`, `Order`, Position/CashEvent, provider SDKs, pandas/numpy, requests, subprocess/tempfile/resources, inspect/random/traceback, JSON, Decimal, runtime `typing`, datetime, symbol-normalization helpers, or smart-limit helpers. |
| `from lumibot.data_sources import DataSource` | ~1.03s | ~0.0026s | pandas/scipy/options helpers, `Asset`, constants, pytz, and `datetime` deferred. |
| `from lumibot.brokers import Broker` | ~1.09s initial / ~0.033s prior patch | ~0.032s | pandas/calendar/parquet imports deferred. |
| `from lumibot.brokers import Alpaca` | ~0.67s | ~0.0033s | Alpaca SDK imports deferred. |
| `Alpaca({...}, connect_stream=False, start_orders_thread=False)` with fake creds | ~0.64s | ~0.0138s | TradingClient creation deferred until first API call. |
| scheduled `Alpaca({...})` default constructor with fake creds | ~0.442s with stream/order threads | ~0.00004s | Omitted `connect_stream`/`start_orders_thread` now honor scheduled defaults, and default `AlpacaData` construction is deferred; explicit `True` still preserves eager stream/order-thread behavior. |
| scheduled `Alpaca + DiversifiedLeverage` construction with broker balance/position I/O mocked | ~0.034s / ~0.0079s before executor and Asset datetime deferral | ~0.0064s | Stream/order thread off. AlpacaData/data-source modules, logger stack, runtime telemetry, JSON, traceback, runtime `typing`, `datetime`, `Order`, and `StrategyExecutor` stay deferred; live cash position still loads `Asset`, Decimal, and `Position`. |
| pre-open scheduled Alpaca `run_once` closed-market exit | ~0.316s after data-source wall-clock fix / ~0.0072s after first precheck | ~0.00004s run_once segment / ~0.0065s full cold path | Alpaca scheduled regular-equity precheck skips pandas/calendar stack, `inspect`, `zoneinfo`, scheduled-timing module, broker market-open check, and live data-source materialization when wall-clock UTC is definitely outside possible regular US equity hours or on a weekend. |
| `from lumibot.traders import Trader` | ~0.0115s with logging stack | ~0.0008s | Logger setup, `logging`, `dataclasses`, `inspect`, `signal`, `threading`, and runtime `typing` deferred/removed from import. |
| initialized-calendar market-open check | ~190-270ms repeated PMC schedule path | ~0.141ms | Broker reuses `_trading_days` set by scheduled run_once. |
| 24/7 market open check | ~270ms first `get_trading_days("24/7")` path | ~0.0001ms | Scheduled run_once skips calendar init for `market == "24/7"`. |
| `from lumibot.brokers import Tradier` | ~0.965s | ~0.033s | Tradier SDK, pandas, requests, constants deferred. |
| `from lumibot.data_sources import TradierData` | ~0.848s | ~0.022s | Tradier SDK, pandas, scipy, requests, constants deferred. |
| `from lumibot.brokers import Ccxt` | ~0.553s | ~0.031s | CCXT and pandas deferred from export. |
| `from lumibot.data_sources import CcxtData` | ~0.552s | ~0.021s | CCXT and pandas deferred from export. |
| `Ccxt({...})` with fake creds | ~0.553s + constructor `load_markets()` network | ~0.240s no network | `load_markets()` deferred to first market-data/order path. |
| `from lumibot.brokers import Schwab` | ~0.533s | ~0.037s | Schwab SDK, authlib/httpx, pandas, polars, and SchwabData deferred from export. |
| `from lumibot.data_sources import SchwabData` | ~0.391s | ~0.021s | pandas, constants, tools, Bars/Quote/Chains deferred. |
| `from lumibot.brokers import Bitunix` | ~0.374s | ~0.031s | pandas, BitunixData, and BitUnixClient deferred from export. |
| `from lumibot.data_sources import BitunixData` | not measured | ~0.021s | pandas, pytz, Bars, and BitUnixClient deferred from export. |
| `Bitunix({...})` with fake creds | constructor called `change_position_mode("HEDGE")` | ~0.783ms, zero mode calls | Position mode API call deferred to first submitted order, guarded once. |
| `import lumibot.credentials` in Bitunix env | ~0.074-0.077s with `requests` | ~0.031s | `requests` moved from Bitunix helper import to first REST request. Scheduled lazy import is ~0.006s. |
| `from lumibot.brokers import ProjectX` | ~0.288s | ~0.031s | pandas, constants, requests, SignalR, and ProjectX helpers deferred from export. |
| `from lumibot.data_sources import ProjectXData` | ~0.377s | ~0.021s | pandas, constants, Bars/Quote, requests, and ProjectXClient deferred from export. |
| `ProjectX({...})` with fake data source | constructor performed auth POST | ~0.924ms, zero client calls | Lazy client proxy defers auth until first client method access and captures constructor-scoped patches. |
| `ProjectXData({...})` | constructor performed auth POST | ~0.022ms, zero client calls | Lazy client proxy defers auth until first data method requiring ProjectXClient. |
| `from lumibot.brokers import InteractiveBrokersREST` | ~0.372s | ~0.032s | REST data source, pandas, requests, and Bars deferred from export. |
| `from lumibot.data_sources import InteractiveBrokersRESTData` | ~0.374s | ~0.030s | pandas, requests, timezone constants, urllib3 warning setup, and Bars deferred from export. |
| `from lumibot.brokers import Tradovate` | ~0.373s | ~0.033s | requests and TradovateData deferred from export. |
| `from lumibot.data_sources import TradovateData` | ~0.337s | ~0.021s | Bars import deferred from export. |
| `Tradovate({...})` with fake creds | constructor performed token/account/user REST | ~1.036ms, zero auth/account/user calls | Default constructor now validates config and connects lazily on first authenticated broker operation. `CONNECT_ON_INIT=True` preserves eager behavior. |

## Implemented Optimizations

- Added `lumibot._lazy_imports.LazyModule` for module-like lazy imports that still pass `inspect.ismodule`.
- Added `lumibot._lazy_imports.lazy_class` for class-like lazy imports with `isinstance`/`issubclass` support.
- Added `lumibot._lazy_imports.lazy_typing` so public `typing.get_type_hints()` keeps resolving stringified annotations without importing `typing` on startup.
- Added `LazyPytzTimezone` for default timezone class attributes that should not force constants/pytz during package import.
- Split `LazyPytzTimezone` into `lumibot._lazy_timezone` so generic lazy import helpers no longer import `datetime`.
- Added `LazyPytzTimezoneRef` so `DataSource.DEFAULT_PYTZ` and provider timezone constants materialize only on access/use, avoiding `datetime` during provider export.
- Made `DataSource._delay` and `DataSource.tzinfo` materialize lazily, with `tzinfo` resolving to a concrete pytz timezone on access so pandas timezone operations stay compatible.
- Preserved lazy class proxy subclassing and `inspect.signature()` compatibility for public module-level class aliases.
- Deferred heavy `Strategy` imports: backtesting providers, pandas, polars, matplotlib, requests, SQLAlchemy, parquet helpers, indicator helpers, strategy components, and executor.
- Deferred scheduled-live `StrategyExecutor` construction until first `_executor` access or `Trader.run_all()`, while keeping normal live and backtesting eager and preserving explicit stream/order-thread safety.
- Deferred `StrategyExecutor` dependencies: APScheduler, pandas, pandas-market-calendars, and unbounded calendar setup for scheduled `run_once`.
- Bounded scheduled `run_once` calendar initialization to a 14-day window around strategy time, with fallback to legacy full calendar on error.
- Switched scheduled `run_once` live-calendar initialization to scheduler wall-clock UTC time instead of `strategy.get_datetime()`, so closed-market one-shot exits do not materialize live broker data sources before market-open checks.
- Added an Alpaca scheduled regular-equity precheck that skips exchange-calendar construction entirely when wall-clock UTC is definitely outside possible US equity regular trading hours or on a weekend; holiday/early-close intraday cases still fall back to the existing calendar path.
- Avoided importing `inspect` during scheduled `run_once` initialization for plain Python `initialize()` methods by using cheap code-object argument names and falling back to `inspect` only for wrappers/non-Python callables.
- Deferred `ScheduledRunTiming` from `StrategyExecutor` construction until exact target timing or post-iteration draining actually needs it, and moved `pathlib.Path` inside the timing-file path helper.
- Deferred `Broker`/`DataSource` pandas/calendar/parquet/option-helper imports.
- Removed runtime `typing` imports from startup-sensitive strategy, broker, data-source, provider, and IBKR secdef modules that already use postponed annotations.
- Restored runtime annotation introspection with lazy aliases for `Asset`, `SmartLimitConfig`, `Quote`, `CashEvent`, `Union`, `List`, `Dict`, `Optional`, and related public API type names.
- Deferred Alpaca SDK trading/data clients and stream classes while preserving module-level patch points for tests.
- Made omitted `Alpaca(connect_stream=..., start_orders_thread=...)` defaults honor scheduled execution, matching credential-created brokers while preserving explicit `True` opt-ins.
- Deferred scheduled-only default `AlpacaData` construction behind a lightweight proxy that preserves `SOURCE`, cheap timing fields, first-use materialization, and constructor-time credential validation.
- Moved DiversifiedLeverage script-only backtesting imports behind `if __name__ == "__main__"`.
- Removed eager `lumibot.constants` load from strategy, data-source, and AlpacaData import paths where only stable string defaults were needed.
- Reused initialized broker calendars for live market-open checks instead of repeating pandas-market-calendars schedules.
- Skipped scheduled `run_once` calendar construction for 24/7 markets.
- Skipped user iteration and `_on_strategy_end()`/stats dumping when scheduled `run_once` sees a closed market before target wait.
- Deferred Tradier broker SDK/pandas/requests/order-leg/helper imports until constructor or first broker method use.
- Deferred TradierData SDK/pandas/Bars/Quote/black-scholes/helper imports until constructor or first data method use.
- Deferred CCXT SDK/pandas/Bars imports and removed constructor-time `load_markets()` network call, with guarded first-use market loading.
- Deferred Schwab SDK/client/stream/authlib/httpx, SchwabData, pandas, polars, tools, and Bars/Quote/Chains imports while preserving module-level test patch points.
- Added `SchwabData(auto_create_client=False)` for broker-owned data sources so Schwab broker construction does not trigger a second legacy Schwab client path.
- Deferred Bitunix pandas/client/data-source imports and moved non-fatal `change_position_mode("HEDGE")` from constructor to guarded first order submission.
- Deferred ProjectX broker/data-source pandas/constants/helper imports while preserving `ProjectXClient` patch points.
- Deferred ProjectX broker/data-source constructor authentication behind a lazy client proxy that captures constructor-time monkeypatches and delegates nested `client.api` access on first use.
- Deferred IBKR REST broker/data-source pandas/requests/Bars/timezone imports while preserving current constructor gateway startup behavior.
- Deferred Tradovate broker requests/data-source imports and TradovateData Bars import.
- Deferred Tradovate constructor token/account/user REST calls behind a centralized `_ensure_connected()` gate; first authenticated broker operation connects once and updates broker/data-source tokens. `CONNECT_ON_INIT=True` preserves explicit fail-fast behavior.
- Added scheduled-only lazy default credential materialization. `BROKER`/`DATA_SOURCE` still resolve to real objects on access, while scheduled imports stay config-only.
- Disabled stream startup for credential-created brokers when `LUMIBOT_SCHEDULED_EXECUTION` is truthy, with `LUMIBOT_CONNECT_STREAM=true` as an explicit opt-in override.
- Made `lumibot.trading_builtins` lazily expose `CustomStream`, `PollingStream`, `SafeList`, and `SafeOrderDict`.
- Moved polling stream imports behind `_get_stream_object()` in Alpaca, Bitunix, Tradier, IBKR REST, Tradovate, and Schwab.
- Added `connect_stream` support to IBKR REST, Tradovate, and Schwab credential-created brokers so scheduled construction can avoid stream startup consistently.
- Moved broker timezone and option-chain helper imports to first use, and kept `DataSource` imports under `TYPE_CHECKING` for broker type hints.
- Deferred `SmartLimitConfig`, `TradingFee`, and `TradingSlippage` imports from scheduled strategy/order paths.
- Skipped importing runtime telemetry during scheduled execution unless `LUMIBOT_TELEMETRY` explicitly opts in.
- Added lazy logger proxies for import-time modules so scheduled imports do not load LumiBot logging until a message should emit.
- Deferred `termcolor` from scheduled credentials, strategy, broker, and provider export paths; colored text is now formatted only when a message is emitted or a method needs it.
- Deferred `inspect`/`traceback` from `StrategyExecutor` and `lumibot.tools.decorators` until executor runtime/error paths.
- Deferred provider-export `Position`/`CashEvent` imports across CCXT, Bitunix, IBKR REST, ProjectX, Schwab, Tradier, and Tradovate broker modules.
- Deferred provider-export stdlib/module drags: Tradovate no longer imports LumiBot logging/random/traceback on export; IBKR REST data no longer imports subprocess/tempfile/importlib.resources on export.
- Deferred `AlpacaData` helper import until crypto pair sanitization needs it.
- Fixed lazy scheduler regression by having live cron callback registration call `StrategyExecutor.ensure_scheduler()` before `add_job()`.
- Made backtest performance CSV recording opt-in via `LUMIBOT_RECORD_BACKTEST_PERFORMANCE=1`, preventing verification runs from dirtying tracked history.
- Kept `LazyPytzTimezone` lazy during abstract-method probing and reused it as the default `DataSource` timezone, avoiding `pytz` on scheduled Alpaca broker materialization.
- Deferred `CashEvent`, `Position`, and `Quote` imports from strategy, broker, and Alpaca import paths while preserving module-level compatibility attributes.
- Deferred `Asset.symbol2asset()` parser import until an option/futures symbol actually needs parsing.
- Skipped the Alpaca base orders thread for scheduled credential-created brokers by default, with `LUMIBOT_START_ORDERS_THREAD=true` as an opt-in override.
- Deferred Bitunix helper `requests` import until first signed REST request.
- Deferred Bitunix helper logger setup until a debug/error path actually emits.
- Restored BitunixData's UTC default timezone with a lazy default timezone reference.
- Deferred ProjectX broker/data-source constructor logger setup in scheduled/error-level deployments.
- Deferred broker symbol-normalization helper import until symbol normalization methods are called.
- Deferred `Trader` logging setup and signal/threading imports, and replaced its import-time dataclass with a small slotted config object.
- Prevented Schwab base `Broker.__init__` from launching a stream before Schwab OAuth/client setup; stream launch remains in Schwab finalization.
- Made Schwab broker-local dotenv loading honor `LUMIBOT_DISABLE_DOTENV`.
- Deferred `Order` from strategy, executor, broker, Alpaca, and provider broker import paths with lazy class proxies while keeping public `lumibot.entities.Order` as the real class on access.
- Deferred `Asset` and `datetime` from strategy and executor class imports with lazy proxies and call-time quote-asset defaults.
- Deferred `datetime` from plain `Asset` import/construction; option/future expiration and auto-expiry paths still materialize it on demand.
- Switched live broker-balance throttle bookkeeping from `datetime.now()` to `time.monotonic()` to keep scheduled construction datetime-free.
- Deferred `Asset` and datetime imports from core provider broker/data-source exports; import-time `Asset(...)` defaults in Alpaca/Bitunix paths now use call-time caches.
- Deferred JSON imports from broker constraint keys, strategy backup/serialization helpers, scheduled timing writes, and Tradier/Schwab token cache paths.
- Deferred smart-limit helper imports from `Strategy` and `StrategyExecutor` until SMART_LIMIT order handling.
- Deferred Decimal imports from strategy/executor/broker/provider export paths with lazy class proxies, and removed annotation-only Decimal imports from core provider data-source exports.
- Made strategy construction use `LazyStrategyLogger` so scheduled construction no longer imports LumiBot logging until a message is emitted.
- Added postponed annotations to `Position` so constructing the live cash position does not import `Order` through `add_order` annotations.
- Avoided duplicate `Decimal(cash)` work when creating non-crypto live cash positions; crypto quote-asset availability remains preserved.
- Moved `DiversifiedLeverage` example `datetime` import behind the script-only backtest block.
- Deferred top-level `warnings` import from `lumibot.__init__` into rare warning branches.
- Restored backtesting provider and `Trader` module-level patch points as lazy classes so tests/extensions can monkeypatch without forcing backtesting imports.
- Kept provider package exports returning real classes instead of lazy class proxies because subclassing and identity compatibility depends on real provider classes.

## Audited Dead Ends

- Live strategy construction should keep loading `Position` and Decimal by default. `_Strategy.__init__` immediately snapshots live balances into a cash `Position`, preserving `strategy.cash`, `portfolio_value`, quote-position tracking, and `get_positions(include_cash_positions=True)` semantics.
- Deferring live `update_broker_balances()` and `_set_initial_positions()` out of `Strategy.__init__` could make `DiversifiedLeverage` construction even cheaper, but it is not broad-safe because user code may read `cash`, `portfolio_value`, or positions immediately after construction or before the first executor sync.
- Deferring `StrategyExecutor` construction for all live/backtest strategies is not broad-safe. Brokers can emit subscriber events before first executor access, and many backtest tests/custom workflows call `strategy._executor.process_queue()` immediately. The deferral is scoped to scheduled live strategies with no active stream/order thread.
- Deferring `_strategy`'s `credentials` config constants from bare `Strategy` import is now done only for scheduled live imports. Non-scheduled imports still preserve existing `.env`/credential side effects.
- Deferring `Strategy` import of `credentials` for non-scheduled users is still a visible side-effect change because it would delay `.env` loading. Keep the deferral scoped to scheduled live execution.
- Package-level provider export proxies can save another small amount on provider export imports, but public class identity/subclassing behavior changes (`issubclass(Alpaca, Broker)`, `class Child(Alpaca)`) make it a medium-risk compatibility change.
- Scheduled-only lazy broker instance proxies can save another ~2-3ms on `credentials.BROKER.name`, but `BROKER` public identity/monkeypatch behavior changes make it a medium-high-risk follow-up, not a default-safe patch.
- Moving IBKR REST `urllib3.disable_warnings()` from constructor into `start()` does not improve current constructor startup because `InteractiveBrokersRESTData.__init__()` immediately calls `start()`; a real win would require a separate lazy-start mode.
- Top-level `LUMIBOT_DEFAULT_PYTZ` could be made lazy, but returning a non-pytz proxy from `lumibot.LUMIBOT_DEFAULT_PYTZ` is a medium-risk public identity/compatibility change. Keep top-level default timezone eager unless that API contract is relaxed.

## Verified Tests

Latest broad touched-provider run after provider-export cleanup, cron scheduler regression fix, backtest CSV opt-in, lazy `Asset`/`Order`/`datetime`/timezone/JSON/Decimal/smart-limit deferrals, lazy strategy logger, `Position` annotation deferral, lazy timezone compatibility fixes, constructor logger cleanup, runtime `typing` deferral, Trader import cleanup, scheduled `_strategy` credential deferral, scheduled Alpaca constructor defaults and data-source deferral, scheduled executor construction deferral, and Asset datetime deferral:

```bash
.venv/bin/pytest -m 'not apitest' \
  tests/test_credentials_disable_dotenv.py \
  tests/test_lazy_exports.py \
  tests/test_scheduled_run_once.py \
  tests/test_notifications_and_memory.py \
  tests/test_indicators_unit.py \
  tests/test_agent_tool_permissions.py \
  tests/test_strategy_methods.py \
  tests/test_alpaca.py \
  tests/test_alpaca_oauth.py \
  tests/test_alpaca_auth_fix.py \
  tests/test_alpaca_multileg_fix.py \
  tests/test_alpaca_backtesting.py \
  tests/test_broker_initialization.py \
  tests/test_tradier.py \
  tests/test_tradier_force_refresh.py \
  tests/test_ccxt.py \
  tests/test_broker_bitunix.py \
  tests/test_projectx.py \
  tests/test_projectx_data.py \
  tests/test_projectx_datetime_columns.py \
  tests/test_projectx_datetime_index.py \
  tests/test_projectx_timestep_alias.py \
  tests/test_projectx_lifecycle.py \
  tests/test_projectx_lifecycle_unit.py \
  tests/test_projectx_bracket_lifecycle_unit.py \
  tests/test_projectx_url_mappings.py \
  tests/test_tradovate.py \
  tests/test_order.py \
  tests/test_asset.py \
  tests/test_smart_limit_multileg_unit.py \
  tests/test_smart_limit_single_leg_unit.py \
  tests/test_backtesting_parameters.py \
  tests/test_backtesting_data_source_env.py \
  tests/test_backtesting_datetime_normalization.py \
  tests/backtest/test_example_strategies.py::TestExampleStrategies::test_stock_diversified_leverage \
  -q
```

Result: `409 passed, 5 skipped, 7 deselected`.

Latest focused startup/backtest slice:

```bash
.venv/bin/python -m pytest tests/test_lazy_exports.py tests/test_scheduled_run_once.py \
  tests/test_strategy_methods.py tests/test_smart_limit_multileg_unit.py \
  tests/test_smart_limit_single_leg_unit.py tests/test_backtesting_parameters.py \
  tests/test_backtesting_data_source_env.py tests/test_backtesting_datetime_normalization.py \
  tests/backtest/test_example_strategies.py::TestExampleStrategies::test_stock_diversified_leverage -q
```

Result: `107 passed`.

Latest focused Alpaca/lazy/scheduled regression slice after scheduled Alpaca data-source deferral:

```bash
.venv/bin/python -m pytest tests/test_lazy_exports.py tests/test_alpaca.py \
  tests/test_alpaca_data.py tests/test_broker_initialization.py tests/test_scheduled_run_once.py \
  tests/test_strategy_methods.py \
  tests/backtest/test_example_strategies.py::TestExampleStrategies::test_stock_diversified_leverage -q
```

Result: `90 passed, 6 skipped`.

Latest scheduled run-once guard after wall-clock calendar initialization:

```bash
.venv/bin/python -m pytest tests/test_scheduled_run_once.py -q
```

Result: `15 passed`.

Latest lazy/startup guard:

```bash
.venv/bin/python -m pytest tests/test_lazy_exports.py -q
```

Result: `32 passed`.

Latest changed-area provider/backtest smoke:

```bash
.venv/bin/pytest tests/test_lazy_exports.py tests/test_broker_initialization.py \
  tests/test_broker_bitunix.py tests/test_ccxt.py tests/test_tradier.py \
  tests/test_tradier_force_refresh.py tests/test_projectx.py tests/test_projectx_data.py \
  tests/test_tradovate.py \
  tests/backtest/test_example_strategies.py::TestExampleStrategies::test_stock_diversified_leverage -q
```

Result: `146 passed, 6 skipped`.

Latest broad touched-provider run after scheduled lazy credentials and stream/entity/telemetry deferral:

```bash
.venv/bin/pytest -m 'not apitest' \
  tests/test_credentials_disable_dotenv.py \
  tests/test_lazy_exports.py \
  tests/test_scheduled_run_once.py \
  tests/test_notifications_and_memory.py \
  tests/test_indicators_unit.py \
  tests/test_agent_tool_permissions.py \
  tests/test_alpaca.py \
  tests/test_alpaca_oauth.py \
  tests/test_alpaca_auth_fix.py \
  tests/test_alpaca_multileg_fix.py \
  tests/test_alpaca_backtesting.py \
  tests/test_broker_initialization.py \
  tests/test_tradier.py \
  tests/test_tradier_force_refresh.py \
  tests/test_ccxt.py \
  tests/test_broker_bitunix.py \
  tests/test_projectx.py \
  tests/test_projectx_data.py \
  tests/test_projectx_datetime_columns.py \
  tests/test_projectx_datetime_index.py \
  tests/test_projectx_timestep_alias.py \
  tests/test_projectx_lifecycle.py \
  tests/test_projectx_lifecycle_unit.py \
  tests/test_projectx_bracket_lifecycle_unit.py \
  tests/test_projectx_url_mappings.py \
  tests/test_tradovate.py \
  tests/test_order.py \
  tests/test_smart_limit_multileg_unit.py \
  tests/backtest/test_example_strategies.py::TestExampleStrategies::test_stock_diversified_leverage \
  -q
```

Result: `315 passed, 5 skipped, 7 deselected`.

Latest focused stream/entity/logger/timezone/order-thread follow-up:

```bash
.venv/bin/pytest tests/test_lazy_exports.py tests/test_broker_initialization.py \
  tests/test_scheduled_run_once.py tests/test_alpaca.py tests/test_alpaca_oauth.py \
  tests/test_order.py tests/test_smart_limit_multileg_unit.py \
  tests/test_broker_bitunix.py tests/test_tradovate.py -q
```

Result: `153 passed, 6 skipped`.

Latest focused startup/credentials runs after scheduled lazy credentials and Bitunix/Schwab follow-up:

```bash
.venv/bin/pytest tests/test_credentials_disable_dotenv.py tests/test_lazy_exports.py \
  tests/test_scheduled_run_once.py tests/test_broker_initialization.py tests/test_broker_bitunix.py -q
```

Result: `52 passed`.

```bash
.venv/bin/pytest tests/test_lazy_exports.py::test_bitunix_helper_import_defers_requests \
  tests/test_broker_initialization.py::test_schwab_base_init_does_not_launch_stream_before_client_setup \
  tests/test_broker_initialization.py::test_schwab_force_refresh_on_startup_rewrites_token \
  tests/test_broker_bitunix.py -q
```

Result: `16 passed`.

Earlier broad targeted run after constants deferral:

```bash
.venv/bin/pytest -m 'not apitest' \
  tests/test_lazy_exports.py \
  tests/test_scheduled_run_once.py \
  tests/test_notifications_and_memory.py \
  tests/test_indicators_unit.py \
  tests/test_agent_tool_permissions.py \
  tests/test_alpaca.py \
  tests/test_alpaca_oauth.py \
  tests/test_alpaca_auth_fix.py \
  tests/test_alpaca_multileg_fix.py \
  tests/test_alpaca_backtesting.py \
  tests/test_broker_initialization.py \
  tests/backtest/test_example_strategies.py::TestExampleStrategies::test_stock_diversified_leverage \
  -q
```

Result: `112 passed, 1 skipped, 5 deselected`.

Additional provider target:

```bash
.venv/bin/pytest tests/test_tradier.py tests/test_tradier_force_refresh.py -q
```

Result: `30 passed, 3 skipped`.

CCXT target:

```bash
.venv/bin/pytest tests/test_ccxt.py tests/test_lazy_exports.py -q
```

Result: `14 passed`.

Additional provider targets after Schwab/Bitunix/ProjectX/IBKR REST/Tradovate import deferral:

```bash
.venv/bin/pytest tests/test_broker_bitunix.py \
  tests/test_projectx.py tests/test_projectx_data.py tests/test_projectx_datetime_columns.py \
  tests/test_projectx_datetime_index.py tests/test_projectx_timestep_alias.py \
  tests/test_projectx_lifecycle.py tests/test_projectx_lifecycle_unit.py \
  tests/test_projectx_bracket_lifecycle_unit.py tests/test_projectx_url_mappings.py \
  tests/test_tradovate.py tests/test_lazy_exports.py::test_startup_class_exports_defer_heavy_dependencies \
  -q
```

Results before ProjectX/Tradovate lazy-auth follow-up: Bitunix `13 passed`; ProjectX target `71 passed, 2 skipped`; Tradovate target `37 passed, 1 skipped`. Lazy startup regression passed in each target.

ProjectX lazy-auth follow-up:

```bash
.venv/bin/pytest tests/test_projectx.py tests/test_projectx_data.py \
  tests/test_projectx_datetime_columns.py tests/test_projectx_datetime_index.py \
  tests/test_projectx_timestep_alias.py tests/test_projectx_lifecycle.py \
  tests/test_projectx_lifecycle_unit.py tests/test_projectx_bracket_lifecycle_unit.py \
  tests/test_projectx_url_mappings.py tests/test_lazy_exports.py::test_startup_class_exports_defer_heavy_dependencies \
  -q
```

Result: `75 passed, 2 skipped`.

Tradovate lazy-connect follow-up:

```bash
.venv/bin/pytest tests/test_tradovate.py -q
```

Result: `38 passed, 1 skipped`.

Smaller targeted run:

```bash
.venv/bin/pytest -m 'not apitest' \
  tests/test_lazy_exports.py \
  tests/test_scheduled_run_once.py \
  tests/test_alpaca.py \
  tests/test_alpaca_oauth.py \
  tests/test_alpaca_auth_fix.py \
  tests/test_alpaca_multileg_fix.py \
  tests/test_alpaca_backtesting.py \
  tests/test_broker_initialization.py \
  tests/backtest/test_example_strategies.py::TestExampleStrategies::test_stock_diversified_leverage \
  -q
```

Result: `76 passed, 1 skipped, 5 deselected`.

## Dead Ends

- Rust rewrites: no useful target found. Startup cost was import graph, SDK/client setup, calendar setup, and broker I/O, not CPU-bound compute.
- Deferring live broker balance/position sync from strategy construction: rejected for now. Real scheduled deployments need fresh account state before iteration, and pre-start can use that time before exact target execution.

## Remaining Work

- `Order` import deferral remains the largest visible project-level startup opportunity on the scheduled `Strategy`/Alpaca broker path. It touches about 195 runtime references across strategy, broker, and Alpaca order handling, so it should be done as a dedicated refactor with broad order lifecycle coverage rather than mixed into provider cleanup.
- Consider optional deferred gateway start for IBKR REST behind an explicit constructor parameter; current patch only defers import cost because constructor currently pulls/runs Docker and authenticates.
- Consider a narrow scheduled-prestart benchmark with real paper credentials in a safe environment, if available.
