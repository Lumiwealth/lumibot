# Imports And Startup

> How LumiBot keeps package imports lightweight while preserving legacy public import paths.

**Last Updated:** 2026-05-19
**Status:** Active
**Audience:** Developers + AI Agents

---

## Overview

LumiBot package initializers use lazy exports for high-level namespaces such as `lumibot`, `lumibot.brokers`, `lumibot.data_sources`, `lumibot.entities`, `lumibot.tools`, and `lumibot.traders`.

The goal is to make common startup paths faster and less fragile by avoiding broker SDKs, market-data helpers, plotting/dataframe utilities, CCXT-related helpers, and agent/provider tooling until a caller actually asks for that object.

---

## Public Import Contract

Supported import styles still include:

```python
import lumibot
from lumibot.brokers import Alpaca
from lumibot.entities import Asset, Order
from lumibot.tools import parse_symbol
from lumibot.tools import *
```

Legacy `entities` imports are also preserved:

```python
import entities
from entities.asset import Asset
from entities.order import Order
```

These compatibility aliases are installed only when another real `entities` package is not already present.

---

## Lazy Import Semantics

Lazy exports change import timing, not trading behavior:

- Importing a namespace should be cheap.
- Accessing a concrete export imports the underlying implementation module.
- Missing optional dependencies or broken feature modules should fail when that feature is accessed.
- The failure should be explicit. Lazy loading must not silently skip broker, data-source, order, fill, or accounting behavior.

This means `import lumibot` may succeed even if an optional package needed by a specific broker is missing. The broker import should still fail when the broker is accessed.

## Market Data Hot Path Notes

Several market-data modules also use small local lazy loaders for dataframe,
provider SDK, and plotting dependencies. These loaders are intentionally scoped
to the module that owns the dependency (`yahoo_data`, DataBento/Polars helpers,
Tradovate data, and option/math helpers) so deferred import errors point at the
feature being used. To debug an optional dependency issue, import the concrete
provider class or call the first provider method in an isolated shell instead of
only testing `import lumibot`.

ProjectX helpers lazily resolve `pandas`, `requests`, and
`signalrcore.hub_connection_builder`. `SIGNALR_AVAILABLE` only reports whether
the SignalR builder module can be discovered; call
`_hub_connection_builder_class()` at runtime when constructing a streaming
connection so missing optional SignalR dependencies fail at the streaming
feature boundary rather than during LumiBot import.

`Bars` and `Data.get_bars()` can now keep already-normalized slices on a fast
path. `Bars.from_pandas_fast`, `skip_timezone`, and `return_polars` avoid
unnecessary dataframe conversion when the caller has already selected the
backend and timezone semantics. Callers that pass naive timestamps still get the
normal `LUMIBOT_DEFAULT_PYTZ` handling; fast-path callers are responsible for
only using `skip_timezone` with data that is already correctly localized.

ThetaData corporate-action normalization uses the simulation datetime, or
`BACKTESTING_END` for deterministic full-window backtests, as the split/dividend
horizon. This keeps option strike reconstruction, frame normalization, and chain
handling consistent across warm-cache and live-like replay runs.

The local Black-Scholes normal distribution replaces the previous runtime
`scipy.stats.norm` dependency for the small `cdf`/`pdf` surface used by
`black_scholes.py`. The API shape is preserved, but missing SciPy should no
longer block this helper path.

---

## Compatibility Tests

Before releasing import/startup changes, run:

```bash
python3 -m pytest -q tests/test_lazy_exports.py tests/test_symbol_parser.py
```

The lazy export suite verifies:

- every public `__all__` export resolves;
- common heavy submodules are deferred until first use;
- star imports still expose legacy helper names;
- patching via string paths still works for tests;
- legacy `entities` package aliases resolve and reload.

---

## Symbol Parsing

`lumibot.tools.parse_symbol` and `lumibot.tools.helpers.parse_symbol` share the same implementation in `lumibot.tools.symbol_parser`.

The parser:

- strips whitespace and uppercases symbols;
- returns `{"type": None}` for non-string or empty input;
- requires full OCC-style option symbol matches;
- parses OCC `YYMMDD` years as `2000 + YY`;
- returns stock symbols unchanged after normalization when the option pattern does not match.

Keep this parser conservative. Do not add partial matches or permissive fallback behavior that could misclassify a live order symbol.
