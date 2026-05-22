Imports and Startup
===================

LumiBot package initializers use lazy exports for high-level namespaces such as
``lumibot``, ``lumibot.brokers``, ``lumibot.data_sources``,
``lumibot.entities``, ``lumibot.tools``, and ``lumibot.traders``.

The goal is to keep common startup paths fast by avoiding broker SDKs,
market-data helpers, plotting/dataframe utilities, CCXT-related helpers, and
provider tooling until a caller actually asks for that object.

Supported Import Styles
-----------------------

These import styles remain supported:

.. code-block:: python

   import lumibot
   from lumibot.brokers import Alpaca
   from lumibot.entities import Asset, Order
   from lumibot.tools import parse_symbol
   from lumibot.tools import *

Legacy ``entities`` imports are also preserved:

.. code-block:: python

   import entities
   from entities.asset import Asset
   from entities.order import Order

These compatibility aliases are installed only when another real ``entities``
package is not already present.

Lazy Import Semantics
---------------------

Lazy exports change import timing, not trading behavior:

- Importing a namespace should be cheap.
- Accessing a concrete export imports the underlying implementation module.
- Missing optional dependencies or broken feature modules should fail when that
  feature is accessed.
- The failure should be explicit. Lazy loading must not silently skip broker,
  data-source, order, fill, or accounting behavior.

This means ``import lumibot`` may succeed even if an optional package needed by
a specific broker is missing. The broker import should still fail when the
broker is accessed.

Market Data Hot Paths
---------------------

Market-data providers and helpers may defer dataframe, provider SDK, plotting,
and math dependencies until the concrete provider path is used. If a dependency
is missing, the error can surface when constructing or calling that provider
rather than during ``import lumibot``. To debug, import the concrete provider
class or call a minimal provider method directly.

ProjectX helpers lazily resolve ``pandas``, ``requests``, and
``signalrcore.hub_connection_builder``. ``SIGNALR_AVAILABLE`` only reports
whether the SignalR builder module can be discovered; call
``_hub_connection_builder_class()`` at runtime when constructing a streaming
connection so missing optional SignalR dependencies fail at the streaming
feature boundary rather than during LumiBot import.

``Bars`` and ``Data.get_bars()`` include fast paths for already-normalized
historical slices. ``Bars.from_pandas_fast``, ``skip_timezone``, and
``return_polars`` avoid unnecessary conversion when the caller has already
selected the backend and timezone semantics. Naive timestamps still use the
default LumiBot timezone; only pass ``skip_timezone`` when the data is already
localized correctly.

ThetaData split/dividend normalization uses the current simulation datetime, or
``BACKTESTING_END`` for deterministic full-window backtests, as the corporate
action horizon. This keeps option strike reconstruction and chain handling
consistent across warm-cache and replay runs.

``lumibot.tools.black_scholes`` now uses a local normal-distribution helper for
the ``cdf``/``pdf`` surface it needs, so that path no longer depends on loading
``scipy.stats.norm`` at runtime.

Symbol Parsing
--------------

``lumibot.tools.parse_symbol`` and ``lumibot.tools.helpers.parse_symbol`` share
the same implementation in ``lumibot.tools.symbol_parser``.

The parser:

- strips whitespace and uppercases symbols;
- returns ``{"type": None}`` for non-string or empty input;
- requires full OCC-style option symbol matches;
- parses OCC ``YYMMDD`` years as ``2000 + YY``;
- returns stock symbols after normalization when the option pattern does not
  match.

The parser is intentionally conservative so live order symbols are not
misclassified by partial matches.
