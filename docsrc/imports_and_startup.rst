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
