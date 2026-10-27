Imports and Startup
===================

.. meta::
   :description: LumiBot package initializers use lazy exports for high-level namespaces such as lumibot, lumibot.brokers, lumibot.data_sources, lumibot.entities, lumibot.tools.

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

Frequently asked questions
--------------------------

Why does import lumibot load so fast?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LumiBot uses lazy exports. Importing a namespace such as ``lumibot`` or
``lumibot.brokers`` is cheap. The code behind a broker, data source, or tool is
only imported when you actually use that object.

How do I import YahooDataBacktesting in LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``from lumibot.backtesting import YahooDataBacktesting``. The same package
also exports ``PolygonDataBacktesting`` and ``PandasDataBacktesting``. Import
your strategy base class with ``from lumibot.strategies import Strategy``.

Which import styles does LumiBot support?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use ``import lumibot`` or import names from a subpackage, such as
``from lumibot.brokers import Alpaca``, ``from lumibot.entities import Asset,
Order``, and ``from lumibot.tools import parse_symbol``. ``from lumibot.tools
import *`` also works.

Why does import lumibot work but my broker import fails?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Because of lazy loading, ``import lumibot`` can succeed even when a package that
one broker needs is missing. The error shows up when you access that broker,
not at import time. Install the optional package that broker needs.

Does lazy loading change how my strategy trades?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. Lazy exports only change when code is imported. They do not change broker,
data source, order, fill, or accounting behavior, and a broken feature fails
with a clear error instead of being skipped.

Do old entities imports still work?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. Imports such as ``from entities.asset import Asset`` still work. LumiBot
only adds these compatibility names when no other real ``entities`` package is
already installed.

How do I hide the LumiBot starting message?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set ``LUMIBOT_LOG_LEVEL=ERROR`` before you import LumiBot. That hides the
``LumiBot v... starting`` line and skips the console logger setup at import
time. See :doc:`environment_variables`.

What does parse_symbol return?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``parse_symbol`` returns a dictionary. A full OCC option symbol such as
``SPY250117C00500000`` returns ``"type": "option"`` with the underlying symbol,
expiration date, option type, and strike price. Any other text returns
``"type": "stock"`` with the cleaned, uppercase symbol. Empty input or a value
that is not a string returns ``{"type": None}``.
