Reference
=========

.. meta::
   :description: Reference. Reference documentation for specialized Lumibot features.

Reference documentation for specialized Lumibot features.

Start with :doc:`strategy_api_overview` for the small set of Strategy methods
used in most examples, then use the categorized reference pages for complete
signatures and specialized behavior.

.. toctree::
   :maxdepth: 2

   vars
   futures
   smart_limit
   options_helper
   environment_variables

Frequently asked questions
--------------------------

What is in the LumiBot reference section?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It covers specialized features: :doc:`vars` (saving strategy variables),
:doc:`futures`, :doc:`smart_limit`, :doc:`options_helper`, and
:doc:`environment_variables`. For the Strategy methods used in most examples,
start with :doc:`strategy_api_overview`.

Which asset types does LumiBot support?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``Asset.AssetType`` values are ``stock``, ``option``, ``future``,
``crypto_future``, ``cont_future`` (continuous future), ``forex``, ``crypto``,
``prediction_contract``, ``index``, and ``multileg``. Which ones you can trade
or backtest also depends on your broker and data source.

How do I keep strategy variables after my bot restarts?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Store them in ``self.vars`` and set the ``DB_CONNECTION_STR`` environment
variable to a PostgreSQL database. LumiBot loads saved variables before the
first trading iteration and backs them up after each iteration and when the bot
stops or crashes. See :doc:`vars`.

What is a continuous future in LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A continuous future is an asset created with
``Asset.AssetType.CONT_FUTURE``, for example ``Asset("ES",
asset_type=Asset.AssetType.CONT_FUTURE)``. It has no expiration date to manage,
so it is the recommended choice for futures backtests. See :doc:`futures`.

How do I always trade the front month futures contract?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create the future with ``auto_expiry=Asset.AutoExpiry.FRONT_MONTH``. LumiBot
then works out the expiration date from the current date. This is useful for
live trading when you always want the front month.

What is a SMART_LIMIT order?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A SMART_LIMIT order is a limit order that starts at the midpoint of the bid and
ask, then steps toward the bid or ask on a timer. The FAST, NORMAL, and PATIENT
presets use 3, 4, and 5 price levels. If it is still unfilled after the final
hold window, it is canceled. See :doc:`smart_limit`.

How do SMART_LIMIT orders fill in a backtest?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In a backtest, a buy fills at the midpoint plus slippage and a sell fills at
the midpoint minus slippage. If bid and ask quotes are missing, the order falls
back to a market order fill at the next bar open.

What is OptionsHelper in LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``OptionsHelper`` picks option expirations, strikes, and deltas and builds
multi-leg orders. It checks that an option has usable market data, which makes
options backtests more reliable and faster than scanning many strikes. Create it
with ``self.options_helper = OptionsHelper(self)`` after importing it from
``lumibot.components.options_helper``. See :doc:`options_helper`.
