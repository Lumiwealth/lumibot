Bybit CCXT Backtesting
======================

Bybit is documented as a CCXT backtesting exchange id in Lumibot. It is not
currently one of the global auto-detected live credential paths.

Status
------

* **Live trading path:** not auto-detected by Lumibot's global credentials
* **Backtesting:** documented CCXT backtesting exchange id: ``bybit``
* **Asset class:** crypto

Backtesting
-----------

.. code-block:: bash

   export BACKTESTING_DATA_SOURCE=bybit

Use Bybit backtesting when you need historical crypto bars through the CCXT data
path. Do not assume a live Bybit broker path is production-ready until an
explicit ``Ccxt`` config has been validated for balances, positions, orders,
fills, cancellation, fees, and product semantics.
