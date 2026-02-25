.. _backtesting.trades_files:

Trades Files
============

The **Trades HTML** and **Trades CSV** files provide detailed information about each trade executed by the strategy. This includes:

- **Buy and Sell Orders:** The times and prices at which buy or sell orders were placed, along with the asset involved (e.g., option strike price or stock ticker).
- **Portfolio Value:** The value of the portfolio at each time point.
- **Cash:** The amount of cash available at each time point.

Option Lifecycle Statuses
-------------------------

For options backtests, expiration outcomes are exported as explicit lifecycle events in the trade artifacts.

- ``cash_settled``: cash settlement at intrinsic value (used for cash-settled products, such as index options).
- ``assigned``: short in-the-money physically-settled option assignment at expiration.
- ``exercised``: long in-the-money physically-settled option exercise at expiration.
- ``expired``: out-of-the-money expiration (or long ITM contracts that cannot be exercised due to account constraints in simulation).

When physical settlement occurs, LumiBot also writes the underlying delivery row as a separate trade event
(``status=fill``) with ``type`` set to the originating lifecycle event (for example, ``type=assigned`` or
``type=exercised``). This allows downstream charting/reporting systems to distinguish ordinary trades from
assignment/exercise delivery.

.. figure:: _html/images/trades_example.png
   :alt: Trades example
   :width: 600px
   :align: center
