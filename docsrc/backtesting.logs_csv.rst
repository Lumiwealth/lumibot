Logs CSV
========

.. meta::
   :description: The Logs CSV file logs all events and actions taken by the strategy during the backtest.

The **Logs CSV** file logs all events and actions taken by the strategy during the backtest. It is useful for debugging and understanding the decision-making process of the strategy.

These files collectively provide a comprehensive view of the backtest, allowing for detailed analysis and refinement of trading strategies.

Market-input observations
-------------------------

Backtests also export ``market_input_manifest.json``, ``market_inputs.csv`` and
``market_input_observations.csv``. The manifest records file checksums and any
incomplete capture. Historical and indicator frames remain distinct from
scalar last-price returns.

An observation with ``purpose=last_price`` records the actual
``get_last_price`` result, its ``valueType``, effective asset and quote currency,
exchange argument and simulation time. Decimal values retain their precision
as text. ``unavailableReason`` distinguishes ``returned_none``, ``non_finite``
and ``lookup_error``; finite zero and negative results remain literal values.
History rows leave these additional scalar fields blank.

Identical same-time returns, including cached reads, are deduplicated. These
counts are observations, not API-call counts. Capture uses the existing
observation limit and reports truncation rather than calling it complete.
It makes no extra market-data requests and does not change trading behavior.
A last-price result can be a historical close or a cached value, and framework
valuation also uses this API. It is not proof of an exchange quote, a fill,
or the strategy's decision.
