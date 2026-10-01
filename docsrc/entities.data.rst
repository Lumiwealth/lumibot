Data
----------------------------

.. meta::
   :description: Data LumiBot documentation in the LumiBot Python trading framework.

Intraday bar visibility
~~~~~~~~~~~~~~~~~~~~~~

Minute and hour bars are timestamped at their start. History includes a bar
after its full interval closes, even if the next bar has not arrived. While a
bar is forming, last-price and close-derived quote prices use its open rather
than its future close. Polars-backed data applies the same rules for
nanosecond, microsecond, and millisecond timestamps. Actual quote snapshots
retain their separate pricing semantics.

.. automodule:: lumibot.entities.data
   :noindex:
   :members:
   :undoc-members:
   :show-inheritance:
