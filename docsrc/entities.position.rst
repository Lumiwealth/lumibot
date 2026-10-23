Position
--------------------------------

.. meta::
   :description: Position LumiBot documentation in the LumiBot Python trading framework.

.. automodule:: lumibot.entities.position
   :noindex:
   :members:
   :undoc-members:
   :show-inheritance:

Derivative position identity
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``Position`` and ``Order`` accept optional ``position_side`` (``"long"`` or
``"short"``) and ``position_id`` fields. A one-way position keeps the default
``None`` side. These fields survive serialization, fills and synchronization;
the order's buy/sell direction remains separate from its position leg.

For example, a strategy can hold a long position of ten contracts and a short
position of four contracts in the same instrument. ``get_positions()`` returns
both. Select one with ``get_position(asset, position_side="short")`` or
``close_position(asset, position_side="short")``. An unselected single-position
lookup raises when multiple legs match, rather than silently selecting one.
``close_position(asset)`` closes every matching leg and returns a list when
multiple orders are needed; existing single-position callers retain their
single-order return value. Derivative closing orders preserve leg identity
and reduce-only intent.

An adapter must also map these fields to its venue's account modes and native
requests. Entity support alone does not qualify an exchange's hedge trading.
