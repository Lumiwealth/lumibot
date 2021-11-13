Position
--------------------------------

This object represents a position. Each position belongs to a specific strategy. Position object has the following properties

* strategy (str): the strategy name that this order belongs to
* symbol (str): the string representation of the asset e.g. "GOOG" for Google
* quantity (int): the number of shares held
* orders (list(order)): a list of orders objects responsible for the current state of the position

Position objects have also the following helper methods

* get_selling_order(): returns an order for selling all the shares attached to this position.

Documentation
"""""""""""""""""""

.. automodule:: entities.position
   :members:
   :undoc-members:
   :show-inheritance: