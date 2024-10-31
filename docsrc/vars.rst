Variable Backup & Restore
=========================

Every `Strategy` object includes a `vars` attribute (`self.vars`), which is an instance of the `Vars` class. It stores the strategy's runtime variables and is periodically backed up to the database provided with the `DB_CONNECTION_STR` environment variable.

.. important::

  We strongly recommend using `self.vars` for storing your strategy's runtime variables, as this ensures that if the strategy stops unexpectedly, it can safely resume from the backup state stored in `self.vars`.

Class Definition
----------------

.. code-block:: python

  class Vars:
    def __init__(self):
      super().__setattr__('_vars_dict', {})

    def __getattr__(self, name):
      try:
        return self._vars_dict[name]
      except KeyError:
        raise AttributeError(f"'Vars' object has no attribute '{name}'")

    def __setattr__(self, name, value):
      self._vars_dict[name] = value

    def set(self, name, value):
      self._vars_dict[name] = value

    def all(self):
      return self._vars_dict.copy()

Usage Examples
--------------

**Set attributes:**

.. code-block:: python

  self.vars.some_var = 10
  self.vars.another_var = "Hello, World!"

**Get attributes:**

.. code-block:: python

  print(self.vars.some_var)    # Output: 10
  print(self.vars.another_var) # Output: Hello, World!

**Get all attributes:**

.. code-block:: python

  print(self.vars.all())  # Output: {'some_var': 10, 'another_var': 'Hello, World!'}

.. tip::

   **Tip:** Use the `all()` method to get a copy of all stored variables in a dictionary format.