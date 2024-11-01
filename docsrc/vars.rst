Variable Backup & Restore
=========================

Every `Strategy` object has a `vars` attribute (`self.vars`), an instance of the `Vars` class. It stores runtime variables and is periodically backed up to the database specified by the `DB_CONNECTION_STR` environment variable.

How It Works
------------

- **Loading Variables:** Before the first trading iteration, saved variables are loaded into `self.vars`.

- **Backing Up Variables:** After each trading iteration and when the bot stops or crashes, `self.vars` is automatically backed up to the database.

Benefits of `self.vars`
-----------------------

- **Persistence:** Resume from the last state after interruptions.
- **Namespacing:** Prevent conflicts with other components or strategies.
- **Scalability:** Efficient storage, especially when scaling or using multiple strategies.

Usage Guide
-----------

**Setting Variables:**

Assign values using attribute notation:

.. code-block:: python

   def on_trading_iteration(self):
       self.vars.trade_count = self.vars.get('trade_count', 0) + 1

**Accessing Variables:**

Retrieve variables using attribute notation:

.. code-block:: python

   def on_trading_iteration(self):
       current_count = self.vars.trade_count
       print(f"Current trade count: {current_count}")

**Getting All Variables:**

Use the `all()` method:

.. code-block:: python

   all_variables = self.vars.all()
   print(all_variables)

   # Output: {'trade_count': 5, 'last_price': 102.5}

.. tip::

   Use `all()` for debugging or processing multiple variables at once.

Database Configuration
----------------------

Set the `DB_CONNECTION_STR` environment variable in your .env file:

.. code-block:: bash

   DB_CONNECTION_STR="postgresql://user:password@localhost:5432/database_name"

Database Storage Structure
--------------------------

Variables are stored in a PostgreSQL table:

- **Table Name:** Defined by `self.backup_table_name` (default is `vars_backup`).

- **Columns:**
  - `id` (Primary Key)
  - `last_updated`: Timestamp of the last backup.
  - `variables`: JSON string of the strategy's variables (`self.vars.all()`).
  - `strategy_id`: Strategy name (`self.name`).

Example Database:

+------------+---------------------+---------------------------+-------------------------+
| **id**     | **last_updated**    | **variables**             | **strategy_id**         |
+============+=====================+===========================+=========================+
| 550e8400   | 2023-10-05 14:30:00 | {"var1": 10, "var2": "A"} | OptionsCondorMartingale |
+------------+---------------------+---------------------------+-------------------------+
