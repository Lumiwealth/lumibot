Strategy Methods
===================================

Strategy methods are the methods that you will use inside of a strategy to do things such as submit orders, get pricing data and more. We have divided them into sections for you so that you get get a sense of what each one is used for.

Every strategy method will be used inside of one of your functions or lifecycle methods (eg. inside of **on_trading_iteration()**) and is usually preceded by "**self.**" (eg. **self.submit_order()**)

Check out the list of available strategy methods below:

.. toctree::
   :maxdepth: 3
   :caption: Contents:
   
   strategy_methods.orders
   strategy_methods.account
   strategy_methods.data
   strategy_methods.chart
   strategy_methods.parameters
   strategy_methods.options
   strategy_methods.datetime
   strategy_methods.misc
