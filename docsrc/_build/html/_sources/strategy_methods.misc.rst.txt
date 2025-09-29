Miscellaneous
===================================

Miscellaneous methods are the methods that do not fit into other categories. You can use these methods to log messages to your log files, sleep for a few seconds and more. You can see a list of them below:

.. currentmodule:: lumibot.strategies.strategy

.. autosummary::
    :toctree: strategy_methods.misc
    :template: strategy_methods_template.rst

        Strategy.log_message
        Strategy.sleep
        Strategy.set_market
        Strategy.update_parameters
        Strategy.get_parameters
        Strategy.await_market_to_close
        Strategy.await_market_to_open
        Strategy.wait_for_order_registration
        Strategy.wait_for_order_execution
        Strategy.wait_for_orders_registration
        Strategy.wait_for_orders_execution
        Strategy.register_cron_callback
