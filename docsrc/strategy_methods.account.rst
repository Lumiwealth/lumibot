Account Management
===================================

Account management functions are used to get your account value, cash, etc. You can see a list of them below.

For the full cash-accounting flow, including how these methods affect
cash-adjusted returns and live ``cash_events``, see :doc:`cash_accounting`.

.. currentmodule:: lumibot.strategies.strategy

.. autosummary::
    :toctree: strategy_methods.account
    :template: strategy_methods_template.rst

        Strategy.adjust_cash
        Strategy.configure_cash_financing
        Strategy.deposit_cash
        Strategy.get_portfolio_value
        Strategy.get_cash
        Strategy.get_position
        Strategy.get_positions
        Strategy.set_cash_financing_rates
        Strategy.withdraw_cash
