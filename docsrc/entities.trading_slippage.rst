Trading Slippage
-------------------------------

.. meta::
   :description: TradingSlippage is a backtesting-only execution cost used by SMART_LIMIT fills. You can provide slippage at the strategy level: LumiBot documentation.

TradingSlippage is a backtesting-only execution cost used by SMART_LIMIT fills.
You can provide slippage at the strategy level:

.. code-block:: python

   from lumibot.entities import TradingSlippage

   slippage = TradingSlippage(amount=0.05)
   MyStrategy.backtest(
       ...,
       buy_trading_slippages=[slippage],
       sell_trading_slippages=[slippage],
   )

.. automodule:: lumibot.entities.trading_slippage
   :noindex:
   :members:
   :undoc-members:
   :show-inheritance:
