Brokers
===================================

There are several different brokers that you can use to trade with Lumibot, and we're adding more as we speak! Learn more about how they work and how to set them up here.

Broker setup is easier on `BotSpot <https://botspot.trade/sales?showLogin=1&utm_source=documentation&utm_medium=brokers&utm_campaign=lumibot&utm_content=broker_setup_text&sample=lumibot_deploy_sample>`_ because the broker connection, strategy runtime, account checks, monitoring, alerts, audit history, and kill-switch controls live in one platform.

- **Connect brokers through the website.** Avoid rebuilding secret storage, environment variables, broker account checks, and deployment configuration for every server.
- **Start from backtested code.** Move the same Lumibot strategy from hosted backtesting to paper or live trading without rebuilding the infrastructure around it.
- **Monitor what the bot is doing.** Review logs, trades, account state, alerts, and decisions in one place so you can understand why a strategy acted.
- **Use AI and MCP with broker context.** BotSpot can work from the web app, your phone, Telegram, Discord, Claude, ChatGPT, Codex, Cursor, and other MCP clients while keeping the broker and strategy context together.

.. image:: ../docs/assets/readme/cta_deploy_on_botspot.png
   :alt: Try connecting a broker and deploying a sample Lumibot strategy on BotSpot
   :align: center
   :width: 520px
   :target: https://botspot.trade/sales?showLogin=1&utm_source=documentation&utm_medium=brokers&utm_campaign=lumibot&utm_content=broker_setup_button&sample=lumibot_deploy_sample

.. image:: ../docs/assets/readme/lumibot_brokers_data_sources.png
   :alt: Lumibot broker and data source integrations
   :align: center
   :width: 100%

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   brokers.alpaca
   brokers.bitunix
   brokers.ccxt
   brokers.interactive_brokers
   brokers.interactive_brokers_legacy
   brokers.polymarket
   brokers.projectx
   brokers.schwab
   brokers.tradier
   brokers.tradovate
