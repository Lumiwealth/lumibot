Deployment Guide
================

This guide will walk you through the deployment process for your trading strategy. We will cover the following topics:

- **Choosing Your Deployment Platform:** Decide whether to deploy on **Render** or **Replit**.
- **Deploying to Render:** Step-by-step instructions for deploying on Render.
- **Deploying to Replit:** Step-by-step instructions for deploying on Replit.
- **Secrets Configuration:** Detailed information on setting up your environment variables.
- **Broker Configuration:** Required secrets for different brokers.
- **General Environment Variables:** Additional environment variables required for the strategy to function correctly.

Before deploying your application, ensure that you have the necessary environment variables configured. The environment variables are crucial for the successful deployment of your application. We will cover the required environment variables for different brokers and general environment variables that are essential for the strategy to function correctly.

Example Strategy for Deployment
-------------------------------

.. important::

   **Important:** This example strategy is for those without a strategy ready to deploy. If you have your own strategy, skip to `Choosing Your Deployment Platform <#id1>`_.

Use this example to see the deployment process in action. It’s not intended for real-money use. More details are available in the GitHub repository: `Example Algorithm GitHub <https://github.com/Lumiwealth-Strategies/stock_example_algo>`_

To run the example strategy, click the Deploy to Render button or the Run on Repl.it button. See `Deploying to Render <#id2>`_ and `Deploying to Replit <#id3>`_ for more details.

.. raw:: html

    <div style="display: flex; justify-content: center; gap: 20px; margin-bottom: 20px;">
        <a href="https://render.com/deploy?repo=https://github.com/Lumiwealth-Strategies/stock_example_algo" target="_blank">
            <img src="https://render.com/images/deploy-to-render-button.svg" alt="Deploy to Render" style="width: 200px; height: auto;">
        </a>
        <a href="https://replit.com/new/github/Lumiwealth-Strategies/stock_example_algo" target="_blank">
            <img src="https://replit.com/badge/github/Lumiwealth-Strategies/stock_example_algo" alt="Replit" style="width: 200px; height: auto;">
        </a>
    </div>

Render is recommended for ease of use and affordability. Replit is more expensive but great for in-browser code editing, if you want to see/edit the code directly in your browser.

.. tip::

   **Tip:** Scroll down to the :ref:`Secrets Configuration <secrets-configuration>` section for detailed information on setting up your environment variables.

Choosing Your Deployment Platform
---------------------------------

We recommend using **Render** for deployment because it is easier to use and more affordable compared to Replit. However, **Replit** is an excellent choice for developers who want to edit code directly in the browser.

.. note::

   **Render** costs **$7/month**, while **Replit** is priced at **$25/month**. Choose the platform that best fits your needs and budget.

Deploying to Render
-------------------

Render offers powerful deployment options with easy scalability. Follow these steps to deploy your application on Render:

1. **Click the "Deploy to Render" Button**

   Start by clicking the **"Deploy to Render"** button on the GitHub repository.

   .. figure:: _static/images/render_deploy_button.png
      :alt: Deploy to Render Button
      :align: center

      **Figure 1:** Deploy to Render button on GitHub.

2. **Configure the Blueprint**

   - **Choose a Blueprint Name:** Enter a name for your blueprint, e.g., **"Blueprint Name"**.
   - **Deploy Blueprint:** Click the **"Deploy Blueprint"** button.

   .. figure:: _static/images/render_deploy_blueprint.png
      :alt: Deploy Blueprint on Render
      :align: center

      **Figure 2:** Deploying Blueprint on Render.

3. **Navigate to the Worker**

   - **Navigate to the Background Worker:** Click on the name of the background worker, e.g., **options-butterfly-condor-worker-afas (Starter)** so you can configure this specific bot worker (we are currently in the blueprint configuration, not the bot itself).

   .. figure:: _static/images/render_worker.png
      :alt: Worker on Render
      :align: center

      **Figure 3:** Worker on Render.

4. **Configure Environment Variables**

   - **Select Environment:** On the worker's page, select **Environment** from the left sidebar.
   - **Edit Environment Variables:** Click **Edit** and fill in the required keys as detailed in the :ref:`Secrets Configuration <secrets-configuration>` section. Once you have added your values for the environment variables, click **Save**.
   - **Delete Unnecessary Variables:** If you have any unnecessary environment variables, you can delete them by clicking the **Delete (trashcan)** button next to the variable. One example of an unnecessary variable is `POLYGON_API_KEY` which is only used if you are backtesting.

   .. figure:: _static/images/render_worker_environment.png
      :alt: Environment Settings on Render
      :align: center

      **Figure 4:** Editing Environment Variables on Render.

   .. note::

      **Note:** Only the environment variables required for your chosen broker are mandatory. Refer to the :ref:`Secrets Configuration <secrets-configuration>` section to see which ones are mandatory and which are optional. If an optional environment variable is not needed, **you can delete it.**

5. **Restart the Service**

   After configuring the environment variables, navigate to the top right corner and click **"Manual Deploy"**, then **"Deploy latest commit"** to apply the changes.

   .. figure:: _static/images/render_redeploy.png
      :alt: Restart Service on Render
      :align: center

      **Figure 5:** Redeploying the Service on Render using the latest commit.

6. **View The Logs**

   - **Check the Logs:** Navigate to the **Logs** tab on the left to view the deployment logs and ensure that there are no errors.

   .. figure:: _static/images/render_logs.png
      :alt: Logs on Render
      :align: center

      **Figure 6:** Viewing Logs on Render.

7. **Monitor Bot Performance**

   - **Monitor Performance:** Go to your broker account to monitor the bot's performance and ensure that it is executing trades as expected.

   .. figure:: _static/images/replit_monitor_bot.png
      :alt: Monitor bot performance
      :align: center

      **Figure 7:** Monitoring bot performance.

   .. note::

      **Note:** Monitor the bot's performance regularly to ensure that it is functioning correctly and making profitable trades.

Deploying to Replit
-------------------

Replit is a versatile platform that allows you to deploy applications quickly. Follow these steps to deploy your application on Replit:

1. **Click the "Deploy on Replit" Button**

   Start by clicking the **"Deploy on Replit"** button on the GitHub repository.

   .. figure:: _static/images/deploy_replit_button.png
      :alt: Deploy on Replit Button
      :align: center

      **Figure 8:** Deploy on Replit button on GitHub.

2. **Open Secrets Configuration**

   Once your Replit project is created, navigate to the secrets configuration:

   - Go to **Tools** in the sidebar.
   - Select **Secrets** at the bottom left corner.

   .. figure:: _static/images/replit_tools_secrets.png
      :alt: Replit Tools -> Secrets
      :align: center

      **Figure 9:** Accessing Secrets in Replit.

3. **Add Required Secrets**

   In the **Secrets** tab, add the necessary environment variables as detailed in the :ref:`Secrets Configuration <secrets-configuration>` section.

   .. figure:: _static/images/replit_add_secret.png
      :alt: Adding a new secret in Replit
      :align: center

      **Figure 10:** Adding a new secret in Replit.

4. **Test Run the Application**

   After adding all required secrets, click **Run**. This step is crucial as it installs all necessary libraries and ensures that the secrets are correctly configured.

   When you press **Run**, the application will start running in the console. You can see the logs in real-time to ensure that everything is working as expected.

   .. figure:: _static/images/replit_run.png
      :alt: Running the application in Replit
      :align: center

      **Figure 11:** Running the application in Replit.

   .. figure:: _static/images/replit_logs.png
      :alt: Viewing logs in Replit
      :align: center

      **Figure 12:** Viewing logs in Replit.

5. **Deployment Part 1**

   - **Click Deploy:** Navigate to **Deploy** located under **Tools** in the top right or within the **Background Workers** section.
   - **Select Reserved VM:** The strategies will only work on a **Reserved VM**, none of the other options will work.

   .. figure:: _static/images/replit_reserved_vm.png
      :alt: Select Reserved VM and Background Worker
      :align: center

      **Figure 13:** Selecting Reserved VM and Background Worker on Replit.

   .. note::

      **Note:** Ensure that you have downgraded the vCPU before selecting the Background Worker to optimize costs effectively.

6. **Deployment Part 2**

   - **Downgrade vCPU:** We recommend downgrading to **0.25 vCPU** to reduce costs. As of today, it costs **$6/month** compared to the default **$12/month** for **0.5 vCPU**.
   - **Select Background Worker:** Choose **"Background Worker"**.
   - **Click Deploy:** Click **"Deploy"** to deploy your application.
   - **Wait for Deployment:** The deployment process may take a few minutes. Once completed, you will see a success message.

   .. figure:: _static/images/replit_deploy.png
      :alt: Deploying the application in Replit
      :align: center

      **Figure 14:** Deploying the application in Replit.

   .. figure:: _static/images/replit_deploy_process.png
      :alt: Deployment process
      :align: center

      **Figure 15:** Deployment process in Replit.

7. **Check The Logs**

   - **View Logs:** Navigate to the **Logs** tab in **Deployment** to view the deployment logs and ensure that there are no errors.

   .. figure:: _static/images/replit_deploy_logs.png
      :alt: Logs on Replit
      :align: center

      **Figure 16:** Viewing Logs on Replit.

8. **Monitor Bot Performance**

   - **Monitor Performance:** Go to your broker account to monitor the bot's performance and ensure that it is executing trades as expected.

   .. figure:: _static/images/replit_monitor_bot.png
      :alt: Monitor bot performance
      :align: center

      **Figure 17:** Monitoring bot performance.

   .. note::

      **Note:** Monitor the bot's performance regularly to ensure that it is functioning correctly and making profitable trades.

Secrets Configuration
=====================

Proper configuration of environment variables is crucial for the successful deployment of your application. The most important secrets are those related to your chosen broker. First, set up the secrets for your broker, then add any additional general secrets if needed.

.. tip::

   **Tip:** First, add the secrets for the broker you are using. After configuring the broker-specific secrets, you can add general secrets such as Discord webhook URLs or database connection strings.

.. important::

   **The secrets required for your chosen broker are mandatory, you only need to pick one. Also, depending on the strategy you are running, you might also need to set the `LIVE_CONFIG` environment variable.**

Broker Configuration
====================

To support different brokers, we have separate sections. Choose the one that corresponds to the broker you are using.

Tradier Configuration
---------------------

Tradier is great because they can trade stocks, options, and soon futures. Tradier also offers an incredible plan for $10/month, providing commission-free options trading. This can save a lot of money for those day trading options or engaging in similar activities. To create an account, visit the `Tradier <https://tradier.com/>`_ website.

.. list-table:: Tradier Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - TRADIER_ACCESS_TOKEN
     - Your Access Token from Tradier
     - qTRz3zUrl9244AHUw4AoyAPgvYra
   * - TRADIER_ACCOUNT_NUMBER
     - Your Account Number from Tradier
     - VA12204793
   * - TRADIER_IS_PAPER
     - **Set to "True"** to use the paper trading API, **set to "False"** to use the real money trading API. Defaults to True.
     - True

Tradovate Configuration
------------------------

Tradovate is a futures broker that provides access to CME Group markets. **Important: Tradovate does not provide market data, so you must configure a separate data source** (such as DataBento or ProjectX). To create an account, visit the `Tradovate <https://www.tradovate.com/>`_ website.

.. list-table:: Tradovate Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - TRADOVATE_USERNAME
     - Your Tradovate username
     - your_username
   * - TRADOVATE_DEDICATED_PASSWORD
     - Your dedicated API password (not your login password)
     - your_api_password
   * - TRADOVATE_CID
     - Your Client ID from Tradovate API dashboard
     - 6889
   * - TRADOVATE_SECRET
     - Your Secret from Tradovate API dashboard
     - x4078409-db42-4d6a-8469-a468a8b94ok8
   * - TRADOVATE_IS_PAPER
     - **Set to "True"** for demo trading, **set to "False"** for live trading. Defaults to True.
     - True
   * - TRADOVATE_APP_ID
     - Application identifier (optional, defaults to "Lumibot")
     - Lumibot
   * - TRADOVATE_APP_VERSION
     - Application version (optional, defaults to "1.0")
     - 1.0

.. note::
   
   Since Tradovate doesn't provide market data, you must also configure a separate data source. Set ``TRADING_BROKER=tradovate`` and ``DATA_SOURCE`` to one of: ``databento``, ``projectx``, ``ibrest`` (Interactive Brokers REST), or ``ib`` (Interactive Brokers Legacy).
Alpaca Configuration
--------------------

Alpaca is great because they're a commission-free broker specifically designed for API trading, which aligns perfectly with our platform. Alpaca supports trading stocks, crypto, and soon options, with their APIs working seamlessly for automated trading strategies. To create an account, visit the `Alpaca <https://alpaca.markets/>`_ website.

.. list-table:: Alpaca Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - ALPACA_API_KEY
     - Your API key from your Alpaca brokerage account
     - PK7T6YVAX6PMH1EM20YN
   * - ALPACA_API_SECRET
     - Your secret key from your Alpaca brokerage account
     - 9WgJLS3wIXq54FCpHwwZjCp8JCfJfKuwSrYskKMA
   * - ALPACA_IS_PAPER
     - **Set to "True"** to use the Alpaca paper trading API, **set to "False"** to use the Alpaca real money trading API. Defaults to True.
     - True

Coinbase Configuration
----------------------

Coinbase is a cryptocurrency broker that is easy to set up and operates across all United States, including New York, which is typically challenging to find for crypto brokers. It offers a wide range of cryptocurrencies with user-friendly APIs. To create an account, visit the `Coinbase <https://www.coinbase.com/>`_ website.

.. list-table:: Coinbase Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - COINBASE_API_KEY_NAME
     - Your API key name/identifier for Coinbase. **Required** if you are using Coinbase as your broker.
     - organizations/a7df3e75-5gg5-4b0d-805c-e91c02fd63b8/apiKeys/1abb999e-8442-4607-lkc7-423eb8d478e3
   * - COINBASE_PRIVATE_KEY
     - Your private key for Coinbase. **Required** if you are using Coinbase as your broker.  
     - -----BEGIN EC PRIVATE KEY-----\nPLjCAQEEIFOxfolkj7JmTkEUyctOqAq0hQt02SRBy7GnJHGQyb56jToAoGCCqGSM49\nAwEHoUQDQgAEg1VBKEVkqhy+9eHxeao7b7cMsbXXeB/Ggm2sYKEm2Ebrhq67Nobj\n5ze8ddf78UFICjOcooHovd+1oFcZZ+RLQ==\n-----END EC PRIVATE KEY-----\n"
   * - COINBASE_API_PASSPHRASE
     - Your API passphrase for Coinbase. **Optional** if you are using Coinbase as your broker.
     - 123456
   * - COINBASE_IS_SANDBOX
     - **Set to "True"** to use the Coinbase sandbox (paper trading) API, **set to "False"** to use the Coinbase real money trading API. Defaults to False.
     - False

Kraken Configuration
--------------------

Kraken is an excellent cryptocurrency broker offering very low fees and a wide range of cryptocurrencies, likely more than Coinbase. It is ideal for users focused on crypto trading with competitive pricing. To create an account, visit the `Kraken <https://www.kraken.com/>`_ website.

.. list-table:: Kraken Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - KRAKEN_API_KEY
     - Your API key from Kraken. **Required** if you are using Kraken as your broker.
     - XyZ1234567890abcdef
   * - KRAKEN_API_SECRET
     - Your API secret for Kraken. **Required** if you are using Kraken as your broker.
     - abcdef1234567890abcdef1234567890abcdef1234

Kucoin Configuration
--------------------

Kucoin is a popular global cryptocurrency exchange offering a wide variety of cryptocurrencies and trading pairs. To create an account, visit the `Kucoin <https://www.kucoin.com/>`_ website.

.. list-table:: Kucoin Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - KUCOIN_API_KEY
     - Your Kucoin API key. **Required** if you are using Kucoin as your broker.
     - 5f6a7b8c9d0e1f2a3b4c
   * - KUCOIN_SECRET
     - Your Kucoin secret. **Required** if you are using Kucoin as your broker.
     - abcdef1234567890abcdef1234567890abcdef12
   * - KUCOIN_PASSPHRASE
     - Your Kucoin passphrase. **Required** if you are using Kucoin as your broker.
     - mypassphrase456

Binance Configuration
---------------------

Binance is the world's largest cryptocurrency exchange by trading volume, offering extensive cryptocurrency trading options. To create an account, visit the `Binance <https://www.binance.com/>`_ website.

.. list-table:: Binance Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - BINANCE_API_KEY
     - Your Binance API key. **Required** if you are using Binance as your broker.
     - 9a8b7c6d5e4f3g2h1i0j
   * - BINANCE_SECRET
     - Your Binance secret key. **Required** if you are using Binance as your broker.
     - abcdef1234567890abcdef1234567890abcdef12

Bitmex Configuration
--------------------

Bitmex is a cryptocurrency derivatives exchange specializing in futures and perpetual contracts. To create an account, visit the `Bitmex <https://www.bitmex.com/>`_ website.

.. list-table:: Bitmex Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - BITMEX_API_KEY
     - Your Bitmex API key. **Required** if you are using Bitmex as your broker.
     - 1a2b3c4d5e6f7g8h9i0j
   * - BITMEX_SECRET
     - Your Bitmex secret. **Required** if you are using Bitmex as your broker.
     - abcdef1234567890abcdef1234567890abcdef12

Bybit Configuration
-------------------

Bybit is a popular derivatives exchange offering futures and perpetual contracts for cryptocurrency trading. To create an account, visit the `Bybit <https://www.bybit.com/>`_ website.

.. list-table:: Bybit Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - BYBIT_API_KEY
     - Your Bybit API key. **Required** if you are using Bybit as your broker.
     - 2b3c4d5e6f7g8h9i0j1k
   * - BYBIT_SECRET
     - Your Bybit secret. **Required** if you are using Bybit as your broker.
     - abcdef1234567890abcdef1234567890abcdef12

OKX Configuration
-----------------

OKX is a major global cryptocurrency exchange offering spot, futures, and options trading. To create an account, visit the `OKX <https://www.okx.com/>`_ website.

.. list-table:: OKX Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - OKX_API_KEY
     - Your OKX API key. **Required** if you are using OKX as your broker.
     - 3c4d5e6f7g8h9i0j1k2l
   * - OKX_SECRET
     - Your OKX secret key. **Required** if you are using OKX as your broker.
     - abcdef1234567890abcdef1234567890abcdef12
   * - OKX_PASSPHRASE
     - Your OKX passphrase. **Required** if you are using OKX as your broker.
     - mypassphrase789

Interactive Brokers Configuration
---------------------------------

Interactive Brokers is ideal for international users as they offer a wide array of asset classes, including stocks, options, futures, forex, CFDs, and more. Their global presence makes them suitable for users around the world. To create an account, visit the `Interactive Brokers <https://www.interactivebrokers.com/>`_ website.

.. list-table:: Interactive Brokers Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - IB_USERNAME
     - Your Interactive Brokers username.
     - user123
   * - IB_PASSWORD
     - Your Interactive Brokers password.
     - password123
   * - ACCOUNT_ID
     - (Optional) An Interactive Brokers subaccount to trade on.
     - U17369206
   * - IB_API_URL
     - (Optional) The URL of your self-hosted Interactive Brokers REST API. You likely don't need it.
     - https://localhost:8000

Interactive Brokers-Legacy Configuration
---------------------------------

This is the legacy version of our Interactive Brokers implementation, which uses their TWS API. It is maintained for compatibility purposes, but we recommend using the newer implementation.

.. list-table:: Interactive Brokers-Legacy Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - INTERACTIVE_BROKERS_PORT
     - Socket port for Interactive Brokers.
     - 7497
   * - INTERACTIVE_BROKERS_CLIENT_ID
     - Client ID for Interactive Brokers.
     - 123456
   * - INTERACTIVE_BROKERS_IP
     - IP address for Interactive Brokers (defaults to "127.0.0.1"). **Required** if you are using Interactive Brokers as your broker.
     - 127.0.0.1
   * - IB_SUBACCOUNT
     - Subaccount for Interactive Brokers. **Required** if you are using Interactive Brokers as your broker.
     - Subaccount1

Schwab Configuration
--------------------

Charles Schwab provides API access for automated trading and market data through its developer platform. To create an account, visit the `Charles Schwab <https://www.schwab.com/>`_ website and follow the instructions to register for a developer account at `developer.schwab.com <https://developer.schwab.com>`_.

.. list-table:: Schwab Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - SCHWAB_API_KEY
     - Your Schwab API key obtained from the developer dashboard.
     - your_api_key_here
   * - SCHWAB_SECRET
     - Your Schwab API secret obtained from the developer dashboard.
     - your_api_secret_here
   * - SCHWAB_ACCOUNT_NUMBER
     - Your Schwab account number used for trading.
     - 123456789

Bitunix Configuration
---------------------

Bitunix is a cryptocurrency derivatives exchange that supports perpetual futures trading. To create an account, visit the `Bitunix <https://www.bitunix.com/>`_ website.

.. list-table:: Bitunix Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - BITUNIX_API_KEY
     - Your Bitunix API key. **Required** if you are using Bitunix as your broker.
     - your_api_key_here
   * - BITUNIX_API_SECRET
     - Your Bitunix API secret. **Required** if you are using Bitunix as your broker.
     - your_api_secret_here

DataBento Configuration
-----------------------

DataBento provides high-quality market data for stocks, futures, and options. This is primarily used as a data source for backtesting and live trading with futures. To create an account, visit the `DataBento <https://databento.com/>`_ website.

.. list-table:: DataBento Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - DATABENTO_API_KEY
     - Your API key from DataBento. **Required** if you are using DataBento as your data source.
     - db-xxxxxxxxxxxxxxxxxxxxxxxx

ProjectX Configuration
----------------------

ProjectX is a futures-only broker that connects to multiple prop trading firms and futures brokers. Each broker requires its own specific environment variables. Choose the section below that matches your broker.

TopstepX
^^^^^^^^

.. list-table:: TopstepX Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - PROJECTX_TOPSTEPX_API_KEY
     - Your API key from TopstepX
     - your_api_key
   * - PROJECTX_TOPSTEPX_USERNAME
     - Your TopstepX username
     - your_username
   * - PROJECTX_TOPSTEPX_PREFERRED_ACCOUNT_NAME
     - (Optional) Preferred account name
     - Practice Account 1

Top One Futures
^^^^^^^^^^^^^^^

.. list-table:: Top One Futures Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - PROJECTX_TOPONE_API_KEY
     - Your API key from Top One Futures
     - your_api_key
   * - PROJECTX_TOPONE_USERNAME
     - Your Top One Futures username
     - your_username
   * - PROJECTX_TOPONE_PREFERRED_ACCOUNT_NAME
     - (Optional) Preferred account name
     - Practice Account 1

TickTickTrader
^^^^^^^^^^^^^^

.. list-table:: TickTickTrader Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - PROJECTX_TICKTICKTRADER_API_KEY
     - Your API key from TickTickTrader
     - your_api_key
   * - PROJECTX_TICKTICKTRADER_USERNAME
     - Your TickTickTrader username
     - your_username
   * - PROJECTX_TICKTICKTRADER_PREFERRED_ACCOUNT_NAME
     - (Optional) Preferred account name
     - Practice Account 1

AlphaTicks
^^^^^^^^^^

.. list-table:: AlphaTicks Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - PROJECTX_ALPHATICKS_API_KEY
     - Your API key from AlphaTicks
     - your_api_key
   * - PROJECTX_ALPHATICKS_USERNAME
     - Your AlphaTicks username
     - your_username
   * - PROJECTX_ALPHATICKS_PREFERRED_ACCOUNT_NAME
     - (Optional) Preferred account name
     - Practice Account 1

Aqua Futures
^^^^^^^^^^^^

.. list-table:: Aqua Futures Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - PROJECTX_AQUAFUTURES_API_KEY
     - Your API key from Aqua Futures
     - your_api_key
   * - PROJECTX_AQUAFUTURES_USERNAME
     - Your Aqua Futures username
     - your_username
   * - PROJECTX_AQUAFUTURES_PREFERRED_ACCOUNT_NAME
     - (Optional) Preferred account name
     - Practice Account 1

Blue Guardian Futures
^^^^^^^^^^^^^^^^^^^^^^

.. list-table:: Blue Guardian Futures Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - PROJECTX_BLUEGUARDIANFUTURES_API_KEY
     - Your API key from Blue Guardian Futures
     - your_api_key
   * - PROJECTX_BLUEGUARDIANFUTURES_USERNAME
     - Your Blue Guardian Futures username
     - your_username
   * - PROJECTX_BLUEGUARDIANFUTURES_PREFERRED_ACCOUNT_NAME
     - (Optional) Preferred account name
     - Practice Account 1

Blusky
^^^^^^

.. list-table:: Blusky Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - PROJECTX_BLUSKY_API_KEY
     - Your API key from Blusky
     - your_api_key
   * - PROJECTX_BLUSKY_USERNAME
     - Your Blusky username
     - your_username
   * - PROJECTX_BLUSKY_PREFERRED_ACCOUNT_NAME
     - (Optional) Preferred account name
     - Practice Account 1

Bulenox
^^^^^^^

.. list-table:: Bulenox Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - PROJECTX_BULENOX_API_KEY
     - Your API key from Bulenox
     - your_api_key
   * - PROJECTX_BULENOX_USERNAME
     - Your Bulenox username
     - your_username
   * - PROJECTX_BULENOX_PREFERRED_ACCOUNT_NAME
     - (Optional) Preferred account name
     - Practice Account 1

E8 Futures
^^^^^^^^^^

.. list-table:: E8 Futures Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - PROJECTX_E8X_API_KEY
     - Your API key from E8 Futures
     - your_api_key
   * - PROJECTX_E8X_USERNAME
     - Your E8 Futures username
     - your_username
   * - PROJECTX_E8X_PREFERRED_ACCOUNT_NAME
     - (Optional) Preferred account name
     - Practice Account 1

Funding Futures
^^^^^^^^^^^^^^^

.. list-table:: Funding Futures Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - PROJECTX_FUNDINGFUTURES_API_KEY
     - Your API key from Funding Futures
     - your_api_key
   * - PROJECTX_FUNDINGFUTURES_USERNAME
     - Your Funding Futures username
     - your_username
   * - PROJECTX_FUNDINGFUTURES_PREFERRED_ACCOUNT_NAME
     - (Optional) Preferred account name
     - Practice Account 1

The Futures Desk
^^^^^^^^^^^^^^^^

.. list-table:: The Futures Desk Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - PROJECTX_THEFUTURESDESK_API_KEY
     - Your API key from The Futures Desk
     - your_api_key
   * - PROJECTX_THEFUTURESDESK_USERNAME
     - Your Futures Desk username
     - your_username
   * - PROJECTX_THEFUTURESDESK_PREFERRED_ACCOUNT_NAME
     - (Optional) Preferred account name
     - Practice Account 1

Futures Elite
^^^^^^^^^^^^^

.. list-table:: Futures Elite Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - PROJECTX_FUTURESELITE_API_KEY
     - Your API key from Futures Elite
     - your_api_key
   * - PROJECTX_FUTURESELITE_USERNAME
     - Your Futures Elite username
     - your_username
   * - PROJECTX_FUTURESELITE_PREFERRED_ACCOUNT_NAME
     - (Optional) Preferred account name
     - Practice Account 1

FXIFY Futures
^^^^^^^^^^^^^

.. list-table:: FXIFY Futures Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - PROJECTX_FXIFYFUTURES_API_KEY
     - Your API key from FXIFY Futures
     - your_api_key
   * - PROJECTX_FXIFYFUTURES_USERNAME
     - Your FXIFY Futures username
     - your_username
   * - PROJECTX_FXIFYFUTURES_PREFERRED_ACCOUNT_NAME
     - (Optional) Preferred account name
     - Practice Account 1

Goat Funded Futures
^^^^^^^^^^^^^^^^^^^

.. list-table:: Goat Funded Futures Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - PROJECTX_GOATFUNDEDFUTURES_API_KEY
     - Your API key from Goat Funded Futures
     - your_api_key
   * - PROJECTX_GOATFUNDEDFUTURES_USERNAME
     - Your Goat Funded Futures username
     - your_username
   * - PROJECTX_GOATFUNDEDFUTURES_PREFERRED_ACCOUNT_NAME
     - (Optional) Preferred account name
     - Practice Account 1

Hola Prime
^^^^^^^^^^

.. list-table:: Hola Prime Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - PROJECTX_HOLAPRIME_API_KEY
     - Your API key from Hola Prime
     - your_api_key
   * - PROJECTX_HOLAPRIME_USERNAME
     - Your Hola Prime username
     - your_username
   * - PROJECTX_HOLAPRIME_PREFERRED_ACCOUNT_NAME
     - (Optional) Preferred account name
     - Practice Account 1

Nexgen Futures
^^^^^^^^^^^^^^

.. list-table:: Nexgen Futures Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - PROJECTX_NEXGEN_API_KEY
     - Your API key from Nexgen Futures
     - your_api_key
   * - PROJECTX_NEXGEN_USERNAME
     - Your Nexgen Futures username
     - your_username
   * - PROJECTX_NEXGEN_PREFERRED_ACCOUNT_NAME
     - (Optional) Preferred account name
     - Practice Account 1

TX3 Funding
^^^^^^^^^^^

.. list-table:: TX3 Funding Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - PROJECTX_TX3FUNDING_API_KEY
     - Your API key from TX3 Funding
     - your_api_key
   * - PROJECTX_TX3FUNDING_USERNAME
     - Your TX3 Funding username
     - your_username
   * - PROJECTX_TX3FUNDING_PREFERRED_ACCOUNT_NAME
     - (Optional) Preferred account name
     - Practice Account 1

DayTraders
^^^^^^^^^^

.. list-table:: DayTraders Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - PROJECTX_DAYTRADERS_API_KEY
     - Your API key from DayTraders
     - your_api_key
   * - PROJECTX_DAYTRADERS_USERNAME
     - Your DayTraders username
     - your_username
   * - PROJECTX_DAYTRADERS_PREFERRED_ACCOUNT_NAME
     - (Optional) Preferred account name
     - Practice Account 1

Demo/Testing
^^^^^^^^^^^^

.. list-table:: Demo Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - PROJECTX_DEMO_API_KEY
     - Your API key for demo/testing
     - your_api_key
   * - PROJECTX_DEMO_USERNAME
     - Your demo username
     - your_username
   * - PROJECTX_DEMO_PREFERRED_ACCOUNT_NAME
     - (Optional) Preferred account name
     - Practice Account 1

General Environment Variables
=============================

In addition to broker-specific secrets, the following environment variables are required for the strategy to function correctly:

.. list-table:: General Environment Variables
   :widths: 25 50 25
   :header-rows: 1

   * - **Secret**
     - **Description**
     - **Example**
   * - LUMIWEALTH_API_KEY
     - Your API key from the BotSpot.trade website so that you can track your bot's performance. To get this API key, visit the `BotSpot.trade <https://botspot.trade/>`_ website and add/create a bot. After creating the bot, you will receive an API key.
     - 694rr2c8d9234b43a40fab494a79f5634ghd4f39d44ccf2e
   * - LIVE_CONFIG
     - Your live config file, only needed for strategies that have multiple configurations (there will be a folder named "configurations" in the src/ folder) and if you are running the strategy live.
     - paper_1
   * - IS_BACKTESTING
     - **(Optional)** Set to **"True"** to run the strategy in backtesting mode, set to **"False"** to run the strategy live (defaults to False).
     - False
   * - BACKTESTING_START
     - **(Optional)** The start date for backtesting in the format "YYYY-MM-DD". Only needed if you are backtesting.
     - 2025-01-01
   * - BACKTESTING_END
     - **(Optional)** The end date for backtesting in the format "YYYY-MM-DD". Only needed if you are backtesting.
     - 2025-01-31
   * - POLYGON_API_KEY
     - **(Optional)** Your API key from your Polygon account, only needed if you are backtesting.
     - a7py0zIdhxde6QkX8OjjKNp7cD87hwKU
   * - DISCORD_WEBHOOK_URL
     - **(Optional)** Your Discord webhook URL, only needed if you want to send notifications to Discord. Learn how to get a Discord webhook URL here: `Discord Webhooks <https://support.discord.com/hc/en-us/articles/228383668-Intro-to-Webhooks>`_
     - https://discord.com/api/webhooks/123456789/
   * - DB_CONNECTION_STR
     - **(Optional)** Your connection string to your account history database, only needed if you want to save your account history to a database.
     - sqlite:///account_history.db
   * - STRATEGY_NAME
     - **(Optional)** The name of the strategy. This will change the strategy_id in the database and in the Discord messages.
     - My Strategy
   * - MARKET
     - **(Optional)** The market you want the bot to think it is. Eg. "24/7" will make the bot think it is trading in a 24/7 market.
     - NYSE
   * - POLYGON_MAX_MEMORY_BYTES
     - **(Optional)** The maximum memory in bytes that the Polygon API can use. This is useful for limiting memory usage during backtesting.
     - 512000000
   * - TRADING_BROKER
     - **(Optional)** For live trading, specify the broker to use for executing trades. If not set, the default broker configuration will be used for both trading and data. Valid options (case insensitive): "alpaca", "tradier", "ccxt", "coinbase", "kraken", "ib" (or "interactivebrokers"), "ibrest" (or "interactivebrokersrest"), "tradovate", "schwab", "bitunix", "projectx"
     - tradier
   * - DATA_SOURCE
     - **(Optional)** For live trading, specify a separate data source for market data. If not set, the same broker as trading will be used for data. This allows you to use one broker for trading and a different data provider for market data. Valid options (case insensitive): "alpaca", "tradier", "ccxt", "coinbase", "kraken", "ib" (or "interactivebrokers"), "ibrest" (or "interactivebrokersrest"), "yahoo", "schwab", "databento", "bitunix", "projectx"
     - databento
   * - DATA_SOURCE_DELAY
     - **(Optional)** Sets a delay parameter to control how many minutes to delay non-crypto data for. For example, the AlpacaData source uses a 16 minute delay by default. Override this to 0 if you have the paid SIP plan with not delayed data.
     - 0

.. tip::

   **Tip:** If you are running the strategy on your own computer, create a `.env` file in the same directory as `main.py` and add the environment variables there instead of using Replit or Render secrets.

Final Steps
-----------

After configuring all the necessary environment variables and deploying your application on your chosen platform, ensure that everything is running smoothly:

- **Verify Deployment:** Check the deployment logs to ensure there are no errors.
- **Test Functionality:** Perform a few tests to confirm that the application behaves as expected.
- **Monitor Performance:** Use the monitoring tools provided by Render or Replit to keep an eye on your application's performance.

Conclusion
----------

Deploying your application is straightforward with our GitHub deployment buttons for **Render** and **Replit**. By following this guide, you can quickly set up your environment variables and get your application live. Happy deploying! 🎉

For further assistance, refer to the `Render Documentation <https://render.com/docs>`_ or the `Replit Documentation <https://docs.replit.com/>`_.
