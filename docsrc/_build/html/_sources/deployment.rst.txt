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
   * - COINBASE_API_KEY
     - Your API key for Coinbase. **Required** if you are using Coinbase as your broker.
     - STeea9fhIsznTMpIHQjUdEqOliTJ0JAvZ
   * - COINBASE_API_SECRET
     - Your API secret for Coinbase. **Required** if you are using Coinbase as your broker.
     - NUzcnprsXjxxOUxRhQE5k2K1XnqLPcKH2XCUTIfkCw==
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
     - 24/7
   * - POLYGON_MAX_MEMORY_BYTES
     - **(Optional)** The maximum memory in bytes that the Polygon API can use. This is useful for limiting memory usage during backtesting.
     - 512000000

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
