Deployment Guide
================

Deploying your application is straightforward with our GitHub deployment buttons for **Render** and **Replit**. Follow the steps below to get your application up and running quickly. 🚀

.. important::

   **Render** is our recommended platform due to its ease of use and cost-effectiveness. **Replit** is also a great option, especially for developers who prefer editing code directly in the browser.

.. tip::

   **Tip:** Scroll down to the :ref:`Secrets Configuration <secrets-configuration>` section for detailed information on setting up your environment variables.

Choosing Your Deployment Platform
--------------------------------

We recommend using **Render** for deployment because it is easier to use and more affordable compared to Replit. However, **Replit** is an excellent choice for developers who want to edit code directly in the browser.

.. note::

   **Render** costs **$7/month**, while **Replit** is priced at **$25/month**. Choose the platform that best fits your needs and budget.

Deploying to Render
-------------------

Render offers powerful deployment options with easy scalability. Follow these steps to deploy your application on Render:

1. **Click the "Deploy to Render" Button**

   Start by clicking the **"Deploy to Render"** button on the GitHub repository.

   .. figure:: images/render_deploy_button.png
      :alt: Deploy to Render Button
      :align: center

      **Figure 1:** Deploy to Render button on GitHub.

2. **Configure the Blueprint**

   - **Choose a Blueprint Name:** Enter a name for your blueprint, e.g., **"Blueprint Name"**.
   - **Deploy Blueprint:** Click the **"Deploy Blueprint"** button.

   .. figure:: images/render_deploy_blueprint.png
      :alt: Deploy Blueprint on Render
      :align: center

      **Figure 2:** Deploying Blueprint on Render.

3. **Configure Environment Variables**

   - **Navigate to Environment Settings:** Click on the name of the background worker, e.g., **options-butterfly-condor-worker-jljk (Starter)**.
   - **Select Environment:** On the worker's page, select **Environment** from the left sidebar.
   - **Edit Environment Variables:** Click **Edit** and fill in the required keys as detailed in the :ref:`Secrets Configuration <secrets-configuration>` section.

   .. figure:: images/render_worker_environment.png
      :alt: Environment Settings on Render
      :align: center

      **Figure 3:** Editing Environment Variables on Render.

   .. note::

      **Note:** Only the environment variables required for your chosen broker are mandatory. Refer to the :ref:`Secrets Configuration <secrets-configuration>` section to see which ones are mandatory and which are optional.

4. **Restart the Service**

   After configuring the environment variables, navigate to the top right corner and click **"Manual Deploy"**, then **"Restart Service"** to apply the changes.

   .. figure:: images/render_restart_service.png
      :alt: Restart Service on Render
      :align: center

      **Figure 4:** Restarting Service on Render.

5. **Finalize Deployment**

   Once the service restarts without errors, your deployment on Render is successful! You can monitor the deployment status and view logs on the left sidebar to ensure everything is running smoothly.

   .. figure:: images/render_finalize_deployment.png
      :alt: Finalize Deployment on Render
      :align: center

      **Figure 5:** Finalizing Deployment on Render.

Deploying to Replit
-------------------

Replit is a versatile platform that allows you to deploy applications quickly. Follow these steps to deploy your application on Replit:

1. **Click the "Deploy on Replit" Button**

   Start by clicking the **"Deploy on Replit"** button on the GitHub repository.

   .. figure:: images/deploy_replit_button.png
      :alt: Deploy on Replit Button
      :align: center

      **Figure 6:** Deploy on Replit button on GitHub.

2. **Open Secrets Configuration**

   Once your Replit project is created, navigate to the secrets configuration:

   - Go to **Tools** in the sidebar.
   - Select **Secrets** at the bottom left corner.

   .. figure:: images/replit_tools_secrets.png
      :alt: Replit Tools -> Secrets
      :align: center

      **Figure 7:** Accessing Secrets in Replit.

3. **Add Required Secrets**

   In the **Secrets** tab, add the necessary environment variables as detailed in the :ref:`Secrets Configuration <secrets-configuration>` section.

   .. figure:: images/replit_add_secret.png
      :alt: Adding a new secret in Replit
      :align: center

      **Figure 8:** Adding a new secret in Replit.

4. **Test Run the Application**

   After adding all required secrets, click **Run**. This step is crucial as it installs all necessary libraries and ensures that the secrets are correctly configured.

   .. figure:: images/replit_run.png
      :alt: Running the application in Replit
      :align: center

      **Figure 9:** Running the application in Replit.

5. **Deployment**

   - **Click Deploy:** Navigate to **Deploy** located under **Tools** in the top right or within the **Background Workers** section.
   - **Select Reserved VM:** The strategies will only work on a **Reserved VM**, none of the other options will work.
   - **Downgrade vCPU:** We recommend downgrading to **0.25 vCPU** to reduce costs. As of today, it costs **$6/month** compared to the default **$12/month** for **0.5 vCPU**.
   - **Select Background Worker:** Choose **"Background Worker"**.

   .. figure:: images/replit_reserved_vm.png
      :alt: Select Reserved VM and Background Worker
      :align: center

      **Figure 10:** Selecting Reserved VM and Background Worker on Replit.

   **Note:** Ensure that you have downgraded the vCPU before selecting the Background Worker to optimize costs effectively.

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

.. table:: Tradier Configuration

   +------------------------+-----------------------------------------------------------------------------------------------------------+----------------------------------------------+
   | **Secret**             | **Description**                                                                                           | **Example**                                  |
   +========================+===========================================================================================================+==============================================+
   | TRADIER_ACCESS_TOKEN   | Your Access Token from Tradier                                                                            | qTRz3zUrl9244AHUw4AoyAPgvYra                  |
   +------------------------+-----------------------------------------------------------------------------------------------------------+----------------------------------------------+
   | TRADIER_ACCOUNT_NUMBER | Your Account Number from Tradier                                                                            | VA12204793                                    |
   +------------------------+-----------------------------------------------------------------------------------------------------------+----------------------------------------------+
   | TRADIER_IS_PAPER       | **Set to "True"** to use the paper trading API, **set to "False"** to use the real money trading API. Defaults to True. | True                                           |
   +------------------------+-----------------------------------------------------------------------------------------------------------+----------------------------------------------+

   Tradier is great because they can trade stocks, options, and soon futures. Tradier also offers an incredible plan for $10/month, providing commission-free options trading. This can save a lot of money for those day trading options or engaging in similar activities. To create an account, visit the `Tradier <https://tradier.com/>`_ website.

Alpaca Configuration
--------------------

.. table:: Alpaca Configuration

   +---------------------+------------------------------------------------------------------------------------------------------------+----------------------------------------------+
   | **Secret**          | **Description**                                                                                            | **Example**                                  |
   +=====================+============================================================================================================+==============================================+
   | ALPACA_API_KEY      | Your API key from your Alpaca brokerage account                                                           | PK7T6YVAX6PMH1EM20YN                           |
   +---------------------+------------------------------------------------------------------------------------------------------------+----------------------------------------------+
   | ALPACA_API_SECRET   | Your secret key from your Alpaca brokerage account                                                        | 9WgJLS3wIXq54FCpHwwZjCp8JCfJfKuwSrYskKMA        |
   +---------------------+------------------------------------------------------------------------------------------------------------+----------------------------------------------+
   | ALPACA_IS_PAPER     | **Set to "True"** to use the Alpaca paper trading API, **set to "False"** to use the Alpaca real money trading API. Defaults to True. | True                                           |
   +---------------------+------------------------------------------------------------------------------------------------------------+----------------------------------------------+

   Alpaca is great because they're a commission-free broker specifically designed for API trading, which aligns perfectly with our platform. Alpaca supports trading stocks, crypto, and soon options, with their APIs working seamlessly for automated trading strategies. To create an account, visit the `Alpaca <https://alpaca.markets/>`_ website.

Coinbase Configuration
----------------------

.. table:: Coinbase Configuration

   +----------------------+-------------------------------------------------------------------------------------------------------------+----------------------------------------------+
   | **Secret**           | **Description**                                                                                             | **Example**                                  |
   +======================+=============================================================================================================+==============================================+
   | COINBASE_API_KEY     | Your API key for Coinbase. **Required** if you are using Coinbase as your broker.                             | STeea9fhIsznTMpIHQjUdEqOliTJ0JAvZ              |
   +----------------------+-------------------------------------------------------------------------------------------------------------+----------------------------------------------+
   | COINBASE_API_SECRET  | Your API secret for Coinbase. **Required** if you are using Coinbase as your broker.                          | NUzcnprsXjxxOUxRhQE5k2K1XnqLPcKH2XCUTIfkCw==   |
   +----------------------+-------------------------------------------------------------------------------------------------------------+----------------------------------------------+
   | COINBASE_IS_SANDBOX  | **Set to "True"** to use the Coinbase sandbox (paper trading) API, **set to "False"** to use the Coinbase real money trading API. Defaults to False. | False                                         |
   +----------------------+-------------------------------------------------------------------------------------------------------------+----------------------------------------------+

   Coinbase is a cryptocurrency broker that is easy to set up and operates across all United States, including New York, which is typically challenging to find for crypto brokers. It offers a wide range of cryptocurrencies with user-friendly APIs. To create an account, visit the `Coinbase <https://www.coinbase.com/>`_ website.

Kraken Configuration
--------------------

.. table:: Kraken Configuration

   +---------------------+------------------------------------------------------------------------------------------------------------+----------------------------------------------+
   | **Secret**          | **Description**                                                                                            | **Example**                                  |
   +=====================+============================================================================================================+==============================================+
   | KRAKEN_API_KEY      | Your API key from Kraken. **Required** if you are using Kraken as your broker.                               | XyZ1234567890abcdef                           |
   +---------------------+------------------------------------------------------------------------------------------------------------+----------------------------------------------+
   | KRAKEN_API_SECRET   | Your API secret for Kraken. **Required** if you are using Kraken as your broker.                            | abcdef1234567890abcdef1234567890abcdef1234    |
   +---------------------+------------------------------------------------------------------------------------------------------------+----------------------------------------------+

   Kraken is an excellent cryptocurrency broker offering very low fees and a wide range of cryptocurrencies, likely more than Coinbase. It is ideal for users focused on crypto trading with competitive pricing. To create an account, visit the `Kraken <https://www.kraken.com/>`_ website.

Interactive Brokers Configuration
--------------------------------

.. table:: Interactive Brokers Configuration

   +-----------------------------+--------------------------------------------------------------------------------------------------------------+----------------------------------------------+
   | **Secret**                  | **Description**                                                                                              | **Example**                                  |
   +=============================+==============================================================================================================+==============================================+
   | INTERACTIVE_BROKERS_PORT    | Socket port for Interactive Brokers.         | 7497                                         |
   +-----------------------------+--------------------------------------------------------------------------------------------------------------+----------------------------------------------+
   | INTERACTIVE_BROKERS_CLIENT_ID| Client ID for Interactive Brokers.          | 123456                                       |
   +-----------------------------+--------------------------------------------------------------------------------------------------------------+----------------------------------------------+
   | INTERACTIVE_BROKERS_IP       | IP address for Interactive Brokers (defaults to "127.0.0.1"). **Required** if you are using Interactive Brokers as your broker. | 127.0.0.1                                     |
   +-----------------------------+--------------------------------------------------------------------------------------------------------------+----------------------------------------------+
   | IB_SUBACCOUNT                | Subaccount for Interactive Brokers. **Required** if you are using Interactive Brokers as your broker.        | Subaccount1                                  |
   +-----------------------------+--------------------------------------------------------------------------------------------------------------+----------------------------------------------+

   Interactive Brokers is ideal for international users as they offer a wide array of asset classes, including stocks, options, futures, forex, CFDs, and more. Their global presence makes them suitable for users around the world. To create an account, visit the `Interactive Brokers <https://www.interactivebrokers.com/>`_ website.

General Environment Variables
============================

In addition to broker-specific secrets, the following environment variables are required for the strategy to function correctly:

.. table:: General Environment Variables

   +--------------------------+----------------------------------------------------------------------------------------------------------+----------------------------------------------+
   | **Secret**               | **Description**                                                                                          | **Example**                                  |
   +==========================+==========================================================================================================+==============================================+
   | LIVE_CONFIG              | Your live config file, only needed for strategies that have multiple configurations (there will be a folder named "configurations" in the src/ folder) and if you are running the strategy live.        | paper_1                                       |
   +--------------------------+----------------------------------------------------------------------------------------------------------+----------------------------------------------+
   | IS_BACKTESTING           | **(Optional)** Set to **"True"** to run the strategy in backtesting mode, set to **"False"** to run the strategy live (defaults to False). | False                                         |
   +--------------------------+----------------------------------------------------------------------------------------------------------+----------------------------------------------+
   | POLYGON_API_KEY          | **(Optional)** Your API key from your Polygon account, only needed if you are backtesting.              | a7py0zIdhxde6QkX8OjjKNp7cD87hwKU              |
   +--------------------------+----------------------------------------------------------------------------------------------------------+----------------------------------------------+
   | DISCORD_WEBHOOK_URL      | **(Optional)** Your Discord webhook URL, only needed if you want to send notifications to Discord. Learn how to get a Discord webhook URL here: `Discord Webhooks <https://support.discord.com/hc/en-us/articles/228383668-Intro-to-Webhooks>`_ | https://discord.com/api/webhooks/123456789/    |
   +--------------------------+----------------------------------------------------------------------------------------------------------+----------------------------------------------+
   | DB_CONNECTION_STR        | **(Optional)** Your connection string to your account history database, only needed if you want to save your account history to a database. | sqlite:///account_history.db                  |
   +--------------------------+----------------------------------------------------------------------------------------------------------+----------------------------------------------+
   | STRATEGY_NAME            | **(Optional)** The name of the strategy. This will change the strategy_id in the database and in the Discord messages. | My Strategy                                   |
   +--------------------------+----------------------------------------------------------------------------------------------------------+----------------------------------------------+

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

For further assistance, refer to the [Render Documentation](https://render.com/docs) or the [Replit Documentation](https://docs.replit.com/).
