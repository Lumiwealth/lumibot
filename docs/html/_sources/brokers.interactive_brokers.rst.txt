Interactive Brokers
===================

Interactive Brokers is a brokerage firm that operates in most countries around the world. It's a great choice for investing and trading, especially if you don't have access to other supported platforms.

**Note:** If you have access to other supported brokers, compare their fee structures to see which one offers the best deal for you.

Market Data Subscriptions
-------------------------

To access real-time market data, you need to subscribe to the appropriate market data packages. Interactive Brokers offers various subscriptions depending on the exchanges and types of data you need. Here are some common options:

- **US Securities Snapshot and Futures Value Bundle**
- **US Equity and Options Add-On Streaming Bundle**
- **NASDAQ (Network C/UTP) TotalView**
- **NYSE (Network A/CTA) OpenBook Ultra**
- **OPRA (US Options Exchanges)**

.. note::

  Different strategies may require different market data subscriptions. For top options-related strategies, the **OPRA (US Options Exchanges)** subscription should suffice.

**To subscribe to market data:**

.. important::
  Market data subscriptions are login-specific. Ensure you're logged in with the same credentials you plan to use with Lumibot before proceeding.

1. Log in to the `IBKR Client Portal <https://www.interactivebrokers.com>`_.
2. Navigate to the **Settings** menu.

  .. image:: images/ib-main.png
    :alt: IB Main Menu
    :align: center

3. Under **Account Settings**, find the **Market Data Subscriptions** section.

  .. image:: images/ib-settings.png
    :alt: IB Settings
    :align: center

4. Click **Configure** and select the desired market data packages.

  .. image:: images/ib-market-data-subscriptions.png
    :alt: IB Market Data Subscriptions
    :align: center

5. Find your desired subscription.
6. Follow the prompts to complete the subscription process.

**Note:** Market data subscriptions may incur additional fees. Review the costs associated with each package before subscribing.

Two-Factor Authentication (2FA)
-------------------------------

Interactive Brokers requires two-factor authentication (2FA) for account security. When using Lumibot, you'll receive 2FA notifications through the IB Key mobile app, which require manual approval. This can interrupt automated trading operations.

**Important:** Automated handling of 2FA is not currently supported in Lumibot.

**Possible Solution: Create a Secondary Username**

To enable fully automated trading, you can create a secondary username for your account that doesn't require 2FA. This approach is secure and convenient:

- The primary account maintains full 2FA security for website access.
- The secondary account, used only for API connections, can operate without 2FA.
- Both accounts have separate access controls you can configure.

.. note::

  When using a secondary account without 2FA:

  - You'll need separate market data subscriptions for each user.
  - If trading with both primary and secondary users, you'll pay for market data twice.
  - Enhance security by:
    - Setting a complex password.
    - Restricting IP addresses for TWS/IBGateway access.
    - Limiting the account's permissions to only what's needed.
    - Keeping API connection details secure.

See the **Using a Secondary Username** section below for detailed setup instructions.

Using a Secondary Username
--------------------------

Interactive Brokers allows only one login at a time for any given set of credentials. Attempting to use the IBKR website while the API connection is running will disconnect the API connection, causing a re-authentication loop.

An easy solution is to stop the API connection before using the website, but this can be tedious. A more convenient solution is to create a **secondary username** for your account and use that for the API connection.

**Steps to create a secondary username:**

1. Log in to the **IBKR Client Portal** with your primary username.
2. Click on the **Settings** menu in the upper right corner.
3. Scroll down and find the **Users & Access Rights** link.
4. Click the **plus sign (+)** button in the **Users** header to add a new user.
5. Fill out the username and password fields for the new user.
6. Click through the settings pages, adjusting notifications and permissions as needed.
7. At the end of the process, you'll receive an email with a **confirmation code**.

**Activating your secondary username:**

1. Log out and log back in using the **new secondary username and password**.
2. Enter the **confirmation code** received via email.
  - **Note:** The code expires quickly, so act promptly.
3. You may be prompted to **change the password** for the secondary user.

**Finalizing the setup:**

- You might receive an email asking for a **"Proof of trader authority"** for the new username.
- Log in using your **primary username** (not the secondary one).
- You'll be prompted to upload the requested document.
  - Click on **"Click Here to Upload Documents"**.
  - You'll see an EULA and a signature block.
  - Type your **name** (not the new user's name) and click **Submit**.

After completing these steps, your secondary credentials will be ready to use with the API connection. Use your **primary credentials** on the website and the **secondary credentials** for the API connection.

Using a Paper Trading Account
-----------------------------

When using a paper trading account, log in with your paper trading username and password. This allows you to practice trading without risking real money.

**Steps to get your paper trading username and password:**

1. Log in to the **IBKR Client Portal** using your primary (live) account credentials.
2. Navigate to the **Settings** menu in the upper right corner.
3. Under **Account Settings**, find the **Paper Trading Account** section.
4. Click on **Configure** or **Request Paper Trading Account**.
5. Follow the prompts to set up your paper trading account.
6. Once the setup is complete, you'll receive a separate **username** and **password** for your paper trading account.
7. Use these credentials when logging into the paper trading environment and configuring your API connection.

**Note:** The paper trading account is separate from your live account. Ensure you're using the correct credentials for each environment to avoid any login conflicts.

Strategy Setup
--------------

Add these variables to a `.env` file in the same directory as your strategy:

.. list-table:: Interactive Brokers Configuration
  :widths: 25 50 25
  :header-rows: 1

  * - **Secret**
    - **Description**
    - **Example**
  * - `IB_USERNAME`
    - Your Interactive Brokers username.
    - `user123`
  * - `IB_PASSWORD`
    - Your Interactive Brokers password.
    - `password123`
  * - `ACCOUNT_ID`
    - (Optional) An Interactive Brokers subaccount to trade on.
    - `U17369206`
  * - `IB_API_URL`
    - (Optional) The URL of your self-hosted Interactive Brokers REST API.
    - `https://localhost:8000`

Example Strategy
----------------

.. code-block:: python

  from lumibot.traders import Trader
  from lumibot.strategies.examples import Strangle

  trader = Trader()
  strategy = Strangle()
  trader.add_strategy(strategy)
  trader.run_all()
