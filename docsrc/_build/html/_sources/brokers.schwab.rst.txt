Schwab
======

Lumibot integrates directly with Charles Schwab's *Trader* API for equities and options.  Everything you need is built-in; no external wrapper is required.

Prerequisites
-------------

1. A Schwab brokerage account that is **approved for API access** (apply once in the Schwab Developer Portal).
2. A Schwab **App Key** (sometimes called *Consumer Key*) generated inside your Developer Portal application.
3. The brokerage **account number** you want the bot to trade in.
4. A **callback URL** (HTTPS) you entered when creating the app.  For local testing just use ``https://127.0.0.1:8182``.

Environment variables
---------------------

.. note::
   **Easy Setup with .env File**
   
   LumiBot automatically loads your API credentials from a `.env` file! Simply create a `.env` file in the same folder as your trading strategy and add your Schwab credentials. LumiBot will automatically detect and use these credentials - no additional configuration required.
   
   **Example .env file:**
   
   .. code-block:: bash
   
      # Schwab Configuration
      SCHWAB_ACCOUNT_NUMBER=your_account_number
      SCHWAB_TOKEN=your_token_if_needed
      TRADING_BROKER=schwab
   
   That's it! LumiBot handles the rest automatically.

Set the following before running your strategy (``.env`` file, Render secret, Replit secret, Docker env, etc.):

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - **Variable**
     - **Purpose**
   * - ``SCHWAB_ACCOUNT_NUMBER``
     - The brokerage account to trade.
   * - ``SCHWAB_TOKEN`` *(optional)*
     - Base-64 payload pasted from the **first** OAuth login screen – only needed for headless deploys.
   * - ``TRADING_BROKER`` *(optional)*
     - Force Lumibot to select Schwab (``schwab``) even when other creds are present.

First-time login
----------------

• **Desktop** – run your bot, a browser pops up, log in, click *Allow*.  A ``token.json`` file is written next to your strategy.  Restart and you're done.

• **Headless / Render / Replit** – the console prints a one-time URL.  Open it on any device, log in, copy the payload string that appears, set it as ``SCHWAB_TOKEN`` and restart.

Token life-cycle
----------------

* Access token ≈ 30 min; refresh token ≈ 7 days.
* Lumibot automatically refreshes the token every ~25 min and rewrites ``token.json``.
* As long as the bot is running (or restarted at least once a week) you will **never see the login page again**.
* If the bot is **offline > 7 days** the refresh token expires – simply run the login flow once more.

Supported functionality
-----------------------

* Equities and ETF trading (market, limit, stop, stop-limit).
* Single-leg options (buy/sell, open/close).
* Streaming quotes for equities/options.
* Historical bars – up to 15 years daily, 6 months intraday.

Multi-leg option spreads, advanced orders (OCO/OTO), and futures trades are not yet implemented.

Example ``.env``
----------------

.. code-block:: bash

   TRADING_BROKER=schwab
   SCHWAB_ACCOUNT_NUMBER=12345678
   # optional if deploying headless
   SCHWAB_TOKEN=YOUR_TOKEN

Example strategy snippet
------------------------

.. code-block:: python

   from lumibot.entities import Asset

   last = self.get_last_price("SPY")
   chains = self.get_chains(Asset("SPY"))

   first_expiry = chains.expirations("CALL")[0]
   atm_strike  = min(chains.strikes(first_expiry), key=lambda s: abs(s-last))

   contract = Asset(
       symbol="SPY",
       asset_type=Asset.AssetType.OPTION,
       expiration=first_expiry,
       strike=atm_strike,
       right=Asset.OptionRight.CALL,
   )
   order = self.create_order(contract, 1, side="buy")
   self.submit_order(order)

Troubleshooting
---------------

* **401/400 errors** at login usually mean your callback URL does not match the value in the Developer Portal **exactly**.
* Keep ``token.json`` out of version control.
* Schwab's API still evolves; join the Lumibot Discord for the latest community fixes.

.. note::
   Schwab API access requires a developer account and application approval. You must apply for API access and set up your app in the Schwab Developer Portal.

API Credentials
---------------

To use Schwab with Lumibot, you need to set the following environment variables in your `.env` file:

.. list-table:: Schwab API Credentials
  :widths: 30 50 20
  :header-rows: 1

  * - **Variable**
    - **Description**
    - **Example**
  * - `SCHWAB_API_KEY`
    - (old name) – **use `SCHWAB_APP_KEY` instead**. Back-compat supported but
      new projects should switch.
    - `abc123xyz`
  * - `SCHWAB_APP_SECRET`
    - Your Schwab API secret (Consumer Secret).
    - `supersecret`
  * - `SCHWAB_ACCOUNT_NUMBER`
    - Your Schwab brokerage account number.
    - `12345678`
  * - `SCHWAB_BACKEND_CALLBACK_URL`
    - The **exact** OAuth2 callback URL that you registered in the Developer
      Portal. Defaults to `https://127.0.0.1:8182` for local flows.
    - `https://yourdomain.com/callback`
  * - `TRADING_BROKER`
    - (Optional) Set to `schwab` to force Schwab as the broker.
    - `schwab`
  * - `SCHWAB_TOKEN`  
      *(optional)*
    - Base64url payload string returned by the **first** OAuth login.  Use it
      when running in head-less environments (Render, Replit, Docker) so the
      bot can bootstrap itself without an interactive prompt.
    - `<big-string>`

.. important::
   `SCHWAB_TOKEN` is only read **once** (on first run) to build `token.json`.
   After that, automatic refresh keeps the file current; you do **not** need to
   rotate the env-var every 7 days.

Token Life-cycle & Auto-refresh
-------------------------------

* Access-token ≈ 30 min, refresh-token ≈ 7 days (per Schwab policy).
* Lumibot configures an `OAuth2Session` with ``auto_refresh_url`` so that tokens
  refresh themselves quietly in the background every ~25 min.
* The refreshed token is written back to `token.json`; it rolls the 7-day window
  forward.  As long as the bot is running (or restarted at least once a week)
  you never need to log in again.
* Only if the service is **offline for >7 days** will the refresh-token expire.
  In that case repeat the browser login once and redeploy the new payload or
  token file.

Creating an App & Getting Keys
------------------------------

1. Register on the `Schwab Developer Portal <https://developer.schwab.com/>`_.
2. Go to **Dashboard → Apps → Create App**.
3. Enter an app name and a **Callback URL** (must be HTTPS, ≤ 256 chars, matches exactly).
4. Request the **Trader API** product, accept terms, and submit.
5. Wait for manual approval (typically 1–3 business days).
6. Once approved, copy your **API Key (Consumer Key)** and **API Secret** from the app details.

OAuth2 Authentication Flow
--------------------------

Schwab uses OAuth2 for authentication. The first time you run your strategy, a browser window will open for you to log in and approve access. A `token.json` file will be created in your strategy directory (or at `SCHWAB_TOKEN_PATH` if set).

- **Access tokens** last 30 minutes; **refresh tokens** last 7 days.
- The `schwab-py` library will auto-refresh tokens as needed.
- If running on a server, run the login flow once locally and copy `token.json` to the server.
- For headless/cloud environments, use the CLI/manual login helper (`schwab.auth.client_from_manual_flow`), which prints a URL to paste into any browser.
- Keep `token.json` secure and out of version control.
- If you delete or move `token.json`, you will need to re-authorize.

.. warning::
   If your refresh token expires (after 7 days without re-auth), you must repeat the browser login flow.

**First-time Schwab login (cloud or local)**

- **Cloud (Replit, Render, etc.):**  
  Deploy the bot and watch the logs for a green line:  
  `Open https://…/schwab-login in your browser`  
  Click, sign in, hit **Allow**, wait for "✅ Schwab token saved", then restart the bot.  
  That's it—no weekly re-login as long as the bot stays active.

- **Local laptop:**  
  Deploy the bot and Lumibot opens a browser window automatically (same as before).  
  Complete the login and you're set.

As long as your bot checks Schwab at least once per day, the token
auto-refreshes and you will *not* be asked to log in again.  
If the service is stopped for 7+ days, redeploy and repeat the link.

(Optional) override the callback route with  
`SCHWAB_REDIRECT_URI=https://YOUR_DOMAIN/schwab-login`.

Sandbox vs Production
---------------------

Schwab offers a **Sandbox** environment for safe testing with synthetic accounts and data.

- Enable Sandbox when creating your app, or promote your app later in the Developer Portal.
- Use the same credentials; only the API base URL changes.
- Use separate apps for production and sandbox to avoid confusion.

Supported Assets & Order Types
------------------------------

.. list-table:: Supported Asset Classes and Order Types
  :widths: 20 15 15 15 15 20
  :header-rows: 1

  * - **Asset**
    - **Market**
    - **Limit**
    - **Stop**
    - **Stop-Limit**
    - **Advanced (OCO/Bracket)**
  * - Stocks/ETFs
    - ✔
    - ✔
    - ✔
    - ✔
    - ✖ (not yet)
  * - Options
    - ✔ (buy/sell, open/close)
    - ✔
    - —
    - —
    - ✖ (not yet)
  * - Futures
    - ✖ (quotes only)
    - ✖
    - ✖
    - ✖
    - ✖

- Multi-leg/spread options and advanced orders are not yet implemented in Lumibot.
- **Futures trading is not supported; only streaming quotes are available.**

Market Data
-----------

- Real-time quotes, option chains, and historical bars (up to 15 years daily, 6 months intraday for equities/options).
- **Level-I/II streaming quotes are available for equities, options, and futures; historical bars only for equities/ETFs.**
- No extra entitlements required for individual developers.
- Futures quotes available; historical futures bars not yet supported.

Rate Limits & Token Expiry
--------------------------

- **~120 requests/minute** for data; **2–4 trade requests/sec**.
- Exceeding limits returns HTTP 429 errors.
- Error codes: `429-001` = rate, `429-005` = burst; back-off 60 seconds if hit.
- Access tokens expire after 30 minutes; refresh tokens after 7 days.

Known Issues & Best Practices
-----------------------------

- Initial OAuth requires browser login every 7 days.
- `token.json` must be unique per account/app.
- Advanced orders (OCO/OTO/Bracket) not yet supported.
- Callback URL must match exactly (including trailing slash).
- Refresh tokens proactively (every 28–29 min) to avoid expiry.
- Secure `token.json` (chmod 600) and rotate secrets regularly.
- Use separate apps for sandbox and production.
- **Attempting to place a futures order returns HTTP 400 "Unsupported instrument".**
- **No official docs for futures endpoints—implementation subject to change.**

Example Strategy
----------------

You can provide your Schwab credentials in several ways:
- By creating a `.env` file in the same directory as your strategy (recommended for local development).
- By setting them as secrets in Replit, or as environment variables in cloud platforms like Render.
- By exporting them as environment variables in your shell.

**Example `.env` file:**

.. code-block:: bash

   # .env
   TRADING_BROKER=schwab
   SCHWAB_ACCOUNT_NUMBER=XXXXXXXX

Then, create your `main.py` (or `strategy.py`) file:

.. code-block:: python

   from lumibot.traders import Trader
   from lumibot.strategies.strategy import Strategy

   class MyStrategy(Strategy):
       def initialize(self):
           self.sleeptime = "1D"
           self.symbol = "SPY"

       def on_trading_iteration(self):
           last = self.get_last_price(self.symbol)
           self.log_message(f"Last price for {self.symbol}: {last}")
           asset = self.create_asset(self.symbol)
           order = self.create_order(asset, 1, "buy")
           self.submit_order(order)

   trader = Trader()
   strategy = MyStrategy()
   trader.add_strategy(strategy)
   trader.run_all()

Support & Contact
-----------------

- Schwab Developer Portal: https://developer.schwab.com/
- API Documentation: https://schwab-py.readthedocs.io/
- Support: Developer Portal → Support → Create Ticket, or email api-development@schwab.com

.. note::
   For advanced usage and troubleshooting, see the `schwab-py documentation <https://schwab-py.readthedocs.io/>`_ and the Lumibot source code for `Schwab` broker and `SchwabData` data source.

.. important::
   The example above shows what *our strategy* did in a sandbox environment; it is **not** investment advice.

.. disclaimer::
   This integration is for educational purposes only. Please consult with a financial advisor before using any trading strategy with real funds.