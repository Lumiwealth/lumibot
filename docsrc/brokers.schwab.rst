Schwab
======

Lumibot integrates directly with Charles Schwab's *Trader* API for equities and options.  Everything you need is built-in; no external wrapper is required.

If you want the managed path, `BotSpot <https://botspot.trade/sales?showLogin=1&utm_source=documentation&utm_medium=schwab&utm_campaign=lumibot&utm_content=managed_schwab_text&prompt=I%20want%20to%20connect%20Schwab%20and%20run%20a%20Lumibot%20strategy%20on%20BotSpot.%20Please%20help%20me%20set%20up%20broker%20connections%2C%20monitoring%2C%20and%20paper%20or%20live%20deployment.>`_ can help you connect supported brokers through the website, run paper or live Lumibot strategies, and monitor logs, account state, alerts, audit history, and kill-switch controls without maintaining your own deployment server.

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
* OTO / one-triggers-other orders for single stock/ETF and option parent/child orders (experimental; live broker validation recommended before production use).
* OCO / one-cancels-other and bracket orders for single stock/ETF and option orders (experimental; live broker validation recommended before production use).
* Streaming quotes for equities/options.
* Historical bars – up to 15 years daily, 6 months intraday.

Multi-leg option spreads and futures trades are not yet implemented.

Order and position freshness
----------------------------

In live trading, LumiBot refreshes broker state before returning from:

* ``self.get_order(order_id)``
* ``self.get_orders(...)``
* ``self.get_position(asset)``
* ``self.get_positions()``
* ``self.get_cash()``
* ``self.get_portfolio_value()``

After submitting an order, use direct order lookup when checking that exact
order:

.. code-block:: python

   submitted = self.submit_order(order)
   latest = self.get_order(submitted.identifier)

Schwab can expose a just-submitted order through direct lookup before it appears
in the broad account order list. If your strategy must use ``self.get_orders(...)``
immediately after submitting, add a short LumiBot sleep first:

.. code-block:: python

   submitted = self.submit_order(order)
   self.sleep(1, process_pending_orders=True)
   active_orders = self.get_orders(statuses=Order.ACTIVE_STATUSES)

Use enum status filters for active/open order checks:

.. code-block:: python

   active_orders = self.get_orders(statuses=Order.ACTIVE_STATUSES)

Do not use raw string status filters. For duplicate-order guards, filter by
active status and by the exact asset or option contract.

Simple stock and single-leg option ``self.modify_order(...)`` calls use Schwab's
replace-order endpoint. Schwab returns a new broker order id for the replacement;
LumiBot updates the order object's ``identifier`` and keeps the old id in
``previous_identifiers``. For timeout logic where the intended behavior is to
remove the order, prefer ``self.cancel_order(order)`` instead of modifying the
order into a different price. A successful cancel response is acceptance, not
terminal confirmation: the order remains active with ``CANCELLING`` status
until Schwab later reports ``CANCELED``, ``FILLED``, ``EXPIRED``, or a
rejection/error. Do not put an immediate direct order read in front of the
cancellation deadline.

Schwab account history can include rows that are not ordinary strategy orders,
such as mutual funds, sweep/cash-equivalent records, bonds, option exercise
records, or future Schwab asset/order/status values. LumiBot preserves
representable unknown Schwab records as ``Asset.AssetType.UNKNOWN``,
``Order.OrderType.UNKNOWN``, ``Order.OrderSide.UNKNOWN``, or
``Order.OrderStatus.UNKNOWN`` instead of failing the whole broker refresh.

If Schwab cannot return fresh cash or portfolio value, ``self.get_cash()`` and
``self.get_portfolio_value()`` return ``None`` and leave cached values unchanged.
Do not treat ``None`` as zero.

Fast cancellation and request budgets
-------------------------------------

Separate three measurements when implementing a cancel-after deadline:

* the local time at which the strategy dispatches ``cancel_order``;
* the HTTP response time for the cancel request;
* the later broker-terminal outcome such as ``CANCELED`` or ``FILLED``.

The strategy controls the first measurement. It cannot guarantee the other two
at an exact deadline.

Use a configurable ``cancel_after_seconds`` policy and an absolute
``time.monotonic()`` deadline. Do not use one fixed timeout for every strategy.
Finish expensive chain, quote, and liquidity work before submission; then
process local pending order events while waiting for the deadline:

.. code-block:: python

   deadline = time.monotonic() + cancel_after_seconds
   while time.monotonic() < deadline and order.is_active():
       remaining = deadline - time.monotonic()
       self.sleep(min(0.05, remaining), process_pending_orders=True)

   if order.is_active():
       self.cancel_order(order)

The short ``self.sleep`` calls above process local queued events. They are not
broker polls. Avoid calling ``self.get_order`` every fraction of a second or
placing a broker read immediately before the deadline. Use a bounded exact-order
read later for a missed callback, restart/reconnect, or ambiguous cancel result.

``on_filled_order`` is the fast path for a fill and already includes the filled
order. ``on_canceled_order`` reports terminal cancellation; it does not initiate
the cancel. ``cancel_order`` may return before the queued
``on_canceled_order`` callback runs. Route callbacks and later reconciliation
through one idempotent reducer keyed by the broker order identifier or a stable
causal group.

These ``on_*`` methods are lifecycle callback methods: LumiBot invokes them
after broker observations. ``on_partially_filled_order(position, order, price,
quantity, multiplier)`` receives the newly observed fill delta in ``quantity``;
it is not cumulative across callbacks. A later ``on_filled_order`` receives the
remaining fill delta and must share the same idempotency state.

LumiBot uses Schwab account-activity WebSocket messages to wake exact reads of
locally tracked active orders. REST snapshots and stream-triggered observations
feed one serialized transition reducer, including after login or reconnect. A
30-second broad history poll remains as a healing fallback. This is deliberately
not one-second broad polling: one-second polling multiplies request pressure and
does not remove fill/cancel races.

Scope blocking to the strategy's actual risk invariant. A cancel-pending order
must block a conflicting replacement for the same exposure. Independent symbols
may continue when capital and risk policy permit. Unknown broker state is not
terminal, but it does not automatically require a strategy-wide freeze.

Schwab developer applications have an application-level order limit for make,
cancel, and replace requests per minute. Treat throttling as an aggregate
broker-call budget across market data, order lists, exact reads, submits,
cancels, and replaces. Do not infer a universal requests-per-second guarantee.
See the `schwab-py order-limit documentation
<https://schwab-py.readthedocs.io/en/latest/getting-started.html#order-limit>`_
and :doc:`lifecycle_methods.on_canceled_order`.

When Schwab returns HTTP 429, LumiBot honors ``Retry-After`` when present and
otherwise applies bounded exponential backoff with jitter for that endpoint
family. A throttled read returns no new observation and never converts the
tracked order to a terminal status.

An authorized local sample of 16 successful cancel HTTP responses ranged from
228 ms to 444 ms, with a 302.5 ms median and 444 ms 95th percentile. This small
sample is not a Schwab service-level guarantee and does not measure terminal
callback visibility. Do not choose a strategy deadline by multiplying these
observations.

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
* In managed environments where a trusted parent process owns broker OAuth
  refresh, set ``LUMIBOT_OAUTH_REFRESH_MODE=external``. In that mode LumiBot does
  not call the Schwab OAuth refresh endpoint; it reloads an access-token-only
  token file when the parent atomically replaces it. Refresh-token fields are
  stripped from LumiBot's child broker state in this mode.
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
    - **Advanced**
  * - Stocks/ETFs
    - ✔
    - ✔
    - ✔
    - ✔
    - OTO/OCO/bracket (experimental)
  * - Options
    - ✔ (buy/sell, open/close)
    - ✔
    - —
    - —
    - OTO/OCO/bracket (experimental)
  * - Futures
    - ✖ (quotes only)
    - ✖
    - ✖
    - ✖
    - ✖

- Multi-leg/spread options are not yet implemented in Lumibot.
- Schwab OTO, OCO, and bracket orders use Schwab's trigger and one-cancels-other support and should be live-tested with the target account and order shape before relying on them in production.
- **Futures trading is not supported; only streaming quotes are available.**

Market Data
-----------

- Real-time quotes, option chains, and historical bars (up to 15 years daily, 6 months intraday for equities/options).
- **Level-I/II streaming quotes are available for equities, options, and futures; historical bars only for equities/ETFs.**
- No extra entitlements required for individual developers.
- Futures quotes available; historical futures bars not yet supported.

Rate Limits & Token Expiry
--------------------------

- Schwab developer applications expose a configurable **order limit**: the
  number of make, cancel, and replace requests the app may place per minute.
  The application's configured value is authoritative for that app.
- Schwab may return HTTP 429 when a request budget or burst limit is exceeded.
  Respect ``Retry-After`` when present; otherwise use bounded exponential
  backoff with jitter.
- Do not rely on a universal data-requests-per-minute or trade-requests-per-second
  value. Budget all broker endpoint families and measure the target app's
  actual behavior.
- Access tokens expire after 30 minutes; refresh tokens after 7 days.

Known Issues & Best Practices
-----------------------------

- Initial OAuth requires browser login every 7 days.
- `token.json` must be unique per account/app.
- OTO, OCO, and bracket advanced orders are experimental.
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

.. warning::
   This integration is for educational purposes only. Consult a qualified financial advisor before trading real funds.
