Interactive Brokers
===================

Interactive Brokers is a brokerage firm that operates in most countries around the world. It's a great choice for investing and trading, especially if you don't have access to other supported platforms.

**Note:** If you have access to other supported brokers, compare their fee structures to see which one offers the best deal for you.

.. note::
   **Easy Setup with .env File**
   
   LumiBot automatically loads your API credentials from a `.env` file! Simply create a `.env` file in the same folder as your trading strategy and add your Interactive Brokers credentials. LumiBot will automatically detect and use these credentials - no additional configuration required.
   
   **Example .env file:**
   
   .. code-block:: bash
   
      # Interactive Brokers Configuration  
      IB_USERNAME=your_ib_username
      IB_PASSWORD=your_ib_password
      IB_ACCOUNT_ID=your_account_id
   
   That's it! LumiBot handles the rest automatically.

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

Interactive Brokers requires two-factor authentication for Client Portal Gateway users. Individual users must authenticate in a browser on the same machine as the gateway and reauthenticate at least daily. LumiBot does not provide a supported way to bypass this requirement.

See `IBKR Web API documentation <https://ibkrcampus.com/campus/ibkr-api-page/webapi-doc/>`_.

.. warning::

  LumiBot can start IBeam as a local convenience for controlled individual or internal paper testing. IBeam is a third-party wrapper, and IBKR does not support automated Client Portal Gateway authentication or third-party wrappers. Do not use this path to collect customer credentials. Commercial third-party products should complete IBKR onboarding and use approved OAuth.

Using a Secondary Username
--------------------------

IBKR allows one active brokerage session per username. Logging into Client Portal, TWS, IB Gateway, or another trading-enabled session with the same username can displace the API session.

An additional username can isolate API activity from normal Client Portal use. It does not remove IBKR's 2FA or session-authentication requirements. Market-data subscriptions are username-specific and may incur separate fees.

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

Client Portal REST order-expiry limitation
------------------------------------------

The Interactive Brokers Client Portal REST adapter supports the currently
documented time-in-force values such as ``day`` and ``gtc``. It rejects
``time_in_force="gtd"`` and any ``good_till_date`` because IBKR's Client
Portal order schema does not document a verified exact-date expiration field.
Use the Legacy Interactive Brokers Gateway adapter when your strategy requires
exact-date GTD orders. Orders created through another IBKR interface can still
be read by the REST adapter.

REST advanced orders
--------------------

The LumiBot ``Order`` entity is provider-generic. The
``InteractiveBrokersREST`` adapter performs the IBKR-specific translation:

* **BRACKET** sends one atomic request containing the executable parent and
  one or two attached children.
* **OTO** sends one atomic request containing the executable parent and its
  single attached child.
* **OCO** sends only its two executable children as an IBKR single-group/OCA
  package. Each child receives a unique IBKR client order ID so responses can
  be correlated even when IBKR returns them out of request order. Its LumiBot
  parent is a local container and is never sent to IBKR.

Each executable leg receives and tracks a separate broker order ID. Canceling
a BRACKET or OTO parent attempts all known broker-backed members; canceling an
OCO parent attempts both executable children. A direct child cancellation is
limited to that child. The adapter continues attempting remaining members if
one cancellation fails.

For example, advanced orders use the current generic names
``secondary_limit_price`` and ``secondary_stop_price``:

.. code-block:: python

   order = strategy.create_order(
       asset,
       quantity=10,
       side="buy",
       order_type="limit",
       order_class="bracket",
       limit_price=100,
       secondary_limit_price=110,
       secondary_stop_price=95,
   )

REST polling updates the already-tracked native legs and preserves their
parent/child relationships across IBKR integer or string identifier formats.
Reconstruction of an advanced package submitted before process startup is not
claimed. See the `IBKR Client Portal API documentation
<https://ibkrcampus.com/campus/ibkr-api-page/webapi-doc/#orders>`_ and the
`new-order endpoint reference
<https://ibkrcampus.com/docs/web-api/api-reference/trading/trading-orders/submit-new-order>`_
for provider details.

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
    - `<your-paper-username>`
  * - `IB_PASSWORD`
    - Your Interactive Brokers password.
    - `<your-paper-password>`
  * - `IB_ACCOUNT_ID`
    - (Optional) An Interactive Brokers subaccount to trade on.
    - `<your-account-id>`
  * - `IB_API_URL`
    - (Optional) URL of an externally managed Client Portal or approved REST transport.
    - `https://localhost:4234`
  * - `IB_USE_PAPER_ACCOUNT`
    - Local IBeam paper-account toggle. Defaults to ``true``.
    - `true`
  * - `IB_GATEWAY_PORT`
    - (Optional) Host port for local IBeam or a localhost sidecar.
    - `4234`
  * - `IB_AUTH_TIMEOUT`
    - (Optional) Maximum authentication wait in seconds.
    - `300`

REST Paper-Order Test Safety
----------------------------

LumiBot's repository-only IBKR REST paper-order API-test fixture requires
``IB_USE_PAPER_ACCOUNT=true`` to be explicitly present, even though the local
IBeam configuration defaults to paper mode. Before an order test constructs an
order, the fixture verifies that the authenticated selected account uses the
IBKR paper-account ``DU`` convention and that it matches an explicitly supplied
``IB_ACCOUNT_ID``. Test output masks account identifiers; do not add credentials
or account identifiers to test logs.
Within one pytest invocation, the fixture reuses one authenticated data source
so individual paper tests do not start competing local IBeam containers.

.. warning::

   The opt-in IBKR REST paper suites are real broker API tests, even though
   they use a paper account, and are excluded from ordinary CI. An authorized
   maintainer must use a dedicated paper username with an already authenticated
   paper gateway and set ``IB_USE_PAPER_ACCOUNT=true`` explicitly; never use a
   production account for these tests. The
   authenticated selected account must pass the paper ``DU`` identity gate.
   For an external ``IB_API_URL`` gateway, the session must already be the
   paper session; the local flag cannot convert a live session. The advanced
   suite submits deliberately non-marketable orders and attempts cancellation,
   but process termination can interrupt cleanup, so inspect the paper account
   afterward. Only masked account suffixes may be shown in output. Passing
   these tests does not establish production OAuth readiness.

   The GTD capability probe is separate from advanced-order submission tests
   and uses only the non-ordering ``/orders/whatif`` endpoint. It does not
   enable production GTD behavior; any production implementation requires a
   separate reviewed decision.

Example Strategy
----------------

.. code-block:: python

  from lumibot.traders import Trader
  from lumibot.strategies.examples import Strangle

  trader = Trader()
  strategy = Strangle()
  trader.add_strategy(strategy)
  trader.run_all()
