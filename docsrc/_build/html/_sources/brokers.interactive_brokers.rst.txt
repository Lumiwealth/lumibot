Interactive Brokers
===================

Interactive Brokers is a brokerage firm that operates in most countries around the world. This makes it a great choice for investing and trading if you don't have access to our other supported platforms.

**Note:** If you do have access to other supported brokers, compare their fee structures to see which one is the best deal for you.

Market Data Subscriptions
-------------------------
To access real-time market data, you need to subscribe to the appropriate market data packages. Interactive Brokers offers various market data subscriptions depending on the exchanges and types of data you need. Here are some common subscriptions:

- US Securities Snapshot and Futures Value Bundle
- US Equity and Options Add-On Streaming Bundle
- NASDAQ (Network C/UTP) TotalView
- NYSE (Network A/CTA) OpenBook Ultra
- OPRA (US Options Exchanges)

.. note::

    Different strategies may require different market data subscriptions. For our top options-related strategies, the OPRA (US Options Exchanges) should suffice.

**To subscribe to market data:**

1. Log in to the IBKR Client Portal.
2. Navigate to the "Settings" menu.
3. Under "Account Settings", find the "Market Data Subscriptions" section.
4. Click "Configure" and select the desired market data packages.
5. Follow the prompts to complete the subscription process.

**Note:** Market data subscriptions may incur additional fees. Be sure to review the costs associated with each package before subscribing.

Automating Two-Factor Authentication
------------------------------------
Currently, automating two-factor authentication (2FA) is not supported. For now, Lumibot will send you 2FA notifications through IBKey, and you will need to respond to them manually.

Using a Secondary Account
-------------------------
IBKR only permits a single login at a time for any given set of credentials. Consequently, you can't use the IBKR website while the Gateway (or a sort of api connection) is running. If you try to anyway, the Gateway will be disconnected, which will trigger a re-authentication attempt, which will potentially disconnect your website session. If you then log in to the website again, you'll just start the loop all over again. An easy solution is to stop a sort of api connection before using the website, but that can be tedious.

A more convenient solution is to create a second username for your account and use that for a sort of api connection. From the client portal API documentation section "Multiple usernames":

Clients wishing to use multiple IBKR products at the same time (TWS, IBKR Mobile or Client Portal) can do so by creating a new username that can then be used to log into other services while using the Client Portal API. To create a second username please see the following IBKR Knowledge Base article.

The Knowledge Base article linked above is slightly out of date with respect to the current layout of the IBKR Client Portal:

- The "Manage Account" item in the User menu (upper right corner) is now called "Settings"
- The "Users & Access Rights" panel is no longer on the Settings page, but you can find a similarly named link at the very bottom-left of the page (you may have to scroll to find it)
- The resulting page doesn't have a "Configure (gear) icon". It does have a plus sign button in the "Users" header that does the same thing.

From there you'll be shown a fairly standard-looking username and password dialog. Fill it out in the normal fashion, then click through the many many pages of boilerplate and settings. Feel free to disable many of the notification settings for the new username. Otherwise you'll start getting duplicate emails from IBKR.

At the end of the process, you'll get an email with a confirmation code, but no indication as to what to do with it. Log out and log back in using the new username and password that you just created. You'll then be prompted to enter the confirmation code. Note that you need to be quick about this part because the code expires fast. If you're not fast enough you'll get another automated email with a new code a few minutes after the previous one expired. In fact, you'll keep getting new codes until you get it right.

After entering the confirmation code you'll likely be prompted to change the password for the secondary user.

The final screen of the process says that the request for account creation will be reviewed on the next business day. Some time later you'll receive an email asking you to upload a "Proof of trader authority" for the new username. The verbiage implies that you need to generate a document to upload, however that isn't the case. Simply log in using your primary username (not the secondary!) and you'll be prompted to upload the requested document. At the bottom of the pop-up will be a button titled "Click Here to Upload Documents". Click that and you'll be shown a EULA and a signature block. Type your name (not the new user's name) and click Submit.

After that you should be back at the normal dashboard page, with little indication that you're logged in as the second user. Log out and the new credentials will be ready to use with a sort of api connection. In the future be sure to only use your primary credentials on the website and the secondary credentials for a sort of api connection.

**Source:** `IBeam GitHub <https://github.com/Voyz/ibeam/wiki/Runtime-environment>`_

Strategy Setup
--------------
Add these variables to a `.env` file in the same directory as your strategy:

.. list-table:: Interactive Brokers Configuration
    :widths: 25 50 25
    :header-rows: 1

    * - **Secret**
      - **Description**
      - **Example**
    * - IB_USERNAME
      - Your Interactive Brokers username.
      - `user123`
    * - IB_PASSWORD
      - Your Interactive Brokers password.
      - `password123`
    * - ACCOUNT_ID
      - (Optional) An Interactive Brokers subaccount to trade on.
      - `U17369206`
    * - IB_API_URL
      - (Optional) The URL of your self-hosted Interactive Brokers REST API. You likely don't need it.
      - `https://localhost:8000`

Example Strategy
----------------

.. code-block:: python
     :emphasize-lines: 3,12

     from lumibot.traders import Trader
     # Import interactive brokers
     from lumibot.strategies.examples import Strangle

     trader = Trader()

     strategy = Strangle()
     trader.add_strategy(strategy)
     trader.run_all()