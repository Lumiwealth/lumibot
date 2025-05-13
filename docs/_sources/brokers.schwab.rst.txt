Schwab API Setup
===============

Charles Schwab provides API access for automated trading and market data through its developer platform. This guide outlines how to set up your environment variables and complete the initial OAuth login process, which works seamlessly with any strategy once configured.

.. note::

   Before proceeding, ensure you have created a developer account at `developer.schwab.com <https://developer.schwab.com>`_ and registered an application to obtain your API credentials.

Developer Account and Application Setup
--------------------------------------

To use the Schwab API, you first need to create a developer account and register an application. This process will provide you with an **API Key** and **API Secret**, which are essential for authentication.

1. **Create a Developer Account**

   - Visit `developer.schwab.com <https://developer.schwab.com>`_ and click **Sign Up**.
   - Follow the instructions to create your account.
   - Log in to access your dashboard.

2. **Create and Configure an Application**

   - In your Schwab developer dashboard, navigate to the **Applications** section and click **Create Application**.
   - Fill in the required fields (app name, description, API Product, etc.). We recommend selecting **Accounts and Trading Production** unless you have a specific reason to choose otherwise.
   - Specify an **Order Limit** (the default is often 120 orders per minute).
   - Set your **Callback URL** to:
     
     ``https://127.0.0.1:8182``
     
     .. important::
        Do not include a trailing slash.

   - Submit your application and wait for it to be approved (the status should eventually change to **Ready For Use**).
   - Once approved, retrieve your **API Key** and **API Secret** from the application details.

Environment Variables Setup
--------------------------

For Schwab API integration, you must set the following environment variables in your ``.env`` file (located in the same directory as your strategy). These variables allow your application to authenticate with Schwab's API seamlessly with any strategy you use.

.. list-table:: Schwab Configuration
   :widths: 25 50 25
   :header-rows: 1

   * - **Variable**
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

.. important::
   
   Double-check that the API key, secret, and callback URL are entered exactly as specified on the Schwab developer portal to avoid authentication issues.

OAuth Process and Token Creation
-------------------------------

When you run your trading application for the first time, an OAuth flow will be initiated to securely log you into your Schwab account. During this process, you will see output in your terminal similar to:

.. code-block:: text

   This is the browser-assisted login and token creation flow for
   schwab-py. This flow automatically opens the login page on your
   browser, captures the resulting OAuth callback, and creates a token
   using the result. The authorization URL is:

   https://api.schwabapi.com/v1/oauth/authorize?response_type=code&client_id=RfUVxotUc8p6CbeCwFmophgNZSat0TLv&redirect_uri=https%3A%2F%2F127.0.0.1%3A8182&state=6pYvtte5gHRZKXRyrQjkjHNIYuO2Ra

   IMPORTANT: Your browser will give you a security warning about an
   invalid certificate prior to issuing the redirect. This is because
   schwab-py has started a server on your machine to receive the OAuth
   redirect using a self-signed SSL certificate. You can ignore that
   warning, but make sure to first check that the URL matches your
   callback URL (ignoring URL parameters). As a reminder, your callback URL
   is:

   https://127.0.0.1:8182

   See here to learn more about self-signed SSL certificates:
   https://schwab-py.readthedocs.io/en/latest/auth.html#ssl-errors

   If you encounter any issues, see here for troubleshooting:
   https://schwab-py.readthedocs.io/en/latest/auth.html#troubleshooting

   ⸻

   Press ENTER to open the browser. Note you can call this method with interactive=False to skip this input.

After completing the OAuth flow:

- A ``token.json`` file will be created and saved on your system. This file stores your login details (access tokens) so that you do not need to complete the OAuth process every time you run your application.
- Ensure you keep this file secure, as it contains sensitive authentication details.

Summary
------

1. **Environment Variables**: Set ``SCHWAB_API_KEY``, ``SCHWAB_SECRET``, and ``SCHWAB_ACCOUNT_NUMBER`` in your ``.env`` file.  
2. **OAuth Flow**: On the first run, you'll complete a browser-assisted login process. A ``token.json`` file will be created to store your session tokens.  
3. **Callback URL**: Use ``https://127.0.0.1:8182`` exactly as specified when creating your application.

By following these steps, your Schwab API integration should be up and running with any trading strategy you choose to deploy. Happy trading!