.. _environment_variables:

Environment Variables
=====================

LumiBot supports configuring many behaviors via environment variables. This page documents the variables most commonly used for **backtesting**, **ThetaData**, and **remote caching**.

.. important::

   **Never commit secrets** (API keys, passwords, AWS secret keys) into any repo or docs. Document variable names and semantics only.

Backtesting configuration
-------------------------

LUMIBOT_DISABLE_DOTENV
^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Disable automatic ``.env`` discovery and loading at startup.
- Values: truthy enables (``1``, ``true``, ``yes``); unset/``0`` disables.
- Default: disabled.
- Notes:
  - When dotenv loading is enabled, LumiBot looks upward from the running script directory for the nearest ``.env`` file. If none is found there, it also checks upward from the current working directory.
  - LumiBot no longer recursively scans every nested directory under the start path. This reduces startup latency and lowers the chance of loading an unrelated nested repo's ``.env`` file.
  - In production/BotManager/BotSpot runs, prefer injected environment variables and set ``LUMIBOT_DISABLE_DOTENV=1`` so local files cannot override runtime secrets.

LUMIBOT_DISABLE_DOTENV_LOCAL
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Skip the optional ``.env.local`` override file while still allowing the primary ``.env`` file.
- Values: truthy enables (``1``, ``true``, ``yes``, ``on``); unset/``0`` disables.
- Default: disabled.
- Notes:
  - When dotenv loading is enabled and a ``.env`` file is found, LumiBot also loads a sibling ``.env.local`` after ``.env`` with override behavior.
  - Use ``.env.local`` only for local developer overrides. Do not commit it.
  - Set ``LUMIBOT_DISABLE_DOTENV_LOCAL=1`` in repeatable test or deployment contexts where the base ``.env`` should be loaded but local overrides must be ignored.

IS_BACKTESTING
^^^^^^^^^^^^^^

- Purpose: Signals backtesting mode for certain code paths.
- Values: ``True`` / ``False`` (string).

BACKTESTING_START / BACKTESTING_END
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Default date range used when dates are not passed in code.
- Format: ``YYYY-MM-DD``

BACKTESTING_BUDGET
^^^^^^^^^^^^^^^^^^

- Purpose: Override the starting cash used for backtests (initial portfolio cash).
- Format: Positive number. Accepted examples: ``500``, ``5000``, ``5k``, ``1_000_000``, ``$10,000``.
- Notes:
  - When set, this value is preferred over any ``budget=`` passed in strategy code, so it can be controlled per-run via injected environment variables.
  - Default (when unset and no code budget is provided): ``100000``.

BACKTESTING_PARAMETERS
^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Override or inject strategy parameters via environment variable, without modifying strategy code.
- Format: JSON string representing a dictionary. Example: ``{"symbol": "AAPL", "quantity": 10}``
- Notes:
  - When set, the parsed dict is merged on top of the strategy's existing ``parameters`` dict with highest priority (wins over both class-level defaults and code-level overrides).
  - Useful for parameter sweeps: run the same strategy code with different parameter sets per backtest.
  - Nested dicts are supported (e.g. ``{"ALLOCATION": {"SPY": 0.50, "IWM": 0.50}}``).
  - Invalid JSON or non-dict values are ignored with a warning.

BACKTESTING_DATA_SOURCE
^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Select the backtesting datasource **even if your code passes a `datasource_class`**.
- Values (case-insensitive):
  - ``thetadata``, ``yahoo``, ``polygon``, ``alpaca``, ``ccxt``, ``databento``, ``polymarket``, ``polymarket_clob``
  - ``ibkr`` / ``interactivebrokersrest`` / ``interactive_brokers_rest`` (IBKR Client Portal REST)
  - ``router`` (multi-provider routing; defaults to Theta for stock/option/index and IBKR for futures/crypto)
  - JSON mapping (multi-provider routing by asset type), e.g. ``{"default":"thetadata","stock":"thetadata","option":"thetadata","index":"thetadata","future":"ibkr","crypto":"ibkr"}``

    - Provider values are case/whitespace/_/- insensitive.
    - Supported values include ``thetadata``, ``ibkr``, ``polygon``, ``alpaca``, ``ccxt``, and ``polymarket``.
    - For CCXT backtesting, you may use ``ccxt`` (auto-select exchange from existing env/credentials) **or** specify a supported CCXT backtesting exchange id directly. Documented backtesting examples are ``kraken``, ``binance``, ``kucoin``, ``bitmex``, ``bybit``, and ``okx``.
    - Routing keys are the canonical asset types (``future``, ``cont_future``, ``crypto``, etc.). Common plural aliases like ``futures``/``cont_futures`` are accepted.

  - ``none`` to disable the env override and rely on code.

Testing / CI guardrails
-----------------------

LUMIBOT_ACCEPTANCE_TRIPWIRE
^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: **Acceptance backtests only** — when truthy, a Python startup hook aborts the subprocess the moment it attempts to call a remote data service.
- Values: truthy enables (``1``, ``true``, ``yes``); unset/``0`` disables.
- Notes:
  - This is an engineering/CI guardrail to enforce “warm-cache” acceptance backtests. It should not be used for normal production backtests.
  - When triggered, it prints a marker and exits the subprocess with a non-zero code so the test fails reliably.

Live scheduled execution (BotSpot/BotManager)
---------------------------------------------

- ``LUMIBOT_SCHEDULED_EXECUTION``: internal BotManager flag. Truthy values (``1``, ``true``, ``yes``, ``y``, ``on``) make ``Strategy.run_live()`` run one live iteration and exit.
- ``LUMIBOT_SCHEDULED_TARGET_RUN_AT``: UTC ISO-8601 target time for exact scheduled runs. When present, LumiBot initializes the strategy/broker first, waits locally until this timestamp immediately before ``on_trading_iteration()``, and skips the iteration if the drift budget is exceeded.
- ``LUMIBOT_SCHEDULED_PRE_START_AT``: UTC ISO-8601 pre-start time used by BotManager telemetry to compare scheduler launch timing with the requested target.
- ``LUMIBOT_SCHEDULED_MAX_TARGET_DRIFT_MS``: maximum allowed late drift in milliseconds for exact scheduled runs. Defaults to ``1000``.
- ``LUMIBOT_SCHEDULED_POST_ITERATION_SECONDS``: drain window after the one live iteration. During this window LumiBot continues processing broker/order queue events before exiting.
- ``LUMIBOT_SCHEDULED_TIMING_FILE``: local JSON timing file written by LumiBot for BotManager bootstrap telemetry.
- ``LUMIBOT_SCHEDULED_STATE_BACKEND``: external state backend prepared by BotManager: ``s3``, ``dynamodb``, or ``none``. ``none`` disables scheduled ``self.vars`` file load/save.
- ``LUMIBOT_SCHEDULED_STATE_FILE``: local JSON file managed by BotManager/bootstrap code to restore and persist ``self.vars`` for one scheduled live run. State is restored before scheduled lifecycle hooks.

Backtest artifacts + UX flags
-----------------------------

SHOW_PLOT / SHOW_INDICATORS / SHOW_TEARSHEET
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Enable/disable artifact generation.
- Values: ``True`` / ``False`` (string).

LUMIBOT_BACKTEST_PARQUET_MODE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Control parquet export semantics for backtest artifacts (indicators/trades/stats/trade events).
- Values:
  - ``best_effort`` (default): parquet failures log warnings; CSV remains the compatibility layer.
  - ``required``: parquet export failures raise and should fail the backtest (artifact contract mode).
- Notes:
  - This is primarily intended for BotManager/BotSpot backtests where downstream tools depend on Parquet for performance.
  - When set to ``required``, a parquet export error should fail the backtest so missing artifacts are never silently ignored.

BACKTESTING_QUIET_LOGS
^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Reduce log noise during backtests.
- Values: ``true`` / ``false`` (string).

BACKTESTING_SHOW_PROGRESS_BAR
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Enable progress bar updates.
- Values: ``true`` / ``false`` (string).

Backtest progress file (BotSpot/BotManager UI)
----------------------------------------------

LOG_BACKTEST_PROGRESS_TO_FILE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: When truthy, write ``logs/progress.csv`` during backtests so BotManager/BotSpot can show live progress.
- Values: truthy enables (``1``, ``true``, ``yes``); unset/``0`` disables.
- Notes:
  - On startup, LumiBot writes an initial ``progress.csv`` row immediately to reduce “time-to-first-progress” latency for short backtests.
  - In BotManager, a background thread watches ``/app/logs/*progress.csv`` and uploads the most recent row to DynamoDB.

BACKTESTING_PROGRESS_HEARTBEAT
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Enable periodic ``progress.csv`` updates while a ThetaData download is active (prevents the UI appearing stuck when simulation datetime is not advancing).
- Values: ``true`` / ``false`` (string).
- Default: enabled (``true``).

BACKTESTING_PROGRESS_HEARTBEAT_SECONDS
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Heartbeat interval (seconds) for writing ``progress.csv`` while downloading.
- Values: float seconds (string).
- Default: ``2.0``

Trade audit telemetry (accuracy investigations)
-----------------------------------------------

LUMIBOT_BACKTEST_AUDIT
^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Emit **per-fill audit telemetry** into the trade-event CSV as ``audit.*`` columns.
- Values: ``1`` enables (any truthy value); unset/``0`` disables.
- Output:
  - Writes a full trade-event export ``*_trade_events.csv`` with ``audit.*`` columns (for example, quote bid/ask snapshots, bar OHLC, SMART_LIMIT inputs, and multileg linkage).
- Notes:
  - This increases CSV width and can add overhead; keep it enabled only when you need a full audit trail.

Profiling (performance + parity investigations)
------------------------------------------------

BACKTESTING_PROFILE
^^^^^^^^^^^^^^^^^^^

- Purpose: Enable profiling during backtests to attribute runtime (S3 IO vs compute vs artifacts).
- Values:
  - ``yappi`` (supported)
- Output:
  - Produces a ``*_profile_yappi.csv`` artifact alongside other backtest artifacts.

LUMIBOT_CACHE_MISS_DEBUG
^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Opt-in diagnostic logging for the IBKR history cache path. Emits ``[CACHE_MISS]`` (why a cache miss fired) and ``[FETCH]`` (including a short Python traceback of the caller) at WARNING level.
- Values: truthy enables (``1``, ``true``); unset/``0`` disables.
- Default: disabled. Zero runtime cost when unset — the check is gated before any logging work.
- When to use: diagnosing unexpected IBKR roundtrips in warm-cache backtests (e.g., when profiling shows time in ``_fetch_history_between_dates`` that you expected to be cached).

ThetaData option-chain building (performance)
---------------------------------------------

THETADATA_CHAIN_DEFAULT_MAX_DAYS_OUT
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Bounds the default option-chain expiration window for equity underlyings to reduce strike-list fanout in cold caches/backtests.
- Values: integer days.
- Default: ``730`` (2 years).
- Notes: set to ``0`` to disable the default bound (fetch all expirations).

THETADATA_CHAIN_DEFAULT_MAX_DAYS_OUT_INDEX
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Same as ``THETADATA_CHAIN_DEFAULT_MAX_DAYS_OUT``, but for index-like underlyings (SPX/NDX/VIX/etc) with dense expiration schedules.
- Values: integer days.
- Default: ``180``.
- Notes: set to ``0`` to disable the default bound.

THETADATA_CHAIN_RECENT_FILE_TOLERANCE_DAYS
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Local chain cache file reuse window (equities) when no chain hints are in effect.
- Values: integer days.
- Default: ``7``.

THETADATA_CHAIN_STRIKES_TIMEOUT
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Downloader wait timeout per strike-list request when building chains.
- Values: seconds (float).
- Default: ``300``.

THETADATA_CHAIN_STRIKES_BATCH_SIZE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Number of in-flight strike-list requests when building chains.
- Values: integer.
- Default: ``0`` (use queue client concurrency).

ThetaData corporate action normalization (accuracy)
------------------------------------------------------------

THETADATA_APPLY_CORPORATE_ACTIONS_INTRADAY
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Apply split/dividend adjustments to **intraday** frames (minute/second/hour) in backtests so intraday prices match daily split-adjusted prices and option-chain strike normalization stays consistent.
- Values: ``1`` / ``true`` enable; ``0`` / ``false`` disable.
- Default:
  - enabled when ``IS_BACKTESTING`` is truthy
  - disabled otherwise
- Notes:
  - Disabling can break options strike selection around splits (example: NVDA 10-for-1 split on 2024-06-10).

Remote cache (S3)
-----------------

LUMIBOT_CACHE_BACKEND / LUMIBOT_CACHE_MODE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Enable remote cache mirroring (for example, mirroring parquet cache files to S3).
- Common values:
  - ``LUMIBOT_CACHE_BACKEND=s3``
  - ``LUMIBOT_CACHE_MODE=readwrite`` (or ``readonly``)

LUMIBOT_CACHE_FOLDER
^^^^^^^^^^^^^^^^^^^^

- Purpose: Override the local cache folder (useful to simulate a fresh container/task).

LUMIBOT_CACHE_S3_BUCKET / LUMIBOT_CACHE_S3_PREFIX / LUMIBOT_CACHE_S3_REGION
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: S3 target configuration.

LUMIBOT_CACHE_S3_VERSION
^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Namespace/version the remote cache without deleting anything.
- Practical use: set a unique version to simulate a “cold S3” run safely.

LUMIBOT_CACHE_S3_ACCESS_KEY_ID / LUMIBOT_CACHE_S3_SECRET_ACCESS_KEY / LUMIBOT_CACHE_S3_SESSION_TOKEN
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Credentials for S3 access when not using an instance/task role.
- Values: provided by your runtime environment (**do not hardcode**).

For cache key layout and validation workflow, see :doc:`Backtesting <backtesting>` and the engineering notes in ``docs/remote_cache.md``.

Strategy configuration
----------------------

STRATEGY_NAME
^^^^^^^^^^^^^

- Purpose: Name for the strategy to be used in database logging and identification.
- Values: Any string.

MARKET
^^^^^^

- Purpose: Market to be traded (used for market calendar selection).
- Values: ``NYSE``, ``NASDAQ``, ``24/7`` (crypto), etc.

HIDE_TRADES / HIDE_POSITIONS
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Hide trade and position information in logs/output.
- Values: ``true`` / ``false`` (string).
- Default: ``false``.

DISCORD_WEBHOOK_URL
^^^^^^^^^^^^^^^^^^^

- Purpose: Discord webhook URL for notifications.
- Values: Full Discord webhook URL (**do not hardcode in public repos**).

Database configuration
----------------------

DB_CONNECTION_STR
^^^^^^^^^^^^^^^^^

- Purpose: PostgreSQL connection string for account history and strategy persistence.
- Values: ``postgresql://user:password@host:port/database`` (**do not hardcode**).
- Note: Replaces deprecated ``ACCOUNT_HISTORY_DB_CONNECTION_STR``.

LOG_BACKTEST_PROGRESS_TO_FILE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Log backtest progress to a file instead of stdout.
- Values: ``true`` / ``false`` (string).

Broker selection
----------------

TRADING_BROKER
^^^^^^^^^^^^^^

- Purpose: Explicitly specify which broker to use for live trading.
- Values (case-insensitive):
  - ``alpaca``, ``tradier``, ``ccxt``, ``coinbase``, ``kraken``, ``weex``
  - ``ib``, ``interactivebrokers``, ``ibrest``, ``interactivebrokersrest``
  - ``tradovate``, ``schwab``, ``bitunix``, ``polymarket``, ``polymarket_clob``
  - ``projectx`` / ``projectx-topstepx`` for TopstepX futures (via ProjectX)
- Note: If not set, broker is auto-detected based on available credentials.

DATA_SOURCE
^^^^^^^^^^^

- Purpose: Explicitly specify which data source to use.
- Values (case-insensitive):
  - ``alpaca``, ``tradier``, ``polygon``, ``yahoo``, ``thetadata``, ``databento``
  - ``ccxt``, ``coinbase``, ``kraken``, ``weex``, ``schwab``, ``bitunix``, ``projectx``, ``polymarket``, ``polymarket_clob``
- Note: If not set, uses broker's default data source.

Alpaca broker
-------------

ALPACA_API_KEY / ALPACA_API_SECRET
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Alpaca API credentials for trading.
- Values: Obtain from Alpaca dashboard (**do not hardcode**).

ALPACA_OAUTH_TOKEN
^^^^^^^^^^^^^^^^^^

- Purpose: OAuth token (alternative to API key/secret).
- Values: OAuth token (**do not hardcode**).
- Note: Either OAuth token OR API key/secret must be provided, not both.

ALPACA_IS_PAPER
^^^^^^^^^^^^^^^

- Purpose: Toggle between paper and live trading.
- Values: ``true`` (paper) / ``false`` (live).
- Default: ``true`` (paper trading).

ALPACA_NEWS_API_KEY / ALPACA_NEWS_API_SECRET
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Optional bring-your-own-key credentials for ``BuiltinTools.news.alpaca_news()`` when the active broker is not Alpaca.
- Values: Alpaca API credentials with news/data access (**do not hardcode**).
- Note: When the active broker is Alpaca, the built-in news tool reuses that broker's OAuth token or API key/secret instead. If the active broker is not Alpaca and these news-specific variables are absent, the built-in news tool is not exposed to agents.

Polymarket CLOB broker
----------------------

POLYMARKET_PRIVATE_KEY
^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Wallet private key or session signer used to sign Polymarket CLOB orders and derive API credentials.
- Values: Provider/wallet secret (**do not hardcode**).
- Note: Required for authenticated CLOB reads and trading. Treat this as a high-risk wallet credential.

POLYMARKET_WALLET_ADDRESS
^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Polymarket proxy wallet, deposit wallet, Safe, or other funder address passed to the CLOB client.
- Values: Wallet address.

POLYMARKET_OWNER_ADDRESS
^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Optional owner/signer address for Magic/proxy accounts. LumiBot uses it as a fallback for Data API position
  and value reads when it differs from ``POLYMARKET_WALLET_ADDRESS``.
- Values: Wallet address.

POLYMARKET_SIGNATURE_TYPE
^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Polymarket CLOB signature type used by the authenticated client.
- Values: ``0`` for EOA, ``1`` for existing Polymarket proxy/Magic wallets, ``2`` for Gnosis Safe, ``3`` for deposit
  wallet / ``POLY_1271``.
- Default: ``1`` when ``POLYMARKET_OWNER_ADDRESS`` and ``POLYMARKET_WALLET_ADDRESS`` differ, otherwise ``3``. Set this
  explicitly for live testing and Bot Manager deployments.

POLYMARKET_CLOB_API_KEY / POLYMARKET_CLOB_API_SECRET / POLYMARKET_CLOB_API_PASSPHRASE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Optional derived/generated CLOB API credentials for authenticated order, trade, cancel, and balance requests.
- Values: CLOB API credentials (**do not hardcode**).
- Note: If omitted, LumiBot attempts to derive/create credentials in memory through ``py-clob-client-v2`` when a private key is available.

POLYMARKET_API_CREDENTIALS_JSON
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Optional JSON wrapper for CLOB API credentials.
- Values: JSON object containing key/secret/passphrase fields (**do not hardcode**).
- Note: Prefer separate env vars for readability unless a secret store provides one JSON payload.

POLYMARKET_RELAYER_API_KEY / POLYMARKET_RELAYER_API_KEY_ADDRESS
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Relayer credentials for wallet deployment/approval flows when required by the Polymarket account setup.
- Values: Relayer credential fields (**do not hardcode**).
- Note: Relayer credentials are not the same as CLOB trading credentials. The CLOB client still needs L2 API key,
  secret, and passphrase fields for private order/balance/trade endpoints and WebSocket user streams.

POLYMARKET_BUILDER_CODE
^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Optional Polymarket builder attribution code.
- Values: Builder code.

POLYMARKET_AUTO_APPROVE
^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: When truthy, attempt CLOB collateral approval setup before live trading.
- Values: truthy enables (``1``, ``true``, ``yes``, ``on``); unset disables.
- Default: disabled.

POLYMARKET_MAX_MARKET_ORDER_NOTIONAL
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Hard cap for Polymarket market BUY order dollar amount.
- Values: Positive decimal.
- Default: ``5``.

POLYMARKET_TEST_TOKEN_ID / POLYMARKET_LIVE_TRADING_ENABLED / POLYMARKET_TEST_MAX_NOTIONAL
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Test-only gates for Polymarket live smoke tests.
- Values: Explicit CLOB token id; ``POLYMARKET_LIVE_TRADING_ENABLED=true`` to allow live submit/cancel tests; decimal max notional.
- Note: Live tests are skipped unless the required env vars are present. Default live-test max notional is capped at ``5``.
- Smoke helpers: ``scripts/polymarket_smoke.py`` and ``scripts/polymarket_lumibot_smoke.py`` load local ``.env.local``
  values, redact output, and only submit live orders when the live gate and caps are present. The LumiBot smoke helper
  supports explicit order-matrix cases such as ``fak-buy``, ``fok-buy``, ``fak-sell``, ``fok-sell``, ``gtc-buy``,
  ``gtd-buy``, ``post-only-buy``, ``cancel-single``, ``cancel-multiple``, ``cancel-all``, ``cancel-market``, and
  ``--websocket``.

Polymarket CLOB implementation notes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Set ``TRADING_BROKER=polymarket`` for the normal live broker path. ``DATA_SOURCE=polymarket`` is only the optional
  separate public-market-data override.
- CLOB collateral balances are returned in 6-decimal raw units by balance/allowance reads. LumiBot scales those values
  into dollars before returning account cash.
- The current international CLOB adapter supports read-only account state, market discovery, token resolution, order
  books, quotes, last price, supported price history, public market WebSockets, and authenticated user WebSocket
  subscription. Live submit/cancel is implemented and has been proven through the LumiBot broker with a funded deposit
  wallet, pUSD approvals, and conditional-token sell approvals. Existing Magic/proxy accounts may still be rejected by
  Polymarket with a deposit-wallet/API-key binding error until migrated through the supported deposit-wallet flow.
- SELL orders need conditional-token approvals from the deposit wallet. The local setup helper can submit these approvals
  with ``scripts/polymarket_deposit_wallet_setup.py --approve-conditional``.

Tradier broker
--------------

TRADIER_ACCESS_TOKEN
^^^^^^^^^^^^^^^^^^^^

- Purpose: Tradier API access token.
- Values: Obtain from Tradier dashboard (**do not hardcode**).

TRADIER_TOKEN_PATH
^^^^^^^^^^^^^^^^^^

- Purpose: Path to a Tradier OAuth token JSON file.
- Values: A local file containing provider token fields such as ``access_token``,
  ``refresh_token``, ``expires_in``, and ``issued_at``.
- Note: When LumiBot refreshes an OAuth token, it writes the updated provider
  token payload back to this file atomically. If the file cannot be written,
  refresh fails instead of silently continuing with only in-memory token state.

LUMIBOT_OAUTH_REFRESH_MODE
^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Controls who calls the broker OAuth refresh endpoint for Schwab and
  Tradier token-file integrations.
- Values: ``auto`` (default) lets LumiBot refresh the provider OAuth token and write the updated token payload back to
  the configured token file. ``external`` means LumiBot does not call the provider OAuth refresh endpoint; a trusted
  parent process must atomically replace the configured token file, and LumiBot reloads it before broker requests and
  after auth failures.
- Note: Use ``external`` only when another trusted process owns refresh-token
  handling. The token file still needs a valid access token for broker API calls.

TRADIER_ACCOUNT_NUMBER
^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Tradier account number for trading.
- Values: Your Tradier account number.

TRADIER_IS_PAPER
^^^^^^^^^^^^^^^^

- Purpose: Toggle between paper and live trading.
- Values: ``true`` (paper) / ``false`` (live).
- Default: ``true`` (paper trading).

Interactive Brokers
-------------------

INTERACTIVE_BROKERS_PORT
^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Socket port for IB Gateway/TWS connection.
- Values: Integer (e.g., ``7497`` for paper, ``7496`` for live).

INTERACTIVE_BROKERS_CLIENT_ID
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Client ID for IB connection (must be unique per connection).
- Values: Integer.

INTERACTIVE_BROKERS_IP
^^^^^^^^^^^^^^^^^^^^^^

- Purpose: IP address of IB Gateway/TWS.
- Values: IP address string.
- Default: ``127.0.0.1``.

IB_SUBACCOUNT
^^^^^^^^^^^^^

- Purpose: Sub-account identifier for IB multi-account setups.
- Values: Account identifier string.

Interactive Brokers REST
------------------------

IB_USERNAME / IB_PASSWORD
^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Credentials for IB REST API authentication.
- Values: IB credentials (**do not hardcode**).

IB_ACCOUNT_ID
^^^^^^^^^^^^^

- Purpose: Account ID for IB REST API.
- Values: Account identifier string.

IB_API_URL
^^^^^^^^^^

- Purpose: Base URL for IB REST API endpoint.
- Values: URL string.

IBKR_HISTORY_SOURCE
^^^^^^^^^^^^^^^^^^^

- Purpose: Select which IBKR Client Portal history source to use for OHLC bars in IBKR REST backtests.
- Values: ``Trades`` / ``Midpoint`` / ``Bid_Ask`` (case-insensitive; hyphen/underscore variants accepted).
- Default: ``Trades``.

IBKR_FUTURES_EXCHANGE
^^^^^^^^^^^^^^^^^^^^^

- Purpose: Fallback futures exchange for IBKR REST when ``exchange=`` is not provided and automatic exchange routing cannot resolve a unique venue.
- Values: Exchange code string (for example: ``CME``, ``CBOT``, ``COMEX``, ``NYMEX``).
- Default: ``CME``.

IBKR_CRYPTO_VENUE
^^^^^^^^^^^^^^^^^

- Purpose: Default IBKR crypto venue when backtesting spot crypto via IBKR REST.
- Values: Venue/exchange string (for example: ``ZEROHASH``).
- Default: ``ZEROHASH``.

LUMIBOT_IBKR_ENABLE_FUTURES_BID_ASK
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Opt-in derivation of per-bar futures bid/ask quotes using IBKR ``Bid_Ask`` + ``Midpoint`` history sources.
- Values: ``true``/``false`` (or ``1``/``0``).
- Default: disabled.

Schwab broker
-------------

SCHWAB_ACCOUNT_NUMBER
^^^^^^^^^^^^^^^^^^^^^

- Purpose: Schwab account number (required).
- Values: Account number string.

SCHWAB_APP_KEY / SCHWAB_APP_SECRET
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Schwab API application credentials.
- Values: Obtain from Schwab developer portal (**do not hardcode**).

SCHWAB_TOKEN
^^^^^^^^^^^^

- Purpose: Optional pre-existing OAuth token.
- Values: Token string (**do not hardcode**).

SCHWAB_BACKEND_CALLBACK_URL
^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: OAuth callback URL for authentication flow.
- Values: URL string.

Tradovate broker
----------------

TRADOVATE_USERNAME / TRADOVATE_DEDICATED_PASSWORD
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Tradovate credentials.
- Values: Tradovate credentials (**do not hardcode**).

TRADOVATE_APP_ID / TRADOVATE_APP_VERSION
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Application identification for Tradovate API.
- Values: String identifiers.
- Default: ``Lumibot`` / ``1.0``.

TRADOVATE_CID / TRADOVATE_SECRET
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Client credentials for Tradovate OAuth.
- Values: Obtain from Tradovate (**do not hardcode**).

TRADOVATE_IS_PAPER
^^^^^^^^^^^^^^^^^^

- Purpose: Toggle between paper and live trading.
- Values: ``true`` (paper) / ``false`` (live).
- Default: ``true``.

TRADOVATE_MD_URL
^^^^^^^^^^^^^^^^

- Purpose: Market data URL override.
- Values: URL string.
- Default: ``https://md.tradovateapi.com/v1``.

Crypto brokers (CCXT)
---------------------

KRAKEN_API_KEY / KRAKEN_API_SECRET
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Kraken exchange API credentials.
- Values: Obtain from Kraken (**do not hardcode**).

COINBASE_API_KEY_NAME / COINBASE_PRIVATE_KEY
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Coinbase Advanced Trade API credentials.
- Values: Obtain from Coinbase (**do not hardcode**).

COINBASE_API_PASSPHRASE
^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: API passphrase (if required by Coinbase).
- Values: Passphrase string (**do not hardcode**).

COINBASE_SANDBOX
^^^^^^^^^^^^^^^^

- Purpose: Use Coinbase sandbox environment.
- Values: ``true`` / ``false``.
- Default: ``false``.

Bitunix broker
--------------

BITUNIX_API_KEY / BITUNIX_API_SECRET
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Bitunix exchange API credentials.
- Values: Obtain from Bitunix (**do not hardcode**).

BITUNIX_TRADING_MODE
^^^^^^^^^^^^^^^^^^^^

- Purpose: Trading mode selection.
- Values: ``FUTURES`` / ``SPOT``.
- Default: ``FUTURES``.

ProjectX brokers
----------------

ProjectX support is primarily documented for TopstepX futures. The lower-level adapter can read firm-specific ProjectX environment variable prefixes, but new firms should be tested before being treated as production-ready.

PROJECTX_FIRM
^^^^^^^^^^^^^

- Purpose: Select which ProjectX firm to use.
- Values: ``TOPSTEPX`` for the documented TopstepX path. Additional ProjectX firm ids may exist in the adapter, but require validation before use.

PROJECTX_{FIRM}_API_KEY
^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: API key for the specified firm.
- Example: ``PROJECTX_TOPSTEPX_API_KEY``, ``PROJECTX_TOPONE_API_KEY``
- Values: Obtain from firm's platform (**do not hardcode**).

PROJECTX_{FIRM}_USERNAME
^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Username for the specified firm.
- Example: ``PROJECTX_TOPSTEPX_USERNAME``
- Values: Your username on the firm's platform.

PROJECTX_{FIRM}_PREFERRED_ACCOUNT_NAME
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Preferred account name when multiple accounts exist.
- Example: ``PROJECTX_TOPSTEPX_PREFERRED_ACCOUNT_NAME``
- Values: Account name string.

Data source credentials
-----------------------

POLYGON_API_KEY
^^^^^^^^^^^^^^^

- Purpose: Polygon.io API key for market data.
- Values: Obtain from Polygon.io (**do not hardcode**).

POLYGON_MAX_MEMORY_BYTES
^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Hard limit on memory Polygon can use for caching.
- Values: Integer (bytes).

THETADATA_USERNAME / THETADATA_PASSWORD
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: ThetaData API credentials.
- Values: Obtain from ThetaData (**do not hardcode**).
- Note: Required for ThetaData backtesting and live data.

THETADATA_BASE_URL
^^^^^^^^^^^^^^^^^^

- Purpose: Base URL for the local ThetaTerminal REST API.
- Default: ``http://127.0.0.1:25503``
- Values: URL string.
- Note: You typically do not need to set this; LumiBot auto-manages a local ThetaTerminal for ThetaData usage.

DATABENTO_API_KEY
^^^^^^^^^^^^^^^^^

- Purpose: DataBento API key for market data.
- Values: Obtain from DataBento (**do not hardcode**).

DATABENTO_TIMEOUT
^^^^^^^^^^^^^^^^^

- Purpose: Request timeout for DataBento API calls.
- Values: Integer (seconds).
- Default: ``30``.

DATABENTO_MAX_RETRIES
^^^^^^^^^^^^^^^^^^^^^

- Purpose: Maximum retry attempts for failed DataBento requests.
- Values: Integer.
- Default: ``3``.

LUMIWEALTH_API_KEY
^^^^^^^^^^^^^^^^^^

- Purpose: LumiWealth platform API key (for enterprise features).
- Values: Obtain from LumiWealth (**do not hardcode**).

Runtime telemetry (memory/health)
---------------------------------

LUMIBOT_TELEMETRY
^^^^^^^^^^^^^^^^^

- Purpose: Enable/disable runtime telemetry emission (single-line JSON to stdout prefixed with ``LUMIBOT_TELEMETRY``).
- Values: truthy enables (``1``, ``true``, ``yes``); falsy disables (``0``, ``false``).
- Default: enabled for live runs; disabled for backtests and pytest.

LUMIBOT_TELEMETRY_INTERVAL_SECONDS
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Base telemetry cadence.
- Values: seconds (float).
- Default: ``300``.

LUMIBOT_TELEMETRY_DEEP
^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Enable deep snapshot mode for diagnosing unknown memory sources.
- Values: truthy enables; falsy disables.
- Default: disabled.

Notes:

- Burst mode (more frequent telemetry logs) turns on automatically above ~80% of container memory.
- Deep snapshots trigger above ~90% with a ~1 hour cooldown (these thresholds are fixed defaults today).

AI agent model providers
------------------------

LumiBot's AI agent subsystem (``self.agents.create(model=...)`` or ``default_model=...``) supports multiple LLM providers. You only need the key matching the provider id you pass for each agent. Non-Gemini ids are routed through LiteLLM, which ships as a LumiBot dependency.

GEMINI_API_KEY
^^^^^^^^^^^^^^

- Purpose: Auth for Gemini models (the default provider).
- Values: Obtain from https://aistudio.google.com/apikey.
- Required when ``default_model`` starts with ``gemini-`` (e.g. ``gemini-3.1-flash-lite-preview``).

OPENAI_API_KEY
^^^^^^^^^^^^^^

- Purpose: Auth for OpenAI models (GPT-5.4 family and others).
- Values: Obtain from https://platform.openai.com/api-keys.
- Required when ``default_model`` looks like ``openai/gpt-5.4-mini`` or any other ``openai/...`` id.

XAI_API_KEY or GROK_API_KEY
^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Auth for xAI Grok models.
- Values: Obtain from https://console.x.ai/.
- Required when ``default_model`` looks like ``xai/grok-4.20-0309-reasoning`` or any other ``xai/...`` id.
- ``XAI_API_KEY`` is the canonical provider env var; ``GROK_API_KEY`` is also accepted for user-facing Grok naming.

ANTHROPIC_API_KEY
^^^^^^^^^^^^^^^^^

- Purpose: Auth for Anthropic Claude models.
- Values: Obtain from https://console.anthropic.com/.
- Required when ``default_model`` looks like ``anthropic/claude-opus-4-7`` or any other ``anthropic/...`` id.

Other providers (Groq, Mistral, Cohere, Fireworks, Together, etc.) use the provider-prefixed id format and the corresponding provider env var; see the LiteLLM documentation for the full list.

SEC fundamentals, market sentiment, and agent memory
----------------------------------------------------

LUMIBOT_SEC_USER_AGENT
^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Contact-style SEC EDGAR user agent header.
- Values: Human-readable app/contact string.
- Default: LumiBot support contact.

LUMIBOT_SEC_CACHE_DIR
^^^^^^^^^^^^^^^^^^^^^

- Purpose: Override local SEC fundamentals and filing cache.
- Default: ``~/.lumibot/cache/sec``.

FRED_API_KEY
^^^^^^^^^^^^

- Purpose: Required official FRED/ALFRED API key for macro data tools.
- Default: unset.
- Notes: When set, LumiBot requests vintage observations with
  ``realtime_start`` and ``realtime_end`` for point-in-time backtests.
  Built-in FRED agent tools are not exposed during backtests without this key
  because LumiBot does not use revised public CSV fallbacks for macro data.

LUMIBOT_FRED_CACHE_DIR
^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Override local FRED macro data cache.
- Default: ``~/.lumibot/cache/fred``.

ADANOS_API_KEY
^^^^^^^^^^^^^^

- Purpose: Optional Adanos Market Sentiment API key for the
  ``adanos_market_sentiment`` built-in agent tool and
  ``strategy.sentiment`` helper.
- Default: unset.
- Notes: When set, LumiBot can fetch US-equity sentiment from Reddit,
  X / FinTwit, news, and Polymarket via the Adanos API.

ADANOS_API_BASE_URL
^^^^^^^^^^^^^^^^^^^

- Purpose: Override the Adanos API base URL.
- Default: ``https://api.adanos.org``.

LUMIBOT_MEMORY_DIR
^^^^^^^^^^^^^^^^^^

- Purpose: Override local SQLite agent memory root.
- Default: ``.lumibot/memory`` under the current working directory.

LUMIBOT_MEMORY_EXPORT_PARQUET
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Enable or disable best-effort Parquet exports for agent memory
  events, retrievals, and current state.
- Default: ``1``.
- Notes: SQLite is still the live memory store. Parquet exports are derived
  artifacts for DuckDB queries, BotSpot uploads, and post-run inspection.

LUMIBOT_AGENT_MEMORY_NOTE_MAX_CHARS
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Limit how much prior agent-run memory is injected back into the next
  agent prompt.
- Default: ``2000``.
- Notes: Full run traces and artifacts are still written separately. This only
  compacts the lightweight runtime notes so repeated backtest iterations do not
  blow up the model context window.

LUMIBOT_AGENT_MAX_MODEL_CALLS
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Hard cap uncached agent model calls in a single strategy run.
- Default: unset.
- Notes: When set, LumiBot raises before making the next provider call once the
  cap is reached. Use this for expensive AI backtests and smoke runs where
  accidental spend matters.

LUMIBOT_AGENT_MAX_RUN_ATTEMPTS
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Purpose: Override the retry budget for a single agent model call.
- Default: ``2`` in backtests, ``10`` in live trading.
- Notes: Backtests default to a lower retry budget so a bad provider window does
  not multiply model spend across many simulated iterations.

Telegram notifications
----------------------

TELEGRAM_BOT_TOKEN
^^^^^^^^^^^^^^^^^^

- Purpose: Telegram Bot API token for ``self.notifications.configure_telegram()``.
- Values: Bot token from BotFather.

TELEGRAM_CHAT_ID
^^^^^^^^^^^^^^^^

- Purpose: Telegram chat/channel/user id for strategy notifications.
- Values: Telegram chat id.
