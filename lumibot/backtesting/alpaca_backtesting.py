import json
import math
import os
import time
from collections import OrderedDict, deque
from datetime import date, datetime, time as dt_time, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Optional

import numpy as np
import pandas as pd
import pytz
from alpaca.data.historical import (
    CryptoHistoricalDataClient,
    OptionHistoricalDataClient,
    StockHistoricalDataClient,
)
from alpaca.data.requests import CryptoBarsRequest, OptionBarsRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

from lumibot.constants import LUMIBOT_CACHE_FOLDER
from lumibot.data_sources import AlpacaData, DataSourceBacktesting
from lumibot.entities import Asset, Bars, Chains
from lumibot.tools.helpers import (
    date_n_trading_days_from_date,
    get_decimals,
    get_timezone_from_datetime,
    get_trading_days,
    get_trading_times,
    quantize_to_num_decimals,
)
from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)

from lumibot.tools.alpaca_helpers import sanitize_base_and_quote_asset
from lumibot.tools.alpaca_history import fetch_alpaca_bars

# PERF: daily marks and daily history reads find their bar by the calendar date of each label.
# ``index.date`` builds one Python date object per label on every call (about 0.2 ms for ten
# years of bars, asked for every held symbol several times per simulated day). An index never
# changes, so its dates are computed once per index object as numpy days: the labels'
# wall-clock dates in their own timezone, exactly what ``index.date`` returns.
_SESSION_DAYS_CACHE: "OrderedDict[int, tuple]" = OrderedDict()
_SESSION_DAYS_CACHE_MAX = 512


def _index_session_days(index: pd.DatetimeIndex) -> np.ndarray:
    key = id(index)
    hit = _SESSION_DAYS_CACHE.get(key)
    if hit is not None and hit[0] is index:
        return hit[1]
    wall = index.tz_localize(None) if getattr(index, "tz", None) is not None else index
    days = np.asarray(wall.values).astype("datetime64[D]")
    # The entry holds the index, so its id cannot be reused while the entry exists.
    _SESSION_DAYS_CACHE[key] = (index, days)
    if len(_SESSION_DAYS_CACHE) > _SESSION_DAYS_CACHE_MAX:
        _SESSION_DAYS_CACHE.popitem(last=False)
    return days


class AlpacaOptionHistoryUnavailable(RuntimeError):
    """Alpaca answered an option bars request successfully but returned no bars."""


class AlpacaOptionsDataPlanError(RuntimeError):
    """Alpaca refused option bars with HTTP 403: the account's data plan has no options history.

    Raised unchanged through every option price path so a backtest stops with this one
    message instead of a generic fetch failure repeated for each contract.
    """


def _alpaca_asset_class(fn) -> str | None:
    """Asset class of an Alpaca SDK call, from its method name (run telemetry only)."""
    name = str(getattr(fn, "__name__", "") or "").lower()
    for word, asset_class in (("option", "option"), ("crypto", "crypto"), ("stock", "stock"), ("news", "news")):
        if word in name:
            return asset_class
    return None


class AlpacaBacktesting(DataSourceBacktesting):
    """Backtest with historical data from your own Alpaca account (stocks, crypto, options).

    Options ("bring your own key"):

    - ``get_chains()`` lists contracts with Alpaca's option contracts API
      (``GET /v2/options/contracts``), expired (``status=inactive``) and live
      (``status=active``) together, following ``next_page_token`` pagination. The chain
      holds expirations from the simulated date through ``OPTION_CHAIN_MAX_DAYS`` (90)
      days later, or the ``min/max_expiration_date`` hint that ``OptionsHelper`` sets.
      Only standard 100-share contracts whose root is the underlying are included.
    - Contracts are cached per (underlying, expiration month) in memory and as JSON under
      ``<LUMIBOT_CACHE_FOLDER>/alpaca/option_chains/by_expiration_month/``; each simulated
      day's chain is built from those months and kept in memory. A month is listed from
      Alpaca once (one listing covers every missing month in a row plus one ahead), and
      once all of its expirations have passed it is shared through the remote cache with
      every other backtest. A 2-year run makes a few dozen listing calls, not one per day.
    - Option prices come from Alpaca historical option bars, which are trade prints.
      They are kept as-is: no reindexing, no forward fill, no back fill (RULE #1 in
      ``docs/BACKTESTING_ARCHITECTURE.md``). ``get_last_price()`` returns the open of a
      bar that printed in the current minute, otherwise the close of the most
      recent earlier print, and ``None`` before the first print. Daily marks switch
      from open to close at the actual exchange session close. ``BacktestingBroker``
      fills an option order only on a bar that printed in the current bucket, so orders
      wait for a real trade instead of filling at a stale price.
    - A contract with no bars at all logs one clear error and returns ``None``.

    Known limits:

    - Alpaca option history starts around February 2024. Earlier contracts return no bars.
    - The contract listing is not point-in-time: Alpaca's contracts API has no listing
      date, so a strike or expiration listed after the simulated date can appear in that
      date's chain (a small lookahead). Chain membership is not proof the contract existed
      then. Contracts with no trade before the simulated date have no price and cannot fill.
    - No historical bid/ask or greeks for options. Fills use trade bars, so spreads are
      not modeled. ``Strategy.get_greeks()`` still works: LumiBot computes greeks locally
      from the last trade and the underlying price, which is only as fresh as the prints.
    - Daily option bars start at the day's first trade, which can print after the open.
      Use minute bars when fill timing matters.
    - Free keys allow about 200 requests per minute. Requests are throttled below that
      and HTTP 429 answers wait (``Retry-After`` when present) with a bounded retry.
    - Option history needs an Alpaca data plan that includes options. When Alpaca answers
      an option bars request with HTTP 403, the backtest stops with
      ``AlpacaOptionsDataPlanError`` ("Your Alpaca data plan does not include options
      history") and Alpaca's own reason; nothing is retried or substituted.
    - Stock bars use Alpaca's default feed. Checked 2026-09-23 on a free key: history comes
      back as SIP (all US exchanges, SPY about 60M shares a day versus about 2M on IEX), but
      the latest 15 minutes of SIP are refused ("subscription does not permit querying
      recent SIP data"). Requests run to the end date plus one day, so end a backtest at
      least one full day before today.
    """

    SOURCE = "ALPACA"
    APPLY_BACKTEST_POSITION_SPLITS = False
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {"timestep": "day", "representations": [TimeFrame.Day]},
        {"timestep": "minute", "representations": [TimeFrame.Minute]},
    ]
    # AlpacaData builds its default quote asset lazily and leaves the class attribute None
    # until first use (2026-07-02). Copying that None made `_get_asset_key(quote_asset=None)`
    # crash in the legacy backtest tests; resolve the real USD quote asset here instead.
    LUMIBOT_DEFAULT_QUOTE_ASSET = AlpacaData._default_quote_asset()
    # Option chain horizon (days after the simulated date) when no expiration hint is set.
    OPTION_CHAIN_MAX_DAYS = 90
    # Extra expiration months fetched per contract listing so later simulated days reuse it.
    OPTION_CHAIN_LISTING_EXTRA_MONTHS = 1
    OPTION_CONTRACTS_PAGE_LIMIT = 10000
    # Alpaca free-tier keys allow about 200 data/trading requests per minute.
    ALPACA_MAX_REQUESTS_PER_MINUTE = 180
    ALPACA_RATE_LIMIT_MAX_RETRIES = 5
    ALPACA_RATE_LIMIT_MAX_WAIT_SECONDS = 120.0
    OPTION_HISTORY_START_HINT = "Alpaca option history starts around February 2024"
    CACHE_SUBFOLDER = "alpaca"
    # History before backtesting_start (history_before_start, on by default in environment
    # mode): one reach back covers at most this many trading sessions.
    HISTORY_BEFORE_START_MAX_INTRADAY_SESSIONS = 260  # about one year of intraday bars
    HISTORY_BEFORE_START_MAX_DAILY_SESSIONS = 2520  # about ten years of daily bars
    REGULAR_SESSION_MINUTES = 390

    def __init__(
            self,
            datetime_start: datetime | None = None,
            datetime_end: datetime | None = None,
            backtesting_started: datetime | None = None,
            config: dict | None = None,
            api_key: str | None = None,
            show_progress_bar: bool = True,
            delay: int | None = None,
            pandas_data: dict | list = None,
            **kwargs
    ):
        """
        Initializes a class instance for handling backtesting data and parameters. This initialization 
        process involves setting up key configurations, verifying account types, and preparing backtesting 
        timings, timezones, and historical data clients. Data caching and warm-up trading days are also 
        appropriately configured.

        Args:
            datetime_start (tz aware datetime): The starting datetime for the backtesting process. Inclusive.
            datetime_end (tz aware datetime): The ending datetime for the backtesting process. Inclusive.
            backtesting_started (datetime | None): Represents the datetime when backtesting started. Defaults to None.
            config (dict | None): API_KEY/API_SECRET or OAUTH_TOKEN, PAPER and optional MARKET. When None
                ("environment mode", how BotSpot selects this source through BACKTESTING_DATA_SOURCE=alpaca),
                credentials are read from ALPACA_API_KEY, ALPACA_API_SECRET, ALPACA_OAUTH_TOKEN and
                ALPACA_IS_PAPER, bars default to minute, and the backtest runs through backtesting_end.
            api_key (str | None): API key for authorized data access. Optional as it can typically be found 
                within the provided config.
            show_progress_bar (bool): Indicates whether to show a progress bar during data operations. 
                Defaults to True.
            delay (int | None): Delay in seconds added between operations to simulate real-world activity. 
                Defaults to None.
            pandas_data (dict | list): Data to be loaded directly into pandas, allowing analysis or backtesting 
                without requiring external API calls.
            **kwargs: Additional keyword arguments, such as:
                - timestep (str): Interval for data ("day" or "minute"). Defaults to "day" with an explicit
                  config and to "minute" in environment mode. A default (not explicit) timestep is switched to
                  "day" for strategies that sleep a day or more.
                - full_window (bool): Run through backtesting_end. Defaults to True in environment mode and to
                  False (the legacy stop at the open of the third-to-last trading day) with an explicit config.
                - history_before_start (bool): When a history request needs more finished bars than the data
                  window holds (for example 250 five-minute bars or 15 daily bars at the first bar), fetch the
                  real bars before it, the way IBKR and ThetaData backtests do. Each reach back is bounded by the
                  request, cached like the window, and never filled; a request that still cannot be met returns
                  the bars that exist instead of raising. Defaults to True in environment mode and to False with
                  an explicit config (the data then starts at backtesting_start, or earlier with
                  warm_up_trading_days).
                - option_chain_max_days (int): get_chains() horizon when no expiration hint is set. Default 90.
                - refresh_cache (bool): Whether to force cache refresh. Defaults to False.
                - intraday_bars_from_minutes (bool): Build 5, 10, 15 and 30-minute stock bars from the
                  1-minute series already in the cache instead of downloading them, when every
                  requested bar is inside today's regular session (identical bars). Defaults to True;
                  the environment variable BACKTESTING_INTRADAY_BARS_FROM_MINUTES=false turns it off.
                - warm_up_trading_days (int): The number of trading days used for warm-up before processing 
                  the primary dataset. Defaults to 0.
                - market (str): Indicates the stock exchange or market (e.g., "NYSE"). Defaults to "NYSE".
                - auto_adjust (bool): Determines whether to auto-adjust data, such as stock splits. Defaults 
                  to True.
                - remove_incomplete_current_bar (bool): Return only bars that have closed by the simulated
                  time. Defaults to True in both environment and explicit-config mode, the contract IBKR,
                  ThetaData and Polygon backtests follow. Pass False only to opt in to the old behavior:
                  history then includes the bar that is still forming at the simulated time (for example
                  today's daily bar at 09:30, or the 10:00 five-minute bar at 10:02) with its final close,
                  high, low and volume. In a backtest that is a lookahead of up to one bar. get_last_price
                  and order fills use the open of the current bar either way.

        Raises:
            ValueError: If the credentials are missing or the config is not a paper account.

        """
        self._datetime = None

        # Call the base class. Forward the progress-file settings run_backtest passes so hosted
        # runs (BotSpot) get the same progress.csv as every other backtesting source.
        super().__init__(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            backtesting_started=backtesting_started,
            show_progress_bar=show_progress_bar,
            delay=delay,
            pandas_data=None,
            log_backtest_progress_to_file=kwargs.get("log_backtest_progress_to_file", False),
            progress_csv_path=kwargs.get("progress_csv_path"),
        )

        # Environment mode: no config was passed, so credentials come from the process
        # environment. This is how BotSpot runs Alpaca backtests: the strategy calls
        # backtest(datasource_class=None), BotSpot Node sets BACKTESTING_DATA_SOURCE=alpaca and
        # ALPACA_IS_PAPER=true, and the customer's ALPACA_API_KEY/ALPACA_API_SECRET or
        # ALPACA_OAUTH_TOKEN are injected. Before 2026-09-23 config=None raised, so no existing
        # caller depends on the defaults below.
        # A routing map (BACKTESTING_DATA_SOURCE={"default": "alpaca", "index": "ibkr"}) arrives
        # inside the config. It is not credentials: take it out, so a config that held nothing
        # else is still environment mode.
        routing = None
        if isinstance(config, dict) and self._ROUTING_CONFIG_KEY in config:
            config = dict(config)
            routing = config.pop(self._ROUTING_CONFIG_KEY)
            if not config:
                config = None
        self._other_source_routes = self._routes_to_other_sources(routing)
        self._routed_companion = None
        self._routed_companion_window = (datetime_start, datetime_end)

        environment_mode = config is None
        if environment_mode:
            config = self._config_from_environment()

        self.market = (
                kwargs.get("market", None)
                or (config.get("MARKET") if config else None)
                or os.environ.get("MARKET")
                or "NASDAQ"
        )

        explicit_timestep = kwargs.get('timestep')
        # Environment mode defaults to minute bars so intraday strategies (5-minute opening range
        # breakouts, for example) get minute prices and minute fills. A daily-cadence strategy is
        # still switched to day bars by StrategyExecutor because the default is not explicit.
        # With an explicit config the historical default stays "day".
        self._timestep: str = explicit_timestep or ('minute' if environment_mode else 'day')
        # The caller chose the bar size. StrategyExecutor's daily-cadence priming (4.4.53)
        # must not silently switch an explicit timestep="minute" to day bars.
        self._timestep_explicit = explicit_timestep is not None
        warm_up_trading_days: int = kwargs.get('warm_up_trading_days', 0)

        self._auto_adjust: bool = kwargs.get('auto_adjust', True)
        self.CACHE_SUBFOLDER = 'alpaca'
        self._data_store: dict[str, pd.DataFrame] = {}
        self._refreshed_keys = {}
        self._refresh_cache: bool = kwargs.get('refresh_cache', False)
        # 5, 10, 15 and 30-minute stock bars are built from the cached 1-minute series when the
        # answer is identical to the provider's (see _bars_from_cached_minutes). On by default;
        # False, or BACKTESTING_INTRADAY_BARS_FROM_MINUTES=false, always downloads the provider series.
        self._intraday_bars_from_minutes = kwargs.get('intraday_bars_from_minutes')
        # History never shows the bar that is still forming: a bar is returned only after it
        # has closed, like IBKR, ThetaData and Polygon backtests, in both environment and
        # explicit-config mode. Its final OHLCV would be a lookahead. An explicit False opts in
        # to the old behavior. get_last_price and order fills always use the open of the bar
        # that starts now, whatever this is set to.
        requested_remove_incomplete = kwargs.get('remove_incomplete_current_bar')
        self._remove_incomplete_current_bar = (
            True if requested_remove_incomplete is None else bool(requested_remove_incomplete)
        )
        # History before backtesting_start: a strategy that asks at its first bars for N bars
        # gets real earlier bars in environment mode, like IBKR and ThetaData. An explicit config
        # keeps its window unless asked (the documented default before 2026-09-23).
        requested_history_before_start = kwargs.get('history_before_start')
        self._history_before_start = (
            environment_mode if requested_history_before_start is None else bool(requested_history_before_start)
        )

        if not config.get("PAPER", True):
            raise ValueError("Backtesting is restricted to paper accounts. Pass in a paper account config.")

        # Initialize clients based on available authentication method
        oauth_token = config.get("OAUTH_TOKEN")
        api_key = config.get("API_KEY")
        api_secret = config.get("API_SECRET")
        
        if api_key and api_secret:
            self._crypto_client = CryptoHistoricalDataClient(
                api_key=api_key,
                secret_key=api_secret
            )
            self._stock_client = StockHistoricalDataClient(
                api_key=api_key,
                secret_key=api_secret
            )
            self._option_client = OptionHistoricalDataClient(
                api_key=api_key,
                secret_key=api_secret,
            )
        elif oauth_token:
            self._crypto_client = CryptoHistoricalDataClient(oauth_token=oauth_token)
            self._stock_client = StockHistoricalDataClient(oauth_token=oauth_token)
            self._option_client = OptionHistoricalDataClient(oauth_token=oauth_token)
        else:
            raise ValueError("Either OAuth token or API key/secret must be provided for Alpaca authentication")

        # Option chains come from the Trading API. Create that client lazily with the same
        # credentials and the same precedence as the live Alpaca broker: key/secret first,
        # then OAuth token. Backtests are always paper (checked above).
        self._alpaca_credentials = {
            "api_key": api_key,
            "api_secret": api_secret,
            "oauth_token": oauth_token,
            "paper": bool(config.get("PAPER", True)),
        }
        self._trading_client = None
        self._option_chain_max_days = int(kwargs.get("option_chain_max_days") or self.OPTION_CHAIN_MAX_DAYS)
        if oauth_token and not (api_key and api_secret):
            # Bars are cached once for every Alpaca connection. Bars cached before that under
            # this OAuth account are still found through the account identity (read-only).
            for data_client in (self._crypto_client, self._stock_client, self._option_client):
                data_client._lumibot_cache_identity = self._oauth_account_cache_identity

        # Create an AlpacaData instance for internal use
        self._alpaca_data = AlpacaData(config)

        # Ensure datetime_start and datetime_end have the same tzinfo
        if str(datetime_start.tzinfo) != str(datetime_end.tzinfo):
            raise ValueError("datetime_start and datetime_end must have the same tzinfo.")

        # Get timezone from datetime_start if it has one, otherwise use Lumibot default
        self.tzinfo = get_timezone_from_datetime(datetime_start)

        # We want self._data_datetime_start and self._data_datetime_end to be the start and end dates
        # of the data for the entire backtest including the warmup dates.

        # The start should be midnight.
        start_dt = datetime(
            year=datetime_start.year,
            month=datetime_start.month,
            day=datetime_start.day,
        )
        start_dt = self.tzinfo.localize(start_dt)  # Use localize instead of tzinfo in constructor

        # The end should be the last minute of the day.
        end_dt = datetime(
            year=datetime_end.year,
            month=datetime_end.month,
            day=datetime_end.day,
            hour=23,
            minute=59,
            second=59,
        )
        end_dt = self.tzinfo.localize(end_dt)  # Use localize instead of tzinfo in constructor

        if warm_up_trading_days > 0:
            warm_up_start_dt = date_n_trading_days_from_date(
                n_days=warm_up_trading_days,
                start_datetime=start_dt,
                market=self.market,
            )
            # Combine with a default time (midnight)
            warm_up_start_dt = datetime.combine(warm_up_start_dt, datetime.min.time())
            # Make it timezone-aware
            warm_up_start_dt = self.tzinfo.localize(warm_up_start_dt)
        else:
            warm_up_start_dt = start_dt

        self._data_datetime_start = warm_up_start_dt
        self._data_datetime_end = end_dt

        if self._timestep not in ['day', 'minute']:
            raise ValueError("Invalid timestep passed. Must be 'day' or 'minute'.")

        self._trading_days = get_trading_days(
            self.market,
            self._data_datetime_start,
            self._data_datetime_end + timedelta(days=1),  # end_date is exclusive in this function
            tzinfo=self.tzinfo
        )

        full_window = kwargs.get('full_window')
        if full_window is None:
            full_window = environment_mode
        self._full_window = bool(full_window)

        if not self._full_window:
            # Legacy behavior for an explicit config (kept for existing scripts and the legacy
            # tests): stop at the open of the third-to-last trading day. The original comment:
            # "lumibot crashed calculating portfolio value after the last day of data, so the
            # backtest ends before the data runs out". Environment mode and full_window=True run
            # through backtesting_end like every other source (the base class already set
            # datetime_end to backtesting_end minus one minute), and the data window covers the
            # whole end date.
            end_shift = -3
            last_trading_day = self._trading_days.iloc[end_shift]['market_open']
            self.datetime_end = last_trading_day

        self.datetime_start = start_dt
        self._datetime = self.datetime_start

    def _sanitize_base_and_quote_asset(self, base_asset, quote_asset) -> tuple[Asset, Asset]:
        asset, quote = sanitize_base_and_quote_asset(base_asset, quote_asset)
        return asset, quote

    def get_last_price(
            self,
            asset: Asset,
            quote: Asset | None = None,
            exchange: str | None = None
    ) -> float | Decimal | None:
        """Return an available mark, using a daily close only after its session ends.

        Options use real trade prints only: the open of a bar that printed in the current
        bucket, otherwise the close of the latest earlier print, otherwise ``None``.
        """

        other_source = self._other_source_for(asset)
        if other_source is not None:
            return other_source.get_last_price(asset, timestep=self._timestep, quote=quote, exchange=exchange)

        asset, quote = self._sanitize_base_and_quote_asset(asset, quote)

        if self._is_option(asset):
            return self._get_option_last_price(asset, quote)

        timestep = self._timestep
        source_timestep, resample_rule = self._normalize_timestep_for_source(timestep)
        if resample_rule is None:
            # PERF: every Alpaca strategy asks this on every iteration. The answer is one row
            # of the stored frame, the same row get_historical_prices(length=1,
            # remove_incomplete_current_bar=False, _extend_history=False) would wrap in a Bars
            # object (about 0.2 ms on a Mac, 1 ms in the cloud per call). Same data, same
            # row, same errors; resampled steps keep the full path below.
            try:
                df = self.get_historical_prices_between_dates(
                    base_asset=asset,
                    quote_asset=quote,
                    timestep=source_timestep,
                    data_datetime_start=self._data_datetime_start,
                    data_datetime_end=self._data_datetime_end,
                    auto_adjust=self._auto_adjust,
                )
            except Exception as e:
                raise RuntimeError(f"Unable to fetch historical prices during backtest: {e}")
            if len(df) < 1:
                raise ValueError("Not enough historical data. Requested 1 bars but only 0 available.")
            position = self._newest_bar_position(df.index, self._datetime, timestep, False)
            if position < 0:
                raise ValueError(f"Datetime {self._datetime} not found in the dataset.")
            if timestep == "day":
                return self._daily_bar_mark(df.iloc[position:position + 1], asset, quote)
            price = df["open"].iat[position]
            return quantize_to_num_decimals(price, get_decimals(price))

        bars = self.get_historical_prices(
            asset=asset,
            length=1,  # Get one bar
            timestep=self._timestep,
            quote=quote,
            remove_incomplete_current_bar=False,  # We want the incomplete bar (aka current bar) for get_last_price
            _extend_history=False,
        )

        if bars is None or bars.df.empty:
            return None

        if self._timestep == "day":
            return self._daily_bar_mark(bars.df, asset, quote)

        # The backtesting_broker, fills market orders using the open price of the current bar, so
        # get_last_price should also return the open. (It would be weird to fill on the open but provide the close
        # as the last price). Daily bars use the session-aware mark above; this
        # remaining path preserves the existing intraday fill-price convention.
        # Support both pandas and polars-backed Bars without exceptions
        df_local = bars.df
        if hasattr(df_local, "iloc"):
            # pandas: scalar-fast path
            price = df_local["open"].iat[0]
        else:
            # polars
            price = df_local["open"][0]
        num_decimals = get_decimals(price)
        return quantize_to_num_decimals(price, num_decimals)

    def _session_bounds(self, session_date):
        """(market_open, market_close) of the loaded calendar's session on ``session_date``, or None.

        PERF: a daily mark is asked for every symbol on every iteration. Finding the session
        by converting the whole calendar to dates on each call cost about 1 ms per price; the
        calendar is indexed by date once instead (rebuilt if the calendar object changes).
        """
        sessions = self._trading_days
        cached = getattr(self, "_session_bounds_index", None)
        if cached is None or cached[0] is not sessions or cached[1] != len(sessions):
            lookup = {}
            dates = sessions["market_open"].dt.tz_convert("America/New_York").dt.date
            for day, opened, closed in zip(dates, sessions["market_open"], sessions["market_close"]):
                lookup.setdefault(day, (opened, closed))  # the first matching session, as before
            cached = (sessions, len(sessions), lookup)
            self._session_bounds_index = cached
        return cached[2].get(session_date)

    def _daily_bar_mark(self, frame, asset, quote):
        """Use the opening mark during a session and its close after completion.

        Daily stock bars are midnight-labeled. Their open is unavailable before
        the exchange opens; their close is unavailable until the actual exchange
        close, including shortened sessions. Crypto retains its full-day window.
        """
        stamp = pd.Timestamp(frame.index[-1])
        now = pd.Timestamp(self._datetime)
        if str(getattr(asset, "asset_type", "")).lower() == "crypto":
            opened, closed = stamp, stamp + pd.Timedelta(days=1)
        else:
            # Alpaca equity daily labels denote the New York session even
            # when the caller requested another display timezone.
            session_date = stamp.tz_convert("America/New_York").date()
            bounds = None if self.market in ("24/7", "24/5") else self._session_bounds(session_date)
            if bounds is None:
                # Warmup may precede the loaded backtest calendar. Stock marks
                # also retain equity sessions when the strategy trades crypto.
                matched = get_trading_days(
                    market="NYSE" if self.market in ("24/7", "24/5") else self.market,
                    start_date=pd.Timestamp(session_date),
                    end_date=pd.Timestamp(session_date) + pd.Timedelta(days=1),
                    tzinfo=self.tzinfo,
                )
                if matched.empty:
                    return None
                bounds = (matched.iloc[0]["market_open"], matched.iloc[0]["market_close"])
            opened = max(pd.Timestamp(bounds[0]), stamp)
            closed = pd.Timestamp(bounds[1])
        if now < opened:
            previous = self.get_historical_prices(
                asset=asset, quote=quote, length=1, timestep="day",
                timeshift=now - stamp.normalize(),
                remove_incomplete_current_bar=True,
            )
            if previous is None or previous.df.empty or previous.df.index[-1] >= stamp.normalize():
                return None
            return self._daily_bar_mark(previous.df, asset, quote)
        price = frame["close" if now >= closed else "open"].iloc[-1]
        if price is None or pd.isna(price):
            return None
        return quantize_to_num_decimals(price, get_decimals(price))

    def get_historical_prices(
            self,
            asset: Asset,
            length: int,
            timestep: str | None = None,
            timeshift: timedelta | None = None,
            quote: Asset | None = None,
            exchange: str | None = None,
            include_after_hours: bool = True,
            return_polars: bool = False,
            remove_incomplete_current_bar: Optional[bool] = None,
            _extend_history: bool = True,
    ) -> Bars | None:
        """
        Get bars for an asset by delegating to get_historical_prices_between_dates
        for fetching the historical data, followed by additional processing.

        Get bars for a given asset, going back in time from now, getting length number of bars by timestep.
        For example, with a length of 10 and a timestep of "day", and now timeshift, this
        would return the last 10 daily bars.

        - Higher-level method that returns a `Bars` object
        - Handles timezone conversions automatically
        - Includes additional metadata and processing
        - Preferred for strategy development and backtesting
        - Returns normalized data with consistent format across data sources

        Parameters
        ----------
        asset : Asset
            The asset to get the bars for.
        length : int
            The number of bars to get.
        timestep : str
            The timestep to get the bars at. Accepts "day", "minute", and
            multi-timeframe aliases such as "15min", "1h", or "2d".
        timeshift : datetime.timedelta
            The amount of time to shift the reference point (self._datetime).
            If you want 10 daily bars from 1 week ago (not including the last week),
            you'd use timeshift=timedelta(days=7)
        quote : Asset
            The quote asset to get the bars for.
        exchange : str
            The exchange to get the bars for.
        include_after_hours : bool
            Whether to include after hours data.

        Returns
        -------
        Bars | None
            The bars for the asset.
        """
        if length <= 0:
            raise ValueError("Length must be positive.")

        # Default values for arguments
        if remove_incomplete_current_bar is None:
            remove_incomplete_current_bar = self._remove_incomplete_current_bar

        if timestep is None:
            timestep = self._timestep

        other_source = self._other_source_for(asset)
        if other_source is not None:
            return other_source.get_historical_prices(
                asset, length, timestep=timestep, timeshift=timeshift, quote=quote, exchange=exchange,
                include_after_hours=include_after_hours, return_polars=return_polars,
            )

        source_timestep, resample_rule = self._normalize_timestep_for_source(timestep)

        if quote is None:
            quote = self.LUMIBOT_DEFAULT_QUOTE_ASSET

        if self._is_option(asset):
            return self._get_option_historical_prices(
                asset,
                length,
                timestep=timestep,
                source_timestep=source_timestep,
                resample_rule=resample_rule,
                timeshift=timeshift,
                quote=quote,
                return_polars=return_polars,
                remove_incomplete_current_bar=remove_incomplete_current_bar,
            )

        # Determine search target datetime
        search_datetime = self._datetime
        if timeshift:
            search_datetime = self._datetime - timeshift

        rebuilt = self._bars_from_cached_minutes(
            asset, quote, source_timestep, timestep, search_datetime, length, remove_incomplete_current_bar,
        )
        if rebuilt is not None:
            return Bars(rebuilt, self.SOURCE, asset=asset, quote=quote, return_polars=return_polars,
                        tzinfo=self.tzinfo)

        try:
            # Fetch historical prices during the backtest using the dedicated function
            df = self.get_historical_prices_between_dates(
                base_asset=asset,
                quote_asset=quote,
                timestep=source_timestep,
                data_datetime_start=self._data_datetime_start,
                data_datetime_end=self._data_datetime_end,
                auto_adjust=self._auto_adjust
            )
        except Exception as e:
            # Handle errors if fetching data fails
            raise RuntimeError(f"Unable to fetch historical prices during backtest: {e}")

        # _extend_history=False is for the lookups that read the bar starting now (get_last_price
        # and the broker's fill); they never need bars from before the window.
        reach_back = _extend_history and bool(getattr(self, "_history_before_start", False))
        view = self._resample_ohlcv_dataframe(df, resample_rule) if resample_rule is not None else df
        current_index = self._newest_bar_position(
            view.index,
            search_datetime,
            timestep,
            remove_incomplete_current_bar,
        )
        if reach_back and current_index + 1 < length:
            extended = self._reach_back_for_history(
                asset=asset,
                quote=quote,
                source_timestep=source_timestep,
                frame=df,
                search_datetime=search_datetime,
                length=length,
                timestep=timestep,
            )
            if extended is not None:
                view = self._resample_ohlcv_dataframe(extended, resample_rule) if resample_rule is not None else extended
                current_index = self._newest_bar_position(
                    view.index,
                    search_datetime,
                    timestep,
                    remove_incomplete_current_bar,
                )
        df = view

        # Ensure sufficient bars are available. With history before the start, a series that
        # simply has fewer bars (a recent listing) returns the bars that exist instead, like the
        # other backtesting sources.
        if length > len(df) and not reach_back:
            raise ValueError(
                f"Not enough historical data. Requested {length} bars but only {len(df)} available."
            )

        # Handle data retrieval and slicing
        if current_index < 0:
            if remove_incomplete_current_bar:
                # No bar has finished yet (the very start of the data window). Missing is
                # honest: return None, as the other backtesting sources do, rather than the
                # bar that is still forming.
                return None
            raise ValueError(f"Datetime {search_datetime} not found in the dataset.")

        if current_index >= len(df):
            raise ValueError(f"Datetime {search_datetime} exceeds the dataset range.")

        if length == 1:
            result_df = df.iloc[[current_index]]
        else:
            result_df = df.iloc[max(0, current_index - length + 1): current_index + 1]

        return Bars(
            result_df,
            self.SOURCE,
            asset=asset,
            quote=quote,
            return_polars=return_polars,
            tzinfo=self.tzinfo,
        )

    # Bar sizes that can be built from the cached 1-minute series: each lines up with the
    # 09:30 open, and the provider's own regular-session bars are equal field for field
    # (measured, see docs/investigations/2026-11-14_INTRADAY_BARS_FROM_MINUTE_BARS.md).
    MINUTE_REBUILD_BAR_MINUTES = (5, 10, 15, 30)
    _REGULAR_SESSION_OPEN = dt_time(9, 30)
    _REGULAR_SESSION_CLOSE = dt_time(16, 0)

    def _intraday_bars_from_minutes_enabled(self) -> bool:
        configured = self.__dict__.get("_intraday_bars_from_minutes")
        if configured is not None:
            return bool(configured)
        value = os.environ.get("BACKTESTING_INTRADAY_BARS_FROM_MINUTES", "true")
        return str(value).strip().lower() not in ("0", "false", "no", "off")

    def _bars_from_cached_minutes(self, asset, quote, source_timestep, timestep, search_datetime,
                                  length, remove_incomplete_current_bar) -> pd.DataFrame | None:
        """Answer a 5, 10, 15 or 30-minute stock request from the cached 1-minute series.

        Returns the bars only when the answer is the same as the provider's own series would
        give: every requested bar lies inside today's regular session. The provider series
        also has pre-market and after-hours bars the 1-minute cache does not hold, so any
        request that reaches outside the session (or is not adjacent to the newest finished
        bar) returns None and the caller downloads the provider series as before. Once the
        provider series is loaded or cached it is always used.
        """
        if not source_timestep.endswith("minute") or source_timestep == "minute":
            return None
        try:
            bar_minutes = int(source_timestep[:-len("minute")])
        except ValueError:
            return None
        if bar_minutes not in self.MINUTE_REBUILD_BAR_MINUTES or not self._intraday_bars_from_minutes_enabled():
            return None
        if self.__dict__.get("_refresh_cache"):
            return None
        base, base_quote = self._sanitize_base_and_quote_asset(asset, quote)
        if str(getattr(base, "asset_type", "")).lower() != "stock":
            return None

        store = self.__dict__.get("_data_store")
        if store is None or getattr(self, "market", None) is None:
            return None  # an instance without loaded series or calendar (built without __init__)
        cache_dir = os.path.join(LUMIBOT_CACHE_FOLDER, self.CACHE_SUBFOLDER)
        native_key = self._get_asset_key(base_asset=base, quote_asset=base_quote, timestep=source_timestep)
        if native_key in store or os.path.exists(os.path.join(cache_dir, f"{native_key}.csv")):
            return None
        minute_key = self._get_asset_key(base_asset=base, quote_asset=base_quote, timestep="minute")
        minutes = store.get(minute_key)
        if minutes is None:
            if not os.path.exists(os.path.join(cache_dir, f"{minute_key}.csv")) \
                    or not self._load_ohlcv_into_data_store(minute_key):
                return None
            minutes = store.get(minute_key)
        if minutes is None or minutes.empty:
            return None

        built = self._state("_minute_rebuilt_frames", dict)
        known = built.get((minute_key, bar_minutes))
        if known is None or known[0] is not minutes:
            frame = self._resample_ohlcv_dataframe(
                minutes[["open", "high", "low", "close", "volume"]], f"{bar_minutes}min")
            # A filled minute has no volume. A period with no trades has no provider bar.
            frame = frame[frame["volume"] > 0]
            known = (minutes, frame)
            built[(minute_key, bar_minutes)] = known
        frame = known[1]

        position = self._newest_bar_position(frame.index, search_datetime, timestep, remove_incomplete_current_bar)
        first = position - int(length) + 1
        if position < 0 or first < 0:
            return None
        delta = timedelta(minutes=bar_minutes)
        cutoff = search_datetime - delta if remove_incomplete_current_bar else search_datetime
        newest = frame.index[position]
        if cutoff - newest >= delta:
            return None  # the provider series may hold a newer bar (after hours, or a gap)
        market_tz = pytz.timezone("America/New_York")
        oldest_local = frame.index[first].tz_convert(market_tz)
        newest_local = newest.tz_convert(market_tz)
        if oldest_local.date() != newest_local.date() \
                or oldest_local.time() < self._REGULAR_SESSION_OPEN \
                or newest_local.time() >= self._REGULAR_SESSION_CLOSE:
            return None
        from lumibot.tools import backtest_telemetry

        backtest_telemetry.record_cache("hit", layer="alpaca_bars_from_minutes", source="alpaca", asset_class="stock")
        return frame.iloc[first: position + 1]

    @staticmethod
    def _normalize_timestep_for_source(timestep: str) -> tuple[str, str | None]:
        """Return the Alpaca source timestep and optional pandas resample rule."""
        if timestep in ("day", "minute"):
            return timestep, None

        delta, unit = DataSourceBacktesting.convert_timestep_str_to_timedelta(timestep)

        if unit == "day":
            days = max(int(delta.total_seconds() // 86400), 1)
            return "day", None if days == 1 else f"{days}D"

        minutes = max(int(delta.total_seconds() // 60), 1)
        if minutes % 60 == 0:
            hours = minutes // 60
            if 1 <= hours <= 23:
                return ("hour" if hours == 1 else f"{hours}hour"), None
        if 1 <= minutes <= 59:
            return ("minute" if minutes == 1 else f"{minutes}minute"), None

        return "minute", f"{minutes}min"

    @staticmethod
    def _newest_bar_position(
            index: pd.DatetimeIndex,
            search_datetime: datetime,
            timestep: str,
            remove_incomplete_current_bar: bool,
    ) -> int:
        """Position in ``index`` of the newest bar to return at ``search_datetime``, or -1.

        Alpaca labels a bar with its start time, so the bar labeled 10:00 in a 5-minute series
        holds trades from 10:00 to 10:04:59 and closes at 10:05. Daily bars are labeled at
        midnight and close with the session.

        With ``remove_incomplete_current_bar`` only a bar that has finished by
        ``search_datetime`` qualifies: its label plus its length is at or before
        ``search_datetime`` (for daily bars, its date plus its length in days is at or before
        the search date). That is LumiBot's history contract for IBKR, ThetaData and Polygon
        (``Data._get_bars_row_bounds`` stops before the bar that is in progress), and it holds
        when ``search_datetime`` falls inside a multi-minute or resampled bar, not only when a
        bar starts exactly then.

        Without it, the bar that is still forming at ``search_datetime`` is included with its
        final close, high, low and volume. That is the documented default for an explicit
        config and what get_last_price and order fills use (they read the bar's open only).
        """
        delta, unit = DataSourceBacktesting.convert_timestep_str_to_timedelta(timestep)
        if unit == "day":
            search_date = search_datetime.date()
            if remove_incomplete_current_bar:
                search_date -= timedelta(days=max(delta.days, 1))
            days = _index_session_days(index)
            return int(days.searchsorted(np.datetime64(search_date, "D"), side="right")) - 1

        cutoff = search_datetime
        if remove_incomplete_current_bar:
            cutoff = search_datetime - delta
        return int(index.searchsorted(cutoff, side="right")) - 1

    @staticmethod
    def _get_alpaca_timeframe(timestep: str) -> TimeFrame:
        """Convert a normalized Lumibot timestep to an Alpaca SDK TimeFrame."""
        delta, unit = DataSourceBacktesting.convert_timestep_str_to_timedelta(timestep)

        if unit == "day":
            days = max(int(delta.total_seconds() // 86400), 1)
            if days != 1:
                raise ValueError("Alpaca only supports native 1Day bars; multi-day bars must be resampled.")
            return TimeFrame.Day

        if unit == "hour":
            hours = max(int(delta.total_seconds() // 3600), 1)
            return TimeFrame(hours, TimeFrameUnit.Hour)

        minutes = max(int(delta.total_seconds() // 60), 1)
        if minutes % 60 == 0:
            hours = minutes // 60
            return TimeFrame(hours, TimeFrameUnit.Hour)
        return TimeFrame(minutes, TimeFrameUnit.Minute)

    @staticmethod
    def _get_trading_times_for_timestep(pcal: pd.DataFrame, timestep: str) -> pd.DatetimeIndex:
        if timestep in ("day", "minute"):
            return get_trading_times(pcal=pcal, timestep=timestep)

        delta, unit = DataSourceBacktesting.convert_timestep_str_to_timedelta(timestep)
        if unit == "day":
            return get_trading_times(pcal=pcal, timestep="day")

        interval = pd.Timedelta(seconds=max(int(delta.total_seconds()), 60))
        trading_times = []
        for _, row in pcal.iterrows():
            start = row["market_open"]
            end = row["market_close"]
            is_24_7 = end.hour == 23 and end.minute == 59
            times = pd.date_range(start=start, end=end, freq=interval)
            times = times[times <= end] if is_24_7 else times[times < end]
            trading_times.extend(times)

        return pd.DatetimeIndex(trading_times)

    @staticmethod
    def _resample_ohlcv_dataframe(df: pd.DataFrame, resample_rule: str) -> pd.DataFrame:
        """Aggregate source OHLCV bars into a requested multi-timeframe."""
        if df.empty:
            return df
        from lumibot.tools.candles import resample_ohlcv

        # Shared builder: standard OHLCV columns present, sorted, empty buckets dropped.
        return resample_ohlcv(df, resample_rule)

    # ------------------------------------------------------------------
    # Options
    # ------------------------------------------------------------------
    @staticmethod
    def _is_option(asset) -> bool:
        return str(getattr(asset, "asset_type", "")).lower() == "option"

    # ------------------------------------------------------------------
    # Asset types another provider serves (routing map with "default": "alpaca")
    # ------------------------------------------------------------------
    _ROUTING_CONFIG_KEY = "backtesting_data_routing"

    @staticmethod
    def _routing_asset_type(value) -> str:
        raw = str(value or "").strip().lower()
        return raw.split(".")[-1] if "." in raw else raw

    @classmethod
    def _routes_to_other_sources(cls, routing) -> dict:
        """``{asset type: provider}`` for the map entries that name a provider other than Alpaca.

        Alpaca has no history for some asset types (indexes such as VIX or SPX). A routing map
        names who serves them. Everything the map leaves to Alpaca, options included, stays here.
        """
        routes = {}
        if not isinstance(routing, dict):
            return routes
        for key, provider in routing.items():
            asset_type = cls._routing_asset_type(key)
            provider_name = str(provider or "").strip()
            if asset_type in ("", "default") or not provider_name or provider_name.lower() == "alpaca":
                continue
            routes[asset_type] = provider_name
        return routes

    def _other_source_for(self, asset):
        """The routed source that serves ``asset``, set to this backtest's clock, or ``None``."""
        routes = self.__dict__.get("_other_source_routes")
        if not routes:
            return None
        base = asset[0] if isinstance(asset, tuple) else asset
        if self._routing_asset_type(getattr(base, "asset_type", "")) not in routes:
            return None
        companion = self.__dict__.get("_routed_companion")
        if companion is None:
            from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas

            companion_routes = dict(routes)
            # Only the listed asset types are ever asked of it; its default is never used.
            companion_routes["default"] = next(iter(routes.values()))
            start, end = self._routed_companion_window
            companion = RoutedBacktestingPandas(
                datetime_start=start,
                datetime_end=end,
                config={self._ROUTING_CONFIG_KEY: companion_routes},
                show_progress_bar=False,
            )
            self._routed_companion = companion
        # One clock: the companion answers at this backtest's simulated time and bar size.
        companion._datetime = self._datetime
        companion._timestep = self._timestep
        return companion

    @staticmethod
    def _config_from_environment() -> dict:
        """Alpaca credentials from the environment, the variables BotSpot and Bot Manager set."""
        paper_raw = os.environ.get("ALPACA_IS_PAPER")
        return {
            "API_KEY": (os.environ.get("ALPACA_API_KEY") or "").strip() or None,
            "API_SECRET": (os.environ.get("ALPACA_API_SECRET") or "").strip() or None,
            "OAUTH_TOKEN": (os.environ.get("ALPACA_OAUTH_TOKEN") or "").strip() or None,
            "PAPER": True if paper_raw is None else paper_raw.strip().lower() in ("true", "1", "yes", "y", "on"),
        }

    @staticmethod
    def _is_unauthorized(exc: Exception) -> bool:
        status = getattr(exc, "status_code", None)
        if status is None:
            status = getattr(getattr(exc, "response", None), "status_code", None)
        if status in (401, 403):
            return True
        text = str(exc).lower()
        return "unauthorized" in text or "forbidden" in text

    def _switch_trading_endpoint(self):
        """Use the other Trading API endpoint (paper vs live) for the read-only contract list.

        BotSpot always sends ALPACA_IS_PAPER=true, but a customer's connection can hold a live
        key, which paper-api rejects with 401 (and a paper key is rejected by the live API).
        Both endpoints serve the same contract master list. Backtests never send orders.
        """
        creds = self._state("_alpaca_credentials", dict)
        creds["paper"] = not bool(creds.get("paper", True))
        self._trading_endpoint_switched = True
        self._trading_client = None
        logger.info(
            "Alpaca Trading API rejected this key on the %s endpoint; listing option contracts from the %s "
            "endpoint instead (read-only; backtests never send orders to Alpaca).",
            "live" if creds["paper"] else "paper",
            "paper" if creds["paper"] else "live",
        )
        return self._get_trading_client()

    def _state(self, name, factory):
        """Per-instance state that also exists on instances built without __init__ (tests)."""
        value = self.__dict__.get(name)
        if value is None:
            value = factory()
            self.__dict__[name] = value
        return value

    def _oauth_account_cache_identity(self):
        """``account:<id>`` for the OAuth token's Alpaca account, resolved once; None if unknown.

        Used only to find bars cached per account before the cache was shared by every
        connection (see ``alpaca_history._legacy_scopes``). Read-only: one account lookup per
        backtest. Any failure returns None and only the shared cache is read.
        """
        if "_oauth_cache_identity" in self.__dict__:
            return self.__dict__["_oauth_cache_identity"]
        identity = None
        creds = self.__dict__.get("_alpaca_credentials") or {}
        token = creds.get("oauth_token")
        if token:
            from alpaca.trading.client import TradingClient

            # A connection can hold a live token while backtests say paper: try both endpoints.
            for paper in (bool(creds.get("paper", True)), not bool(creds.get("paper", True))):
                try:
                    account_id = getattr(TradingClient(oauth_token=token, paper=paper).get_account(), "id", None)
                except Exception as exc:
                    logger.debug("Alpaca account lookup for cache scope failed (%s)", type(exc).__name__)
                    continue
                if account_id:
                    identity = f"account:{account_id}"
                    break
        self.__dict__["_oauth_cache_identity"] = identity
        return identity

    def _get_trading_client(self):
        client = self.__dict__.get("_trading_client")
        if client is not None:
            return client
        from alpaca.trading.client import TradingClient

        creds = self.__dict__.get("_alpaca_credentials") or {}
        paper = bool(creds.get("paper", True))
        if creds.get("api_key") and creds.get("api_secret"):
            client = TradingClient(creds["api_key"], creds["api_secret"], paper=paper)
        elif creds.get("oauth_token"):
            client = TradingClient(oauth_token=creds["oauth_token"], paper=paper)
        else:
            raise ValueError("Alpaca option chains need an API key/secret or an OAuth token")
        self._trading_client = client
        return client

    def _sleep_seconds(self, seconds: float) -> None:
        sleeper = self.__dict__.get("_sleep") or time.sleep
        sleeper(seconds)

    def _throttle_alpaca_requests(self) -> None:
        """Keep this process under Alpaca's free-tier limit (about 200 requests per minute)."""
        recent = self._state("_alpaca_request_times", deque)
        limit = max(1, int(self.ALPACA_MAX_REQUESTS_PER_MINUTE))
        now = time.monotonic()
        while recent and now - recent[0] >= 60.0:
            recent.popleft()
        if len(recent) >= limit:
            wait = 60.0 - (now - recent[0]) + 0.05
            if wait > 0:
                logger.info("Alpaca request budget reached (%d per minute); waiting %.1fs", limit, wait)
                self._sleep_seconds(wait)
                from lumibot.tools import backtest_telemetry

                backtest_telemetry.record_wait("rate_limit", wait)
            now = time.monotonic()
            while recent and now - recent[0] >= 60.0:
                recent.popleft()
        recent.append(time.monotonic())

    @staticmethod
    def _is_rate_limited(exc: Exception) -> bool:
        status = getattr(exc, "status_code", None)
        if status is None:
            status = getattr(getattr(exc, "response", None), "status_code", None)
        if status == 429:
            return True
        text = str(exc).lower()
        return "429" in text or "too many requests" in text or "rate limit" in text

    @staticmethod
    def _rate_limit_wait_seconds(exc: Exception, attempt: int) -> float:
        headers = getattr(getattr(exc, "response", None), "headers", None) or {}
        try:
            retry_after = headers.get("Retry-After") or headers.get("retry-after")
            if retry_after is not None:
                return float(min(max(float(retry_after), 1.0), 60.0))
        except (TypeError, ValueError):
            pass
        try:
            reset = headers.get("X-RateLimit-Reset") or headers.get("x-ratelimit-reset")
            if reset is not None:
                return float(min(max(float(reset) - time.time(), 1.0), 60.0))
        except (TypeError, ValueError):
            pass
        return float(min(5.0 * (2 ** max(attempt - 1, 0)), 60.0))

    @staticmethod
    def _is_option_data_plan_refusal(fn, exc: Exception) -> bool:
        """HTTP 403 on the option bars endpoint only: the key works, the plan lacks the feed.

        Contract listing (trading API) keeps its own 401/403 handling, and a 403 on stock or
        crypto bars says nothing about options.
        """
        if getattr(fn, "__name__", "") != "get_option_bars":
            return False
        status = getattr(exc, "status_code", None)
        if status is None:
            status = getattr(getattr(exc, "response", None), "status_code", None)
        return status == 403

    def _alpaca_request(self, fn, request, *, what: str):
        """Call one Alpaca SDK method politely: client-side throttle plus bounded 429 retry.

        The SDK already retries a 429 three times after 3 seconds. This adds a longer,
        bounded wait (``Retry-After`` when Alpaca sends it) before failing loudly.
        """
        from lumibot.tools import backtest_telemetry

        with backtest_telemetry.provider_scope("alpaca", _alpaca_asset_class(fn)):
            return self._alpaca_request_in_scope(fn, request, what=what)

    def _alpaca_request_in_scope(self, fn, request, *, what: str):
        from lumibot.tools import backtest_telemetry

        retries = 0
        waited = 0.0
        while True:
            self._throttle_alpaca_requests()
            started = time.perf_counter()
            try:
                answer = fn(request)
                backtest_telemetry.record_provider_request(time.perf_counter() - started)
                return answer
            except Exception as exc:
                backtest_telemetry.record_provider_request(time.perf_counter() - started)
                if self._is_option_data_plan_refusal(fn, exc):
                    raise AlpacaOptionsDataPlanError(
                        "Your Alpaca data plan does not include options history. Alpaca answered "
                        f"HTTP 403 for option bars ({what}): {exc}. Enable options market data on "
                        "this Alpaca account, or backtest options with another data source."
                    ) from exc
                if not self._is_rate_limited(exc):
                    raise
                retries += 1
                backtest_telemetry.record_retry()
                wait = self._rate_limit_wait_seconds(exc, retries)
                if (
                    retries > self.ALPACA_RATE_LIMIT_MAX_RETRIES
                    or waited + wait > self.ALPACA_RATE_LIMIT_MAX_WAIT_SECONDS
                ):
                    raise RuntimeError(
                        f"Alpaca rate limit persisted while requesting {what} after {retries - 1} waits "
                        f"({waited:.0f}s). Free keys allow about 200 requests per minute; retry later."
                    ) from exc
                logger.warning(
                    "Alpaca rate limit (HTTP 429) while requesting %s; waiting %.1fs (retry %d of %d)",
                    what,
                    wait,
                    retries,
                    self.ALPACA_RATE_LIMIT_MAX_RETRIES,
                )
                waited += wait
                self._sleep_seconds(wait)
                backtest_telemetry.record_wait("rate_limit", wait)

    @staticmethod
    def _as_date(value) -> Optional[date]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        try:
            return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
        except ValueError:
            return None

    @staticmethod
    def _option_root(symbol: str) -> str:
        return "".join(ch for ch in str(symbol or "").upper() if ch.isalnum())

    @staticmethod
    def _month_start(day: date) -> date:
        return day.replace(day=1)

    @staticmethod
    def _next_month(month: date) -> date:
        return (month.replace(day=28) + timedelta(days=4)).replace(day=1)

    @classmethod
    def _month_end(cls, month: date) -> date:
        return cls._next_month(month) - timedelta(days=1)

    def _option_month_file(self, symbol: str, month: date) -> str:
        return os.path.join(
            LUMIBOT_CACHE_FOLDER, self.CACHE_SUBFOLDER, "option_chains", "by_expiration_month",
            f"{symbol}_{month.strftime('%Y-%m')}.json",
        )

    def _option_month_is_final(self, month: date) -> bool:
        """True once every expiration in the month has passed, so the listing can no longer grow."""
        return self._month_end(month) < datetime.now(pytz.UTC).date() - timedelta(days=1)

    def _list_option_contracts(self, symbol: str, first_expiration: date, last_expiration: date) -> list:
        """Return (expiration, right, strike) for standard contracts expiring in the window.

        Contracts are kept per (underlying, expiration month): every simulated day whose window
        touches a month reuses that month's list, in memory, on disk and through the shared
        remote cache. Alpaca's contract listing has no as-of date, so a month's list is the same
        answer for every simulated day and every Alpaca connection; filtering it to the window
        gives exactly the chain a per-day listing would. A month is shared only once all of its
        expirations have passed, because an open month can still gain contracts.
        """
        months = []
        month = self._month_start(first_expiration)
        while month <= last_expiration:
            months.append(month)
            month = self._next_month(month)

        memory = self._state("_option_contract_months", dict)
        missing = [m for m in months if (symbol, m) not in memory]
        if missing:
            self._load_option_months(symbol, missing)
        out = []
        for m in months:
            out.extend(c for c in memory[(symbol, m)] if first_expiration <= c[0] <= last_expiration)
        return out

    def _load_option_months(self, symbol: str, months: list) -> None:
        """Fill the month cache from local disk, then the shared cache, then Alpaca (one listing per gap)."""
        from lumibot.tools import backtest_telemetry

        memory = self._state("_option_contract_months", dict)
        remote_cache = None
        try:
            from lumibot.tools.backtest_cache import get_backtest_cache

            remote_cache = get_backtest_cache()
            if remote_cache is not None and not remote_cache.enabled:
                remote_cache = None
        except Exception as exc:  # the remote cache must never break a chain lookup
            logger.debug("Alpaca option chain remote cache unavailable (%s)", type(exc).__name__)

        to_fetch = []
        for month in months:
            path = self._option_month_file(symbol, month)
            if remote_cache is not None and not os.path.exists(path):
                try:
                    remote_cache.ensure_local_file(Path(path))
                except Exception as exc:  # a remote miss or outage falls through to Alpaca
                    logger.debug("Alpaca option chain remote read skipped (%s)", type(exc).__name__)
            hit = False
            if os.path.exists(path):
                try:
                    with open(path, "r", encoding="utf-8") as handle:
                        rows = json.load(handle)
                    memory[(symbol, month)] = [
                        (date.fromisoformat(row[0]), str(row[1]), float(row[2]), str(row[3] if len(row) > 3 else symbol))
                        for row in rows
                    ]
                    hit = True
                except (OSError, ValueError, TypeError) as exc:
                    logger.warning("Ignoring unreadable Alpaca option chain cache %s: %s", path, exc)
            backtest_telemetry.record_cache("hit" if hit else "miss", layer="alpaca_option_chains",
                                            source="alpaca", asset_class="option")
            if not hit:
                to_fetch.append(month)
        if not to_fetch:
            return

        # One listing per run of consecutive missing months, plus a little extra ahead so the
        # next simulated days find their months already loaded.
        groups = []
        for month in to_fetch:
            if groups and self._next_month(groups[-1][-1]) == month:
                groups[-1].append(month)
            else:
                groups.append([month])
        for group in groups:
            fetch_months = list(group)
            for _ in range(int(self.OPTION_CHAIN_LISTING_EXTRA_MONTHS)):
                extra = self._next_month(fetch_months[-1])
                if (symbol, extra) in memory or os.path.exists(self._option_month_file(symbol, extra)):
                    break
                fetch_months.append(extra)
            contracts = self._fetch_option_contracts(symbol, fetch_months[0], self._month_end(fetch_months[-1]))
            by_month = {m: [] for m in fetch_months}
            for contract in contracts:
                key = self._month_start(contract[0])
                if key in by_month:
                    by_month[key].append(contract)
            for month, rows in by_month.items():
                rows = sorted(set(rows))
                memory[(symbol, month)] = rows
                self._write_option_month(symbol, month, rows, remote_cache)

    def _write_option_month(self, symbol: str, month: date, rows: list, remote_cache) -> None:
        path = self._option_month_file(symbol, month)
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as handle:
                json.dump([[row[0].isoformat(), *row[1:]] for row in rows], handle,
                          separators=(",", ":"))
            if remote_cache is not None and self._option_month_is_final(month):
                remote_cache.on_local_update(Path(path))
        except OSError as exc:
            logger.warning("Could not write Alpaca option chain cache %s: %s", path, exc)
        except Exception as exc:  # an upload failure must never break a chain lookup
            logger.debug("Alpaca option chain upload skipped (%s)", type(exc).__name__)

    def _fetch_option_contracts(self, symbol: str, first_expiration: date, last_expiration: date) -> list:
        """One Alpaca contract listing for the expiration window.

        Expired contracts are ``status=inactive`` and live ones ``status=active``; a historical
        chain needs both. Follows ``next_page_token`` to the end.
        """
        from alpaca.trading.enums import AssetStatus
        from alpaca.trading.requests import GetOptionContractsRequest

        listing_end = last_expiration
        root = self._option_root(symbol)
        weekly_root = self._index_weekly_root(symbol)
        client = self._get_trading_client()
        contracts = []
        for status in (AssetStatus.INACTIVE, AssetStatus.ACTIVE):
            page_token = None
            while True:
                request = GetOptionContractsRequest(
                    # Index options also trade under a weekly/PM-settled root (SPX and SPXW).
                    underlying_symbols=[symbol] + ([weekly_root] if weekly_root else []),
                    status=status,
                    expiration_date_gte=first_expiration,
                    expiration_date_lte=listing_end,
                    limit=int(self.OPTION_CONTRACTS_PAGE_LIMIT),
                    page_token=page_token,
                )
                try:
                    response = self._alpaca_request(
                        client.get_option_contracts,
                        request,
                        what=f"{symbol} option contracts ({status.value})",
                    )
                except Exception as exc:
                    if not self._is_unauthorized(exc) or self.__dict__.get("_trading_endpoint_switched"):
                        raise
                    client = self._switch_trading_endpoint()
                    response = self._alpaca_request(
                        client.get_option_contracts,
                        request,
                        what=f"{symbol} option contracts ({status.value})",
                    )
                for contract in getattr(response, "option_contracts", None) or []:
                    parsed = self._parse_option_contract(contract, root, weekly_root)
                    if parsed is not None:
                        contracts.append(parsed)
                page_token = getattr(response, "next_page_token", None)
                if not page_token:
                    break

        return contracts

    @staticmethod
    def _index_weekly_root(symbol: str) -> Optional[str]:
        from lumibot.tools.index_option_roots import index_weekly_root

        return index_weekly_root(symbol)

    def _alpaca_option_root(self, asset: Asset) -> str:
        """The root Alpaca lists this contract under. SPX expiring on a non-monthly date is SPXW.

        A root the listing reported for this exact contract wins; when the listing has both
        (a monthly date lists AM-settled SPX and PM-settled SPXW), the monthly root is used.
        A strategy that names the weekly root itself (SPXW) keeps it.
        """
        symbol = str(asset.symbol)
        weekly = self._index_weekly_root(symbol)
        if weekly is None:
            return symbol
        expiration = self._as_date(asset.expiration)
        if expiration is None:
            return symbol
        side = "CALL" if str(asset.right or "").upper().startswith("C") else "PUT"
        try:
            strike = float(asset.strike)
        except (TypeError, ValueError):
            strike = None
        listed = self._state("_listed_option_roots", dict).get((symbol.upper(), expiration.isoformat(), side, strike))
        if listed and len(listed) == 1:
            return next(iter(listed))
        from lumibot.tools.index_option_roots import index_option_root

        return index_option_root(symbol, expiration)

    def _parse_option_contract(self, contract, root: str, weekly_root: Optional[str] = None):
        try:
            size = float(getattr(contract, "size", 100) or 100)
        except (TypeError, ValueError):
            return None
        if size != 100:
            # Adjusted deliverables after corporate actions are not the standard contract.
            return None
        contract_root = getattr(contract, "root_symbol", None)
        contract_root_clean = self._option_root(contract_root) if contract_root else root
        if contract_root and contract_root_clean not in {root, self._option_root(weekly_root or root)}:
            return None
        expiration = self._as_date(getattr(contract, "expiration_date", None))
        right = str(getattr(getattr(contract, "type", None), "value", getattr(contract, "type", "")) or "").upper()
        if right not in ("CALL", "PUT") or expiration is None:
            return None
        try:
            strike = float(getattr(contract, "strike_price"))
        except (TypeError, ValueError):
            return None
        return expiration, right, strike, contract_root_clean

    @staticmethod
    def _empty_chain(symbol: str) -> dict:
        return {"Multiplier": 100, "Exchange": "SMART", "UnderlyingSymbol": symbol, "Chains": {"CALL": {}, "PUT": {}}}

    @staticmethod
    def _copy_chain(chain: dict) -> Chains:
        copied = dict(chain)
        copied["Chains"] = {
            side: {expiry: list(strikes) for expiry, strikes in (chain.get("Chains", {}).get(side) or {}).items()}
            for side in ("CALL", "PUT")
        }
        return Chains(copied)

    def get_chains(self, asset, quote=None):
        """Return the option chain for ``asset`` as of the simulated date.

        Shape (same as the other backtesting sources)::

            {
                "Multiplier": 100,
                "Exchange": "SMART",
                "UnderlyingSymbol": "SPY",
                "Chains": {
                    "CALL": {"2026-08-21": [640.0, 645.0, ...], ...},
                    "PUT": {"2026-08-21": [640.0, 645.0, ...], ...},
                },
            }

        Expirations run from the simulated date (or ``min_expiration_date``) through
        ``OPTION_CHAIN_MAX_DAYS`` days later (or ``max_expiration_date``). See the class
        docstring for caching, rate limits and the listing lookahead limit.
        """
        if isinstance(asset, str):
            asset = Asset(asset)
        symbol = str(getattr(asset, "symbol", "") or "").upper()
        current_date = self.get_datetime().date()

        constraints = getattr(self, "_chain_constraints", None) or {}
        first_expiration = current_date
        min_hint = self._as_date(constraints.get("min_expiration_date")) if isinstance(constraints, dict) else None
        if min_hint is not None and min_hint > first_expiration:
            first_expiration = min_hint
        max_hint = self._as_date(constraints.get("max_expiration_date")) if isinstance(constraints, dict) else None
        default_days = int(self.__dict__.get("_option_chain_max_days") or self.OPTION_CHAIN_MAX_DAYS)
        last_expiration = max_hint if max_hint is not None else current_date + timedelta(days=default_days)
        if last_expiration < first_expiration:
            return self._copy_chain(self._empty_chain(symbol))

        cache_key = (symbol, current_date.isoformat(), first_expiration.isoformat(), last_expiration.isoformat())
        memory = self._state("_option_chain_cache", dict)
        if cache_key in memory:
            return self._copy_chain(memory[cache_key])

        # The day's chain is built from per-expiration-month contract lists, which are cached in
        # memory, on disk and in the shared remote cache (see _list_option_contracts).
        chain = self._empty_chain(symbol)
        listed_roots = self._state("_listed_option_roots", dict)
        for expiration, right, strike, *root in self._list_option_contracts(symbol, first_expiration, last_expiration):
            chain["Chains"][right].setdefault(expiration.isoformat(), set()).add(strike)
            if root:
                listed_roots.setdefault((symbol, expiration.isoformat(), right, float(strike)), set()).add(root[0])
        for side in ("CALL", "PUT"):
            chain["Chains"][side] = {
                expiry: sorted(strikes) for expiry, strikes in sorted(chain["Chains"][side].items())
            }
        memory[cache_key] = chain
        self._remember_listed_contracts(chain)
        return self._copy_chain(chain)

    def _remember_listed_contracts(self, chain: dict) -> None:
        """Record which contracts Alpaca's own contract listing returned (the chain's source)."""
        listed = self._state("_listed_option_strikes", dict)
        underlying = str(chain.get("UnderlyingSymbol") or "").upper()
        for side, expirations in (chain.get("Chains") or {}).items():
            for expiry, strikes in (expirations or {}).items():
                listed.setdefault((underlying, str(expiry)[:10], side), set()).update(float(k) for k in strikes)

    def _listed_occ_symbols(self, assets) -> set:
        """OCC symbols of ``assets`` that Alpaca listed. Only for these is "no bars" an answer:
        a zero-row reply for an unlisted symbol may just be a mistyped contract."""
        listed = self._state("_listed_option_strikes", dict)
        out = set()
        for asset in assets:
            try:
                expiry = asset.expiration.isoformat() if hasattr(asset.expiration, "isoformat") else str(asset.expiration)
                side = "CALL" if str(asset.right or "").upper().startswith("C") else "PUT"
                from lumibot.tools.index_option_roots import index_underlying_symbol

                # A strategy naming the weekly root (SPXW) uses the index's chain (SPX).
                key = (index_underlying_symbol(asset.symbol).upper(), expiry[:10], side)
                if float(asset.strike) in listed.get(key, ()):
                    out.add(self._occ_symbol(asset))
            except (AttributeError, TypeError, ValueError):
                continue
        return out

    # GET /v1beta1/options/bars accepts at most 100 symbols per request (Alpaca docs,
    # checked 2026-11-11 for alpaca-py 0.44.0). The SDK follows page tokens itself.
    ALPACA_OPTION_SYMBOLS_PER_REQUEST = 100

    # How far before a pick its candidates are loaded from (rounded down to a month start).
    OPTION_PICK_LOOKBACK_DAYS = 7

    def prefetch_option_bars(self, assets, quote: Asset | None = None, timestep: str | None = None,
                             until: datetime | None = None, through_window_end: bool = False) -> int:
        """Load real trade bars for many option contracts with as few requests as possible.

        A delta or price search probes many strikes. Each probe used to download one
        contract's whole backtest window in its own request. This asks Alpaca for up to
        ``ALPACA_OPTION_SYMBOLS_PER_REQUEST`` contracts per request, through the same shared
        bar cache, and stores each contract exactly as a single download would (real prints
        only; a contract that never traded is remembered as empty). Later price and
        history lookups read the stored frames. Returns how many contracts were requested.

        ``until`` (optional) loads prints only up to the end of that day. A probe at time T
        needs nothing later; the rest of the window is loaded once, the first time a later
        lookup on that contract needs it (``_get_option_bars_frame``). Partial frames are
        never written to the per-window CSV cache.

        ``through_window_end`` (with ``until``) keeps the late start but loads through the end
        of the window (each contract's expiry), so nothing needs extending later.
        """
        timestep = timestep or self.__dict__.get("_timestep") or "minute"
        source_timestep, _ = self._normalize_timestep_for_source(timestep)
        if quote is None:
            quote = self.LUMIBOT_DEFAULT_QUOTE_ASSET
        store = self._state("_data_store", dict)
        refreshed = self._state("_refreshed_keys", dict)
        covered = self._state("_option_covered_until", dict)
        wanted = {}
        extend = {}
        for asset in assets or []:
            if not self._is_option(asset):
                continue
            asset, asset_quote = self._sanitize_base_and_quote_asset(asset, quote)
            key = self._get_asset_key(base_asset=asset, quote_asset=asset_quote, timestep=source_timestep)
            if key in store and key in covered and pd.Timestamp(self._datetime) > covered[key]:
                extend.setdefault(key, (asset, asset_quote))
                continue
            if key in wanted or key in store:
                continue
            if self._refresh_cache and key not in refreshed:
                wanted[key] = (asset, asset_quote)
                continue
            if self._load_ohlcv_into_data_store(key):
                continue
            wanted[key] = (asset, asset_quote)
        if extend:
            self._extend_option_frames(extend, source_timestep)
        if not wanted:
            return len(extend)
        items = list(wanted.items())
        covered_until = None
        loaded_from = None
        if until is not None:
            day_end = pd.Timestamp(until).tz_convert(self.tzinfo).normalize() + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
            if day_end < pd.Timestamp(self._data_datetime_end) and not through_window_end:
                covered_until = day_end
            # Load from the start of the month a week before the pick (a shared-cache partition),
            # not the start of the backtest: earlier prints are read only if a lookup needs them
            # (no print in that stretch, or a history request). A week back keeps picks early in
            # a month from needing a separate earlier read for most candidates.
            month = (pd.Timestamp(until) - pd.Timedelta(days=self.OPTION_PICK_LOOKBACK_DAYS)).tz_convert("UTC")
            month_start = pd.Timestamp(year=month.year, month=month.month, day=1, tz="UTC").tz_convert(self.tzinfo)
            if month_start > pd.Timestamp(self._data_datetime_start):
                loaded_from = month_start
        size = max(1, int(self.ALPACA_OPTION_SYMBOLS_PER_REQUEST))
        for offset in range(0, len(items), size):
            chunk = items[offset:offset + size]
            symbols = sorted(self._occ_symbol(asset) for _key, (asset, _quote) in chunk)
            first_asset, first_quote = chunk[0][1]
            client, request = self._history_request(
                base_asset=first_asset,
                quote_asset=first_quote,
                timestep=source_timestep,
                data_datetime_start=self._data_datetime_start,
                data_datetime_end=self._data_datetime_end,
                auto_adjust=self._auto_adjust,
            )
            # One contract keeps the plain-string request a single download sends.
            update = {"symbol_or_symbols": symbols[0] if len(symbols) == 1 else symbols}
            if covered_until is not None:
                update["end"] = covered_until.to_pydatetime()
            if loaded_from is not None:
                update["start"] = loaded_from.to_pydatetime()
            update["end"] = self._option_request_end(
                [asset for _key, (asset, _quote) in chunk], request.start, update.get("end", request.end)
            )
            request = request.model_copy(update=update)
            what = f"{len(symbols)} option contracts"
            listed = self._listed_occ_symbols([asset for _key, (asset, _quote) in chunk])
            try:
                bars = fetch_alpaca_bars(
                    client, request,
                    lambda query: self._alpaca_request(client.get_option_bars, query, what=what),
                    refresh=getattr(self, "_refresh_cache", False), empty_ok=listed.__contains__,
                )
            except AlpacaOptionsDataPlanError:
                raise
            except Exception as e:
                raise RuntimeError(f"Failed to fetch data for {what}: {e}")
            by_symbol = self._rows_by_symbol(bars.df)
            for key, (asset, asset_quote) in chunk:
                rows = by_symbol.get(self._occ_symbol(asset))
                self._store_option_rows(key, asset, rows, covered_until=covered_until, loaded_from=loaded_from)
                if self._refresh_cache:
                    refreshed[key] = True
        if items:
            through = f" up to {covered_until.date()}" if covered_until is not None else ""
            logger.info("Loaded %d option contracts in %d request(s)%s", len(items),
                        (len(items) + size - 1) // size, through)
        return len(items) + len(extend)

    @staticmethod
    def _rows_by_symbol(frame: pd.DataFrame) -> dict:
        """Each symbol's rows of a (symbol, timestamp) frame, as ``frame.xs(symbol).reset_index()`` gives them.

        One groupby pass instead of one full scan per contract (a pick asks for up to 100).
        """
        if frame is None or frame.empty:
            return {}
        return {symbol: rows.droplevel(0).reset_index()
                for symbol, rows in frame.groupby(level=0, sort=False)}

    def _extend_option_frames(self, contracts: dict, timestep: str) -> None:
        """``_extend_option_frame`` for many contracts: one request per covered day and 100 symbols.

        Each contract gets exactly the rows its own extension would fetch (from its covered
        day to its expiry, same shared-cache series), merged the same way. On a cloud
        container each single extension was its own shared-cache read and S3 round trips.
        """
        covered = self._state("_option_covered_until", dict)
        store = self._state("_data_store", dict)
        groups = {}
        for key, (asset, quote) in contracts.items():
            groups.setdefault(covered[key], []).append((key, asset, quote))
        size = max(1, int(self.ALPACA_OPTION_SYMBOLS_PER_REQUEST))
        for covered_until, members in sorted(groups.items(), key=lambda kv: kv[0]):
            for offset in range(0, len(members), size):
                chunk = members[offset:offset + size]
                _key, first_asset, first_quote = chunk[0]
                client, request = self._history_request(
                    base_asset=first_asset, quote_asset=first_quote, timestep=timestep,
                    data_datetime_start=covered_until.to_pydatetime(), data_datetime_end=self._data_datetime_end,
                    auto_adjust=self._auto_adjust,
                )
                end = self._option_request_end(
                    [a for _k, a, _q in chunk], request.start, self._data_datetime_end + timedelta(days=1)
                )
                # Same "no later than 16 minutes ago" limit as a single request.
                latest_allowed = datetime.now(pytz.UTC) - timedelta(minutes=16)
                if pd.Timestamp(end) > pd.Timestamp(latest_allowed):
                    end = max(latest_allowed, covered_until.to_pydatetime() + timedelta(minutes=1))
                request = request.model_copy(update={
                    "symbol_or_symbols": sorted(self._occ_symbol(a) for _k, a, _q in chunk),
                    "end": pd.Timestamp(end).to_pydatetime(),
                })
                what = f"{len(chunk)} option contracts after {covered_until.date()}"
                for key, _a, _q in chunk:
                    covered.pop(key, None)
                listed = self._listed_occ_symbols([a for _k, a, _q in chunk])
                try:
                    bars = fetch_alpaca_bars(
                        client, request,
                        lambda query: self._alpaca_request(client.get_option_bars, query, what=what),
                        refresh=getattr(self, "_refresh_cache", False), empty_ok=listed.__contains__,
                    )
                except Exception as e:
                    for key, _a, _q in chunk:
                        covered[key] = covered_until
                    if isinstance(e, AlpacaOptionsDataPlanError):
                        raise
                    raise RuntimeError(f"Failed to fetch data for {what}: {e}")
                by_symbol = self._rows_by_symbol(bars.df)
                for key, asset, quote in chunk:
                    later = by_symbol.get(self._occ_symbol(asset))
                    self._apply_extension(key, asset, quote, timestep, store.get(key), later)

    def _store_option_rows(self, key: str, asset: Asset, rows: pd.DataFrame | None,
                           covered_until: pd.Timestamp | None = None,
                           loaded_from: pd.Timestamp | None = None) -> None:
        """Store one contract's prefetched rows the way a single download stores them.

        ``loaded_from`` marks rows loaded only from that time on: earlier prints are loaded
        by ``_backfill_option_frame`` the first time a lookup needs them.
        """
        store = self._state("_data_store", dict)
        if loaded_from is not None:
            self._state("_option_loaded_from", dict)[key] = loaded_from
            if covered_until is None:
                frame = self._finish_option_frame(
                    key, rows if rows is not None else pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"]),
                    self.tzinfo, self._data_datetime_start, self._data_datetime_end, write_csv=False,
                )
                store[key] = frame
                return
        if covered_until is not None:
            # Prints only up to covered_until: no print yet is not "never traded".
            self._state("_option_covered_until", dict)[key] = covered_until
            frame = self._finish_option_frame(
                key, rows if rows is not None else pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"]),
                self.tzinfo, self._data_datetime_start, covered_until, write_csv=False,
            )
            store[key] = frame
            return
        if rows is None or rows.empty:
            logger.error(
                "%s",
                AlpacaOptionHistoryUnavailable(
                    f"Alpaca returned no option bars for {self._occ_symbol(asset)} between "
                    f"{self._data_datetime_start.date()} and {self._data_datetime_end.date()}. "
                    f"{self.OPTION_HISTORY_START_HINT}, and a contract that never traded in the window "
                    "has no bars. Its price is None for this backtest."
                ),
            )
            empty = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
            empty.index = pd.DatetimeIndex([], tz=self.tzinfo, name="timestamp")
            store[key] = empty
            self._state("_refreshed_keys", dict)[key] = True
            return
        store[key] = self._finish_option_frame(key, rows, self.tzinfo, self._data_datetime_start, self._data_datetime_end)

    def _get_option_bars_frame(self, asset: Asset, quote: Asset, timestep: str) -> pd.DataFrame:
        """Real option trade bars for the whole backtest window, or an empty frame.

        ``_partial_frame_ok`` set (by the last-price fast path): the caller only reads the newest print at or before now and falls
        back to this method without it when there is none, so rows loaded from the pick month
        on are enough (earlier prints are older). Everyone else gets the whole window.
        """
        asset, quote = self._sanitize_base_and_quote_asset(asset, quote)
        key = self._get_asset_key(base_asset=asset, quote_asset=quote, timestep=timestep)
        store = self._state("_data_store", dict)
        if key not in store and self._first_load_from_lookup_month(asset):
            # First read of a single contract: load it from the month a week before now (as a
            # pick does) through its expiry, not the whole window from the backtest start. In a
            # long backtest that whole-window read planned one shared-cache partition per
            # elapsed month, nearly all from before the contract was listed. Earlier prints are
            # still loaded the first time a lookup needs them, so every answer is the same.
            self.prefetch_option_bars([asset], quote, timestep, until=self._datetime, through_window_end=True)
        covered = self._state("_option_covered_until", dict)
        if key in store and key in covered and pd.Timestamp(self._datetime) > covered[key]:
            self._extend_option_frame(key, asset, quote, timestep)
        partial_ok = self.__dict__.get("_partial_frame_ok", False)
        if not partial_ok and key in store and key in self._state("_option_loaded_from", dict):
            self._backfill_option_frame(key, asset, quote, timestep)
        if key in store:
            return store[key]
        try:
            return self.get_historical_prices_between_dates(base_asset=asset, quote_asset=quote, timestep=timestep)
        except AlpacaOptionHistoryUnavailable as exc:
            # Missing is honest; remember it for the run so every bar does not re-ask Alpaca.
            logger.error("%s", exc)
            empty = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
            empty.index = pd.DatetimeIndex([], tz=self.tzinfo, name="timestamp")
            store[key] = empty
            self._state("_refreshed_keys", dict)[key] = True
            return empty

    # First single-contract reads load from the lookup month (see ``_get_option_bars_frame``).
    # Class attribute so a test can compare against the whole-window read.
    OPTION_FIRST_LOAD_FROM_LOOKUP_MONTH = True

    def _first_load_from_lookup_month(self, asset) -> bool:
        """True when a first read of ``asset`` should start at the lookup month."""
        if not self.OPTION_FIRST_LOAD_FROM_LOOKUP_MONTH or not self._is_option(asset):
            return False
        if getattr(asset, "expiration", None) is None or getattr(self, "_datetime", None) is None:
            return False
        return getattr(self._datetime, "tzinfo", None) is not None

    def _extend_option_frame(self, key: str, asset: Asset, quote: Asset, timestep: str) -> None:
        """Load the rest of the window for a contract prefetched only up to an earlier day."""
        covered = self._state("_option_covered_until", dict)
        covered_until = covered.pop(key)
        store = self._state("_data_store", dict)
        loaded = store.get(key)
        client, request = self._history_request(
            base_asset=asset, quote_asset=quote, timestep=timestep,
            data_datetime_start=covered_until.to_pydatetime(), data_datetime_end=self._data_datetime_end,
            auto_adjust=self._auto_adjust,
        )
        what = f"{self._occ_symbol(asset)} after {covered_until.date()}"
        listed = self._listed_occ_symbols([asset])
        try:
            bars = fetch_alpaca_bars(
                client, request,
                lambda query: self._alpaca_request(client.get_option_bars, query, what=what),
                refresh=getattr(self, "_refresh_cache", False), empty_ok=listed.__contains__,
            )
        except Exception as e:
            covered[key] = covered_until
            if isinstance(e, AlpacaOptionsDataPlanError):
                raise
            raise RuntimeError(f"Failed to fetch data for {what}: {e}")
        frame = bars.df
        occ = self._occ_symbol(asset)
        later = frame.xs(occ, level=0).reset_index() if not frame.empty and occ in set(frame.index.get_level_values(0)) else None
        self._apply_extension(key, asset, quote, timestep, loaded, later)

    def _apply_extension(self, key, asset, quote, timestep, loaded, later) -> None:
        """Merge a contract's rows after its covered day into its stored frame."""
        pieces = [loaded.reset_index()] if loaded is not None and not loaded.empty else []
        if later is not None and not later.empty:
            pieces.append(later)
        partial = key in self._state("_option_loaded_from", dict)
        if not pieces and partial:
            # Nothing since the pick month: whether it traded earlier decides the answer.
            self._backfill_option_frame(key, asset, quote, timestep)
            return
        if not pieces:
            self._store_option_rows(key, asset, None)
            return
        merged = pd.concat(pieces, ignore_index=True)
        merged['timestamp'] = pd.to_datetime(merged['timestamp'], utc=True)
        merged = merged.drop_duplicates(subset="timestamp", keep="last")
        # Keep any prints from before the window that a history reach-back already added.
        start = min(pd.Timestamp(self._data_datetime_start), merged['timestamp'].min().tz_convert(self.tzinfo))
        self._finish_option_frame(key, merged, self.tzinfo, start, self._data_datetime_end, write_csv=not partial)

    def _backfill_option_frame(self, key: str, asset: Asset, quote: Asset, timestep: str) -> None:
        """Load the prints before the pick month for a contract a pick loaded from that month."""
        loaded_from = self._state("_option_loaded_from", dict).pop(key)
        store = self._state("_data_store", dict)
        loaded = store.get(key)
        client, request = self._history_request(
            base_asset=asset, quote_asset=quote, timestep=timestep,
            data_datetime_start=self._data_datetime_start, data_datetime_end=self._data_datetime_end,
            auto_adjust=self._auto_adjust, request_end=loaded_from.to_pydatetime(),
        )
        what = f"{self._occ_symbol(asset)} before {loaded_from.date()}"
        listed = self._listed_occ_symbols([asset])
        try:
            bars = fetch_alpaca_bars(
                client, request,
                lambda query: self._alpaca_request(client.get_option_bars, query, what=what),
                refresh=getattr(self, "_refresh_cache", False), empty_ok=listed.__contains__,
            )
        except Exception as e:
            self._state("_option_loaded_from", dict)[key] = loaded_from
            if isinstance(e, AlpacaOptionsDataPlanError):
                raise
            raise RuntimeError(f"Failed to fetch data for {what}: {e}")
        frame = bars.df
        occ = self._occ_symbol(asset)
        earlier = frame.xs(occ, level=0).reset_index() if not frame.empty and occ in set(frame.index.get_level_values(0)) else None
        pieces = []
        if earlier is not None and not earlier.empty:
            pieces.append(earlier)
        if loaded is not None and not loaded.empty:
            pieces.append(loaded.reset_index())
        covered = self._state("_option_covered_until", dict)
        if not pieces:
            if key in covered:
                # No print yet, and the rest of the window is still to be loaded.
                self._store_option_rows(key, asset, None, covered_until=covered[key])
            else:
                self._store_option_rows(key, asset, None)
            return
        merged = pd.concat(pieces, ignore_index=True)
        merged['timestamp'] = pd.to_datetime(merged['timestamp'], utc=True)
        merged = merged.drop_duplicates(subset="timestamp", keep="last")
        start = min(pd.Timestamp(self._data_datetime_start), merged['timestamp'].min().tz_convert(self.tzinfo))
        end = covered.get(key, self._data_datetime_end)
        self._finish_option_frame(key, merged, self.tzinfo, start, end, write_csv=key not in covered)

    def _get_option_historical_prices(
            self,
            asset: Asset,
            length: int,
            *,
            timestep: str,
            source_timestep: str,
            resample_rule: str | None,
            timeshift: timedelta | None,
            quote: Asset,
            return_polars: bool,
            remove_incomplete_current_bar: bool,
            extend_history: bool = True,
    ) -> Bars | None:
        search_datetime = self._datetime - timeshift if timeshift else self._datetime
        df = None
        if resample_rule is None:
            # A contract a pick loaded only from a recent month: when that part already holds
            # ``length`` bars at or before now, they are exactly the newest ``length`` bars of
            # the whole window (earlier prints are older), so the earlier months are not read.
            self.__dict__["_partial_frame_ok"] = True
            try:
                partial = self._get_option_bars_frame(asset, quote, source_timestep)
            finally:
                self.__dict__["_partial_frame_ok"] = False
            sanitized, sanitized_quote = self._sanitize_base_and_quote_asset(asset, quote)
            key = self._get_asset_key(base_asset=sanitized, quote_asset=sanitized_quote, timestep=source_timestep)
            if key not in self._state("_option_loaded_from", dict):
                df = partial
            elif partial is not None and not partial.empty and self._newest_bar_position(
                partial.index, search_datetime, timestep, remove_incomplete_current_bar,
            ) + 1 >= length:
                df = partial
        if df is None:
            df = self._get_option_bars_frame(asset, quote, source_timestep)
        if extend_history and getattr(self, "_history_before_start", False):
            # Prints from before the window (real trades only) when the request needs more
            # finished bars than the window holds at this time.
            view = self._resample_ohlcv_dataframe(df, resample_rule) if resample_rule is not None else df
            available = self._newest_bar_position(
                view.index,
                search_datetime,
                timestep,
                remove_incomplete_current_bar,
            ) + 1
            if available < length:
                extended = self._reach_back_for_history(
                    asset=asset,
                    quote=quote,
                    source_timestep=source_timestep,
                    frame=df,
                    search_datetime=search_datetime,
                    length=length,
                    timestep=timestep,
                )
                if extended is not None:
                    df = extended
        if df is None or df.empty:
            return None
        if resample_rule is not None:
            df = self._resample_ohlcv_dataframe(df, resample_rule)
            if df.empty:
                return None

        current_index = self._newest_bar_position(
            df.index,
            search_datetime,
            timestep,
            remove_incomplete_current_bar,
        )

        if current_index < 0:
            # No trade printed (or finished) yet. Never back-fill a later price into the past.
            return None

        result_df = df.iloc[max(0, current_index - length + 1): current_index + 1]
        return Bars(
            result_df,
            self.SOURCE,
            asset=asset,
            quote=quote,
            return_polars=return_polars,
            tzinfo=self.tzinfo,
        )

    def quotes_have_bid_ask(self, asset) -> bool:
        """False: quotes from this source never carry a bid or ask (see ``get_quote``).

        The broker skips a bid/ask check that can only come back empty. An asset routed to
        another data source keeps the check, since that source may have quotes.
        """
        return self._other_source_for(asset) is not None

    def get_quote(self, asset, quote=None, exchange=None, snapshot_only=False, **kwargs):
        """Quote at the simulated time built from real trades.

        Alpaca historical bars are trades, not NBBO quotes, so ``bid`` and ``ask`` stay ``None``
        and ``price`` is the same last real trade ``get_last_price`` returns. Option selection
        helpers read marks through this method; without it every expiration probe fails.
        """
        from lumibot.entities import Quote

        price = self.get_last_price(asset, quote=quote, exchange=exchange)
        return Quote(
            asset=asset,
            price=float(price) if price is not None else None,
            timestamp=self._datetime,
            raw_data={"source": "alpaca_trade_bars"},
        )

    def _option_mark_arrays(self, frame: pd.DataFrame, timestep: str):
        """(UTC nanosecond stamps, opens, closes) of a stored option frame, built once per frame.

        A minute strategy marks every held leg on every bar. Reading a pandas column and
        searching a DatetimeIndex per call cost about 0.1 ms; these arrays give the same row
        with one searchsorted. Rebuilt whenever the stored frame object changes (an extension
        or backfill stores a new frame). Only for minute bars on a sorted, tz-aware index;
        anything else returns None and the caller keeps the pandas path.
        """
        if timestep != "minute":
            return None
        cache = self._state("_option_mark_cache", dict)
        entry = cache.get(id(frame))
        if entry is not None and entry[0] is frame:
            return entry[1]
        index = frame.index
        if not isinstance(index, pd.DatetimeIndex) or index.tz is None or not index.is_monotonic_increasing:
            return None
        if "open" not in frame.columns or "close" not in frame.columns:
            return None
        marks = (index.asi8, frame["open"].to_numpy(), frame["close"].to_numpy())
        if len(cache) > 4096:
            cache.clear()
        cache[id(frame)] = (frame, marks)
        return marks

    def recent_bar_arrays(self, asset, length: int, quote: Asset | None = None, timeshift=None,
                          remove_incomplete_current_bar: Optional[bool] = None, _extend_history: bool = True):
        """(UTC nanosecond stamps, opens, closes) of the rows ``get_historical_prices`` returns.

        Same arguments and same rows as ``get_historical_prices(asset, length, quote=quote,
        timeshift=timeshift, ...)`` with the source's own bar size, without building a Bars
        object. The broker values held option legs on every bar and only reads the newest
        trades' times and prices. Returns None whenever the answer needs the full history
        path (a stock, another bar size, a shift, a reach before the window, not enough rows
        in the loaded part, no row yet): the caller then uses ``get_historical_prices``.
        """
        if length is None or int(length) <= 0 or timeshift is not None or not self._is_option(asset):
            return None
        length = int(length)
        timestep = self._timestep
        if self._other_source_for(asset) is not None:
            return None
        source_timestep, resample_rule = self._normalize_timestep_for_source(timestep)
        if resample_rule is not None or source_timestep != "minute" or timestep != source_timestep:
            return None
        now = getattr(self, "_datetime", None)
        if now is None or getattr(now, "tzinfo", None) is None:
            return None
        if remove_incomplete_current_bar is None:
            remove_incomplete_current_bar = self._remove_incomplete_current_bar
        if quote is None:
            quote = self.LUMIBOT_DEFAULT_QUOTE_ASSET
        # The same first read as _get_option_historical_prices (a loaded month is enough when
        # it holds ``length`` rows at or before now).
        self.__dict__["_partial_frame_ok"] = True
        try:
            frame = self._get_option_bars_frame(asset, quote, source_timestep)
        finally:
            self.__dict__["_partial_frame_ok"] = False
        if frame is None or frame.empty:
            return None
        sanitized, sanitized_quote = self._sanitize_base_and_quote_asset(asset, quote)
        key = self._get_asset_key(base_asset=sanitized, quote_asset=sanitized_quote, timestep=source_timestep)
        position = self._newest_bar_position(frame.index, now, timestep, remove_incomplete_current_bar)
        if position < 0:
            return None
        if position + 1 < length and (
            key in self._state("_option_loaded_from", dict)
            or (_extend_history and getattr(self, "_history_before_start", False))
        ):
            return None  # earlier prints or prints before the window decide the answer
        marks = self._option_mark_arrays(frame, source_timestep)
        if marks is None:
            return None
        stamps, opens, closes = marks
        first = max(0, position - length + 1)
        return stamps[first:position + 1], opens[first:position + 1], closes[first:position + 1]

    def _get_option_last_price(self, asset: Asset, quote: Asset) -> float | Decimal | None:
        timestep = self.__dict__.get("_timestep") or "minute"
        if timestep != "day":
            # PERF: a minute strategy asks this on every bar for every contract it watches.
            # When a real print exists at or before now, the answer is one row of the stored
            # frame; building a Bars object per call cost about 1 ms and dominated warm option
            # backtests. No print yet falls through to the full path below, which may reach
            # back before the window start.
            self.__dict__["_partial_frame_ok"] = True
            try:
                frame = self._get_option_bars_frame(asset, quote, timestep)
            finally:
                self.__dict__["_partial_frame_ok"] = False
            if frame is not None and not frame.empty:
                marks = self._option_mark_arrays(frame, timestep)
                if marks is not None and getattr(self._datetime, "tzinfo", None) is None:
                    marks = None  # naive clock: keep the pandas comparison
                if marks is not None:
                    stamps, opens, closes = marks
                    now_ns = pd.Timestamp(self._datetime).value
                    position = int(stamps.searchsorted(now_ns, side="right")) - 1
                else:
                    position = self._newest_bar_position(frame.index, self._datetime, timestep, False)
                if position >= 0:
                    if marks is not None:
                        # Same bar, same rule: the open of a bar that printed this minute,
                        # otherwise the close of the latest earlier print.
                        printed_now = stamps[position] // 60_000_000_000 == now_ns // 60_000_000_000
                        price = opens[position] if printed_now else closes[position]
                    else:
                        bar_ts = pd.Timestamp(frame.index[position])
                        printed_now = bar_ts.floor("min") == pd.Timestamp(self._datetime).floor("min")
                        price = frame["open"].iat[position] if printed_now else frame["close"].iat[position]
                    if price is None or pd.isna(price):
                        return None
                    return quantize_to_num_decimals(price, get_decimals(price))
        # On the first bars of the window the latest real trade can be from before the start.
        bars = self._get_option_historical_prices(
            asset,
            1,
            extend_history=True,
            timestep=timestep,
            source_timestep=timestep,
            resample_rule=None,
            timeshift=None,
            quote=quote,
            return_polars=False,
            remove_incomplete_current_bar=False,
        )
        if bars is None or bars.df.empty:
            return None
        df_local = bars.df
        if timestep == "day":
            return self._daily_bar_mark(df_local, asset, quote)
        bar_ts = pd.Timestamp(df_local.index[-1])
        now = pd.Timestamp(self._datetime)
        printed_now = bar_ts.floor("min") == now.floor("min")
        # A bar that printed now: its open, which is the fill price the broker uses.
        # Otherwise the close of the latest earlier print is the last real trade.
        price = df_local["open"].iloc[-1] if printed_now else df_local["close"].iloc[-1]
        if price is None or pd.isna(price):
            return None
        num_decimals = get_decimals(price)
        return quantize_to_num_decimals(price, num_decimals)

    def _get_asset_key(
            self,
            *,
            base_asset: Asset,
            quote_asset: Asset,
            timestep: str = None,
            market: str = None,
            tzinfo: pytz.tzinfo = None,
            data_datetime_start: datetime = None,
            data_datetime_end: datetime = None,
            auto_adjust: bool = None,
    ) -> str:
        """
        Generate a unique key for an asset combination with specific parameters.

        Parameters
        ----------
        base_asset: Asset - Base asset of the pair.
        quote_asset: Asset - Quote asset of the pair.
        market: str - Market or exchange identifier.
        tzinfo: pytz.tzinfo - Timezone information.
        timestep: str - Timestep of the source data. Accepts "day" or "minute".
        data_datetime_start: datetime - The start date of the data in the backtest.
        data_datetime_end: datetime - The end date of the data in the backtest. Inclusive.
        auto_adjust: bool - Flag to indicate if auto-adjustment is applied.

        Returns
        -------
        str - A unique key string.
        """

        if base_asset is None:
            raise ValueError("Base asset must be provided.")

        # PERF: held legs ask for their key on every bar. With no overrides the key depends only
        # on the asset, quote, timestep and this source's fixed settings, so remember it.
        memo = None
        if market is None and tzinfo is None and data_datetime_start is None and data_datetime_end is None \
                and auto_adjust is None:
            memo = (base_asset, quote_asset, timestep, self.__dict__.get("_timestep"), self.__dict__.get("market"),
                    self.__dict__.get("_data_datetime_start"), self.__dict__.get("_data_datetime_end"),
                    self.__dict__.get("_auto_adjust"), str(self.__dict__.get("tzinfo")))
            known = self._state("_asset_key_memo", dict).get(memo)
            if known is not None:
                return known
        key = self._build_asset_key(base_asset=base_asset, quote_asset=quote_asset, timestep=timestep,
                                    market=market, tzinfo=tzinfo, data_datetime_start=data_datetime_start,
                                    data_datetime_end=data_datetime_end, auto_adjust=auto_adjust)
        if memo is not None:
            memos = self._state("_asset_key_memo", dict)
            if len(memos) > 100_000:
                memos.clear()
            memos[memo] = key
        return key

    def _build_asset_key(self, *, base_asset, quote_asset, timestep=None, market=None, tzinfo=None,
                         data_datetime_start=None, data_datetime_end=None, auto_adjust=None) -> str:
        if base_asset is None:
            raise ValueError("Base asset must be provided.")

        if quote_asset is None:
            quote_asset = self.LUMIBOT_DEFAULT_QUOTE_ASSET

        if market is None:
            market = self.market

        if data_datetime_start is None:
            data_datetime_start = self._data_datetime_start

        if data_datetime_end is None:
            data_datetime_end = self._data_datetime_end

        if tzinfo is None:
            tzinfo = self.tzinfo

        if auto_adjust is None:
            auto_adjust = self._auto_adjust

        if timestep is None:
            timestep = self._timestep

        timestep, _ = self._normalize_timestep_for_source(timestep)

        base_quote = f"{base_asset.symbol}-{base_asset.asset_type}_{quote_asset.symbol}-{quote_asset.asset_type}"
        if str(getattr(base_asset, "asset_type", "")).lower() == "option":
            expiration = getattr(base_asset, "expiration", None)
            expiration_text = expiration.isoformat() if hasattr(expiration, "isoformat") else str(expiration)
            strike = float(base_asset.strike)
            strike_text = str(int(strike)) if strike.is_integer() else str(strike)
            right = str(getattr(base_asset, "right", "") or "")
            # "TRADES": option files hold only real trade prints. Older option cache files
            # were reindexed and forward-filled like stock bars, so they must not be reused.
            base_quote = f"{base_quote}_{strike_text}_{right}_{expiration_text}_TRADES"
        market = market
        tzinfo_str = str(tzinfo).replace("_", "-")
        start_date_str = data_datetime_start.strftime("%Y-%m-%d")
        end_date_str = data_datetime_end.strftime("%Y-%m-%d")
        auto_adjust_str = "AA" if auto_adjust else ""

        key_parts = [
            base_quote, market, timestep, tzinfo_str,
            auto_adjust_str, start_date_str, end_date_str
        ]
        key = "_".join(part for part in key_parts if part).upper()
        key = key.replace("/", "-")
        if str(getattr(base_asset, "asset_type", "")).lower() == "option" and timestep == "minute":
            # Remember which contract a minute option key belongs to, so the broker's estimate
            # can read the contracts already loaded (loaded_option_minute_frames).
            self._state("_option_key_assets", dict)[key] = base_asset
        return key

    def loaded_option_minute_frames(self, asset) -> list:
        """(contract, minute trade frame) pairs already in memory for ``asset``'s underlying and expiration.

        Never fetches. Strike searches (delta or price picks) load many contracts of one
        expiration; the broker's option estimate fits a volatility smile across them instead of
        relying on one contract's own last trade. Contracts with no stored prints are left out.
        Frames hold real trade prints only; the broker reads only bars before the current bar.
        """
        expiration = getattr(asset, "expiration", None)
        symbol = getattr(asset, "symbol", None)
        if expiration is None or symbol is None:
            return []
        store = self.__dict__.get("_data_store") or {}
        owners = self.__dict__.get("_option_key_assets") or {}
        out = []
        for key, contract in list(owners.items()):
            if contract.symbol != symbol or contract.expiration != expiration:
                continue
            frame = store.get(key)
            if isinstance(frame, pd.DataFrame) and not frame.empty and "close" in frame.columns:
                out.append((contract, frame))
        return out

    def _occ_symbol(self, asset: Asset) -> str:
        """Build the Alpaca OCC symbol for one listed option (index weeklies use their own root)."""
        strike_formatted = f"{float(asset.strike):08.3f}".replace(".", "").rjust(8, "0")
        expiration = asset.expiration
        if hasattr(expiration, "strftime"):
            date = expiration.strftime("%y%m%d")
        else:
            date = datetime.strptime(str(expiration)[:10], "%Y-%m-%d").strftime("%y%m%d")
        right = str(asset.right or "C")[0].upper()
        return f"{self._alpaca_option_root(asset)}{date}{right}{strike_formatted}"

    @staticmethod
    def _option_request_end(assets, start, end):
        """``end``, but no later than midnight New York after the latest expiry among ``assets``.

        A contract has no prints after it expires. Requests used to run to the end of the
        backtest, so in a long run every read of a short-dated contract planned shared-cache
        partitions (S3 reads and provider gaps) for each month after its expiry.
        """
        expiries = []
        for asset in assets:
            expiration = getattr(asset, "expiration", None)
            if isinstance(expiration, datetime):
                expiration = expiration.date()
            if expiration is None:
                return end  # unknown expiry: keep the full request
            expiries.append(expiration)
        if not expiries or end is None:
            return end
        limit = pytz.timezone("America/New_York").localize(
            datetime.combine(max(expiries) + timedelta(days=1), datetime.min.time())
        )
        end_ts = pd.Timestamp(end)
        end_aware = end_ts.tz_localize("UTC") if end_ts.tzinfo is None else end_ts
        if end_aware <= pd.Timestamp(limit):
            return end
        clamped = limit
        if start is not None:
            start_ts = pd.Timestamp(start)
            start_aware = start_ts.tz_localize("UTC") if start_ts.tzinfo is None else start_ts
            floor = (start_aware + pd.Timedelta(minutes=1)).to_pydatetime()
            if pd.Timestamp(clamped) < pd.Timestamp(floor):
                clamped = floor
        if end_ts.tzinfo is None:
            return pd.Timestamp(clamped).tz_convert("UTC").tz_localize(None).to_pydatetime()
        return clamped

    def _history_request(
            self,
            *,
            base_asset: Asset,
            quote_asset: Asset,
            timestep: str,
            data_datetime_start: datetime,
            data_datetime_end: datetime,
            auto_adjust: bool,
            request_end: datetime | None = None,
            adjustment: str | None = None,
    ):
        """Return the Alpaca client and bar request for this asset.

        ``request_end`` replaces the default end (``data_datetime_end`` plus one day), for a
        history segment that must stop where the loaded window begins. ``adjustment``
        overrides the stock price adjustment (``"raw"`` for prices as traded).
        """
        end = request_end if request_end is not None else data_datetime_end + timedelta(days=1)
        # A window that reaches today (for example an end clamped to "now") used to ask for
        # tomorrow. Free keys refuse the latest 15 minutes of SIP data ("subscription does not
        # permit querying recent SIP data") and there are no bars after now anyway.
        latest_allowed = datetime.now(pytz.UTC) - timedelta(minutes=16)
        if end > latest_allowed:
            end = max(latest_allowed, data_datetime_start + timedelta(minutes=1))
        timeframe = self._get_alpaca_timeframe(timestep)
        asset_type = str(getattr(base_asset, "asset_type", "")).lower()
        if asset_type == "crypto":
            request = CryptoBarsRequest(
                symbol_or_symbols=f"{base_asset.symbol}/{quote_asset.symbol}",
                timeframe=timeframe,
                start=data_datetime_start,
                end=end,
            )
            return self._crypto_client, request
        if asset_type == "option":
            request = OptionBarsRequest(
                symbol_or_symbols=self._occ_symbol(base_asset),
                timeframe=timeframe,
                start=data_datetime_start,
                end=self._option_request_end([base_asset], data_datetime_start, end),
            )
            return self._option_client, request
        request = StockBarsRequest(
            symbol_or_symbols=base_asset.symbol,
            timeframe=timeframe,
            start=data_datetime_start,
            end=end,
            adjustment=adjustment or ("all" if auto_adjust else "split"),
        )
        return self._stock_client, request

    def _download_and_cache_ohlcv_data(
            self,
            *,
            base_asset: Asset = None,
            quote_asset: Asset = None,
            timestep: str = None,
            market: str = None,
            tzinfo: pytz.tzinfo = None,
            data_datetime_start: datetime = None,
            data_datetime_end: datetime = None,
            auto_adjust: bool = None,
    ) -> pd.DataFrame:
        if base_asset is None:
            raise ValueError("The parameter 'base_asset' cannot be None.")
        if quote_asset is None:
            raise ValueError("The parameter 'quote_asset' cannot be None.")
        if timestep is None:
            raise ValueError("The parameter 'timestep' cannot be None.")
        if market is None:
            raise ValueError("The parameter 'market' cannot be None.")
        if tzinfo is None:
            raise ValueError("The parameter 'tzinfo' cannot be None.")
        if data_datetime_start is None:
            raise ValueError("The parameter 'data_datetime_start' cannot be None.")
        if data_datetime_end is None:
            raise ValueError("The parameter 'data_datetime_end' cannot be None.")
        if auto_adjust is None:
            raise ValueError("The parameter 'auto_adjust' cannot be None.")

        key = self._get_asset_key(
            base_asset=base_asset,
            quote_asset=quote_asset,
            timestep=timestep,
            market=market,
            tzinfo=tzinfo,
            data_datetime_start=data_datetime_start,
            data_datetime_end=data_datetime_end,
            auto_adjust=auto_adjust,
        )

        # Directory to save cached data.
        cache_dir = os.path.join(LUMIBOT_CACHE_FOLDER, self.CACHE_SUBFOLDER)
        os.makedirs(cache_dir, exist_ok=True)

        # File path based on the unique key
        filename = f"{key}.csv"
        filepath = os.path.join(cache_dir, filename)

        logger.info(f"Fetching and caching data for {key}")

        client, request_params = self._history_request(
            base_asset=base_asset,
            quote_asset=quote_asset,
            timestep=timestep,
            data_datetime_start=data_datetime_start,
            data_datetime_end=data_datetime_end,
            auto_adjust=auto_adjust,
        )

        is_option = self._is_option(base_asset)
        try:
            if isinstance(request_params, CryptoBarsRequest):
                bars = fetch_alpaca_bars(
                    client, request_params,
                    lambda query: self._alpaca_request(client.get_crypto_bars, query, what=key),
                    refresh=getattr(self, "_refresh_cache", False),
                )
            elif isinstance(request_params, OptionBarsRequest):
                listed = self._listed_occ_symbols([base_asset])
                bars = fetch_alpaca_bars(
                    client, request_params,
                    lambda query: self._alpaca_request(client.get_option_bars, query, what=key),
                    refresh=getattr(self, "_refresh_cache", False), empty_ok=listed.__contains__,
                )
            else:
                bars = fetch_alpaca_bars(
                    client, request_params,
                    lambda query: self._alpaca_request(client.get_stock_bars, query, what=key),
                    refresh=getattr(self, "_refresh_cache", False),
                )
        except AlpacaOptionsDataPlanError:
            raise
        except Exception as e:
            raise RuntimeError(f"Failed to fetch data for {key}: {e}")

        df = bars.df.reset_index()
        if df.empty:
            if is_option:
                raise AlpacaOptionHistoryUnavailable(
                    f"Alpaca returned no option bars for {self._occ_symbol(base_asset)} between "
                    f"{data_datetime_start.date()} and {data_datetime_end.date()}. "
                    f"{self.OPTION_HISTORY_START_HINT}, and a contract that never traded in the window "
                    "has no bars. Its price is None for this backtest."
                )
            raise RuntimeError(f"No data fetched for {key}.")

        if is_option:
            # Ensure 'timestamp' is a pandas timestamp object
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            if df['timestamp'].dt.tz is None:
                df['timestamp'] = df['timestamp'].dt.tz_localize(tzinfo)
            else:
                df['timestamp'] = df['timestamp'].dt.tz_convert(tzinfo)
            return self._finish_option_frame(key, df, tzinfo, data_datetime_start, data_datetime_end)

        return self._finish_window_frame(key, df, timestep, tzinfo, data_datetime_start, data_datetime_end)

    def _finish_window_frame(self, key, df, timestep, tzinfo, data_datetime_start, data_datetime_end) -> pd.DataFrame:
        """One stock or crypto symbol's window rows: align to the calendar, cache to disk, store.

        Shared by the single download and the grouped download (``_prefetch_stock_bars``) so
        both write exactly the same file and frame for a symbol.
        """
        # Ensure 'timestamp' is a pandas timestamp object
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        if df['timestamp'].dt.tz is None:
            df['timestamp'] = df['timestamp'].dt.tz_localize(tzinfo)
        else:
            df['timestamp'] = df['timestamp'].dt.tz_convert(tzinfo)

        # A real copy: the fills below then write to this frame, not to a view of the
        # provider's rows (pandas logged several SettingWithCopyWarning lines per symbol).
        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()

        if timestep in ("day", "minute"):
            trading_times = self._get_trading_times_for_timestep(
                pcal=self._trading_days,
                timestep=timestep,
            )

            # Reindex the dataframe with a row for each bar we should have a trading iteration for.
            # Fill any empty bars with previous data.
            df = self._reindex_and_fill(df=df, trading_times=trading_times, timestep=timestep)
        else:
            # Alpaca aligns native intraday multiples on provider-specific boundaries
            # such as 09:40 for 20Min NYSE bars. Preserve those native timestamps.
            df = df.sort_values("timestamp")

        # Filter data to include only rows between data_datetime_start and data_datetime_end
        df = df[(df['timestamp'] >= data_datetime_start) & (df['timestamp'] <= data_datetime_end)]

        # Save to cache
        cache_dir = os.path.join(LUMIBOT_CACHE_FOLDER, self.CACHE_SUBFOLDER)
        os.makedirs(cache_dir, exist_ok=True)
        df.to_csv(os.path.join(cache_dir, f"{key}.csv"), index=False)

        # Store in _data_store
        df = df.set_index('timestamp')
        self._data_store[key] = df
        logger.info(f"Finished fetching and caching data for {key}")
        return df

    # ------------------------------------------------------------------
    # Grouped stock history (get_historical_prices_for_assets)
    # ------------------------------------------------------------------
    ALPACA_STOCK_SYMBOLS_PER_REQUEST = 100

    def get_bars(self, assets, length, timestep="minute", timeshift=None, chunk_size=2, max_workers=2,
                 quote=None, exchange=None, include_after_hours=True, sleep_time=None):
        """History for many assets. Stock bars that are not loaded yet are downloaded in grouped
        requests (up to ``ALPACA_STOCK_SYMBOLS_PER_REQUEST`` symbols each) before the per-asset
        lookups run, so a scanner over N stocks costs about N/100 requests instead of N.
        The bars, cache files and errors are the same as asking for each asset by itself.
        """
        try:
            self._prefetch_stock_history(assets, length, timestep, timeshift, quote)
        except Exception as exc:
            # The grouped download is only a shortcut: anything it could not load is fetched
            # (and reported) per asset below, exactly as before.
            logger.debug("Grouped Alpaca stock history download skipped: %s", exc)
        return super().get_bars(assets, length, timestep=timestep, timeshift=timeshift, chunk_size=chunk_size,
                                max_workers=max_workers, quote=quote, exchange=exchange,
                                include_after_hours=include_after_hours, sleep_time=sleep_time)

    # ------------------------------------------------------------------
    # As-traded closes (what a share cost that day, before any adjustment)
    # ------------------------------------------------------------------
    def prefetch_backtest_as_traded_daily_closes(self, assets) -> None:
        """Load the daily closes ``get_backtest_as_traded_daily_closes`` needs for many stocks.

        Grouped requests (``ALPACA_STOCK_SYMBOLS_PER_REQUEST`` symbols each) instead of two
        requests per stock. Anything this does not load is loaded per stock when it is read.
        """
        stocks, seen = [], set()
        for asset in assets or []:
            if isinstance(asset, str):
                asset = Asset(symbol=asset)
            if str(getattr(asset, "asset_type", "")).lower() != "stock" or asset.symbol in seen:
                continue
            seen.add(asset.symbol)
            stocks.append(asset)
        if len(stocks) < 2:
            return
        try:
            self._prefetch_stock_history(stocks, 1, "day", None, None)
        except Exception as exc:
            logger.debug("Grouped Alpaca stock history download skipped: %s", exc)
        closes = self._state("_as_traded_daily_closes", dict)
        wanted = [asset for asset in stocks if asset.symbol not in closes]
        if not wanted:
            return
        rows = self._stock_bars_in_groups(
            [(asset, self.LUMIBOT_DEFAULT_QUOTE_ASSET) for asset in wanted], "day",
            self._data_datetime_start, self._data_datetime_end,
            refresh=bool(getattr(self, "_refresh_cache", False)), adjustment="raw",
        )
        for asset in wanted:
            closes[asset.symbol] = self._closes_by_session(rows.get(asset.symbol))
        logger.info("Loaded as-traded daily closes for %d stock symbols", len(wanted))

    def get_backtest_as_traded_daily_closes(self, asset, quote=None):
        """Daily ``close`` (as this backtest prices it) and ``as_traded_close`` for a stock, or None.

        Alpaca backtest prices are adjusted for splits, and with ``auto_adjust`` (the default)
        for dividends too, so a price from years ago is not what a share cost that day.
        ``as_traded_close`` is Alpaca's unadjusted close. Anything that needs the price traded
        on a date (market cap = shares outstanding x price) multiplies the backtest price by
        ``as_traded_close / close`` of that day. One row per session Alpaca has a real bar for.
        """
        if isinstance(asset, str):
            asset = Asset(symbol=asset)
        if str(getattr(asset, "asset_type", "")).lower() != "stock":
            return None
        frames = self._state("_as_traded_frames", dict)
        if asset.symbol in frames:
            return frames[asset.symbol]
        closes = self._state("_as_traded_daily_closes", dict)
        if asset.symbol not in closes:
            try:
                rows = self._stock_bars_in_groups(
                    [(asset, quote or self.LUMIBOT_DEFAULT_QUOTE_ASSET)], "day",
                    self._data_datetime_start, self._data_datetime_end,
                    refresh=bool(getattr(self, "_refresh_cache", False)), adjustment="raw",
                )
            except Exception as exc:
                logger.debug("As-traded closes for %s unavailable: %s", asset.symbol, exc)
                rows = {}
            closes[asset.symbol] = self._closes_by_session(rows.get(asset.symbol))
        traded = closes[asset.symbol]
        frame = None
        if traded is not None:
            try:
                adjusted = self._closes_by_session(
                    self.get_historical_prices_between_dates(base_asset=asset, quote_asset=quote, timestep="day")
                )
            except Exception as exc:
                logger.debug("Daily closes for %s unavailable: %s", asset.symbol, exc)
                adjusted = None
            if adjusted is not None:
                frame = pd.DataFrame({"close": adjusted, "as_traded_close": traded}).dropna()
                frame.index = pd.DatetimeIndex(frame.index, name="session")
                if frame.empty:
                    frame = None
        frames[asset.symbol] = frame
        return frame

    @staticmethod
    def _closes_by_session(rows):
        """Close per New York session date from daily bars (``timestamp`` as a column or the index)."""
        if rows is None or len(rows) == 0 or "close" not in rows.columns:
            return None
        stamps = rows["timestamp"] if "timestamp" in rows.columns else rows.index
        stamps = pd.DatetimeIndex(pd.to_datetime(stamps))
        stamps = stamps.tz_localize("UTC") if stamps.tz is None else stamps
        sessions = pd.DatetimeIndex(stamps.tz_convert("America/New_York").date)
        closes = pd.Series(pd.to_numeric(rows["close"], errors="coerce").to_numpy(), index=sessions).dropna()
        closes = closes[closes > 0]
        closes = closes[~closes.index.duplicated(keep="last")].sort_index()
        return closes if len(closes) else None

    def _stock_bars_in_groups(self, members, timestep, start, end, *, request_end=None, refresh=False,
                              adjustment=None):
        """Rows per symbol for ``members`` ([(asset, quote), ...]), a few symbols-per-request at a time."""
        by_symbol = {}
        size = max(1, int(self.ALPACA_STOCK_SYMBOLS_PER_REQUEST))
        symbols = sorted({asset.symbol for asset, _quote in members})
        first_asset, first_quote = members[0]
        for offset in range(0, len(symbols), size):
            chunk = symbols[offset:offset + size]
            client, request = self._history_request(
                base_asset=first_asset, quote_asset=first_quote, timestep=timestep,
                data_datetime_start=start, data_datetime_end=end, auto_adjust=self._auto_adjust,
                request_end=request_end, adjustment=adjustment,
            )
            request = request.model_copy(update={"symbol_or_symbols": chunk})
            what = f"{len(chunk)} stock symbols"
            bars = fetch_alpaca_bars(
                client, request,
                lambda query: self._alpaca_request(client.get_stock_bars, query, what=what),
                refresh=refresh,
            )
            by_symbol.update(self._rows_by_symbol(bars.df))
        return by_symbol

    def _prefetch_stock_history(self, assets, length, timestep, timeshift, quote) -> None:
        if not isinstance(assets, list) or len(assets) < 2:
            return
        if timestep is None:
            timestep = self._timestep
        source_timestep, resample_rule = self._normalize_timestep_for_source(timestep)
        store = self._state("_data_store", dict)
        refreshed = self._state("_refreshed_keys", dict)
        tried = self._state("_stock_prefetch_tried", set)
        members = {}
        for item in assets:
            base, item_quote = (item[0], item[1]) if isinstance(item, tuple) else (item, quote)
            if isinstance(base, str):
                base = Asset(symbol=base)
            if str(getattr(base, "asset_type", "")).lower() in ("option", "crypto"):
                continue
            base, item_quote = self._sanitize_base_and_quote_asset(base, item_quote or self.LUMIBOT_DEFAULT_QUOTE_ASSET)
            key = self._get_asset_key(base_asset=base, quote_asset=item_quote, timestep=source_timestep)
            members.setdefault(key, (base, item_quote))
        if len(members) < 2:
            return

        # 1. The backtest window, for symbols with no frame and no cache file yet.
        cache_dir = os.path.join(LUMIBOT_CACHE_FOLDER, self.CACHE_SUBFOLDER)
        wanted = {}
        for key, member in members.items():
            if key in tried:
                continue
            if self._refresh_cache and key not in refreshed:
                wanted[key] = member
            elif key not in store and not os.path.exists(os.path.join(cache_dir, f"{key}.csv")):
                wanted[key] = member
        if len(wanted) > 1:
            tried.update(wanted)  # a symbol with no bars is not asked for in every later scan
            rows = self._stock_bars_in_groups(
                list(wanted.values()), source_timestep, self._data_datetime_start, self._data_datetime_end,
                refresh=bool(self._refresh_cache),
            )
            for key, (base, _quote) in wanted.items():
                frame = rows.get(base.symbol)
                if frame is None or frame.empty:
                    continue  # the per-asset lookup reports it, as before
                self._finish_window_frame(key, frame, source_timestep, self.tzinfo,
                                          self._data_datetime_start, self._data_datetime_end)
                if self._refresh_cache:
                    refreshed[key] = True
            logger.info("Loaded %d stock symbols in %d request(s)", len(wanted),
                        (len(wanted) + self.ALPACA_STOCK_SYMBOLS_PER_REQUEST - 1) // self.ALPACA_STOCK_SYMBOLS_PER_REQUEST)

        # 2. Bars from before the window (history_before_start), for symbols that need them now.
        if not getattr(self, "_history_before_start", False):
            return
        search_datetime = self._datetime - timeshift if timeshift else self._datetime
        needed_start = self._history_start_needed(search_datetime, length, timestep)
        loaded_from = self._state("_history_loaded_from", dict)
        attempts = self._state("_history_reach_attempts", set)
        groups = {}
        for key, member in members.items():
            loaded_start = loaded_from.get(key, self._data_datetime_start)
            frame = store.get(key)
            if needed_start >= loaded_start or frame is None:
                continue
            if (key, int(length), str(timestep), search_datetime.date()) in attempts:
                continue
            last_included = loaded_start - timedelta(seconds=1)
            segment_key = self._get_asset_key(
                base_asset=member[0], quote_asset=member[1], timestep=source_timestep,
                data_datetime_start=needed_start, data_datetime_end=last_included,
            ) + "_HISTORY"
            segment_refresh = bool(self._refresh_cache) and segment_key not in refreshed
            if not segment_refresh and (
                    segment_key in store or os.path.exists(os.path.join(cache_dir, f"{segment_key}.csv"))):
                continue
            view = self._resample_ohlcv_dataframe(frame, resample_rule) if resample_rule is not None else frame
            position = self._newest_bar_position(view.index, search_datetime, timestep,
                                                 self._remove_incomplete_current_bar)
            if position + 1 >= length:
                continue
            groups.setdefault((loaded_start, segment_refresh), []).append((segment_key, member))
        for (loaded_start, segment_refresh), group in groups.items():
            if len(group) < 2:
                continue
            last_included = loaded_start - timedelta(seconds=1)
            rows = self._stock_bars_in_groups(
                [member for _key, member in group], source_timestep, needed_start, last_included,
                request_end=last_included, refresh=segment_refresh,
            )
            for segment_key, (base, _quote) in group:
                frame = rows.get(base.symbol)
                bars = None if frame is None else SimpleNamespace(df=frame.set_index("timestamp"))
                self._finish_history_segment(segment_key, self._bars_to_frame(bars), base, source_timestep,
                                             needed_start, loaded_start, segment_refresh)

    def _finish_option_frame(self, key, df, tzinfo, data_datetime_start, data_datetime_end,
                             write_csv: bool = True) -> pd.DataFrame:
        """Real option trade prints for the window: sort, clip, cache to disk, store.

        Option bars are sparse trade prints. Reindexing and forward/back filling them would
        invent prices (and back-fill a later trade into the past). Keep only real bars;
        get_last_price/get_historical_prices handle the gaps honestly.
        """
        # PERF: a delta pick stores about 200 contracts. Convert the timestamps once, keep the
        # six columns, and sort only when needed (the shared cache already returns sorted rows);
        # the result equals the copy, convert, select, sort, filter sequence it replaces.
        stamps = df['timestamp']
        if df.empty or not pd.api.types.is_datetime64_any_dtype(stamps):
            # Already datetimes (the shared cache returns them): converting again only costs time.
            stamps = pd.to_datetime(stamps, utc=df.empty)
        stamps = stamps.dt.tz_localize(tzinfo) if stamps.dt.tz is None else stamps.dt.tz_convert(tzinfo)
        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].copy()
        df['timestamp'] = stamps
        if not (df['timestamp'].is_monotonic_increasing and df['timestamp'].is_unique):
            df = df.sort_values("timestamp")
        df = df[(df['timestamp'] >= data_datetime_start) & (df['timestamp'] <= data_datetime_end)]
        if write_csv:
            cache_dir = os.path.join(LUMIBOT_CACHE_FOLDER, self.CACHE_SUBFOLDER)
            os.makedirs(cache_dir, exist_ok=True)
            df.to_csv(os.path.join(cache_dir, f"{key}.csv"), index=False)
        df = df.set_index('timestamp')
        self._state("_data_store", dict)[key] = df
        # Debug only: a delta pick stores about 200 contracts; prefetch_option_bars logs one summary.
        logger.debug("Finished fetching and caching data for %s", key)
        return df

    @staticmethod
    def _key_asset_class(key: str) -> str | None:
        """Asset class in a data-store key (``<symbol>-<asset_type>_<quote>-...``), for telemetry."""
        head = str(key).split("_", 1)[0]
        return head.rsplit("-", 1)[-1] if "-" in head else None

    def _load_ohlcv_into_data_store(self, key: str) -> bool:
        """
        Loads OHLCV data from a cached file into the data store. If the loading is successful, returns True;
        otherwise, returns False.
    
        Parameters
        ----------
        key : str
            The unique key for the cached data file.
    
        Returns
        -------
        bool
            True if data is successfully loaded into the _data_store, False otherwise.
        """
        # Directory to find the cached data file.
        cache_dir = os.path.join(LUMIBOT_CACHE_FOLDER, self.CACHE_SUBFOLDER)
        filename = f"{key}.csv"
        filepath = os.path.join(cache_dir, filename)

        # Check if the file exists
        from lumibot.tools import backtest_telemetry

        asset_class = self._key_asset_class(key)
        if not os.path.exists(filepath):
            backtest_telemetry.record_cache("miss", layer="alpaca_files", source="alpaca", asset_class=asset_class)
            return False
        try:
            nbytes = os.path.getsize(filepath)
        except OSError:
            nbytes = 0
        backtest_telemetry.record_cache("hit", layer="alpaca_files", source="alpaca", asset_class=asset_class,
                                        nbytes=nbytes)

        try:
            # Read CSV file with 'timestamp' column parsed as dates
            df = pd.read_csv(filepath, parse_dates=['timestamp'])

            # Convert timestamp column to datetime objects, interpreting them as UTC times
            # utc=True ensures proper handling of timezone-aware data
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

            # Convert timestamps from UTC to the timezone specified in self.tzinfo
            # For example: if self.tzinfo is 'America/New_York', converts UTC times to NY time
            df['timestamp'] = df['timestamp'].dt.tz_convert(self.tzinfo)

            df.set_index('timestamp', inplace=True)
            self._data_store[key] = df
            logger.info(f"Loaded cached data for key: {key} from cache.")
            return True
        except Exception as e:
            logger.error(f"Failed to load cached data for key: {key}. Error: {e}")
            return False

    # ------------------------------------------------------------------
    # History before the data window (history_before_start)
    # ------------------------------------------------------------------
    def _history_start_needed(self, search_datetime: datetime, length: int, timestep: str) -> datetime:
        """Midnight of the first trading session a request for ``length`` bars needs.

        Sessions are counted on the market calendar. Intraday bars assume the 390-minute regular
        session, which over-counts for series that include extended hours (native multi-minute
        bars) and so reaches back far enough. A quarter more plus two sessions covers half days,
        halts and bars that do not print every period.
        """
        delta, unit = DataSourceBacktesting.convert_timestep_str_to_timedelta(timestep)
        if unit == "day":
            sessions = int(length) * max(delta.days, 1)
            limit = self.HISTORY_BEFORE_START_MAX_DAILY_SESSIONS
        else:
            bar_minutes = delta.total_seconds() / 60.0
            sessions = math.ceil(int(length) * bar_minutes / self.REGULAR_SESSION_MINUTES)
            limit = self.HISTORY_BEFORE_START_MAX_INTRADAY_SESSIONS
        sessions = min(math.ceil(sessions * 1.25) + 2, limit)
        first_day = date_n_trading_days_from_date(
            n_days=sessions,
            start_datetime=search_datetime,
            market=self.market,
        )
        return self.tzinfo.localize(datetime.combine(first_day, datetime.min.time()))

    def _reach_back_for_history(
            self,
            *,
            asset: Asset,
            quote: Asset,
            source_timestep: str,
            frame: pd.DataFrame,
            search_datetime: datetime,
            length: int,
            timestep: str,
    ) -> pd.DataFrame | None:
        """Add real bars from before the loaded series so ``length`` finished bars can exist.

        Returns the extended series (also stored for later calls), or None when nothing was
        added. The series only ever grows backwards, one segment per reach, and a reach that is
        already covered, or was already tried today for this request, never asks Alpaca again:
        a symbol with no earlier bars (a new listing, an option before its first trade) must
        not cost one request per bar.
        """
        asset, quote = self._sanitize_base_and_quote_asset(asset, quote)
        key = self._get_asset_key(base_asset=asset, quote_asset=quote, timestep=source_timestep)
        attempts = self._state("_history_reach_attempts", set)
        marker = (key, int(length), str(timestep), search_datetime.date())
        if marker in attempts:
            return None
        attempts.add(marker)

        loaded_from = self._state("_history_loaded_from", dict)
        loaded_start = loaded_from.get(key, self._data_datetime_start)
        needed_start = self._history_start_needed(search_datetime, length, timestep)
        if needed_start >= loaded_start:
            return None

        segment = self._history_segment(asset, quote, source_timestep, needed_start, loaded_start)
        loaded_from[key] = needed_start
        if segment.empty:
            return None
        merged = pd.concat([segment, frame])
        merged = merged[~merged.index.duplicated(keep="last")].sort_index()
        self._state("_data_store", dict)[key] = merged
        logger.info(
            "History before %s for %s: %d real bars from %s",
            loaded_start.date(),
            key,
            len(segment),
            needed_start.date(),
        )
        return merged

    def _history_segment(
            self,
            asset: Asset,
            quote: Asset,
            source_timestep: str,
            segment_start: datetime,
            segment_end: datetime,
    ) -> pd.DataFrame:
        """Real Alpaca bars in [segment_start, segment_end), cached on disk like the window.

        Nothing is reindexed or filled (RULE #1). Stock and crypto 1-minute bars keep only the
        regular-session minutes of the market calendar, the same minutes the window's minute
        series holds; daily, native multi-minute and option bars are kept as Alpaca returns them.
        """
        last_included = segment_end - timedelta(seconds=1)
        key = self._get_asset_key(
            base_asset=asset,
            quote_asset=quote,
            timestep=source_timestep,
            data_datetime_start=segment_start,
            data_datetime_end=last_included,
        ) + "_HISTORY"
        store = self._state("_data_store", dict)
        refreshed = self._state("_refreshed_keys", dict)
        refresh = bool(getattr(self, "_refresh_cache", False)) and key not in refreshed
        if not refresh and (key in store or self._load_ohlcv_into_data_store(key)):
            return store[key]

        client, request = self._history_request(
            base_asset=asset,
            quote_asset=quote,
            timestep=source_timestep,
            data_datetime_start=segment_start,
            data_datetime_end=last_included,
            auto_adjust=self._auto_adjust,
            request_end=last_included,
        )
        if isinstance(request, CryptoBarsRequest):
            fetch = client.get_crypto_bars
        elif isinstance(request, OptionBarsRequest):
            fetch = client.get_option_bars
        else:
            fetch = client.get_stock_bars
        try:
            bars = fetch_alpaca_bars(
                client, request, lambda query: self._alpaca_request(fetch, query, what=key),
                refresh=refresh,
            )
        except AlpacaOptionsDataPlanError:
            raise
        except Exception as exc:
            raise RuntimeError(f"Failed to fetch history before the backtest window for {key}: {exc}")

        return self._finish_history_segment(key, self._bars_to_frame(bars), asset, source_timestep,
                                            segment_start, segment_end, refresh)

    def _finish_history_segment(self, key, df, asset, source_timestep, segment_start, segment_end,
                                refresh) -> pd.DataFrame:
        """Clip, cache and store one symbol's bars from before the window (single or grouped download)."""
        store = self._state("_data_store", dict)
        if not df.empty and not self._is_option(asset) and source_timestep == "minute":
            calendar = get_trading_days(
                self.market,
                segment_start,
                segment_end + timedelta(days=1),
                tzinfo=self.tzinfo,
            )
            session_minutes = self._get_trading_times_for_timestep(pcal=calendar, timestep="minute")
            df = df[df["timestamp"].isin(session_minutes)]
        df = df[(df["timestamp"] >= segment_start) & (df["timestamp"] < segment_end)].sort_values("timestamp")

        cache_dir = os.path.join(LUMIBOT_CACHE_FOLDER, self.CACHE_SUBFOLDER)
        os.makedirs(cache_dir, exist_ok=True)
        df.to_csv(os.path.join(cache_dir, f"{key}.csv"), index=False)
        df = df.set_index("timestamp")
        store[key] = df
        if refresh:
            self._state("_refreshed_keys", dict)[key] = True
        return df

    def _bars_to_frame(self, bars) -> pd.DataFrame:
        """A BarSet as timestamp/open/high/low/close/volume rows in this source's timezone."""
        columns = ["timestamp", "open", "high", "low", "close", "volume"]
        df = bars.df.reset_index() if bars is not None else pd.DataFrame()
        if df.empty or "timestamp" not in df.columns:
            empty = pd.DataFrame({column: pd.Series(dtype="float64") for column in columns[1:]})
            empty.insert(0, "timestamp", pd.DatetimeIndex([], tz=self.tzinfo))
            return empty
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        if df["timestamp"].dt.tz is None:
            df["timestamp"] = df["timestamp"].dt.tz_localize(self.tzinfo)
        else:
            df["timestamp"] = df["timestamp"].dt.tz_convert(self.tzinfo)
        return df[columns]

    def get_historical_prices_between_dates(
            self,
            *,
            base_asset: Asset = None,
            quote_asset: Asset = None,
            timestep: str = None,
            market: str = None,
            tzinfo: pytz.tzinfo = None,
            data_datetime_start: datetime = None,
            data_datetime_end: datetime = None,
            auto_adjust: bool = None,
    ) -> pd.DataFrame:

        if base_asset is None:
            raise ValueError("Base asset must be provided.")

        if quote_asset is None:
            quote_asset = self.LUMIBOT_DEFAULT_QUOTE_ASSET

        asset, quote = self._sanitize_base_and_quote_asset(base_asset, quote_asset)

        if timestep is None:
            timestep = self._timestep

        if market is None:
            market = self.market

        if tzinfo is None:
            tzinfo = self.tzinfo

        if data_datetime_start is None:
            data_datetime_start = self._data_datetime_start

        if data_datetime_end is None:
            data_datetime_end = self._data_datetime_end

        if auto_adjust is None:
            auto_adjust = self._auto_adjust

        key = self._get_asset_key(base_asset=asset, quote_asset=quote, timestep=timestep)

        if self._refresh_cache and key not in self._refreshed_keys:
            # If we need are refreshing cache and we didn't refresh this key's cache yet, refresh it.
            self._download_and_cache_ohlcv_data(
                base_asset=asset,
                quote_asset=quote,
                timestep=timestep,
                market=market,
                tzinfo=tzinfo,
                data_datetime_start=data_datetime_start,
                data_datetime_end=data_datetime_end,
                auto_adjust=auto_adjust
            )
            self._refreshed_keys[key] = True
        elif key not in self._data_store and not self._load_ohlcv_into_data_store(key):
            # If not refreshing or already refreshed, try to load from cache or download
            self._download_and_cache_ohlcv_data(
                base_asset=asset,
                quote_asset=quote,
                timestep=timestep,
                market=market,
                tzinfo=tzinfo,
                data_datetime_start=data_datetime_start,
                data_datetime_end=data_datetime_end,
                auto_adjust=auto_adjust
            )

        df = self._data_store[key]
        return df

    def _reindex_and_fill(
            self,
            df: pd.DataFrame,
            trading_times: pd.DatetimeIndex,
            timestep: str
    ) -> pd.DataFrame:
        if df.index.name == 'timestamp':
            df = df.reset_index()

        # Check if all required columns are present
        required_columns = {"timestamp", "open", "high", "low", "close", "volume"}
        missing_columns = required_columns - set(df.columns)
        if missing_columns:
            raise ValueError(f"The dataframe is missing the following required columns: {', '.join(missing_columns)}")

        source_timestep, _ = self._normalize_timestep_for_source(timestep)

        # For daily bars, we want to preserve original timestamps but add missing days
        if source_timestep == 'day':
            # Get just the dates from trading_times
            trading_dates = trading_times.date
            # Get dates from df timestamps
            df_dates = df['timestamp'].dt.date

            # Convert both to sets of dates for proper comparison
            trading_dates_set = set(trading_dates)
            df_dates_set = set(df_dates)

            # Find truly missing dates
            missing_dates = trading_dates_set - df_dates_set

            # Add rows for missing dates (at midnight)
            for date in missing_dates:
                # Get timezone from the first timestamp in df
                tz = df['timestamp'].iloc[0].tz

                missing_row = pd.DataFrame({
                    'timestamp': [pd.Timestamp(date).tz_localize(tz)],
                    'open': [None],
                    'high': [None],
                    'low': [None],
                    'close': [None],
                    'volume': [0.0]
                })

                # Remove any all-NA columns from `missing_row`
                missing_row = missing_row.dropna(axis=1, how='all')

                # Proceed with the concatenation
                df = pd.concat([df, missing_row], ignore_index=True)

            # Sort by timestamp
            df.sort_values('timestamp', inplace=True)
        else:
            # For non-daily bars, use the original reindexing logic
            if df.index.name != "timestamp":
                # Ensure timestamp is the index for reindexing
                df = df.set_index("timestamp")
            df = df.reindex(trading_times)
            df.index.name = 'timestamp'  # Restore the index name
            df.sort_values('timestamp', inplace=True)
            df.reset_index(inplace=True)

        # Fill missing volume values with 0.0
        df['volume'] = df['volume'].fillna(0.0)

        # Forward fill missing close prices
        df['close'] = df['close'].ffill()

        # Fill missing open, high, low with close prices
        for column in ['open', 'high', 'low']:
            df[column] = df[column].fillna(df['close'])

        # Backward fill remaining missing open prices
        df['open'] = df['open'].bfill()

        # Fill any remaining missing high, low, close with open prices
        for column in ['high', 'low', 'close']:
            df[column] = df[column].fillna(df['open'])

        return df
