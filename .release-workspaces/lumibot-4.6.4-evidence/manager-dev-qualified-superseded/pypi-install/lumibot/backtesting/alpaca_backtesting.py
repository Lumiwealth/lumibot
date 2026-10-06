import json
import math
import os
import time
from collections import deque
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional

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


class AlpacaOptionHistoryUnavailable(RuntimeError):
    """Alpaca answered an option bars request successfully but returned no bars."""


class AlpacaBacktesting(DataSourceBacktesting):
    """Backtest with historical data from your own Alpaca account (stocks, crypto, options).

    Options ("bring your own key"):

    - ``get_chains()`` lists contracts with Alpaca's option contracts API
      (``GET /v2/options/contracts``), expired (``status=inactive``) and live
      (``status=active``) together, following ``next_page_token`` pagination. The chain
      holds expirations from the simulated date through ``OPTION_CHAIN_MAX_DAYS`` (90)
      days later, or the ``min/max_expiration_date`` hint that ``OptionsHelper`` sets.
      Only standard 100-share contracts whose root is the underlying are included.
    - Chains are cached per (underlying, simulated date, expiration window) in memory
      and as JSON under ``<LUMIBOT_CACHE_FOLDER>/alpaca/option_chains/``. One contract
      listing is reused for later dates while it still covers their window, so a
      strategy that calls ``get_chains()`` every bar makes about three API calls per
      listing, not one per bar.
    - Option prices come from Alpaca historical option bars, which are trade prints.
      They are kept as-is: no reindexing, no forward fill, no back fill (RULE #1 in
      ``docs/BACKTESTING_ARCHITECTURE.md``). ``get_last_price()`` returns the open of a
      bar that printed in the current minute (or day), otherwise the close of the most
      recent earlier print, and ``None`` before the first print. ``BacktestingBroker``
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
    # Extra expiration days fetched per contract listing so later simulated days reuse it.
    OPTION_CHAIN_LISTING_EXTRA_DAYS = 30
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
        """Returns the open price of the current bar.

        Options use real trade prints only: the open of a bar that printed in the current
        bucket, otherwise the close of the latest earlier print, otherwise ``None``.
        """

        asset, quote = self._sanitize_base_and_quote_asset(asset, quote)

        if self._is_option(asset):
            return self._get_option_last_price(asset, quote)

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

        # The backtesting_broker, fills market orders using the open price of the current bar, so
        # get_last_price should also return the open. (It would be weird to fill on the open but provide the close
        # as the last price). This approach works for daily and minute bars. For daily bars, this returns the open
        # price, even if now is 9:30 and the daily bar was indexed at 00:00. Thats the only weird thing. But it makes
        # sense. The open of the daily bar for stocks was not at 00:00. It was at 9:30 anyway.
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
            return int(index.date.searchsorted(search_date, side="right")) - 1

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

        aggregation = {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
        available_aggregation = {column: method for column, method in aggregation.items() if column in df.columns}

        resampled = (
            df.sort_index()
            .resample(resample_rule, label="left", closed="left")
            .agg(available_aggregation)
        )
        required_columns = [column for column in ("open", "high", "low", "close") if column in resampled.columns]
        if required_columns:
            resampled = resampled.dropna(subset=required_columns, how="any")
        return resampled

    # ------------------------------------------------------------------
    # Options
    # ------------------------------------------------------------------
    @staticmethod
    def _is_option(asset) -> bool:
        return str(getattr(asset, "asset_type", "")).lower() == "option"

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

    def _alpaca_request(self, fn, request, *, what: str):
        """Call one Alpaca SDK method politely: client-side throttle plus bounded 429 retry.

        The SDK already retries a 429 three times after 3 seconds. This adds a longer,
        bounded wait (``Retry-After`` when Alpaca sends it) before failing loudly.
        """
        retries = 0
        waited = 0.0
        while True:
            self._throttle_alpaca_requests()
            try:
                return fn(request)
            except Exception as exc:
                if not self._is_rate_limited(exc):
                    raise
                retries += 1
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

    def _list_option_contracts(self, symbol: str, first_expiration: date, last_expiration: date) -> list:
        """Return (expiration, right, strike) for standard contracts expiring in the window.

        Expired contracts are ``status=inactive`` and live ones ``status=active``; a historical
        chain needs both. One listing is kept per underlying and reused for later simulated
        days while it still covers their window.
        """
        from alpaca.trading.enums import AssetStatus
        from alpaca.trading.requests import GetOptionContractsRequest

        listings = self._state("_option_contract_listings", dict)
        cached = listings.get(symbol)
        if cached is not None and cached[0] <= first_expiration and last_expiration <= cached[1]:
            return [c for c in cached[2] if first_expiration <= c[0] <= last_expiration]

        listing_end = last_expiration + timedelta(days=int(self.OPTION_CHAIN_LISTING_EXTRA_DAYS))
        root = self._option_root(symbol)
        client = self._get_trading_client()
        contracts = []
        for status in (AssetStatus.INACTIVE, AssetStatus.ACTIVE):
            page_token = None
            while True:
                request = GetOptionContractsRequest(
                    underlying_symbols=[symbol],
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
                    parsed = self._parse_option_contract(contract, root)
                    if parsed is not None:
                        contracts.append(parsed)
                page_token = getattr(response, "next_page_token", None)
                if not page_token:
                    break

        listings[symbol] = (first_expiration, listing_end, contracts)
        return [c for c in contracts if first_expiration <= c[0] <= last_expiration]

    def _parse_option_contract(self, contract, root: str):
        try:
            size = float(getattr(contract, "size", 100) or 100)
        except (TypeError, ValueError):
            return None
        if size != 100:
            # Adjusted deliverables after corporate actions are not the standard contract.
            return None
        contract_root = getattr(contract, "root_symbol", None)
        if contract_root and self._option_root(contract_root) != root:
            return None
        expiration = self._as_date(getattr(contract, "expiration_date", None))
        right = str(getattr(getattr(contract, "type", None), "value", getattr(contract, "type", "")) or "").upper()
        if right not in ("CALL", "PUT") or expiration is None:
            return None
        try:
            strike = float(getattr(contract, "strike_price"))
        except (TypeError, ValueError):
            return None
        return expiration, right, strike

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

        cache_dir = os.path.join(LUMIBOT_CACHE_FOLDER, self.CACHE_SUBFOLDER, "option_chains")
        cache_file = os.path.join(cache_dir, "_".join(cache_key) + ".json")
        if os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as handle:
                    chain = json.load(handle)
                memory[cache_key] = chain
                return self._copy_chain(chain)
            except (OSError, ValueError) as exc:
                logger.warning("Ignoring unreadable Alpaca option chain cache %s: %s", cache_file, exc)

        chain = self._empty_chain(symbol)
        for expiration, right, strike in self._list_option_contracts(symbol, first_expiration, last_expiration):
            chain["Chains"][right].setdefault(expiration.isoformat(), set()).add(strike)
        for side in ("CALL", "PUT"):
            chain["Chains"][side] = {
                expiry: sorted(strikes) for expiry, strikes in sorted(chain["Chains"][side].items())
            }

        try:
            os.makedirs(cache_dir, exist_ok=True)
            with open(cache_file, "w", encoding="utf-8") as handle:
                json.dump(chain, handle, separators=(",", ":"))
        except OSError as exc:
            logger.warning("Could not write Alpaca option chain cache %s: %s", cache_file, exc)
        memory[cache_key] = chain
        return self._copy_chain(chain)

    def _get_option_bars_frame(self, asset: Asset, quote: Asset, timestep: str) -> pd.DataFrame:
        """Real option trade bars for the whole backtest window, or an empty frame."""
        asset, quote = self._sanitize_base_and_quote_asset(asset, quote)
        key = self._get_asset_key(base_asset=asset, quote_asset=quote, timestep=timestep)
        store = self._state("_data_store", dict)
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
        df = self._get_option_bars_frame(asset, quote, source_timestep)
        search_datetime = self._datetime - timeshift if timeshift else self._datetime
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

    def _get_option_last_price(self, asset: Asset, quote: Asset) -> float | Decimal | None:
        timestep = self.__dict__.get("_timestep") or "minute"
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
        bar_ts = pd.Timestamp(df_local.index[-1])
        now = pd.Timestamp(self._datetime)
        if timestep == "day":
            printed_now = bar_ts.date() == now.date()
        else:
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
        return key

    @staticmethod
    def _occ_symbol(asset: Asset) -> str:
        """Build the Alpaca OCC symbol for one listed option."""
        strike_formatted = f"{float(asset.strike):08.3f}".replace(".", "").rjust(8, "0")
        expiration = asset.expiration
        if hasattr(expiration, "strftime"):
            date = expiration.strftime("%y%m%d")
        else:
            date = datetime.strptime(str(expiration)[:10], "%Y-%m-%d").strftime("%y%m%d")
        right = str(asset.right or "C")[0].upper()
        return f"{asset.symbol}{date}{right}{strike_formatted}"

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
    ):
        """Return the Alpaca client and bar request for this asset.

        ``request_end`` replaces the default end (``data_datetime_end`` plus one day), for a
        history segment that must stop where the loaded window begins.
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
                end=end,
            )
            return self._option_client, request
        request = StockBarsRequest(
            symbol_or_symbols=base_asset.symbol,
            timeframe=timeframe,
            start=data_datetime_start,
            end=end,
            adjustment="all" if auto_adjust else "split",
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
                bars = self._alpaca_request(client.get_crypto_bars, request_params, what=key)
            elif isinstance(request_params, OptionBarsRequest):
                bars = self._alpaca_request(client.get_option_bars, request_params, what=key)
            else:
                bars = self._alpaca_request(client.get_stock_bars, request_params, what=key)
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

        # Ensure 'timestamp' is a pandas timestamp object
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        if df['timestamp'].dt.tz is None:
            df['timestamp'] = df['timestamp'].dt.tz_localize(tzinfo)
        else:
            df['timestamp'] = df['timestamp'].dt.tz_convert(tzinfo)

        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]

        if is_option:
            # Option bars are sparse trade prints. Reindexing and forward/back filling them
            # would invent prices (and back-fill a later trade into the past). Keep only
            # real bars; get_last_price/get_historical_prices handle the gaps honestly.
            df = df.sort_values("timestamp")
        elif timestep in ("day", "minute"):
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
            df.sort_values("timestamp", inplace=True)

        # Filter data to include only rows between data_datetime_start and data_datetime_end
        df = df[(df['timestamp'] >= data_datetime_start) & (df['timestamp'] <= data_datetime_end)]

        # Save to cache
        df.to_csv(filepath, index=False)

        # Store in _data_store
        df.set_index('timestamp', inplace=True)
        self._data_store[key] = df
        logger.info(f"Finished fetching and caching data for {key}")
        return df

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
        if not os.path.exists(filepath):
            return False

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
            request_end=segment_end,
        )
        if isinstance(request, CryptoBarsRequest):
            fetch = client.get_crypto_bars
        elif isinstance(request, OptionBarsRequest):
            fetch = client.get_option_bars
        else:
            fetch = client.get_stock_bars
        try:
            bars = self._alpaca_request(fetch, request, what=key)
        except Exception as exc:
            raise RuntimeError(f"Failed to fetch history before the backtest window for {key}: {exc}")

        df = self._bars_to_frame(bars)
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
            refreshed[key] = True
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
