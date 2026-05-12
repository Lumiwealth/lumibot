from __future__ import annotations

# pyright: reportMissingTypeStubs=false
# pyright: reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false
# pyright: reportUnknownParameterType=false, reportMissingParameterType=false, reportMissingTypeArgument=false
# pyright: reportConstantRedefinition=false, reportInvalidTypeForm=false, reportOptionalMemberAccess=false
# pyright: reportAttributeAccessIssue=false, reportIncompatibleMethodOverride=false, reportArgumentType=false
# pyright: reportIndexIssue=false, reportUnnecessaryComparison=false
import datetime as dt
from collections import defaultdict
from collections.abc import Callable
from decimal import Decimal
from importlib import import_module
from types import ModuleType
from typing import TYPE_CHECKING, Any, TypeAlias, cast

import pytz

from lumibot.constants import LUMIBOT_DEFAULT_TIMEZONE
from lumibot.entities.asset import Asset
from lumibot.entities.quote import Quote
from lumibot.tools import black_scholes
from lumibot.tools.lumibot_logger import get_logger

from .data_source import DataSource

if TYPE_CHECKING:
    from lumibot.entities.bars import Bars

logger = get_logger(__name__)
PandasDataFrame: TypeAlias = Any  # noqa: UP040
AssetInput: TypeAlias = Asset | tuple[Asset, Asset]  # noqa: UP040
ChainMap: TypeAlias = dict[str, Any]  # noqa: UP040
GreeksMap: TypeAlias = dict[str, Any]  # noqa: UP040


class _LazyModule(ModuleType):
    _module_name: str
    _module: ModuleType | None

    __slots__ = ("_module_name", "_module")

    def __init__(self, module_name: str) -> None:
        super().__init__(module_name)
        self._module_name = module_name
        self._module = None

    def _load(self) -> ModuleType:
        module = self._module
        if module is None:
            module = import_module(self._module_name)
            self._module = module
        return module

    def __getattr__(self, name: str) -> Any:
        return getattr(self._load(), name)


pd = _LazyModule("pandas")
_tradier_class_cache: type[Any] | None = None
_bars_class_cache: type[Any] | None = None
_create_options_symbol_cache: Callable[..., str] | None = None
_date_n_trading_days_from_date_cache: Callable[..., Any] | None = None
_parse_timestep_qty_and_unit_cache: Callable[..., tuple[int, str]] | None = None


def _tradier_class() -> type[Any]:
    global _tradier_class_cache
    if _tradier_class_cache is None:
        from lumiwealth_tradier import Tradier

        _tradier_class_cache = Tradier
    return _tradier_class_cache


def _bars_class() -> type[Any]:
    global _bars_class_cache
    if _bars_class_cache is None:
        from lumibot.entities.bars import Bars

        _bars_class_cache = Bars
    return _bars_class_cache


def _create_options_symbol(*args: Any, **kwargs: Any) -> str:
    global _create_options_symbol_cache
    if _create_options_symbol_cache is None:
        from lumibot.tools.helpers import create_options_symbol

        _create_options_symbol_cache = create_options_symbol
    return str(_create_options_symbol_cache(*args, **kwargs))


def _date_n_trading_days_from_date(*args: Any, **kwargs: Any) -> dt.date:
    global _date_n_trading_days_from_date_cache
    if _date_n_trading_days_from_date_cache is None:
        from lumibot.tools.helpers import date_n_trading_days_from_date

        _date_n_trading_days_from_date_cache = date_n_trading_days_from_date
    return cast(dt.date, _date_n_trading_days_from_date_cache(*args, **kwargs))


def _parse_timestep_qty_and_unit(*args: Any, **kwargs: Any) -> tuple[int, str]:
    global _parse_timestep_qty_and_unit_cache
    if _parse_timestep_qty_and_unit_cache is None:
        from lumibot.tools.helpers import parse_timestep_qty_and_unit

        _parse_timestep_qty_and_unit_cache = parse_timestep_qty_and_unit
    qty, unit = _parse_timestep_qty_and_unit_cache(*args, **kwargs)
    return int(qty), str(unit)


class _LazyModule:
    __slots__ = ("_module_name", "_module")

    def __init__(self, module_name: str):
        self._module_name = module_name
        self._module = None

    def _load(self):
        module = self._module
        if module is None:
            module = import_module(self._module_name)
            self._module = module
        return module

    def __getattr__(self, name):
        return getattr(self._load(), name)


pd = _LazyModule("pandas")
_TRADIER_CLASS = None
_BARS_CLASS = None
_CREATE_OPTIONS_SYMBOL = None
_DATE_N_TRADING_DAYS_FROM_DATE = None
_PARSE_TIMESTEP_QTY_AND_UNIT = None


def _tradier_class():
    global _TRADIER_CLASS
    if _TRADIER_CLASS is None:
        from lumiwealth_tradier import Tradier

        _TRADIER_CLASS = Tradier
    return _TRADIER_CLASS


def _bars_class():
    global _BARS_CLASS
    if _BARS_CLASS is None:
        from lumibot.entities import Bars

        _BARS_CLASS = Bars
    return _BARS_CLASS


def _create_options_symbol(*args, **kwargs):
    global _CREATE_OPTIONS_SYMBOL
    if _CREATE_OPTIONS_SYMBOL is None:
        from lumibot.tools.helpers import create_options_symbol

        _CREATE_OPTIONS_SYMBOL = create_options_symbol
    return _CREATE_OPTIONS_SYMBOL(*args, **kwargs)


def _date_n_trading_days_from_date(*args, **kwargs):
    global _DATE_N_TRADING_DAYS_FROM_DATE
    if _DATE_N_TRADING_DAYS_FROM_DATE is None:
        from lumibot.tools.helpers import date_n_trading_days_from_date

        _DATE_N_TRADING_DAYS_FROM_DATE = date_n_trading_days_from_date
    return _DATE_N_TRADING_DAYS_FROM_DATE(*args, **kwargs)


def _parse_timestep_qty_and_unit(*args, **kwargs):
    global _PARSE_TIMESTEP_QTY_AND_UNIT
    if _PARSE_TIMESTEP_QTY_AND_UNIT is None:
        from lumibot.tools.helpers import parse_timestep_qty_and_unit

        _PARSE_TIMESTEP_QTY_AND_UNIT = parse_timestep_qty_and_unit
    return _PARSE_TIMESTEP_QTY_AND_UNIT(*args, **kwargs)


class TradierAPIError(Exception):
    pass


class TradierData(DataSource):
    MIN_TIMESTEP = "minute"
    SOURCE = "Tradier"
    TIMESTEP_MAPPING = [
        {
            "timestep": "tick",
            "representations": [
                "tick",
            ],
        },
        {
            "timestep": "minute",
            "representations": [
                "minute",
            ],
        },
        {
            "timestep": "day",
            "representations": [
                "daily",
            ],
        },
        {
            "timestep": "week",
            "representations": [
                "weekly",
            ],
        },
        {
            "timestep": "month",
            "representations": [
                "monthly",
            ],
        },
    ]

    def __init__(
        self,
        account_number: str,
        access_token: str,
        paper: bool = True,
        max_workers: int = 20,
        delay: int | None = None,
        tzinfo: pytz.tzinfo.BaseTzInfo | None = None,
        remove_incomplete_current_bar: bool = False,
        **kwargs: Any,
    ) -> None:
        """
        Initializes the trading account with the specified parameters.

        Parameters:
        - account_number (str): The account number used for accessing the trading account.
        - access_token (str): The access token for authenticating requests.
        - paper (bool, optional): Indicates whether to use the paper trading environment.
          Defaults to True.
        - max_workers (int, optional): The maximum number of workers for parallel processing.
          Defaults to 20.
        - delay (int, optional): A delay parameter to control how many minutes to delay non-crypto data for.
          If not specified, uses DATA_SOURCE_DELAY environment variable or defaults to 0.
        - tzinfo (pytz.timezone, optional): Timezone for data adjustments. Determines how datetime objects
          are adjusted when retrieving historical data. Defaults to the `LUMIBOT_DEFAULT_TIMEZONE`.
        - remove_incomplete_current_bar (bool, optional): Default False.
          Whether to remove the incomplete current bar from the data.
          Tradier includes incomplete bars for the current bar (ie: it gives you a daily bar for the current day even if
          the day isn't over yet). Some Lumibot users night not expect that, so this option will remove the incomplete
          bar from the data.

        Returns:
        - None
        """

        if tzinfo is None:
            tzinfo = pytz.timezone(LUMIBOT_DEFAULT_TIMEZONE)

        super().__init__(api_key=access_token, delay=delay, tzinfo=tzinfo)
        self._account_number = account_number
        self._paper = paper
        self.max_workers = min(max_workers, 50)
        self.tradier = _tradier_class()(account_number, access_token, paper)
        self._remove_incomplete_current_bar = remove_incomplete_current_bar

    def _sanitize_base_and_quote_asset(
        self,
        base_asset: AssetInput,
        quote_asset: Asset | None,
    ) -> tuple[Asset, Asset | None]:
        if isinstance(base_asset, tuple):
            quote = base_asset[1]
            asset = base_asset[0]
        else:
            asset = base_asset
            quote = quote_asset

        if isinstance(asset, str):
            raise NotImplementedError(f"TradierData doesn't support string assets like: {asset} yet.")

        return asset, quote

    def get_chains(
        self,
        asset: Asset,
        quote: Asset | None = None,
        exchange: str | None = None,
    ) -> ChainMap:
        """
        Obtains option chain information for the asset (stock) from each
        of the exchanges the options trade on and returns a dictionary
        for each exchange.

        Parameters
        ----------
        asset : Asset
            The asset to get the option chains for
        quote : Asset | None
            The quote asset to get the option chains for
        exchange: str | None
            The exchange to get the option chains for

        Returns
        -------
        dictionary of dictionary
            Format:
            - `Multiplier` (str) eg: `100`
            - 'Chains' - paired Expiration/Strike info to guarentee that the strikes are valid for the specific
                         expiration date.
                         Format:
                           chains['Chains']['CALL'][exp_date] = [strike1, strike2, ...]
                         Expiration Date Format: 2023-07-31
        """
        df_chains = self.tradier.market.get_option_expirations(asset.symbol)
        if not isinstance(df_chains, pd.DataFrame) or df_chains.empty:
            raise LookupError(f"Could not find Tradier option chains for {asset.symbol}")

        # Tradier doesn't report multiple exchanges, just use SMART
        multiplier = int(df_chains.contract_size.mode()[0])  # Use most common, should always be 100
        chains: ChainMap = {
            "Multiplier": multiplier,
            "Exchange": "unknown",
            "Chains": {"CALL": defaultdict(list), "PUT": defaultdict(list)},
        }
        for row in df_chains.reset_index().to_dict("records"):
            exp_date = row["date"].strftime("%Y-%m-%d")
            strikes = row["strikes"]
            try:
                strikes = sorted(float(s) for s in strikes)
            except Exception:
                try:
                    strikes = sorted(strikes)
                except Exception:
                    pass
            chains["Chains"]["CALL"][exp_date] = strikes
            chains["Chains"]["PUT"][exp_date] = strikes

        return chains

    def get_chain_full_info(
        self,
        asset: Asset,
        expiry: str,
        chains: ChainMap | None = None,
        underlying_price: float | None = None,
        risk_free_rate: float | None = None,
        strike_min: float | None = None,
        strike_max: float | None = None,
    ) -> PandasDataFrame:
        """
        Get the full chain information for an option asset, including: greeks, bid/ask, open_interest, etc. For
        brokers that do not support this, greeks will be calculated locally. For brokers like Tradier this function
        is much faster as only a single API call can be done to return the data for all options simultaneously.

        Parameters
        ----------
        asset : Asset
            The option asset to get the chain information for.
        expiry : str | dt.datetime | dt.date
            The expiry date of the option chain.
        chains : dict
            The chains dictionary created by `get_chains` method. This is used
            to get the list of strikes needed to calculate the greeks.
        underlying_price : float
            Price of the underlying asset.
        risk_free_rate : float
            The risk-free rate used in interest calculations.
        strike_min : float
            The minimum strike price to return in the chain. If None, will return all strikes.
            This is not necessary for Tradier as all option data is returned in a single query.
        strike_max : float
            The maximum strike price to return in the chain. If None, will return all strikes.
            This is not necessary for Tradier as all option data is returned in a single query.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the full chain information for the option asset. Greeks columns will be named as
            'greeks.delta', 'greeks.theta', etc.
        """
        df = self.tradier.market.get_option_chains(asset.symbol, expiry, greeks=True)

        # Filter the dataframe by strike_min and strike_max
        if strike_min is not None:
            df = df[df.strike >= strike_min]
        if strike_max is not None:
            df = df[df.strike <= strike_max]

        return df

    def get_historical_prices(
        self,
        asset: AssetInput,
        length: int,
        timestep: str = "",
        timeshift: Any | None = None,
        quote: Asset | None = None,
        exchange: str | None = None,
        include_after_hours: bool = True,
        return_polars: bool = False,
        **kwargs: Any,
    ) -> Bars | None:
        """
        Get bars for a given asset

        Parameters
        ----------
        asset : Asset
            The asset to get the bars for.
        length : int
            The number of bars to get.
        timestep : str
            The timestep to get the bars at. Accepts "day" or "minute".
        timeshift : dt.timedelta
            The amount of time to shift the bars by. For example, if you want the bars from 1 hour ago to now,
            you would set timeshift to 1 hour.
        quote : Asset
            The quote asset to get the bars for.
        exchange : str
            The exchange to get the bars for.
        include_after_hours : bool
            Whether to include after hours data.
        """
        asset, quote = self._sanitize_base_and_quote_asset(asset, quote)
        timestep = timestep if timestep else self.MIN_TIMESTEP

        # Parse the timestep
        timestep_qty, timestep_unit = _parse_timestep_qty_and_unit(timestep)

        parsed_timestep_unit = self._parse_source_timestep(timestep_unit, reverse=True)

        if asset.asset_type == "option":
            symbol = _create_options_symbol(
                asset.symbol,
                asset.expiration,
                asset.right,
                asset.strike,
            )
        else:
            symbol = asset.symbol

        # Create end time
        now = dt.datetime.now(self.tzinfo)
        if self._delay:
            end_dt = now - self._delay
        else:
            end_dt = now

        if timeshift is not None:
            if not isinstance(timeshift, dt.timedelta):
                raise TypeError("timeshift must be a timedelta")
            end_dt = end_dt - timeshift

        if timestep == "day":
            days_needed = length
        else:
            # For minute bars, calculate additional days needed accounting for weekends/holidays
            # minutes_per_day = 390  # ~6.5 hours of trading per day
            minutes_per_day = 24 * 60 / timestep_qty  # Need to include premarket and after hours
            days_needed = int(length // minutes_per_day) + 1

        start_date = _date_n_trading_days_from_date(
            n_days=days_needed,
            start_datetime=end_dt,
            # TODO: pass market into DataSource
            # This works for now. Crypto gets more bars but throws them out.
            market="NYSE",
        )
        start_dt = self.tzinfo.localize(dt.datetime.combine(start_date, dt.datetime.min.time()))

        # Check what timestep we are using, different endpoints are required for different timesteps
        try:
            if parsed_timestep_unit == "minute":
                df = self.tradier.market.get_timesales(
                    symbol,
                    interval=timestep_qty,
                    start_date=start_dt,
                    end_date=end_dt,
                    session_filter="all" if include_after_hours else "open",
                )
            else:
                df = self.tradier.market.get_historical_quotes(
                    symbol,
                    interval=parsed_timestep_unit,
                    start_date=start_dt,
                    end_date=end_dt,
                    session_filter="all" if include_after_hours else "open",
                )
        except Exception as e:
            logger.error(f"Error getting historical prices for {symbol}: {e}")
            return None

        # Drop the "time" and "timestamp" columns if they exist
        if "time" in df.columns:
            df = df.drop(columns=["time"])
        if "timestamp" in df.columns:
            df = df.drop(columns=["timestamp"])

        # If the index contains date objects, convert and handle timezone
        if isinstance(df.index[0], dt.date):  # Check if the index contains date objects
            df.index = pd.to_datetime(df.index)  # Always ensure it's a DatetimeIndex

            # Check if the index is timezone-naive or already timezone-aware
            if df.index.tz is None:  # Naive index, localize to data source timezone
                df.index = df.index.tz_localize(self.tzinfo)
            else:  # Already timezone-aware, convert to data source timezone
                df.index = df.index.tz_convert(self.tzinfo)

        # Check for incomplete bars
        if self._remove_incomplete_current_bar:
            if timestep == "minute":
                # For minute bars, remove the current minute
                current_minute = now.replace(second=0, microsecond=0)
                df = df[df.index < current_minute]
            else:
                # For daily bars, remove today's bar if market is open
                current_date = now.date()
                df = df[df.index.date < current_date]

        # Ensure df only contains the last N bars
        if len(df) > length:
            df = df.iloc[-length:]

        # Convert the dataframe to a Bars object
        bars = _bars_class()(df, self.SOURCE, asset, raw=df, quote=quote)

        return bars

    def get_last_price(
        self,
        asset: AssetInput,
        quote: Asset | None = None,
        exchange: str | None = None,
    ) -> float | Decimal | None:
        """
        This function returns the last price of an asset.
        Parameters
        ----------
        asset: Asset
            The asset to get the last price for
        quote: Asset
            The quote asset to get the last price for (currently not used for Tradier)
        exchange: str
            The exchange to get the last price for (currently not used for Tradier)

        Returns
        -------
        float or Decimal or none
           Price of the asset
        """
        asset, quote = self._sanitize_base_and_quote_asset(asset, quote)

        symbol = None
        try:
            if asset.asset_type == "option":
                symbol = _create_options_symbol(
                    asset.symbol,
                    asset.expiration,
                    asset.right,
                    asset.strike,
                )
            elif asset.asset_type == "index":
                symbol = f"I:{asset.symbol}"
            else:
                symbol = asset.symbol

            price = self.tradier.market.get_last_price(symbol)
            return price

        except Exception as e:
            logger.error(f"Error getting last price for {symbol or asset.symbol}: {e}")
            return None

    def get_quote(
        self,
        asset: AssetInput,
        quote: Asset | None = None,
        exchange: str | None = None,
    ) -> Quote:
        """
        This function returns the quote of an asset.
        Parameters
        ----------
        asset: Asset
            The asset to get the quote for
        quote: Asset
            The quote asset to get the quote for (currently not used for Tradier)
        exchange: str
            The exchange to get the quote for (currently not used for Tradier)

        Returns
        -------
        Quote
           Quote object containing bid, ask, last price and other information
        """

        asset, quote = self._sanitize_base_and_quote_asset(asset, quote)

        if asset.asset_type == "option":
            symbol = _create_options_symbol(
                asset.symbol,
                asset.expiration,
                asset.right,
                asset.strike,
            )
        else:
            symbol = asset.symbol

        quotes_df = self.tradier.market.get_quotes([symbol])

        # If the dataframe is empty, return an empty Quote
        if quotes_df is None or quotes_df.empty:
            return Quote(asset=asset)

        # Get the quote from the dataframe
        quote_dict = quotes_df.iloc[0].to_dict()

        # Extract relevant fields for the Quote object
        return Quote(
            asset=asset,
            price=quote_dict.get("last"),
            bid=quote_dict.get("bid"),
            ask=quote_dict.get("ask"),
            volume=quote_dict.get("volume"),
            timestamp=dt.datetime.now(pytz.UTC),
            bid_size=quote_dict.get("bidsize"),
            ask_size=quote_dict.get("asksize"),
            change=quote_dict.get("change"),
            percent_change=quote_dict.get("change_percentage"),
            raw_data=quote_dict,
        )

    def query_greeks(self, asset: Asset) -> GreeksMap:
        """
        This function returns the greeks of an option as reported by the Tradier API.

        Parameters
        ----------
        asset : Asset
            The option asset to get the greeks for.

        Returns
        -------
        dict
            A dictionary containing the greeks of the option.
        """
        greeks: GreeksMap = {}
        stock_symbol = asset.symbol
        expiration = asset.expiration
        option_symbol = _create_options_symbol(stock_symbol, expiration, asset.right, asset.strike)
        df_chains = self.tradier.market.get_option_chains(stock_symbol, expiration, greeks=True)
        df = df_chains[df_chains["symbol"] == option_symbol]
        if df.empty:
            return {}

        for col in [x for x in df.columns if "greeks" in x]:
            greek_name = col.replace("greeks.", "")
            greeks[greek_name] = df[col].iloc[0]

        # Tradier can round extremely small deltas to 0.0 for far OTM options.
        # When we have IV available, compute a more precise delta as a best-effort fallback
        # so callers (and legacy tests) can rely on delta sign being correct.
        try:
            delta = float(greeks.get("delta", 0.0) or 0.0)
        except Exception:
            delta = 0.0

        if asset.right.upper() == "CALL" and delta == 0.0:
            try:
                iv = float(greeks.get("mid_iv") or greeks.get("ask_iv") or greeks.get("bid_iv") or 0.0)
            except Exception:
                iv = 0.0

            if iv > 0:
                try:
                    underlying_price = self.get_last_price(Asset(stock_symbol, asset_type="stock"))
                    underlying_price = float(underlying_price) if underlying_price is not None else None
                except Exception:
                    underlying_price = None

                if underlying_price is not None:
                    expiration = asset.expiration
                    if isinstance(expiration, str):
                        expiration = dt.datetime.strptime(expiration, "%Y-%m-%d").date()
                    if isinstance(expiration, dt.datetime):
                        expiration = expiration.date()

                    if isinstance(expiration, dt.date):
                        expiration_dt = dt.datetime.combine(expiration, dt.time.min)
                        expiration_dt = self.tzinfo.localize(expiration_dt).replace(
                            hour=16, minute=0, second=0, microsecond=0
                        )
                        now = self.get_datetime()
                        days_to_expiration = (expiration_dt - now).total_seconds() / (60 * 60 * 24)
                        # Options can be queried after market close in CI/local runs. Avoid a near-zero
                        # time-to-expiration that would underflow deltas to 0.0 for slightly OTM calls.
                        days_to_expiration = max(days_to_expiration, 1.0 / 24.0)

                        interest = 0.0
                        c = black_scholes.BS(
                            [underlying_price, float(asset.strike), interest, days_to_expiration],
                            volatility=iv * 100,
                        )
                        greeks["delta"] = float(c.callDelta)
        return greeks
