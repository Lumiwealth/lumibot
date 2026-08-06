import json
import math
import os
from datetime import date, datetime, timedelta, timezone
from importlib import import_module
from typing import Any, Literal

from .docs_tools import search_lumibot_docs
from .asset_resolution import resolve_asset_and_quote
from .schemas import BoundTool, ToolDefinition
from .tool_context import current_agent_tool_context


AssetTypeArg = Literal["stock", "option", "future", "cont_future", "forex", "crypto", "index", "multileg", "us_equity"]
OrderSideArg = Literal[
    "buy",
    "sell",
    "buy_to_open",
    "buy_to_close",
    "sell_to_open",
    "sell_to_close",
    "sell_short",
    "buy_to_cover",
]
OrderTypeArg = Literal["market", "limit", "stop", "stop_limit", "trailing_stop", "smart_limit"]
TimeInForceArg = Literal["day", "gtc", "gtd"]
OptionRightArg = Literal["call", "put"]
MultilegPriceStyleArg = Literal["market", "best", "mid", "fastest"]
NewsSortArg = Literal["asc", "desc"]

COMMON_INDICATORS = [
    "sma",
    "ema",
    "rsi",
    "macd",
    "bbands",
    "atr",
    "vwap",
    "vwma",
    "roc",
    "stoch",
]


def _agent_memory_context_kwargs() -> dict[str, Any]:
    context = current_agent_tool_context()
    kwargs: dict[str, Any] = {}
    for key in ("agent_name", "model_call_id"):
        value = context.get(key)
        if value:
            kwargs[key] = value
    return kwargs


class _LazyModule:
    """Read-only proxy that imports the target module on first attribute access.

    object.__setattr__ touches the internal slots directly; callers should only
    read attributes through this proxy so module mutation is not hidden here.
    """

    __slots__ = ("_module_name", "_module")

    def __init__(self, module_name: str):
        object.__setattr__(self, "_module_name", module_name)
        object.__setattr__(self, "_module", None)

    def _load(self):
        module = object.__getattribute__(self, "_module")
        if module is None:
            module = import_module(object.__getattribute__(self, "_module_name"))
            object.__setattr__(self, "_module", module)
        return module

    def __getattr__(self, name):
        return getattr(self._load(), name)

    def __setattr__(self, name, value):
        if name in {"_module_name", "_module"}:
            object.__setattr__(self, name, value)
            return
        setattr(self._load(), name, value)

    def __delattr__(self, name):
        if name in {"_module_name", "_module"}:
            object.__delattr__(self, name)
            return
        delattr(self._load(), name)


requests = _LazyModule("requests")


def _requests():
    return requests


def _asset_class():
    from lumibot.entities import Asset

    return Asset


def _parse_datetime_value(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _coerce_same_timezone(value: datetime, reference: datetime) -> datetime:
    if value.tzinfo is None and reference.tzinfo is not None:
        return value.replace(tzinfo=reference.tzinfo)
    if value.tzinfo is not None and reference.tzinfo is None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    if value.tzinfo is not None and reference.tzinfo is not None:
        return value.astimezone(reference.tzinfo)
    return value


def _coerce_expiration(expiration: Any) -> Any:
    if isinstance(expiration, str) and expiration.strip():
        try:
            return datetime.fromisoformat(expiration).date()
        except ValueError:
            return expiration
    return expiration


def _require_non_empty_text(name: str, value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name} is required and must be a non-empty string.")
    return text


def _require_single_symbol_text(name: str, value: Any) -> str:
    text = _require_non_empty_text(name, value)
    if "," in text:
        raise ValueError(
            f"{name} must be one tradable symbol, not a comma-separated list. "
            "Call this tool once per symbol."
        )
    return text


def _require_positive_int(name: str, value: Any) -> int:
    try:
        parsed = int(value)
    except Exception as exc:
        raise ValueError(f"{name} must be a positive integer.") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be greater than 0.")
    return parsed


def _require_positive_number(name: str, value: Any) -> float:
    try:
        parsed = float(value)
    except Exception as exc:
        raise ValueError(f"{name} must be a positive number.") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise ValueError(f"{name} must be a finite number greater than 0.")
    return parsed


def _agent_tool_calls_for_current_run() -> list[dict[str, Any]]:
    context = current_agent_tool_context()
    calls = context.get("tool_calls")
    if isinstance(calls, list):
        return [call for call in calls if isinstance(call, dict)]
    return []


def _tool_call_was_successful(call: dict[str, Any]) -> bool:
    return call.get("ok") is not False


def _has_successful_tool_call(tool_name: str) -> bool:
    return any(
        call.get("tool_name") == tool_name and _tool_call_was_successful(call)
        for call in _agent_tool_calls_for_current_run()
    )


def _symbols_from_tool_argument(value: Any) -> set[str]:
    """Normalize symbol arguments from single-symbol or batch price tools."""
    symbols: set[str] = set()
    if value is None:
        return symbols
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return symbols
        if text.startswith("["):
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, list):
                for item in parsed:
                    symbol = str(item or "").strip().upper()
                    if symbol:
                        symbols.add(symbol)
                return symbols
        for part in text.split(","):
            symbol = part.strip().upper()
            if symbol:
                symbols.add(symbol)
        return symbols
    if isinstance(value, (list, tuple, set)):
        for item in value:
            symbol = str(item or "").strip().upper()
            if symbol:
                symbols.add(symbol)
    return symbols


def _has_successful_market_last_price_for_symbol(symbol: str) -> bool:
    normalized_symbol = str(symbol or "").strip().upper()
    for call in _agent_tool_calls_for_current_run():
        tool_name = call.get("tool_name")
        if tool_name not in {"market_last_price", "market_last_prices"} or not _tool_call_was_successful(call):
            continue
        arguments = call.get("arguments") if isinstance(call.get("arguments"), dict) else {}
        if tool_name == "market_last_price":
            if str(arguments.get("symbol") or "").strip().upper() == normalized_symbol:
                return True
            continue
        # Batch tool: accept either symbols list or symbols_json.
        batch_symbols = _symbols_from_tool_argument(arguments.get("symbols"))
        batch_symbols |= _symbols_from_tool_argument(arguments.get("symbols_json"))
        if normalized_symbol in batch_symbols:
            return True
    return False


def _require_agent_order_readiness(symbol: str) -> None:
    context = current_agent_tool_context()
    if not bool(context.get("enforce_order_readiness")):
        return
    missing: list[str] = []
    if not _has_successful_tool_call("account_portfolio"):
        missing.append("account_portfolio")
    if not _has_successful_tool_call("account_positions"):
        missing.append("account_positions")
    if not _has_successful_market_last_price_for_symbol(symbol):
        missing.append(
            f"market_last_price(symbol={symbol!r}) or market_last_prices including {symbol!r}"
        )
    if missing:
        raise ValueError(
            "ORDER_READINESS_REQUIRED: Before submitting an order, call "
            f"{', '.join(missing)} in this same agent run. "
            "Agents must inspect cash, portfolio value, positions, and the latest price for the ordered asset before trading."
        )


def _parse_symbol_list(
    *,
    symbols: list[str] | tuple[str, ...] | str | None = None,
    symbols_json: str | None = None,
    max_symbols: int = 150,
) -> list[str]:
    """Parse a JSON-friendly symbol universe for batch market tools."""
    values: list[str] = []
    if symbols is not None:
        if isinstance(symbols, str):
            values.extend(_symbols_from_tool_argument(symbols))
        elif isinstance(symbols, (list, tuple)):
            for index, item in enumerate(symbols):
                try:
                    values.append(_require_non_empty_text("symbol", item).upper())
                except ValueError as exc:
                    raise ValueError(f"Invalid symbol at index {index}: {exc}") from exc
        else:
            raise ValueError("symbols must be a list of symbols or a comma-separated string.")
    if symbols_json is not None and str(symbols_json).strip():
        raw = _require_non_empty_text("symbols_json", symbols_json)
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"symbols_json must be valid JSON: {exc}") from exc
        if isinstance(parsed, str):
            parsed = [parsed]
        if not isinstance(parsed, list) or not parsed:
            raise ValueError("symbols_json must decode to a non-empty list of symbols.")
        for index, item in enumerate(parsed):
            try:
                values.append(_require_non_empty_text("symbol", item).upper())
            except ValueError as exc:
                raise ValueError(f"Invalid symbol at index {index}: {exc}") from exc
    unique: list[str] = []
    seen: set[str] = set()
    for value in values:
        symbol = str(value or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        unique.append(symbol)
    if not unique:
        raise ValueError("Provide symbols and/or symbols_json with at least one symbol.")
    if len(unique) > max_symbols:
        raise ValueError(f"At most {max_symbols} symbols are allowed per batch market call.")
    return unique


_HISTORY_BAR_COLUMNS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "dividend",
    "stock_splits",
    "bid",
    "ask",
    "dividend_yield",
)


def _bars_to_records(bars: Any) -> list[dict[str, Any]]:
    """Serialize a Bars object (or bars-like frame) into JSON-friendly OHLCV rows."""
    if bars is None:
        return []
    frame = getattr(bars, "pandas_df", None)
    if frame is None:
        frame = getattr(bars, "df", None)
    if frame is None:
        return []
    try:
        working = frame.copy()
    except Exception:
        return []
    if getattr(working, "empty", False):
        return []
    try:
        if getattr(working.index, "name", None) is not None or str(getattr(working.index, "dtype", "")).startswith(
            "datetime"
        ):
            working = working.reset_index()
    except Exception:
        pass
    datetime_col = None
    for candidate in ("datetime", "date", "timestamp", "time", "index"):
        if candidate in working.columns:
            datetime_col = candidate
            break
    records: list[dict[str, Any]] = []
    for row in working.to_dict(orient="records"):
        record: dict[str, Any] = {}
        if datetime_col is not None:
            record["datetime"] = _jsonable(row.get(datetime_col))
        for column in _HISTORY_BAR_COLUMNS:
            if column not in row:
                continue
            value = row.get(column)
            try:
                if value is None:
                    record[column] = None
                else:
                    number = float(value)
                    record[column] = number if math.isfinite(number) else None
            except Exception:
                record[column] = _jsonable(value)
        records.append(record)
    return records


def _symbol_from_bars_key(key: Any, fallback: str | None = None) -> str:
    symbol = getattr(key, "symbol", None)
    if symbol is None and isinstance(key, (list, tuple)) and key:
        symbol = getattr(key[0], "symbol", key[0])
    if symbol is None:
        symbol = key if isinstance(key, str) else fallback
    return str(symbol or "").strip().upper()


def _asset_to_dict(asset: Any) -> dict[str, Any] | str:
    if asset is None:
        return "None"
    expiration = getattr(asset, "expiration", None)
    if isinstance(expiration, (datetime, date)):
        expiration_value = expiration.strftime("%Y-%m-%d")
    else:
        expiration_value = expiration
    return {
        "symbol": getattr(asset, "symbol", None),
        "asset_type": getattr(asset, "asset_type", None),
        "expiration": expiration_value,
        "strike": getattr(asset, "strike", None),
        "right": getattr(asset, "right", None),
        "multiplier": getattr(asset, "multiplier", None),
    }


def _position_to_dict(position: Any) -> dict[str, Any]:
    asset = getattr(position, "asset", None)
    asset_payload = _asset_to_dict(asset)
    quantity = getattr(position, "quantity", None)
    try:
        quantity = float(quantity)
    except Exception:
        quantity = quantity
    return {
        "asset": asset_payload,
        "quantity": quantity,
        "avg_fill_price": _jsonable(getattr(position, "avg_fill_price", None)),
        "current_price": _jsonable(getattr(position, "current_price", None)),
        "market_value": _jsonable(getattr(position, "market_value", None)),
        "pnl": _jsonable(
            getattr(position, "pnl", None)
            if hasattr(position, "pnl")
            else getattr(position, "unrealized_pnl", None)
        ),
        "pnl_percent": _jsonable(getattr(position, "pnl_percent", None)),
    }


def _order_to_dict(order: Any) -> dict[str, Any]:
    asset = getattr(order, "asset", None)
    asset_payload = _asset_to_dict(asset)
    quantity = getattr(order, "quantity", None)
    try:
        quantity = float(quantity)
    except Exception:
        quantity = quantity
    return {
        "identifier": _jsonable(getattr(order, "identifier", None)),
        "status": _jsonable(getattr(order, "status", None)),
        "side": _jsonable(getattr(order, "side", None)),
        "asset": asset_payload,
        "quantity": quantity,
        "order_type": _jsonable(getattr(order, "order_type", None)),
        "time_in_force": _jsonable(getattr(order, "time_in_force", None)),
        "limit_price": _jsonable(getattr(order, "limit_price", None)),
        "stop_price": _jsonable(getattr(order, "stop_price", None)),
    }


def _options_helper_for_strategy(strategy: Any) -> Any:
    helper = getattr(strategy, "_agent_options_helper", None)
    if helper is None:
        from lumibot.components.options_helper import OptionsHelper

        helper = OptionsHelper(strategy)
        setattr(strategy, "_agent_options_helper", helper)
    return helper


def _underlying_asset(
    strategy: Any,
    *,
    symbol: str,
    asset_type: Literal["stock", "index"] = "stock",
) -> Any:
    symbol = _require_single_symbol_text("symbol", symbol)
    asset, _ = resolve_asset_and_quote(strategy, symbol=symbol, asset_type=asset_type)
    return asset


def _option_asset(
    strategy: Any,
    *,
    symbol: str,
    expiration: str,
    strike: float,
    right: OptionRightArg,
) -> Any:
    symbol = _require_single_symbol_text("symbol", symbol)
    expiration_value = _coerce_expiration(_require_non_empty_text("expiration", expiration))
    if not isinstance(expiration_value, date):
        raise ValueError("expiration must use YYYY-MM-DD format.")
    strike_value = _require_positive_number("strike", strike)
    asset, _ = resolve_asset_and_quote(
        strategy,
        symbol=symbol,
        asset_type="option",
        expiration=expiration_value,
        strike=strike_value,
        right=right,
    )
    return asset


def _parse_option_legs(strategy: Any, legs_json: str, *, time_in_force: TimeInForceArg = "day") -> list[Any]:
    raw = _require_non_empty_text("legs_json", legs_json)
    try:
        legs = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"legs_json must be valid JSON: {exc}") from exc
    if not isinstance(legs, list) or len(legs) < 2:
        raise ValueError("legs_json must decode to a list containing at least two option legs.")

    orders: list[Any] = []
    for index, leg in enumerate(legs):
        if not isinstance(leg, dict):
            raise ValueError(f"legs_json item {index} must be a JSON object.")
        try:
            symbol = _require_single_symbol_text("symbol", leg.get("symbol"))
            expiration = _require_non_empty_text("expiration", leg.get("expiration"))
            strike = _require_positive_number("strike", leg.get("strike"))
            right = str(leg.get("right") or "").strip().lower()
            side = str(leg.get("side") or "").strip().lower()
            quantity = _require_positive_number("quantity", leg.get("quantity"))
        except ValueError as exc:
            raise ValueError(f"Invalid option leg at index {index}: {exc}") from exc
        if right not in {"call", "put"}:
            raise ValueError(f"Invalid option leg at index {index}: right must be 'call' or 'put'.")
        if side not in {
            "buy",
            "sell",
            "buy_to_open",
            "buy_to_close",
            "sell_to_open",
            "sell_to_close",
        }:
            raise ValueError(
                f"Invalid option leg at index {index}: side must describe a buy or sell action for an option contract."
            )
        option = _option_asset(
            strategy,
            symbol=symbol,
            expiration=expiration,
            strike=strike,
            right=right,
        )
        orders.append(strategy.create_order(option, quantity, side, time_in_force=time_in_force))
    return orders


def _bind_positions(strategy: Any, manager: Any) -> BoundTool:
    def positions() -> dict[str, Any]:
        return {
            "positions": [_position_to_dict(position) for position in strategy.get_positions(include_cash_positions=True)],
            "as_of": strategy.get_datetime().isoformat(),
        }

    return BoundTool(
        name="account_positions",
        description=(
            "Return current positions as structured data. "
            "Each entry includes exact asset fields, signed quantity, average fill price, current price, market value, and P&L fields when the broker or backtest provides them. "
            "For options, signed quantity is authoritative: quantity > 0 is a long contract and must use sell_to_close to reduce it; quantity < 0 is a short contract and must use buy_to_close to reduce it. "
            "Use expiration, strike, right, signed quantity, and average fill price to reconstruct and manage an existing multi-leg position. Never report the option portfolio as flat while any option entry has nonzero quantity. "
            "Use this before trading to understand current exposure, whether a symbol is already held, and whether the current portfolio is concentrated. "
            "Example: call this before rotating into a new symbol so you can compare it against what is already owned."
        ),
        function=positions,
        metadata={"kind": "builtin"},
    )


def _bind_portfolio(strategy: Any, manager: Any) -> BoundTool:
    def portfolio() -> dict[str, Any]:
        return {
            "cash": strategy.get_cash(),
            "portfolio_value": strategy.get_portfolio_value(),
            "datetime": strategy.get_datetime().isoformat(),
        }

    return BoundTool(
        name="account_portfolio",
        description=(
            "Return current cash and total portfolio value for sizing decisions. "
            "Use this before placing orders when you need to calculate a sensible whole-share quantity or compare a risky asset against a defensive parking asset. "
            "Example: call this before buying TQQQ so you can size a near-fully-invested position intentionally instead of buying one share."
        ),
        function=portfolio,
        metadata={"kind": "builtin"},
    )


def _bind_last_price(strategy: Any, manager: Any) -> BoundTool:
    def last_price(
        *,
        symbol: str,
        asset_type: AssetTypeArg = "stock",
        expiration: str | None = None,
        strike: float | None = None,
        right: str | None = None,
        quote_symbol: str | None = None,
        exchange: str | None = None,
    ) -> dict[str, Any]:
        symbol = _require_single_symbol_text("symbol", symbol)
        asset, quote = resolve_asset_and_quote(
            strategy,
            symbol=symbol,
            asset_type=asset_type,
            expiration=_coerce_expiration(expiration),
            strike=strike,
            right=right,
            quote_symbol=quote_symbol,
        )
        price = strategy.get_last_price(asset, quote=quote, exchange=exchange)
        return {
            "symbol": symbol,
            "asset_type": asset_type,
            "price": float(price) if price is not None else None,
            "datetime": strategy.get_datetime().isoformat(),
        }

    return BoundTool(
        name="market_last_price",
        description=(
            "Get the current last price for one asset. "
            "Arguments: symbol, asset_type, optional expiration/strike/right for derivatives, optional quote_symbol, optional exchange. "
            "Valid asset_type values: stock, option, future, cont_future, forex, crypto, index, multileg, us_equity. "
            "The symbol argument must be one tradable symbol. For scanning a universe of equities or ETFs, prefer market_last_prices. "
            "For historical bars across many symbols, prefer market_historical_prices. "
            "Use stock for normal equities. Do not pass economic series ids such as DCOILWTICO, FEDFUNDS, or M2SL as market symbols; use macro/FRED tools for those instead. "
            "Example: market_last_price(symbol='SPY', asset_type='stock')."
        ),
        function=last_price,
        metadata={"kind": "builtin", "replay_on_cache": True},
    )


def _bind_last_prices(strategy: Any, manager: Any) -> BoundTool:
    def last_prices(
        *,
        symbols: list[str] | tuple[str, ...] | str | None = None,
        symbols_json: str | None = None,
        asset_type: AssetTypeArg = "stock",
        quote_symbol: str | None = None,
        exchange: str | None = None,
    ) -> dict[str, Any]:
        """Return last prices for many symbols at the current runtime datetime.

        Prefer this over calling market_last_price once per symbol when scanning a
        provided universe (for example opening-range breakout across ~100 tickers).
        Missing prices are returned as null; never invent a price.
        """
        symbol_list = _parse_symbol_list(symbols=symbols, symbols_json=symbols_json, max_symbols=150)
        prices: dict[str, float | None] = {}
        missing: list[str] = []
        # Prefer the batch broker path when available; fall back per symbol so one
        # bad ticker does not discard the whole universe scan.
        batch_fn = getattr(strategy, "get_last_prices", None)
        if callable(batch_fn) and asset_type in {"stock", "us_equity", "index"}:
            try:
                batch_result = batch_fn(symbol_list, quote=None, exchange=exchange)
            except Exception:
                batch_result = None
            if isinstance(batch_result, dict):
                for symbol in symbol_list:
                    raw = batch_result.get(symbol)
                    if raw is None:
                        # Some brokers key by Asset; try case-insensitive match.
                        for key, value in batch_result.items():
                            key_symbol = getattr(key, "symbol", key)
                            if str(key_symbol or "").strip().upper() == symbol:
                                raw = value
                                break
                    try:
                        price = float(raw) if raw is not None else None
                    except Exception:
                        price = None
                    if price is None or not math.isfinite(price):
                        prices[symbol] = None
                        missing.append(symbol)
                    else:
                        prices[symbol] = price
            else:
                batch_result = None
        else:
            batch_result = None

        if batch_result is None:
            for symbol in symbol_list:
                try:
                    asset, quote = resolve_asset_and_quote(
                        strategy,
                        symbol=symbol,
                        asset_type=asset_type,
                        quote_symbol=quote_symbol,
                    )
                    raw = strategy.get_last_price(asset, quote=quote, exchange=exchange)
                    price = float(raw) if raw is not None else None
                    if price is None or not math.isfinite(price):
                        prices[symbol] = None
                        missing.append(symbol)
                    else:
                        prices[symbol] = price
                except Exception:
                    prices[symbol] = None
                    missing.append(symbol)

        available = [symbol for symbol, price in prices.items() if price is not None]
        return {
            "prices": prices,
            "symbols_requested": symbol_list,
            "symbols_available": available,
            "symbols_missing": missing,
            "count_requested": len(symbol_list),
            "count_available": len(available),
            "asset_type": asset_type,
            "datetime": strategy.get_datetime().isoformat(),
        }

    return BoundTool(
        name="market_last_prices",
        description=(
            "Get current last prices for many symbols in one call (JSON-friendly universe scan). "
            "Arguments: symbols as a list or comma-separated string, and/or symbols_json as a JSON array; "
            "optional asset_type (default stock), optional quote_symbol, optional exchange. "
            "Cap is 150 symbols per call. Returns prices keyed by symbol, plus symbols_available and symbols_missing. "
            "Use this to scan a provided equity/ETF universe before fetching detailed history with "
            "market_historical_prices for finalists or a full multi-symbol history request. "
            "Never invent prices for missing symbols. "
            "Example: market_last_prices(symbols_json='[\"SPY\",\"QQQ\",\"AAPL\",\"MSFT\"]')."
        ),
        function=last_prices,
        metadata={"kind": "builtin", "replay_on_cache": True},
    )


def _bind_historical_prices(strategy: Any, manager: Any) -> BoundTool:
    def historical_prices(
        *,
        symbols: list[str] | tuple[str, ...] | str | None = None,
        symbols_json: str | None = None,
        length: int,
        timestep: str = "day",
        asset_type: AssetTypeArg = "stock",
        quote_symbol: str | None = None,
        exchange: str | None = None,
        include_after_hours: bool = True,
        chunk_size: int = 100,
        max_workers: int = 200,
    ) -> dict[str, Any]:
        """Return historical OHLCV bars for many symbols in one call.

        Prefer this over calling market_load_history_table once per symbol when the
        strategy needs bars for a provided universe or a shortlist of finalists.
        """
        symbol_list = _parse_symbol_list(symbols=symbols, symbols_json=symbols_json, max_symbols=150)
        length = _require_positive_int("length", length)
        timestep = _require_non_empty_text("timestep", timestep)
        chunk_size = _require_positive_int("chunk_size", chunk_size)
        max_workers = _require_positive_int("max_workers", max_workers)

        bars_by_symbol: dict[str, list[dict[str, Any]]] = {}
        missing: list[str] = []
        batch_fn = getattr(strategy, "get_historical_prices_for_assets", None)
        batch_result = None
        if callable(batch_fn) and asset_type in {"stock", "us_equity", "index"}:
            try:
                batch_result = batch_fn(
                    symbol_list,
                    length,
                    timestep=timestep,
                    chunk_size=chunk_size,
                    max_workers=max_workers,
                    exchange=exchange,
                    include_after_hours=include_after_hours,
                )
            except Exception:
                batch_result = None

        if isinstance(batch_result, dict):
            keyed: dict[str, Any] = {}
            for key, bars in batch_result.items():
                keyed[_symbol_from_bars_key(key)] = bars
            for symbol in symbol_list:
                records = _bars_to_records(keyed.get(symbol))
                if records:
                    bars_by_symbol[symbol] = records
                else:
                    bars_by_symbol[symbol] = []
                    missing.append(symbol)
        else:
            for symbol in symbol_list:
                try:
                    asset, quote = resolve_asset_and_quote(
                        strategy,
                        symbol=symbol,
                        asset_type=asset_type,
                        quote_symbol=quote_symbol,
                    )
                    bars = strategy.get_historical_prices(
                        asset,
                        length=length,
                        timestep=timestep,
                        quote=quote,
                        exchange=exchange,
                        include_after_hours=include_after_hours,
                    )
                    records = _bars_to_records(bars)
                    bars_by_symbol[symbol] = records
                    if not records:
                        missing.append(symbol)
                except Exception:
                    bars_by_symbol[symbol] = []
                    missing.append(symbol)

        available = [symbol for symbol, records in bars_by_symbol.items() if records]
        return {
            "bars_by_symbol": bars_by_symbol,
            "symbols_requested": symbol_list,
            "symbols_available": available,
            "symbols_missing": missing,
            "count_requested": len(symbol_list),
            "count_available": len(available),
            "length": length,
            "timestep": timestep,
            "asset_type": asset_type,
            "include_after_hours": bool(include_after_hours),
            "datetime": strategy.get_datetime().isoformat(),
        }

    return BoundTool(
        name="market_historical_prices",
        description=(
            "Get historical OHLCV bars for many symbols in one call via "
            "Strategy.get_historical_prices_for_assets. "
            "Arguments: symbols as a list or comma-separated string, and/or symbols_json as a JSON array; "
            "required length; timestep (default day); optional asset_type (default stock), quote_symbol, "
            "exchange, include_after_hours, chunk_size, max_workers. "
            "Cap is 150 symbols per call. Returns bars_by_symbol keyed by symbol with datetime/open/high/low/close/volume rows, "
            "plus symbols_available and symbols_missing. "
            "Never loop market_load_history_table or market_last_price once per symbol when you need multi-symbol history. "
            "Use market_last_prices for a cheap latest-price universe scan, then this tool for history on finalists or the full list. "
            "For SQL analysis of one already-loaded table, use market_load_history_table plus duckdb_query. "
            "Example: market_historical_prices(symbols_json='[\"SPY\",\"QQQ\",\"AAPL\"]', length=20, timestep='minute')."
        ),
        function=historical_prices,
        metadata={"kind": "builtin", "replay_on_cache": True},
    )


def _bind_options_get_chain(strategy: Any, manager: Any) -> BoundTool:
    def get_chain(
        *,
        symbol: str,
        underlying_asset_type: Literal["stock", "index"] = "stock",
        include_strikes: bool = False,
    ) -> dict[str, Any]:
        underlying = _underlying_asset(strategy, symbol=symbol, asset_type=underlying_asset_type)
        chains = strategy.get_chains(underlying)
        if not chains:
            return {
                "symbol": symbol.upper(),
                "underlying_asset_type": underlying_asset_type,
                "available": False,
                "call_expirations": [],
                "put_expirations": [],
            }

        chain_root = chains.get("Chains", {}) if hasattr(chains, "get") else {}
        call_map = chain_root.get("CALL", {}) if isinstance(chain_root, dict) else {}
        put_map = chain_root.get("PUT", {}) if isinstance(chain_root, dict) else {}

        def side_payload(side_map: Any) -> dict[str, Any]:
            if not isinstance(side_map, dict):
                return {}
            result: dict[str, Any] = {}
            for expiration in sorted(str(value) for value in side_map.keys()):
                strikes = side_map.get(expiration) or []
                normalized_strikes = sorted({float(value) for value in strikes})
                entry: dict[str, Any] = {
                    "strike_count": len(normalized_strikes),
                    "min_strike": normalized_strikes[0] if normalized_strikes else None,
                    "max_strike": normalized_strikes[-1] if normalized_strikes else None,
                }
                if include_strikes:
                    entry["strikes"] = normalized_strikes
                result[expiration] = entry
            return result

        calls = side_payload(call_map)
        puts = side_payload(put_map)
        return {
            "symbol": symbol.upper(),
            "underlying_asset_type": underlying_asset_type,
            "available": True,
            "multiplier": _jsonable(chains.get("Multiplier") if hasattr(chains, "get") else None),
            "exchange": _jsonable(chains.get("Exchange") if hasattr(chains, "get") else None),
            "call_expirations": list(calls.keys()),
            "put_expirations": list(puts.keys()),
            "calls": calls,
            "puts": puts,
            "strikes_included": include_strikes,
            "datetime": strategy.get_datetime().isoformat(),
        }

    return BoundTool(
        name="options_get_chain",
        description=(
            "Retrieve the option chain available for one underlying through LumiBot's configured broker or backtest data source. "
            "Arguments: symbol, optional underlying_asset_type='stock' or 'index', optional include_strikes. "
            "The default compact response lists call and put expirations plus strike counts and ranges. Set include_strikes=true only when you need every strike for every expiration. "
            "Use this before choosing option contracts. Never invent an expiration or strike that is absent from this result. "
            "Example: options_get_chain(symbol='SPY', include_strikes=false)."
        ),
        function=get_chain,
        metadata={"kind": "builtin", "replay_on_cache": True},
    )


def _bind_options_get_strikes(strategy: Any, manager: Any) -> BoundTool:
    def get_strikes(
        *,
        symbol: str,
        expiration: str,
        right: OptionRightArg,
        underlying_asset_type: Literal["stock", "index"] = "stock",
    ) -> dict[str, Any]:
        underlying = _underlying_asset(strategy, symbol=symbol, asset_type=underlying_asset_type)
        expiration_value = _coerce_expiration(_require_non_empty_text("expiration", expiration))
        if not isinstance(expiration_value, date):
            raise ValueError("expiration must use YYYY-MM-DD format.")
        chains = strategy.get_chains(underlying)
        if not chains:
            strikes: list[float] = []
        elif hasattr(chains, "strikes"):
            strikes = chains.strikes(expiration_value, right.upper()) or []
        else:
            strikes = (
                chains.get("Chains", {})
                .get(right.upper(), {})
                .get(expiration_value.isoformat(), [])
            )
        normalized = sorted({float(value) for value in strikes})
        return {
            "symbol": symbol.upper(),
            "expiration": expiration_value.isoformat(),
            "right": right,
            "strikes": normalized,
            "count": len(normalized),
            "datetime": strategy.get_datetime().isoformat(),
        }

    return BoundTool(
        name="options_get_strikes",
        description=(
            "Return every listed strike for one exact underlying, expiration, and option right. "
            "Arguments: symbol, expiration in YYYY-MM-DD, right='call' or 'put', optional underlying_asset_type='stock' or 'index'. "
            "First use options_get_chain to choose a listed expiration, then use this result when selecting exact contracts. "
            "Example: options_get_strikes(symbol='SPY', expiration='2026-09-18', right='put')."
        ),
        function=get_strikes,
        metadata={"kind": "builtin", "replay_on_cache": True},
    )


def _bind_options_get_greeks(strategy: Any, manager: Any) -> BoundTool:
    def get_greeks(
        *,
        symbol: str,
        expiration: str,
        strike: float,
        right: OptionRightArg,
        underlying_price: float | None = None,
        option_price: float | None = None,
        risk_free_rate: float | None = None,
        query_greeks: bool = False,
    ) -> dict[str, Any]:
        option = _option_asset(
            strategy,
            symbol=symbol,
            expiration=expiration,
            strike=strike,
            right=right,
        )
        greeks = strategy.get_greeks(
            option,
            asset_price=option_price,
            underlying_price=underlying_price,
            risk_free_rate=risk_free_rate,
            query_greeks=query_greeks,
        )
        return {
            "asset": _asset_to_dict(option),
            "greeks": _jsonable(greeks),
            "available": greeks is not None,
            "datetime": strategy.get_datetime().isoformat(),
        }

    return BoundTool(
        name="options_get_greeks",
        description=(
            "Get Greeks for one exact listed option contract. "
            "Arguments: symbol, expiration, strike, right, and optional underlying_price, option_price, risk_free_rate, query_greeks. "
            "Use the exact expiration and strike returned by options_get_chain/options_get_strikes. The result proves Greeks only for the exact strike and right named in the result. Never transfer or reuse that delta for a neighboring strike. A null greeks result means the data source cannot value that contract at the current runtime datetime. "
            "Example: options_get_greeks(symbol='SPY', expiration='2026-09-18', strike=650, right='call')."
        ),
        function=get_greeks,
        metadata={"kind": "builtin", "replay_on_cache": True},
    )


def _bind_options_find_strike_for_delta(strategy: Any, manager: Any) -> BoundTool:
    def find_strike_for_delta(
        *,
        symbol: str,
        expiration: str,
        right: OptionRightArg,
        target_delta: float,
        underlying_price: float | None = None,
        underlying_asset_type: Literal["stock", "index"] = "stock",
    ) -> dict[str, Any]:
        target_delta_value = float(target_delta)
        if not math.isfinite(target_delta_value) or abs(target_delta_value) > 1:
            raise ValueError("target_delta must be a finite value from -1 through 1.")
        expiration_value = _coerce_expiration(_require_non_empty_text("expiration", expiration))
        if not isinstance(expiration_value, date):
            raise ValueError("expiration must use YYYY-MM-DD format.")
        underlying = _underlying_asset(strategy, symbol=symbol, asset_type=underlying_asset_type)
        if underlying_price is None:
            underlying_price = strategy.get_last_price(underlying)
        if underlying_price is None:
            raise ValueError(f"No underlying price is available for {symbol.upper()}.")
        chains = strategy.get_chains(underlying)
        strike = _options_helper_for_strategy(strategy).find_strike_for_delta(
            underlying,
            float(underlying_price),
            target_delta_value,
            expiration_value,
            right,
            chains=chains,
        )
        return {
            "symbol": symbol.upper(),
            "expiration": expiration_value.isoformat(),
            "right": right,
            "target_delta": target_delta_value,
            "underlying_price": float(underlying_price),
            "strike": float(strike) if strike is not None else None,
            "available": strike is not None,
            "datetime": strategy.get_datetime().isoformat(),
        }

    return BoundTool(
        name="options_find_strike_for_delta",
        description=(
            "Find the listed strike whose calculated delta is closest to a target for one expiration and right. "
            "Arguments: symbol, expiration, right, target_delta, optional underlying_price, optional underlying_asset_type. "
            "Use positive target delta for calls and negative target delta for puts. First retrieve the chain and choose a listed expiration. "
            "The returned strike is only a search candidate. It does not prove that the contract's current delta equals or is acceptably close to target_delta. Call options_get_greeks on that exact strike, use the exact returned delta, and reject the candidate when it is outside the strategy's permitted range. Never relabel, round, or describe a materially different verified delta as the target delta. "
            "Example: options_find_strike_for_delta(symbol='SPY', expiration='2026-09-18', right='put', target_delta=-0.16)."
        ),
        function=find_strike_for_delta,
        metadata={"kind": "builtin", "replay_on_cache": True},
    )


def _bind_options_evaluate_market(strategy: Any, manager: Any) -> BoundTool:
    def evaluate_market(
        *,
        symbol: str,
        expiration: str,
        strike: float,
        right: OptionRightArg,
        max_spread_pct: float | None = None,
    ) -> dict[str, Any]:
        option = _option_asset(
            strategy,
            symbol=symbol,
            expiration=expiration,
            strike=strike,
            right=right,
        )
        evaluation = _options_helper_for_strategy(strategy).evaluate_option_market(
            option,
            max_spread_pct=max_spread_pct,
        )
        return {
            "asset": _asset_to_dict(option),
            "market": _jsonable(vars(evaluation)),
            "datetime": strategy.get_datetime().isoformat(),
        }

    return BoundTool(
        name="options_evaluate_market",
        description=(
            "Inspect executable quote quality for one exact option contract and return bid, ask, last, spread percentage, suggested buy/sell prices, and data-quality flags. "
            "Arguments: symbol, expiration, strike, right, optional max_spread_pct as a fraction such as 0.20 for 20 percent. "
            "Call this for every proposed leg before submitting a multi-leg order. Do not trade a contract whose response says the market is unavailable or unacceptably wide under your policy. "
            "Example: options_evaluate_market(symbol='SPY', expiration='2026-09-18', strike=650, right='call', max_spread_pct=0.20)."
        ),
        function=evaluate_market,
        metadata={"kind": "builtin", "replay_on_cache": True},
    )


def _bind_options_calculate_multileg_price(strategy: Any, manager: Any) -> BoundTool:
    def calculate_multileg_price(
        *,
        legs_json: str,
        price_style: Literal["best", "mid", "fastest"] = "mid",
    ) -> dict[str, Any]:
        orders = _parse_option_legs(strategy, legs_json)
        net_price = _options_helper_for_strategy(strategy).calculate_multileg_limit_price(orders, price_style)
        if net_price is None:
            return {
                "available": False,
                "price_style": price_style,
                "net_limit_price": None,
                "legs": [_order_to_dict(order) for order in orders],
            }
        net_price = float(net_price)
        order_type = "debit" if net_price > 0 else "credit" if net_price < 0 else "even"
        return {
            "available": True,
            "price_style": price_style,
            "net_limit_price": net_price,
            "order_type": order_type,
            "broker_price": abs(net_price),
            "legs": [_order_to_dict(order) for order in orders],
            "datetime": strategy.get_datetime().isoformat(),
        }

    return BoundTool(
        name="options_calculate_multileg_price",
        description=(
            "Calculate a provider-generic net limit price for two or more exact option legs without submitting them. "
            "Arguments: legs_json and optional price_style='best', 'mid', or 'fastest'. legs_json must be a JSON array; every leg requires symbol, expiration, strike, right, quantity, and side. "
            "Use buy_to_open/sell_to_open when opening and buy_to_close/sell_to_close when closing. A positive net_limit_price is a debit and a negative value is a credit. "
            "For a closing order, a positive account_positions quantity is long and requires sell_to_close; a negative quantity is short and requires buy_to_close. Closing quantity is the absolute value of the position quantity. "
            "When comparing a per-unit multi-leg opening credit with a per-unit closing debit, price one contract per leg here. Use the full absolute position quantities only in the later orders_submit_multileg call. "
            "Independently reconcile the returned net price from the four option midpoint values you just observed. For a defined-risk structure, reject a result that conflicts materially with those leg mids or violates the structure's economic bounds. "
            "Example legs_json: [{\"symbol\":\"SPY\",\"expiration\":\"2026-09-18\",\"strike\":620,\"right\":\"put\",\"quantity\":1,\"side\":\"buy_to_open\"},{\"symbol\":\"SPY\",\"expiration\":\"2026-09-18\",\"strike\":625,\"right\":\"put\",\"quantity\":1,\"side\":\"sell_to_open\"}]."
        ),
        function=calculate_multileg_price,
        metadata={"kind": "builtin", "replay_on_cache": True},
    )


def _bind_options_find_expiration(strategy: Any, manager: Any) -> BoundTool:
    def find_expiration(
        *,
        symbol: str,
        right: OptionRightArg = "call",
        min_days: int | None = None,
        target_date: str | None = None,
        underlying_asset_type: Literal["stock", "index"] = "stock",
        allow_prior: bool = False,
    ) -> dict[str, Any]:
        if min_days is None and not target_date:
            raise ValueError("Provide min_days and/or target_date.")
        if min_days is not None:
            min_days_value = int(min_days)
            if min_days_value < 0:
                raise ValueError("min_days must be >= 0.")
        else:
            min_days_value = None

        current_dt = strategy.get_datetime()
        current_day = current_dt.date() if isinstance(current_dt, datetime) else current_dt
        if target_date:
            target = _coerce_expiration(_require_non_empty_text("target_date", target_date))
            if not isinstance(target, date):
                raise ValueError("target_date must use YYYY-MM-DD format.")
        else:
            target = current_day + timedelta(days=int(min_days_value))

        if min_days_value is not None:
            earliest = current_day + timedelta(days=min_days_value)
            if target < earliest:
                target = earliest

        underlying = _underlying_asset(strategy, symbol=symbol, asset_type=underlying_asset_type)
        chains = strategy.get_chains(underlying)
        expiration = _options_helper_for_strategy(strategy).get_expiration_on_or_after_date(
            target,
            chains,
            right,
            underlying_asset=underlying,
            allow_prior=bool(allow_prior),
        )
        expiration_iso = expiration.isoformat() if isinstance(expiration, date) else None
        days_to_expiration = None
        if isinstance(expiration, date):
            days_to_expiration = (expiration - current_day).days
        return {
            "symbol": symbol.upper(),
            "right": right,
            "requested_target_date": target.isoformat(),
            "min_days": min_days_value,
            "allow_prior": bool(allow_prior),
            "expiration": expiration_iso,
            "days_to_expiration": days_to_expiration,
            "available": expiration is not None,
            "datetime": strategy.get_datetime().isoformat(),
        }

    return BoundTool(
        name="options_find_expiration",
        description=(
            "Find a listed option expiration on or after a target date for one underlying and right. "
            "Arguments: symbol, optional right='call' or 'put', optional min_days, optional target_date in YYYY-MM-DD, "
            "optional underlying_asset_type='stock' or 'index', optional allow_prior. "
            "Provide min_days and/or target_date. When both are set, the later of the two floors is used. "
            "This wraps OptionsHelper.get_expiration_on_or_after_date and validates tradeable data when possible. "
            "Example: options_find_expiration(symbol='SPY', min_days=30, right='put')."
        ),
        function=find_expiration,
        metadata={"kind": "builtin", "replay_on_cache": True},
    )


def _bind_options_check_spread_profit(strategy: Any, manager: Any) -> BoundTool:
    def check_spread_profit(
        *,
        legs_json: str,
        initial_cost: float,
        contract_multiplier: int = 100,
    ) -> dict[str, Any]:
        try:
            initial_cost_value = float(initial_cost)
        except Exception as exc:
            raise ValueError("initial_cost must be a finite number.") from exc
        if not math.isfinite(initial_cost_value) or initial_cost_value == 0:
            raise ValueError("initial_cost must be a nonzero finite number.")
        multiplier = _require_positive_int("contract_multiplier", contract_multiplier)
        orders = _parse_option_legs(strategy, legs_json)
        profit_pct = _options_helper_for_strategy(strategy).check_spread_profit(
            initial_cost_value,
            orders,
            contract_multiplier=multiplier,
        )
        return {
            "available": profit_pct is not None,
            "initial_cost": initial_cost_value,
            "contract_multiplier": multiplier,
            "profit_pct": float(profit_pct) if profit_pct is not None else None,
            "legs": [_order_to_dict(order) for order in orders],
            "datetime": strategy.get_datetime().isoformat(),
            "notes": (
                "initial_cost is the cash paid (positive debit) or cash received as a negative credit when the "
                "spread was opened, matching OptionsHelper.check_spread_profit. profit_pct is relative to that cost."
            ),
        }

    return BoundTool(
        name="options_check_spread_profit",
        description=(
            "Estimate current multi-leg spread P&L percentage from exact option legs and the opening cash cost. "
            "Arguments: legs_json, initial_cost, optional contract_multiplier (default 100). "
            "legs_json must be a JSON array of exact contracts with symbol, expiration, strike, right, quantity, and side. "
            "For opening-cost accounting, use a positive initial_cost for a net debit paid and a negative initial_cost "
            "for a net credit received. Returns profit_pct relative to initial_cost, or available=false when a leg price is missing. "
            "This is generic multi-leg math; it does not assume an iron condor or any named structure. "
            "Example: options_check_spread_profit(legs_json='[...]', initial_cost=-200)."
        ),
        function=check_spread_profit,
        metadata={"kind": "builtin", "replay_on_cache": True},
    )


def _order_status_payload(order: Any) -> dict[str, Any]:
    status = str(getattr(order, "status", "") or "").strip().lower()
    is_filled = bool(getattr(order, "is_filled", lambda: False)())
    is_canceled = bool(getattr(order, "is_canceled", lambda: False)())
    is_active = bool(getattr(order, "is_active", lambda: not (is_filled or is_canceled))())
    is_terminal = bool(is_filled or is_canceled or not is_active)
    payload = _order_to_dict(order)
    payload.update(
        {
            "status_normalized": status or None,
            "is_filled": is_filled,
            "is_canceled": is_canceled,
            "is_active": is_active,
            "is_terminal": is_terminal,
        }
    )
    return payload


def _parse_order_identifiers(*, identifier: str | None = None, identifiers_json: str | None = None) -> list[str]:
    values: list[str] = []
    if identifier is not None and str(identifier).strip():
        values.append(_require_non_empty_text("identifier", identifier))
    if identifiers_json is not None and str(identifiers_json).strip():
        raw = _require_non_empty_text("identifiers_json", identifiers_json)
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"identifiers_json must be valid JSON: {exc}") from exc
        if isinstance(parsed, str):
            parsed = [parsed]
        if not isinstance(parsed, list) or not parsed:
            raise ValueError("identifiers_json must decode to a non-empty list of order identifiers.")
        for index, item in enumerate(parsed):
            try:
                values.append(_require_non_empty_text("identifier", item))
            except ValueError as exc:
                raise ValueError(f"Invalid identifier at index {index}: {exc}") from exc
    # Preserve order while dropping duplicates.
    unique: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        unique.append(value)
    if not unique:
        raise ValueError("Provide identifier and/or identifiers_json with at least one order identifier.")
    return unique


def _bind_orders_get_status(strategy: Any, manager: Any) -> BoundTool:
    def get_status(
        *,
        identifier: str | None = None,
        identifiers_json: str | None = None,
    ) -> dict[str, Any]:
        identifiers = _parse_order_identifiers(identifier=identifier, identifiers_json=identifiers_json)
        orders_payload: list[dict[str, Any]] = []
        missing: list[str] = []
        for order_id in identifiers:
            order = strategy.get_order(order_id)
            if order is None:
                missing.append(order_id)
                orders_payload.append(
                    {
                        "identifier": order_id,
                        "available": False,
                        "status": None,
                        "is_filled": False,
                        "is_canceled": False,
                        "is_active": False,
                        "is_terminal": False,
                    }
                )
                continue
            payload = _order_status_payload(order)
            payload["available"] = True
            orders_payload.append(payload)
        return {
            "orders": orders_payload,
            "count": len(orders_payload),
            "missing_identifiers": missing,
            "all_terminal": bool(orders_payload) and all(item.get("is_terminal") for item in orders_payload),
            "all_filled": bool(orders_payload) and all(item.get("is_filled") for item in orders_payload),
            "datetime": strategy.get_datetime().isoformat(),
        }

    return BoundTool(
        name="orders_get_status",
        description=(
            "Get current status for one or more tracked order identifiers. "
            "Arguments: optional identifier, optional identifiers_json as a JSON array of identifiers. "
            "Reuse this after orders_submit_order or orders_submit_multileg. Never claim a fill unless "
            "is_filled is true for the exact identifier. Missing identifiers are returned with available=false. "
            "Example: orders_get_status(identifier='bt_1') or orders_get_status(identifiers_json='[\"bt_1\",\"bt_2\"]')."
        ),
        function=get_status,
        metadata={"kind": "builtin"},
    )


def _bind_orders_wait_for_terminal(strategy: Any, manager: Any) -> BoundTool:
    def wait_for_terminal(
        *,
        identifier: str | None = None,
        identifiers_json: str | None = None,
        timeout_seconds: float = 30.0,
        poll_interval_seconds: float = 1.0,
    ) -> dict[str, Any]:
        identifiers = _parse_order_identifiers(identifier=identifier, identifiers_json=identifiers_json)
        try:
            timeout_value = float(timeout_seconds)
            poll_value = float(poll_interval_seconds)
        except Exception as exc:
            raise ValueError("timeout_seconds and poll_interval_seconds must be numbers.") from exc
        if not math.isfinite(timeout_value) or timeout_value <= 0:
            raise ValueError("timeout_seconds must be a finite number greater than 0.")
        if not math.isfinite(poll_value) or poll_value <= 0:
            raise ValueError("poll_interval_seconds must be a finite number greater than 0.")
        # Keep agent waits bounded so a hung broker cannot stall an iteration forever.
        timeout_value = min(timeout_value, 120.0)
        poll_value = min(max(poll_value, 0.25), 30.0)

        import time as _time

        started = _time.monotonic()
        polls = 0
        status_tool = _bind_orders_get_status(strategy, manager).function
        latest: dict[str, Any] = {}
        is_backtesting = bool(getattr(strategy, "is_backtesting", False))
        # In backtests, strategy.sleep advances simulation time instantly. Bound by
        # poll count / simulated seconds so a wait cannot race through the remainder
        # of the backtest window (the previous wall-clock-only path left market
        # orders stuck in `new` and produced placeholder tearsheets).
        max_polls = max(1, int(math.ceil(timeout_value / poll_value)) + 1)
        if is_backtesting:
            max_polls = min(max_polls, 60)
            # Prefer minute-scale advances so equity market orders can fill on the
            # next bar even when the agent passes a 1s poll interval.
            backtest_sleep_for = max(poll_value, 60.0)
            max_sim_seconds = min(timeout_value * max(backtest_sleep_for / max(poll_value, 1e-9), 1.0), 3600.0)
            sim_slept = 0.0
            broker = getattr(strategy, "broker", None)
            process_pending = getattr(broker, "process_pending_orders", None)
            if callable(process_pending):
                try:
                    process_pending(strategy=strategy)
                except TypeError:
                    process_pending(strategy)
        else:
            backtest_sleep_for = poll_value
            max_sim_seconds = timeout_value
            sim_slept = 0.0

        while True:
            polls += 1
            latest = status_tool(identifiers_json=json.dumps(identifiers))
            if latest.get("all_terminal"):
                break
            if is_backtesting:
                if polls >= max_polls or sim_slept >= max_sim_seconds:
                    break
            else:
                elapsed = _time.monotonic() - started
                if elapsed >= timeout_value:
                    break
            remaining = timeout_value - ((_time.monotonic() - started) if not is_backtesting else 0.0)
            sleep_for = (
                min(backtest_sleep_for, max(max_sim_seconds - sim_slept, 0.0))
                if is_backtesting
                else min(poll_value, max(remaining, 0.0))
            )
            if sleep_for <= 0:
                break
            sleeper = getattr(strategy, "sleep", None)
            if callable(sleeper):
                sleeper(sleep_for, process_pending_orders=True)
            else:
                if is_backtesting:
                    broker = getattr(strategy, "broker", None)
                    process_pending = getattr(broker, "process_pending_orders", None)
                    updater = getattr(broker, "_update_datetime", None)
                    if callable(updater):
                        updater(sleep_for)
                    if callable(process_pending):
                        try:
                            process_pending(strategy=strategy)
                        except TypeError:
                            process_pending(strategy)
                else:
                    _time.sleep(sleep_for)
            if is_backtesting:
                sim_slept += sleep_for

        elapsed_total = _time.monotonic() - started
        return {
            **latest,
            "timed_out": not bool(latest.get("all_terminal")),
            "timeout_seconds": timeout_value,
            "poll_interval_seconds": poll_value,
            "polls": polls,
            "elapsed_seconds": elapsed_total,
            "datetime": strategy.get_datetime().isoformat(),
        }

    return BoundTool(
        name="orders_wait_for_terminal",
        description=(
            "Poll one or more tracked order identifiers until every order is terminal or a bounded timeout elapses. "
            "Arguments: optional identifier, optional identifiers_json, optional timeout_seconds (default 30, max 120), "
            "optional poll_interval_seconds (default 1). Uses strategy.sleep so pending broker fills can process. "
            "In backtests, this advances simulation time in bounded steps and processes pending fills; prefer a short "
            "timeout and confirm with orders_get_status. Never claim a fill unless is_filled is true. "
            "Example: orders_wait_for_terminal(identifiers_json='[\"bt_1\"]', timeout_seconds=15)."
        ),
        function=wait_for_terminal,
        metadata={"kind": "builtin"},
    )


def _bind_load_history(strategy: Any, manager: Any) -> BoundTool:
    def load_history_table(
        *,
        symbol: str,
        length: int,
        timestep: str = "day",
        table_name: str | None = None,
        asset_type: AssetTypeArg = "stock",
        quote_symbol: str | None = None,
        exchange: str | None = None,
        expiration: str | None = None,
        strike: float | None = None,
        right: str | None = None,
        include_after_hours: bool = True,
    ) -> dict[str, Any]:
        symbol = _require_single_symbol_text("symbol", symbol)
        length = _require_positive_int("length", length)
        timestep = _require_non_empty_text("timestep", timestep)
        return manager.duckdb.load_history_table(
            symbol=symbol,
            length=length,
            timestep=timestep,
            table_name=table_name,
            asset_type=asset_type,
            quote_symbol=quote_symbol,
            exchange=exchange,
            expiration=_coerce_expiration(expiration),
            strike=strike,
            right=right,
            include_after_hours=include_after_hours,
        )

    return BoundTool(
        name="market_load_history_table",
        description=(
            "Load visible historical bars for one symbol into DuckDB and return the table metadata. "
            "Arguments: symbol, length, timestep, optional table_name, asset_type, quote_symbol, exchange, expiration, strike, right, include_after_hours. "
            "Valid asset_type values: stock, option, future, cont_future, forex, crypto, index, multileg, us_equity. "
            "The symbol argument must be the exact tradable symbol, such as XLY or SPY, not a generated table name such as XLY_HIST. "
            "For two or more symbols of history, prefer market_historical_prices instead of calling this once per symbol. "
            "Use stock for normal equities. If asset_type is omitted, stock is assumed. Do not pass economic series ids such as DCOILWTICO, FEDFUNDS, or M2SL as market symbols; use macro/FRED tools for those instead. "
            "The loaded price tables usually expose columns such as datetime, open, high, low, close, volume, bid, ask, dividend, and dividend_yield. "
            "Use datetime for timestamps and close for the traded price unless the returned sample rows show otherwise. "
            "Caveat: this only loads bars visible at the current LumiBot runtime datetime. "
            "Example: market_load_history_table(symbol='TQQQ', length=252, timestep='day', table_name='recent_prices')."
        ),
        function=load_history_table,
        metadata={"kind": "builtin", "replay_on_cache": True},
    )


def _bind_duckdb_query(strategy: Any, manager: Any) -> BoundTool:
    def duckdb_query(*, sql: str, limit: int = 200) -> dict[str, Any]:
        sql = _require_non_empty_text("sql", sql)
        limit = _require_positive_int("limit", limit)
        return manager.duckdb.query(sql=sql, limit=limit)

    return BoundTool(
        name="duckdb_query",
        description=(
            "Run a read-only SQL query against tables previously loaded into DuckDB. "
            "Arguments: sql, optional limit. "
            "Load a table first with market_load_history_table, then analyze it here. "
            "For LumiBot price tables, prefer datetime for timestamps and close for prices unless the loaded sample rows show different column names. "
            "Caveat: only read-only SQL is allowed. "
            "Example: duckdb_query(sql='SELECT AVG(close) AS avg_close FROM recent_prices')."
        ),
        function=duckdb_query,
        metadata={"kind": "builtin"},
    )


def _bind_docs_search(strategy: Any, manager: Any) -> BoundTool:
    def docs_search(*, query: str, max_results: int = 5, limit: int | None = None) -> dict[str, Any]:
        query = _require_non_empty_text("query", query)
        if limit is not None:
            max_results = limit
        max_results = _require_positive_int("max_results", max_results)
        return search_lumibot_docs(query=query, max_results=max_results)

    return BoundTool(
        name="lumibot_docs_search",
        description=(
            "Search LumiBot's local documentation and return the best matching snippets. "
            "Arguments: query, optional max_results or limit. "
            "Use this when you are unsure how a LumiBot tool, asset type, benchmark, or backtesting feature works. "
            "Example: lumibot_docs_search(query='run_backtest benchmark_asset SPY')."
        ),
        function=docs_search,
        metadata={"kind": "builtin"},
    )


ALPACA_NEWS_DESCRIPTION = (
    "Fetch Alpaca/Benzinga news articles using the user's own Alpaca API key. "
    "This is symbol/date-window retrieval, not keyword search: arguments are optional symbols comma-list, "
    "start, end, limit <= 50, include_content, exclude_contentless, page_token, optional content_max_chars, and sort. "
    "In backtests, only use articles at or before the current simulated datetime; if end is omitted, LumiBot uses "
    "the current simulated datetime, and future end times are clamped to avoid look-ahead bias. "
    "Use a two-step workflow: first scan with include_content=False to read headlines, summaries, timestamps, URLs, "
    "sources, and symbols. Use limit=10-20 for focused single-symbol checks, limit=30-50 for broad market or sector scans, "
    "and use page_token when next_page_token is returned to fetch more pages. Do not trade from one weak or noisy article. If a story matters, call "
    "again for the same/narrower window with include_content=True and usually exclude_contentless=True to read the full article body. Full content is "
    "not truncated unless you explicitly set content_max_chars. Use page_token when next_page_token is returned. "
    "If single-stock news is sparse, broaden intelligently: broad market SPY,QQQ,DIA,IWM; tech/AI/semis QQQ,XLK,SMH; "
    "financials/banks XLF,KRE; energy/oil XLE,USO; healthcare/biotech XLV,XBI; industrials XLI; consumer discretionary "
    "XLY; staples XLP; utilities XLU; materials XLB; real estate XLRE; rates/bonds TLT,IEF,SHY; gold/commodities GLD,SLV,DBC."
)


def _bind_alpaca_news(strategy: Any, manager: Any) -> BoundTool:
    def _warn_unavailable() -> None:
        message = (
            "[agents] alpaca_news is not configured and will not be exposed. "
            "Use an Alpaca broker connection or set ALPACA_NEWS_API_KEY and ALPACA_NEWS_API_SECRET."
        )
        if manager is not None:
            warned = getattr(manager, "_warned_unavailable_builtin_tools", None)
            if warned is None:
                warned = set()
                setattr(manager, "_warned_unavailable_builtin_tools", warned)
            if "alpaca_news" in warned:
                return
            warned.add("alpaca_news")
        log_message = getattr(strategy, "log_message", None)
        if callable(log_message):
            try:
                log_message(message, color="yellow")
                return
            except Exception:
                pass
        warning = getattr(manager, "warning", None) if manager is not None else None
        if callable(warning):
            warning(message)

    def _resolve_alpaca_news_headers() -> tuple[dict[str, str] | None, str | None]:
        # Prefer news-only credentials when supplied. They are intentionally
        # separate from generic Alpaca broker env vars so a Tradier/IBKR/etc.
        # strategy can use Alpaca/Benzinga news without changing broker routing.
        api_key = str(os.environ.get("ALPACA_NEWS_API_KEY") or "").strip()
        api_secret = str(os.environ.get("ALPACA_NEWS_API_SECRET") or "").strip()
        if api_key and api_secret:
            return {
                "APCA-API-KEY-ID": api_key,
                "APCA-API-SECRET-KEY": api_secret,
            }, "byok_alpaca_news_env"

        broker = getattr(strategy, "broker", None)
        if str(getattr(broker, "name", "") or "").lower() == "alpaca":
            oauth_token = str(getattr(broker, "oauth_token", "") or "").strip()
            if oauth_token:
                return {"Authorization": f"Bearer {oauth_token}"}, "alpaca_broker_oauth"

            api_key = str(getattr(broker, "api_key", "") or "").strip()
            api_secret = str(getattr(broker, "api_secret", "") or "").strip()
            if api_key and api_secret:
                return {
                    "APCA-API-KEY-ID": api_key,
                    "APCA-API-SECRET-KEY": api_secret,
                }, "alpaca_broker_api_key"

        return None, None

    def _unavailable_alpaca_news(**kwargs: Any) -> dict[str, Any]:
        return {
            "ok": False,
            "tool_error": True,
            "error": {
                "type": "MissingCredentials",
                "message": "alpaca_news is not configured. Use an Alpaca broker connection or set ALPACA_NEWS_API_KEY and ALPACA_NEWS_API_SECRET.",
            },
            "articles": [],
            "count": 0,
        }

    if _resolve_alpaca_news_headers()[0] is None:
        _warn_unavailable()
        return BoundTool(
            name="alpaca_news",
            description=ALPACA_NEWS_DESCRIPTION,
            function=_unavailable_alpaca_news,
            metadata={
                "kind": "builtin",
                "disabled": True,
                "disabled_reason": "missing Alpaca broker credentials or ALPACA_NEWS_API_KEY / ALPACA_NEWS_API_SECRET",
            },
        )

    def alpaca_news(
        *,
        symbols: str = "",
        start: str = "",
        end: str = "",
        limit: int = 30,
        include_content: bool = False,
        exclude_contentless: bool = False,
        page_token: str = "",
        content_max_chars: int | None = None,
        sort: NewsSortArg = "desc",
    ) -> dict[str, Any]:
        auth_headers, credential_source = _resolve_alpaca_news_headers()
        if not auth_headers:
            return {
                "ok": False,
                "tool_error": True,
                "error": {
                    "type": "MissingCredentials",
                    "message": "Use an Alpaca broker connection or set ALPACA_NEWS_API_KEY and ALPACA_NEWS_API_SECRET to use alpaca_news.",
                },
                "articles": [],
                "count": 0,
            }

        current_dt = strategy.get_datetime()
        if not end:
            end = current_dt.isoformat()
        if not start:
            start = (current_dt - timedelta(days=7)).isoformat()
        limit = max(1, min(int(limit), 50))
        sort = "asc" if str(sort).lower() == "asc" else "desc"
        content_limit: int | None = None
        if content_max_chars is not None:
            try:
                content_limit = int(content_max_chars)
            except Exception as exc:
                raise ValueError("content_max_chars must be an integer when provided.") from exc
            if content_limit <= 0:
                raise ValueError("content_max_chars must be greater than 0 when provided.")

        requested_end = str(end)
        lookahead_clamped = False
        if getattr(strategy, "is_backtesting", False):
            parsed_end = _parse_datetime_value(end)
            if parsed_end is not None:
                comparable_end = _coerce_same_timezone(parsed_end, current_dt)
                if comparable_end > current_dt:
                    end = current_dt.isoformat()
                    lookahead_clamped = True

        params: dict[str, Any] = {
            "start": start,
            "end": end,
            "sort": sort,
            "limit": limit,
            "include_content": bool(include_content),
            "exclude_contentless": bool(exclude_contentless),
        }
        if symbols:
            params["symbols"] = symbols
        if page_token:
            params["page_token"] = page_token

        response = _requests().get(
            "https://data.alpaca.markets/v1beta1/news",
            headers=auth_headers,
            params=params,
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        normalized_articles: list[dict[str, Any]] = []
        content_available_count = 0
        summary_available_count = 0
        for article in payload.get("news", []) or []:
            if not isinstance(article, dict):
                continue
            raw_content = str(article.get("content") or "")
            raw_summary = str(article.get("summary") or "")
            if raw_content:
                content_available_count += 1
            if raw_summary:
                summary_available_count += 1
            normalized: dict[str, Any] = {
                "id": article.get("id"),
                "headline": article.get("headline"),
                "summary": article.get("summary"),
                "author": article.get("author"),
                "source": article.get("source"),
                "created_at": article.get("created_at"),
                "updated_at": article.get("updated_at"),
                "url": article.get("url"),
                "symbols": article.get("symbols") or [],
                "content_available": bool(raw_content),
                "summary_available": bool(raw_summary),
            }
            if include_content and raw_content:
                normalized["content_original_length"] = len(raw_content)
                if content_limit is not None and len(raw_content) > content_limit:
                    normalized["content"] = raw_content[:content_limit]
                    normalized["content_truncated"] = True
                    normalized["content_max_chars"] = content_limit
                else:
                    normalized["content"] = raw_content
                    normalized["content_truncated"] = False
            normalized_articles.append(normalized)

        return {
            "ok": True,
            "provider": "alpaca",
            "source": "benzinga",
            "endpoint": "v1beta1/news",
            "credential_source": credential_source,
            "window_start": start,
            "window_end": end,
            "requested_end": requested_end,
            "effective_end": end,
            "lookahead_clamped": lookahead_clamped,
            "query_symbols": symbols,
            "include_content": bool(include_content),
            "content_included": bool(include_content),
            "count": len(normalized_articles),
            "content_available_count": content_available_count,
            "summary_available_count": summary_available_count,
            "next_page_token": payload.get("next_page_token"),
            "articles": normalized_articles,
        }

    return BoundTool(
        name="alpaca_news",
        description=ALPACA_NEWS_DESCRIPTION,
        function=alpaca_news,
        metadata={"kind": "builtin"},
    )


def _bind_open_orders(strategy: Any, manager: Any) -> BoundTool:
    def open_orders() -> dict[str, Any]:
        orders = strategy.get_orders()
        return {
            "orders": [_order_to_dict(order) for order in orders],
            "datetime": strategy.get_datetime().isoformat(),
        }

    return BoundTool(
        name="orders_open_orders",
        description="List the strategy's currently tracked orders, including identifiers, status, side, quantity, and prices.",
        function=open_orders,
        metadata={"kind": "builtin"},
    )


def _bind_cancel_order(strategy: Any, manager: Any) -> BoundTool:
    def cancel_order(*, identifier: str) -> dict[str, Any]:
        identifier = _require_non_empty_text("identifier", identifier)
        order = strategy.get_order(identifier)
        if order is None:
            raise ValueError(f"Unknown order identifier: {identifier}")
        strategy.cancel_order(order)
        return {"identifier": identifier, "status": getattr(order, "status", None) or "cancel_requested"}

    return BoundTool(
        name="orders_cancel_order",
        description=(
            "Cancel an existing tracked order by identifier. "
            "Arguments: identifier from orders_open_orders. "
            "Example: orders_cancel_order(identifier='bt_1')."
        ),
        function=cancel_order,
        metadata={"kind": "builtin", "replay_on_cache": True},
    )


def _bind_modify_order(strategy: Any, manager: Any) -> BoundTool:
    def modify_order(*, identifier: str, limit_price: float | None = None, stop_price: float | None = None) -> dict[str, Any]:
        identifier = _require_non_empty_text("identifier", identifier)
        order = strategy.get_order(identifier)
        if order is None:
            raise ValueError(f"Unknown order identifier: {identifier}")
        if limit_price is None and stop_price is None:
            raise ValueError("orders_modify_order requires at least one of limit_price or stop_price.")
        strategy.modify_order(order, limit_price=limit_price, stop_price=stop_price)
        return {
            "identifier": identifier,
            "limit_price": limit_price,
            "stop_price": stop_price,
        }

    return BoundTool(
        name="orders_modify_order",
        description=(
            "Modify an existing tracked order. "
            "Arguments: identifier, optional limit_price, optional stop_price. "
            "Example: orders_modify_order(identifier='bt_7', limit_price=101.25)."
        ),
        function=modify_order,
        metadata={"kind": "builtin", "replay_on_cache": True},
    )


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "as_dict"):
        return _jsonable(value.as_dict())
    if hasattr(value, "item"):
        try:
            return _jsonable(value.item())
        except Exception:
            pass
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    try:
        json.dumps(value)
        return value
    except TypeError:
        return str(value)
    return value


def _bind_list_indicators(strategy: Any, manager: Any) -> BoundTool:
    def list_indicators() -> dict[str, Any]:
        return {
            "ok": True,
            "common_indicators": COMMON_INDICATORS,
            "notes": (
                "Use get_indicator for one current-bar indicator value. "
                "Lumibot slices indicator outputs to the current strategy datetime, so backtests do not see future bars."
            ),
        }

    return BoundTool(
        name="list_indicators",
        description="List common pandas-ta-classic indicator names available through Lumibot's current-bar indicator system.",
        function=list_indicators,
        source="builtin",
        metadata={"kind": "indicator"},
    )


def _bind_get_indicator(strategy: Any, manager: Any) -> BoundTool:
    def get_indicator(
        symbol: str,
        indicator: str,
        timestep: str = "day",
        asset_type: AssetTypeArg = "stock",
        parameters_json: str | None = None,
    ) -> dict[str, Any]:
        asset = _asset_class()(symbol, asset_type=asset_type)
        indicator_name = _require_non_empty_text("indicator", indicator)
        indicator_kwargs: dict[str, Any] = {}
        if parameters_json:
            try:
                parsed = json.loads(parameters_json)
            except json.JSONDecodeError as exc:
                return {
                    "ok": False,
                    "tool_error": True,
                    "error": {
                        "type": "InvalidParametersJson",
                        "message": f"parameters_json must be valid JSON: {exc}",
                    },
                }
            if not isinstance(parsed, dict):
                return {
                    "ok": False,
                    "tool_error": True,
                    "error": {
                        "type": "InvalidParametersJson",
                        "message": "parameters_json must decode to a JSON object.",
                    },
                }
            indicator_kwargs = parsed
        fn = getattr(strategy.indicators, indicator_name)
        value = fn(asset, timestep=timestep, **indicator_kwargs)
        return {
            "ok": True,
            "symbol": symbol.upper(),
            "asset_type": asset_type,
            "indicator": indicator_name,
            "timestep": timestep,
            "datetime": strategy.get_datetime().isoformat() if hasattr(strategy.get_datetime(), "isoformat") else str(strategy.get_datetime()),
            "value": _jsonable(value),
            "no_lookahead": True,
        }

    return BoundTool(
        name="get_indicator",
        description=(
            "Get one technical indicator for the current strategy datetime. "
            "Arguments: symbol, indicator, timestep='day', asset_type='stock', optional parameters_json as a JSON object string. "
            "Examples: get_indicator(symbol='SPY', indicator='rsi', parameters_json='{\"length\": 14}'); "
            "get_indicator(symbol='NVDA', indicator='macd'). "
            "In backtests this returns only the current-bar value and does not expose future bars."
        ),
        function=get_indicator,
        source="builtin",
        metadata={"kind": "indicator"},
    )


def _bind_get_indicators(strategy: Any, manager: Any) -> BoundTool:
    def get_indicators(
        symbol: str,
        indicators: list[str],
        timestep: str = "day",
        asset_type: AssetTypeArg = "stock",
    ) -> dict[str, Any]:
        results = []
        single = _bind_get_indicator(strategy, manager).function
        for name in indicators:
            try:
                results.append(single(symbol=symbol, indicator=name, timestep=timestep, asset_type=asset_type))
            except Exception as exc:
                results.append({"ok": False, "indicator": name, "error": str(exc)})
        return {"ok": True, "symbol": symbol.upper(), "results": results}

    return BoundTool(
        name="get_indicators",
        description="Get multiple current-bar technical indicators for one symbol. Pass indicators=['rsi', 'macd', 'bbands', ...].",
        function=get_indicators,
        source="builtin",
        metadata={"kind": "indicator"},
    )


def _bind_get_income_statement(strategy: Any, manager: Any) -> BoundTool:
    def get_income_statement(symbol: str, as_of: str | None = None, raw: bool = False) -> dict[str, Any]:
        return strategy.fundamentals.get_income_statement(symbol, as_of=as_of, raw=raw)

    return BoundTool(
        name="get_income_statement",
        description=(
            "Get SEC income statement facts for a US equity, gated to as_of or the current strategy datetime. "
            "Fields are kept within one SEC filing/statement period when possible; mismatched old facts are omitted with warnings."
        ),
        function=get_income_statement,
        source="builtin",
        metadata={"kind": "fundamentals", "cache_scope": "strategy_day"},
    )


def _bind_get_balance_sheet(strategy: Any, manager: Any) -> BoundTool:
    def get_balance_sheet(symbol: str, as_of: str | None = None, raw: bool = False) -> dict[str, Any]:
        return strategy.fundamentals.get_balance_sheet(symbol, as_of=as_of, raw=raw)

    return BoundTool(
        name="get_balance_sheet",
        description=(
            "Get SEC balance sheet facts for a US equity, gated to as_of or the current strategy datetime. "
            "Fields are kept within one SEC filing/statement period when possible; mismatched old facts are omitted with warnings."
        ),
        function=get_balance_sheet,
        source="builtin",
        metadata={"kind": "fundamentals", "cache_scope": "strategy_day"},
    )


def _bind_get_cash_flow(strategy: Any, manager: Any) -> BoundTool:
    def get_cash_flow(symbol: str, as_of: str | None = None, raw: bool = False) -> dict[str, Any]:
        return strategy.fundamentals.get_cash_flow(symbol, as_of=as_of, raw=raw)

    return BoundTool(
        name="get_cash_flow",
        description=(
            "Get SEC cash flow facts for a US equity, gated to as_of or the current strategy datetime. "
            "Fields are kept within one SEC filing/statement period when possible; mismatched old facts are omitted with warnings."
        ),
        function=get_cash_flow,
        source="builtin",
        metadata={"kind": "fundamentals", "cache_scope": "strategy_day"},
    )


def _bind_get_company_facts(strategy: Any, manager: Any) -> BoundTool:
    def get_company_facts(
        symbol: str,
        as_of: str | None = None,
        raw: bool = False,
        max_facts: int | None = 80,
    ) -> dict[str, Any]:
        return strategy.fundamentals.get_company_facts(symbol, as_of=as_of, raw=raw, max_facts=max_facts)

    return BoundTool(
        name="get_company_facts",
        description=(
            "Get compact or raw SEC companyfacts for a US equity, gated to as_of or the current strategy datetime. "
            "Default output is capped to important/latest facts so agent runs stay within context; use max_facts or raw=True only when needed."
        ),
        function=get_company_facts,
        source="builtin",
        metadata={"kind": "fundamentals", "cache_scope": "strategy_day"},
    )


def _bind_get_filings(strategy: Any, manager: Any) -> BoundTool:
    def get_filings(symbol: str, form: str | None = None, as_of: str | None = None, limit: int = 10) -> dict[str, Any]:
        return strategy.fundamentals.get_filings(symbol, form=form, as_of=as_of, limit=limit)

    return BoundTool(
        name="get_filings",
        description=(
            "List SEC filings for a US equity, point-in-time gated by as_of/current strategy datetime. "
            "Use form='10-K' or form='10-Q' when you need annual or quarterly reports."
        ),
        function=get_filings,
        source="builtin",
        metadata={"kind": "filings", "cache_scope": "strategy_day"},
    )


def _bind_search_filing(strategy: Any, manager: Any) -> BoundTool:
    def search_filing(
        symbol: str,
        accession_number: str,
        query: str,
        primary_document: str | None = None,
        max_results: int = 5,
    ) -> dict[str, Any]:
        return strategy.fundamentals.search_filing(
            symbol,
            accession_number=accession_number,
            query=query,
            primary_document=primary_document,
            max_results=max_results,
        )

    return BoundTool(
        name="search_filing",
        description=(
            "Keyword-search a cached SEC filing document and return matching context snippets. "
            "Use after get_filings when you want annual-report details about risks, margins, debt, accounting, "
            "customers, liquidity, guidance, dilution, buybacks, or management commentary."
        ),
        function=search_filing,
        source="builtin",
        metadata={"kind": "filings", "cache_scope": "strategy_day"},
    )


def _bind_get_filing_document(strategy: Any, manager: Any) -> BoundTool:
    def get_filing_document(
        symbol: str,
        accession_number: str,
        primary_document: str | None = None,
        max_chars: int | None = 20000,
    ) -> dict[str, Any]:
        return strategy.fundamentals.get_filing_document(
            symbol,
            accession_number=accession_number,
            primary_document=primary_document,
            max_chars=max_chars,
        )

    return BoundTool(
        name="get_filing_document",
        description=(
            "Read a SEC filing document as text. This can be large, so prefer search_filing first. "
            "Use max_chars to bound context, or set max_chars=None only when you intentionally need the full document."
        ),
        function=get_filing_document,
        source="builtin",
        metadata={"kind": "filings", "cache_scope": "strategy_day"},
    )


def _bind_list_filing_sections(strategy: Any, manager: Any) -> BoundTool:
    def list_filing_sections(
        symbol: str,
        accession_number: str,
        primary_document: str | None = None,
    ) -> dict[str, Any]:
        return strategy.fundamentals.list_filing_sections(
            symbol,
            accession_number=accession_number,
            primary_document=primary_document,
        )

    return BoundTool(
        name="list_filing_sections",
        description=(
            "List detected sections in a SEC filing, such as item_1a risk factors, item_7 MD&A, "
            "item_7a market risk, and item_8 financial statements. Use after get_filings before reading a long report."
        ),
        function=list_filing_sections,
        source="builtin",
        metadata={"kind": "filings", "cache_scope": "strategy_day"},
    )


def _bind_get_filing_section(strategy: Any, manager: Any) -> BoundTool:
    def get_filing_section(
        symbol: str,
        accession_number: str,
        section: str,
        primary_document: str | None = None,
        max_chars: int | None = 12000,
    ) -> dict[str, Any]:
        return strategy.fundamentals.get_filing_section(
            symbol,
            accession_number=accession_number,
            section=section,
            primary_document=primary_document,
            max_chars=max_chars,
        )

    return BoundTool(
        name="get_filing_section",
        description=(
            "Read one sanitized text section from a SEC filing without loading the whole report. "
            "Useful section values include risk_factors, mda, liquidity, results_of_operations, market_risk, "
            "financial_statements, controls, or exact IDs like item_1a and item_7."
        ),
        function=get_filing_section,
        source="builtin",
        metadata={"kind": "filings", "cache_scope": "strategy_day"},
    )


def _disabled_fred_tool_if_needed(strategy: Any, manager: Any, tool_name: str) -> BoundTool | None:
    if not bool(getattr(strategy, "is_backtesting", False)):
        return None
    macro = getattr(strategy, "macro", None)
    api_key = str(getattr(macro, "api_key", "") or os.environ.get("FRED_API_KEY") or "").strip()
    if api_key:
        return None

    message = (
        "[agents] FRED macro tools are not configured for point-in-time backtesting and will not be exposed. "
        "Set FRED_API_KEY to use FRED/ALFRED vintage data in backtests."
    )
    if manager is not None:
        warned = getattr(manager, "_warned_unavailable_builtin_tools", None)
        if warned is None:
            warned = set()
            setattr(manager, "_warned_unavailable_builtin_tools", warned)
        if "fred_macro_tools" not in warned:
            warned.add("fred_macro_tools")
            log_message = getattr(strategy, "log_message", None)
            if callable(log_message):
                try:
                    log_message(message, color="yellow")
                except Exception:
                    warning = getattr(manager, "warning", None)
                    if callable(warning):
                        warning(message)
            else:
                warning = getattr(manager, "warning", None)
                if callable(warning):
                    warning(message)

    def unavailable_fred_tool(**kwargs: Any) -> dict[str, Any]:
        return {
            "ok": False,
            "tool_error": True,
            "error": {
                "type": "MissingCredentials",
                "message": "FRED macro tools require FRED_API_KEY during backtests to avoid revised-data look-ahead bias.",
            },
            "observations": [],
        }

    return BoundTool(
        name=tool_name,
        description="FRED macro tool unavailable in backtests without FRED_API_KEY.",
        function=unavailable_fred_tool,
        source="builtin",
        metadata={
            "kind": "macro",
            "disabled": True,
            "disabled_reason": "missing FRED_API_KEY for point-in-time backtesting",
        },
    )


def _bind_list_fred_series(strategy: Any, manager: Any) -> BoundTool:
    disabled = _disabled_fred_tool_if_needed(strategy, manager, "list_fred_series")
    if disabled is not None:
        return disabled

    def list_fred_series(category: str | None = None) -> dict[str, Any]:
        return strategy.macro.list_series(category=category)

    return BoundTool(
        name="list_fred_series",
        description=(
            "List curated Federal Reserve FRED macro series available to agents, grouped by category. "
            "Use this before requesting rates, inflation, labor, growth, liquidity, credit, or risk data."
        ),
        function=list_fred_series,
        source="builtin",
        metadata={"kind": "macro", "cache_scope": "strategy_day"},
    )


def _bind_get_fred_series(strategy: Any, manager: Any) -> BoundTool:
    disabled = _disabled_fred_tool_if_needed(strategy, manager, "get_fred_series")
    if disabled is not None:
        return disabled

    def get_fred_series(
        series_id: str,
        start: str | None = None,
        end: str | None = None,
        as_of: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        return strategy.macro.get_series(series_id, start=start, end=end, as_of=as_of, limit=limit)

    return BoundTool(
        name="get_fred_series",
        description=(
            "Get a FRED macro time series. In backtests, as_of defaults to the strategy datetime. "
            "Requires FRED_API_KEY and requests vintage data using realtime_start/realtime_end "
            "so backtests do not accidentally use future revisions."
        ),
        function=get_fred_series,
        source="builtin",
        metadata={"kind": "macro", "cache_scope": "strategy_day"},
    )


def _bind_get_fred_latest(strategy: Any, manager: Any) -> BoundTool:
    disabled = _disabled_fred_tool_if_needed(strategy, manager, "get_fred_latest")
    if disabled is not None:
        return disabled

    def get_fred_latest(series_id: str, as_of: str | None = None) -> dict[str, Any]:
        return strategy.macro.get_latest(series_id, as_of=as_of)

    return BoundTool(
        name="get_fred_latest",
        description=(
            "Get the latest FRED macro observation available as of the strategy datetime or explicit as_of date."
        ),
        function=get_fred_latest,
        source="builtin",
        metadata={"kind": "macro", "cache_scope": "strategy_day"},
    )


def _bind_get_fred_snapshot(strategy: Any, manager: Any) -> BoundTool:
    disabled = _disabled_fred_tool_if_needed(strategy, manager, "get_fred_snapshot")
    if disabled is not None:
        return disabled

    def get_fred_snapshot(series_ids: list[str] | str, as_of: str | None = None) -> dict[str, Any]:
        return strategy.macro.get_snapshot(series_ids, as_of=as_of)

    return BoundTool(
        name="get_fred_snapshot",
        description=(
            "Get latest available values for several FRED macro series as of the strategy datetime. "
            "Pass a list or comma-separated string such as FEDFUNDS,DGS10,CPIAUCSL,UNRATE."
        ),
        function=get_fred_snapshot,
        source="builtin",
        metadata={"kind": "macro", "cache_scope": "strategy_day"},
    )


def _bind_notify_user(strategy: Any, manager: Any) -> BoundTool:
    def notify_user(title: str, message: str, severity: str = "info", enabled: bool | None = None) -> dict[str, Any]:
        results = strategy.notify(title, message, severity=severity, enabled=enabled)
        return {"ok": all(result.ok for result in results), "results": [_jsonable(result.__dict__) for result in results]}

    return BoundTool(
        name="notify_user",
        description=(
            "Send a user notification through configured Lumibot notification providers. "
            "Backtests keep notifications disabled by default unless enabled=True is passed or notifications are configured as enabled."
        ),
        function=notify_user,
        source="builtin",
        metadata={"kind": "notification"},
    )


def _bind_memory_remember(strategy: Any, manager: Any) -> BoundTool:
    def remember(text: str, kind: str = "memory", tags: list[str] | None = None) -> dict[str, Any]:
        return strategy.memory.remember(text, kind=kind, tags=tags, **_agent_memory_context_kwargs())

    return BoundTool(name="remember", description="Store a local Lumibot agent memory or note.", function=remember, source="builtin", metadata={"kind": "memory"})


def _bind_memory_search(strategy: Any, manager: Any) -> BoundTool:
    def search_memory(
        query: str,
        limit: int = 10,
        kind: str | None = None,
        symbol: str | None = None,
        status: str | None = None,
    ) -> dict[str, Any]:
        return strategy.memory.search(
            query,
            limit=limit,
            kind=kind,
            symbol=symbol,
            status=status,
            **_agent_memory_context_kwargs(),
        )

    return BoundTool(
        name="search_memory",
        description=(
            "Search local Lumibot agent memories, lessons, decisions, and theses. "
            "Use symbol/status filters when checking an open thesis for a held position."
        ),
        function=search_memory,
        source="builtin",
        metadata={"kind": "memory"},
    )


def _bind_remember_decision(strategy: Any, manager: Any) -> BoundTool:
    def remember_decision(text: str, symbol: str | None = None, action: str | None = None) -> dict[str, Any]:
        return strategy.memory.remember_decision(text, symbol=symbol, action=action, **_agent_memory_context_kwargs())

    return BoundTool(
        name="remember_decision",
        description=(
            "Record an actual AI trading decision in the local decision journal. "
            "Use this for the final trading agent's executed or intentional decision, not for research proposals."
        ),
        function=remember_decision,
        source="builtin",
        metadata={"kind": "memory", "mutates_trading": True},
    )


def _bind_remember_proposal(strategy: Any, manager: Any) -> BoundTool:
    def remember_proposal(
        text: str,
        symbol: str | None = None,
        action: str | None = None,
        tags: list[str] | None = None,
    ) -> dict[str, Any]:
        return strategy.memory.remember_proposal(
            text,
            symbol=symbol,
            action=action,
            tags=tags,
            **_agent_memory_context_kwargs(),
        )

    return BoundTool(
        name="remember_proposal",
        description="Record a research proposal or non-final trade idea without marking it as an executed trading decision.",
        function=remember_proposal,
        source="builtin",
        metadata={"kind": "memory"},
    )


def _bind_remember_risk_note(strategy: Any, manager: Any) -> BoundTool:
    def remember_risk_note(
        text: str,
        symbol: str | None = None,
        tags: list[str] | None = None,
    ) -> dict[str, Any]:
        return strategy.memory.remember_risk_note(
            text,
            symbol=symbol,
            tags=tags,
            **_agent_memory_context_kwargs(),
        )

    return BoundTool(
        name="remember_risk_note",
        description="Record a compact risk note or bear-case memory without marking it as an executed trading decision.",
        function=remember_risk_note,
        source="builtin",
        metadata={"kind": "memory"},
    )


def _bind_remember_lesson(strategy: Any, manager: Any) -> BoundTool:
    def remember_lesson(text: str, symbol: str | None = None) -> dict[str, Any]:
        return strategy.memory.remember_lesson(text, symbol=symbol, **_agent_memory_context_kwargs())

    return BoundTool(name="remember_lesson", description="Record a compact trading lesson for future agent runs.", function=remember_lesson, source="builtin", metadata={"kind": "memory"})


def _bind_open_thesis(strategy: Any, manager: Any) -> BoundTool:
    def open_thesis(text: str, symbol: str | None = None, tags: list[str] | None = None) -> dict[str, Any]:
        return strategy.memory.open_thesis(text, symbol=symbol, tags=tags, **_agent_memory_context_kwargs())

    return BoundTool(name="open_thesis", description="Open a hedge-fund-style investment thesis in local Lumibot memory.", function=open_thesis, source="builtin", metadata={"kind": "memory"})


def _bind_update_thesis(strategy: Any, manager: Any) -> BoundTool:
    def update_thesis(thesis_id: str, text: str) -> dict[str, Any]:
        return strategy.memory.update_thesis(thesis_id, text, **_agent_memory_context_kwargs())

    return BoundTool(name="update_thesis", description="Append an update to an open investment thesis.", function=update_thesis, source="builtin", metadata={"kind": "memory"})


def _bind_close_thesis(strategy: Any, manager: Any) -> BoundTool:
    def close_thesis(thesis_id: str, text: str) -> dict[str, Any]:
        return strategy.memory.close_thesis(thesis_id, text, **_agent_memory_context_kwargs())

    return BoundTool(name="close_thesis", description="Close an investment thesis and record its outcome/reflection.", function=close_thesis, source="builtin", metadata={"kind": "memory"})


def _bind_submit_order(strategy: Any, manager: Any) -> BoundTool:
    def submit_order(
        *,
        symbol: str,
        quantity: float,
        side: OrderSideArg,
        asset_type: AssetTypeArg = "stock",
        expiration: str | None = None,
        strike: float | None = None,
        right: str | None = None,
        order_type: OrderTypeArg = "market",
        limit_price: float | None = None,
        stop_price: float | None = None,
        stop_limit_price: float | None = None,
        trail_price: float | None = None,
        trail_percent: float | None = None,
        quote_symbol: str | None = None,
        exchange: str | None = None,
        time_in_force: TimeInForceArg = "day",
    ) -> dict[str, Any]:
        symbol = _require_single_symbol_text("symbol", symbol)
        quantity = _require_positive_number("quantity", quantity)
        _require_agent_order_readiness(symbol)
        if order_type == "limit" and limit_price is None:
            raise ValueError("orders_submit_order with order_type='limit' requires limit_price.")
        if order_type in {"stop", "stop_limit"} and stop_price is None:
            raise ValueError(f"orders_submit_order with order_type={order_type!r} requires stop_price.")
        if order_type == "stop_limit" and stop_limit_price is None and limit_price is None:
            raise ValueError("orders_submit_order with order_type='stop_limit' requires stop_limit_price or limit_price.")
        if order_type == "trailing_stop" and trail_price is None and trail_percent is None:
            raise ValueError("orders_submit_order with order_type='trailing_stop' requires trail_price or trail_percent.")
        asset, quote = resolve_asset_and_quote(
            strategy,
            symbol=symbol,
            asset_type=asset_type,
            expiration=_coerce_expiration(expiration),
            strike=strike,
            right=right,
            quote_symbol=quote_symbol,
        )
        created = strategy.create_order(
            asset,
            quantity,
            side,
            order_type=order_type,
            limit_price=limit_price,
            stop_price=stop_price,
            stop_limit_price=stop_limit_price,
            trail_price=trail_price,
            trail_percent=trail_percent,
            exchange=exchange,
            quote=quote,
            time_in_force=time_in_force,
        )
        submitted = strategy.submit_order(created)
        order_payload = _order_to_dict(submitted)
        memory = getattr(strategy, "memory", None)
        if memory is not None and hasattr(memory, "record_order_submitted"):
            try:
                memory.record_order_submitted(
                    order=submitted,
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    order_type=order_type,
                    asset_type=asset_type,
                    limit_price=limit_price,
                    stop_price=stop_price,
                    stop_limit_price=stop_limit_price,
                    trail_price=trail_price,
                    trail_percent=trail_percent,
                    quote_symbol=quote_symbol,
                    exchange=exchange,
                    time_in_force=time_in_force,
                    order_payload=order_payload,
                    **_agent_memory_context_kwargs(),
                )
            except Exception:
                pass
        return {"order": order_payload}

    return BoundTool(
        name="orders_submit_order",
        description=(
            "Create and submit a LumiBot order. "
            "Arguments: symbol, quantity, side, optional asset_type, expiration, strike, right, order_type, limit_price, stop_price, stop_limit_price, trail_price, trail_percent, quote_symbol, exchange, time_in_force. "
            "Valid asset_type values: stock, option, future, cont_future, forex, crypto, index, multileg, us_equity. "
            "Use stock for normal equities. "
            "Before using this tool, call account_portfolio, account_positions, and market_last_price (or market_last_prices including the symbol) for the same symbol in the current agent run; otherwise the order is rejected with ORDER_READINESS_REQUIRED. "
            "Valid side values: buy, sell, buy_to_open, buy_to_close, sell_to_open, sell_to_close, sell_short, buy_to_cover. "
            "Valid order_type values: market, limit, stop, stop_limit, trailing_stop, smart_limit. "
            "Valid time_in_force values: day, gtc, gtd. "
            "Caveats: limit orders require limit_price; stop and stop_limit orders require stop_price; trailing_stop requires trail_price or trail_percent; smart_limit uses LumiBot's built-in smart-limit behavior. "
            "Example: orders_submit_order(symbol='SPY', quantity=100, side='buy', asset_type='stock', order_type='market')."
        ),
        function=submit_order,
        metadata={"kind": "builtin", "replay_on_cache": True},
    )


def _bind_submit_multileg_order(strategy: Any, manager: Any) -> BoundTool:
    def submit_multileg(
        *,
        legs_json: str,
        price_style: MultilegPriceStyleArg = "mid",
        net_limit_price: float | None = None,
        time_in_force: TimeInForceArg = "day",
    ) -> dict[str, Any]:
        orders = _parse_option_legs(strategy, legs_json, time_in_force=time_in_force)
        symbols = sorted({str(getattr(order.asset, "symbol", "")).upper() for order in orders})
        for symbol in symbols:
            _require_agent_order_readiness(symbol)

        submit_kwargs: dict[str, Any] = {
            "is_multileg": True,
            "duration": time_in_force,
        }
        resolved_net_price: float | None = None
        if price_style == "market":
            if net_limit_price is not None:
                raise ValueError("net_limit_price cannot be used when price_style='market'.")
            submit_kwargs["order_type"] = "market"
        else:
            if net_limit_price is None:
                calculated = _options_helper_for_strategy(strategy).calculate_multileg_limit_price(orders, price_style)
                if calculated is None:
                    raise ValueError(
                        "Unable to calculate a multi-leg limit price from the current quotes. Evaluate every leg or use price_style='market' only if your trading policy permits it."
                    )
                resolved_net_price = float(calculated)
            else:
                resolved_net_price = float(net_limit_price)
                if not math.isfinite(resolved_net_price):
                    raise ValueError("net_limit_price must be finite.")
            submit_kwargs["order_type"] = (
                "debit" if resolved_net_price > 0 else "credit" if resolved_net_price < 0 else "even"
            )
            submit_kwargs["price"] = abs(resolved_net_price)

        submitted = strategy.submit_order(orders, **submit_kwargs)
        submitted_orders = submitted if isinstance(submitted, list) else [submitted]
        return {
            "submitted": [_order_to_dict(order) for order in submitted_orders if order is not None],
            "legs": [_order_to_dict(order) for order in orders],
            "price_style": price_style,
            "net_limit_price": resolved_net_price,
            "order_type": submit_kwargs["order_type"],
            "time_in_force": time_in_force,
            "datetime": strategy.get_datetime().isoformat(),
        }

    return BoundTool(
        name="orders_submit_multileg",
        description=(
            "Create and submit one atomic multi-leg option order from exact contracts selected by the agent. This is generic and does not choose a strategy or its legs. "
            "Arguments: legs_json, optional price_style='market', 'best', 'mid', or 'fastest', optional signed net_limit_price, optional time_in_force. legs_json must be a JSON array with at least two legs; each leg requires symbol, expiration, strike, right, quantity, and side. "
            "Before submitting, call account_portfolio, account_positions, and market_last_price or market_last_prices for each underlying symbol, retrieve the chain, and evaluate every exact leg. "
            "Opening sides are buy_to_open and sell_to_open. Closing sides are buy_to_close and sell_to_close. Use matching quantities when the intended position requires matched contracts. "
            "When closing existing positions, map signed account quantities exactly: positive long quantity -> sell_to_close; negative short quantity -> buy_to_close. Reversing that mapping increases exposure instead of closing it. "
            "Every proposed closing leg must reduce the corresponding exact position quantity toward zero. Do not use the same closing side for positive and negative position quantities. "
            "Current nonzero option positions remain open until a later account_positions result shows zero quantity. A submitted or filled order result is not itself proof that positions are flat, and a final response must not claim submission unless this tool returned submitted orders. "
            "Before opening more option exposure, compare the proposed legs with all current option positions and pending orders. Do not add another structure when the strategy policy permits only one open structure. "
            "For limit execution, a positive signed net_limit_price is a debit and a negative value is a credit. If omitted, LumiBot calculates the selected best/mid/fastest price. "
            "The agent must validate that signed price against its exact leg quotes and strategy economics before submission. For equal-width credit spreads, credit must be positive and strictly less than the wing width. "
            "Use price_style='market' only when the strategy policy explicitly accepts market execution. The tool returns the submitted child orders and pricing classification."
        ),
        function=submit_multileg,
        metadata={"kind": "builtin", "replay_on_cache": True},
    )


class _AccountTools:
    def positions(self) -> ToolDefinition:
        return ToolDefinition(
            name="account_positions",
            description="Return current positions with asset fields and quantity.",
            binder=_bind_positions,
        )

    def portfolio(self) -> ToolDefinition:
        return ToolDefinition(
            name="account_portfolio",
            description="Return current cash and portfolio value for sizing decisions.",
            binder=_bind_portfolio,
        )


class _MarketTools:
    def last_price(self) -> ToolDefinition:
        return ToolDefinition(
            name="market_last_price",
            description="Get the current last price for one asset.",
            binder=_bind_last_price,
        )

    def last_prices(self) -> ToolDefinition:
        return ToolDefinition(
            name="market_last_prices",
            description="Get current last prices for many symbols in one JSON-friendly call.",
            binder=_bind_last_prices,
        )

    def historical_prices(self) -> ToolDefinition:
        return ToolDefinition(
            name="market_historical_prices",
            description="Get historical OHLCV bars for many symbols in one JSON-friendly call.",
            binder=_bind_historical_prices,
        )

    def load_history_table(self) -> ToolDefinition:
        return ToolDefinition(
            name="market_load_history_table",
            description="Load visible historical bars into DuckDB.",
            binder=_bind_load_history,
        )


class _OptionsTools:
    def get_chain(self) -> ToolDefinition:
        return ToolDefinition(name="options_get_chain", description="Retrieve an underlying's available option chain.", binder=_bind_options_get_chain)

    def get_strikes(self) -> ToolDefinition:
        return ToolDefinition(name="options_get_strikes", description="List strikes for one expiration and option right.", binder=_bind_options_get_strikes)

    def get_greeks(self) -> ToolDefinition:
        return ToolDefinition(name="options_get_greeks", description="Get Greeks for one exact option contract.", binder=_bind_options_get_greeks)

    def find_strike_for_delta(self) -> ToolDefinition:
        return ToolDefinition(name="options_find_strike_for_delta", description="Find a listed option strike closest to a target delta.", binder=_bind_options_find_strike_for_delta)

    def evaluate_market(self) -> ToolDefinition:
        return ToolDefinition(name="options_evaluate_market", description="Evaluate quote quality for one exact option contract.", binder=_bind_options_evaluate_market)

    def calculate_multileg_price(self) -> ToolDefinition:
        return ToolDefinition(name="options_calculate_multileg_price", description="Calculate a signed net price for exact option legs.", binder=_bind_options_calculate_multileg_price)

    def find_expiration(self) -> ToolDefinition:
        return ToolDefinition(name="options_find_expiration", description="Find a listed expiration on or after a target date.", binder=_bind_options_find_expiration)

    def check_spread_profit(self) -> ToolDefinition:
        return ToolDefinition(name="options_check_spread_profit", description="Estimate multi-leg spread P&L percentage from exact legs.", binder=_bind_options_check_spread_profit)


class _DuckDBTools:
    def query(self) -> ToolDefinition:
        return ToolDefinition(
            name="duckdb_query",
            description="Run a read-only SQL query against loaded DuckDB tables.",
            binder=_bind_duckdb_query,
        )


class _DocsTools:
    def search(self) -> ToolDefinition:
        return ToolDefinition(
            name="lumibot_docs_search",
            description="Search LumiBot's local documentation before guessing about tool or backtesting behavior.",
            binder=_bind_docs_search,
        )


class _NewsTools:
    def alpaca_news(self) -> ToolDefinition:
        return ToolDefinition(
            name="alpaca_news",
            description=ALPACA_NEWS_DESCRIPTION,
            binder=_bind_alpaca_news,
        )


class _IndicatorTools:
    def list_indicators(self) -> ToolDefinition:
        return ToolDefinition(name="list_indicators", description="List common technical indicators.", binder=_bind_list_indicators)

    def get_indicator(self) -> ToolDefinition:
        return ToolDefinition(name="get_indicator", description="Get one current-bar technical indicator.", binder=_bind_get_indicator)

    def get_indicators(self) -> ToolDefinition:
        return ToolDefinition(name="get_indicators", description="Get multiple current-bar technical indicators.", binder=_bind_get_indicators)


class _FundamentalTools:
    def income_statement(self) -> ToolDefinition:
        return ToolDefinition(name="get_income_statement", description="Get SEC income statement facts.", binder=_bind_get_income_statement)

    def balance_sheet(self) -> ToolDefinition:
        return ToolDefinition(name="get_balance_sheet", description="Get SEC balance sheet facts.", binder=_bind_get_balance_sheet)

    def cash_flow(self) -> ToolDefinition:
        return ToolDefinition(name="get_cash_flow", description="Get SEC cash flow facts.", binder=_bind_get_cash_flow)

    def company_facts(self) -> ToolDefinition:
        return ToolDefinition(name="get_company_facts", description="Get SEC companyfacts.", binder=_bind_get_company_facts)

    def filings(self) -> ToolDefinition:
        return ToolDefinition(name="get_filings", description="List SEC filings.", binder=_bind_get_filings)

    def search_filing(self) -> ToolDefinition:
        return ToolDefinition(name="search_filing", description="Search a SEC filing.", binder=_bind_search_filing)

    def filing_document(self) -> ToolDefinition:
        return ToolDefinition(name="get_filing_document", description="Read a SEC filing document.", binder=_bind_get_filing_document)

    def list_filing_sections(self) -> ToolDefinition:
        return ToolDefinition(name="list_filing_sections", description="List SEC filing sections.", binder=_bind_list_filing_sections)

    def filing_section(self) -> ToolDefinition:
        return ToolDefinition(name="get_filing_section", description="Read one SEC filing section.", binder=_bind_get_filing_section)


class _MacroTools:
    def list_fred_series(self) -> ToolDefinition:
        return ToolDefinition(name="list_fred_series", description="List curated FRED macro series.", binder=_bind_list_fred_series)

    def get_fred_series(self) -> ToolDefinition:
        return ToolDefinition(name="get_fred_series", description="Get a FRED macro time series.", binder=_bind_get_fred_series)

    def get_fred_latest(self) -> ToolDefinition:
        return ToolDefinition(name="get_fred_latest", description="Get the latest FRED macro observation.", binder=_bind_get_fred_latest)

    def get_fred_snapshot(self) -> ToolDefinition:
        return ToolDefinition(name="get_fred_snapshot", description="Get a multi-series FRED macro snapshot.", binder=_bind_get_fred_snapshot)


class _NotificationTools:
    def notify_user(self) -> ToolDefinition:
        return ToolDefinition(name="notify_user", description="Send a user notification.", binder=_bind_notify_user)


class _MemoryTools:
    def remember(self) -> ToolDefinition:
        return ToolDefinition(name="remember", description="Store a local memory.", binder=_bind_memory_remember)

    def search(self) -> ToolDefinition:
        return ToolDefinition(name="search_memory", description="Search local memories.", binder=_bind_memory_search)

    def remember_decision(self) -> ToolDefinition:
        return ToolDefinition(
            name="remember_decision",
            description="Record an actual trading decision.",
            binder=_bind_remember_decision,
            metadata={"mutates_trading": True},
        )

    def remember_proposal(self) -> ToolDefinition:
        return ToolDefinition(name="remember_proposal", description="Record a non-final trade proposal.", binder=_bind_remember_proposal)

    def remember_risk_note(self) -> ToolDefinition:
        return ToolDefinition(name="remember_risk_note", description="Record a compact risk note.", binder=_bind_remember_risk_note)

    def remember_lesson(self) -> ToolDefinition:
        return ToolDefinition(name="remember_lesson", description="Record a compact lesson.", binder=_bind_remember_lesson)

    def open_thesis(self) -> ToolDefinition:
        return ToolDefinition(name="open_thesis", description="Open an investment thesis.", binder=_bind_open_thesis)

    def update_thesis(self) -> ToolDefinition:
        return ToolDefinition(name="update_thesis", description="Update an investment thesis.", binder=_bind_update_thesis)

    def close_thesis(self) -> ToolDefinition:
        return ToolDefinition(name="close_thesis", description="Close an investment thesis.", binder=_bind_close_thesis)


class _OrderTools:
    def submit(self) -> ToolDefinition:
        return ToolDefinition(
            name="orders_submit_order",
            description="Submit an order with explicit side/type/time_in_force.",
            binder=_bind_submit_order,
            metadata={"mutates_trading": True},
        )

    def cancel(self) -> ToolDefinition:
        return ToolDefinition(
            name="orders_cancel_order",
            description="Cancel a tracked order by identifier.",
            binder=_bind_cancel_order,
            metadata={"mutates_trading": True},
        )

    def submit_multileg(self) -> ToolDefinition:
        return ToolDefinition(
            name="orders_submit_multileg",
            description="Submit one atomic multi-leg option order from exact agent-selected contracts.",
            binder=_bind_submit_multileg_order,
            metadata={"mutates_trading": True},
        )

    def open_orders(self) -> ToolDefinition:
        return ToolDefinition(name="orders_open_orders", description="List tracked orders and their identifiers.", binder=_bind_open_orders)

    def get_status(self) -> ToolDefinition:
        return ToolDefinition(name="orders_get_status", description="Get status for one or more tracked order identifiers.", binder=_bind_orders_get_status)

    def wait_for_terminal(self) -> ToolDefinition:
        return ToolDefinition(
            name="orders_wait_for_terminal",
            description="Poll tracked order identifiers until terminal or timeout.",
            binder=_bind_orders_wait_for_terminal,
        )

    def modify(self) -> ToolDefinition:
        return ToolDefinition(
            name="orders_modify_order",
            description="Modify a tracked order by identifier.",
            binder=_bind_modify_order,
            metadata={"mutates_trading": True},
        )


class _BuiltinTools:
    account = _AccountTools()
    market = _MarketTools()
    options = _OptionsTools()
    duckdb = _DuckDBTools()
    docs = _DocsTools()
    news = _NewsTools()
    indicators = _IndicatorTools()
    fundamentals = _FundamentalTools()
    macro = _MacroTools()
    notifications = _NotificationTools()
    memory = _MemoryTools()
    orders = _OrderTools()

    def all(self) -> list[ToolDefinition]:
        """Return all built-in tools. Used as the default when tools=None in agent creation."""
        return [
            self.account.positions(),
            self.account.portfolio(),
            self.market.last_price(),
            self.market.last_prices(),
            self.market.historical_prices(),
            self.market.load_history_table(),
            self.options.get_chain(),
            self.options.get_strikes(),
            self.options.get_greeks(),
            self.options.find_strike_for_delta(),
            self.options.find_expiration(),
            self.options.evaluate_market(),
            self.options.calculate_multileg_price(),
            self.options.check_spread_profit(),
            self.duckdb.query(),
            self.docs.search(),
            self.news.alpaca_news(),
            self.indicators.list_indicators(),
            self.indicators.get_indicator(),
            self.indicators.get_indicators(),
            self.fundamentals.income_statement(),
            self.fundamentals.balance_sheet(),
            self.fundamentals.cash_flow(),
            self.fundamentals.company_facts(),
            self.fundamentals.filings(),
            self.fundamentals.search_filing(),
            self.fundamentals.filing_document(),
            self.fundamentals.list_filing_sections(),
            self.fundamentals.filing_section(),
            self.macro.list_fred_series(),
            self.macro.get_fred_series(),
            self.macro.get_fred_latest(),
            self.macro.get_fred_snapshot(),
            self.notifications.notify_user(),
            self.memory.remember(),
            self.memory.search(),
            self.memory.remember_proposal(),
            self.memory.remember_risk_note(),
            self.memory.remember_decision(),
            self.memory.remember_lesson(),
            self.memory.open_thesis(),
            self.memory.update_thesis(),
            self.memory.close_thesis(),
            self.orders.submit(),
            self.orders.submit_multileg(),
            self.orders.cancel(),
            self.orders.open_orders(),
            self.orders.get_status(),
            self.orders.wait_for_terminal(),
            self.orders.modify(),
        ]


BuiltinTools = _BuiltinTools()
