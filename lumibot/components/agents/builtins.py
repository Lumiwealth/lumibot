import math
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal

import requests

from lumibot.entities import Asset, Order

from .docs_tools import search_lumibot_docs
from .asset_resolution import resolve_asset_and_quote
from .schemas import BoundTool, ToolDefinition


AssetTypeArg = Literal["stock", "option", "future", "cont_future", "forex", "crypto", "index", "multileg", "us_equity"]
OrderSideArg = Literal["buy", "sell", "buy_to_open", "sell_to_close", "sell_short", "buy_to_cover"]
OrderTypeArg = Literal["market", "limit", "stop", "stop_limit", "trailing_stop", "smart_limit"]
TimeInForceArg = Literal["day", "gtc", "gtd"]
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
        "identifier": getattr(order, "identifier", None),
        "status": getattr(order, "status", None),
        "side": getattr(order, "side", None),
        "asset": asset_payload,
        "quantity": quantity,
        "order_type": getattr(order, "order_type", None),
        "time_in_force": getattr(order, "time_in_force", None),
        "limit_price": getattr(order, "limit_price", None),
        "stop_price": getattr(order, "stop_price", None),
    }


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
            "Each entry includes asset fields and quantity. "
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
        symbol = _require_non_empty_text("symbol", symbol)
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
            "Use stock for normal equities. Do not pass economic series ids such as DCOILWTICO, FEDFUNDS, or M2SL as market symbols; use macro/FRED tools for those instead. "
            "Example: market_last_price(symbol='SPY', asset_type='stock')."
        ),
        function=last_price,
        metadata={"kind": "builtin", "replay_on_cache": True},
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
        symbol = _require_non_empty_text("symbol", symbol)
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
            "Load visible historical bars into DuckDB and return the table metadata. "
            "Arguments: symbol, length, timestep, optional table_name, asset_type, quote_symbol, exchange, expiration, strike, right, include_after_hours. "
            "Valid asset_type values: stock, option, future, cont_future, forex, crypto, index, multileg, us_equity. "
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
    def docs_search(*, query: str, max_results: int = 5) -> dict[str, Any]:
        query = _require_non_empty_text("query", query)
        max_results = _require_positive_int("max_results", max_results)
        return search_lumibot_docs(query=query, max_results=max_results)

    return BoundTool(
        name="lumibot_docs_search",
        description=(
            "Search LumiBot's local documentation and return the best matching snippets. "
            "Arguments: query, optional max_results. "
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
        api_key = (
            os.environ.get("ALPACA_API_KEY")
            or os.environ.get("APCA_API_KEY_ID")
            or ""
        ).strip()
        api_secret = (
            os.environ.get("ALPACA_API_SECRET")
            or os.environ.get("APCA_API_SECRET_KEY")
            or ""
        ).strip()
        if not api_key or not api_secret:
            return {
                "ok": False,
                "tool_error": True,
                "error": {
                    "type": "MissingCredentials",
                    "message": "Set ALPACA_API_KEY and ALPACA_API_SECRET to use alpaca_news.",
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

        response = requests.get(
            "https://data.alpaca.markets/v1beta1/news",
            headers={
                "APCA-API-KEY-ID": api_key,
                "APCA-API-SECRET-KEY": api_secret,
            },
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
    if hasattr(value, "as_dict"):
        return value.as_dict()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
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
        kwargs: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        asset = Asset(symbol, asset_type=asset_type)
        indicator_name = _require_non_empty_text("indicator", indicator)
        fn = getattr(strategy.indicators, indicator_name)
        value = fn(asset, timestep=timestep, **(kwargs or {}))
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
            "Arguments: symbol, indicator, timestep='day', asset_type='stock', kwargs={...}. "
            "Examples: get_indicator(symbol='SPY', indicator='rsi', kwargs={'length': 14}); "
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
        description="Get SEC income statement facts for a US equity, gated to as_of or the current strategy datetime.",
        function=get_income_statement,
        source="builtin",
        metadata={"kind": "fundamentals"},
    )


def _bind_get_balance_sheet(strategy: Any, manager: Any) -> BoundTool:
    def get_balance_sheet(symbol: str, as_of: str | None = None, raw: bool = False) -> dict[str, Any]:
        return strategy.fundamentals.get_balance_sheet(symbol, as_of=as_of, raw=raw)

    return BoundTool(
        name="get_balance_sheet",
        description="Get SEC balance sheet facts for a US equity, gated to as_of or the current strategy datetime.",
        function=get_balance_sheet,
        source="builtin",
        metadata={"kind": "fundamentals"},
    )


def _bind_get_cash_flow(strategy: Any, manager: Any) -> BoundTool:
    def get_cash_flow(symbol: str, as_of: str | None = None, raw: bool = False) -> dict[str, Any]:
        return strategy.fundamentals.get_cash_flow(symbol, as_of=as_of, raw=raw)

    return BoundTool(
        name="get_cash_flow",
        description="Get SEC cash flow facts for a US equity, gated to as_of or the current strategy datetime.",
        function=get_cash_flow,
        source="builtin",
        metadata={"kind": "fundamentals"},
    )


def _bind_get_company_facts(strategy: Any, manager: Any) -> BoundTool:
    def get_company_facts(symbol: str, as_of: str | None = None, raw: bool = False) -> dict[str, Any]:
        return strategy.fundamentals.get_company_facts(symbol, as_of=as_of, raw=raw)

    return BoundTool(
        name="get_company_facts",
        description="Get compact or raw SEC companyfacts for a US equity, gated to as_of or the current strategy datetime.",
        function=get_company_facts,
        source="builtin",
        metadata={"kind": "fundamentals"},
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
        metadata={"kind": "filings"},
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
        metadata={"kind": "filings"},
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
        metadata={"kind": "filings"},
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
        return strategy.memory.remember(text, kind=kind, tags=tags)

    return BoundTool(name="remember", description="Store a local Lumibot agent memory or note as JSONL.", function=remember, source="builtin", metadata={"kind": "memory"})


def _bind_memory_search(strategy: Any, manager: Any) -> BoundTool:
    def search_memory(query: str, limit: int = 10, kind: str | None = None) -> dict[str, Any]:
        return strategy.memory.search(query, limit=limit, kind=kind)

    return BoundTool(name="search_memory", description="Search local Lumibot agent memories, lessons, decisions, and theses.", function=search_memory, source="builtin", metadata={"kind": "memory"})


def _bind_remember_decision(strategy: Any, manager: Any) -> BoundTool:
    def remember_decision(text: str, symbol: str | None = None, action: str | None = None) -> dict[str, Any]:
        return strategy.memory.remember_decision(text, symbol=symbol, action=action)

    return BoundTool(name="remember_decision", description="Record an AI trading decision in the local decision journal.", function=remember_decision, source="builtin", metadata={"kind": "memory"})


def _bind_remember_lesson(strategy: Any, manager: Any) -> BoundTool:
    def remember_lesson(text: str, symbol: str | None = None) -> dict[str, Any]:
        return strategy.memory.remember_lesson(text, symbol=symbol)

    return BoundTool(name="remember_lesson", description="Record a compact trading lesson for future agent runs.", function=remember_lesson, source="builtin", metadata={"kind": "memory"})


def _bind_open_thesis(strategy: Any, manager: Any) -> BoundTool:
    def open_thesis(text: str, symbol: str | None = None, tags: list[str] | None = None) -> dict[str, Any]:
        return strategy.memory.open_thesis(text, symbol=symbol, tags=tags)

    return BoundTool(name="open_thesis", description="Open a hedge-fund-style investment thesis in local Lumibot memory.", function=open_thesis, source="builtin", metadata={"kind": "memory"})


def _bind_update_thesis(strategy: Any, manager: Any) -> BoundTool:
    def update_thesis(thesis_id: str, text: str) -> dict[str, Any]:
        return strategy.memory.update_thesis(thesis_id, text)

    return BoundTool(name="update_thesis", description="Append an update to an open investment thesis.", function=update_thesis, source="builtin", metadata={"kind": "memory"})


def _bind_close_thesis(strategy: Any, manager: Any) -> BoundTool:
    def close_thesis(thesis_id: str, text: str) -> dict[str, Any]:
        return strategy.memory.close_thesis(thesis_id, text)

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
        symbol = _require_non_empty_text("symbol", symbol)
        quantity = _require_positive_number("quantity", quantity)
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
        return {"order": _order_to_dict(submitted)}

    return BoundTool(
        name="orders_submit_order",
        description=(
            "Create and submit a LumiBot order. "
            "Arguments: symbol, quantity, side, optional asset_type, expiration, strike, right, order_type, limit_price, stop_price, stop_limit_price, trail_price, trail_percent, quote_symbol, exchange, time_in_force. "
            "Valid asset_type values: stock, option, future, cont_future, forex, crypto, index, multileg, us_equity. "
            "Use stock for normal equities. "
            "Valid side values: buy, sell, buy_to_open, sell_to_close, sell_short, buy_to_cover. "
            "Valid order_type values: market, limit, stop, stop_limit, trailing_stop, smart_limit. "
            "Valid time_in_force values: day, gtc, gtd. "
            "Caveats: limit orders require limit_price; stop and stop_limit orders require stop_price; trailing_stop requires trail_price or trail_percent; smart_limit uses LumiBot's built-in smart-limit behavior. "
            "Example: orders_submit_order(symbol='SPY', quantity=100, side='buy', asset_type='stock', order_type='market')."
        ),
        function=submit_order,
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

    def load_history_table(self) -> ToolDefinition:
        return ToolDefinition(
            name="market_load_history_table",
            description="Load visible historical bars into DuckDB.",
            binder=_bind_load_history,
        )


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


class _NotificationTools:
    def notify_user(self) -> ToolDefinition:
        return ToolDefinition(name="notify_user", description="Send a user notification.", binder=_bind_notify_user)


class _MemoryTools:
    def remember(self) -> ToolDefinition:
        return ToolDefinition(name="remember", description="Store a local memory.", binder=_bind_memory_remember)

    def search(self) -> ToolDefinition:
        return ToolDefinition(name="search_memory", description="Search local memories.", binder=_bind_memory_search)

    def remember_decision(self) -> ToolDefinition:
        return ToolDefinition(name="remember_decision", description="Record an agent decision.", binder=_bind_remember_decision)

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

    def open_orders(self) -> ToolDefinition:
        return ToolDefinition(name="orders_open_orders", description="List tracked orders and their identifiers.", binder=_bind_open_orders)

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
    duckdb = _DuckDBTools()
    docs = _DocsTools()
    news = _NewsTools()
    indicators = _IndicatorTools()
    fundamentals = _FundamentalTools()
    notifications = _NotificationTools()
    memory = _MemoryTools()
    orders = _OrderTools()

    def all(self) -> list[ToolDefinition]:
        """Return all built-in tools. Used as the default when tools=None in agent creation."""
        return [
            self.account.positions(),
            self.account.portfolio(),
            self.market.last_price(),
            self.market.load_history_table(),
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
            self.notifications.notify_user(),
            self.memory.remember(),
            self.memory.search(),
            self.memory.remember_decision(),
            self.memory.remember_lesson(),
            self.memory.open_thesis(),
            self.memory.update_thesis(),
            self.memory.close_thesis(),
            self.orders.submit(),
            self.orders.cancel(),
            self.orders.open_orders(),
            self.orders.modify(),
        ]


BuiltinTools = _BuiltinTools()
