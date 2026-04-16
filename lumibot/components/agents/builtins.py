import math
from datetime import date, datetime
from typing import Any, Literal

from lumibot.entities import Order

from .docs_tools import search_lumibot_docs
from .asset_resolution import resolve_asset_and_quote
from .schemas import BoundTool, ToolDefinition


AssetTypeArg = Literal["stock", "option", "future", "cont_future", "forex", "crypto", "index", "multileg", "us_equity"]
OrderSideArg = Literal["buy", "sell", "buy_to_open", "sell_to_close", "sell_short", "buy_to_cover"]
OrderTypeArg = Literal["market", "limit", "stop", "stop_limit", "trailing_stop", "smart_limit"]
TimeInForceArg = Literal["day", "gtc", "gtd"]


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
            "Use stock for normal equities. Example: market_last_price(symbol='SPY', asset_type='stock')."
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
            "Use stock for normal equities. If asset_type is omitted, stock is assumed. "
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


class _OrderTools:
    def submit(self) -> ToolDefinition:
        return ToolDefinition(name="orders_submit_order", description="Submit an order with explicit side/type/time_in_force.", binder=_bind_submit_order)

    def cancel(self) -> ToolDefinition:
        return ToolDefinition(name="orders_cancel_order", description="Cancel a tracked order by identifier.", binder=_bind_cancel_order)

    def open_orders(self) -> ToolDefinition:
        return ToolDefinition(name="orders_open_orders", description="List tracked orders and their identifiers.", binder=_bind_open_orders)

    def modify(self) -> ToolDefinition:
        return ToolDefinition(name="orders_modify_order", description="Modify a tracked order by identifier.", binder=_bind_modify_order)


class _BuiltinTools:
    account = _AccountTools()
    market = _MarketTools()
    duckdb = _DuckDBTools()
    docs = _DocsTools()
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
            self.orders.submit(),
            self.orders.cancel(),
            self.orders.open_orders(),
            self.orders.modify(),
        ]


BuiltinTools = _BuiltinTools()
