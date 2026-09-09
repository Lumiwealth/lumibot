"""Isolated market/broker fixtures beneath the production strategy and agent tools.

No replacement agent-facing trading functions live here. Orders are real LumiBot
orders, and fills are produced by BacktestingBroker against fixture OHLCV data.
"""

from __future__ import annotations

import json
import sys
import tempfile
import uuid
from datetime import date
from pathlib import Path

import pandas as pd

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.components.agents import MCPServer
from lumibot.entities import Asset, Data, Position
from lumibot.strategies import Strategy


class _NoRemoteCache:
    def ensure_local_file(self, path):
        return None

    def on_local_update(self, path):
        return None


class _EvalStrategy(Strategy):
    def initialize(self):
        self.set_market("24/7")

    def on_trading_iteration(self):
        return None


class _FixtureBroker(BacktestingBroker):
    """Recorded Greeks are a broker response; execution uses the real broker."""

    def get_greeks(self, asset, **kwargs):
        delta = self.fixture.greek(float(asset.strike), str(asset.right).lower())
        return {"delta": delta, "gamma": 0.012, "theta": -0.05, "vega": 0.10, "rho": 0.02, "implied_volatility": 0.2}


def _history(asset, fixture, quote_asset=None):
    # Minute observations include today's opening range and prior daily closes.
    now = pd.Timestamp("2026-08-11T14:35:00Z")
    index = pd.date_range("2026-08-04T13:30:00Z", now + pd.Timedelta(minutes=2), freq="min")
    if asset.asset_type == "option":
        market_quote = fixture.quote(float(asset.strike), str(asset.right).lower())
        price = market_quote["last"]
    elif asset.symbol == "BTC" and asset.asset_type == "crypto":
        price = 100_000.0
        market_quote = {"bid": price - 5.0, "ask": price + 5.0}
    elif asset.symbol == "BTC":
        price = 10.0
        market_quote = {"bid": 9.95, "ask": 10.05}
    else:
        price = 230.0 if asset.symbol == "AAPL" else fixture.underlying_price
        market_quote = {"bid": price - 0.05, "ask": price + 0.05}
    frame = pd.DataFrame(
        {"open": price, "high": price + 0.05, "low": price - 0.05, "close": price, "volume": 1000, **market_quote}, index=index
    )
    if asset.symbol == "AAPL" and asset.asset_type == "stock":
        closes = [
            227.1,
            227.2,
            227.3,
            227.4,
            227.6,
            227.7,
            227.8,
            227.9,
            228.0,
            228.1,
            228.15,
            228.2,
            228.25,
            228.28,
            228.3,
            228.6,
            228.9,
            229.2,
            229.6,
            230.0,
        ]
        for i, close in enumerate(closes):
            at = pd.Timestamp("2026-08-11T13:30:00Z") + pd.Timedelta(minutes=i)
            previous = closes[i - 1] if i else 227.0
            frame.loc[at, ["open", "high", "low", "close", "volume"]] = [
                previous,
                max(close + 0.05, [228.0, 228.4, 228.5, 230.2][i // 5] if i % 5 == 4 else close),
                min(previous, close) - 0.1,
                close,
                [200, 220, 210, 480][i // 5],
            ]
    return Data(asset, frame, quote=quote_asset, timestep="minute")


class ProductionFixture:
    def __init__(self, fixture):
        self.fixture = fixture
        self.directory = tempfile.TemporaryDirectory(prefix="lumibot-agent-eval-")
        self.root = Path(self.directory.name)
        self.closed = False
        assets = [Asset("AAPL"), Asset("SPY")]
        for right, strikes in (("put", (592, 594, 596, 598)), ("call", (602, 604, 606, 608))):
            assets.extend(
                Asset(
                    "SPY",
                    asset_type="option",
                    expiration=date.fromisoformat(fixture.expiration),
                    strike=strike,
                    right=right,
                )
                for strike in strikes
            )
        data = [_history(asset, fixture) for asset in assets]
        if fixture.name == "crypto_instrument_identity":
            stock_btc = Asset("BTC", asset_type="stock")
            crypto_btc = Asset("BTC", asset_type="crypto")
            crypto_usd = Asset("USD", asset_type="crypto")
            data.extend(
                [
                    _history(stock_btc, fixture),
                    _history(crypto_btc, fixture, quote_asset=crypto_usd),
                ]
            )
        source = PandasDataBacktesting(
            pandas_data=data,
            datetime_start=pd.Timestamp("2026-08-11T14:35:00Z"),
            datetime_end=pd.Timestamp("2026-08-11T14:37:00Z"),
            show_progress_bar=False,
            market="24/7",
        )
        source.load_data()
        self.broker = _FixtureBroker(data_source=source)
        self.broker.fixture = fixture
        self.broker.initialize_market_calendars(source.get_trading_days_pandas())
        self.broker._first_iteration = False
        self.strategy = _EvalStrategy(
            broker=self.broker,
            budget=100_000.0,
            name=f"eval_{uuid.uuid4().hex}",
            risk_free_rate=0.04,
            analyze_backtest=False,
            parameters={},
        )
        self.strategy._first_iteration = False
        for position in fixture.positions:
            value = dict(position)
            quantity = value.pop("quantity")
            if value.get("expiration"):
                value["expiration"] = date.fromisoformat(value["expiration"])
            self.broker._filled_positions.append(Position(self.strategy.name, Asset(**value), quantity))
        if fixture.name == "stock_pending_exit":
            order = self.strategy.create_order(Asset("AAPL"), 40, "sell", order_type="limit", limit_price=250.0)
            order.identifier = "bt_pending_exit"
            self.strategy.submit_order(order)
        self.manager = self.strategy.agents
        self.manager.replay_cache.root = self.root / "replay"
        self.manager.replay_cache.remote_cache = _NoRemoteCache()

    def create_agent(self, case, runtime, *, name="trader", allow_trading=True, system_prompt=None):
        rules = self.root / "rules.json"
        rules.write_text(json.dumps(case.get("rules") or {"version": 1, "rules": []}), encoding="utf-8")
        servers = []
        if str(case.get("fixture", "")).startswith("research"):
            servers = [
                MCPServer(
                    name="botspot_research",
                    command=sys.executable,
                    args=[str(Path(__file__).with_name("agent_eval_research_server.py")), self.fixture.name],
                    exposed_tools=["search_data_catalog", "query_data", "search_documents", "get_document"],
                )
            ]
        return self.manager.create(
            name=name,
            model=case["model"],
            system_prompt=system_prompt or case["systemPrompt"],
            rules_path=rules,
            _runtime=runtime,
            mcp_servers=servers,
            allow_trading=allow_trading,
        )

    def tools(self):
        from lumibot.components.agents import BuiltinTools

        return [definition.binder(self.strategy, self.manager) for definition in BuiltinTools.all()]

    def settle(self):
        self.broker.process_pending_orders(self.strategy)

    def capture(self, result):
        from lumibot.components.agents.builtins import _order_to_dict, _position_to_dict

        self.settle()
        self.fixture.calls = [{"name": event.tool_name, "arguments": event.payload} for event in result.tool_calls]
        self.fixture.positions = [
            _position_to_dict(p) for p in self.strategy.get_positions() if p.asset.symbol != "USD"
        ]
        self.fixture.submissions = []
        for event in result.tool_calls:
            if event.tool_name not in {"orders_submit_order", "orders_submit_multileg"}:
                continue
            args = event.payload or {}
            if event.tool_name == "orders_submit_multileg":
                legs = args.get("legs_json", "[]")
                self.fixture.submissions.append(
                    {
                        "tool": event.tool_name,
                        "legs": json.loads(legs) if isinstance(legs, str) else legs,
                        "net_limit_price": args.get("net_limit_price"),
                    }
                )
            else:
                self.fixture.submissions.append({"tool": event.tool_name, **args})
        return [_order_to_dict(order) for order in self.strategy.get_orders()]

    def close(self):
        if self.closed:
            return
        self.closed = True
        self.broker.stream.stop()
        self.directory.cleanup()
