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
        expiration = getattr(asset, "expiration", None)
        text = expiration.isoformat() if isinstance(expiration, date) else str(expiration or "")[:10]
        if text in set(getattr(self.fixture, "empty_expirations", ()) or ()):
            return None
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
        # Earlier sessions step up one dollar a day to 229.00 on August 10, so
        # the premise of stock_price_before_order ("current price and recent
        # completed daily bars confirm it remains above its five-day average")
        # holds on the evidence: each recent completed close and today's
        # 230.00 sit above the five-day average. With every session at 230.00
        # the price only equalled the average, and GPT-6 Luna rightly declined.
        for day in range(4, 11):
            session = (frame.index >= pd.Timestamp(f"2026-08-{day:02d}T04:00:00Z")) & (
                frame.index < pd.Timestamp(f"2026-08-{day + 1:02d}T04:00:00Z")
            )
            close = 222.0 + (day - 3)
            frame.loc[session, ["open", "high", "low", "close", "bid", "ask"]] = [
                close, close + 0.05, close - 0.05, close, close - 0.05, close + 0.05,
            ]
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


# Recorded SEC ticker map for the built-in SEC tools. ACME, the research
# fixtures' company, is fictional and deliberately absent: EDGAR has no filings
# for it, and its SEC evidence comes only from the managed research fixture.
_RECORDED_SEC_TICKERS = {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}}


def _recorded_sec_fundamentals(strategy, cache_dir):
    """Production SEC client on a private recorded cache.

    Without this the built-in SEC tools read the developer's ~/.lumibot SEC
    cache locally and hit the network boundary on CI, so the same eval saw
    different evidence in each place. Backtest cache mode never refetches.
    """
    from lumibot.fundamentals import SECFundamentals

    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / "company_tickers.json").write_text(json.dumps(_RECORDED_SEC_TICKERS), encoding="utf-8")
    return SECFundamentals(strategy, cache_dir=cache_dir, cache_mode="backtest", min_request_interval_seconds=0)


def _order_call_outcomes(result):
    """Map positions in result.tool_calls of order calls to their tool result payloads.

    Calls and results pair by call_id; without ids they pair in order per tool.
    """
    calls = list(result.tool_calls)
    results = list(result.tool_results)
    by_id = {event.call_id: event for event in results if getattr(event, "call_id", None)}
    unmatched: dict[str, list] = {}
    for event in results:
        if not getattr(event, "call_id", None):
            unmatched.setdefault(event.tool_name, []).append(event)
    outcomes = {}
    for position, call in enumerate(calls):
        if call.tool_name not in {"orders_submit_order", "orders_submit_multileg"}:
            continue
        outcome = by_id.get(call.call_id) if getattr(call, "call_id", None) else None
        if outcome is None and unmatched.get(call.tool_name):
            outcome = unmatched[call.tool_name].pop(0)
        payload = getattr(outcome, "payload", None)
        if isinstance(payload, dict) and isinstance(payload.get("result"), dict):
            payload = payload["result"]
        outcomes[position] = payload
    return outcomes


def _rejected_order_calls(result):
    """Positions in result.tool_calls of order calls whose tool result was a tool error."""
    return {
        position
        for position, payload in _order_call_outcomes(result).items()
        if isinstance(payload, dict) and payload.get("tool_error") is True
    }


def _resolved_multileg_legs(payload):
    """Legs LumiBot actually built, flattened to the harness leg shape.

    In action='close' mode the agent sends only contracts and LumiBot derives
    each side and quantity, so the call arguments are not the submitted legs.
    """
    if not isinstance(payload, dict) or not isinstance(payload.get("legs"), list):
        return None
    legs = []
    for leg in payload["legs"]:
        asset = leg.get("asset") if isinstance(leg, dict) else None
        if not isinstance(asset, dict):
            return None
        legs.append(
            {
                "symbol": asset.get("symbol"),
                "expiration": asset.get("expiration"),
                "strike": asset.get("strike"),
                "right": str(asset.get("right") or "").lower(),
                "side": leg.get("side"),
                "quantity": leg.get("quantity"),
            }
        )
    return legs


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
        if fixture.empty_expirations:
            original_get_chains = self.strategy.get_chains
            empty = tuple(fixture.empty_expirations)
            extra_strikes = {"CALL": [602.0, 604.0, 606.0, 608.0], "PUT": [592.0, 594.0, 596.0, 598.0]}

            def get_chains(asset, *args, **kwargs):
                chains = original_get_chains(asset, *args, **kwargs)
                if str(getattr(asset, "symbol", "")).upper() != "SPY":
                    return chains
                root = chains.setdefault("Chains", {})
                for side, strikes in extra_strikes.items():
                    side_map = root.get(side) or {}
                    normalized: dict[str, list] = {}
                    for key, listed in list(side_map.items()):
                        text = key.isoformat() if isinstance(key, date) else str(key)[:10]
                        normalized[text] = [float(value) for value in listed]
                    for expiration in empty:
                        # Listed, with no bars in the data store, so quotes and last trades miss.
                        normalized[expiration] = list(strikes)
                    root[side] = normalized
                return chains

            self.strategy.get_chains = get_chains
        if fixture.name == "congress_public_filings":
            self.strategy.house_disclosure_records = [
                {
                    "Ticker": "AAPL",
                    "Politician": "Nancy Pelosi",
                    "Transaction": "P",
                    "TransactionDate": "2026-07-28",
                    "ReportDate": "2026-08-01",
                    "Amount": "$1,001 - $15,000",
                    "side": "buy",
                    "asset_code": "ST",
                    "doc_id": "111",
                    "source": "house_ptr",
                },
                {
                    "Ticker": "ZZZZ",
                    "Politician": "Nancy Pelosi",
                    "Transaction": "P",
                    "TransactionDate": "2026-09-10",
                    "ReportDate": "2026-09-15",
                    "Amount": "$1,001 - $15,000",
                    "side": "buy",
                    "asset_code": "ST",
                    "doc_id": "222",
                    "source": "house_ptr",
                },
            ]
        self.strategy.fundamentals = _recorded_sec_fundamentals(self.strategy, self.root / "sec")
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
            reasoning_effort=None if str(case["model"]).startswith("gemini") else "medium",
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
        self.fixture.rejected_submissions = []
        outcomes = _order_call_outcomes(result)
        rejected = _rejected_order_calls(result)
        for position, event in enumerate(result.tool_calls):
            if event.tool_name not in {"orders_submit_order", "orders_submit_multileg"}:
                continue
            args = event.payload or {}
            if event.tool_name == "orders_submit_multileg":
                legs = args.get("legs_json", "[]")
                try:
                    legs = json.loads(legs) if isinstance(legs, str) else legs
                except json.JSONDecodeError:
                    legs = []
                if args.get("action") == "close" and position not in rejected:
                    legs = _resolved_multileg_legs(outcomes.get(position)) or legs
                record = {
                    "tool": event.tool_name,
                    "legs": legs,
                    "net_limit_price": args.get("net_limit_price"),
                    "price_style": args.get("price_style"),
                }
            else:
                record = {"tool": event.tool_name, **args}
            # Only orders the tool accepted reached the broker.
            target = self.fixture.rejected_submissions if position in rejected else self.fixture.submissions
            target.append(record)
        return [_order_to_dict(order) for order in self.strategy.get_orders()]

    def close(self):
        if self.closed:
            return
        self.closed = True
        self.broker.stream.stop()
        self.directory.cleanup()
