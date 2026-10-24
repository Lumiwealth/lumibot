"""Checks for the generated per-exchange CCXT documentation pages.

Why: the pages in ``docsrc/brokers.ccxt.<id>.rst`` are generated from CCXT
metadata plus hand-checked notes. Their code examples must actually run with
LumiBot, their facts must match the installed CCXT, and they must follow Rob's
copy rules. Everything here is offline: the exchange's network methods are
replaced with fakes, so no request, key or order ever reaches an exchange.
"""

import json
import re
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "docsrc" / "_data"
NOTES = json.loads((DATA / "ccxt_exchange_notes.json").read_text())["exchanges"]
FACTS = {fx["id"]: fx for fx in json.loads((DATA / "ccxt_exchange_facts.json").read_text())["exchanges"]}
PAGES = sorted(NOTES)
AFFILIATES = json.loads((DATA / "ccxt_exchange_affiliates.json").read_text())

sys.path.insert(0, str(ROOT / "scripts"))
import generate_ccxt_exchange_pages as pages_gen  # noqa: E402


def page_text(exchange_id):
    return (ROOT / "docsrc" / f"brokers.ccxt.{exchange_id}.rst").read_text(encoding="utf-8")


def python_examples(text):
    blocks = re.findall(r"\.\. code-block:: python\n\n((?:   .*\n|\n)+)", text)
    return ["\n".join(line[3:] for line in block.splitlines()) for block in blocks if "class " in block]


def page_env(exchange_id):
    """Parse the page's .env block, swapping placeholder keys for fake values."""
    blocks = re.findall(r"\.\. code-block:: bash\n\n((?:   .*\n|\n)+)", page_text(exchange_id))
    block = next(b for b in blocks if "TRADING_BROKER=" in b)
    env = {}
    for line in block.splitlines():
        line = line.split("#", 1)[0].strip()
        if "=" in line:
            key, value = line.split("=", 1)
            env[key] = "fake-value" if value.startswith("your_") else value
    return env


def page_env_config(exchange_id):
    from lumibot.credentials import generic_ccxt_config
    from lumibot.tools.ccxt_exchange_catalog import validate_ccxt_config

    env = page_env(exchange_id)
    # ``ccxt:<id>`` reaches a CCXT exchange whose id collides with a LumiBot broker name.
    assert env["TRADING_BROKER"] in (exchange_id, f"ccxt:{exchange_id}")
    config = generic_ccxt_config(env["TRADING_BROKER"], env)
    validate_ccxt_config(exchange_id, config)  # the page lists every required variable
    return config


def fake_markets(exchange_id):
    """Fake CCXT markets for the page's example: spot pairs, or its perpetual."""
    notes = NOTES[exchange_id]
    quote = notes.get("quote_symbol", "USDT")
    base = notes.get("example_base", "BTC")
    import ccxt

    # Precision values must match the exchange's CCXT precision mode: a tick
    # size (0.000001) for TICK_SIZE, a digit count (6) for the other modes.
    tick = getattr(ccxt, exchange_id)().precisionMode == ccxt.TICK_SIZE
    amount_precision, price_precision = (0.000001, 0.01) if tick else (6, 2)

    def market(symbol, b, q, settle=None, inverse=False):
        contract = settle is not None
        return {
            "id": symbol.replace("/", "").replace(":", ""), "symbol": symbol, "base": b, "quote": q,
            "baseId": b, "quoteId": q, "settle": settle, "settleId": settle,
            "type": "swap" if contract else "spot", "spot": not contract, "swap": contract, "future": False,
            "option": False, "contract": contract, "linear": contract and not inverse, "inverse": inverse,
            "contractSize": (100 if inverse else 0.001) if contract else None, "active": True,
            "precision": {"amount": amount_precision, "price": price_precision},
            "limits": {"amount": {"min": 0.000001, "max": None}, "price": {"min": None, "max": None},
                       "cost": {"min": 1, "max": None}},
            "info": {},
        }

    markets = [market(f"{base}/{quote}", base, quote), market(f"ETH/{quote}", "ETH", quote)]
    symbol = notes.get("example_symbol")
    if symbol and not FACTS[exchange_id]["markets"].get("spot"):
        b, rest = symbol.split("/", 1)
        q, settle = rest.split(":", 1)
        markets.append(market(symbol, b, q, settle=settle, inverse=(settle == b)))
    return markets


def expected_order_symbol(exchange_id):
    notes = NOTES[exchange_id]
    if notes.get("example_symbol") and not FACTS[exchange_id]["markets"].get("spot"):
        return notes["example_symbol"]
    return f"{notes.get('example_base', 'BTC')}/{notes.get('quote_symbol', 'USDT')}"


def build_example(exchange_id, source, monkeypatch):
    """Exec one page example and build its strategy against a fake exchange."""
    from lumibot.brokers.ccxt import Ccxt
    from lumibot.entities import Asset
    from lumibot.strategies import Strategy

    monkeypatch.setenv("LUMIBOT_TELEMETRY", "false")
    # A developer's own .env may select another live broker or data source.
    # The page examples pass an explicit Ccxt broker, so test them without that.
    for name in ("TRADING_BROKER", "DATA_SOURCE", "LIVE_DATA_SOURCE"):
        monkeypatch.delenv(name, raising=False)
    # Known platform behavior (reported 2026-10-23, not changed here): when the
    # environment resolves a default live data source, Strategy(broker=Ccxt(...))
    # replaces the explicit broker's CcxtData with it. Simulate a clean machine
    # with no other broker configured, which is what a docs reader has.
    import lumibot.strategies._strategy as strategy_module
    monkeypatch.setattr(strategy_module, "get_default_data_source", lambda: None)
    namespace = {"__name__": "docs_example"}  # keeps the run_live() block from running
    exec(compile(source, f"{exchange_id}_example", "exec"), namespace)
    strategy_class = next(v for v in namespace.values()
                          if isinstance(v, type) and issubclass(v, Strategy) and v is not Strategy)
    # The examples get their broker from the page's .env block, exactly as a reader would.
    config = page_env_config(exchange_id)

    quote = NOTES[exchange_id].get("quote_symbol", "USDT")
    broker = Ccxt(config, connect_stream=False, start_orders_thread=False)
    # Hard offline guarantee: any CCXT request that is not faked below fails
    # here instead of reaching the exchange with the fake keys.
    broker.api.request = Mock(side_effect=AssertionError("network disabled in docs example tests"))
    # A small stateful fake exchange: fills change its balances and positions,
    # so LumiBot's normal position sync sees the result of the trade.
    balances = {quote: 1000.0}
    contracts = []
    broker.api.fetch_positions = Mock(side_effect=lambda *a, **k: [dict(c) for c in contracts])
    broker.api.fetch_my_trades = Mock(return_value=[])
    broker.api.fetch_closed_orders = Mock(return_value=[])
    broker.api.fetch_ticker = Mock(return_value={"last": 50000, "bid": 49990, "ask": 50010, "close": 50000})
    broker.api.set_markets(fake_markets(exchange_id))
    broker.data_source._markets_loaded = True
    broker.api.fetch_balance = Mock(side_effect=lambda *a, **k: {
        "total": dict(balances), "free": dict(balances), "used": {k2: 0 for k2 in balances}})
    broker.api.fetch_open_orders = Mock(return_value=[])

    def fill(symbol, order_type, side, amount, price=None, params=None):
        # The fake exchange fills the whole order at once at the last price.
        market = broker.api.markets[symbol]
        amount = float(amount)
        if market.get("contract"):
            contracts.append({"symbol": symbol, "contracts": amount, "side": "long" if side == "buy" else "short",
                              "contractSize": market["contractSize"], "entryPrice": 50000.0, "markPrice": 50000.0,
                              "unrealizedPnl": 0.0, "hedged": False, "info": {}})
        else:
            balances[market["base"]] = balances.get(market["base"], 0.0) + amount
            balances[market["quote"]] -= amount * 50000.0
        return {"id": "fake-order", "status": "closed", "symbol": symbol, "side": side,
                "amount": amount, "filled": amount, "average": 50000.0}

    broker.api.create_order = Mock(side_effect=fill)
    broker.data_source.get_last_price = Mock(return_value=50000)

    strategy = strategy_class(broker=broker, quote_asset=Asset(quote, asset_type=Asset.AssetType.CRYPTO))
    assert strategy.broker is broker
    return strategy, config, broker


@pytest.mark.parametrize("exchange_id", PAGES)
def test_pages_are_current_with_generator(exchange_id):
    facts = json.loads((DATA / "ccxt_exchange_facts.json").read_text())
    affiliates = json.loads((DATA / "ccxt_exchange_affiliates.json").read_text())
    rendered = pages_gen.render(exchange_id, FACTS, NOTES, affiliates, facts, PAGES)
    assert rendered == page_text(exchange_id), "page is stale: rerun scripts/generate_ccxt_exchange_pages.py"


@pytest.mark.parametrize("exchange_id", PAGES)
def test_regular_example_places_one_spot_order_offline(exchange_id, monkeypatch):
    source = python_examples(page_text(exchange_id))[0]  # regular example
    strategy, config, broker = build_example(exchange_id, source, monkeypatch)

    assert config["exchange_id"] == exchange_id
    assert config["sandbox"] is pages_gen.Page(exchange_id, FACTS, NOTES, AFFILIATES).sandbox
    for credential in FACTS[exchange_id]["required_credentials"]:
        assert config[credential] == "fake-value"

    strategy.initialize()
    strategy.on_trading_iteration()
    assert broker.api.create_order.called, "the example never sent an order"
    args, _ = broker.api.create_order.call_args
    assert args[0] == expected_order_symbol(exchange_id) and args[2] == "buy"
    assert float(args[3]) > 0
    # Limit-only exchanges (no createMarketOrder in CCXT) must get a limit order.
    page = pages_gen.Page(exchange_id, FACTS, NOTES, AFFILIATES)
    assert str(args[1]).endswith("market" if page.market_orders else "limit")
    assert_fill_processed(strategy, broker, expected_order_symbol(exchange_id))


def assert_fill_processed(strategy, broker, symbol):
    """The fake fill went through LumiBot's real order lifecycle and became a position."""
    filled = [o for o in broker.get_tracked_orders(strategy.name) if o.is_filled()]
    assert filled, "the order was submitted but its fill was never processed"
    order = filled[0]
    assert order.avg_fill_price == 50000.0 and order.identifier == "fake-order"
    broker.sync_positions(strategy)  # live brokers learn positions from the exchange
    position = broker.get_tracked_position(strategy.name, order.asset)
    assert position is not None and float(position.quantity) == float(order.quantity)
    assert symbol.startswith(order.asset.symbol.split("/")[0])


def swap_pages():
    return [i for i in PAGES if pages_gen.swap_example_page(pages_gen.Page(i, FACTS, NOTES, AFFILIATES)) is not None]


@pytest.mark.parametrize("exchange_id", swap_pages())
def test_perpetual_example_opens_one_contract_position_offline(exchange_id, monkeypatch):
    """Exchanges that list spot and swaps get a perpetual example that really runs."""
    text = page_text(exchange_id)
    assert f"Perpetual futures example on {FACTS[exchange_id]['name']}" in text
    source = next(s for s in python_examples(text) if "OpenOneLong" in s)
    swap_page = pages_gen.swap_example_page(pages_gen.Page(exchange_id, FACTS, NOTES, AFFILIATES))
    symbol = swap_page.perp_symbol
    base, rest = symbol.split("/", 1)
    quote, settle = rest.split(":", 1)
    strategy, _, broker = build_example(exchange_id, source, monkeypatch)
    markets = list(broker.api.markets.values())
    contract = dict(markets[0], id=symbol.replace("/", "").replace(":", ""), symbol=symbol, base=base, quote=quote,
                    settle=settle, settleId=settle, type="swap", spot=False, swap=True, contract=True,
                    linear=settle != base, inverse=settle == base, contractSize=0.001)
    broker.api.set_markets(markets + [contract])
    if exchange_id == "phemex":
        # Phemex positions come from its contract-account endpoint (covered in test_ccxt_derivatives);
        # here the fake exchange's positions stand in for that endpoint.
        broker._fetch_derivative_positions = broker.api.fetch_positions

    strategy.initialize()
    strategy.on_trading_iteration()
    args, _ = broker.api.create_order.call_args
    assert args[0] == symbol and args[2] == "buy" and float(args[3]) > 0
    order = broker.get_tracked_orders(strategy.name)[0]
    assert order.asset.asset_type == "crypto_future" and order.asset.symbol == symbol
    assert_fill_processed(strategy, broker, symbol)


@pytest.mark.parametrize("exchange_id", PAGES)
def test_page_has_botspot_mcp_callout_near_the_top_and_fee_note(exchange_id):
    text = page_text(exchange_id)
    include = pages_gen.MCP_CALLOUT_INCLUDE.strip()
    assert text.count(include) == 1
    assert text.index(include) < text.index(f"{FACTS[exchange_id]['name']} at a glance")
    assert f"Fees and partner IDs on {FACTS[exchange_id]['name']}" in text
    assert "builderFee`` to ``False" not in text  # LumiBot does this itself now


def test_mcp_callout_snippet_is_the_single_shared_copy():
    snippets = list((ROOT / "docsrc").rglob("botspot_mcp_callout.rst"))
    assert snippets == [ROOT / "docsrc" / "_snippets" / "botspot_mcp_callout.rst"]
    body = snippets[0].read_text()
    assert "https://botspot.trade/agents" in body and "—" not in body and "–" not in body
    assert "Claude, ChatGPT, Codex or Cursor" in body


@pytest.mark.parametrize("exchange_id", PAGES)
def test_code_config_snippet_matches_env_config(exchange_id, monkeypatch):
    """The 'build the broker in code' snippet must describe the same connection."""
    from lumibot.brokers.ccxt import Ccxt

    text = page_text(exchange_id)
    snippet = next(b for b in re.findall(r"\.\. code-block:: python\n\n((?:   .*\n|\n)+)", text) if "_CONFIG = {" in b)
    snippet = "\n".join(line[3:] for line in snippet.splitlines()).split("from lumibot.brokers", 1)[0]
    for key, value in page_env(exchange_id).items():
        monkeypatch.setenv(key, value)
    namespace = {}
    exec(compile(snippet, f"{exchange_id}_config", "exec"), namespace)
    config = namespace[f"{exchange_id.upper()}_CONFIG"]
    env_config = page_env_config(exchange_id)
    for field in FACTS[exchange_id]["required_credentials"]:
        assert config[field] == env_config[field] == "fake-value"
    assert config["sandbox"] is env_config["sandbox"]
    broker = Ccxt(config, connect_stream=False, start_orders_thread=False)  # constructs offline
    broker.api.request = Mock(side_effect=AssertionError("network disabled in docs example tests"))
    assert broker.api.id == exchange_id


@pytest.mark.parametrize("exchange_id", PAGES)
def test_agent_example_creates_one_trading_agent_and_no_python_logic(exchange_id, monkeypatch):
    source = python_examples(page_text(exchange_id))[1]
    strategy, _, _ = build_example(exchange_id, source, monkeypatch)
    strategy.initialize()

    trader = strategy.agents["trader"]
    assert trader is not None
    iteration = source.split("def on_trading_iteration", 1)[1].split('if __name__', 1)[0]
    # The agentic pattern: the iteration only hands work to the agent.
    assert "self.agents[\"trader\"].run(" in iteration
    for forbidden in ("if ", "for ", "submit_order", "create_order", "get_historical_prices"):
        assert forbidden not in iteration


@pytest.mark.parametrize("exchange_id", PAGES)
def test_page_copy_rules(exchange_id):
    text = page_text(exchange_id)
    assert "—" not in text and "–" not in text
    lowered = text.lower()
    for banned in ("community supported", "tested by us", "not available in the us", "restricted countries",
                   "guaranteed", "make money", "returns of", "passive income"):
        assert banned not in lowered, banned
    assert "botspot.ai" not in lowered
    assert re.search(r"https://botspot\.trade/", text), "pages link to BotSpot"
    assert text.count(FACTS[exchange_id]["name"]) >= 8  # the exchange name is used naturally


def test_referral_link_and_disclosure_only_when_we_have_our_own_link():
    facts = json.loads((DATA / "ccxt_exchange_facts.json").read_text())
    affiliates = json.loads((DATA / "ccxt_exchange_affiliates.json").read_text())
    exchange_id = PAGES[0]
    plain = pages_gen.render(exchange_id, FACTS, NOTES, {**affiliates, "exchanges": {}}, facts)
    assert affiliates["disclosure"] not in plain

    ours = "https://example-referral.invalid/ours"
    with_link = pages_gen.render(exchange_id, FACTS, NOTES,
                                 {**affiliates, "exchanges": {exchange_id: {"referral_url": ours}}}, facts)
    assert ours in with_link and affiliates["disclosure"] in with_link


def test_ccxt_referral_urls_are_never_written():
    raw = (DATA / "ccxt_exchange_facts.json").read_text()
    assert '"referral"' not in raw and "referralCode" not in raw


def test_facts_match_installed_ccxt_for_pilots(tmp_path):
    import ccxt

    out = tmp_path / "facts.json"
    subprocess.run([sys.executable, str(ROOT / "scripts" / "generate_ccxt_exchange_facts.py"),
                    "--out", str(out), "--only", *PAGES], check=True, capture_output=True)
    fresh = {fx["id"]: fx for fx in json.loads(out.read_text())["exchanges"]}
    for exchange_id in PAGES:
        assert fresh[exchange_id]["markets"] == FACTS[exchange_id]["markets"]
        assert fresh[exchange_id]["required_credentials"] == FACTS[exchange_id]["required_credentials"]
        assert fresh[exchange_id]["sandbox"] == FACTS[exchange_id]["sandbox"]
    assert json.loads(out.read_text())["ccxt_version"] == ccxt.__version__
