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

sys.path.insert(0, str(ROOT / "scripts"))
import generate_ccxt_exchange_pages as pages_gen  # noqa: E402


def page_text(exchange_id):
    return (ROOT / "docsrc" / f"brokers.ccxt.{exchange_id}.rst").read_text(encoding="utf-8")


def python_examples(text):
    blocks = re.findall(r"\.\. code-block:: python\n\n((?:   .*\n|\n)+)", text)
    return ["\n".join(line[3:] for line in block.splitlines()) for block in blocks if "class " in block]


def fake_markets(quote):
    def market(base):
        return {
            "id": f"{base}{quote}", "symbol": f"{base}/{quote}", "base": base, "quote": quote,
            "baseId": base, "quoteId": quote, "settle": None, "settleId": None,
            "type": "spot", "spot": True, "swap": False, "future": False, "option": False,
            "contract": False, "linear": False, "inverse": False, "active": True,
            "precision": {"amount": 0.000001, "price": 0.01},
            "limits": {"amount": {"min": 0.000001, "max": None}, "price": {"min": None, "max": None},
                       "cost": {"min": 1, "max": None}},
            "info": {},
        }
    return [market("BTC"), market("ETH")]


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
    for credential in FACTS[exchange_id]["required_credentials"]:
        monkeypatch.setenv(pages_gen.env_name(exchange_id, credential), "fake-value")

    namespace = {"__name__": "docs_example"}  # keeps the run_live() block from running
    exec(compile(source, f"{exchange_id}_example", "exec"), namespace)
    strategy_class = next(v for v in namespace.values()
                          if isinstance(v, type) and issubclass(v, Strategy) and v is not Strategy)
    config = next(v for k, v in namespace.items() if k.endswith("_CONFIG"))

    quote = NOTES[exchange_id].get("quote_symbol", "USDT")
    broker = Ccxt(config, connect_stream=False, start_orders_thread=False)
    broker.api.set_markets(fake_markets(quote))
    broker.data_source._markets_loaded = True
    broker.api.fetch_balance = Mock(return_value={"total": {quote: 1000}, "free": {quote: 1000}, "used": {quote: 0}})
    broker.api.fetch_open_orders = Mock(return_value=[])
    broker.api.create_order = Mock(return_value={"id": "fake-order", "status": "open"})
    broker.data_source.get_last_price = Mock(return_value=50000)

    strategy = strategy_class(broker=broker, quote_asset=Asset(quote, asset_type=Asset.AssetType.CRYPTO))
    assert strategy.broker is broker
    return strategy, config, broker


@pytest.mark.parametrize("exchange_id", PAGES)
def test_pages_are_current_with_generator(exchange_id):
    facts = json.loads((DATA / "ccxt_exchange_facts.json").read_text())
    affiliates = json.loads((DATA / "ccxt_exchange_affiliates.json").read_text())
    rendered = pages_gen.render(exchange_id, FACTS, NOTES, affiliates, facts)
    assert rendered == page_text(exchange_id), "page is stale: rerun scripts/generate_ccxt_exchange_pages.py"


@pytest.mark.parametrize("exchange_id", PAGES)
def test_regular_example_places_one_spot_order_offline(exchange_id, monkeypatch):
    source = python_examples(page_text(exchange_id))[0]  # regular example
    strategy, config, broker = build_example(exchange_id, source, monkeypatch)

    assert config["exchange_id"] == exchange_id
    assert config["sandbox"] is FACTS[exchange_id]["sandbox"]
    for credential in FACTS[exchange_id]["required_credentials"]:
        assert config[credential] == "fake-value"

    strategy.initialize()
    strategy.on_trading_iteration()
    args, _ = broker.api.create_order.call_args
    quote = NOTES[exchange_id].get("quote_symbol", "USDT")
    assert args[0] == f"BTC/{quote}" and args[2] == "buy"


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
    assert "https://botspot.trade/" in text
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
