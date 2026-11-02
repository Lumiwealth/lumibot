"""Retired exchange guides remain reachable without advertising working setup."""

import json
from pathlib import Path

import pytest

from tests.test_docs_broker_faq_callout import _load_docs_config

ROOT = Path(__file__).resolve().parents[1]
DOCSRC = ROOT / "docsrc"
NOTES = json.loads((DOCSRC / "_data" / "ccxt_exchange_notes.json").read_text())
RETIRED = NOTES["retired"]
HISTORICAL_BASE = "https://github.com/Lumiwealth/lumibot/blob/71c21d231ee9a64170458b087a8076a24ead95f1/docsrc"


@pytest.mark.parametrize("exchange_id", RETIRED)
def test_retired_adapter_page_explains_removal_and_keeps_installed_capability(exchange_id):
    notice = (DOCSRC / f"brokers.ccxt.{exchange_id}.rst").read_text()
    assert exchange_id not in NOTES["exchanges"]
    assert RETIRED[exchange_id]["source"] in notice
    assert "no exchange allowlist" in notice
    assert f'print("{exchange_id}" in ccxt.exchanges)' in notice
    assert "TRADING_BROKER=" not in notice
    assert "submit_order" not in notice and "class " not in notice
    assert ":doc:`brokers.crypto`" in notice and ":doc:`brokers.ccxt`" in notice
    assert f"{HISTORICAL_BASE}/brokers.ccxt.{exchange_id}.rst" in notice
    assert f"{HISTORICAL_BASE}/_data/ccxt_exchange_facts.json" in notice


def test_current_snapshot_does_not_advertise_retired_adapters():
    snapshot = json.loads((DOCSRC / "_data" / "ccxt_exchange_facts.json").read_text())
    assert not RETIRED.keys() & {exchange["id"] for exchange in snapshot["exchanges"]}
    assert not snapshot["errors"]


def test_weex_guide_distinguishes_futures_simulation_from_live_spot():
    page = (DOCSRC / "brokers.ccxt.weex.rst").read_text()
    assert "Futures simulation only; spot example is live" in page
    assert "It rejects private spot requests in sandbox mode." in page
    assert page.count(NOTES["exchanges"]["weex"]["testnet_note"]) == 2  # setup and FAQ
    assert '"sandbox": False' in page and "WEEX_SANDBOX=true" not in page
    assert "no sandbox for this exchange" not in page


@pytest.mark.parametrize(
    "exchange_id,name",
    [
        ("aftermath", "AftermathFinance"),
        ("bitmart", "BitMart"),
        ("bitmex", "BitMEX"),
        ("coinex", "CoinEx"),
    ],
)
def test_retired_adapter_keeps_navigation_with_current_status(exchange_id, name):
    conf = _load_docs_config()
    rows = conf.broker_index_rows(DOCSRC)
    docname = f"brokers.ccxt.{exchange_id}"
    row = next(row for row in rows if row[3] == docname)
    assert row[0] == name and row[2] == "Removed from current CCXT; see guide"
    assert (name, docname) in conf.exchange_menu_entries(DOCSRC)
    assert next(row for row in rows if row[3] == "brokers.ccxt.binance")[2] != row[2]


def test_active_guides_do_not_recommend_retired_adapter():
    for exchange_id in NOTES["exchanges"]:
        page = (DOCSRC / f"brokers.ccxt.{exchange_id}.rst").read_text()
        for retired_id in RETIRED:
            assert f"<brokers.ccxt.{retired_id}>" not in page, exchange_id


@pytest.mark.parametrize("exchange_id", RETIRED)
def test_generator_preserves_retired_page(exchange_id, tmp_path, monkeypatch):
    from scripts import generate_ccxt_exchange_pages as pages_gen

    retired = DOCSRC / f"brokers.ccxt.{exchange_id}.rst"
    (tmp_path / retired.name).write_text(retired.read_text())
    index = tmp_path / "brokers.ccxt.rst"
    index.write_text((DOCSRC / index.name).read_text())
    monkeypatch.setattr(pages_gen, "DOCSRC", tmp_path)
    monkeypatch.setattr(pages_gen, "INDEX_PAGE", index)
    pages_gen.main(["binance"])
    assert (tmp_path / retired.name).read_text() == retired.read_text()
    assert (tmp_path / "brokers.ccxt.binance.rst").exists()
