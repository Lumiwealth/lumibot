"""Brokers answer "can you place this order, and how?" from one declaration.

``broker.supports_order(...)`` returns native (the broker holds it), simulated (LumiBot
watches price and acts, so it only works while the strategy runs), unsupported
(submit_order refuses it with that reason) or unknown (not declared). The same
declaration drives the refusal in ``Broker.submit_order``; the contract test checks it
against each broker's real request builder, and this file checks the public docs table.
"""

import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from lumibot.brokers.broker import OrderSupport
from lumibot.brokers.example_broker import ExampleBroker
from lumibot.entities import Asset, Order


class _Undeclared(ExampleBroker):
    """A broker that declares nothing keeps today's behavior: send as asked."""

    def __init__(self):  # no connection work
        self.name = "Undeclared"

    def _submit_order(self, order):
        order.status = "submitted"
        return order


def _bare(cls):
    broker = cls.__new__(cls)
    broker.name = cls.__name__
    return broker


def test_undeclared_broker_answers_unknown_and_still_sends():
    broker = _Undeclared()
    order = Order("s", Asset("SPY"), 1, side="sell", trail_percent=2.0)
    support = broker.supports_order(order)
    assert isinstance(support, OrderSupport)
    assert support.mode == "unknown" and support.supported is None
    broker.resolve_option_order_intent = MagicMock()
    assert broker.submit_order(order).status == "submitted"


def test_submit_order_refuses_what_the_broker_declares_unsupported():
    from lumibot.brokers import Tradovate

    broker = _bare(Tradovate)
    broker._submit_order = MagicMock()
    broker._process_trade_event = MagicMock()
    order = Order("s", Asset("MESZ5", asset_type=Asset.AssetType.FUTURE), 1, side="sell", trail_percent=1.0)
    result = broker.submit_order(order)
    broker._submit_order.assert_not_called()
    assert result is order and order.status == "error"
    assert "not supported by Tradovate" in order.error_message


def test_submit_orders_refuses_only_the_unsupported_orders():
    from lumibot.brokers import Tradovate

    broker = _bare(Tradovate)
    broker._submit_orders = MagicMock(side_effect=lambda orders, **kw: orders)
    broker._process_trade_event = MagicMock()
    asset = Asset("MESZ5", asset_type=Asset.AssetType.FUTURE)
    trailing = Order("s", asset, 1, side="sell", trail_percent=1.0)
    stop = Order("s", asset, 1, side="sell", stop_price=5000.0)
    sent = broker.submit_orders([trailing, stop])
    assert sent == [stop]
    assert trailing.status == "error"


def test_answer_describes_without_an_order_object():
    from lumibot.brokers.alpaca import Alpaca

    broker = _bare(Alpaca)
    assert broker.supports_order(order_type="trailing_stop").mode == "native"
    bracket_trail = broker.supports_order(order_type="market", order_class="bracket", trailing_leg=True)
    assert bracket_trail.mode == "unsupported" and "fixed stop price" in bracket_trail.reason
    stop_entry = broker.supports_order(order_type="stop", order_class="bracket")
    assert stop_entry.mode == "unsupported" and "limit or market entry" in stop_entry.reason


def test_simulated_orders_say_they_need_the_strategy_running():
    from lumibot.brokers.projectx import ProjectX

    support = _bare(ProjectX).supports_order(order_type="market", order_class="bracket")
    assert support.mode == "simulated" and support.supported is True
    assert "while the strategy is running" in support.reason


def test_smart_limit_is_simulated_everywhere_limit_orders_work():
    from lumibot.brokers.alpaca import Alpaca

    support = _bare(Alpaca).supports_order(order_type="smart_limit")
    assert support.mode == "simulated"
    assert "re-prices" in support.reason


@pytest.mark.parametrize("exchange, percent, amount", [("okx", "native", "unsupported"),
                                                       ("bybit", "unsupported", "native"),
                                                       ("coinbase", "unsupported", "unsupported")])
def test_ccxt_trailing_support_follows_the_exchange(monkeypatch, exchange, percent, amount):
    from lumibot.brokers.ccxt import Ccxt

    monkeypatch.setenv("LUMIBOT_TELEMETRY", "false")
    broker = Ccxt({"exchange_id": exchange, "sandbox": False}, connect_stream=False, start_orders_thread=False)
    quote = Asset("USDT", asset_type="crypto")
    by_percent = Order("s", Asset("BTC", asset_type="crypto"), 1, side="sell", quote=quote, trail_percent=2.0)
    by_amount = Order("s", Asset("BTC", asset_type="crypto"), 1, side="sell", quote=quote, trail_price=50.0)
    assert broker.supports_order(by_percent).mode == percent
    assert broker.supports_order(by_amount).mode == amount


# --- the public docs table is checked against the declarations ---------------

_DOC = Path(__file__).resolve().parents[1] / "docsrc" / "brokers.order_support.rst"
_COLUMNS = {
    "Market": dict(order_type="market"),
    "Limit": dict(order_type="limit"),
    "Stop": dict(order_type="stop"),
    "Stop-limit": dict(order_type="stop_limit"),
    "Trailing": dict(order_type="trailing_stop"),
    "Bracket": dict(order_type="market", order_class="bracket"),
    "OTO": dict(order_type="market", order_class="oto"),
    "Trailing leg": dict(order_type="market", order_class="oto", trailing_leg=True),
    "OCO": dict(order_type="limit", order_class="oco"),
}
_CELL = {"Native": "native", "Simulated": "simulated", "No": "unsupported"}


def _doc_rows():
    lines = _DOC.read_text().splitlines()
    start = lines.index(".. order-support-table-start")
    table = [line for line in lines[start:] if line and not line.startswith("..")]
    border = table[0]
    spans = [(m.start(), m.end()) for m in re.finditer(r"=+", border)]

    def cells(line):
        return [line[a:b if i < len(spans) - 1 else None].strip() for i, (a, b) in enumerate(spans)]

    header = cells(table[1])
    rows = {}
    for line in table[3:]:
        if line.startswith("="):
            break
        values = cells(line)
        rows[values[0]] = dict(zip(header[1:], values[1:]))
    return header, rows


def _doc_brokers():
    from lumibot.brokers import Tradovate
    from lumibot.brokers.alpaca import Alpaca
    from lumibot.brokers.bitunix import Bitunix
    from lumibot.brokers.interactive_brokers_rest import InteractiveBrokersREST
    from lumibot.brokers.projectx import ProjectX
    from lumibot.brokers.schwab import Schwab
    from lumibot.brokers.tradier import Tradier

    return {
        "Alpaca": Alpaca, "Tradier": Tradier, "Schwab": Schwab, "Tradovate": Tradovate,
        "Bitunix": Bitunix, "ProjectX": ProjectX, "Interactive Brokers (Client Portal)": InteractiveBrokersREST,
    }


def test_docs_table_matches_every_declaration():
    header, rows = _doc_rows()
    assert header[1:] == list(_COLUMNS)
    brokers = _doc_brokers()
    assert set(brokers) <= set(rows)
    for name, cls in brokers.items():
        broker = _bare(cls)
        for column, query in _COLUMNS.items():
            expected = broker.supports_order(**query).mode
            assert _CELL[rows[name][column]] == expected, (name, column)
