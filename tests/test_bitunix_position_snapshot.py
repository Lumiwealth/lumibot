"""Position refresh must not turn an unreadable broker snapshot into flat state."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from lumibot.brokers.bitunix import Bitunix
from lumibot.brokers.broker import LumibotBrokerAPIError
from lumibot.entities import Asset, Position
from lumibot.strategies import Strategy
from lumibot.tools.bitunix_helpers import BitUnixClient


def row(symbol="BTCUSDT", qty="0.5", side="SHORT", **overrides):
    return {"symbol": symbol, "qty": qty, "side": side, "avgOpenPrice": "51000", **overrides}


@pytest.fixture
def broker():
    # Real client and broker; intercept transport so no account or order is touched.
    client = BitUnixClient(api_key="test_api_key", secret_key="test_api_secret")
    client._request = MagicMock()
    with patch("lumibot.brokers.bitunix.BitUnixClient", return_value=client), patch(
        "lumibot.brokers.bitunix.BitunixData"
    ) as data:
        data.return_value.client_symbols = set()
        result = Bitunix({"API_KEY": "test_api_key", "API_SECRET": "test_api_secret"}, connect_stream=False)
    result._filled_positions.append(
        Position("test_strategy", Asset("BTCUSDT", Asset.AssetType.CRYPTO_FUTURE), Decimal("-0.25"))
    )
    result._filled_positions.append(
        Position("test_strategy", Asset("ETHUSDT", Asset.AssetType.CRYPTO_FUTURE), Decimal("2"))
    )
    return result


@pytest.mark.parametrize(
    "response",
    [
        None,
        {},
        {"code": 10001, "data": []},
        {"code": 0},
        {"code": 0, "data": None},
        {"code": 0, "data": {}},
        {"code": 0, "data": [row(), None]},
        {"code": 0, "data": [row(), row("ETHUSDT", qty="invalid")]},
        {"code": 0, "data": [row(), row("ETHUSDT", qty="NaN")]},
        {"code": 0, "data": [row(), row("ETHUSDT", qty="Infinity")]},
        {"code": 0, "data": [row(), row("ETHUSDT", side="unknown")]},
        {"code": 0, "data": [row(), row(symbol="")]},
        {"code": 0, "data": [row(), {"symbol": "ETHUSDT", "side": "LONG"}]},
        {"code": 0, "data": [row(), row("ETHUSDT", avgOpenPrice="NaN")]},
        {"code": 0, "data": [row(side="LONG"), row(side="SHORT")]},
        {"code": 0, "data": [row(side="SHORT"), row(side="LONG")]},
        {"code": 0, "data": [row(side="LONG"), row(side="LONG")]},
    ],
)
def test_failed_snapshot_preserves_all_positions_and_refresh_retry(broker, response):
    before = [(p.asset, p.quantity) for p in broker._filled_positions.get_list()]
    revision = broker._filled_positions.revision
    broker.api._request.return_value = response
    error = None
    try:
        broker.refresh_positions(SimpleNamespace(name="test_strategy"), ttl_seconds=60)
    except LumibotBrokerAPIError as exc:
        error = exc
    assert [(p.asset, p.quantity) for p in broker._filled_positions.get_list()] == before
    assert broker._filled_positions.revision == revision
    assert error is not None

    # Failed reads must not populate the success throttle or suppress recovery.
    broker.api._request.return_value = {"code": 0, "data": [row()]}
    broker.refresh_positions(SimpleNamespace(name="test_strategy"), ttl_seconds=60)
    assert len(broker._filled_positions.get_list()) == 1
    assert broker._filled_positions.get_list()[0].quantity == Decimal("-0.5")
    assert broker.api._request.call_count == 2


def test_transport_failure_preserves_positions(broker):
    before = [(p.asset, p.quantity) for p in broker._filled_positions.get_list()]
    revision = broker._filled_positions.revision
    broker.api._request.side_effect = TimeoutError("unavailable")
    with pytest.raises(LumibotBrokerAPIError):
        broker.sync_positions(SimpleNamespace(name="test_strategy"))
    assert [(p.asset, p.quantity) for p in broker._filled_positions.get_list()] == before
    assert broker._filled_positions.revision == revision


def test_successful_empty_snapshot_removes_stale_positions(broker):
    cash = Position("test_strategy", broker.get_quote_asset(), Decimal("1000"))
    broker._filled_positions.append(cash)
    broker.api._request.return_value = {"code": 0, "data": []}
    broker.sync_positions(SimpleNamespace(name="test_strategy"))
    assert broker._filled_positions.get_list() == [cash]


@pytest.mark.parametrize("side, expected", [("LONG", "0.5"), ("BUY", "0.5"), ("SHORT", "-0.5"), ("SELL", "-0.5")])
def test_valid_snapshot_preserves_side_and_zero_quantity_contract(broker, side, expected):
    broker.api._request.return_value = {"code": 0, "data": [row(side=side), row("ETHUSDT", qty="0")]}
    broker.sync_positions(None)
    positions = broker._filled_positions.get_list()
    assert len(positions) == 1
    assert positions[0].quantity == Decimal(expected)
    assert positions[0].avg_fill_price == Decimal("51000")
    broker.api._request.assert_called_once_with(method="GET", endpoint="/api/v1/futures/position/get_pending_positions")


def test_public_strategy_position_read_fails_then_recovers(broker):
    strategy = object.__new__(Strategy)
    strategy._name = "test_strategy"
    strategy.broker = broker
    asset = broker._filled_positions.get_list()[0].asset
    broker.api._request.return_value = {"code": 10001}
    with pytest.raises(LumibotBrokerAPIError):
        strategy.get_position(asset)
    assert broker.get_tracked_position(strategy.name, asset).quantity == Decimal("-0.25")

    broker.api._request.return_value = {"code": 0, "data": [row()]}
    assert strategy.get_position(asset).quantity == Decimal("-0.5")


def test_polling_cycle_reports_failure_and_next_cycle_recovers(broker, caplog):
    broker.stream = broker._get_stream_object()
    broker._register_stream_events()
    before = [(p.asset, p.quantity) for p in broker._filled_positions.get_list()]
    broker.api._request.return_value = {"code": 10001}
    broker.stream._poll()
    assert "Bitunix position snapshot request failed" in caplog.text
    assert [(p.asset, p.quantity) for p in broker._filled_positions.get_list()] == before

    broker.api._request.side_effect = [{"code": 0, "data": []}, {"code": 0, "data": {"orderList": []}}]
    broker.stream._poll()
    assert broker._filled_positions.get_list() == []


def test_direct_position_read_accepts_strategy_name(broker):
    broker.api._request.return_value = {"code": 0, "data": [row()]}
    asset = broker._filled_positions.get_list()[0].asset
    position = broker._pull_position("test_strategy", asset)
    assert position.strategy == "test_strategy"
    assert position.quantity == Decimal("-0.5")


@pytest.mark.parametrize("zero_first", [True, False])
def test_zero_quantity_row_does_not_make_active_position_ambiguous(broker, zero_first):
    rows = [row(qty="0", side="LONG"), row(side="SHORT")]
    broker.api._request.return_value = {"code": 0, "data": rows if zero_first else rows[::-1]}
    broker.sync_positions(None)
    positions = broker._filled_positions.get_list()
    assert len(positions) == 1
    assert positions[0].quantity == Decimal("-0.5")
