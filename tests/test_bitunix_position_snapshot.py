"""Position refresh must not turn an unreadable broker snapshot into flat state."""

from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from threading import Event, current_thread
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


@pytest.mark.parametrize(
    "older_rows, newer_response, expected_quantity",
    [
        ([], {"code": 0, "data": [row(qty="1.5")]}, "-1.5"),
        ([row(qty="0.75")], {"code": 0, "data": [row(qty="1.5")]}, "-1.5"),
        ([row(qty="0.75")], {"code": 0, "data": []}, None),
        ([row(qty="0.75")], {"code": 10001}, "-0.75"),
        ([], {"code": 10001}, None),
    ],
    ids=["stale-delete", "stale-quantity", "stale-resurrection", "newer-fails", "newer-fails-empty"],
)
def test_older_refresh_preserves_newer_success_but_survives_newer_failure(
    broker, older_rows, newer_response, expected_quantity
):
    """A slow poll must not overwrite a newer successful accessor refresh."""
    old_read_started = Event()
    release_old_read = Event()
    old_thread_name = []

    def transport(**kwargs):
        """Hold only the older request so the public accessor can finish first."""
        if current_thread().name in old_thread_name:
            old_read_started.set()
            assert release_old_read.wait(5), "test failed to release older read"
            return {"code": 0, "data": older_rows}
        return newer_response

    def old_refresh():
        """Identify the background polling thread before starting its read."""
        old_thread_name.append(current_thread().name)
        broker.sync_positions(None)

    broker.api._request.side_effect = transport
    with ThreadPoolExecutor(max_workers=1) as pool:
        older = pool.submit(old_refresh)
        try:
            assert old_read_started.wait(5), "older refresh did not start"
            strategy = object.__new__(Strategy)
            strategy._name = "test_strategy"
            strategy.broker = broker
            asset = Asset("BTCUSDT", Asset.AssetType.CRYPTO_FUTURE)
            if newer_response["code"] != 0:
                with pytest.raises(LumibotBrokerAPIError):
                    strategy.get_position(asset)
            else:
                strategy.get_position(asset)
            revision_after_newer = broker._filled_positions.revision
        finally:
            release_old_read.set()
        older.result(timeout=5)

    remaining = broker.get_tracked_position("test_strategy", Asset("BTCUSDT", Asset.AssetType.CRYPTO_FUTURE))
    if expected_quantity is None:
        assert remaining is None
    else:
        assert remaining.quantity == Decimal(expected_quantity)
    if newer_response["code"] == 0:
        assert broker._filled_positions.revision == revision_after_newer
