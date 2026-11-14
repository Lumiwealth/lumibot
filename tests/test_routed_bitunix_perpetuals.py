"""Opt-in Bitunix perpetual routing: real perpetual candles, contract terms and funding.

Responses are recorded from Bitunix's public market endpoints (no API key) on
2026-11-13 and replayed through a fake HTTP session, so these tests never touch
the network. They prove:

* routing ``crypto_future`` to ``bitunix`` reads perpetual candles, not spot,
* contract terms come from the exchange, so perpetual orders fill,
* settled funding is charged to an open position,
* a coin-margined contract fails up front with a plain message,
* the default spot-proxy route is unchanged (still "terms unavailable").
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker
from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas, RoutingProviderError
from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
from lumibot.entities import Asset
from lumibot.tools.bitunix_market_data import BitunixMarketData, BitunixMarketDataError, resolve_symbol
from tests.test_backtesting_multileg_unit import _StubStrategy

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "bitunix_public_perpetual_market.json").read_text())


class _Response:
    def __init__(self, body, status=200):
        self._body, self.status_code = body, status

    def json(self):
        return self._body


class _RecordedSession:
    """Serves the recorded Bitunix answers by endpoint name and records each request."""

    def __init__(self, status=200):
        self.calls = []
        self.status = status

    def get(self, url, params=None, timeout=None):
        endpoint = url.rsplit("/", 1)[-1]
        self.calls.append((endpoint, dict(params or {})))
        if self.status != 200:
            return _Response({"msg": "blocked"}, self.status)
        if endpoint == "trading_pairs":
            wanted = set(str(params.get("symbols", "")).split(","))
            body = dict(FIXTURE["trading_pairs"]["response"])
            body["data"] = [row for row in body["data"] if row["symbol"] in wanted]
            return _Response(body)
        if endpoint == "kline":
            return _Response(FIXTURE["kline_1h_page"]["response"])
        if endpoint == "get_funding_rate_history":
            return _Response(FIXTURE["funding_history"]["response"])
        raise AssertionError(f"unexpected endpoint {endpoint}")


def _client(session=None):
    return BitunixMarketData(session=session or _RecordedSession(), pause_seconds=0)


def _router(routing, monkeypatch, session=None):
    import lumibot.tools.thetadata_helper as thetadata_helper

    monkeypatch.setenv("LUMIBOT_SHARED_BARS", "false")
    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_a, **_k: None)
    monkeypatch.setattr(thetadata_helper, "reset_theta_terminal_tracking", lambda *_a, **_k: None)
    router = RoutedBacktestingPandas(
        datetime_start=datetime(2026, 8, 3, 3, tzinfo=timezone.utc),
        datetime_end=datetime(2026, 8, 3, 9, tzinfo=timezone.utc),
        config={"backtesting_data_routing": routing},
        username="dev", password="dev", use_quote_data=False,
        show_progress_bar=False, log_backtest_progress_to_file=False,
    )
    adapter = router._registry._adapters["bitunix"]
    adapter._client = _client(session)
    return router


PERP = Asset("BTCUSDT", asset_type="crypto_future")


@pytest.mark.parametrize("asset,quote,expected", [
    (Asset("BTCUSDT", "crypto_future"), None, "BTCUSDT"),
    (Asset("BTC/USDT:USDT", "crypto_future"), None, "BTCUSDT"),
    (Asset("BTC", "crypto_future"), Asset("USDT", "crypto"), "BTCUSDT"),
    (Asset("ETHUSDC", "crypto_future"), None, "ETHUSDC"),
])
def test_symbol_resolution(asset, quote, expected):
    assert resolve_symbol(asset, quote) == expected


def test_bare_coin_with_usd_quote_is_not_guessed():
    with pytest.raises(BitunixMarketDataError, match="BTCUSDT"):
        resolve_symbol(Asset("BTC", "crypto_future"), Asset("USD", "forex"))


def test_linear_contract_terms_come_from_recorded_metadata():
    spec = _client().contract_spec("BTCUSDT")
    assert (spec.base, spec.quote, spec.settle, spec.inverse, spec.quantity_unit) == (
        "BTC", "USDT", "USDT", False, "base")


def test_coin_margined_contract_is_refused_plainly():
    with pytest.raises(BitunixMarketDataError, match="coin-margined"):
        _client().contract_spec("BTCUSD")


def test_candles_are_real_perpetual_bars_in_time_order():
    frame = _client().candles("BTCUSDT", "hour", "2026-08-03T03:00Z", "2026-08-03T08:00Z")
    recorded = sorted(FIXTURE["kline_1h_page"]["response"]["data"], key=lambda row: int(row["time"]))
    assert list(frame.index) == [pd.Timestamp(int(row["time"]), unit="ms", tz="UTC") for row in recorded]
    assert list(frame["close"]) == [float(row["close"]) for row in recorded]
    assert list(frame["volume"]) == [float(row["baseVol"]) for row in recorded]


def test_short_pages_mid_history_keep_paging_back():
    recorded = FIXTURE["kline_1h_page"]["response"]["data"]

    class _ShortPages(_RecordedSession):
        def get(self, url, params=None, timeout=None):
            self.calls.append(("kline", dict(params)))
            older = [row for row in recorded if int(row["time"]) < int(params["endTime"])]
            older.sort(key=lambda row: -int(row["time"]))
            return _Response({"code": 0, "data": older[:2], "msg": "Success"})  # 2 < page limit

    session = _ShortPages()
    frame = _client(session).candles("BTCUSDT", "hour", "2026-08-03T03:00Z", "2026-08-03T08:00Z")
    assert len(frame) == len(recorded)
    assert len(session.calls) >= 3


def test_empty_page_mid_history_does_not_truncate_the_series():
    # Seen on 2026-11-13: a year of 1-minute paging stopped after 106,000 bars on an empty
    # page although Bitunix serves the older bars. A gap must never cut the series short.
    recorded = FIXTURE["kline_1h_page"]["response"]["data"]
    newest = max(int(row["time"]) for row in recorded)

    class _GapThenOlder(_RecordedSession):
        empty_once = True

        def get(self, url, params=None, timeout=None):
            self.calls.append(("kline", dict(params)))
            end = int(params["endTime"])
            if end > newest:  # newest page: only the newest bar
                data = [row for row in recorded if int(row["time"]) == newest]
            elif end == newest and self.empty_once:  # a transient empty answer mid-history
                self.empty_once, data = False, []
            else:
                data = [row for row in recorded if int(row["time"]) < end]
            return _Response({"code": 0, "data": data, "msg": "Success"})

    frame = _client(_GapThenOlder()).candles("BTCUSDT", "hour", "2026-08-02T00:00Z", "2026-08-03T08:00Z")
    assert len(frame) == len(recorded)


def test_same_series_downloads_once_per_process():
    # A year of minute bars is ~2,600 requests; asking again under another quote spelling
    # must not download it a second time.
    session = _RecordedSession()
    client = _client(session)
    first = client.candles("BTCUSDT", "hour", "2026-08-03T03:00Z", "2026-08-03T08:00Z")
    calls = len(session.calls)
    second = client.candles("BTCUSDT", "hour", "2026-08-03T03:00Z", "2026-08-03T08:00Z")
    assert len(session.calls) == calls
    pd.testing.assert_frame_equal(first, second)


def test_blocked_location_reports_unavailable_instead_of_empty():
    with pytest.raises(BitunixMarketDataError, match="HTTP 451"):
        _client(_RecordedSession(status=451)).candles("BTCUSDT", "hour", "2026-08-03T03:00Z", "2026-08-03T08:00Z")


def test_funding_history_keeps_rate_and_settlement_mark():
    rows = _client().funding_history("BTCUSDT", "2026-08-03T00:00Z", "2026-08-03T09:00Z")
    assert [row["timestamp"] for row in rows] == [1785715200000, 1785744000000]
    assert rows[1]["fundingRate"] == Decimal("0.00006673")
    assert rows[1]["markPrice"] == Decimal("62591.1")


def test_bitunix_route_serves_perpetual_candles_not_spot(monkeypatch):
    router = _router({"crypto_future": "bitunix", "crypto": "coinbase", "default": "ibkr"}, monkeypatch)
    router._datetime = pd.Timestamp("2026-08-03 08:00", tz="UTC").to_pydatetime()
    bars = router.get_historical_prices(PERP, 2, timestep="hour", quote=Asset("USDT", "crypto"))
    assert bars is not None and not bars.df.empty
    recorded = {pd.Timestamp(int(r["time"]), unit="ms", tz="UTC"): float(r["close"])
                for r in FIXTURE["kline_1h_page"]["response"]["data"]}
    for stamp, close in bars.df["close"].items():
        assert recorded[pd.Timestamp(stamp).tz_convert("UTC")] == close


def test_bitunix_route_refuses_coin_margined_before_any_order(monkeypatch):
    router = _router({"crypto_future": "bitunix", "default": "ibkr"}, monkeypatch)
    with pytest.raises(RoutingProviderError, match="coin-margined"):
        router._update_pandas_data(Asset("BTCUSD", "crypto_future"), None, 2, "hour")


def _marker_payload(exc):
    line = next(line for line in str(exc).splitlines() if line.startswith("PERPETUAL_DATA_UNAVAILABLE "))
    return json.loads(line.split(" ", 1)[1])


def test_failure_carries_machine_readable_reason_and_alternatives(monkeypatch):
    from lumibot.backtesting.routed_backtesting import PerpetualDataUnavailable

    router = _router({"crypto_future": "bitunix", "default": "ibkr"}, monkeypatch)
    with pytest.raises(PerpetualDataUnavailable) as caught:
        router._update_pandas_data(Asset("BTCUSD", "crypto_future"), None, 2, "hour")
    payload = _marker_payload(caught.value)
    assert payload["reason"] == caught.value.reason == "coin_margined_unsupported"
    assert payload["source"] == "bitunix" and payload["asset_class"] == "crypto_future"
    assert payload["symbol"] == "BTCUSD"
    assert {item["routing"] for item in payload["alternatives"]} == {"coinbase", "ibkr"}
    assert "Other crypto_future data sources" in str(caught.value)


def test_unreachable_source_fails_before_trading_with_reason(monkeypatch):
    from lumibot.backtesting.routed_backtesting import PerpetualDataUnavailable

    router = _router({"crypto_future": "bitunix", "default": "ibkr"}, monkeypatch,
                     session=_RecordedSession(status=451))
    with pytest.raises(PerpetualDataUnavailable) as caught:
        router._update_pandas_data(PERP, Asset("USDT", "crypto"), 2, "hour")
    assert _marker_payload(caught.value)["reason"] == "source_unreachable"


def test_bitunix_route_never_serves_spot_assets(monkeypatch):
    router = _router({"crypto": "bitunix", "default": "ibkr"}, monkeypatch)
    with pytest.raises(RoutingProviderError, match="crypto perpetuals only"):
        router._update_pandas_data(Asset("BTC", "crypto"), Asset("USD", "forex"), 2, "hour")


def test_default_spot_proxy_route_is_unchanged(monkeypatch):
    # Existing spot-proxy routing reads spot history for perpetuals and has no contract terms.
    # This change must not alter what those backtests compute.
    router = _router({"crypto_future": "coinbase", "crypto": "coinbase", "default": "ibkr"}, monkeypatch)
    with pytest.raises(ValueError, match="Derivative contract terms are unavailable"):
        router.get_contract_spec(PERP)
    assert router.get_funding_events(PERP, pd.Timestamp("2026-08-03", tz="UTC"),
                                     pd.Timestamp("2026-08-04", tz="UTC")) is None


def test_usd_budget_for_usdt_contract_explains_how_to_fund_the_wallet(monkeypatch):
    router = _router({"crypto_future": "bitunix", "default": "ibkr"}, monkeypatch)
    router._datetime = pd.Timestamp("2026-08-03 05:00", tz="UTC").to_pydatetime()
    broker = BacktestingBroker(router)
    try:
        strategy = _StubStrategy(broker=broker, quote_asset=Asset("USD", "forex"), budget=1000,
                                 analyze_backtest=False)
        order = strategy.create_order(Asset("BTCUSDT", "crypto_future", leverage=10), Decimal("0.01"), "buy")
        broker._process_new_order(order)
        broker._execute_filled_order(order, 62600.0, Decimal("0.01"), strategy)
        assert order.status == "error"
        assert "quote_asset=Asset('USDT', 'crypto')" in str(order.error_message or order._error)
    finally:
        broker.cleanup_streams()


def test_perpetual_order_fills_with_margin_and_pays_funding(monkeypatch):
    router = _router({"crypto_future": "bitunix", "default": "ibkr"}, monkeypatch)
    router._datetime = pd.Timestamp("2026-08-03 05:00", tz="UTC").to_pydatetime()
    broker = BacktestingBroker(router)
    try:
        strategy = _StubStrategy(broker=broker, quote_asset=Asset("USDT", "crypto"), budget=1000,
                                 analyze_backtest=False)
        asset = Asset("BTCUSDT", "crypto_future", leverage=10)
        order = strategy.create_order(asset, Decimal("0.01"), "buy")
        broker._process_new_order(order)
        broker._execute_filled_order(order, 62600.0, Decimal("0.01"), strategy)
        assert order.status != "error", order
        assert float(broker.get_tracked_position(strategy.name, asset).quantity) == pytest.approx(0.01)
        # 10x leverage: 626 USDT of exposure reserves 62.6 USDT; cash is not spent on notional.
        assert strategy.cash == pytest.approx(1000)

        router._datetime = pd.Timestamp("2026-08-03 08:30", tz="UTC").to_pydatetime()
        broker._perpetuals.process_events(strategy)
        funding = [event for event in broker.perpetual_events if event["type"] == "funding"]
        # Long pays the 08:00 UTC settlement: 0.01 BTC x 62591.1 mark x 0.00006673.
        assert len(funding) == 1
        assert Decimal(funding[0]["amount"]) == pytest.approx(Decimal("-0.01") * Decimal("62591.1")
                                                              * Decimal("0.00006673"))
        assert strategy.cash == pytest.approx(1000 - 0.01 * 62591.1 * 0.00006673)
    finally:
        broker.cleanup_streams()
