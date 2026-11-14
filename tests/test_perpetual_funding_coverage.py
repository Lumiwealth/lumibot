"""Missing funding history is reported, never shown as zero funding.

Bitunix funding history starts on 2024-03-04. A backtest holding a perpetual before the
first published settlement used to charge nothing and say nothing, so a run looked like
it had zero funding cost. The run must name the uncovered dates in its logs and in a
machine-readable record, and record which exchange supplied prices and which supplied
funding. Recorded public Bitunix responses; no network.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal

import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker
from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas
from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
from lumibot.entities import Asset
from tests.test_backtesting_multileg_unit import _StubStrategy
from tests.test_routed_bitunix_perpetuals import FIXTURE, _client, _RecordedSession

HISTORY_STARTS = 1785715200000  # 2026-08-03 00:00 UTC: first settlement this source publishes


class _HistoryStartsLate(_RecordedSession):
    """Funding history only from HISTORY_STARTS, like Bitunix before 2024-03-04."""

    def get(self, url, params=None, timeout=None):
        response = super().get(url, params, timeout)
        if url.endswith("get_funding_rate_history"):
            body = dict(response.json())
            body["data"] = [row for row in body["data"] if int(row["fundingTime"]) >= HISTORY_STARTS]
            response._body = body
        return response


def _broker(monkeypatch, session):
    import lumibot.tools.thetadata_helper as thetadata_helper

    monkeypatch.setenv("LUMIBOT_SHARED_BARS", "false")
    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_a, **_k: None)
    monkeypatch.setattr(thetadata_helper, "reset_theta_terminal_tracking", lambda *_a, **_k: None)
    router = RoutedBacktestingPandas(
        datetime_start=datetime(2026, 8, 2, 0, tzinfo=timezone.utc),
        datetime_end=datetime(2026, 8, 3, 9, tzinfo=timezone.utc),
        config={"backtesting_data_routing": {"crypto_future": "bitunix", "default": "ibkr"}},
        username="dev", password="dev", use_quote_data=False,
        show_progress_bar=False, log_backtest_progress_to_file=False,
    )
    router._registry._adapters["bitunix"]._client = _client(session)
    router._datetime = pd.Timestamp("2026-08-02 01:00", tz="UTC").to_pydatetime()
    return router, BacktestingBroker(router)


def _open_long(broker, router):
    strategy = _StubStrategy(broker=broker, quote_asset=Asset("USDT", "crypto"), budget=1000, analyze_backtest=False)
    asset = Asset("BTCUSDT", "crypto_future", leverage=10)
    order = strategy.create_order(asset, Decimal("0.01"), "buy")
    broker._process_new_order(order)
    broker._execute_filled_order(order, 62600.0, Decimal("0.01"), strategy)
    assert order.status != "error", order
    router._datetime = pd.Timestamp("2026-08-03 08:30", tz="UTC").to_pydatetime()
    broker._perpetuals.process_events(strategy)
    return strategy


def test_funding_gap_is_reported_with_covered_and_uncovered_dates(monkeypatch, caplog):
    router, broker = _broker(monkeypatch, _HistoryStartsLate())
    try:
        _open_long(broker, router)
        notices = broker.perpetual_funding_unavailable
        assert len(notices) == 1
        notice = notices[0]
        assert notice["symbol"] == "BTCUSDT"
        assert notice["source"] == "bitunix"
        assert notice["covered_from"] == "2026-08-03T00:00:00+00:00"
        assert notice["not_covered"] == [["2026-08-02T01:00:00+00:00", "2026-08-03T00:00:00+00:00"]]
        # Logs and the run's assumptions carry one machine-readable line.
        line = next(text for text in broker.backtest_assumptions if "PERPETUAL_FUNDING_UNAVAILABLE" in text)
        payload = json.loads(line.split("PERPETUAL_FUNDING_UNAVAILABLE ", 1)[1])
        assert payload["not_covered"] == notice["not_covered"]
        assert "not zero funding" in line
        # Settlements that do exist are still charged.
        assert [event["timestamp"] for event in broker.perpetual_events if event["type"] == "funding"] == [
            "2026-08-03T00:00:00+00:00", "2026-08-03T08:00:00+00:00"]
    finally:
        broker.cleanup_streams()


def test_full_funding_history_reports_nothing_and_records_the_sources(monkeypatch):
    router, broker = _broker(monkeypatch, _RecordedSession())
    try:
        _open_long(broker, router)
        assert broker.perpetual_funding_unavailable == []
        assert broker.perpetual_data_sources == {"BTCUSDT": {"prices": "bitunix", "funding": "bitunix"}}
    finally:
        broker.cleanup_streams()
