import json
import logging
import datetime as dt
import importlib
from types import SimpleNamespace

import pytest

from lumibot.strategies._strategy import _Strategy


class _Logger:
    def debug(self, *args, **kwargs):
        pass

    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


class _Response:
    status_code = 200
    headers = {}
    text = "OK"

    def json(self):
        return {"ok": True}


def _fake_strategy(*, balances_updated=True):
    strategy = SimpleNamespace()
    strategy.is_backtesting = False
    strategy.lumiwealth_api_key = "test-api-key"
    strategy._logged_missing_lumiwealth_api_key = False
    strategy._name = "Cloud Snapshot Test"
    strategy.broker = SimpleNamespace(name="TestBroker")
    strategy.logger = _Logger()
    strategy._portfolio_value = 1234.56
    strategy._cash_event_pending_for_cloud = []
    strategy._cash_event_cloud_emit_limit = 50
    strategy._cash_event_sent_ids = set()
    strategy._cash_event_sent_id_order = []
    strategy.update_broker_balances = lambda force_update=True: balances_updated
    strategy.get_cash = lambda: 234.56
    strategy.get_positions = lambda: []
    strategy.get_orders = lambda: []
    return strategy


@pytest.mark.parametrize("status", [200, 401, 400, 413, 500])
def test_cloud_transport_never_logs_credentials_or_echoed_headers(monkeypatch, caplog, status):
    strategy = _fake_strategy()
    strategy.lumiwealth_api_key = "synthetic-listener-secret-must-not-be-logged"
    strategy.logger = logging.getLogger("cloud-credential-regression")
    response = _Response()
    response.status_code = status
    response.headers = {"x-echo": strategy.lumiwealth_api_key}
    response.text = strategy.lumiwealth_api_key
    monkeypatch.setattr("lumibot.strategies._strategy.requests.post", lambda *args, **kwargs: response)
    with caplog.at_level(logging.DEBUG):
        _Strategy.send_update_to_cloud(strategy)
    assert strategy.lumiwealth_api_key not in caplog.text
    assert strategy.lumiwealth_api_key[:10] not in caplog.text


def test_cloud_update_marks_successful_broker_snapshot_verified(monkeypatch):
    strategy = _fake_strategy(balances_updated=True)
    payloads = []

    def fake_post(url, headers=None, data=None, **kwargs):
        payloads.append(data)
        return _Response()

    monkeypatch.setattr("lumibot.strategies._strategy.requests.post", fake_post)

    result = _Strategy.send_update_to_cloud(strategy)

    assert result is not False
    assert payloads, "cloud update should be sent after broker balances are verified"
    assert '"account_snapshot_status": "verified"' in payloads[0]
    assert '"account_snapshot_source": "broker_balance_refresh"' in payloads[0]


@pytest.mark.parametrize("sender", ["send_update_to_cloud", "send_account_snapshot_to_cloud"])
def test_balance_observation_time_precedes_slow_funding_fetch(monkeypatch, sender):
    """Funding reads must not move a previously observed balance into the future."""
    from lumibot.entities import CashEventSync

    strategy = _fake_strategy()
    observed = dt.datetime(2026, 1, 2, 12, 0, 0, 123456, tzinfo=dt.timezone.utc)
    clock = {"now": observed}

    class Clock(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return clock["now"].astimezone(tz) if tz else clock["now"].replace(tzinfo=None)

    module = importlib.import_module("lumibot.strategies._strategy")
    monkeypatch.setattr(module, "datetime", SimpleNamespace(
        datetime=Clock, timezone=dt.timezone, timedelta=dt.timedelta,
    ))

    def get_sync(**_kwargs):
        end = observed + dt.timedelta(seconds=1)
        clock["now"] = observed + dt.timedelta(seconds=5)
        return CashEventSync(
            events=(), broker_name="testbroker", account_id="synthetic-account", mode="paper",
            start=observed - dt.timedelta(days=1), end=end,
            fetched_at=clock["now"], state="complete",
        )

    strategy.broker.get_cash_events = lambda **_kwargs: []
    strategy.broker.get_cash_event_sync = get_sync
    payloads = []
    monkeypatch.setattr(module.requests, "post", lambda _url, **kwargs:
                        payloads.append(json.loads(kwargs["data"])) or _Response())

    assert getattr(_Strategy, sender)(strategy) is True
    assert payloads[0]["account_snapshot_checked_at"] == observed.isoformat()
    assert dt.datetime.fromisoformat(payloads[0]["account_snapshot_checked_at"]) < (
        dt.datetime.fromisoformat(payloads[0]["funding_sync"]["requested"]["end"])
    )


def test_cloud_update_uses_production_listener_by_default(monkeypatch):
    strategy = _fake_strategy(balances_updated=True)
    urls = []

    monkeypatch.delenv("LISTENER_WRITE_URL", raising=False)
    monkeypatch.setattr(
        "lumibot.strategies._strategy.requests.post",
        lambda url, **_kwargs: urls.append(url) or _Response(),
    )

    assert _Strategy.send_update_to_cloud(strategy) is not False
    assert urls == ["https://listener.lumiwealth.com/portfolio_events"]


def test_cloud_update_uses_production_listener_for_blank_override(monkeypatch):
    strategy = _fake_strategy(balances_updated=True)
    urls = []

    monkeypatch.setenv("LISTENER_WRITE_URL", "  ")
    monkeypatch.setattr(
        "lumibot.strategies._strategy.requests.post",
        lambda url, **_kwargs: urls.append(url) or _Response(),
    )

    assert _Strategy.send_update_to_cloud(strategy) is not False
    assert urls == ["https://listener.lumiwealth.com/portfolio_events"]


def test_cloud_update_uses_configured_listener(monkeypatch):
    strategy = _fake_strategy(balances_updated=True)
    urls = []
    timeouts = []

    monkeypatch.setenv(
        "LISTENER_WRITE_URL",
        "https://listener.dev.example/portfolio_events",
    )
    monkeypatch.setattr(
        "lumibot.strategies._strategy.requests.post",
        lambda url, **kwargs: (
            urls.append(url),
            timeouts.append(kwargs.get("timeout")),
            _Response(),
        )[-1],
    )

    assert _Strategy.send_update_to_cloud(strategy) is not False
    assert urls == ["https://listener.dev.example/portfolio_events"]
    assert timeouts == [10]


def test_cloud_update_refreshes_positions_before_publish(monkeypatch):
    strategy = _fake_strategy(balances_updated=True)
    sync_calls = []
    strategy.broker.sync_positions = lambda strategy_arg: sync_calls.append(strategy_arg)
    payloads = []

    def fake_post(url, headers=None, data=None, **kwargs):
        payloads.append(json.loads(data))
        return _Response()

    monkeypatch.setattr("lumibot.strategies._strategy.requests.post", fake_post)

    result = _Strategy.send_update_to_cloud(strategy)

    assert result is not False
    assert sync_calls == [strategy]
    assert payloads[0]["positions"] == []
    assert payloads[0]["positions_snapshot_status"] == "verified"
    assert payloads[0]["positions_snapshot_source"] == "broker_positions_refresh"


def test_cloud_update_preserves_terminal_filled_orders(monkeypatch):
    strategy = _fake_strategy(balances_updated=True)
    strategy.get_orders = lambda: [
        SimpleNamespace(
            to_dict=lambda: {
                "identifier": "synthetic-filled-order-123",
                "symbol": "TQQQ",
                "side": "buy",
                "quantity": 100,
                "status": "filled",
                "avg_fill_price": 55.25,
                "broker_update_date": "2026-07-29T13:45:30+00:00",
            }
        )
    ]
    payloads = []

    def fake_post(url, headers=None, data=None, **kwargs):
        payloads.append(json.loads(data))
        return _Response()

    monkeypatch.setattr("lumibot.strategies._strategy.requests.post", fake_post)

    assert _Strategy.send_update_to_cloud(strategy) is not False
    assert payloads[0]["orders"] == [{
        "identifier": "synthetic-filled-order-123",
        "symbol": "TQQQ",
        "side": "buy",
        "quantity": 100,
        "status": "filled",
        "avg_fill_price": 55.25,
        "broker_update_date": "2026-07-29T13:45:30+00:00",
    }]


def test_cloud_update_omits_positions_when_position_refresh_fails(monkeypatch):
    strategy = _fake_strategy(balances_updated=True)

    def raise_position_sync(_strategy):
        raise RuntimeError("broker positions unavailable")

    strategy.broker.sync_positions = raise_position_sync
    payloads = []

    def fake_post(url, headers=None, data=None, **kwargs):
        payloads.append(json.loads(data))
        return _Response()

    monkeypatch.setattr("lumibot.strategies._strategy.requests.post", fake_post)

    result = _Strategy.send_update_to_cloud(strategy)

    assert result is not False
    assert "positions" not in payloads[0]
    assert payloads[0]["positions_snapshot_status"] == "unverified"
    assert payloads[0]["positions_snapshot_source"] == "positions_refresh_failed"


def test_cloud_update_skips_when_broker_balances_are_not_verified(monkeypatch):
    strategy = _fake_strategy(balances_updated=False)
    payloads = []

    def fake_post(url, headers=None, data=None, **kwargs):
        payloads.append(data)
        return _Response()

    monkeypatch.setattr("lumibot.strategies._strategy.requests.post", fake_post)

    result = _Strategy.send_update_to_cloud(strategy)

    assert result is False
    assert payloads == []


@pytest.mark.parametrize("broker_name", ["Alpaca", "Tradier"])
def test_cloud_update_warns_then_recovers_after_temporary_balance_failure(
    monkeypatch, caplog, broker_name
):
    strategy = _fake_strategy()
    strategy.broker = SimpleNamespace(name=broker_name)
    strategy.logger = logging.getLogger(f"tests.cloud_snapshot.{broker_name.lower()}")
    balance_results = iter([False, True])
    strategy.update_broker_balances = lambda force_update=True: next(balance_results)
    payloads = []

    def fake_post(url, headers=None, data=None, **kwargs):
        payloads.append(json.loads(data))
        return _Response()

    monkeypatch.setattr("lumibot.strategies._strategy.requests.post", fake_post)
    caplog.set_level(logging.DEBUG)

    first_result = _Strategy.send_update_to_cloud(strategy)
    second_result = _Strategy.send_update_to_cloud(strategy)

    assert first_result is False
    assert second_result is not False
    assert len(payloads) == 1
    assert payloads[0]["account_snapshot_status"] == "verified"
    assert any(
        record.levelno == logging.WARNING
        and "broker did not return verified balances" in record.getMessage()
        and "No stale or default balances were published" in record.getMessage()
        and "next cloud update will retry automatically" in record.getMessage()
        for record in caplog.records
    )
    assert all(record.levelno < logging.ERROR for record in caplog.records)
