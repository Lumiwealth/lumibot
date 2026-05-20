import json
from types import SimpleNamespace

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
