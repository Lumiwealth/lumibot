"""One Schwab account read per sync: balances and positions share one answer.

A trading iteration asks Schwab for the account twice (cash/portfolio value, then
positions), and the held-trades loop can ask again. Each is the same
get_account(POSITIONS) call. A fast loop at one pass a second spends a large share of
Schwab's per-minute budget on these repeats. The answer is reused for a short window
(SCHWAB_ACCOUNT_SNAPSHOT_SECONDS, default 1.0; 0 turns reuse off) and dropped at once on
any order event, submit or cancel, so a fill is never read from an old snapshot.
Neutral synthetic data, no network.
"""
from types import SimpleNamespace

import pytest

from lumibot.brokers.schwab import Schwab
from lumibot.entities import Asset


class _Response:
    status_code = 200
    text = "OK"

    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def _payload(cash):
    return {
        "securitiesAccount": {
            "type": "MARGIN",
            "currentBalances": {"cashBalance": cash, "liquidationValue": cash + 1000.0},
            "positions": [
                {"instrument": {"assetType": "EQUITY", "symbol": "LWX"}, "longQuantity": 0,
                 "shortQuantity": 100, "averagePrice": 10.0, "marketValue": -1000.0},
            ],
        }
    }


class _Client:
    Account = SimpleNamespace(Fields=SimpleNamespace(POSITIONS="positions"))

    def __init__(self):
        self.calls = 0
        self.cash = 5000.0

    def get_account(self, hash_value, fields):
        self.calls += 1
        return _Response(_payload(self.cash))


def _broker(monkeypatch, seconds=None):
    if seconds is None:
        monkeypatch.delenv("SCHWAB_ACCOUNT_SNAPSHOT_SECONDS", raising=False)
    else:
        monkeypatch.setenv("SCHWAB_ACCOUNT_SNAPSHOT_SECONDS", str(seconds))
    broker = Schwab.__new__(Schwab)
    broker._broker_fully_ready = True
    broker.schwab_authorization_error = False
    broker.client = _Client()
    broker.hash_value = "account-hash"
    broker._schwab_pace = lambda kind: None
    broker._tracked_orders_cache_key = None
    broker._tracked_orders_filter_cache = {}
    broker._active_tracked_orders_filter_cache = {}
    return broker


def _strategy():
    return SimpleNamespace(name="s", quote_asset=Asset("USD", asset_type="forex"))


def test_balances_then_positions_in_one_sync_make_one_account_read(monkeypatch):
    broker = _broker(monkeypatch)
    assert broker._get_balances_at_broker(Asset("USD", asset_type="forex"), _strategy())[0] == 5000.0
    broker._pull_positions(_strategy())
    assert broker.client.calls == 1


def test_an_order_event_drops_the_snapshot_so_the_next_read_is_fresh(monkeypatch):
    broker = _broker(monkeypatch)
    broker._get_balances_at_broker(Asset("USD", asset_type="forex"), _strategy())
    broker.client.cash = 4000.0
    broker._invalidate_order_caches()  # every order/fill reconciliation calls this
    assert broker._get_balances_at_broker(Asset("USD", asset_type="forex"), _strategy())[0] == 4000.0
    assert broker.client.calls == 2


def test_the_snapshot_expires(monkeypatch):
    broker = _broker(monkeypatch)
    clock = {"now": 100.0}
    monkeypatch.setattr("lumibot.brokers.schwab.time.monotonic", lambda: clock["now"])
    broker._get_balances_at_broker(Asset("USD", asset_type="forex"), _strategy())
    clock["now"] += 1.5
    broker._pull_positions(_strategy())
    assert broker.client.calls == 2


@pytest.mark.parametrize("seconds", [0])
def test_zero_turns_reuse_off(monkeypatch, seconds):
    broker = _broker(monkeypatch, seconds)
    broker._get_balances_at_broker(Asset("USD", asset_type="forex"), _strategy())
    broker._pull_positions(_strategy())
    assert broker.client.calls == 2
