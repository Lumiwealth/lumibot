from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

from lumibot.brokers.tradier import Tradier


@pytest.fixture
def broker():
    broker = object.__new__(Tradier)
    broker._tradier_account_number = "VA000001"
    broker.tradier = SimpleNamespace(account=SimpleNamespace(request=Mock()))
    return broker


@pytest.mark.parametrize("wrapped", [False, True])
def test_tradier_reads_all_native_daily_account_balances(broker, wrapped):
    balances = {
        "balances": [
            {"date": "2026-01-05", "value": 1_025.25},
            {"date": "2024-03-04", "value": 1_000.0},
            {"date": "2024-03-05", "value": 0.0},
        ],
        "delta": 25.25,
        "deltaPercent": 2.525,
    }
    broker.tradier.account.request.return_value = (
        {"historical_balances": balances} if wrapped else balances
    )

    result = broker.get_historical_account_value()

    broker.tradier.account.request.assert_called_once_with(
        endpoint="v1/accounts/VA000001/historical-balances", params={"period": "ALL"}
    )
    assert set(result) == {"day"}
    assert result["day"].index.tolist() == list(
        pd.to_datetime(["2024-03-04", "2024-03-05", "2026-01-05"], utc=True)
    )
    assert result["day"]["equity"].tolist() == [1_000.0, 0.0, 1_025.25]


@pytest.mark.parametrize("balances", [None, [], "null"])
def test_tradier_preserves_empty_account_history(broker, balances):
    broker.tradier.account.request.return_value = {"historical_balances": {"balances": balances}}
    assert broker.get_historical_account_value()["day"].empty


def test_tradier_rejects_corrupt_history_without_returning_a_partial_series(broker):
    broker.tradier.account.request.return_value = {
        "balances": [{"date": "2024-03-04", "value": 500}, {"date": "bad", "value": 1}]
    }
    with pytest.raises(ValueError):
        broker.get_historical_account_value()
