import json
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import jsonpickle

from lumibot.brokers.projectx import ProjectX
from lumibot.data_sources.projectx_data import ProjectXData
from lumibot.entities import Asset
from lumibot.strategies import Strategy


def test_settings_record_connection_metadata_without_serializing_credentials(tmp_path):
    history = ProjectXData({
        "firm": "TEST", "username": "fixture-user", "api_key": "fixture-history-key",
        "base_url": "https://example.invalid/", "streaming_base_url": "https://example.invalid/",
    })
    # Shape observed in an authenticated data-source connection; no provider call.
    history.client._client = SimpleNamespace(
        token="fixture-history-token",
        api=SimpleNamespace(headers={"Authorization": "Bearer fixture-history-token"}),
    )
    broker = ProjectX.__new__(ProjectX)
    broker.config = {"api_key": "fixture-broker-key"}
    parameters = {
        "lookback": 20, "token": "ETH", "threshold": Decimal("1.25"),
        "asset": Asset("MES", asset_type="cont_future", multiplier=5),
        "history": history, "nested": {"sources": [history], "broker": broker},
    }
    strategy = SimpleNamespace(
        broker=SimpleNamespace(data_source=SimpleNamespace(auto_adjust=False, SOURCE="PANDAS")),
        name="settings-privacy", backtesting_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        backtesting_end=datetime(2026, 1, 2, tzinfo=timezone.utc), initial_budget=5000,
        risk_free_rate=0, minutes_before_closing=0, minutes_before_opening=0,
        sleeptime="1D", quote_asset=None, _benchmark_asset=None,
        starting_positions=None, parameters=parameters,
    )
    output = tmp_path / "settings.json"

    Strategy.write_backtest_settings(strategy, str(output))

    text = output.read_text()
    for credential in ("fixture-history-key", "fixture-history-token", "fixture-broker-key"):
        assert credential not in text
    decoded = jsonpickle.decode(text)["parameters"]
    assert decoded["history"] == {"object_type": "ProjectXData", "data_source": "PROJECTX"}
    assert decoded["nested"]["sources"][0] == decoded["history"]
    assert decoded["nested"]["broker"] == {"object_type": "ProjectX"}
    assert decoded["threshold"] == Decimal("1.25")
    assert decoded["asset"] == parameters["asset"]
    assert decoded["asset"].multiplier == 5
    assert decoded["token"] == "ETH"
    assert decoded["lookback"] == 20
    assert parameters["history"] is history
    assert history.config["api_key"] == "fixture-history-key"
    assert history.client._client.token == "fixture-history-token"
    assert broker.config["api_key"] == "fixture-broker-key"
    assert json.loads(text)["lumibot_version"]
