from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest


def test_get_request_uses_downloader_when_base_url_set(monkeypatch):
    import lumibot.tools.data_downloader_queue_client as queue_client
    import lumibot.tools.thetadata_helper as thetadata_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://example:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "test-key")

    calls = {"queue": 0}

    def fake_queue_request(url: str, querystring, headers=None, timeout=None):
        calls["queue"] += 1
        return {"header": {"format": []}, "response": [[1]]}

    monkeypatch.setattr(queue_client, "queue_request", fake_queue_request)
    monkeypatch.setattr(
        thetadata_helper.requests,
        "get",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("requests.get should not be called in downloader mode")),
    )
    monkeypatch.setattr(
        thetadata_helper,
        "check_connection",
        lambda *_a, **_k: (_ for _ in ()).throw(
            AssertionError("check_connection should not be called in downloader mode")
        ),
    )

    result = thetadata_helper.get_request(
        url="http://example:8080/v3/stock/snapshot/ohlc",
        headers={},
        querystring={"symbol": "SPY", "format": "json"},
    )

    assert calls["queue"] == 1
    assert isinstance(result, dict)
    assert result["response"] == [[1]]


def test_get_request_raises_when_downloader_base_url_set_but_api_key_missing(monkeypatch):
    import lumibot.tools.thetadata_helper as thetadata_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://example:8080")
    monkeypatch.delenv("DATADOWNLOADER_API_KEY", raising=False)

    with pytest.raises(RuntimeError, match="DATADOWNLOADER_API_KEY"):
        thetadata_helper.get_request(
            url="http://example:8080/v3/stock/snapshot/ohlc",
            headers={},
            querystring={"symbol": "SPY", "format": "json"},
        )


def test_get_request_uses_local_theta_terminal_when_base_url_unset(monkeypatch):
    import lumibot.tools.data_downloader_queue_client as queue_client
    import lumibot.tools.thetadata_helper as thetadata_helper

    monkeypatch.delenv("DATADOWNLOADER_BASE_URL", raising=False)
    monkeypatch.delenv("DATADOWNLOADER_API_KEY", raising=False)

    monkeypatch.setattr(
        queue_client,
        "queue_request",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("queue_request should not be called in local mode")),
    )
    monkeypatch.setattr(thetadata_helper, "check_connection", lambda *_a, **_k: (None, True))

    fake_response = SimpleNamespace(
        status_code=200,
        text='{"header":{"format":[]},"response":[[1]]}',
        json=lambda: {"header": {"format": []}, "response": [[1]]},
    )
    monkeypatch.setattr(thetadata_helper.requests, "get", lambda *_a, **_k: fake_response)

    result = thetadata_helper.get_request(
        url="http://127.0.0.1:25503/v3/stock/snapshot/ohlc",
        headers={},
        querystring={"symbol": "SPY"},
        username="user",
        password="pass",
    )

    assert isinstance(result, dict)
    assert result["response"] == [[1]]


def test_theta_backtesting_does_not_kill_theta_terminal_when_downloader_configured(monkeypatch):
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://example:8080")

    from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas

    monkeypatch.setattr(
        ThetaDataBacktestingPandas,
        "kill_processes_by_name",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("Should not kill ThetaTerminal.jar in downloader mode")),
    )

    ThetaDataBacktestingPandas(
        datetime_start=datetime(2024, 1, 1),
        datetime_end=datetime(2024, 1, 2),
        username="user",
        password="pass",
        name="TestBacktest",
    )


def test_coerce_json_payload_converts_enveloped_columnar_response():
    from lumibot.tools.thetadata_helper import _coerce_json_payload

    payload = {
        "header": {"format": ["ms_of_day", "open"]},
        "response": {"ms_of_day": [1000, 2000], "open": [1.0, 2.0]},
    }
    coerced = _coerce_json_payload(payload)
    assert coerced["header"]["format"] == ["ms_of_day", "open"]
    assert coerced["response"] == [[1000, 1.0], [2000, 2.0]]
