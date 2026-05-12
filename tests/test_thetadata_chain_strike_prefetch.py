from datetime import date, timedelta

from lumibot.entities import Asset
from lumibot.tools import thetadata_helper


class _FakeQueueClient:
    def __init__(self):
        self.max_concurrent = 8
        self.submitted: list[tuple[str, str]] = []

    def check_or_submit(self, *, method, path, query_params, headers):
        expiration = query_params["expiration"]
        symbol = query_params["symbol"]
        self.submitted.append((symbol, expiration))
        return (f"req::{symbol}::{expiration}", "completed", False)

    def wait_for_result(self, *, request_id: str, timeout: float):
        _prefix, symbol, expiration = request_id.split("::", maxsplit=2)
        # Minimal strike list payload.
        payload = {"header": {"format": ["strike"]}, "response": [[100.0], [105.0], [110.0]]}
        return (payload, 200)


def _fake_expirations_payload(expiration_values: list[str]) -> dict:
    return {"header": {"format": ["expiration"]}, "response": [[exp] for exp in expiration_values]}


def test_build_historical_chain_prefetches_head_and_tail_only(monkeypatch):
    # 100 expirations starting from as_of_date, spaced 1 day apart.
    as_of = date(2025, 1, 2)
    expirations = [(as_of + timedelta(days=i)).isoformat() for i in range(100)]

    fake_queue = _FakeQueueClient()
    monkeypatch.setattr("lumibot.tools.data_downloader_queue_client.get_queue_client", lambda: fake_queue)

    def fake_get_request(*, url, headers, querystring):
        if url.endswith(thetadata_helper.OPTION_LIST_ENDPOINTS["expirations"]):
            return _fake_expirations_payload(expirations)
        raise AssertionError(f"Unexpected request url={url}")

    monkeypatch.setattr(thetadata_helper, "get_request", fake_get_request)

    chain = thetadata_helper.build_historical_chain(
        Asset("AAA", asset_type=Asset.AssetType.STOCK),
        as_of,
        chain_constraints=None,
    )

    assert chain is not None
    call_map = chain["Chains"]["CALL"]

    # We should preserve the full expiration list as keys.
    assert set(call_map.keys()) == set(expirations)

    expected_prefetch = set(expirations[:14] + expirations[-14:])
    submitted_expirations = {exp for _sym, exp in fake_queue.submitted}
    assert submitted_expirations == expected_prefetch

    # Prefetched expirations should have strikes; others should be empty placeholders.
    for exp in expirations:
        strikes = call_map[exp]
        if exp in expected_prefetch:
            assert strikes
        else:
            assert strikes == []
