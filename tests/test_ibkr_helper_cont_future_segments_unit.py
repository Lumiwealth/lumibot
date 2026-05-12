from __future__ import annotations

from datetime import UTC, datetime

from lumibot.entities import Asset
from lumibot.tools import ibkr_helper


def test_ibkr_helper_cont_future_segments_span_roll_produces_multiple_contracts(monkeypatch):
    # MES rolls 8 business days before the quarterly expiry (third Friday).
    # The Dec 2025 expiry is 2025-12-19, so a window spanning early/mid Dec crosses the roll.
    asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

    start = datetime(2025, 12, 5, 0, 0, tzinfo=UTC)
    end = datetime(2025, 12, 15, 0, 0, tzinfo=UTC)

    # Unit test: stub conid resolution so we don't require a populated conids.json or a live downloader.
    def _fake_resolve_conid(*, asset, quote, exchange):  # noqa: ANN001
        return 1

    monkeypatch.setattr(ibkr_helper, "_resolve_conid", _fake_resolve_conid)
    segments = ibkr_helper._resolve_cont_future_segments(asset=asset, start_dt=start, end_dt=end, exchange="CME")
    assert segments, "Expected at least one cont_future segment"
    assert len(segments) >= 2, f"Expected roll schedule to include >=2 segments, got {len(segments)}"

    expirations = [getattr(contract_asset, "expiration", None) for contract_asset, _, _ in segments]
    assert all(expirations), f"Expected each contract segment to have an expiration; got {expirations}"
    assert len(set(expirations)) >= 2, f"Expected at least 2 distinct expirations; got {expirations}"

    # Ensure segments are ordered and non-overlapping.
    for (_a0, s0, e0), (_a1, s1, e1) in zip(segments, segments[1:], strict=False):
        assert s0 < e0
        assert s1 < e1
        assert e0 <= s1
