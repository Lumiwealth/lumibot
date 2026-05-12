from __future__ import annotations

from datetime import UTC, datetime


def test_to_utc_localizes_naive_datetimes_with_dst_rules():
    """Regression test: pytz tzinfo must be attached via localize(), not replace().

    If we incorrectly do `dt.replace(tzinfo=America/New_York)` with pytz, the timezone can
    behave like historical "LMT" (≈ -04:56) which shifts requests and can create pagination
    gaps in IBKR history fetches.
    """
    import lumibot.tools.ibkr_helper as ibkr_helper

    # DST (EDT, UTC-4)
    dt_edt = datetime(2025, 10, 30, 0, 0, 0)
    assert ibkr_helper._to_utc(dt_edt) == datetime(2025, 10, 30, 4, 0, 0, tzinfo=UTC)

    # Standard time (EST, UTC-5) after the 2025 DST fall-back.
    dt_est = datetime(2025, 11, 3, 0, 0, 0)
    assert ibkr_helper._to_utc(dt_est) == datetime(2025, 11, 3, 5, 0, 0, tzinfo=UTC)
