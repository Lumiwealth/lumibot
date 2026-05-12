from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace

import pandas as pd
import pytz

from lumibot.tools import thetadata_helper


def test_get_missing_dates_uses_market_day_not_utc_day_for_intraday_cache(monkeypatch) -> None:
    """
    Regression test: compute missing trading days using the *market-local* day, not the UTC day.

    Why this matters:
    - ThetaData intraday caches are stored in UTC timestamps.
    - When we fetch extended-hours data (04:00-20:00 ET), after-hours bars cross midnight UTC.
      Example: 2025-02-03 19:59 ET == 2025-02-04 00:59 UTC.
    - If we use `df.index.date` (UTC dates), we can incorrectly think the *next* trading day is
      already covered and skip downloading it.

    Observed impact (NVDA backtests):
    - Cache contained ET trading days {Feb 3, Feb 5}, but UTC dates appeared as {Feb 3, Feb 4, Feb 5, Feb 6}
      due to after-hours spillover.
    - `get_missing_dates()` returned `[]`, so we never downloaded Feb 4 / Feb 6, producing forward-filled
      prices and extreme slowness.
    """
    trading_dates = [date(2025, 2, 3), date(2025, 2, 4), date(2025, 2, 5), date(2025, 2, 6)]
    monkeypatch.setattr(thetadata_helper, "get_trading_dates", lambda asset, start, end: trading_dates)
    monkeypatch.setattr(thetadata_helper, "LUMIBOT_DEFAULT_PYTZ", pytz.timezone("US/Eastern"))

    asset = SimpleNamespace(symbol="NVDA", asset_type="stock")

    # Two ET trading days worth of data (Feb 3 + Feb 5), but with after-hours bars that cross midnight UTC.
    idx = pd.to_datetime(
        [
            # Feb 3 regular session + after-hours spill into Feb 4 UTC
            "2025-02-03 14:30:00+00:00",  # 09:30 ET
            "2025-02-04 00:59:00+00:00",  # 19:59 ET (still Feb 3 market day)
            # Feb 5 regular session + after-hours spill into Feb 6 UTC
            "2025-02-05 14:30:00+00:00",  # 09:30 ET
            "2025-02-06 00:59:00+00:00",  # 19:59 ET (still Feb 5 market day)
        ],
        utc=True,
    )
    df_all = pd.DataFrame({"missing": [0, 0, 0, 0]}, index=idx)

    missing = thetadata_helper.get_missing_dates(
        df_all,
        asset,
        start=datetime(2025, 2, 3, tzinfo=pytz.UTC),
        end=datetime(2025, 2, 7, tzinfo=pytz.UTC),
    )

    assert missing == [date(2025, 2, 4), date(2025, 2, 6)]
