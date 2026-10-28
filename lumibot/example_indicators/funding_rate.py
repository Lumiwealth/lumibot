"""Perpetual futures funding rate from a public exchange API, as an indicator.

This shows how an indicator can use outside data. It downloads the funding
rate history of a perpetual swap from OKX's public REST API (no API key) and,
for each bar, returns the latest funding rate that was already settled at the
bar's time. It never uses a funding rate from after the bar, so the same code
is safe in a backtest and live.

The download is cached on the indicator object (not in ``state``): it is
outside data, not something computed from bars, so forming-bar copies do not
need to copy it. To use another exchange or a recorded file, pass a different
``loader`` (any function returning ``[(epoch_ms, rate), ...]``).
"""

from __future__ import annotations

import bisect
import time

import requests

from lumibot.indicators import Indicator, InputSpec, Level, OutputSpec, register

OKX_FUNDING_HISTORY_URL = "https://www.okx.com/api/v5/public/funding-rate-history"


def fetch_okx_funding_history(inst_id: str, pages: int = 3, session=None) -> list:
    """Funding history for ``inst_id`` (for example ``BTC-USDT-SWAP``), oldest
    first, as ``[(funding_time_ms, rate), ...]``. Each page holds up to 100
    funding events (about 33 days at 8-hour funding)."""
    http = session or requests
    rows, cursor = [], None
    for _ in range(pages):
        params = {"instId": inst_id, "limit": 100}
        if cursor is not None:
            params["after"] = cursor  # OKX: return records older than this time
        response = http.get(OKX_FUNDING_HISTORY_URL, params=params, timeout=10)
        response.raise_for_status()
        payload = response.json()
        if str(payload.get("code")) != "0":
            raise RuntimeError(f"OKX funding history error: {payload.get('msg') or payload.get('code')}")
        page = payload.get("data") or []
        if not page:
            break
        for item in page:
            rate = item.get("realizedRate") or item.get("fundingRate")
            if rate not in (None, ""):
                rows.append((int(item["fundingTime"]), float(rate)))
        cursor = page[-1]["fundingTime"]
    return sorted(set(rows))


@register
class FundingRate(Indicator):
    key = "funding_rate"
    version = 1
    title = "Funding Rate (OKX)"
    short_title = "FUND"
    overlay = False

    inputs = [
        InputSpec("inst_id", "string", "BTC-USDT-SWAP", label="Perpetual swap"),
        InputSpec("pages", "int", 3, min=1, max=50, label="History pages"),
    ]
    outputs = [OutputSpec("rate_pct", "Funding %", "histogram", "#F97316")]
    levels = [Level(0, "Zero", "solid")]

    #: Where funding history comes from. Replace it (in a subclass or with
    #: ``indicator.loader = fn``) for another exchange or for tests.
    loader = staticmethod(fetch_okx_funding_history)
    #: Live charts refresh the download at most this often (seconds).
    refresh_seconds = 300

    def init_state(self):
        self._times, self._rates, self._fetched_at = [], [], None
        return {}

    def _ensure_history(self, bar_ms: int) -> None:
        stale = self._fetched_at is None or (
            (not self._times or bar_ms > self._times[-1] + 8 * 3600 * 1000)
            and time.monotonic() - self._fetched_at >= self.refresh_seconds
        )
        if stale:
            rows = self.loader(self.inputs["inst_id"], self.inputs["pages"])
            self._times = [t for t, _ in rows]
            self._rates = [r for _, r in rows]
            self._fetched_at = time.monotonic()

    def step(self, state, bar):
        if bar.time is None:
            return {"rate_pct": None}
        bar_ms = bar.time.value // 1_000_000
        self._ensure_history(bar_ms)
        i = bisect.bisect_right(self._times, bar_ms) - 1  # latest funding at or before the bar
        return {"rate_pct": None if i < 0 else 100.0 * self._rates[i]}
