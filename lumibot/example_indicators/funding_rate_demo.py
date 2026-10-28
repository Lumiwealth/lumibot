"""Show the last few hours of a perpetual swap's funding rate.

Needs the internet (OKX public API, no key)::

    python -m lumibot.example_indicators.funding_rate_demo ETH-USDT-SWAP
"""

import sys

import pandas as pd

from lumibot.example_indicators.funding_rate import FundingRate


def main(inst_id="BTC-USDT-SWAP", hours=48, indicator_cls=FundingRate):
    # Hourly bars ending now. Prices do not matter here; only bar times do.
    times = pd.date_range(end=pd.Timestamp.now(tz="UTC").floor("h"), periods=hours, freq="1h")
    bars = pd.DataFrame({"open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0}, index=times)
    rates = indicator_cls(inst_id=inst_id).compute(bars)["rate_pct"]
    for when, rate in list(zip(times, rates))[-5:]:
        print(f"{when:%Y-%m-%d %H:%M} UTC  funding {rate:+.4f}%" if rate is not None else f"{when} no data")
    return rates


if __name__ == "__main__":  # pragma: no cover - needs the network
    main(*sys.argv[1:2])
