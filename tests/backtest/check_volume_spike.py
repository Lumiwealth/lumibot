"""
Check where the volume spike happens to determine which provider is correct.
Market opens at 9:30 AM, so we should see a massive volume spike AT 9:30 AM.
"""

import requests
import datetime
import pandas as pd
from polygon import RESTClient
from lumibot.credentials import POLYGON_API_KEY


def check_volume_pattern(symbol, date_str):
    """Check volume patterns around market open."""

    print(f"\n{'='*100}")
    print(f"{symbol} - Volume Pattern Analysis")
    print(f"{'='*100}")

    # Get ThetaData
    response = requests.get('http://127.0.0.1:25510/hist/stock/ohlc', params={
        'root': symbol,
        'start_date': date_str,
        'end_date': date_str,
        'ivl': 60000,
        'rth': 'true'
    })

    data = response.json()
    theta_bars = []
    for row in data['response'][:15]:  # First 15 bars
        ms_of_day, o, h, l, c, v, count, date = row
        hours = ms_of_day // (1000 * 60 * 60)
        minutes = (ms_of_day % (1000 * 60 * 60)) // (1000 * 60)
        theta_bars.append({
            'time': f"{hours:02d}:{minutes:02d}",
            'volume': v
        })

    # Get Polygon
    client = RESTClient(POLYGON_API_KEY)
    date = datetime.date(2024, 8, 1)
    aggs = client.get_aggs(ticker=symbol, multiplier=1, timespan="minute", from_=date, to=date, limit=50000)

    polygon_bars = []
    for agg in aggs:
        dt = datetime.datetime.fromtimestamp(agg.timestamp/1000, tz=datetime.timezone.utc)
        dt_et = dt.astimezone(datetime.timezone(datetime.timedelta(hours=-4)))

        # Only first 15 bars after 9:25
        if dt_et.hour == 9 and dt_et.minute >= 25 and len(polygon_bars) < 15:
            polygon_bars.append({
                'time': dt_et.strftime("%H:%M"),
                'volume': agg.volume
            })
        elif dt_et.hour > 9 and len(polygon_bars) < 15:
            polygon_bars.append({
                'time': dt_et.strftime("%H:%M"),
                'volume': agg.volume
            })

    print(f"\nThetaData Bars (first 15):")
    print(f"{'Time':<10} {'Volume':>15} {'Notes':<30}")
    print("-" * 60)
    max_theta_vol = max(b['volume'] for b in theta_bars)
    for bar in theta_bars:
        note = "← SPIKE!" if bar['volume'] == max_theta_vol else ""
        print(f"{bar['time']:<10} {bar['volume']:>15,} {note:<30}")

    print(f"\nPolygon Bars (first 15):")
    print(f"{'Time':<10} {'Volume':>15} {'Notes':<30}")
    print("-" * 60)
    max_polygon_vol = max(b['volume'] for b in polygon_bars)
    for bar in polygon_bars:
        note = "← SPIKE!" if bar['volume'] == max_polygon_vol else ""
        print(f"{bar['time']:<10} {bar['volume']:>15,} {note:<30}")

    # Analysis
    theta_spike_time = next(b['time'] for b in theta_bars if b['volume'] == max_theta_vol)
    polygon_spike_time = next(b['time'] for b in polygon_bars if b['volume'] == max_polygon_vol)

    print(f"\n{'='*100}")
    print(f"ANALYSIS")
    print(f"{'='*100}")
    print(f"ThetaData: Volume spike at {theta_spike_time}")
    print(f"Polygon:   Volume spike at {polygon_spike_time}")
    print(f"\nMarket officially opens at 9:30 AM ET")

    if polygon_spike_time == "09:30":
        print(f"✓ Polygon shows spike at 9:30 AM (CORRECT - matches market open)")
    else:
        print(f"✗ Polygon shows spike at {polygon_spike_time} (WRONG - doesn't match market open)")

    if theta_spike_time == "09:30":
        print(f"✓ ThetaData shows spike at 9:30 AM (CORRECT - matches market open)")
    elif theta_spike_time == "09:31":
        print(f"✗ ThetaData shows spike at 9:31 AM (WRONG - should be at 9:30)")
        print(f"  → This suggests ThetaData timestamps are OFF BY +1 MINUTE")
    else:
        print(f"✗ ThetaData shows spike at {theta_spike_time} (UNEXPECTED)")


def main():
    symbols = ["AMZN", "AAPL", "SPY"]
    date_str = "20240801"

    for symbol in symbols:
        check_volume_pattern(symbol, date_str)


if __name__ == "__main__":
    main()
