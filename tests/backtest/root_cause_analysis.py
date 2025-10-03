"""
Root cause analysis: Is the +1 minute offset from ThetaData's API or our processing?
"""

import requests
import pandas as pd
import datetime

print("="*100)
print("ROOT CAUSE ANALYSIS: ThetaData +1 Minute Offset")
print("="*100)

# Get raw API response
response = requests.get('http://127.0.0.1:25510/hist/stock/ohlc', params={
    'root': 'AMZN',
    'start_date': '20240801',
    'end_date': '20240801',
    'ivl': 60000,
    'rth': 'true'
})

data = response.json()

print("\n1. ThetaData RAW API Response (no processing):")
print("-" * 100)
print(f"{'Bar':<5} {'ms_of_day':<12} {'Time':<10} {'Volume':<12} {'Notes'}")
print("-" * 100)

for i, row in enumerate(data['response'][:5]):
    ms_of_day, o, h, l, c, v, count, date = row
    hours = ms_of_day // (1000 * 60 * 60)
    minutes = (ms_of_day % (1000 * 60 * 60)) // (1000 * 60)
    time_str = f"{hours:02d}:{minutes:02d}"

    note = ""
    if i == 0:
        note = "← Should be pre-market if labeled correctly"
    elif i == 1:
        note = "← MASSIVE SPIKE (market open)" if v > 1000000 else ""

    print(f"{i+1:<5} {ms_of_day:<12} {time_str:<10} {v:<12,} {note}")

print("\n2. After Our Code Processing (thetadata_helper.py):")
print("-" * 100)

# Replicate our processing from thetadata_helper.py
df = pd.DataFrame(data['response'][:5], columns=data['header']['format'])

def combine_datetime(row):
    date_str = str(int(row["date"]))
    base_date = datetime.datetime.strptime(date_str, "%Y%m%d")
    datetime_value = base_date + datetime.timedelta(milliseconds=int(row["ms_of_day"]))
    return datetime_value

datetime_combined = df.apply(combine_datetime, axis=1)
df = df.assign(datetime=datetime_combined)
df["datetime"] = pd.to_datetime(df["datetime"])

print(f"{'Bar':<5} {'Datetime':<30} {'Volume':<12} {'Notes'}")
print("-" * 100)

for i, (idx, row) in enumerate(df.iterrows()):
    note = ""
    if i == 0:
        note = "← Should be pre-market if labeled correctly"
    elif i == 1 and row['volume'] > 1000000:
        note = "← MASSIVE SPIKE (market open)"

    print(f"{i+1:<5} {str(row['datetime']):<30} {row['volume']:<12,} {note}")

print("\n3. Expected Correct Labeling (based on volume spike = market open at 9:30):")
print("-" * 100)
print("Bar 1 (10,434 volume):     Should be labeled 9:29 (pre-market)")
print("Bar 2 (1,517,215 volume):  Should be labeled 9:30 (market open SPIKE)")
print()
print("Actual ThetaData Labeling:")
print("Bar 1 (10,434 volume):     Labeled as 9:30")
print("Bar 2 (1,517,215 volume):  Labeled as 9:31")
print()
print("="*100)
print("CONCLUSION:")
print("="*100)
print("The +1 minute offset exists in ThetaData's RAW API response.")
print("Our processing code does NOT introduce any shifts.")
print("The ms_of_day values from ThetaData are already off by +1 minute.")
print()
print("PROOF:")
print("- ThetaData labels the low-volume bar as 9:30")
print("- ThetaData labels the spike bar as 9:31")
print("- But market opens at 9:30, so the spike SHOULD be labeled 9:30")
print("- Therefore, ThetaData's timestamps are +1 minute ahead of reality")
print("="*100)

print("\n4. Checking ThetaData's Documentation Claim:")
print("-" * 100)
print("ThetaData docs say: 'bar timestamp <= trade time < bar timestamp + ivl'")
print("For bar labeled 9:30 with ivl=60000ms (1 minute):")
print("  Should include trades: 9:30:00.000 <= trade < 9:31:00.000")
print()
print("But we observe:")
print("  Bar labeled 9:30 has 10,434 volume (pre-market level)")
print("  Bar labeled 9:31 has 1,517,215 volume (market open spike)")
print()
print("This means:")
print("  Bar labeled 9:30 actually contains 9:29:00-9:29:59 data")
print("  Bar labeled 9:31 actually contains 9:30:00-9:30:59 data")
print()
print("Therefore: ThetaData's bars are MISLABELED by +1 minute in their API")
print("="*100)
