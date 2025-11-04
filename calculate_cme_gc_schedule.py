#!/usr/bin/env python3
"""
Calculate official CME Gold (GC) futures contract expiration and roll schedule.

Based on CME Group official specifications:
- Contract months: Feb, Apr, Jun, Aug, Oct, Dec (bi-monthly)
- Last Trading Day: 3rd last business day of contract month at 12:30 CT
- Roll convention: Typically ~3 business days before last trading day

This script generates the CORRECT schedule that lumibot should be using.
"""

from datetime import datetime, timedelta
from typing import List, Tuple

import pandas as pd


def is_business_day(date: datetime) -> bool:
    """Check if date is a business day (Mon-Fri, excluding holidays)."""
    # Simplified: only checks weekends
    # Production version should include US federal holidays
    return date.weekday() < 5


def get_last_business_day_of_month(year: int, month: int) -> datetime:
    """Get the last business day of a given month."""
    # Start from last day of month
    if month == 12:
        last_day = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = datetime(year, month + 1, 1) - timedelta(days=1)

    # Walk backwards to find last business day
    while not is_business_day(last_day):
        last_day -= timedelta(days=1)

    return last_day


def get_third_last_business_day(year: int, month: int) -> datetime:
    """
    Calculate the 3rd last business day of the month.
    This is the official CME last trading day for GC futures.
    """
    last_bday = get_last_business_day_of_month(year, month)

    # Count backwards 2 more business days
    count = 0
    current = last_bday
    while count < 2:
        current -= timedelta(days=1)
        if is_business_day(current):
            count += 1

    return current


def calculate_roll_date(last_trading_day: datetime, offset_days: int = 3) -> datetime:
    """
    Calculate the roll date - typically N business days before last trading day.

    Args:
        last_trading_day: The contract's last trading day
        offset_days: Number of business days before LTD to roll (default 3)
    """
    count = 0
    current = last_trading_day
    while count < offset_days:
        current -= timedelta(days=1)
        if is_business_day(current):
            count += 1

    return current


def generate_gc_contracts_schedule(year: int) -> List[Tuple[str, str, datetime, datetime, datetime]]:
    """
    Generate GC futures contract schedule for a given year.

    Returns list of tuples: (contract_code, month_name, roll_date, last_trading_day, expiration_date)
    """
    # GC trades bi-monthly: Feb, Apr, Jun, Aug, Oct, Dec
    gc_months = [2, 4, 6, 8, 10, 12]
    month_codes = {2: "G", 4: "J", 6: "M", 8: "Q", 10: "V", 12: "Z"}
    month_names = {2: "Feb", 4: "Apr", 6: "Jun", 8: "Aug", 10: "Oct", 12: "Dec"}

    # Use single-digit year like DataBento
    year_digit = year % 10

    schedule = []
    for month in gc_months:
        # Contract code: GC + month_code + year_digit
        contract_code = f"GC{month_codes[month]}{year_digit}"
        month_name = month_names[month]

        # Calculate official CME last trading day (3rd last business day)
        last_trading_day = get_third_last_business_day(year, month)

        # Calculate roll date (3 business days before LTD)
        roll_date = calculate_roll_date(last_trading_day, offset_days=3)

        # Expiration is last business day of contract month
        expiration_date = get_last_business_day_of_month(year, month)

        schedule.append((contract_code, month_name, roll_date, last_trading_day, expiration_date))

    return schedule


def generate_continuous_roll_schedule(year: int) -> List[Tuple[str, datetime, datetime]]:
    """
    Generate continuous futures roll schedule showing which contract is active when.

    Returns list of tuples: (contract_code, start_date, end_date)
    """
    schedule = generate_gc_contracts_schedule(year)

    # Build continuous schedule
    continuous = []

    # Start year with January
    start_date = datetime(year, 1, 1)

    for i, (contract, _month_name, roll_date, _ltd, _exp) in enumerate(schedule):
        if i == 0:
            # First contract of year - use Jan 1 as start
            continuous.append((contract, start_date, roll_date))
        else:
            # Subsequent contracts - start from previous roll date
            prev_roll = schedule[i - 1][2]
            continuous.append((contract, prev_roll, roll_date))

    # Add period after last roll to year end
    last_contract, _, last_roll, _, _ = schedule[-1]
    continuous.append((last_contract, last_roll, datetime(year, 12, 31)))

    return continuous


def compare_with_lumibot_schedule(year: int):
    """
    Compare CME-compliant schedule with what lumibot currently generates.
    """
    from datetime import datetime

    import pytz

    from lumibot.entities import Asset
    from lumibot.tools import futures_roll

    print("=" * 80)
    print("CME-Compliant GC Schedule vs Lumibot's Current Schedule")
    print("=" * 80)

    # Generate CME-compliant schedule
    cme_schedule = generate_gc_contracts_schedule(year)

    print(f"\n📅 Official CME GC Futures Schedule for {year}:")
    print(f"\n{'Contract':<8} {'Month':<6} {'Roll Date':<12} {'Last Trading Day':<17} {'Expiration':<12}")
    print(f"{'-'*8} {'-'*6} {'-'*12} {'-'*17} {'-'*12}")

    for contract, month, roll_date, ltd, exp in cme_schedule:
        print(
            f"{contract:<8} {month:<6} {roll_date.strftime('%Y-%m-%d'):<12} "
            f"{ltd.strftime('%Y-%m-%d'):<17} {exp.strftime('%Y-%m-%d'):<12}"
        )

    # Get lumibot's current schedule
    gc_asset = Asset(symbol="GC", asset_type=Asset.AssetType.CONT_FUTURE)
    start_date = datetime(year, 1, 1, tzinfo=pytz.UTC)
    end_date = datetime(year, 12, 31, tzinfo=pytz.UTC)

    lumibot_schedule = futures_roll.build_roll_schedule(
        gc_asset,
        start_date,
        end_date,
        year_digits=1,
    )

    print("\n\n🔧 Lumibot's Current Schedule (INCORRECT):")
    print(f"\n{'Contract':<10} {'Start Date':<12} {'End Date':<12} {'Days':<6}")
    print(f"{'-'*10} {'-'*12} {'-'*12} {'-'*6}")

    if lumibot_schedule:
        for contract, start_dt, end_dt in lumibot_schedule:
            days = (end_dt - start_dt).days
            print(f"{contract:<10} {start_dt.strftime('%Y-%m-%d'):<12} " f"{end_dt.strftime('%Y-%m-%d'):<12} {days:<6}")
    else:
        print("   No schedule generated!")

    # Month-by-month comparison
    print("\n\n📊 Month-by-Month Comparison:")
    print(f"\n{'Month':<12} {'CME Correct':<15} {'Lumibot Actual':<15} {'Match':<6}")
    print(f"{'-'*12} {'-'*15} {'-'*15} {'-'*6}")

    cme_by_month = {}
    for contract, month_name, _roll_date, _ltd, _exp in cme_schedule:
        # Map contract to the months it should be active
        # Before roll: previous contract
        # After roll: this contract
        month_num = {"Feb": 2, "Apr": 4, "Jun": 6, "Aug": 8, "Oct": 10, "Dec": 12}[month_name]

        # Determine which months this contract covers
        if month_num == 2:
            cme_by_month[1] = contract  # Jan -> Feb contract

        # Month after previous roll -> this contract
        cme_by_month[month_num] = contract

    # Fill in the gaps using bi-monthly logic
    cme_by_month = {
        1: f"GCG{year%10}",  # Jan -> Feb
        2: f"GCJ{year%10}",  # Feb (after roll) -> Apr
        3: f"GCJ{year%10}",  # Mar -> Apr
        4: f"GCM{year%10}",  # Apr (after roll) -> Jun
        5: f"GCM{year%10}",  # May -> Jun
        6: f"GCQ{year%10}",  # Jun (after roll) -> Aug
        7: f"GCQ{year%10}",  # Jul -> Aug
        8: f"GCV{year%10}",  # Aug (after roll) -> Oct
        9: f"GCV{year%10}",  # Sep -> Oct
        10: f"GCZ{year%10}",  # Oct (after roll) -> Dec
        11: f"GCZ{year%10}",  # Nov -> Dec
        12: f"GCZ{year%10}",  # Dec -> Dec
    }

    month_names_list = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]

    for month_num in range(1, 13):
        ref_date = datetime(year, month_num, 15, tzinfo=pytz.UTC)
        lumibot_contract = futures_roll.resolve_symbol_for_datetime(gc_asset, ref_date, year_digits=1)
        cme_contract = cme_by_month.get(month_num, "?")

        match = "✓" if lumibot_contract == cme_contract else "✗"

        print(f"{month_names_list[month_num-1]:<12} {cme_contract:<15} {lumibot_contract:<15} {match:<6}")


def export_to_csv(year: int, output_path: str = "cme_gc_schedule.csv"):
    """Export CME-compliant schedule to CSV."""
    schedule = generate_gc_contracts_schedule(year)

    df = pd.DataFrame(
        schedule, columns=["contract_code", "month_name", "roll_date", "last_trading_day", "expiration_date"]
    )

    df.to_csv(output_path, index=False)
    print(f"\n💾 CME-compliant schedule exported to: {output_path}")


if __name__ == "__main__":
    import sys

    year = 2025
    if len(sys.argv) > 1:
        year = int(sys.argv[1])

    print("\n" + "=" * 80)
    print("CME Gold (GC) Futures - Official Schedule Calculator")
    print("=" * 80)
    print("\nBased on CME Group official specifications:")
    print("  - Trading months: Feb, Apr, Jun, Aug, Oct, Dec (bi-monthly)")
    print("  - Last trading day: 3rd last business day of contract month")
    print("  - Roll date: ~3 business days before last trading day")
    print(f"\nCalculating schedule for year: {year}")

    # Generate and display schedule
    schedule = generate_gc_contracts_schedule(year)

    print("\n📋 Official CME GC Contract Schedule:")
    print(f"\n{'Contract':<10} {'Month':<6} {'Roll Date':<12} {'Last Trading Day':<17} {'Expiration':<12}")
    print(f"{'-'*10} {'-'*6} {'-'*12} {'-'*17} {'-'*12}")

    for contract, month, roll_date, ltd, exp in schedule:
        print(
            f"{contract:<10} {month:<6} {roll_date.strftime('%Y-%m-%d'):<12} "
            f"{ltd.strftime('%Y-%m-%d'):<17} {exp.strftime('%Y-%m-%d'):<12}"
        )

    # Export to CSV
    export_to_csv(year)

    # Compare with lumibot's current behavior
    print("\n")
    try:
        compare_with_lumibot_schedule(year)
    except Exception as e:
        print(f"\n⚠️  Could not compare with lumibot schedule: {e}")

    print("\n" + "=" * 80)
    print("✅ Analysis Complete")
    print("=" * 80)
