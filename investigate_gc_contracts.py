#!/usr/bin/env python3
"""
Investigate which GC futures contracts are being selected by lumibot.
This will reveal the root cause of the data quality issues.
"""

from datetime import datetime

import pytz

from lumibot.entities import Asset
from lumibot.tools import futures_roll


def investigate_gc_rollover():
    """Show exactly which GC contracts are selected for the date range."""

    print("=" * 80)
    print("GC Futures Contract Selection Investigation")
    print("=" * 80)

    # Create continuous futures asset for GC
    gc_asset = Asset(symbol="GC", asset_type=Asset.AssetType.CONT_FUTURE)

    # Date range from our data
    start_date = datetime(2025, 1, 1, tzinfo=pytz.UTC)
    end_date = datetime(2025, 10, 31, tzinfo=pytz.UTC)

    print("\n📅 Analyzing Date Range:")
    print(f"   Start: {start_date.strftime('%Y-%m-%d')}")
    print(f"   End:   {end_date.strftime('%Y-%m-%d')}")

    # Get the roll schedule
    print("\n🔍 Lumibot Roll Schedule for GC:")
    print("   (Using futures_roll.build_roll_schedule)")
    print()

    schedule = futures_roll.build_roll_schedule(
        gc_asset,
        start_date,
        end_date,
        year_digits=1,  # DataBento uses single digit years
    )

    if schedule:
        print(f"   Found {len(schedule)} contract periods:\n")
        print(f"   {'Contract':<10} {'Start Date':<12} {'End Date':<12} {'Days':<6} {'Weeks':<6}")
        print(f"   {'-'*10} {'-'*12} {'-'*12} {'-'*6} {'-'*6}")

        for contract, start_dt, end_dt in schedule:
            days = (end_dt - start_dt).days
            weeks = days / 7
            print(
                f"   {contract:<10} {start_dt.strftime('%Y-%m-%d'):<12} "
                f"{end_dt.strftime('%Y-%m-%d'):<12} {days:<6} {weeks:<6.1f}"
            )
    else:
        print("   ⚠️  No schedule generated!")

    # Check if GC has special roll rules
    print("\n📋 GC Roll Rule Configuration:")
    gc_rule = futures_roll.ROLL_RULES.get("GC")

    if gc_rule:
        print("   ✓ GC has custom roll rule:")
        print(f"     - Anchor: {gc_rule.anchor}")
        print(f"     - Offset: {gc_rule.offset_business_days} business days")
    else:
        print("   ✗ GC has NO custom roll rule")
        print("     Falls back to legacy mid-month logic (rolls on 15th)")

    # Show what contracts SHOULD be used for Gold (CME standard)
    print("\n🏦 CME Gold Futures Standard Roll Schedule:")
    print("   Gold (GC) typically trades these months:")
    print("   - Primary: Feb (G), Apr (J), Jun (M), Aug (Q), Oct (V), Dec (Z)")
    print("   - Roll occurs: ~3 business days before last trading day")
    print("   - Last trading day: 3rd to last business day of contract month")

    # Compare: What contracts should we see vs what we're getting
    print("\n⚙️  Expected vs Actual Analysis:")
    print()

    # Expected contracts for 2025 (standard CME GC schedule)
    expected_2025_contracts = [
        ("GCG5", "Feb 2025", "Rolls late Jan"),
        ("GCJ5", "Apr 2025", "Rolls late Mar"),
        ("GCM5", "Jun 2025", "Rolls late May"),
        ("GCQ5", "Aug 2025", "Rolls late Jul"),
        ("GCV5", "Oct 2025", "Rolls late Sep"),
        ("GCZ5", "Dec 2025", "Rolls late Nov"),
    ]

    print("   Expected CME contracts for Jan-Oct 2025:")
    for contract, month, roll_info in expected_2025_contracts:
        print(f"      {contract:<6} - {month:<10} ({roll_info})")

    # Get what lumibot is actually selecting
    print("\n   Lumibot is selecting:")
    if schedule:
        for contract, start_dt, end_dt in schedule:
            duration = (end_dt - start_dt).days
            print(
                f"      {contract:<6} - {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')} "
                f"({duration} days)"
            )

    # Month-by-month analysis
    print("\n📆 Month-by-Month Contract Resolution:")
    print(f"   {'Month':<12} {'Lumibot Selects':<15} {'Should Use':<15}")
    print(f"   {'-'*12} {'-'*15} {'-'*15}")

    expected_by_month = {
        1: "GCG5",  # Jan -> Feb contract
        2: "GCJ5",  # Feb -> Apr contract (after roll ~late Feb)
        3: "GCJ5",  # Mar -> Apr contract
        4: "GCM5",  # Apr -> Jun contract (after roll ~late Apr)
        5: "GCM5",  # May -> Jun contract
        6: "GCQ5",  # Jun -> Aug contract (after roll ~late Jun)
        7: "GCQ5",  # Jul -> Aug contract
        8: "GCV5",  # Aug -> Oct contract (after roll ~late Aug)
        9: "GCV5",  # Sep -> Oct contract
        10: "GCZ5",  # Oct -> Dec contract (after roll ~late Oct)
    }

    for month in range(1, 11):
        # Mid-month reference date
        ref_date = datetime(2025, month, 15, tzinfo=pytz.UTC)
        lumibot_contract = futures_roll.resolve_symbol_for_datetime(gc_asset, ref_date, year_digits=1)
        expected_contract = expected_by_month.get(month, "?")

        match = "✓" if lumibot_contract == expected_contract else "✗"
        month_name = ref_date.strftime("%B")

        print(f"   {month_name:<12} {lumibot_contract:<15} {expected_contract:<15} {match}")

    print("\n" + "=" * 80)
    print("🔎 ROOT CAUSE ANALYSIS")
    print("=" * 80)

    if not gc_rule:
        print("\n❌ PROBLEM IDENTIFIED:")
        print("\n   Lumibot's futures_roll.py does NOT have GC-specific roll rules.")
        print("   ")
        print("   Current roll rules are defined for:")
        for symbol in futures_roll.ROLL_RULES.keys():
            print(f"      - {symbol}")
        print("\n   GC is NOT in this list, so it falls back to 'legacy_mid_month' logic,")
        print("   which simply rolls on the 15th of each month.")
        print("\n   However, CME Gold futures (GC) trade on a bi-monthly cycle:")
        print("      Feb, Apr, Jun, Aug, Oct, Dec")
        print("\n   The mid-month logic is selecting contracts that either:")
        print("      1. Don't exist (odd months have no GC contracts)")
        print("      2. Have very low volume (wrong contract month)")
        print("\n💡 SOLUTION:")
        print("   Add GC-specific roll rules to futures_roll.py ROLL_RULES dict,")
        print("   or use CME's standard rollover schedule for gold.")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    investigate_gc_rollover()
