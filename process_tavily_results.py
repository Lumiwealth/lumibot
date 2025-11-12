#!/usr/bin/env python3
"""
Process Tavily extract results and generate futures_roll_data.json.

Since Tavily extract returns mostly promotional content for CME pages,
this script extracts what we can and documents coverage gaps.
"""

import json

from scrape_cme_futures_specs import TOPSTEP_SYMBOLS, get_cme_contract_specs_url


def parse_gold_specs():
    """
    Manually extract GC (Gold) specs from known good Tavily result.
    """
    return {
        "symbol": "GC",
        "name": "Gold",
        "cme_url": "https://www.cmegroup.com/markets/metals/precious/gold.contractSpecs.html",
        "listed_contracts": (
            "Monthly contracts listed for 26 consecutive months and any Jun and Dec in the nearest 72 months."
        ),
        "contract_months": (
            "Monthly contracts listed for 26 consecutive months and any Jun and Dec in the nearest 72 months."
        ),
        "termination_of_trading": (
            "Trading terminates at 12:30 p.m. CT on the third last business day of the contract month."
        ),
        "last_trading_day": (
            "Trading terminates at 12:30 p.m. CT on the third last business day of the contract month."
        ),
        "settlement_method": "Deliverable",
    }


def parse_micro_gold_specs():
    """
    Manually extract MGC (Micro Gold) specs from known good Tavily result.
    """
    return {
        "symbol": "MGC",
        "name": "Micro Gold",
        "cme_url": "https://www.cmegroup.com/markets/metals/precious/e-micro-gold.contractSpecs.html",
        "listed_contracts": (
            "Monthly contracts listed for any Feb, Apr, Jun, Aug, Oct, and Dec " "in the nearest 24 months"
        ),
        "contract_months": (
            "Monthly contracts listed for any Feb, Apr, Jun, Aug, Oct, and Dec " "in the nearest 24 months"
        ),
        "termination_of_trading": "Trading terminates on the third last business day of the contract month.",
        "last_trading_day": "Trading terminates on the third last business day of the contract month.",
        "settlement_method": "Deliverable",
    }


def generate_placeholder_specs(symbol: str) -> dict:
    """Generate placeholder spec for symbols where we couldn't extract data."""
    return {
        "symbol": symbol,
        "name": TOPSTEP_SYMBOLS.get(symbol, ""),
        "cme_url": get_cme_contract_specs_url(symbol),
        "listed_contracts": None,
        "contract_months": None,
        "termination_of_trading": None,
        "last_trading_day": None,
        "settlement_method": None,
        "note": ("Data extraction failed - Tavily returned promotional content instead of contract specs"),
    }


def main():
    """Generate futures_roll_data.json with available data."""
    print("=" * 80)
    print("Generating futures_roll_data.json from Tavily extract results")
    print("=" * 80)
    print()

    all_specs = []

    # Add the two symbols we successfully extracted
    print("✅ Successfully extracted:")
    gc_specs = parse_gold_specs()
    all_specs.append(gc_specs)
    print(f"   GC (Gold): {gc_specs['contract_months'][:60]}...")

    mgc_specs = parse_micro_gold_specs()
    all_specs.append(mgc_specs)
    print(f"   MGC (Micro Gold): {mgc_specs['contract_months'][:60]}...")

    print()
    print("⚠️  Failed extractions (generating placeholders):")

    # Add placeholders for remaining symbols
    for symbol in sorted(TOPSTEP_SYMBOLS.keys()):
        if symbol not in ["GC", "MGC"]:
            spec = generate_placeholder_specs(symbol)
            all_specs.append(spec)
            print(f"   {symbol} ({TOPSTEP_SYMBOLS[symbol]})")

    # Sort by symbol for consistency
    all_specs.sort(key=lambda x: x["symbol"])

    # Save to JSON
    output_file = "futures_roll_data.json"
    with open(output_file, "w") as f:
        json.dump(all_specs, f, indent=2)

    print()
    print("=" * 80)
    print("📊 Summary:")
    print(f"   Total symbols: {len(all_specs)}")
    print("   Successfully extracted: 2 (GC, MGC)")
    print(f"   Failed/placeholder: {len(all_specs) - 2}")
    print("   Coverage: 3.3% (2/60)")
    print()
    print(f"💾 Saved to: {output_file}")
    print("=" * 80)
    print()
    print("📝 Next Steps:")
    print("   The Tavily extract approach did not work well for CME pages.")
    print("   Most pages returned promotional content instead of contract specs.")
    print()
    print("   Recommended alternatives:")
    print("   1. Use Selenium with better selectors (already implemented in scrape_cme_futures_selenium.py)")
    print("   2. Try direct API access if CME provides one")
    print("   3. Manual data entry for critical symbols")
    print("   4. Contact CME for bulk data access")


if __name__ == "__main__":
    main()
