#!/usr/bin/env python3
"""Quick test of CME scraping on a few symbols."""

from scrape_cme_futures_specs import (
    TOPSTEP_SYMBOLS,
    get_cme_contract_specs_url,
    scrape_cme_contract_specs,
)

# Test on just a few representative symbols
TEST_SYMBOLS = ["GC", "ES", "MES", "MGC", "CL", "ZC"]

print("Testing CME scraper on sample symbols...")
print("=" * 80)

for symbol in TEST_SYMBOLS:
    url = get_cme_contract_specs_url(symbol)
    if url:
        print(f"\n{symbol} ({TOPSTEP_SYMBOLS[symbol]}):")
        print(f"URL: {url}")

        specs = scrape_cme_contract_specs(symbol, url)
        if specs:
            print(f"   Contract Months: {specs.get('contract_months', 'N/A')}")
            print(f"   Termination: {specs.get('termination_of_trading', 'N/A')}")
            print(f"   Settlement: {specs.get('settlement_method', 'N/A')}")
        else:
            print("   Failed to scrape")

print("\n" + "=" * 80)
