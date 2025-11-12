#!/usr/bin/env python3
"""Quick test of Selenium-based CME scraping on a few symbols."""

import json

from scrape_cme_futures_selenium import (
    scrape_cme_contract_specs_selenium,
    setup_driver,
)
from scrape_cme_futures_specs import TOPSTEP_SYMBOLS, get_cme_contract_specs_url

# Test on representative symbols
TEST_SYMBOLS = ["GC", "ES", "MES"]

print("Testing Selenium CME scraper on sample symbols...")
print("=" * 80)

driver = setup_driver()
results = []

try:
    for symbol in TEST_SYMBOLS:
        url = get_cme_contract_specs_url(symbol)
        if url:
            print(f"\n{symbol} ({TOPSTEP_SYMBOLS[symbol]}):")
            specs = scrape_cme_contract_specs_selenium(driver, symbol, url)

            if specs:
                results.append(specs)
                print(f"   Listed Contracts: {specs.get('listed_contracts', 'N/A')}")
                print(f"   Termination: {specs.get('termination_of_trading', 'N/A')}")
                print(f"   Settlement: {specs.get('settlement_method', 'N/A')}")
            else:
                print("   Failed to scrape")
finally:
    driver.quit()

# Save test results
with open("test_futures_data.json", "w") as f:
    json.dump(results, f, indent=2)

print("\n" + "=" * 80)
print(f"✅ Test complete - {len(results)} symbols scraped")
print("💾 Saved to: test_futures_data.json")
