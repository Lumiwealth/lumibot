#!/usr/bin/env python3
"""
Scrape CME Group contract specifications using Selenium for JavaScript-rendered pages.

This script:
1. Uses TopstepX symbol list (60+ contracts including micros)
2. Fetches official CME contract specs using Selenium/Chrome
3. Waits for JavaScript to render, then extracts specs
4. Saves structured data to futures_roll_data.json

Data source: CME Group official contract specifications pages (JavaScript-rendered)
"""

import json
import time
from typing import Dict, Optional

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# Import symbol list and URL builder from original scraper
from scrape_cme_futures_specs import TOPSTEP_SYMBOLS, get_cme_contract_specs_url


def setup_driver():
    """Set up Chrome driver with headless options."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")

    driver = webdriver.Chrome(options=chrome_options)
    return driver


def scrape_cme_contract_specs_selenium(driver, symbol: str, url: str) -> Optional[Dict]:
    """
    Scrape CME contract specifications using Selenium for JS rendering.

    Returns dict with:
    - contract_months: Listed contract months
    - last_trading_day: Last trading day rule
    - termination_of_trading: Termination rule
    - settlement_method: Physical or cash settled
    """
    print(f"🔍 Scraping {symbol}: {url}")

    try:
        driver.get(url)

        # Wait for page to load - CME uses various div/table structures
        # Give time for JavaScript to render content
        time.sleep(3)

        specs = {
            "symbol": symbol,
            "name": TOPSTEP_SYMBOLS.get(symbol, ""),
            "cme_url": url,
            "contract_months": None,
            "listed_contracts": None,
            "last_trading_day": None,
            "termination_of_trading": None,
            "settlement_method": None,
        }

        # Try to find contract specs table
        # CME typically has tables or divs with contract information
        try:
            # Look for all table rows
            rows = driver.find_elements(By.TAG_NAME, "tr")

            for row in rows:
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 2:
                        header = cells[0].text.strip().lower()
                        value = cells[1].text.strip()

                        if "listed contract" in header or "contract month" in header:
                            specs["listed_contracts"] = value
                            specs["contract_months"] = value
                        elif "termination of trading" in header:
                            specs["termination_of_trading"] = value
                            if not specs["last_trading_day"]:
                                specs["last_trading_day"] = value
                        elif "last trading day" in header:
                            specs["last_trading_day"] = value
                            if not specs["termination_of_trading"]:
                                specs["termination_of_trading"] = value
                        elif "settlement method" in header:
                            specs["settlement_method"] = value
                except Exception:
                    continue

            # If nothing found in tables, try looking for divs or spans
            if not specs["listed_contracts"]:
                # Try alternative selectors
                page_text = driver.find_element(By.TAG_NAME, "body").text

                # Look for common patterns
                if "third friday" in page_text.lower():
                    if not specs["termination_of_trading"]:
                        specs["termination_of_trading"] = "Third Friday of contract month"

                # Try to extract contract months from text
                for line in page_text.split("\n"):
                    line_lower = line.lower().strip()
                    if "listed contract" in line_lower or "contract month" in line_lower:
                        # Next line might have the value
                        if line_lower != line.strip():
                            specs["listed_contracts"] = line.strip()
                            specs["contract_months"] = line.strip()

        except Exception as e:
            print(f"   ⚠️  Error parsing tables: {e}")

        return specs

    except Exception as e:
        print(f"   ❌ Error scraping {symbol}: {e}")
        return None


def main():
    """Main scraping workflow with Selenium."""
    print("=" * 80)
    print("CME Futures Contract Specifications Scraper (Selenium)")
    print("=" * 80)
    print(f"\n📋 Scraping {len(TOPSTEP_SYMBOLS)} futures contracts from CME Group")
    print("   Using: Selenium WebDriver (Chrome headless)")
    print("   Data source: Official CME contract specifications pages")
    print()

    # Set up Selenium driver
    print("🌐 Starting Chrome WebDriver...")
    driver = setup_driver()

    all_specs = []

    try:
        for symbol in sorted(TOPSTEP_SYMBOLS.keys()):
            url = get_cme_contract_specs_url(symbol)

            if not url:
                print(f"⚠️  Skipping {symbol} - no URL mapping")
                continue

            specs = scrape_cme_contract_specs_selenium(driver, symbol, url)

            if specs:
                all_specs.append(specs)

                # Show what we found
                months = specs.get("listed_contracts") or specs.get("contract_months") or "N/A"
                term = specs.get("termination_of_trading") or "N/A"

                # Truncate for display
                if len(months) > 60:
                    months = months[:57] + "..."
                if len(term) > 60:
                    term = term[:57] + "..."

                print(f"   ✓ {symbol}: {months}")
                print(f"      Termination: {term}")
            else:
                print(f"   ✗ {symbol}: Failed to scrape")

            # Small delay between requests
            time.sleep(2)

    finally:
        # Clean up
        print("\n🛑 Closing browser...")
        driver.quit()

    # Save to JSON
    output_file = "futures_roll_data.json"
    with open(output_file, "w") as f:
        json.dump(all_specs, f, indent=2)

    print()
    print("=" * 80)
    print(f"✅ Scraped {len(all_specs)} contracts")
    print(f"💾 Data saved to: {output_file}")
    print("=" * 80)

    # Show summary of what we found
    found_months = sum(1 for s in all_specs if s.get("listed_contracts") or s.get("contract_months"))
    found_termination = sum(1 for s in all_specs if s.get("termination_of_trading"))

    print("\n📊 Summary:")
    print(f"   Contract months found: {found_months}/{len(all_specs)}")
    print(f"   Termination rules found: {found_termination}/{len(all_specs)}")


if __name__ == "__main__":
    main()
