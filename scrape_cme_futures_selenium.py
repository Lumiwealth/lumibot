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
import re
import time
from typing import Dict, Optional

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# Import symbol list and URL builder from original scraper
from scrape_cme_futures_specs import TOPSTEP_SYMBOLS, get_cme_contract_specs_url


def is_trading_rule(text: str) -> bool:
    """
    Check if text looks like a trading rule explanation (TAS/TAM/TMAC) rather than a contract spec.

    Returns True if this appears to be a trading rule, not a contract specification.
    """
    text_lower = text.lower()

    # If it starts with contract spec phrases, it's NOT a trading rule (even if long)
    if text_lower.startswith(("monthly contracts", "quarterly contracts", "trading terminates")):
        return False

    # Direct mentions of trading mechanisms that indicate a rule explanation
    trading_rule_phrases = [
        "tas is",
        "tam is",
        "tmac is",
        "tas trades",
        "tam trades",
        "analogous to",
        "differential to a not-yet-known price",
        "base price",
        "clearing price equals",
        "rule 524",
        "subject to the requirements of rule",
    ]

    if any(phrase in text_lower for phrase in trading_rule_phrases):
        return True

    # Very long explanatory text (>200 chars) without contract indicators is likely a rule
    if len(text) > 200 and not any(
        phrase in text_lower for phrase in ["contract", "month", "terminates", "business day"]
    ):
        return True

    return False


def is_valid_listed_contracts(text: str) -> bool:
    """
    Check if text looks like a valid "Listed Contracts" specification.

    Should mention months, years, or specific contract patterns.
    """
    if not text or len(text) < 10:
        return False

    text_lower = text.lower()

    # Should NOT be a trading rule
    if is_trading_rule(text):
        return False

    # Strong indicators it's a listed contracts spec
    if text_lower.startswith(("monthly contracts", "quarterly contracts")):
        return True

    # Should mention months or contract cycles
    month_indicators = [
        "monthly",
        "quarterly",
        "consecutive months",
        "consecutive quarters",
        "jan",
        "feb",
        "mar",
        "apr",
        "may",
        "jun",
        "jul",
        "aug",
        "sep",
        "oct",
        "nov",
        "dec",
        "march",
        "june",
        "september",
        "december",
        "nearest",
        "contract month",
        "listed for",
    ]

    has_month_reference = any(indicator in text_lower for indicator in month_indicators)

    # Should have year references or "months"/"quarters" count
    has_time_reference = bool(re.search(r"\d+\s*(month|year|quarter)", text_lower))

    return has_month_reference or has_time_reference


def is_valid_termination(text: str) -> bool:
    """
    Check if text looks like a valid termination/last trading day rule.
    """
    if not text or len(text) < 10:
        return False

    text_lower = text.lower()

    # Should NOT be a trading rule
    if is_trading_rule(text):
        return False

    # Strong indicator it's a termination rule
    if text_lower.startswith("trading terminates"):
        return True

    # Should mention trading termination or last day
    termination_indicators = [
        "trading terminates",
        "trading ceases",
        "last trading day",
        "final settlement",
        "business day of the",
        "3rd friday",
        "third friday",
        "friday of",
        "a.m. et",
        "a.m. ct",
        "p.m. et",
        "p.m. ct",
        "prior to",
        "last business day",
    ]

    return any(indicator in text_lower for indicator in termination_indicators)


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

    CME pages use a single-column table structure where:
    - Headers like "LISTED CONTRACTS" appear as text
    - Values appear in subsequent single-cell table rows

    Returns dict with:
    - contract_months: Listed contract months
    - last_trading_day: Last trading day rule
    - termination_of_trading: Termination rule
    - settlement_method: Physical or cash settled
    """
    print(f"🔍 Scraping {symbol}: {url}")

    try:
        driver.get(url)

        # Wait for page to load - CME uses JavaScript-rendered pages
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

        try:
            # Get all table cells (single-column structure on CME pages)
            cells = driver.find_elements(By.TAG_NAME, "td")
            cell_texts = [cell.text.strip() for cell in cells if cell.text.strip()]

            # Match cells to headers by finding the value that appears after each header
            for cell_text in cell_texts:
                # Skip if it's a header itself or too short
                if cell_text.upper() in ["LISTED CONTRACTS", "TERMINATION OF TRADING", "SETTLEMENT METHOD"]:
                    continue
                if len(cell_text) < 5:
                    continue

                # Skip trading rules
                if is_trading_rule(cell_text):
                    continue

                # Check if this looks like a listed contracts value
                if is_valid_listed_contracts(cell_text) and not specs["listed_contracts"]:
                    specs["listed_contracts"] = cell_text
                    specs["contract_months"] = cell_text

                # Check if this looks like a termination rule
                elif is_valid_termination(cell_text) and not specs["termination_of_trading"]:
                    specs["termination_of_trading"] = cell_text
                    if not specs["last_trading_day"]:
                        specs["last_trading_day"] = cell_text

                # Check for settlement method (short text)
                elif (
                    cell_text in ["Deliverable", "Financially Settled", "Cash Settled"]
                    and not specs["settlement_method"]
                ):
                    specs["settlement_method"] = cell_text

        except Exception as e:
            print(f"   ⚠️  Error parsing page: {e}")

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
