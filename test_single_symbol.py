#!/usr/bin/env python3
"""
Test scraping a single symbol with debug output.
"""

import re
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By


def is_trading_rule(text: str) -> bool:
    """Check if text looks like a trading rule explanation."""
    text_lower = text.lower()
    # If it starts with contract spec phrases, it's NOT a trading rule (even if long)
    if text_lower.startswith(("monthly contracts", "quarterly contracts", "trading terminates")):
        return False
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
    if len(text) > 200 and not any(
        phrase in text_lower for phrase in ["contract", "month", "terminates", "business day"]
    ):
        return True
    return False


def is_valid_listed_contracts(text: str) -> bool:
    """Check if text looks like valid listed contracts."""
    if not text or len(text) < 10:
        return False
    text_lower = text.lower()
    if is_trading_rule(text):
        return False
    if text_lower.startswith(("monthly contracts", "quarterly contracts")):
        return True
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
    has_time_reference = bool(re.search(r"\d+\s*(month|year|quarter)", text_lower))
    return has_month_reference or has_time_reference


def is_valid_termination(text: str) -> bool:
    """Check if text looks like valid termination rule."""
    if not text or len(text) < 10:
        return False
    text_lower = text.lower()
    if is_trading_rule(text):
        return False
    if text_lower.startswith("trading terminates"):
        return True
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
    """Set up Chrome driver."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
    driver = webdriver.Chrome(options=chrome_options)
    return driver


def test_scrape(symbol, url):
    """Test scraping with debug output."""
    print(f"\n{'='*80}")
    print(f"Testing {symbol}: {url}")
    print(f"{'='*80}\n")

    driver = setup_driver()
    try:
        driver.get(url)
        time.sleep(3)

        cells = driver.find_elements(By.TAG_NAME, "td")
        cell_texts = [cell.text.strip() for cell in cells if cell.text.strip()]

        print(f"Found {len(cell_texts)} non-empty table cells\n")
        print("Processing each cell:\n")

        for i, cell_text in enumerate(cell_texts):
            # Skip headers and short text
            if cell_text.upper() in ["LISTED CONTRACTS", "TERMINATION OF TRADING", "SETTLEMENT METHOD"]:
                print(f"Cell {i+1}: [HEADER] {cell_text}")
                continue
            if len(cell_text) < 5:
                print(f"Cell {i+1}: [TOO SHORT] {cell_text[:60]}")
                continue

            # Check validations
            is_rule = is_trading_rule(cell_text)
            is_contracts = is_valid_listed_contracts(cell_text)
            is_term = is_valid_termination(cell_text)
            is_settlement = cell_text in ["Deliverable", "Financially Settled", "Cash Settled"]

            status = []
            if is_rule:
                status.append("TRADING_RULE")
            if is_contracts:
                status.append("VALID_CONTRACTS")
            if is_term:
                status.append("VALID_TERMINATION")
            if is_settlement:
                status.append("SETTLEMENT")

            if not status:
                status.append("IGNORED")

            print(f"Cell {i+1}: [{', '.join(status)}]")
            print(f"         {cell_text[:100]}")
            print()

    finally:
        driver.quit()


def main():
    """Main test."""
    test_cases = [
        ("ES", "https://www.cmegroup.com/markets/equities/sp/e-mini-sandp500.contractSpecs.html"),
        ("MES", "https://www.cmegroup.com/markets/equities/sp/micro-e-mini-sandp-500.contractSpecs.html"),
    ]

    for symbol, url in test_cases:
        test_scrape(symbol, url)


if __name__ == "__main__":
    main()
