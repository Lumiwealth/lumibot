#!/usr/bin/env python3
"""
Debug script to see what Selenium is actually finding on CME pages.
"""

import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By


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


def debug_page(driver, symbol, url):
    """Debug what we find on a CME page."""
    print(f"\n{'='*80}")
    print(f"Debugging {symbol}: {url}")
    print(f"{'='*80}\n")

    driver.get(url)
    time.sleep(3)

    # Find all tables
    rows = driver.find_elements(By.TAG_NAME, "tr")
    print(f"Found {len(rows)} table rows total\n")

    # Show ALL rows and their cell structure
    for i, row in enumerate(rows[:30]):  # Limit to first 30 rows
        try:
            cells = row.find_elements(By.TAG_NAME, "td")
            if cells:
                print(f"Row {i+1} - {len(cells)} cells:")
                for j, cell in enumerate(cells):
                    text = cell.text.strip()
                    if text:  # Only show non-empty cells
                        print(f"  Cell {j+1}: {text[:100]}")
                print()
        except Exception as e:
            print(f"Row {i+1} - Error: {e}\n")

    # Also check for divs with specific contract spec indicators
    print("\n" + "=" * 80)
    print("Searching page text for key phrases:")
    print("=" * 80 + "\n")

    page_text = driver.find_element(By.TAG_NAME, "body").text
    lines = page_text.split("\n")

    for i, line in enumerate(lines[:100]):  # Check first 100 lines
        line_lower = line.lower()
        if any(keyword in line_lower for keyword in ["listed contract", "termination of trading", "settlement method"]):
            print(f"Line {i+1}: {line[:150]}")

    print(f"\n(Checked first 100 lines of {len(lines)} total)")


def main():
    """Main debug workflow."""
    driver = setup_driver()

    try:
        # Test a few representative symbols
        test_cases = [
            ("GC", "https://www.cmegroup.com/markets/metals/precious/gold.contractSpecs.html"),
            ("ES", "https://www.cmegroup.com/markets/equities/sp/e-mini-sandp500.contractSpecs.html"),
            ("MES", "https://www.cmegroup.com/markets/equities/sp/micro-e-mini-sandp-500.contractSpecs.html"),
        ]

        for symbol, url in test_cases:
            debug_page(driver, symbol, url)

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
