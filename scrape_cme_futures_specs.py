#!/usr/bin/env python3
"""
Scrape CME Group official contract specifications for all TopstepX-allowed futures.

This script:
1. Uses the complete TopstepX symbol list (including micros)
2. Fetches official CME contract specs for each symbol
3. Extracts: contract months, last trading day rules, termination rules
4. Saves structured data to futures_roll_data.json

Data source: CME Group official contract specifications pages
"""

import json
import time
from typing import Dict, Optional

import requests
from bs4 import BeautifulSoup

# Complete TopstepX allowed symbols list (from help.topstep.com)
TOPSTEP_SYMBOLS = {
    # Equities
    "ES": "E-mini S&P 500",
    "MES": "Micro E-mini S&P 500",
    "NKD": "Nikkei 225 Dollar",
    "YM": "E-mini Dow",
    "MYM": "Micro E-mini Dow",
    "NQ": "E-mini NASDAQ 100",
    "MNQ": "Micro E-mini NASDAQ 100",
    "RTY": "E-mini Russell 2000",
    "M2K": "Micro E-mini Russell 2000",
    # Crypto
    "MBT": "Micro Bitcoin",
    "MET": "Micro Ether",
    # Treasuries
    "ZT": "2-Year Note",
    "ZF": "5-Year Note",
    "ZN": "10-Year Note",
    "TN": "Ultra 10-Year Note",
    "ZB": "30-Year Bond",
    "UB": "Ultra Bond",
    # Forex
    "6A": "Australian Dollar",
    "M6A": "Micro Australian Dollar",
    "6B": "British Pound",
    "6C": "Canadian Dollar",
    "6E": "Euro FX",
    "M6E": "Micro Euro",
    "6J": "Japanese Yen",
    "6S": "Swiss Franc",
    "E7": "E-mini Euro FX",
    "6M": "Mexican Peso",
    "6N": "New Zealand Dollar",
    # Energy
    "CL": "Crude Oil",
    "MCL": "Micro Crude Oil",
    "QM": "E-mini Crude Oil",
    "NG": "Natural Gas",
    "QG": "E-mini Natural Gas",
    "RB": "RBOB Gasoline",
    "HO": "Heating Oil",
    # Agriculture
    "HE": "Lean Hogs",
    "LE": "Live Cattle",
    "ZC": "Corn",
    "ZW": "Wheat",
    "ZS": "Soybeans",
    "ZM": "Soybean Meal",
    "ZL": "Soybean Oil",
    # Metals
    "GC": "Gold",
    "MGC": "Micro Gold",
    "SI": "Silver",
    "SIL": "Micro Silver",
    "HG": "Copper",
    "PL": "Platinum",
}


# CME product code mapping (symbol -> CME product page identifier)
# Some symbols don't match exactly between trading symbol and CME URL
CME_PRODUCT_MAP = {
    "ES": "e-mini-sandp500",
    "MES": "micro-e-mini-sandp-500",
    "NKD": "nikkei-225-dollar",
    "YM": "e-mini-dow",
    "MYM": "micro-e-mini-dow",
    "NQ": "e-mini-nasdaq-100",
    "MNQ": "micro-e-mini-nasdaq-100",
    "RTY": "e-mini-russell-2000",
    "M2K": "micro-e-mini-russell-2000",
    "MBT": "micro-bitcoin",
    "MET": "micro-ether",
    "ZT": "2-year-us-treasury-note",
    "ZF": "5-year-us-treasury-note",
    "ZN": "10-year-us-treasury-note",
    "TN": "ultra-10-year-us-treasury-note",
    "ZB": "30-year-us-treasury-bond",
    "UB": "ultra-t-bond",
    "6A": "australian-dollar",
    "M6A": "e-micro-australian-dollar",
    "6B": "british-pound",
    "6C": "canadian-dollar",
    "6E": "euro-fx",
    "M6E": "e-micro-euro",
    "6J": "japanese-yen",
    "6S": "swiss-franc",
    "E7": "e-mini-euro-fx",
    "6M": "mexican-peso",
    "6N": "new-zealand-dollar",
    "CL": "light-sweet-crude",
    "MCL": "micro-wti-crude-oil",
    "QM": "emini-crude-oil",
    "NG": "natural-gas",
    "QG": "emini-natural-gas",
    "RB": "rbob-gasoline",
    "HO": "heating-oil",
    "HE": "lean-hogs",
    "LE": "live-cattle",
    "ZC": "corn",
    "ZW": "wheat",
    "ZS": "soybean",
    "ZM": "soybean-meal",
    "ZL": "soybean-oil",
    "GC": "gold",
    "MGC": "e-micro-gold",
    "SI": "silver",
    "SIL": "1000-oz-silver",
    "HG": "copper",
    "PL": "platinum",
}


def get_cme_contract_specs_url(symbol: str) -> Optional[str]:
    """Construct CME Group contract specifications URL for a given symbol."""
    product_slug = CME_PRODUCT_MAP.get(symbol)
    if not product_slug:
        print(f"⚠️  No CME product mapping for {symbol}")
        return None

    # Determine asset class based on symbol
    if symbol in ["ES", "MES", "NKD", "YM", "MYM", "NQ", "MNQ", "RTY", "M2K"]:
        asset_class = "equity-index"
    elif symbol in ["MBT", "MET"]:
        asset_class = "cryptocurrencies"
    elif symbol in ["ZT", "ZF", "ZN", "TN", "ZB", "UB"]:
        asset_class = "interest-rates"
    elif symbol in ["6A", "M6A", "6B", "6C", "6E", "M6E", "6J", "6S", "E7", "6M", "6N"]:
        asset_class = "fx"
    elif symbol in ["CL", "MCL", "QM", "NG", "QG", "RB", "HO"]:
        asset_class = "energy"
    elif symbol in ["HE", "LE", "ZC", "ZW", "ZS", "ZM", "ZL"]:
        asset_class = "agricultural"
    elif symbol in ["GC", "MGC", "SI", "SIL", "HG", "PL"]:
        asset_class = "metals"
    else:
        return None

    # Construct URL based on asset class structure
    if asset_class == "equity-index":
        return f"https://www.cmegroup.com/markets/equities/sp/{product_slug}.contractSpecs.html"
    elif asset_class == "cryptocurrencies":
        return f"https://www.cmegroup.com/markets/cryptocurrencies/bitcoin/{product_slug}.contractSpecs.html"
    elif asset_class == "interest-rates":
        return f"https://www.cmegroup.com/markets/interest-rates/us-treasury/{product_slug}.contractSpecs.html"
    elif asset_class == "fx":
        return f"https://www.cmegroup.com/markets/fx/g10/{product_slug}.contractSpecs.html"
    elif asset_class == "energy":
        if "crude" in product_slug.lower():
            return f"https://www.cmegroup.com/markets/energy/crude-oil/{product_slug}.contractSpecs.html"
        elif "natural-gas" in product_slug:
            return f"https://www.cmegroup.com/markets/energy/natural-gas/{product_slug}.contractSpecs.html"
        else:
            return f"https://www.cmegroup.com/markets/energy/refined-products/{product_slug}.contractSpecs.html"
    elif asset_class == "agricultural":
        if symbol in ["HE", "LE"]:
            return f"https://www.cmegroup.com/markets/agriculture/livestock/{product_slug}.contractSpecs.html"
        else:
            return f"https://www.cmegroup.com/markets/agriculture/grains/{product_slug}.contractSpecs.html"
    elif asset_class == "metals":
        if symbol in ["GC", "MGC", "SI", "SIL", "PL"]:
            return f"https://www.cmegroup.com/markets/metals/precious/{product_slug}.contractSpecs.html"
        else:
            return f"https://www.cmegroup.com/markets/metals/base/{product_slug}.contractSpecs.html"

    return None


def scrape_cme_contract_specs(symbol: str, url: str) -> Optional[Dict]:
    """
    Scrape CME contract specifications page for rollover information.

    Returns dict with:
    - contract_months: List of months when contracts are listed
    - last_trading_day: Description of last trading day rule
    - termination_of_trading: Exact termination rule
    """
    print(f"🔍 Scraping {symbol}: {url}")

    try:
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        specs = {
            "symbol": symbol,
            "name": TOPSTEP_SYMBOLS.get(symbol, ""),
            "cme_url": url,
            "contract_months": None,
            "last_trading_day": None,
            "termination_of_trading": None,
            "settlement_method": None,
        }

        # Look for contract specs table
        # CME uses various table structures, need to parse flexibly
        tables = soup.find_all("table")

        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    header = cells[0].get_text(strip=True).lower()
                    value = cells[1].get_text(strip=True)

                    if "listed contract" in header or "contract month" in header:
                        specs["contract_months"] = value
                    elif "termination of trading" in header or "last trading day" in header:
                        specs["termination_of_trading"] = value
                        if not specs["last_trading_day"]:
                            specs["last_trading_day"] = value
                    elif "settlement method" in header:
                        specs["settlement_method"] = value

        # Also check for text content outside tables
        if not specs["termination_of_trading"]:
            page_text = soup.get_text()
            if "third friday" in page_text.lower():
                specs["termination_of_trading"] = "Third Friday of contract month"

        return specs

    except requests.RequestException as e:
        print(f"❌ Error fetching {symbol}: {e}")
        return None
    except Exception as e:
        print(f"❌ Error parsing {symbol}: {e}")
        return None


def main():
    """Main scraping workflow."""
    print("=" * 80)
    print("CME Futures Contract Specifications Scraper")
    print("=" * 80)
    print(f"\n📋 Scraping {len(TOPSTEP_SYMBOLS)} futures contracts from CME Group")
    print("   Data source: Official CME contract specifications pages")
    print()

    all_specs = []

    for symbol in sorted(TOPSTEP_SYMBOLS.keys()):
        url = get_cme_contract_specs_url(symbol)

        if not url:
            print(f"⚠️  Skipping {symbol} - no URL mapping")
            continue

        specs = scrape_cme_contract_specs(symbol, url)

        if specs:
            all_specs.append(specs)
            print(f"   ✓ {symbol}: {specs.get('contract_months', 'N/A')}")
        else:
            print(f"   ✗ {symbol}: Failed to scrape")

        # Be polite to CME servers
        time.sleep(1)

    # Save to JSON
    output_file = "futures_roll_data.json"
    with open(output_file, "w") as f:
        json.dump(all_specs, f, indent=2)

    print()
    print("=" * 80)
    print(f"✅ Scraped {len(all_specs)} contracts")
    print(f"💾 Data saved to: {output_file}")
    print("=" * 80)


if __name__ == "__main__":
    main()
