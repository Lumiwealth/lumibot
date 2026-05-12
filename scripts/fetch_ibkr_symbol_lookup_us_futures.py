#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

IBKR_SYMBOL_LOOKUP_ENDPOINT = "https://www.interactivebrokers.com/webrest/search/products-by-filters"


@dataclass(frozen=True)
class ProductKey:
    symbol: str
    exchange: str
    currency: str


def _post_products(*, page_number: int, page_size: int, country: str, domain: str) -> list[dict[str, Any]]:
    headers = {
        "Content-Type": "application/json",
        "Origin": "https://www.interactivebrokers.com",
        "Referer": "https://www.interactivebrokers.com/en/trading/symbol.php",
        "User-Agent": "Mozilla/5.0",
    }
    body = {
        "pageNumber": int(page_number),
        "pageSize": str(int(page_size)),
        "sortField": "symbol",
        "sortDirection": "asc",
        "productCountry": [country],
        "productSymbol": "",
        "newProduct": "all",
        "productType": ["FUT"],
        "domain": domain,
    }
    resp = requests.post(IBKR_SYMBOL_LOOKUP_ENDPOINT, headers=headers, data=json.dumps(body), timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    products = payload.get("products") or []
    if not isinstance(products, list):
        raise RuntimeError(f"Unexpected products payload: {type(products)}")
    return products


def _iter_pages(*, page_size: int, country: str, domain: str, sleep_s: float) -> Iterable[tuple[int, list[dict[str, Any]]]]:
    page = 1
    while True:
        products = _post_products(page_number=page, page_size=page_size, country=country, domain=domain)
        if not products:
            return
        yield page, products
        page += 1
        if sleep_s > 0:
            time.sleep(sleep_s)


def _to_key(product: dict[str, Any]) -> ProductKey | None:
    symbol = str(product.get("symbol") or "").strip().upper()
    exchange = str(product.get("exchangeId") or "").strip().upper()
    currency = str(product.get("currency") or "").strip().upper()
    if not symbol or not exchange or not currency:
        return None
    return ProductKey(symbol=symbol, exchange=exchange, currency=currency)


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch US futures product list from IBKR public Symbol Lookup.")
    parser.add_argument(
        "--out-dir",
        default="data/ibkr_symbol_lookup",
        help="Output directory (relative to repo root).",
    )
    parser.add_argument("--country", default="US", help="Country filter (Symbol Lookup). Default: US.")
    parser.add_argument("--domain", default="hk", help="Domain/entity selector used by IBKR site. Default: hk.")
    parser.add_argument("--page-size", type=int, default=100, help="Page size (site uses 100).")
    parser.add_argument("--sleep-s", type=float, default=0.15, help="Sleep between requests to be polite.")
    args = parser.parse_args()

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_jsonl = out_dir / f"{args.country.lower()}_futures_products_raw.jsonl"
    roots_csv = out_dir / f"{args.country.lower()}_futures_roots.csv"

    seen_roots: set[ProductKey] = set()
    root_rows: list[dict[str, str]] = []

    total_products = 0
    total_pages = 0

    with raw_jsonl.open("w", encoding="utf-8") as raw_handle:
        for page, products in _iter_pages(
            page_size=args.page_size, country=args.country, domain=args.domain, sleep_s=args.sleep_s
        ):
            total_pages = page
            for product in products:
                total_products += 1
                raw_handle.write(json.dumps(product, sort_keys=True) + "\n")

                key = _to_key(product)
                if key is None or key in seen_roots:
                    continue
                seen_roots.add(key)

                row = {
                    "symbol": key.symbol,
                    "exchange": key.exchange,
                    "currency": key.currency,
                    "example_local_symbol": str(product.get("localSymbol") or ""),
                    "example_conid": str(product.get("conid") or ""),
                    "description": str(product.get("description") or ""),
                }
                root_rows.append(row)

    root_rows.sort(key=lambda r: (r["symbol"], r["exchange"], r["currency"]))

    with roots_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "symbol",
                "exchange",
                "currency",
                "example_local_symbol",
                "example_conid",
                "description",
            ],
        )
        writer.writeheader()
        writer.writerows(root_rows)

    summary = {
        "country": args.country,
        "domain": args.domain,
        "pages_fetched": total_pages,
        "raw_products_written": total_products,
        "unique_roots_written": len(seen_roots),
        "raw_jsonl": str(raw_jsonl),
        "roots_csv": str(roots_csv),
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

