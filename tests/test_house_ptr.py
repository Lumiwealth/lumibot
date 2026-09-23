"""Official House PTR parser proofs.

These tests download the public Clerk PDFs. They are the dry-run contract:
Pelosi rows from 20033725, 20034836, and 20035143, option rows carry strike
and expiration, and the private LLC is absent.
"""

from lumibot.components.house_ptr import (
    download_house_pdf,
    parse_house_ptr_text,
    pdf_bytes_to_text,
    reflow_ptr_text,
    tradeable_rows,
)
def _rows(doc_id: str):
    pdf = download_house_pdf(2026, doc_id)
    return parse_house_ptr_text(pdf_bytes_to_text(pdf))


def test_pelosi_filings_parse_stocks_options_and_skip_the_private_llc():
    first = _rows("20033725")
    second = _rows("20034836")
    third = _rows("20035143")
    combined = first + second + third
    rendered_names = " ".join(
        f"{row.get('Ticker') or ''} {row.get('asset_name') or ''} {row.get('description') or ''}"
        for row in combined
        if not row.get("skipped_reason")
    )

    assert "REOF" not in rendered_names
    assert "LLC" not in rendered_names
    assert any(row.get("skipped_reason") == "private_llc" for row in third)
    assert any(row.get("Ticker") == "VSNT" and row.get("skipped_reason") == "spinoff" for row in first)
    assert any(row.get("skipped_reason") == "gift" for row in first)

    options = tradeable_rows(combined, asset_mode="option")
    assert any(
        row["Ticker"] == "GOOGL" and row["strike"] == 150 and row["expiration"] == "2027-01-15" and row["option_type"] == "call"
        for row in options
    )
    assert any(row["Ticker"] == "INTC" and row["expiration"] == "2027-03-19" and row["strike"] == 50 for row in options)
    assert any(row["Ticker"] == "BE" and row["strike"] == 100 and row["expiration"] == "2027-06-17" for row in options)
    assert all(row["option_type"] in {"call", "put"} and row["strike"] and row["expiration"] for row in options)

    stocks = tradeable_rows(first, asset_mode="stock")
    assert any(row["Ticker"] == "GOOGL" and row["side"] == "buy" and row["TransactionDate"] == "2026-01-16" for row in stocks)
    assert all(row["ReportDate"] == "2026-01-23" for row in stocks)


def test_reflow_ptr_text_puts_each_transaction_on_one_line():
    wrapped = """
Filing ID #20033725
SP NVIDIA Corporation - Common Stock
(NVDA) [ST]
S (partial) 12/24/2025 12/24/2025 $1,000,001 -
$5,000,000
D: Sold 20,000 shares.
SP AllianceBernstein Holding L.P. Units
(AB) [AB]
P 01/16/2026 01/16/2026 $1,000,001 -
$5,000,000
Digitally Signed: Hon. Nancy Pelosi , 01/23/2026
"""
    lines = reflow_ptr_text(wrapped).splitlines()
    assert len(lines) == 2
    assert "(NVDA) [ST]" in lines[0] and "S (partial)" in lines[0] and "$5,000,000" in lines[0]
    assert "(AB) [AB]" in lines[1] and " P " in f" {lines[1]} "
    assert reflow_ptr_text("not a filing") == "not a filing"
