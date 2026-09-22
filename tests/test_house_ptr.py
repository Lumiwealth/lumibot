"""Official House PTR parser proofs.

These tests download the public Clerk PDFs. They are the dry-run contract:
Pelosi rows from 20033725, 20034836, and 20035143, option rows carry strike
and expiration, and the private LLC is absent.
"""

from lumibot.components.house_ptr import (
    download_house_pdf,
    parse_house_ptr_text,
    pdf_bytes_to_text,
    tradeable_rows,
)
from lumibot.example_strategies.ai_congress_disclosures import dry_run_pelosi


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


def test_dry_run_prints_pelosi_rows_without_the_private_llc(capsys):
    text = dry_run_pelosi()
    captured = capsys.readouterr().out
    assert "20033725" in text
    assert "strike 150.0 exp 2027-01-15" in captured
    assert "LLC" not in captured
    assert "REOF" not in captured
