# SEC Fundamentals

Lumibot now has native SEC fundamentals through `self.fundamentals`.

Core methods:

```python
self.fundamentals.get_income_statement("AAPL")
self.fundamentals.get_balance_sheet("AAPL")
self.fundamentals.get_cash_flow("AAPL")
self.fundamentals.get_company_facts("AAPL")
self.fundamentals.get_filings("AAPL", form="10-K")
self.fundamentals.search_filing("AAPL", accession_number="...", query="risk")
self.fundamentals.get_filing_document("AAPL", accession_number="...")
```

`get_company_facts()` returns a compact latest-facts view by default, capped at important fields so agent runs do not flood the model context. Pass `max_facts=None` for the full compact fact set, or `raw=True` for the raw SEC companyfacts payload when you explicitly need it.

Agent tools use the same names without `self.fundamentals.`:

- `get_income_statement`
- `get_balance_sheet`
- `get_cash_flow`
- `get_company_facts`
- `get_filings`
- `search_filing`
- `get_filing_document`

Backtest safety:

- Facts are gated by SEC `filed` date.
- Filings are gated by SEC acceptance datetime when available, with filing date as fallback.
- The default `as_of` is the current strategy datetime.
- Responses are cached under `~/.lumibot/cache/sec` unless `LUMIBOT_SEC_CACHE_DIR` is set.

SEC does not require an API key. Lumibot sends a contact-style user agent by default and supports override with `LUMIBOT_SEC_USER_AGENT`.
