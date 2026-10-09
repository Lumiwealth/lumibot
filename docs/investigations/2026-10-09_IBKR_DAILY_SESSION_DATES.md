# IBKR stock/index daily session dates

Daily history returned by TWS encodes its `YYYYMMDD` session label as UTC midnight. That timestamp is a date label, not an instant at which the daily price became available. Converting it to New York before interpreting the date assigned the candle to the preceding day. The decoder now preserves the UTC calendar date for verified TWS stock/index daily responses and labels the resulting candle at the existing 16:00 New York close. REST and intraday decoding retain their existing timestamp contracts.

## Cache transition

Stock/index daily cache filenames now end in `_SESSION_DATE_V2.parquet`. The old files can contain both shifted TWS prices and correctly dated REST prices, so shifting a whole old file would corrupt valid rows. Updated readers and writers use only the new daily key and rebuild it from provider responses. They do not delete or overwrite the old daily file. Minute, multi-minute, hourly, futures, crypto and option keys are unchanged. An older installed package continues to read its previous keys if rolled back.

Provider timeouts or unavailable sessions remain partial history and preserve real downloaded bars. A new key is not proof of complete coverage; inspect actual requested sessions and history health. This migration does not extend provider retention or market-data subscriptions.

## Verification

The regression uses real TWS date-label shape at summer and winter session dates, checks that prices stay on the provider date, and verifies that only stock/index daily keys change while legacy files survive. Backend selection uses the existing validated downloader metadata reader: malformed metadata cannot crash decoding and metadata from another provider cannot reinterpret dates. Three metadata regressions failed before that correction. All 195 tests in the complete affected IBKR helper, daily-gap, split, futures, crypto and cache-registry unit files pass. Cloud CI and the normal package release gates remain required before publication.

No agent prompt change is needed: this corrects provider parsing and cache integrity without adding a strategy-facing API or changing tool choice. No diagram is needed for the narrow timestamp and filename repair.
