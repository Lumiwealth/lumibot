"""Point-in-time correctness and speed contracts for ``SECFundamentals``.

Every test here was written against the old implementation first and failed there:

* statement calls re-read and re-parsed the companyfacts file on every call;
* a restatement filed later (for example a 10-K/A for an older year) replaced the
  current statement, because the newest *filing* won instead of the newest *period*;
* a value filed after the close was visible at that morning's open, because a bare
  ``filed`` date was read as midnight of the filing day;
* the cache lived only on local disk, so every new backtest machine downloaded
  every company again;
* there was no single call to warm a scanner's whole universe.

All data is synthetic. Nothing here talks to the SEC.
"""

import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from lumibot.fundamentals import SECFundamentals
from lumibot.tools import backtest_cache as backtest_cache_module
from lumibot.tools.backtest_cache import BacktestCacheManager, BacktestCacheSettings, CacheMode

NY = ZoneInfo("America/New_York")
CIK = "0000111111"


@pytest.fixture(autouse=True)
def _no_shared_remote_cache(monkeypatch):
    """Keep SEC tests off any remote cache configured in the developer's environment.

    SECFundamentals uses LumiBot's shared backtest cache by default in backtest mode, so a
    configured bucket would otherwise receive these synthetic fixtures.
    """
    from lumibot.tools import backtest_cache

    disabled = backtest_cache.BacktestCacheManager(None)
    monkeypatch.setattr(backtest_cache, "get_backtest_cache", lambda: disabled)


class _Response:
    def __init__(self, *, payload=None, text="", status_code=200, headers=None):
        self._payload = payload
        self.text = text
        self.status_code = status_code
        self.headers = dict(headers or {})

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def _row(val, *, filed, accn, end, start=None, form="10-K", fy=None, fp="FY"):
    row = {"val": val, "filed": filed, "accn": accn, "end": end, "form": form, "fp": fp, "fy": fy}
    if start:
        row["start"] = start
    return row


def _companyfacts(us_gaap):
    return {"cik": int(CIK), "entityName": "Example Corp", "facts": {"us-gaap": us_gaap}}


def _tickers(*symbols):
    return {str(i): {"ticker": s, "cik_str": 111111 + i, "title": f"{s} Corp"} for i, s in enumerate(symbols)}


class _FakeSEC:
    """Records every request; serves companyfacts/submissions per CIK."""

    def __init__(self, companyfacts=None, submissions=None, tickers=None, delay=0.0):
        self.companyfacts = companyfacts or {}
        self.submissions = submissions or {}
        self.tickers = tickers or _tickers("EXM")
        self.delay = delay
        self.calls = []
        self.lock = threading.Lock()
        self.active = 0
        self.max_active = 0
        self.started = []

    def __call__(self, url, **kwargs):
        with self.lock:
            self.calls.append(url)
            self.started.append(time.monotonic())
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            if self.delay:
                time.sleep(self.delay)
            if url.endswith("company_tickers.json"):
                return _Response(payload=self.tickers)
            for cik, payload in self.companyfacts.items():
                if url.endswith(f"companyfacts/CIK{cik}.json"):
                    return _Response(payload=payload)
            for cik, payload in self.submissions.items():
                if url.endswith(f"submissions/CIK{cik}.json"):
                    return _Response(payload=payload)
            return _Response(status_code=404, payload={})
        finally:
            with self.lock:
                self.active -= 1

    def count(self, fragment):
        return sum(fragment in url for url in self.calls)


def _sec(tmp_path, fake, monkeypatch, **kwargs):
    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake)
    kwargs.setdefault("min_request_interval_seconds", 0)
    kwargs.setdefault("remote_cache", None)
    return SECFundamentals(cache_dir=tmp_path, **kwargs)


# ---------------------------------------------------------------------------
# 1. Parse once: statement calls are lookups, not file re-reads
# ---------------------------------------------------------------------------


def _multi_year_facts():
    revenue, income, assets, equity, ocf = [], [], [], [], []
    for year in range(2015, 2025):
        filed = f"{year + 1}-02-10"
        accn = f"10k-{year}"
        start, end = f"{year}-01-01", f"{year}-12-31"
        revenue.append(_row(1000 + year, filed=filed, accn=accn, start=start, end=end, fy=year))
        income.append(_row(100 + year, filed=filed, accn=accn, start=start, end=end, fy=year))
        ocf.append(_row(50 + year, filed=filed, accn=accn, start=start, end=end, fy=year))
        assets.append(_row(5000 + year, filed=filed, accn=accn, end=end, fy=year))
        equity.append(_row(2000 + year, filed=filed, accn=accn, end=end, fy=year))
    return _companyfacts(
        {
            "Revenues": {"units": {"USD": revenue}},
            "NetIncomeLoss": {"units": {"USD": income}},
            "NetCashProvidedByUsedInOperatingActivities": {"units": {"USD": ocf}},
            "Assets": {"units": {"USD": assets}},
            "StockholdersEquity": {"units": {"USD": equity}},
        }
    )


def test_statement_calls_read_companyfacts_once_per_process(monkeypatch, tmp_path):
    fake = _FakeSEC(companyfacts={CIK: _multi_year_facts()})
    sec = _sec(tmp_path, fake, monkeypatch, cache_mode="backtest")

    reads = []
    original = SECFundamentals._get_json

    def spy(self, url, cache_path, **kwargs):
        reads.append(url)
        return original(self, url, cache_path, **kwargs)

    monkeypatch.setattr(SECFundamentals, "_get_json", spy)

    seen = []
    for month in range(1, 13):
        as_of = datetime(2022, month, 15, 10, 0, tzinfo=NY)
        income = sec.get_income_statement("EXM", as_of=as_of)
        balance = sec.get_balance_sheet("EXM", as_of=as_of)
        cash = sec.get_cash_flow("EXM", as_of=as_of)
        seen.append((income["values"]["revenue"]["value"], balance["values"]["assets"]["value"],
                     cash["values"]["operating_cash_flow"]["value"]))

    # Point-in-time values are still right: FY2021 is public from Feb 11, 2022.
    assert seen[0] == (1000 + 2020, 5000 + 2020, 50 + 2020)
    assert seen[2] == (1000 + 2021, 5000 + 2021, 50 + 2021)
    # 36 statement calls, but the companyfacts file was read once and the ticker map once.
    assert sum("companyfacts" in url for url in reads) == 1
    assert sum("company_tickers" in url for url in reads) <= 1
    assert fake.count("companyfacts") == 1


# ---------------------------------------------------------------------------
# 2. Restatements never replace what was known on the date (issue #1141)
# ---------------------------------------------------------------------------


def _restated_facts():
    return _companyfacts(
        {
            "Revenues": {
                "units": {
                    "USD": [
                        # FY2019 as first reported, then FY2020 10-K (with FY2019 comparative).
                        _row(70, filed="2020-02-05", accn="10k-2019", start="2019-01-01", end="2019-12-31", fy=2019),
                        _row(100, filed="2021-02-03", accn="10k-2020", start="2020-01-01", end="2020-12-31", fy=2020),
                        _row(70, filed="2021-02-03", accn="10k-2020", start="2019-01-01", end="2019-12-31", fy=2020),
                        # A 10-K/A filed later that restates the OLDER year only.
                        _row(75, filed="2021-06-01", accn="10ka-2019", start="2019-01-01", end="2019-12-31",
                             form="10-K/A", fy=2019),
                        # FY2021 10-K restates FY2020 (for example after a split or a correction).
                        _row(120, filed="2022-02-03", accn="10k-2021", start="2021-01-01", end="2021-12-31", fy=2021),
                        _row(90, filed="2022-02-03", accn="10k-2021", start="2020-01-01", end="2020-12-31", fy=2021),
                    ]
                }
            },
            "NetIncomeLoss": {
                "units": {
                    "USD": [
                        _row(10, filed="2021-02-03", accn="10k-2020", start="2020-01-01", end="2020-12-31", fy=2020),
                        _row(7, filed="2021-02-03", accn="10k-2020", start="2019-01-01", end="2019-12-31", fy=2020),
                    ]
                }
            },
        }
    )


def test_later_amendment_of_an_older_year_does_not_replace_the_current_statement(monkeypatch, tmp_path):
    fake = _FakeSEC(companyfacts={CIK: _restated_facts()})
    sec = _sec(tmp_path, fake, monkeypatch)

    statement = sec.get_income_statement("EXM", as_of=datetime(2021, 7, 1, 10, tzinfo=NY))

    assert statement["values"]["revenue"]["value"] == 100
    assert statement["values"]["revenue"]["end"] == "2020-12-31"
    assert statement["values"]["revenue"]["accession_number"] == "10k-2020"
    assert statement["values"]["net_income"]["value"] == 10


def test_compact_company_facts_report_latest_period_not_latest_amendment(monkeypatch, tmp_path):
    fake = _FakeSEC(companyfacts={CIK: _restated_facts()})
    sec = _sec(tmp_path, fake, monkeypatch)

    compact = sec.get_company_facts("EXM", as_of=datetime(2021, 7, 1, 10, tzinfo=NY))

    assert compact["facts"]["Revenues"]["value"] == 100
    assert compact["facts"]["Revenues"]["end"] == "2020-12-31"


def test_fact_history_returns_values_as_known_on_each_date(monkeypatch, tmp_path):
    fake = _FakeSEC(companyfacts={CIK: _restated_facts()})
    sec = _sec(tmp_path, fake, monkeypatch)

    def by_end(history):
        return {row["end"]: row["value"] for row in history["values"]}

    mid_2021 = sec.get_fact_history("EXM", "Revenues", as_of=datetime(2021, 7, 1, 10, tzinfo=NY))
    mid_2022 = sec.get_fact_history("EXM", "Revenues", as_of=datetime(2022, 7, 1, 10, tzinfo=NY))

    # What was public in mid 2021: FY2020 as first reported, FY2019 after the 10-K/A.
    assert by_end(mid_2021) == {"2019-12-31": 75, "2020-12-31": 100}
    # The FY2020 restatement only appears once its filing is public.
    assert by_end(mid_2022) == {"2019-12-31": 75, "2020-12-31": 90, "2021-12-31": 120}
    assert [row["end"] for row in mid_2022["values"]] == ["2019-12-31", "2020-12-31", "2021-12-31"]


# ---------------------------------------------------------------------------
# 3. A filing made after the close is not visible until the next session
# ---------------------------------------------------------------------------


def _timing_facts():
    return _companyfacts(
        {
            "Revenues": {
                "units": {
                    "USD": [
                        _row(1, filed="2023-02-01", accn="10k-2022", start="2022-01-01", end="2022-12-31", fy=2022),
                        # Filed after the close on Feb 1, 2024 (acceptance known).
                        _row(2, filed="2024-02-01", accn="after-close", start="2023-01-01", end="2023-12-31", fy=2023),
                        # Filed on Aug 1, 2024 with no acceptance time in the submissions file.
                        _row(3, filed="2024-08-01", accn="no-acceptance", start="2024-01-01", end="2024-06-30",
                             form="10-Q", fp="Q2", fy=2024),
                        # Accepted before the open on Nov 1, 2024.
                        _row(4, filed="2024-11-01", accn="pre-open", start="2024-01-01", end="2024-09-30",
                             form="10-Q", fp="Q3", fy=2024),
                    ]
                }
            }
        }
    )


def _timing_submissions():
    return {
        "cik": CIK,
        "filings": {
            "recent": {
                "accessionNumber": ["after-close", "pre-open"],
                "form": ["10-K", "10-Q"],
                "filingDate": ["2024-02-01", "2024-11-01"],
                # SEC acceptance times are UTC: 22:05Z is 5:05 PM New York, 11:00Z is 7:00 AM.
                "acceptanceDateTime": ["2024-02-01T22:05:00.000Z", "2024-11-01T11:00:00.000Z"],
            }
        },
    }


@pytest.mark.parametrize(
    "as_of, expected",
    [
        (datetime(2024, 2, 1, 10, 0, tzinfo=NY), 1),   # morning of the filing day: not public yet
        (datetime(2024, 2, 1, 17, 30, tzinfo=NY), 1),  # after hours: next session only
        (datetime(2024, 2, 2, 9, 30, tzinfo=NY), 2),   # next session open
        (datetime(2024, 8, 1, 15, 0, tzinfo=NY), 2),   # no acceptance time: not on the filing day
        (datetime(2024, 8, 2, 9, 30, tzinfo=NY), 3),   # ...but from the day after
        (datetime(2024, 11, 1, 9, 30, tzinfo=NY), 4),  # accepted 7:00 AM: visible at that open
    ],
)
def test_after_close_and_undated_filings_wait_for_the_next_session(monkeypatch, tmp_path, as_of, expected):
    fake = _FakeSEC(companyfacts={CIK: _timing_facts()}, submissions={CIK: _timing_submissions()})
    sec = _sec(tmp_path, fake, monkeypatch)

    statement = sec.get_income_statement("EXM", as_of=as_of)

    assert statement["values"]["revenue"]["value"] == expected


def test_acceptance_times_are_only_fetched_when_a_filing_is_close_to_the_date(monkeypatch, tmp_path):
    fake = _FakeSEC(companyfacts={CIK: _timing_facts()}, submissions={CIK: _timing_submissions()})
    sec = _sec(tmp_path, fake, monkeypatch)

    sec.get_income_statement("EXM", as_of=datetime(2024, 5, 15, 10, tzinfo=NY))
    assert fake.count("submissions") == 0

    sec.get_income_statement("EXM", as_of=datetime(2024, 2, 1, 17, 30, tzinfo=NY))
    assert fake.count("submissions") == 1


# ---------------------------------------------------------------------------
# 4. Shared cache: a second machine gets zero SEC requests
# ---------------------------------------------------------------------------


class _FakeS3:
    def __init__(self):
        self.objects = {}
        self.gets = []
        self.puts = []

    def get_object(self, Bucket, Key):
        self.gets.append(Key)
        if Key not in self.objects:
            raise FileNotFoundError(Key)

        class _Body:
            def __init__(self, data):
                self._data = data

            def read(self, size=-1):
                data, self._data = (self._data, b"") if size < 0 else (self._data[:size], self._data[size:])
                return data

            def close(self):
                pass

        return {"Body": _Body(self.objects[Key]), "ETag": '"x"'}

    def upload_file(self, filename, bucket, key):
        self.puts.append(key)
        self.objects[key] = Path(filename).read_bytes()


def _remote(fake_s3, cache_root, monkeypatch):
    monkeypatch.setattr(backtest_cache_module, "LUMIBOT_CACHE_FOLDER", str(cache_root))
    settings = BacktestCacheSettings(backend="s3", mode=CacheMode.S3_READWRITE, bucket="bucket",
                                     prefix="shared", version="v1")
    return BacktestCacheManager(settings, client_factory=lambda _settings: fake_s3)


class _BacktestStrategy:
    is_backtesting = True

    def __init__(self, now, end):
        self._now = now
        self._backtesting_end = end

    def get_datetime(self):
        return self._now


def test_second_machine_reuses_the_shared_cache_with_zero_sec_requests(monkeypatch, tmp_path):
    fake_s3 = _FakeS3()
    strategy = _BacktestStrategy(datetime(2022, 6, 1, 10, tzinfo=NY), datetime(2022, 12, 31, tzinfo=NY))

    first_fake = _FakeSEC(companyfacts={CIK: _multi_year_facts()})
    first = _sec(tmp_path / "machine_a", first_fake, monkeypatch, strategy=strategy,
                 remote_cache=_remote(fake_s3, tmp_path / "root_a", monkeypatch))
    first_value = first.get_income_statement("EXM")["values"]["revenue"]["value"]
    assert first_fake.count("companyfacts") == 1

    # Keys are provider-style: never a user, account or strategy.
    assert "shared/v1/sec/companyfacts/CIK0000111111.json" in fake_s3.objects
    assert "shared/v1/sec/company_tickers.json" in fake_s3.objects

    second_fake = _FakeSEC(companyfacts={CIK: _multi_year_facts()})
    second = _sec(tmp_path / "machine_b", second_fake, monkeypatch, strategy=strategy,
                  remote_cache=_remote(fake_s3, tmp_path / "root_b", monkeypatch))
    second_value = second.get_income_statement("EXM")["values"]["revenue"]["value"]

    assert second_value == first_value == 1000 + 2021
    assert second_fake.calls == []


def test_shared_file_fetched_before_backtest_end_is_refreshed_at_most_daily(monkeypatch, tmp_path):
    fake_s3 = _FakeS3()
    old_fetch = "2022-03-01T00:00:00+00:00"
    payload = dict(_multi_year_facts())
    payload["_lumibot_cache_meta"] = {"fetched_at": old_fetch}
    fake_s3.objects["shared/v1/sec/companyfacts/CIK0000111111.json"] = json.dumps(payload).encode()
    fake_s3.objects["shared/v1/sec/company_tickers.json"] = json.dumps(
        {**_tickers("EXM"), "_lumibot_cache_meta": {"fetched_at": datetime.now(timezone.utc).isoformat()}}
    ).encode()

    # Backtest ends after the shared copy was fetched: the copy could be missing filings.
    strategy = _BacktestStrategy(datetime(2022, 6, 1, 10, tzinfo=NY), datetime(2022, 12, 31, tzinfo=NY))
    fake = _FakeSEC(companyfacts={CIK: _multi_year_facts()})
    sec = _sec(tmp_path / "m", fake, monkeypatch, strategy=strategy,
               remote_cache=_remote(fake_s3, tmp_path / "root", monkeypatch))
    sec.get_income_statement("EXM")
    assert fake.count("companyfacts") == 1  # refreshed from the SEC once

    # A later machine the same day reuses the refreshed copy.
    later_fake = _FakeSEC(companyfacts={CIK: _multi_year_facts()})
    later = _sec(tmp_path / "m2", later_fake, monkeypatch, strategy=strategy,
                 remote_cache=_remote(fake_s3, tmp_path / "root2", monkeypatch))
    later.get_income_statement("EXM")
    assert later_fake.count("companyfacts") == 0

    # A backtest that ended before the shared copy was fetched reuses it as is.
    early = _BacktestStrategy(datetime(2021, 6, 1, 10, tzinfo=NY), datetime(2021, 12, 31, tzinfo=NY))
    fake_s3.objects["shared/v1/sec/companyfacts/CIK0000111111.json"] = json.dumps(payload).encode()
    early_fake = _FakeSEC(companyfacts={CIK: _multi_year_facts()})
    early_sec = _sec(tmp_path / "m3", early_fake, monkeypatch, strategy=early,
                     remote_cache=_remote(fake_s3, tmp_path / "root3", monkeypatch))
    early_sec.get_income_statement("EXM")
    assert early_fake.count("companyfacts") == 0


# ---------------------------------------------------------------------------
# 5. preload(): one call warms a whole universe, in parallel, within the rate limit
# ---------------------------------------------------------------------------


def test_preload_warms_a_universe_in_parallel_and_later_calls_are_free(monkeypatch, tmp_path):
    symbols = [f"S{i:02d}" for i in range(8)]
    tickers = {str(i): {"ticker": s, "cik_str": 500 + i, "title": s} for i, s in enumerate(symbols)}
    tickers["99"] = {"ticker": "NOFACTS", "cik_str": 999, "title": "No facts"}
    facts = {f"{500 + i:010d}": _multi_year_facts() for i in range(len(symbols))}
    fake = _FakeSEC(companyfacts=facts, tickers=tickers, delay=0.05)
    sec = _sec(tmp_path, fake, monkeypatch, cache_mode="backtest", min_request_interval_seconds=0.02)

    summary = sec.preload([*symbols, "NOFACTS", "UNKNOWNTICKER"], max_workers=4)

    assert sorted(summary["loaded"]) == symbols
    assert set(summary["missing"]) == {"NOFACTS", "UNKNOWNTICKER"}
    assert fake.count("companyfacts") == len(symbols) + 1
    assert fake.max_active > 1  # fetched in parallel
    # Never faster than the limit: n request starts span at least (n - 1) intervals
    # (small allowance for thread wake-up jitter in when the fake records a start).
    starts = sorted(fake.started)
    assert starts[-1] - starts[0] >= (len(starts) - 1) * 0.02 - 0.015

    before = len(fake.calls)
    for symbol in symbols:
        assert sec.get_income_statement(symbol, as_of="2022-06-01")["values"]["revenue"]["value"] == 1000 + 2021
    assert len(fake.calls) == before


def test_default_request_spacing_stays_under_ten_per_second():
    assert SECFundamentals(cache_dir=None, remote_cache=None).min_request_interval_seconds >= 0.1
