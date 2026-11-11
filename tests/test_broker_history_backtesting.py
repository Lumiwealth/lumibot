"""Backtests on the user's own broker history (Schwab, Tradier): one generic source, loud limits.

The broker clients are the real ones (``lumiwealth_tradier``, ``schwab-py``); only the HTTP
session is replaced. Response bodies have the shapes those clients parse:

* Tradier ``GET /v1/markets/history``: ``{"history": {"day": [{"date", "open", "high", "low",
  "close", "volume"}]}}`` (``lumiwealth_tradier.market.get_historical_quotes``), and
  ``{"history": null}`` when there are no bars.
* Schwab ``GET /marketdata/v1/pricehistory``: ``{"candles": [{"open", "high", "low", "close",
  "volume", "datetime"}], "symbol", "empty"}`` with epoch milliseconds
  (``lumibot.data_sources.schwab_data`` parses the same fields).

The bars are synthetic numbers used to check slicing and limits; they are not market data.
"""

from datetime import date, datetime

import pandas as pd
import pytest

from lumibot.backtesting.broker_history_backtesting import (
    BrokerHistoryBacktesting,
    SchwabHistoryBacktesting,
    TradierHistoryBacktesting,
)
from lumibot.entities import Asset
from lumibot.tools import broker_history
from lumibot.tools.broker_history import BrokerHistoryRateLimited, BrokerHistoryUnavailable

NY = "America/New_York"
DAYS = [d for d in pd.date_range("2024-05-20", "2024-06-28", freq="1D")
        if d.weekday() < 5 and d != pd.Timestamp("2024-05-27")]  # Memorial Day
CLOSES = {d.date(): 100.0 + i for i, d in enumerate(DAYS)}


class _Response:
    def __init__(self, body, status=200):
        self._body, self.status_code = body, status
        self.text = str(body)
        self.headers = {}

    def json(self):
        return self._body


# --------------------------------------------------------------------------- Tradier


class _TradierSession:
    def __init__(self, fail_quota=0, no_history=False):
        self.calls = []
        self.fail_quota = fail_quota
        self.no_history = no_history

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        if self.fail_quota:
            self.fail_quota -= 1
            return _Response({"fault": {"faultstring": "Quota Violation"}}, status=400)
        if self.no_history:
            return _Response({"history": None})
        start, end = date.fromisoformat(params["start"]), date.fromisoformat(params["end"])
        rows = [{"date": d.isoformat(), "open": c - 0.5, "high": c + 1, "low": c - 1, "close": c, "volume": 1000}
                for d, c in CLOSES.items() if start <= d <= end]
        return _Response({"history": {"day": rows if len(rows) != 1 else rows[0]}})


def _tradier_source(monkeypatch, session, start=datetime(2024, 6, 3), end=datetime(2024, 6, 28)):
    from lumiwealth_tradier import Tradier

    client = Tradier("VA000000", "standin-token", True)
    monkeypatch.setattr(type(client.market), "requests_retry_session", lambda self, *a, **k: session)
    monkeypatch.setattr("lumiwealth_tradier.base.sleep", lambda s: None, raising=False)
    adapter = broker_history.TradierHistory(client, scope="tradier-standin", sleep=lambda s: None)
    return TradierHistoryBacktesting(pd.Timestamp(start, tz=NY).to_pydatetime(),
                                     pd.Timestamp(end, tz=NY).to_pydatetime(), adapter=adapter)


def _at(source, when):
    source._datetime = pd.Timestamp(when, tz=NY).to_pydatetime()


def test_tradier_daily_bars_are_point_in_time(monkeypatch):
    session = _TradierSession()
    source = _tradier_source(monkeypatch, session)
    stock = Asset("MIDC", "stock")
    _at(source, "2024-06-12 10:00")
    bars = source.get_historical_prices(stock, 5, "day")
    closes = bars.df["close"].tolist()
    # During June 12 the latest complete daily bar is June 11 (bars are stamped at the 16:00 close).
    assert closes == [CLOSES[date(2024, 6, d)] for d in (5, 6, 7, 10, 11)]
    assert bars.df.index.max().date() == date(2024, 6, 11)
    _at(source, "2024-06-12 16:00")  # after the close the day's bar is complete
    assert source.get_historical_prices(stock, 1, "day").df.index[-1].date() == date(2024, 6, 12)
    _at(source, "2024-06-20 10:00")
    assert source.get_historical_prices(stock, 1, "day").df.index[-1].date() == date(2024, 6, 19)
    # One request for the whole window (backtest start minus lookback, to the end), sent with the
    # regular session filter.
    assert len(session.calls) == 1
    assert session.calls[0][1]["session_filter"] == "open"


def test_daily_bars_are_stamped_at_the_new_york_close(monkeypatch):
    source = _tradier_source(monkeypatch, _TradierSession())
    _at(source, "2024-06-12 10:00")
    index = source.get_historical_prices(Asset("MIDC", "stock"), 3, "day").df.index
    assert all((ts.tz_convert(NY).hour, ts.tz_convert(NY).minute) == (16, 0) for ts in index)


def test_tradier_quota_violation_waits_and_retries_then_fails_loudly(monkeypatch):
    source = _tradier_source(monkeypatch, _TradierSession(fail_quota=1))
    _at(source, "2024-06-12 10:00")
    assert source.get_historical_prices(Asset("MIDC", "stock"), 2, "day") is not None

    failing = _tradier_source(monkeypatch, _TradierSession(fail_quota=99))
    _at(failing, "2024-06-12 10:00")
    with pytest.raises(BrokerHistoryRateLimited, match="120 requests per minute"):
        failing.get_historical_prices(Asset("MIDC", "stock"), 2, "day")


def test_no_bars_fails_loudly_and_nothing_is_filled(monkeypatch):
    source = _tradier_source(monkeypatch, _TradierSession(no_history=True))
    _at(source, "2024-06-12 10:00")
    with pytest.raises(BrokerHistoryUnavailable, match="returned no daily bars for MIDC"):
        source.get_historical_prices(Asset("MIDC", "stock"), 2, "day")


def test_missing_days_stay_missing(monkeypatch):
    holiday = date(2024, 6, 19)  # Juneteenth: the stand-in has a bar (synthetic); remove it
    saved = CLOSES.pop(holiday)
    try:
        source = _tradier_source(monkeypatch, _TradierSession())
        _at(source, "2024-06-21 16:00")
        index = source.get_historical_prices(Asset("MIDC", "stock"), 4, "day").df.index
        assert holiday not in {ts.date() for ts in index}
        expected = [date(2024, 6, 17), date(2024, 6, 18), date(2024, 6, 20), date(2024, 6, 21)]
        assert [ts.date() for ts in index] == expected
    finally:
        CLOSES[holiday] = saved


@pytest.mark.parametrize("asset", [
    Asset("SPY", Asset.AssetType.OPTION, expiration=date(2024, 6, 21), strike=500, right="CALL"),
    Asset("ES", Asset.AssetType.FUTURE, expiration=date(2024, 6, 21)),
    Asset("BTC", Asset.AssetType.CRYPTO),
    Asset("SPX", Asset.AssetType.INDEX),
])
def test_only_stocks_and_etfs(monkeypatch, asset):
    source = _tradier_source(monkeypatch, _TradierSession())
    _at(source, "2024-06-12 10:00")
    with pytest.raises(BrokerHistoryUnavailable, match="stocks and ETFs only"):
        source.get_historical_prices(asset, 2, "day")
    with pytest.raises(BrokerHistoryUnavailable, match="option chains are not available"):
        source.get_chains(Asset("SPY", "stock"))


def test_intraday_requests_name_the_broker_depth(monkeypatch):
    source = _tradier_source(monkeypatch, _TradierSession())
    _at(source, "2024-06-12 10:00")
    with pytest.raises(BrokerHistoryUnavailable, match="1-minute bars for about 20 days"):
        source.get_historical_prices(Asset("MIDC", "stock"), 30, "minute")


# --------------------------------------------------------------------------- Schwab


class _SchwabSession:
    def __init__(self):
        self.calls = []

    def get(self, url, params=None):
        self.calls.append((url, dict(params or {})))
        start = pd.Timestamp(int(params["startDate"]), unit="ms", tz="UTC")
        end = pd.Timestamp(int(params["endDate"]), unit="ms", tz="UTC")
        candles = []
        for d, c in CLOSES.items():
            # Daily candles are stamped at midnight US Central time.
            stamp = pd.Timestamp(d, tz="America/Chicago")
            if start <= stamp <= end:
                candles.append({"open": c - 0.5, "high": c + 1, "low": c - 1, "close": c, "volume": 1000,
                                "datetime": int(stamp.value // 10**6)})
        return _Response({"candles": candles, "symbol": params.get("symbol"), "empty": not candles})


def _schwab_source(session, start=datetime(2024, 6, 3), end=datetime(2024, 6, 28)):
    from schwab.client import Client

    client = Client("standin-key", session)
    adapter = broker_history.SchwabHistory(client, scope="schwab-standin", sleep=lambda s: None)
    return SchwabHistoryBacktesting(pd.Timestamp(start, tz=NY).to_pydatetime(),
                                    pd.Timestamp(end, tz=NY).to_pydatetime(), adapter=adapter)


def test_schwab_daily_candles_become_their_trading_dates(monkeypatch):
    session = _SchwabSession()
    source = _schwab_source(session)
    _at(source, "2024-06-12 16:00")
    bars = source.get_historical_prices(Asset("MIDC", "stock"), 3, "day")
    assert [ts.date() for ts in bars.df.index] == [date(2024, 6, 10), date(2024, 6, 11), date(2024, 6, 12)]
    assert bars.df["close"].tolist() == [CLOSES[date(2024, 6, d)] for d in (10, 11, 12)]
    assert len(session.calls) == 1
    assert session.calls[0][0].endswith("/marketdata/v1/pricehistory")
    assert session.calls[0][1]["needExtendedHoursData"] is False


def test_schwab_daily_history_before_1985_fails_with_the_limit():
    source = _schwab_source(_SchwabSession(), start=datetime(1984, 6, 1), end=datetime(1984, 12, 31))
    _at(source, "1984-06-04 10:00")
    with pytest.raises(BrokerHistoryUnavailable, match="Schwab daily history starts 1985-01-01"):
        source.get_historical_prices(Asset("MIDC", "stock"), 5, "day")


def test_schwab_intraday_names_its_depth():
    source = _schwab_source(_SchwabSession())
    _at(source, "2024-06-12 10:00")
    with pytest.raises(BrokerHistoryUnavailable, match="1-minute bars for about 48 days"):
        source.get_historical_prices(Asset("MIDC", "stock"), 30, "minute")


# --------------------------------------------------------------------------- generic pieces


def test_rate_limiter_keeps_each_broker_under_its_requests_per_minute():
    now = [0.0]
    waits = []

    def sleep(seconds):
        waits.append(seconds)
        now[0] += seconds

    limiter = broker_history.RateLimiter(120, clock=lambda: now[0], sleep=sleep)
    for _ in range(120):
        assert limiter.acquire() == 0
    assert limiter.acquire() > 59  # the 121st request in the same minute waits for the window
    assert len(waits) == 1


def test_generic_source_needs_a_broker():
    with pytest.raises(ValueError, match="needs a broker"):
        BrokerHistoryBacktesting(datetime(2024, 6, 3), datetime(2024, 6, 28))


def test_backtest_data_source_names_route_to_broker_history():
    from lumibot.strategies._strategy import _get_backtesting_class

    assert _get_backtesting_class("SchwabHistoryBacktesting") is SchwabHistoryBacktesting
    assert _get_backtesting_class("TradierHistoryBacktesting") is TradierHistoryBacktesting


def test_full_daily_backtest_on_tradier_history(monkeypatch, tmp_path):
    """A daily strategy backtests end to end on Tradier history, buying on the first day."""
    from lumibot.strategies import Strategy

    session = _TradierSession()
    from lumiwealth_tradier import Tradier

    client = Tradier("VA000000", "standin-token", True)
    monkeypatch.setattr(type(client.market), "requests_retry_session", lambda self, *a, **k: session)
    adapter = broker_history.TradierHistory(client, scope="tradier-standin", sleep=lambda s: None)
    monkeypatch.setattr(broker_history, "make_adapter", lambda name, config=None, **k: adapter)
    monkeypatch.setenv("BACKTESTING_DATA_SOURCE", "tradier")  # selected by name, like "alpaca"
    monkeypatch.chdir(tmp_path)

    class BuyAndHold(Strategy):
        def initialize(self):
            self.sleeptime = "1D"

        def on_trading_iteration(self):
            if self.first_iteration:
                self.submit_order(self.create_order(Asset("MIDC", "stock"), 10, "buy"))

    result, strategy = BuyAndHold.run_backtest(
        None, datetime(2024, 6, 3), datetime(2024, 6, 28), benchmark_asset=None,
        show_plot=False, show_tearsheet=False, save_tearsheet=False, show_indicators=False,
        show_progress_bar=False, budget=10000, quiet_logs=True,
    )
    position = strategy.get_position(Asset("MIDC", "stock"))
    assert position is not None and position.quantity == 10
    # One download for the window, and at most one more for an earlier stretch when a later
    # request needs a longer lookback (the reports read about 60 days of benchmark-style history).
    assert len(session.calls) <= 2
    if len(session.calls) == 2:
        first, second = session.calls[0][1], session.calls[1][1]
        assert second["end"] <= first["start"]  # only the missing earlier stretch (end is inclusive)


def test_no_cash_dividends_are_credited(monkeypatch):
    """Broker history has no dividend column; the docs say cash dividends are not applied."""
    source = _tradier_source(monkeypatch, _TradierSession())
    _at(source, "2024-06-12 10:00")
    bars = source.get_historical_prices(Asset("MIDC", "stock"), 3, "day")
    assert "dividend" not in bars.df.columns
    assert not source.get_yesterday_dividend(Asset("MIDC", "stock"))
