"""Binance bulk-file routing: USD-M perpetuals and spot from data.binance.vision.

Fixtures are tiny zips built here in the exact shape Binance publishes (CSV inside a
zip, a ``.CHECKSUM`` file next to it holding the SHA-256), served by a fake HTTP
session. No network. They prove:

* ``crypto_future`` routed to ``binance`` reads perpetual klines, contract terms and
  funding (with Binance's mark price at each settlement),
* complete months come from monthly files, the current month from daily files,
* every file is checked against its CHECKSUM,
* data Binance has not published is reported as unavailable, never filled,
* ``crypto`` routed to ``binance`` reads spot, and a USD request is served from the
  USDT pair only with a recorded note,
* the shared cache key holds only provider, symbol, bar size and price kind.
"""

from __future__ import annotations

import hashlib
import io
import json
import zipfile
from datetime import datetime, timezone
from decimal import Decimal

import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker
from lumibot.backtesting.routed_backtesting import (
    PerpetualDataUnavailable,
    RoutedBacktestingPandas,
    RoutingProviderError,
)
from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
from lumibot.entities import Asset
from lumibot.tools.binance_bulk_data import (
    BinanceBulkData,
    BinanceBulkDataError,
    perpetual_symbol,
    shared_series,
    spot_symbol,
)
from tests.test_backtesting_multileg_unit import _StubStrategy

ROOT = "https://data.binance.vision/data"
KLINE_HEADER = ("open_time,open,high,low,close,volume,close_time,quote_volume,count,"
                "taker_buy_volume,taker_buy_quote_volume,ignore")
NOW = pd.Timestamp("2026-08-04 06:00", tz="UTC")  # Aug 3 daily files exist; Aug 4 not yet


def _kline_rows(start, periods, step, base, unit="ms"):
    rows = []
    for i in range(periods):
        opened = pd.Timestamp(start) + i * step
        stamp = int(opened.timestamp() * (1_000_000 if unit == "us" else 1000))
        price = base + i
        rows.append(f"{stamp},{price},{price + 5},{price - 5},{price + 1},1.5,{stamp + 1},1,1,1,1,0")
    return rows


def _zip(name, lines):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(name.replace(".zip", ".csv"), "\n".join(lines) + "\n")
    payload = buffer.getvalue()
    return payload, f"{hashlib.sha256(payload).hexdigest()}  {name}\n".encode()


class _Response:
    def __init__(self, content=None, status=200):
        self.content, self.status_code = content, status


class _Archive:
    """URL -> bytes; anything not present answers 404, like the real archive."""

    def __init__(self):
        self.files: dict[str, bytes] = {}
        self.calls: list[str] = []
        self.status = None

    def add(self, path, lines, *, corrupt=False):
        name = path.rsplit("/", 1)[-1]
        payload, checksum = _zip(name, lines)
        self.files[f"{ROOT}/{path}"] = payload
        self.files[f"{ROOT}/{path}.CHECKSUM"] = b"0" * 64 + b"  x\n" if corrupt else checksum

    def get(self, url, timeout=None):
        self.calls.append(url)
        if self.status:
            return _Response(None, self.status)
        return _Response(self.files[url]) if url in self.files else _Response(None, 404)


def _perp_archive(symbol="BTCUSDT"):
    archive = _Archive()
    hour = pd.Timedelta(hours=1)
    # July: complete month -> monthly files (header row, like newer Binance files).
    archive.add(f"futures/um/monthly/klines/{symbol}/1h/{symbol}-1h-2026-07.zip",
                [KLINE_HEADER] + _kline_rows("2026-07-31 20:00", 4, hour, 60000))
    archive.add(f"futures/um/monthly/markPriceKlines/{symbol}/1h/{symbol}-1h-2026-07.zip",
                [KLINE_HEADER] + _kline_rows("2026-07-31 20:00", 4, hour, 60010))
    archive.add(f"futures/um/monthly/fundingRate/{symbol}/{symbol}-fundingRate-2026-07.zip",
                ["calc_time,funding_interval_hours,last_funding_rate",
                 f"{int(pd.Timestamp('2026-07-31 16:00', tz='UTC').timestamp() * 1000)},8,0.0001"])
    # August: current month -> daily files only (older files have no header row).
    for day in ("2026-08-01", "2026-08-02", "2026-08-03"):
        archive.add(f"futures/um/daily/klines/{symbol}/1h/{symbol}-1h-{day}.zip",
                    _kline_rows(f"{day} 00:00", 24, hour, 62000))
    return archive


def _client(archive):
    return BinanceBulkData(session=archive, workers=4, now=NOW)


def _router(routing, monkeypatch, archive, start="2026-08-03 03:00", end="2026-08-03 09:00"):
    import lumibot.tools.thetadata_helper as thetadata_helper

    monkeypatch.setenv("LUMIBOT_SHARED_BARS", "false")
    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_a, **_k: None)
    monkeypatch.setattr(thetadata_helper, "reset_theta_terminal_tracking", lambda *_a, **_k: None)
    router = RoutedBacktestingPandas(
        datetime_start=pd.Timestamp(start, tz="UTC").to_pydatetime(),
        datetime_end=pd.Timestamp(end, tz="UTC").to_pydatetime(),
        config={"backtesting_data_routing": routing},
        username="dev", password="dev", use_quote_data=False,
        show_progress_bar=False, log_backtest_progress_to_file=False,
    )
    router._registry._adapters["binance"]._client = _client(archive)
    return router


PERP = Asset("BTCUSDT", asset_type="crypto_future")
USDT = Asset("USDT", "crypto")


# -- symbols -----------------------------------------------------------------------------
@pytest.mark.parametrize("asset,quote,expected", [
    (Asset("BTCUSDT", "crypto_future"), None, "BTCUSDT"),
    (Asset("BTC/USDT:USDT", "crypto_future"), None, "BTCUSDT"),
    (Asset("BTC", "crypto_future"), USDT, "BTCUSDT"),
    (Asset("1000PEPEUSDT", "crypto_future"), None, "1000PEPEUSDT"),
    (Asset("ETHUSDC", "crypto_future"), None, "ETHUSDC"),
])
def test_perpetual_symbol(asset, quote, expected):
    assert perpetual_symbol(asset, quote) == expected


@pytest.mark.parametrize("raw,reason", [("BTCUSD", "coin_margined_unsupported"),
                                        ("BTCUSD_PERP", "coin_margined_unsupported"),
                                        ("BTCUSDT_260925", "contract_not_listed")])
def test_unsupported_perpetual_symbols_say_why(raw, reason):
    with pytest.raises(BinanceBulkDataError) as caught:
        perpetual_symbol(Asset(raw, "crypto_future"))
    assert caught.value.reason == reason


def test_bare_coin_with_usd_quote_is_not_guessed_for_perpetuals():
    with pytest.raises(BinanceBulkDataError) as caught:
        perpetual_symbol(Asset("BTC", "crypto_future"), Asset("USD", "forex"))
    assert caught.value.reason == "ambiguous_symbol"


def test_spot_usd_is_never_served_from_the_usdt_pair():
    # USDT trades about 0.12% below USD; serving BTC/USD from BTCUSDT would shift every price.
    with pytest.raises(BinanceBulkDataError, match="BTCUSDT"):
        spot_symbol(Asset("BTC", "crypto"), Asset("USD", "forex"))
    assert spot_symbol(Asset("ETH", "crypto"), Asset("USDC", "crypto")) == "ETHUSDC"
    assert spot_symbol(Asset("SOL", "crypto"), USDT) == "SOLUSDT"
    assert spot_symbol(Asset("BTC", "crypto"), Asset("FDUSD", "crypto")) == "BTCFDUSD"


# -- files ---------------------------------------------------------------------------------
def test_monthly_then_daily_files_make_one_series():
    archive = _perp_archive()
    frame = _client(archive).candles("um", "BTCUSDT", "hour", "2026-07-31 20:00", "2026-08-04 00:00")
    assert frame.index[0] == pd.Timestamp("2026-07-31 20:00", tz="UTC")
    assert frame.index[-1] == pd.Timestamp("2026-08-03 23:00", tz="UTC")
    assert len(frame) == 4 + 72 and frame.index.is_monotonic_increasing
    assert frame.loc[pd.Timestamp("2026-08-01 00:00", tz="UTC"), "close"] == 62001
    # Today's daily file is never asked for (Binance publishes it tomorrow).
    assert not any("2026-08-04" in url for url in archive.calls)


def test_checksum_mismatch_is_refused():
    archive = _Archive()
    archive.add("futures/um/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2026-08-03.zip",
                _kline_rows("2026-08-03", 24, pd.Timedelta(hours=1), 1), corrupt=True)
    with pytest.raises(BinanceBulkDataError, match="checksum"):
        _client(archive).candles("um", "BTCUSDT", "hour", "2026-08-03", "2026-08-04")


def test_spot_microsecond_timestamps_are_read_correctly():
    archive = _Archive()
    archive.add("spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2026-08-03.zip",
                _kline_rows("2026-08-03 00:00", 3, pd.Timedelta(minutes=1), 100, unit="us"))
    frame = _client(archive).candles("spot", "BTCUSDT", "minute", "2026-08-03", "2026-08-03 00:03")
    assert list(frame.index) == list(pd.date_range("2026-08-03", periods=3, freq="min", tz="UTC"))


def test_unreachable_archive_is_not_reported_as_empty():
    archive = _perp_archive()
    archive.status = 503
    with pytest.raises(BinanceBulkDataError) as caught:
        _client(archive).candles("um", "BTCUSDT", "hour", "2026-08-03", "2026-08-04")
    assert caught.value.reason == "source_unreachable"


def test_funding_carries_binance_mark_and_coverage_end():
    events, covered = _client(_perp_archive()).funding_history(
        "BTCUSDT", pd.Timestamp("2026-07-31", tz="UTC"), pd.Timestamp("2026-08-03 09:00", tz="UTC"))
    assert len(events) == 1
    assert events[0]["timestamp"] == pd.Timestamp("2026-07-31 16:00", tz="UTC")
    assert events[0]["fundingRate"] == Decimal("0.0001")
    # The 16:00 mark is not in the fixture's mark file (it starts at 20:00): no invented mark.
    assert events[0]["markPrice"] is None
    assert covered == pd.Timestamp("2026-07-31 23:59:59", tz="UTC")


def test_shared_cache_key_names_only_what_changes_the_numbers():
    series = shared_series("um", "BTCUSDT", "minute")
    assert series.provider == "binance_um" and series.symbol == "BTCUSDT"
    assert series == shared_series("um", "BTCUSDT", "minute")  # same for every user
    assert series != shared_series("spot", "BTCUSDT", "minute")
    assert series != shared_series("um", "BTCUSDT", "minute", kind="markPriceKlines")


# -- routing --------------------------------------------------------------------------------
def test_binance_route_serves_perpetual_candles(monkeypatch):
    router = _router({"crypto_future": "binance", "default": "ibkr"}, monkeypatch, _perp_archive())
    router._datetime = pd.Timestamp("2026-08-03 08:00", tz="UTC").to_pydatetime()
    bars = router.get_historical_prices(PERP, 2, timestep="hour", quote=USDT)
    assert bars is not None and not bars.df.empty
    assert set(bars.df["close"]) <= {62000.0 + i + 1 for i in range(24)}
    spec = router.get_contract_spec(PERP)
    assert (spec.base, spec.quote) == ("BTC", "USDT")


def test_window_past_published_files_is_reported_unavailable(monkeypatch):
    router = _router({"crypto_future": "binance", "default": "ibkr"}, monkeypatch, _perp_archive(),
                     end="2026-08-04 05:00")
    with pytest.raises(PerpetualDataUnavailable) as caught:
        router._update_pandas_data(PERP, USDT, 2, "hour")
    assert caught.value.reason == "no_data_for_window"
    assert "2026-08-03 23:00" in str(caught.value) and "PERPETUAL_DATA_UNAVAILABLE" in str(caught.value)


def test_unpublished_symbol_is_unavailable_with_a_hint(monkeypatch):
    router = _router({"crypto_future": "binance", "default": "ibkr"}, monkeypatch, _perp_archive())
    with pytest.raises(PerpetualDataUnavailable) as caught:
        router._update_pandas_data(Asset("PEPEUSDT", "crypto_future"), USDT, 2, "hour")
    assert caught.value.reason == "contract_not_listed"
    assert "1000PEPEUSDT" in str(caught.value)


def test_coin_margined_contract_fails_before_any_order(monkeypatch):
    router = _router({"crypto_future": "binance", "default": "ibkr"}, monkeypatch, _perp_archive())
    with pytest.raises(PerpetualDataUnavailable) as caught:
        router._update_pandas_data(Asset("BTCUSD", "crypto_future"), None, 2, "hour")
    assert caught.value.reason == "coin_margined_unsupported"


def test_binance_is_listed_as_a_perpetual_alternative(monkeypatch):
    router = _router({"crypto_future": "binance", "default": "ibkr"}, monkeypatch, _perp_archive())
    with pytest.raises(PerpetualDataUnavailable) as caught:
        router._update_pandas_data(Asset("BTCUSD", "crypto_future"), None, 2, "hour")
    assert "bitunix" in {item["routing"] for item in caught.value.alternatives}


def test_provenance_records_price_and_funding_source(monkeypatch):
    router = _router({"crypto_future": "binance", "default": "ibkr"}, monkeypatch, _perp_archive())
    router._datetime = pd.Timestamp("2026-08-03 08:00", tz="UTC").to_pydatetime()
    router.get_historical_prices(PERP, 2, timestep="hour", quote=USDT)
    route = next(r for r in router.get_data_provenance()["observedRoutes"] if r["assetClass"] == "crypto_future")
    assert route["priceSource"] == "binance_usdm_bulk" and route["fundingSource"] == "binance_usdm_bulk"
    assert route["servedSymbol"] == "BTCUSDT"


def test_funding_after_last_published_month_is_recorded_not_invented(monkeypatch):
    router = _router({"crypto_future": "binance", "default": "ibkr"}, monkeypatch, _perp_archive(),
                     start="2026-07-31 12:00")
    events = router.get_funding_events(PERP, pd.Timestamp("2026-07-31", tz="UTC"),
                                       pd.Timestamp("2026-08-03 09:00", tz="UTC"))
    assert [e["timestamp"] for e in events] == [pd.Timestamp("2026-07-31 16:00", tz="UTC")]
    notices = router.get_data_provenance()["notices"]
    assert any(n["kind"] == "funding_not_published" and n["symbol"] == "BTCUSDT"
               and n["through"].startswith("2026-07-31") for n in notices)


def test_window_ending_at_a_published_month_end_has_no_funding_notice(monkeypatch):
    # A backtest ending at midnight Aug 1 is fully covered by July's funding file.
    router = _router({"crypto_future": "binance", "default": "ibkr"}, monkeypatch, _perp_archive(),
                     start="2026-07-31 12:00", end="2026-08-01 00:00")
    router.get_funding_events(PERP, pd.Timestamp("2026-07-31", tz="UTC"), pd.Timestamp("2026-08-01", tz="UTC"))
    assert "notices" not in router.get_data_provenance()


def test_perpetual_order_fills_and_pays_funding_at_binance_mark(monkeypatch):
    archive = _perp_archive()
    archive.add("futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2026-07.zip",
                ["calc_time,funding_interval_hours,last_funding_rate",
                 f"{int(pd.Timestamp('2026-07-31 21:00', tz='UTC').timestamp() * 1000) + 3},8,0.0002"])
    router = _router({"crypto_future": "binance", "default": "ibkr"}, monkeypatch, archive,
                     start="2026-07-31 20:00", end="2026-08-01 02:00")
    router._datetime = pd.Timestamp("2026-07-31 20:30", tz="UTC").to_pydatetime()
    broker = BacktestingBroker(router)
    try:
        strategy = _StubStrategy(broker=broker, quote_asset=USDT, budget=1000, analyze_backtest=False)
        asset = Asset("BTCUSDT", "crypto_future", leverage=10)
        order = strategy.create_order(asset, Decimal("0.01"), "buy")
        broker._process_new_order(order)
        broker._execute_filled_order(order, 60000.0, Decimal("0.01"), strategy)
        assert order.status != "error", order
        router._datetime = pd.Timestamp("2026-07-31 22:30", tz="UTC").to_pydatetime()
        broker._perpetuals.process_events(strategy)
        funding = [e for e in broker.perpetual_events if e["type"] == "funding"]
        # Mark-price kline opening 21:00 has open 60011 (base 60010 + 1).
        assert len(funding) == 1
        assert Decimal(funding[0]["amount"]) == pytest.approx(Decimal("-0.01") * Decimal("60011") * Decimal("0.0002"))
    finally:
        broker.cleanup_streams()


def _spot_archive():
    archive = _Archive()
    archive.add("spot/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2026-08-03.zip",
                _kline_rows("2026-08-03", 24, pd.Timedelta(hours=1), 50000, unit="us"))
    return archive


def test_binance_route_serves_spot_stablecoin_pairs_exactly(monkeypatch):
    router = _router({"crypto": "binance", "default": "ibkr"}, monkeypatch, _spot_archive())
    router._datetime = pd.Timestamp("2026-08-03 08:00", tz="UTC").to_pydatetime()
    bars = router.get_historical_prices(Asset("BTC", "crypto"), 2, timestep="hour", quote=USDT)
    assert bars is not None and set(bars.df["close"]) <= {50000.0 + i + 1 for i in range(24)}
    route = next(r for r in router.get_data_provenance()["observedRoutes"] if r["assetClass"] == "crypto")
    assert route["priceSource"] == "binance_spot_bulk" and route["servedSymbol"] == "BTCUSDT"


def test_explicit_binance_route_refuses_usd_and_names_the_usdt_pair(monkeypatch):
    router = _router({"crypto": "binance", "default": "ibkr"}, monkeypatch, _spot_archive())
    with pytest.raises(RoutingProviderError, match="BTCUSDT") as caught:
        router._update_pandas_data(Asset("BTC", "crypto"), Asset("USD", "forex"), 2, "hour")
    assert "public" in str(caught.value)  # says which route serves USD pairs


def _record_ccxt(router):
    calls = []

    def update_pandas_data(**kwargs):
        calls.append(kwargs)
        return None

    router._registry._adapters["ccxt"].update_pandas_data = update_pandas_data
    return calls


def test_public_route_serves_usd_pairs_from_coinbase(monkeypatch):
    router = _router({"crypto": "public", "crypto_future": "public", "default": "ibkr"}, monkeypatch, _spot_archive())
    calls = _record_ccxt(router)
    router._update_pandas_data(Asset("BTC", "crypto"), Asset("USD", "forex"), 2, "hour")
    assert len(calls) == 1 and calls[0]["provider_spec"].ccxt_exchange_id == "coinbase"
    route = next(r for r in router.get_data_provenance()["observedRoutes"] if r["assetClass"] == "crypto")
    assert route["priceSource"] == "coinbase_public" and route["servedSymbol"] == "BTC/USD"


def test_public_route_serves_stablecoin_pairs_and_perpetuals_from_binance(monkeypatch):
    archive = _perp_archive()
    archive.files.update(_spot_archive().files)
    router = _router({"crypto": "public", "crypto_future": "public", "default": "ibkr"}, monkeypatch, archive)
    calls = _record_ccxt(router)
    router._datetime = pd.Timestamp("2026-08-03 08:00", tz="UTC").to_pydatetime()
    spot = router.get_historical_prices(Asset("BTC", "crypto"), 2, timestep="hour", quote=USDT)
    perp = router.get_historical_prices(PERP, 2, timestep="hour", quote=USDT)
    assert calls == []
    assert spot is not None and set(spot.df["close"]) <= {50000.0 + i + 1 for i in range(24)}
    assert perp is not None and set(perp.df["close"]) <= {62000.0 + i + 1 for i in range(24)}
    assert router.get_contract_spec(PERP).quote == "USDT"
    sources = {r["assetClass"]: r["priceSource"] for r in router.get_data_provenance()["observedRoutes"]}
    assert sources == {"crypto": "binance_spot_bulk", "crypto_future": "binance_usdm_bulk"}


def test_binance_route_refuses_non_crypto_assets(monkeypatch):
    router = _router({"stock": "binance", "default": "ibkr"}, monkeypatch, _perp_archive())
    with pytest.raises(RoutingProviderError, match="crypto"):
        router._update_pandas_data(Asset("SPY", "stock"), None, 2, "hour")


def test_other_ccxt_exchange_ids_still_route_to_ccxt(monkeypatch):
    router = _router({"crypto": "kraken", "default": "ibkr"}, monkeypatch, _perp_archive())
    assert router._provider_spec_for_asset(Asset("BTC", "crypto")).provider == "ccxt"
    assert router._provider_spec_for_asset(Asset("BTC", "crypto")).ccxt_exchange_id == "kraken"
