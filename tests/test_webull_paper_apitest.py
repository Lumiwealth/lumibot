"""Actual PaperTrade qualification. Use only an owned test account."""
import json
import os
import stat
import time
from decimal import Decimal
from pathlib import Path

import pytest

from lumibot.brokers import Webull
from lumibot.entities import Asset
from lumibot.strategies import Strategy
from lumibot.tools.webull_helpers import WebullAPIError, make_client, request_json

pytestmark = [pytest.mark.apitest, pytest.mark.webull]


@pytest.fixture
def paper(tmp_path, request):
    configured = os.environ.get("LUMIBOT_WEBULL_PAPER_CONFIG")
    if not configured:
        pytest.skip("Set LUMIBOT_WEBULL_PAPER_CONFIG to the owned PaperTrade test config")
    path = Path(configured)
    assert not (stat.S_IMODE(path.stat().st_mode) & 0o077)
    config = json.loads(path.read_bytes())
    assert config["PAPER"] is True, "This test only runs in Webull PaperTrade"
    assert all(config.get(key) for key in ("APP_KEY", "APP_SECRET", "ACCOUNT_ID"))
    config["TOKEN_DIR"] = str(tmp_path / "tokens")
    observer = make_client(config, trading=True)
    accounts = request_json("independent account discovery", observer.account_v2.get_account_list)
    requested_class = getattr(request, "param", None)
    if requested_class:
        matches = [a for a in accounts if a["account_class"] == requested_class]
        assert len(matches) == 1, f"PaperTrade has no unique {requested_class} account"
        config["ACCOUNT_ID"] = matches[0]["account_id"]
    else:
        assert len([a for a in accounts if a["account_id"] == config["ACCOUNT_ID"] and
                    a["account_class"] in {"INDIVIDUAL_CASH", "INDIVIDUAL_MARGIN"}]) == 1
    broker = Webull(config, connect_stream=False, start_orders_thread=False)
    try:
        yield broker, observer, config["ACCOUNT_ID"]
    finally:
        broker.cleanup_streams()


def test_paper_equity_account_quote_and_daily_history(paper):
    broker, observer, account = paper
    direct_balance = request_json("independent balance", observer.account_v2.get_account_balance, account)
    assert broker._get_balances_at_broker(None, None) == tuple(float(direct_balance[key]) for key in (
        "total_cash_balance", "total_market_value", "total_net_liquidation_value",
    ))
    asset = Asset("AAPL")
    quote = broker.data_source.get_quote(asset)
    assert quote.price > 0 and quote.timestamp.tzinfo is not None
    bars = broker.data_source.get_historical_prices(asset, 3, timestep="day")
    assert len(bars.pandas_df) == 3
    assert bars.pandas_df.index.is_monotonic_increasing


def _detail(observer, account, identifier, predicate):
    for pause in (0, 1, 2, 3, 4):
        time.sleep(pause)
        try:
            group = request_json("independent order detail", observer.order_v3.get_order_detail,
                                 account, identifier)
        except WebullAPIError as error:
            if error.status not in (400, 404):
                raise
            continue
        for item in group.get("orders", []):
            if item.get("client_order_id") == identifier and predicate(item):
                return item
    pytest.fail("Independent Webull order detail did not reach the expected state")


def test_paper_stock_limit_submit_replace_cancel(paper):
    broker, observer, account = paper
    strategy = Strategy(broker=broker, name="webull_qualification", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    asset = Asset("AAPL")
    quote = broker.data_source.get_quote(asset)
    limit = (Decimal(str(quote.price)) * Decimal("0.95")).quantize(Decimal("0.01"))
    replacement = limit - Decimal("0.01")
    order = strategy.create_order(asset, 1, "buy", time_in_force="day", limit_price=limit,
                                  custom_params={"support_trading_session": "ALL"})
    before_positions = request_json("positions before", observer.account_v2.get_account_position, account)
    before_cash = request_json("balance before", observer.account_v2.get_account_balance, account)
    try:
        strategy.submit_order(order)
        assert order.status != order.OrderStatus.ERROR, order.error_message
        placed = _detail(observer, account, order.identifier, lambda row: bool(row.get("order_id")))
        assert placed["symbol"] == asset.symbol
        assert placed["side"] == "BUY"
        assert placed["order_type"] == "LIMIT"
        assert placed["time_in_force"] == "DAY"
        assert placed["support_trading_session"] == "ALL"
        assert Decimal(placed["total_quantity"]) == 1
        assert Decimal(placed["limit_price"]) == limit
        assert placed["status"] not in {"FAILED", "REJECTED", "CANCELLED", "CANCELED", "EXPIRED"}
        strategy.modify_order(order, limit_price=replacement)
        _detail(observer, account, order.identifier, lambda row: Decimal(row["limit_price"]) == replacement)
    finally:
        if order.was_transmitted():
            strategy.cancel_order(order)
            cancelled = _detail(observer, account, order.identifier,
                                lambda row: row["status"] in {"CANCELLED", "CANCELED"})
            assert Decimal(cancelled["filled_quantity"]) == 0
    after_positions = request_json("positions after", observer.account_v2.get_account_position, account)
    after_cash = request_json("balance after", observer.account_v2.get_account_balance, account)
    assert {(r["symbol"], r["quantity"]) for r in after_positions} == {
        (r["symbol"], r["quantity"]) for r in before_positions
    }
    assert after_cash["total_cash_balance"] == before_cash["total_cash_balance"]


@pytest.mark.parametrize("paper", ["CRYPTO"], indirect=True)
def test_paper_crypto_market_round_trip(paper):
    broker, observer, account = paper
    strategy = Strategy(broker=broker, name="webull_crypto_qualification", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    asset = Asset("BTC", asset_type="crypto")
    usd = Asset("USD", asset_type="forex")

    def balance():
        return request_json("independent crypto balance", observer.account_v2.get_account_balance, account)

    def position():
        rows = request_json("independent crypto position", observer.account_v2.get_account_position, account)
        return sum((Decimal(r["quantity"]) for r in rows if r["symbol"] in {"BTC", "BTCUSD"}), Decimal(0))

    def buying_power():
        return next(Decimal(r["buying_power"]) for r in balance()["account_currency_assets"]
                    if r["currency"] == "USD")

    before_quantity = position()
    before_power = buying_power()
    opening = strategy.create_order(asset, Decimal("0.0001"), "buy", quote=usd,
                                    time_in_force="ioc", order_type="market")
    strategy.submit_order(opening)
    assert opening.status != opening.OrderStatus.ERROR, opening.error_message
    bout = _detail(observer, account, opening.identifier,
                   lambda row: row.get("status") in {"FILLED", "CANCELLED", "CANCELED", "REJECTED", "FAILED"})
    bought = Decimal(bout["filled_quantity"])
    try:
        assert bout["status"] == "FILLED"
        assert bought == opening.quantity
        assert bout["symbol"] == "BTCUSD" and bout["side"] == "BUY" and bout["time_in_force"] == "IOC"
        assert Decimal(bout["filled_price"]) > 0
        for pause in (0, 1, 2, 3):
            time.sleep(pause)
            if (position() == before_quantity + bought and
                    buying_power() < before_power):
                break
        else:
            pytest.fail("Webull confirmed the buy fill but the account snapshot did not reconcile")
    finally:
        if bought > 0:
            closing = strategy.create_order(asset, bought, "sell", quote=usd,
                                            time_in_force="ioc", order_type="market")
            strategy.submit_order(closing)
            assert closing.status != closing.OrderStatus.ERROR, closing.error_message
            done = _detail(observer, account, closing.identifier, lambda row: row.get("status") == "FILLED")
            assert Decimal(done["filled_quantity"]) == bought
            assert done["symbol"] == "BTCUSD" and done["side"] == "SELL"
    expected_change = bought * (Decimal(done["filled_price"]) - Decimal(bout["filled_price"]))
    for pause in (0, 1, 2, 3):
        time.sleep(pause)
        if position() == before_quantity and abs(buying_power() - before_power - expected_change) <= Decimal("0.02"):
            break
    else:
        pytest.fail("Confirmed close did not reconcile the initial BTC position and USD buying power")


@pytest.mark.parametrize("duration", ["day", "gtc"])
def test_paper_stock_bracket_is_resting_at_the_venue(paper, duration):
    broker, observer, account = paper
    strategy = Strategy(broker=broker, name="webull_bracket_qualification", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    asset = Asset("AAPL")
    last = Decimal(str(broker.data_source.get_quote(asset).price))
    entry_limit = (last * Decimal("0.95")).quantize(Decimal("0.01"))
    target_limit = (last * Decimal("1.04")).quantize(Decimal("0.01"))
    stop_price = (last * Decimal("0.90")).quantize(Decimal("0.01"))
    order = strategy.create_order(asset, 1, "buy", limit_price=entry_limit, time_in_force=duration,
                                  order_class="bracket", secondary_limit_price=target_limit,
                                  secondary_stop_price=stop_price,
                                  custom_params={"support_trading_session": "CORE"})
    for child in order.child_orders:
        child.time_in_force = duration
    try:
        strategy.submit_order(order)
        assert order.status != order.OrderStatus.ERROR, order.error_message
        observed = request_json("independent bracket detail", observer.order_v3.get_order_detail,
                                account, order.identifier)
        assert observed.get("combo_order_id")
        venue_orders = {row["client_order_id"]: row for row in observed["orders"]}
        assert len(venue_orders) == 3
        assert all(row.get("order_id") for row in venue_orders.values())
        assert {r["order_type"] for r in venue_orders.values()} == {"LIMIT", "STOP_LOSS"}
        assert venue_orders[order.identifier]["side"] == "BUY"
        assert Decimal(venue_orders[order.identifier]["filled_quantity"]) == 0
        assert all(venue_orders[child.identifier]["side"] == "SELL" for child in order.child_orders)
    finally:
        if order.was_transmitted():
            strategy.cancel_order(order)
            canceled = _detail(observer, account, order.identifier,
                               lambda row: row["status"] in {"CANCELED", "CANCELLED", "EXPIRED"})
            assert Decimal(canceled["filled_quantity"]) == 0
            ended = request_json("independent bracket cleanup", observer.order_v3.get_order_detail,
                                 account, order.identifier)
            assert all(r["status"] in {"CANCELED", "CANCELLED", "EXPIRED", "FAILED"}
                       for r in ended["orders"])


@pytest.mark.parametrize("product", ["stock", "crypto"])
def test_webull_daily_strategy_replays_actual_papertrade_bars(paper, monkeypatch, product):
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from lumibot.backtesting import WebullHistoryBacktesting

    broker, observer, account = paper
    monkeypatch.delenv("BACKTESTING_DATA_SOURCE", raising=False)

    asset = Asset("AAPL") if product == "stock" else Asset("BTC", asset_type="crypto")
    quantity = 1 if product == "stock" else Decimal("0.0001")
    quote = Asset("USD", "forex")

    class DailyReplay(Strategy):
        def initialize(self):
            if product == "crypto":
                self.set_market("24/7")
            self.sleeptime = "1D"
            self.trade_count = 0

        def on_trading_iteration(self):
            if self.trade_count < 2:
                self.submit_order(self.create_order(asset, quantity, "buy" if self.trade_count == 0 else "sell", quote=quote))
                self.trade_count += 1

    ny = ZoneInfo("America/New_York")
    start = datetime(2026, 11, 9, tzinfo=ny)
    end = datetime(2026, 11, 13, tzinfo=ny)
    baseline = request_json("orders before replay", observer.order_v3.list_order_open, account)
    _, strategy = DailyReplay.run_backtest(
        WebullHistoryBacktesting, start, end, config={"data_source": broker.data_source},
        budget=20_000, benchmark_asset=None, risk_free_rate=0, name="webull_daily_replay",
        analyze_backtest=False, save_stats_file=False, save_tearsheet=False, show_plot=False,
        show_progress_bar=False, show_tearsheet=False,
    )
    orders = strategy.broker.get_all_orders()
    assert len(orders) == 2 and all(order.is_filled() for order in orders)
    assert all(order.avg_fill_price > 0 for order in orders)
    assert request_json("orders after replay", observer.order_v3.list_order_open, account) == baseline


@pytest.mark.parametrize("paper", ["INDIVIDUAL_MARGIN"], indirect=True)
@pytest.mark.parametrize("package", ["single", "vertical"])
def test_paper_single_and_vertical_option_orders(paper, package):
    from datetime import date, timedelta

    broker, observer, account = paper
    strategy = Strategy(broker=broker, name="webull_option_qualification", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    underlying = Asset("AAPL")
    last_price = Decimal(str(broker.data_source.get_quote(underlying).price))
    today = date.today()
    after = today + timedelta(days=21)
    contracts = broker.data_source.option_contracts(
        underlying, root_symbol="AAPL", option_type="CALL", end_date=after.isoformat(),
        strike_price_gte=str(last_price - 12), strike_price_lte=str(last_price + 12),
    )
    expiry = min(row["expiration_date"] for row in contracts if row["expiration_date"] >= after.isoformat())
    matches = sorted((row for row in contracts if row["expiration_date"] == expiry),
                     key=lambda row: abs(Decimal(row["strike_price"]) - last_price))
    first, second = sorted(matches[:2], key=lambda row: Decimal(row["strike_price"]))
    assets = [Asset("AAPL", asset_type="option", right="CALL", expiration=date.fromisoformat(expiry),
                    strike=float(row["strike_price"]), multiplier=float(Decimal(row["multiplier"])))
              for row in (first, second)]

    def verify_and_cancel(order, native_legs):
        assert order.status != order.OrderStatus.ERROR, order.error_message
        try:
            observed = _detail(observer, account, order.identifier, lambda row: bool(row.get("order_id")))
            assert observed["instrument_type"] == "OPTION"
            assert Decimal(observed["total_quantity"]) == 1
            assert len(observed["legs"]) == native_legs
            assert observed["status"] not in {"FAILED", "REJECTED", "EXPIRED"}
            assert {item["side"] for item in observed["legs"]} == (
                {"BUY"} if native_legs == 1 else {"BUY", "SELL"}
            )
        finally:
            if order.was_transmitted():
                strategy.cancel_order(order)
                canceled = _detail(observer, account, order.identifier,
                                   lambda row: row["status"] in {"CANCELED", "CANCELLED", "EXPIRED"})
                assert Decimal(canceled["filled_quantity"]) == 0

    if package == "single":
        single = strategy.create_order(assets[0], 1, "buy_to_open", time_in_force="day", limit_price=0.05)
        strategy.submit_order(single)
        verify_and_cancel(single, 1)
        return

    legs = [strategy.create_order(asset, 1, side, time_in_force="day")
            for asset, side in zip(assets, ("buy_to_open", "sell_to_open"))]
    [native] = strategy.submit_orders(legs, is_multileg=True, order_type="debit", duration="day", price=0.05)
    verify_and_cancel(native, 2)


@pytest.mark.parametrize("paper", ["CRYPTO"], indirect=True)
def test_paper_private_trade_stream_observes_real_crypto_order(paper):
    from threading import Event, Thread

    from lumibot.brokers.webull_events import WebullOrderEvents

    broker, observer, account = paper
    setup = broker._webull_config
    feed = WebullOrderEvents(setup["APP_KEY"], setup["APP_SECRET"], account, paper=True)
    ready = Event()
    received = Event()
    strategy = Strategy(broker=broker, name="webull_order_feed_qualification", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    asset, currency = Asset("BTC", asset_type="crypto"), Asset("USD", asset_type="forex")
    snapshot = broker.data_source.get_quote(asset, quote=currency)
    limit = (Decimal(str(snapshot.price)) * Decimal("0.96")).quantize(Decimal("0.01"))
    order = strategy.create_order(asset, Decimal("0.0001"), "buy", limit_price=limit, quote=currency,
                                  time_in_force="day")

    def observe(event):
        if (event.get("client_order_id") == order.identifier
                and event.get("scene_type") == "CANCEL_SUCCESS"):
            received.set()

    thread = Thread(target=feed.run, args=(observe, lambda s: ready.set() if s == "acknowledged" else None))
    thread.start()
    cancel_requested = False
    try:
        assert ready.wait(10), "Webull did not acknowledge the private event subscription"
        strategy.submit_order(order)
        assert order.status != order.OrderStatus.ERROR, order.error_message
        assert _detail(observer, account, order.identifier, lambda r: bool(r.get("order_id")))["side"] == "BUY"
        strategy.cancel_order(order)
        cancel_requested = True
        assert received.wait(10), "No Webull CANCEL_SUCCESS event for the verified crypto order"
    finally:
        try:
            if order.was_transmitted():
                if not cancel_requested:
                    strategy.cancel_order(order)
                r = _detail(observer, account, order.identifier,
                            lambda row: row.get("status") in {"CANCELLED", "CANCELED", "FILLED"})
                if Decimal(r.get("filled_quantity") or 0):
                    closing = strategy.create_order(asset, Decimal(r["filled_quantity"]), "sell", quote=currency,
                                                    time_in_force="ioc", order_type="market")
                    strategy.submit_order(closing)
                    _detail(observer, account, closing.identifier, lambda row: row["status"] == "FILLED")
                    pytest.fail("The passive test order unexpectedly filled and was closed by the test")
        finally:
            feed.close()
            thread.join(3)
            assert not thread.is_alive()


@pytest.mark.parametrize("paper", ["FUTURES"], indirect=True)
def test_paper_dated_futures_limit_lifecycle(paper):
    from datetime import date
    from decimal import ROUND_FLOOR

    from lumibot.tools.webull_helpers import require_number

    broker, observer, account = paper
    data = broker.data_source
    contracts = request_json("dated futures contracts", data.client.instrument.get_futures_instrument_by_code,
                             "MES", "US_FUTURES")
    today = date.today().isoformat()
    dated = sorted((r for r in contracts if r.get("first_trading_date", "~") <= today <
                    r.get("last_trading_date", "") and not r["symbol"].endswith("main")),
                   key=lambda r: r["last_trading_date"])[0]
    asset = Asset(dated["symbol"], asset_type="future", multiplier=dated["size"])
    reference = data.get_quote(asset)
    tick = require_number(dated["min_tick"], "futures tick")
    assert tick > 0 and reference.price > 0 and asset.multiplier > 1
    limit = (Decimal(str(reference.price)) * Decimal("0.96") / tick).to_integral_value(ROUND_FLOOR) * tick
    strategy = Strategy(broker=broker, name="webull_futures_qualification", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    order = strategy.create_order(asset, 1, "buy", limit_price=limit, time_in_force="gtc")
    _qualify_passive_limit(strategy, observer, account, order, "FUTURES")


@pytest.mark.parametrize("paper", ["EVENTS_CASH"], indirect=True)
def test_paper_event_contract_limit_lifecycle(paper):
    from datetime import date

    broker, observer, account = paper
    client = broker.data_source.client
    series = request_json("economic event series", client.instrument.list_event_series,
                          category="ECONOMICS", symbols="KXFEDDECISION")["data"]
    assert len(series) == 1
    markets = request_json("economic event instruments", client.instrument.list_event_instruments,
                           series_symbol=series[0]["symbol"])["data"]
    candidates = [r for r in markets if r.get("tradable_status") == "OC" and
                  r.get("last_trading_date", "") >= date.today().isoformat()]
    for row in candidates[:5]:
        snapshots = request_json("event snapshot", client.event_market_data.get_event_snapshot, row["symbol"])
        if not snapshots:
            continue
        snapshot = snapshots[0]
        if Decimal(snapshot.get("yes_bid") or 0) < Decimal("0.05"):
            continue
        asset = Asset(row["symbol"], asset_type="prediction_contract", outcome="yes")
        strategy = Strategy(broker=broker, name="webull_event_qualification", risk_free_rate=0,
                            synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                            should_send_summary_to_discord=False)
        order = strategy.create_order(asset, 1, "buy", limit_price=Decimal("0.01"), time_in_force="gtc")
        _qualify_passive_limit(strategy, observer, account, order, "EVENT")
        return
    pytest.fail("No listed economic contract with an independently verified passive price")


def _qualify_passive_limit(strategy, observer, account, order, product):
    initial_positions = request_json("independent positions", observer.account_v2.get_account_position, account)
    try:
        strategy.submit_order(order)
        assert order.status != order.OrderStatus.ERROR, order.error_message
        observed = _detail(observer, account, order.identifier, lambda row: bool(row.get("order_id")))
        assert observed["symbol"] == order.asset.symbol
        assert observed["instrument_type"] == product
        assert Decimal(observed["limit_price"]) == order.limit_price
        assert observed["status"] not in {"FAILED", "REJECTED", "EXPIRED"}
    finally:
        if order.was_transmitted():
            strategy.cancel_order(order)
            terminal = _detail(observer, account, order.identifier,
                               lambda row: row.get("status") in {"CANCELLED", "CANCELED", "EXPIRED", "FILLED"})
            exposure = Decimal(terminal.get("filled_quantity") or 0)
            if exposure:
                closing_params = {"limit_price": Decimal("0.01"), "order_type": "limit", "time_in_force": "gtc"}
                if product == "FUTURES":
                    closing_params = {"order_type": "market", "time_in_force": "day"}
                closing = strategy.create_order(order.asset, exposure, "sell", **closing_params)
                strategy.submit_order(closing)
                assert closing.status != closing.OrderStatus.ERROR, closing.error_message
                _detail(observer, account, closing.identifier, lambda row: row["status"] == "FILLED")
                pytest.fail("The passive test limit unexpectedly filled and was closed by the test")
    final_positions = request_json("final independent positions", observer.account_v2.get_account_position, account)
    identity = lambda rows: {(r["symbol"], r.get("event_outcome"), r["quantity"]) for r in rows}
    assert identity(initial_positions) == identity(final_positions)


def test_paper_mqtt_stream_produces_crypto_trade_bars(paper):
    broker, _, _ = paper
    strategy = Strategy(broker=broker, name="webull_data_feed_qualification", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    pair = Asset("BTC", "crypto"), Asset("USD", "forex")
    strategy.start_realtime_bars(pair, keep_bars=5)
    try:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            frame = strategy.get_realtime_bars(pair)
            if frame is not None and not frame.empty:
                assert (frame["volume"] > 0).all()
                assert frame.index.tz is not None
                assert (frame["high"] >= frame["low"]).all()
                assert frame.index.is_monotonic_increasing
                return
            time.sleep(0.5)
        pytest.fail(f"PaperTrade MQTT produced no trade bars (state {broker.data_source._market_stream.state})")
    finally:
        strategy.cancel_realtime_bars(pair)
