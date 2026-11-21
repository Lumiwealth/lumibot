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
            if item.get("client_order_id") == identifier and item.get("status") and predicate(item):
                return item
    pytest.fail("Independent Webull order detail did not reach the expected state")


def test_paper_stock_limit_submit_replace_cancel(paper):
    _qualify_equity_limit_amend(paper, "AAPL", "day")


@pytest.mark.parametrize("paper", ["INDIVIDUAL_MARGIN"], indirect=True)
@pytest.mark.parametrize("symbol", ["AAPL", "SPY"])
def test_paper_gtc_equity_limit_submit_replace_cancel(paper, symbol):
    _qualify_equity_limit_amend(paper, symbol, "gtc")


def _qualify_equity_limit_amend(paper, symbol, duration):
    broker, observer, account = paper
    strategy = Strategy(broker=broker, name="webull_qualification", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    asset = Asset(symbol)
    quote = broker.data_source.get_quote(asset)
    limit = (Decimal(str(quote.price)) * Decimal("0.95")).quantize(Decimal("0.01"))
    replacement = limit - Decimal("0.01")
    order = strategy.create_order(asset, 1, "buy", time_in_force=duration, limit_price=limit,
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
        assert placed["time_in_force"] == duration.upper()
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
    _qualify_crypto_round_trip(paper)


def _qualify_crypto_round_trip(paper, after_buy=None):
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
        if after_buy is not None:
            after_buy(opening, bout)
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
    return closing


def _recover_crypto_fill_in_fresh_process():
    """Called in the child interpreter. Only the test parent receives its output."""
    import sys

    config, checkpoint, identifier, qty, price = json.load(sys.stdin)
    assert config["PAPER"] is True
    callbacks = []

    class RestartedStrategy(Strategy):
        def on_filled_order(self, position, order, price, quantity, multiplier):
            callbacks.append((order.identifier, Decimal(str(quantity)), Decimal(str(price))))

    recovered = Webull(config, connect_stream=False, start_orders_thread=False)
    try:
        strategy = RestartedStrategy(broker=recovered, name=checkpoint["orders"][0]["strategy"], risk_free_rate=0,
                                     synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                                     should_send_summary_to_discord=False)
        executor = strategy._executor
        recovered.restore_order_state(checkpoint)
        order = recovered.get_tracked_order(identifier)
        assert order is not None and not order.transactions
        recovered._poll_tracked_orders()
        recovered._refresh_tracked_order(order)
        executor.process_queue(skip_first_iteration_events=False)
        assert order.is_filled() and len(order.transactions) == 1
        fill = order.transactions[0]
        assert Decimal(str(fill.quantity)) == Decimal(qty)
        assert Decimal(str(fill.price)) == Decimal(price)
        assert callbacks == [(identifier, Decimal(qty), Decimal(price))]
    finally:
        recovered.cleanup_streams()


@pytest.mark.parametrize("paper", ["CRYPTO"], indirect=True)
def test_paper_crypto_gap_restart_and_reconnected_fill_event(paper):
    import subprocess
    import sys
    from contextlib import contextmanager, ExitStack
    from queue import Queue, Empty
    from threading import Event, Thread

    from lumibot.brokers.webull_events import WebullOrderEvents

    broker, _, account = paper
    config = broker._webull_config
    received = Queue()

    @contextmanager
    def subscribed():
        ready = Event()
        feed = WebullOrderEvents(config["APP_KEY"], config["APP_SECRET"], account, paper=True)
        def record(event):
            if event.get("scene_type") == "FINAL_FILLED":
                received.put(event.get("client_order_id"))
        thread = Thread(target=feed.run, args=(record, lambda s: ready.set() if s == "acknowledged" else None))
        thread.start()
        try:
            assert ready.wait(15), "Webull did not acknowledge the private subscription"
            yield
        finally:
            feed.close()
            thread.join(5)
            assert not thread.is_alive()

    # Deliberately leave a blind interval. The buy occurs with both streams down.
    with subscribed():
        pass
    with ExitStack() as streams:
        def recover(opening, independent):
            checkpoint = broker.export_order_state(opening.strategy)
            assert any(row["identifier"] == opening.identifier for row in checkpoint["orders"])
            child = subprocess.run(
                [sys.executable, "-P", "-c",
                 'import runpy, sys; runpy.run_path(sys.argv[1])["_recover_crypto_fill_in_fresh_process"]()',
                 str(Path(__file__).resolve())],
                input=json.dumps((config, checkpoint, opening.identifier, independent["filled_quantity"],
                                  independent["filled_price"]), default=str),
                text=True, capture_output=True, timeout=45, check=False,
            )
            assert child.returncode == 0, "Fresh process did not recover exactly one independently verified fill"
            streams.enter_context(subscribed())

        closing = _qualify_crypto_round_trip(paper, after_buy=recover)
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            try:
                if received.get(timeout=max(0.01, deadline - time.monotonic())) == closing.identifier:
                    return
            except Empty:
                break
        pytest.fail("The private stream did not deliver FINAL_FILLED for the independently settled closing order")


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


@pytest.mark.parametrize("product", ["stock", "crypto", "option"])
def test_webull_daily_strategy_replays_actual_papertrade_bars(paper, monkeypatch, product):
    from datetime import date, datetime
    from zoneinfo import ZoneInfo

    from lumibot.backtesting import WebullHistoryBacktesting

    broker, observer, account = paper
    monkeypatch.delenv("BACKTESTING_DATA_SOURCE", raising=False)

    asset = Asset("AAPL")
    quantity = 1
    if product == "crypto":
        asset = Asset("BTC", asset_type="crypto")
        quantity = Decimal("0.0001")
    elif product == "option":
        asset = Asset("AAPL", asset_type="option", expiration=date(2026, 11, 27), strike=340, right="CALL")
        option_quote = broker.data_source.get_quote(asset)
        assert 0 < option_quote.bid <= option_quote.ask
    quote = Asset("USD", "forex")

    class DailyReplay(Strategy):
        def initialize(self):
            if product == "crypto":
                self.set_market("24/7")
            self.sleeptime = "1D"
            self.trade_count = 0

        def on_trading_iteration(self):
            if self.trade_count < 2:
                if product == "option":
                    side = "buy_to_open" if self.trade_count == 0 else "sell_to_close"
                else:
                    side = "buy" if self.trade_count == 0 else "sell"
                self.submit_order(self.create_order(asset, quantity, side, quote=quote))
                self.trade_count += 1

    ny = ZoneInfo("America/New_York")
    start = datetime(2026, 11, 9, tzinfo=ny)
    end = datetime(2026, 11, 13, tzinfo=ny)
    if product == "option":
        start = datetime(2026, 11, 16, tzinfo=ny)
        end = datetime(2026, 11, 20, tzinfo=ny)
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


def _select_vertical_series(contracts, earliest, latest, spot, minimum_width=0):
    from collections import defaultdict

    expiries = defaultdict(dict)
    for row in contracts:
        expiry = row["expiration_date"]
        if earliest.isoformat() <= expiry <= latest.isoformat():
            expiries[expiry].setdefault(Decimal(row["strike_price"]), row)
    for expiry in sorted(expiries):
        strikes = expiries[expiry]
        if len(strikes) >= 2:
            pairs = [(low, high) for low in strikes for high in strikes if high > low and high - low >= minimum_width]
            if pairs:
                low, high = min(pairs, key=lambda p: (abs(p[0] - spot) + abs(p[1] - spot), p[1] - p[0], p))
                return [strikes[low], strikes[high]]
    pytest.fail("Webull returned no eligible expiration with two distinct call strikes in the requested interval")


def _listed_test_calls(broker, minimum_width=0):
    from datetime import date, timedelta

    underlying = Asset("AAPL")
    last_price = Decimal(str(broker.data_source.get_quote(underlying).price))
    today = date.today()
    through = today + timedelta(days=21)
    contracts = broker.data_source.option_contracts(
        underlying, root_symbol="AAPL", option_type="CALL", end_date=through.isoformat(),
        strike_price_gte=str(last_price - 12), strike_price_lte=str(last_price + 12),
    )
    series = _select_vertical_series(contracts, today + timedelta(days=7), through, last_price, minimum_width)
    return [Asset("AAPL", asset_type="option", right="CALL", expiration=date.fromisoformat(row["expiration_date"]),
                  strike=float(row["strike_price"]), multiplier=float(Decimal(row["multiplier"])))
            for row in series]


@pytest.mark.parametrize("paper", ["INDIVIDUAL_MARGIN"], indirect=True)
@pytest.mark.parametrize("package", ["single", "vertical"])
def test_paper_single_and_vertical_option_orders(paper, package):
    _qualify_option_limit(paper, package, "day")


@pytest.mark.parametrize("paper", ["INDIVIDUAL_MARGIN"], indirect=True)
@pytest.mark.parametrize("package", ["single", "put", "vertical"])
def test_paper_gtc_listed_option_limit_lifecycle(paper, package):
    _qualify_option_limit(paper, package, "gtc")


def _qualify_option_limit(paper, package, duration):
    broker, observer, account = paper
    strategy = Strategy(broker=broker, name="webull_option_qualification", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    assets = _listed_test_calls(broker)

    def verify_and_cancel(order, expected_legs):
        assert order.status != order.OrderStatus.ERROR, order.error_message
        try:
            observed = _detail(observer, account, order.identifier, lambda row: bool(row.get("order_id")))
            assert observed["instrument_type"] == "OPTION"
            assert Decimal(observed["total_quantity"]) == 1
            assert observed["time_in_force"] == duration.upper()
            assert Decimal(observed["limit_price"]) == Decimal("0.05")
            assert observed["status"] not in {"FAILED", "REJECTED", "EXPIRED"}
            def leg_terms(leg):
                return (leg["symbol"], leg["option_expire_date"], Decimal(leg["strike_price"]),
                        leg["option_type"], leg["side"], Decimal(leg["quantity"]))
            expected = [(asset.symbol, asset.expiration.isoformat(), Decimal(str(asset.strike)),
                         str(asset.right).upper(), side, Decimal(1)) for asset, side in expected_legs]
            assert sorted(leg_terms(leg) for leg in observed["legs"]) == sorted(expected)
            if duration == "gtc":
                strategy.modify_order(order, limit_price=Decimal("0.10"), quantity=2)
                modified = _detail(observer, account, order.identifier,
                                   lambda row: Decimal(row.get("limit_price") or 0) == Decimal("0.10")
                                   and Decimal(row.get("total_quantity") or 0) == 2)
                assert modified["order_id"] == observed["order_id"]
                doubled = [(*leg[:-1], leg[-1] * 2) for leg in expected]
                assert sorted(leg_terms(leg) for leg in modified["legs"]) == sorted(doubled)
        finally:
            if order.was_transmitted():
                strategy.cancel_order(order)
                canceled = _detail(observer, account, order.identifier,
                                   lambda row: row["status"] in {"CANCELED", "CANCELLED", "EXPIRED"})
                assert Decimal(canceled["filled_quantity"]) == 0

    if package in {"single", "put"}:
        asset = assets[0]
        if package == "put":
            asset = Asset(asset.symbol, asset_type="option", right="PUT", expiration=asset.expiration,
                          strike=asset.strike, multiplier=asset.multiplier)
            broker.data_source.option_symbol(asset)  # Verify the put is independently listed.
        single = strategy.create_order(asset, 1, "buy_to_open", time_in_force=duration, limit_price=0.05)
        strategy.submit_order(single)
        verify_and_cancel(single, [(asset, "BUY")])
        return

    legs = [strategy.create_order(asset, 1, side, time_in_force=duration)
            for asset, side in zip(assets, ("buy_to_open", "sell_to_open"))]
    [native] = strategy.submit_orders(legs, is_multileg=True, order_type="debit", duration=duration, price=0.05)
    verify_and_cancel(native, [(assets[0], "BUY"), (assets[1], "SELL")])


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


def _listed_micro_future(data):
    from datetime import date

    contracts = request_json("dated futures contracts", data.client.instrument.get_futures_instrument_by_code,
                             "MES", "US_FUTURES")
    today = date.today().isoformat()
    return sorted((r for r in contracts if r.get("first_trading_date", "~") <= today <
                    r.get("last_trading_date", "") and not r["symbol"].endswith("main")),
                   key=lambda r: r["last_trading_date"])[0]


@pytest.mark.parametrize("paper", ["FUTURES"], indirect=True)
def test_paper_dated_futures_limit_lifecycle(paper):
    from decimal import ROUND_FLOOR

    from lumibot.tools.webull_helpers import require_number

    broker, observer, account = paper
    data = broker.data_source
    dated = _listed_micro_future(data)
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
@pytest.mark.parametrize("outcome", ["yes", "no"])
def test_paper_event_contract_limit_lifecycle(paper, outcome):
    broker, observer, account = paper
    asset, _ = _quoted_event_contract(broker.data_source.client, outcome)
    strategy = Strategy(broker=broker, name="webull_event_qualification", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    order = strategy.create_order(asset, 1, "buy", limit_price=Decimal("0.01"), time_in_force="gtc")
    _qualify_passive_limit(strategy, observer, account, order, "EVENT")


def _quoted_event_contract(client, outcome, *, require_two_sided=False):
    from datetime import date

    series = request_json("economic event series", client.instrument.list_event_series,
                          category="ECONOMICS", symbols="KXFEDDECISION")["data"]
    assert len(series) == 1
    markets = request_json("economic event instruments", client.instrument.list_event_instruments,
                           series_symbol=series[0]["symbol"])["data"]
    candidates = [r for r in markets if r.get("tradable_status") == "OC" and
                  r.get("last_trading_date", "") >= date.today().isoformat()]
    executable = []
    for row in candidates[:5]:
        quotes = request_json("event snapshot", client.event_market_data.get_event_snapshot, row["symbol"])
        if not quotes:
            continue
        quote = quotes[0]
        if Decimal(quote.get(f"{outcome}_bid") or 0) < Decimal("0.05"):
            continue
        if require_two_sided:
            if Decimal(quote.get(f"{outcome}_bid_size") or 0) < 1 or Decimal(quote.get(f"{outcome}_ask_size") or 0) < 1:
                continue
            if not 0 < Decimal(quote.get(f"{outcome}_ask") or 0) < 1:
                continue
        asset = Asset(row["symbol"], asset_type="prediction_contract", outcome=outcome)
        if not require_two_sided:
            return asset, quote
        # Prefer a deep, tight two-sided book for an actual matching attempt.
        width = Decimal(quote[f"{outcome}_ask"]) - Decimal(quote[f"{outcome}_bid"])
        depth = min(Decimal(quote[f"{outcome}_ask_size"]), Decimal(quote[f"{outcome}_bid_size"]))
        if width >= 0:
            executable.append((width, -depth, asset, quote))
    if executable:
        _, _, asset, quote = min(executable, key=lambda item: item[:2])
        return asset, quote
    pytest.fail("No eligible listed economic contract with the required quote for this outcome")


@pytest.mark.parametrize("paper", ["EVENTS_CASH"], indirect=True)
@pytest.mark.parametrize("outcome", ["yes", "no"])
def test_paper_event_marketable_limit_round_trip(paper, outcome):
    broker, observer, account = paper
    asset, _ = _quoted_event_contract(broker.data_source.client, outcome, require_two_sided=True)
    strategy = Strategy(broker=broker, name="webull_event_execution", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    def holdings():
        positions = request_json("independent positions", observer.account_v2.get_account_position, account)
        return {(r["symbol"], r.get("event_outcome")): Decimal(r["quantity"]) for r in positions}
    def buying_power():
        balance = request_json("event balance", observer.account_v2.get_account_balance, account)
        usd = next(row for row in balance["account_currency_assets"] if row["currency"] == "USD")
        return Decimal(usd["buying_power"])

    baseline = holdings()
    initial_power = buying_power()
    cap = Decimal("0.99")  # Maximum test outlay is 99 cents of simulated cash.
    buy = strategy.create_order(asset, 1, "buy", limit_price=cap, time_in_force="gtc")
    filled = _submit_and_observe_terminal(strategy, observer, account, buy)
    quantity = Decimal(filled.get("filled_quantity") or 0)
    try:
        assert quantity == 1 and filled["status"] == "FILLED", "Event order did not match before cancellation"
        assert filled["symbol"] == asset.symbol and filled["event_outcome"].lower() == outcome
        assert 0 < Decimal(filled["filled_price"]) <= cap
    finally:
        if quantity:
            floor = Decimal("0.01")
            sell = strategy.create_order(asset, quantity, "sell", limit_price=floor, time_in_force="gtc")
            closed = _submit_and_observe_terminal(strategy, observer, account, sell)
            assert closed["symbol"] == asset.symbol and closed["event_outcome"].lower() == outcome
            assert closed["status"] == "FILLED" and Decimal(closed["filled_quantity"]) == quantity
            assert floor <= Decimal(closed["filled_price"]) <= 1
    assert holdings() == baseline
    fees = sum(Decimal(fee["actual_value"]) for row in (filled, closed) for fee in row["fees"])
    net = quantity * (Decimal(closed["filled_price"]) - Decimal(filled["filled_price"])) - fees
    assert abs(buying_power() - initial_power - net) <= Decimal("0.01")


def _qualify_passive_limit(strategy, observer, account, order, product):
    initial_positions = request_json("independent positions", observer.account_v2.get_account_position, account)
    try:
        strategy.submit_order(order)
        assert order.status != order.OrderStatus.ERROR, order.error_message
        observed = _detail(observer, account, order.identifier, lambda row: bool(row.get("order_id"))
                           and row.get("limit_price") not in (None, "") and row.get("instrument_type") == product)
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
            if product == "EVENT":
                _detail(observer, account, order.identifier, lambda row: row.get("event_outcome") == order.asset.outcome
                        and row.get("status") in {"CANCELED", "CANCELLED", "EXPIRED"})
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


@pytest.mark.parametrize("symbol", ["AAPL", "SPY"])
def test_paper_equity_intraday_market_data_includes_extended_hours(paper, symbol):
    broker, _, _ = paper
    asset = Asset(symbol)
    assert broker.data_source.get_quote(asset).price > 0
    result = broker.data_source.get_historical_prices(asset, 3, timestep="minute", include_after_hours=True)
    assert len(result.df) == 3 and result.df.index.is_monotonic_increasing
    assert result.df.index.tz is not None


def _require_equity_core_session():
    import pandas as pd

    from lumibot.tools.helpers import get_trading_days

    now = pd.Timestamp.now(tz="America/New_York")
    today = now.date().isoformat()
    schedule = get_trading_days("NYSE", today, today)
    if schedule.empty or not schedule.iloc[0]["market_open"] <= now < schedule.iloc[0]["market_close"]:
        pytest.skip("This positive fill and stop-order check needs an eligible US equity core session")


@pytest.mark.parametrize("symbol", ["AAPL", "SPY"])
def test_paper_equity_core_market_round_trip(paper, symbol):
    _require_equity_core_session()
    _qualify_market_round_trip(paper, Asset(symbol))


@pytest.mark.parametrize("paper", ["FUTURES"], indirect=True)
def test_paper_dated_future_market_round_trip(paper):
    dated = _listed_micro_future(paper[0].data_source)
    _qualify_market_round_trip(paper, Asset(dated["symbol"], asset_type="future", multiplier=dated["size"]))


def _qualify_market_round_trip(paper, asset):
    broker, observer, account = paper
    strategy = Strategy(broker=broker, name="webull_market_qualification", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)

    def holdings():
        rows = request_json("independent share positions", observer.account_v2.get_account_position, account)
        return sum((Decimal(row["quantity"]) for row in rows if row["symbol"] == asset.symbol), Decimal(0))

    baseline = holdings()
    opened = strategy.create_order(asset, 1, "buy", order_type="market", time_in_force="day")
    result = _submit_and_observe_terminal(strategy, observer, account, opened)
    bought = Decimal(result.get("filled_quantity") or 0)
    try:
        assert result["status"] == "FILLED" and bought == 1
        assert result["symbol"] == asset.symbol and result["side"] == "BUY"
        assert Decimal(result["filled_price"]) > 0
        assert holdings() - baseline == bought
    finally:
        if bought:
            closed = strategy.create_order(asset, bought, "sell", order_type="market", time_in_force="day")
            final = _submit_and_observe_terminal(strategy, observer, account, closed)
            assert final["status"] == "FILLED" and Decimal(final["filled_quantity"]) == bought
            assert final["symbol"] == asset.symbol and final["side"] == "SELL"
            assert Decimal(final["filled_price"]) > 0
    assert holdings() == baseline


def _submit_and_observe_terminal(strategy, observer, account, order):
    strategy.submit_order(order)
    assert order.status != order.OrderStatus.ERROR, order.error_message
    return _observe_terminal_or_cancel(strategy, observer, account, order)


def _observe_terminal_or_cancel(strategy, observer, account, order):
    terminal = {"FILLED", "CANCELED", "CANCELLED", "EXPIRED", "REJECTED", "FAILED"}
    try:
        return _detail(observer, account, order.identifier, lambda row: row.get("status") in terminal)
    except pytest.fail.Exception:
        strategy.cancel_order(order)
        return _detail(observer, account, order.identifier, lambda row: row.get("status") in terminal)


@pytest.mark.parametrize("type_name,wire_type", [("stop", "STOP_LOSS"), ("stop_limit", "STOP_LOSS_LIMIT")])
def test_paper_equity_core_buy_stop_place_and_cancel(paper, type_name, wire_type):
    _require_equity_core_session()
    _qualify_equity_stop(paper, "AAPL", "day", type_name, wire_type)


@pytest.mark.parametrize("paper", ["INDIVIDUAL_MARGIN"], indirect=True)
@pytest.mark.parametrize("symbol", ["AAPL", "SPY"])
@pytest.mark.parametrize("type_name,wire_type", [("stop", "STOP_LOSS"), ("stop_limit", "STOP_LOSS_LIMIT")])
def test_paper_gtc_equity_stop_place_and_cancel(paper, symbol, type_name, wire_type):
    _qualify_equity_stop(paper, symbol, "gtc", type_name, wire_type)


def _qualify_equity_stop(paper, symbol, duration, type_name, wire_type):
    broker, observer, account = paper
    strategy = Strategy(broker=broker, name="webull_core_stop_qualification", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    asset = Asset(symbol)
    quote = broker.data_source.get_quote(asset)
    stop = (Decimal(str(quote.price)) * Decimal("1.07")).quantize(Decimal("0.01"))
    prices = {"stop_price": stop}
    if type_name == "stop_limit":
        prices["stop_limit_price"] = stop + Decimal("0.01")
    order = strategy.create_order(asset, 1, "buy", order_type=type_name, time_in_force=duration, **prices)
    try:
        strategy.submit_order(order)
        assert order.status != order.OrderStatus.ERROR, order.error_message
        placed = _detail(observer, account, order.identifier, lambda row: bool(row.get("order_id")))
        assert placed["order_type"] == wire_type and Decimal(placed["stop_price"]) == stop
        assert placed["time_in_force"] == duration.upper()
        if "stop_limit_price" in prices:
            assert Decimal(placed["limit_price"]) == prices["stop_limit_price"]
    finally:
        if order.was_transmitted():
            strategy.cancel_order(order)
            accepted_terminal = {"FILLED", "CANCELED", "CANCELLED", "EXPIRED"}
            final = _detail(observer, account, order.identifier, lambda row: row.get("status") in accepted_terminal)
            bought = Decimal(final.get("filled_quantity") or 0)
            if bought:
                closed = strategy.create_order(asset, bought, "sell", order_type="market", time_in_force="day")
                close_status = _submit_and_observe_terminal(strategy, observer, account, closed)
                assert close_status["status"] == "FILLED" and Decimal(close_status["filled_quantity"]) == bought
                pytest.fail("Stop triggered and filled during the passive case. Test exposure closed.")
            assert final["status"] in {"CANCELED", "CANCELLED"}


def _submit_bracket_and_exit():
    import sys
    from lumibot.entities import Order

    config, serialized = json.load(sys.stdin)
    assert config["PAPER"] is True
    broker = Webull(config, connect_stream=False, start_orders_thread=False)
    order = Order.from_dict(serialized)
    strategy = Strategy(broker=broker, name=order.strategy, risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    try:
        strategy.submit_order(order)
        if order.status == order.OrderStatus.ERROR:
            print("BRACKET_REJECTED", order.error_message)
        elif order.was_transmitted():
            print("BRACKET_ACKNOWLEDGED")
    finally:
        broker.cleanup_streams()


@pytest.mark.parametrize("winner", ["target", "stop"])
def test_paper_native_bracket_exit_after_placing_process_ends(paper, tmp_path, winner):
    """An open market must actually trade through the intended exit to pass."""
    import re
    import subprocess
    import sys

    broker, observer, account = paper
    asset = Asset("AAPL")
    strategy = Strategy(broker=broker, name="webull_bracket_exit", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)

    def holding():
        rows = request_json("independent positions", observer.account_v2.get_account_position, account)
        return sum((Decimal(row["quantity"]) for row in rows if row["symbol"] == asset.symbol), Decimal(0))

    baseline = holding()
    quote = broker.data_source.get_quote(asset)
    last = Decimal(str(quote.price))
    cent = Decimal("0.01")
    cap = (last * Decimal("1.001")).quantize(cent)
    target = (last * (Decimal("1.002") if winner == "target" else Decimal("1.05"))).quantize(cent)
    stop = (last * (Decimal("0.998") if winner == "stop" else Decimal("0.95"))).quantize(cent)
    entry = strategy.create_order(asset, 1, "buy", order_type="limit", limit_price=cap,
                                  time_in_force="day", order_class="bracket", secondary_limit_price=target,
                                  secondary_stop_price=stop, custom_params={"support_trading_session": "CORE"})
    for child in entry.child_orders:
        child.time_in_force = "day"
    family = {o.identifier: o for o in [entry, *entry.child_orders]}
    # Keep the IDs for reconciliation even if the child loses its acknowledgment.
    (tmp_path / "bracket-order-ids.json").write_text(json.dumps(list(family)))
    try:
        result = subprocess.run(
            [sys.executable, "-P", "-c",
             'import runpy,sys; runpy.run_path(sys.argv[1])["_submit_bracket_and_exit"]()',
             str(Path(__file__).resolve())],
            input=json.dumps((broker._webull_config, entry.to_dict()), default=str),
            capture_output=True, text=True, timeout=60, check=False,
        )
    except subprocess.TimeoutExpired:
        result = None  # Reconcile the saved identities below before another write.
    output = result.stdout if result is not None else ""
    rejected = re.search(r"^BRACKET_REJECTED .*?\b(OPENAPI_[A-Z_]+)\b", output, re.MULTILINE)
    if rejected:
        pytest.fail(f"Webull rejected native bracket placement: {rejected.group(1)}")
    terminal = {"FILLED", "CANCELLED", "CANCELED", "EXPIRED", "FAILED", "REJECTED"}

    def group():
        observed = request_json("independent native bracket detail", observer.order_v3.get_order_detail,
                                account, entry.identifier)
        rows = {row["client_order_id"]: row for row in observed["orders"]}
        assert set(rows) == set(family), "Inspect the saved client IDs before further orders on this account"
        return rows

    def qty(row):
        return Decimal(row.get("filled_quantity") or 0)

    try:
        assert result is not None and result.returncode == 0 and "BRACKET_ACKNOWLEDGED" in output
        deadline = time.monotonic() + 90
        while True:
            rows = group()
            if all(row["status"] in terminal for row in rows.values()) or time.monotonic() >= deadline:
                break
            time.sleep(3)
        assert all(row.get("order_id") for row in rows.values())
        assert qty(rows[entry.identifier]) == 1
        exits = [rows[child.identifier] for child in entry.child_orders]
        expected = "LIMIT" if winner == "target" else "STOP_LOSS"
        active_exit, sibling = sorted(exits, key=lambda row: row["order_type"] != expected)
        assert active_exit["order_type"] == expected and active_exit["status"] == "FILLED"
        assert qty(active_exit) == 1 and Decimal(active_exit["filled_price"]) > 0
        assert sibling["status"] in {"CANCELED", "CANCELLED"} and qty(sibling) == 0
    finally:
        for identifier in family:
            if group()[identifier]["status"] not in terminal:
                try:
                    broker.cancel_order(family[identifier])
                except WebullAPIError:
                    # The sibling or an in-flight execution can win the cancel race.
                    if group()[identifier]["status"] not in terminal:
                        raise
        for pause in (0, 1, 2, 3, 4):
            time.sleep(pause)
            rows = group()
            if all(row["status"] in terminal for row in rows.values()):
                break
        assert all(row["status"] in terminal for row in rows.values()), "Reconcile working bracket children"
        remaining = qty(rows[entry.identifier]) - sum(qty(rows[c.identifier]) for c in entry.child_orders)
        assert 0 <= remaining <= 1
        if remaining:
            assert holding() == baseline + remaining, "Reconcile independent position before closing"
            close = strategy.create_order(asset, remaining, "sell", order_type="market", time_in_force="day")
            final = _submit_and_observe_terminal(strategy, observer, account, close)
            assert final["status"] == "FILLED" and qty(final) == remaining
        assert holding() == baseline


@pytest.mark.parametrize("paper", ["INDIVIDUAL_MARGIN"], indirect=True)
@pytest.mark.parametrize("package", ["single", "vertical"])
def test_paper_option_marketable_limit_round_trip(paper, package):
    """Open and close a listed contract or vertical atomically with explicit limits."""
    from decimal import ROUND_CEILING

    broker, observer, account = paper
    strategy = Strategy(broker=broker, name="webull_option_round_trip", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    assets = _listed_test_calls(broker, minimum_width=5 if package == "vertical" else 0)[:1 if package == "single" else 2]

    def positions():
        return request_json("independent option positions", observer.account_v2.get_account_position, account)

    assert not positions(), "Use a flat, dedicated options test account"
    assert not request_json("open orders", observer.order_v3.list_order_open, account)["data"]
    quotes = [broker.data_source.get_quote(asset) for asset in assets]
    assert all(q.bid is not None and q.ask is not None and 0 < q.bid <= q.ask for q in quotes)
    # A nickel conforms to both penny and nickel series for the selected underlying.
    tick = Decimal("0.05")
    opening_limit = Decimal(str(quotes[0].ask))
    if package == "vertical":
        opening_limit -= Decimal(str(quotes[1].bid))
    opening_limit = (opening_limit / tick).to_integral_value(rounding=ROUND_CEILING) * tick + tick
    if package == "vertical":
        width = Decimal(str(assets[1].strike)) - Decimal(str(assets[0].strike))
        assert 0 < opening_limit <= width, "No executable debit inside the test spread width"

    def submit(closing=False):
        sides = ["sell_to_close", "buy_to_close"] if closing else ["buy_to_open", "sell_to_open"]
        limit = tick if closing else opening_limit
        if package == "single":
            order = strategy.create_order(assets[0], 1, sides[0], limit_price=limit, time_in_force="day")
            strategy.submit_order(order)
        else:
            legs = [strategy.create_order(asset, 1, side, time_in_force="day") for asset, side in zip(assets, sides)]
            [order] = strategy.submit_orders(legs, is_multileg=True, order_type="credit" if closing else "debit",
                                             price=limit, duration="day")
        assert order.status != order.OrderStatus.ERROR, order.error_message
        return _observe_terminal_or_cancel(strategy, observer, account, order)

    def verify_legs(row, closing):
        assert row["instrument_type"] == "OPTION" and row["status"] == "FILLED"
        assert Decimal(row["filled_quantity"]) == 1 and Decimal(row["filled_price"]) > 0
        terms = {(leg["option_expire_date"], Decimal(leg.get("strike_price") or leg["option_exercise_price"]),
                  leg["option_type"], leg["side"]) for leg in row["legs"]}
        sides = ("SELL", "BUY") if closing else ("BUY", "SELL")
        assert terms == {(a.expiration.isoformat(), Decimal(str(a.strike)), "CALL", side)
                         for a, side in zip(assets, sides)}

    opening = submit()
    acquired = Decimal(opening.get("filled_quantity") or 0)
    try:
        verify_legs(opening, closing=False)
        assert positions(), "Broker reported fills without the expected options position"
    finally:
        if acquired:
            assert acquired == 1, "Inspect the confirmed leg quantities before close"
            closed = submit(closing=True)
            verify_legs(closed, closing=True)
    assert not positions()
    assert not request_json("final open orders", observer.order_v3.list_order_open, account)["data"]


@pytest.mark.parametrize("paper", ["CRYPTO"], indirect=True)
def test_paper_crypto_stop_limit_native_readback_and_cancel(paper):
    broker, observer, account = paper
    asset, quote = Asset("BTC", "crypto"), Asset("USD", "forex")
    strategy = Strategy(broker=broker, name="webull_stop_qualification", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    last = Decimal(str(broker.data_source.get_quote(asset, quote=quote).price))
    cent = Decimal("0.01")
    stop = (last * Decimal("1.06")).quantize(cent)
    cap = stop + cent
    order = strategy.create_order(asset, Decimal("0.0001"), "buy", quote=quote, order_type="stop_limit",
                                  time_in_force="gtc", stop_price=stop, stop_limit_price=cap)
    strategy.submit_order(order)
    assert order.status != order.OrderStatus.ERROR, order.error_message
    try:
        def working(row):
            return bool(row.get("order_id")) and all(row.get(field) for field in ("stop_price", "limit_price")) and row["status"] not in {
                "CANCELED", "CANCELLED", "EXPIRED", "REJECTED", "FAILED", "FILLED"}
        seen = _detail(observer, account, order.identifier, working)
        assert seen["order_type"] == "STOP_LOSS_LIMIT" and seen["time_in_force"] == "GTC"
        assert Decimal(seen["stop_price"]) == stop and Decimal(seen["limit_price"]) == cap
        with pytest.raises(ValueError, match="native crypto replacement"):
            strategy.modify_order(order, limit_price=cap + cent)
        unchanged = _detail(observer, account, order.identifier, working)
        assert unchanged["order_id"] == seen["order_id"] and Decimal(unchanged["limit_price"]) == cap
    finally:
        broker.cancel_order(order)
        final = _detail(observer, account, order.identifier,
                        lambda row: row["status"] in {"CANCELLED", "CANCELED", "FILLED", "EXPIRED"})
        bought = Decimal(final.get("filled_quantity") or 0)
        if bought:
            closing = strategy.create_order(asset, bought, "sell", quote=quote, order_type="market", time_in_force="ioc")
            result = _submit_and_observe_terminal(strategy, observer, account, closing)
            assert result["status"] == "FILLED" and Decimal(result["filled_quantity"]) == bought
        assert final["status"] in {"CANCELED", "CANCELLED"} and bought == 0
