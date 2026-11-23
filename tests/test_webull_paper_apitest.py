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
    if configured:
        path = Path(configured)
        assert not (stat.S_IMODE(path.stat().st_mode) & 0o077)
        config = json.loads(path.read_bytes())
    elif os.environ.get("WEBULL_PAPER", "").lower() == "true":
        config = {name: os.environ.get("WEBULL_" + name) for name in ("APP_KEY", "APP_SECRET", "ACCOUNT_ID")}
        config["PAPER"] = True
    else:
        pytest.skip("Supply credentials for an owned Webull PaperTrade test account")
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


def _detail(observer, account, identifier, predicate, *, pauses=(0, 1, 2, 3, 4)):
    for pause in pauses:
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
@pytest.mark.parametrize("symbol,quantity", [("BTC", "0.0001"), ("ETH", "0.01")])
def test_paper_crypto_market_round_trip(paper, symbol, quantity):
    _qualify_crypto_round_trip(paper, symbol=symbol, quantity=Decimal(quantity))


def _qualify_crypto_round_trip(paper, after_buy=None, *, symbol="BTC", quantity=Decimal("0.0001")):
    broker, observer, account = paper
    strategy = Strategy(broker=broker, name="webull_crypto_qualification", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    asset = Asset(symbol, asset_type="crypto")
    usd = Asset("USD", asset_type="forex")

    def balance():
        return request_json("independent crypto balance", observer.account_v2.get_account_balance, account)

    def position():
        rows = request_json("independent crypto position", observer.account_v2.get_account_position, account)
        return sum((Decimal(r["quantity"]) for r in rows if r["symbol"] in {symbol, symbol + "USD"}), Decimal(0))

    def buying_power():
        return next(Decimal(r["buying_power"]) for r in balance()["account_currency_assets"]
                    if r["currency"] == "USD")

    before_quantity = position()
    before_power = buying_power()
    opening = strategy.create_order(asset, quantity, "buy", quote=usd,
                                    time_in_force="ioc", order_type="market")
    strategy.submit_order(opening)
    assert opening.status != opening.OrderStatus.ERROR, opening.error_message
    bout = _detail(observer, account, opening.identifier,
                   lambda row: row.get("status") in {"FILLED", "CANCELLED", "CANCELED", "REJECTED", "FAILED"})
    bought = Decimal(bout["filled_quantity"])
    try:
        assert bout["status"] == "FILLED"
        assert bought == opening.quantity
        assert bout["symbol"] == symbol + "USD" and bout["side"] == "BUY" and bout["time_in_force"] == "IOC"
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
            assert done["symbol"] == symbol + "USD" and done["side"] == "SELL"
    expected_change = bought * (Decimal(done["filled_price"]) - Decimal(bout["filled_price"]))
    for pause in (0, 1, 2, 3):
        time.sleep(pause)
        if position() == before_quantity and abs(buying_power() - before_power - expected_change) <= Decimal("0.02"):
            break
    else:
        pytest.fail("Confirmed close did not reconcile the initial crypto position and USD buying power")
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


def _native_group(observer, account, identifiers):
    # The detail endpoint accepts a client order ID and can return a single
    # member even when the venue accepted one native three-order group.
    group_id = None
    members = {}
    for identifier in identifiers:
        result = request_json("independent group member", observer.order_v3.get_order_detail, account, identifier)
        current = result.get("combo_order_id")
        assert current and (group_id is None or group_id == current), "Linked legs changed native group"
        group_id = current
        for row in result["orders"]:
            members[row["client_order_id"]] = row
        if set(members) == set(identifiers):
            break
    assert set(members) == set(identifiers)
    return members


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
    _verify_passive_bracket(strategy, observer, account, order)


@pytest.mark.parametrize("child_type", ["stop", "target"])
def test_paper_equity_oto_activates_one_exit(paper, child_type):
    _require_equity_core_session()
    broker, observer, account = paper
    strategy = Strategy(broker=broker, name="webull_oto_qualification", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    asset = Asset("AAPL")

    def held():
        rows = request_json("independent positions", observer.account_v2.get_account_position, account)
        return sum((Decimal(row["quantity"]) for row in rows if row["symbol"] == asset.symbol), Decimal(0))

    baseline = held()
    quote = broker.data_source.get_quote(asset)
    ask, bid = Decimal(str(quote.ask)), Decimal(str(quote.bid))
    assert ask >= bid > 0
    tick = Decimal("0.01")
    entry_limit = (ask * Decimal("1.005")).quantize(tick)
    if child_type == "stop":
        exit_price = (bid * Decimal("0.97")).quantize(tick)
        terms, wire, field = {"secondary_stop_price": exit_price}, "STOP_LOSS", "stop_price"
    else:
        exit_price = (ask * Decimal("1.03")).quantize(tick)
        terms, wire, field = {"secondary_limit_price": exit_price}, "LIMIT", "limit_price"
    parent = strategy.create_order(asset, 1, "buy", limit_price=entry_limit, time_in_force="day",
                                   order_class="oto", **terms)
    child, = parent.child_orders
    strategy.submit_order(parent)
    assert parent.status != parent.OrderStatus.ERROR, parent.error_message
    try:
        placed = _detail(observer, account, parent.identifier, lambda r: r["status"] == "FILLED")
        assert Decimal(placed["filled_quantity"]) == 1 and Decimal(placed["filled_price"]) <= entry_limit
        _detail(observer, account, child.identifier, lambda r: r["status"] in {"OPEN", "SUBMITTED", "WORKING"})
        group = _native_group(observer, account, [parent.identifier, child.identifier])
        exit_row = group[child.identifier]
        assert placed["order_id"] and exit_row["order_id"] and placed["order_id"] != exit_row["order_id"]
        assert exit_row["side"] == "SELL" and exit_row["order_type"] == wire
        assert Decimal(exit_row[field]) == exit_price and Decimal(exit_row["total_quantity"]) == 1
        assert held() == baseline + 1
    finally:
        entry_final = _cancel_then_read(strategy, observer, account, parent)
        exit_final = _cancel_then_read(strategy, observer, account, child)
        residual = Decimal(entry_final.get("filled_quantity") or 0) - Decimal(exit_final.get("filled_quantity") or 0)
        assert residual in (0, 1) and held() == baseline + residual
        if residual:
            close = strategy.create_order(asset, residual, "sell", time_in_force="day", order_type="market")
            filled = _submit_and_observe_terminal(strategy, observer, account, close)
            assert filled["status"] == "FILLED" and Decimal(filled["filled_quantity"]) == residual
        assert held() == baseline


@pytest.mark.parametrize("winner", ["target", "stop"])
def test_paper_oco_exits_an_existing_equity_holding(paper, winner):
    _require_equity_core_session()
    broker, observer, account = paper
    strategy = Strategy(broker=broker, name="webull_oco_qualification", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    asset = Asset("SPY")

    def held():
        rows = request_json("independent positions", observer.account_v2.get_account_position, account)
        return sum((Decimal(row["quantity"]) for row in rows if row["symbol"] == asset.symbol), Decimal(0))

    baseline = held()
    entry = strategy.create_order(asset, 1, "buy", time_in_force="day", order_type="market")
    entered = _submit_and_observe_terminal(strategy, observer, account, entry)
    assert entered["status"] == "FILLED" and Decimal(entered["filled_quantity"]) == 1
    group = None
    try:
        assert held() == baseline + 1
        quote = broker.data_source.get_quote(asset)
        bid, ask = Decimal(str(quote.bid)), Decimal(str(quote.ask))
        cent = Decimal("0.01")
        assert ask >= bid > 0
        # An existing holding can take profit immediately at an executable sell limit.
        target = bid - cent if winner == "target" else ask * Decimal("1.02")
        stop = (min(bid, Decimal(str(quote.price))) - cent if winner == "stop" else bid * Decimal("0.98"))
        group = strategy.create_order(asset, 1, "sell", time_in_force="day", order_class="oco",
                                      limit_price=target.quantize(cent), stop_price=stop.quantize(cent))
        strategy.submit_order(group)
        assert group.status != group.OrderStatus.ERROR, group.error_message
        children = {child.identifier: child for child in group.child_orders}
        assert len(children) == 2
        desired = "LIMIT" if winner == "target" else "STOP_LOSS"
        for delay in (0, 3, 5, 10, 15, 30, 30):
            time.sleep(delay)
            pair = _native_group(observer, account, children)
            if all(r["status"] in {"FILLED", "CANCELLED", "CANCELED"} for r in pair.values()):
                break
        filled, canceled = sorted(pair.values(), key=lambda row: row["order_type"] != desired)
        assert all(r["order_id"] and r["side"] == "SELL" for r in pair.values())
        assert filled["order_type"] == desired and filled["status"] == "FILLED"
        assert Decimal(filled["filled_quantity"]) == 1 and Decimal(filled["filled_price"]) > 0
        assert canceled["status"] in {"CANCELED", "CANCELLED"} and Decimal(canceled["filled_quantity"]) == 0
        assert held() == baseline
    finally:
        if group is not None and group.was_transmitted():
            for child in group.child_orders:
                _cancel_then_read(strategy, observer, account, child)
        working = request_json("independent open orders", observer.order_v3.list_order_open, account)
        assert not working["data"] and not working.get("pagination_key")
        residual = held() - baseline
        assert residual in (0, 1)
        if residual:
            close = strategy.create_order(asset, residual, "sell", time_in_force="day", order_type="market")
            closed = _submit_and_observe_terminal(strategy, observer, account, close)
            assert closed["status"] == "FILLED" and Decimal(closed["filled_quantity"]) == residual
        assert held() == baseline


@pytest.mark.parametrize("paper", ["INDIVIDUAL_MARGIN"], indirect=True)
def test_paper_listed_option_with_protective_exits(paper):
    broker, observer, account = paper
    strategy = Strategy(broker=broker, name="webull_option_protection", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    option = _listed_test_calls(broker)[0]
    quote = broker.data_source.get_quote(option)
    bid = Decimal(str(quote.bid))
    assert bid > Decimal("1") and Decimal(str(quote.ask)) >= bid
    tick = Decimal("0.05")
    entry = ((bid / 2) // tick) * tick
    stop = max(tick, entry - Decimal("0.50"))
    target = entry + Decimal("1.00")
    order = strategy.create_order(option, 1, "buy_to_open", limit_price=entry, time_in_force="day",
                                  order_class="bracket", secondary_limit_price=target, secondary_stop_price=stop)
    _verify_passive_bracket(strategy, observer, account, order)


@pytest.mark.parametrize("paper", ["INDIVIDUAL_MARGIN"], indirect=True)
@pytest.mark.parametrize("kind", ["stop", "stop_limit"])
def test_paper_option_stop_on_existing_contract(paper, kind):
    broker, observer, account = paper
    strategy = Strategy(broker=broker, name="webull_option_stop", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    option = _listed_test_calls(broker)[0]

    def held():
        return sum(p.quantity for p in broker._pull_positions(strategy) if p.asset == option)

    assert held() == 0
    opening = strategy.create_order(option, 1, "buy_to_open", order_type="market", time_in_force="day")
    fill = _submit_and_observe_terminal(strategy, observer, account, opening)
    assert fill["status"] == "FILLED" and Decimal(fill["filled_quantity"]) == 1
    try:
        tick = Decimal("0.05")
        bid = Decimal(str(broker.data_source.get_quote(option).bid))
        risk_price = min(Decimal(fill["filled_price"]), bid)
        assert risk_price > Decimal("1") and held() == 1
        stop = ((risk_price * Decimal("0.75")) // tick) * tick
        terms = {"stop_price": stop}
        if kind == "stop_limit":
            terms["stop_limit_price"] = stop - tick
        protection = strategy.create_order(option, 1, "sell_to_close", order_type=kind, time_in_force="day", **terms)
        try:
            strategy.submit_order(protection)
            assert protection.status != protection.OrderStatus.ERROR, protection.error_message
            row = _detail(observer, account, protection.identifier, lambda r: bool(r.get("order_id")))
            assert row["side"] == "SELL" and row["instrument_type"] == "OPTION"
            assert row["order_type"] == ("STOP_LOSS_LIMIT" if kind == "stop_limit" else "STOP_LOSS")
            assert Decimal(row["stop_price"]) == stop
            if kind == "stop_limit":
                assert Decimal(row["limit_price"]) == stop - tick
        finally:
            if protection.was_transmitted():
                final = _cancel_then_read(strategy, observer, account, protection)
                assert final["status"] in {"CANCELLED", "CANCELED", "FILLED"}
    finally:
        # A rejected cancel can leave the original protection executable.
        working = request_json("check working protection", observer.order_v3.list_order_open, account)
        assert not working.get("pagination_key") and not working["data"]
        balance = held()
        assert balance in (0, 1)
        if balance:
            close = strategy.create_order(option, balance, "sell_to_close", order_type="market", time_in_force="day")
            done = _submit_and_observe_terminal(strategy, observer, account, close)
            assert done["status"] == "FILLED" and Decimal(done["filled_quantity"]) == balance
        assert held() == 0


def _verify_passive_bracket(strategy, observer, account, order):
    for child in order.child_orders:
        child.time_in_force = order.time_in_force
    identifiers = [member.identifier for member in [order, *order.child_orders]]
    try:
        strategy.submit_order(order)
        assert order.status != order.OrderStatus.ERROR, order.error_message
        venue_orders = _native_group(observer, account, identifiers)
        assert len(venue_orders) == 3
        assert all(row.get("order_id") for row in venue_orders.values())
        assert {r["order_type"] for r in venue_orders.values()} == {"LIMIT", "STOP_LOSS"}
        assert venue_orders[order.identifier]["side"] == "BUY"
        assert Decimal(venue_orders[order.identifier]["filled_quantity"]) == 0
        assert all(venue_orders[child.identifier]["side"] == "SELL" for child in order.child_orders)
    finally:
        if order.was_transmitted():
            canceled = _cancel_then_read(strategy, observer, account, order)
            assert canceled["status"] in {"CANCELED", "CANCELLED", "EXPIRED"}
            assert Decimal(canceled["filled_quantity"]) == 0
            ended = _native_group(observer, account, identifiers)
            assert all(r["status"] in {"CANCELED", "CANCELLED", "EXPIRED", "FAILED"}
                       for r in ended.values())


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


def _listed_test_calls(broker, minimum_width=0, *, right="CALL", underlying_symbol="AAPL", minimum_days=7):
    from datetime import date, timedelta

    underlying = Asset(underlying_symbol)
    last_price = Decimal(str(broker.data_source.get_quote(underlying).price))
    today = date.today()
    through = today + timedelta(days=21)
    contracts = broker.data_source.option_contracts(
        underlying, root_symbol=underlying.symbol, option_type=right, end_date=through.isoformat(),
        strike_price_gte=str(last_price - 12), strike_price_lte=str(last_price + 12),
    )
    series = _select_vertical_series(contracts, today + timedelta(days=minimum_days), through, last_price, minimum_width)
    return [Asset(underlying.symbol, asset_type="option", right=right, expiration=date.fromisoformat(row["expiration_date"]),
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
                canceled = _cancel_then_read(strategy, observer, account, order)
                assert canceled["status"] in {"CANCELED", "CANCELLED", "EXPIRED"}
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


def _listed_micro_future(data, product="MES"):
    from datetime import date

    contracts = request_json("dated futures contracts", data.client.instrument.get_futures_instrument_by_code,
                             product, "US_FUTURES")
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
    _qualify_passive_limit(strategy, observer, account, order, "FUTURES", replacement=limit - tick)


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


def _qualify_passive_limit(strategy, observer, account, order, product, *, replacement=None):
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
        if replacement is not None:
            strategy.modify_order(order, limit_price=replacement)
            changed = _detail(observer, account, order.identifier,
                              lambda r: Decimal(r.get("limit_price") or 0) == replacement)
            assert changed["order_id"] == observed["order_id"]
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


@pytest.mark.parametrize("paper", ["CRYPTO"], indirect=True)
def test_paper_mqtt_stream_produces_crypto_trade_bars(paper):
    _qualify_realtime_bars(paper, (Asset("BTC", "crypto"), Asset("USD", "forex")))


@pytest.mark.parametrize("paper", ["FUTURES"], indirect=True)
def test_paper_mqtt_stream_produces_dated_future_trade_bars(paper):
    contract = _listed_micro_future(paper[0].data_source)
    asset = Asset(contract["symbol"], asset_type="future", multiplier=contract["size"])
    _qualify_realtime_bars(paper, asset)


def test_paper_mqtt_stream_produces_equity_trade_bars(paper):
    _qualify_realtime_bars(paper, Asset("AAPL"))


def _qualify_realtime_bars(paper, asset):
    broker, _, _ = paper
    strategy = Strategy(broker=broker, name="webull_data_feed_qualification", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    strategy.start_realtime_bars(asset, keep_bars=5)
    try:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            frame = strategy.get_realtime_bars(asset)
            if frame is not None and not frame.empty:
                assert (frame["volume"] > 0).all()
                assert frame.index.tz is not None
                assert (frame["high"] >= frame["low"]).all()
                assert frame.index.is_monotonic_increasing
                return
            time.sleep(0.5)
        pytest.fail(f"PaperTrade MQTT produced no trade bars (state {broker.data_source._market_stream.state})")
    finally:
        strategy.cancel_realtime_bars(asset)


@pytest.mark.parametrize("symbol", ["AAPL", "SPY"])
def test_paper_equity_intraday_market_data_includes_extended_hours(paper, symbol):
    broker, _, _ = paper
    asset = Asset(symbol)
    assert broker.data_source.get_quote(asset).price > 0
    result = broker.data_source.get_historical_prices(asset, 3, timestep="minute", include_after_hours=True)
    assert len(result.df) == 3 and result.df.index.is_monotonic_increasing
    assert result.df.index.tz is not None


def _require_equity_core_session(now=None):
    import pandas as pd

    from lumibot.tools.helpers import get_trading_days

    now = pd.Timestamp.now(tz="America/New_York") if now is None else now
    today = now.date().isoformat()
    tomorrow = (now + pd.DateOffset(days=1)).date().isoformat()
    schedule = get_trading_days("NYSE", today, tomorrow)
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


@pytest.mark.parametrize("paper", ["FUTURES"], indirect=True)
def test_paper_dated_future_sell_to_open_and_buy_to_cover(paper):
    dated = _listed_micro_future(paper[0].data_source)
    asset = Asset(dated["symbol"], asset_type="future", multiplier=dated["size"])
    _qualify_market_round_trip(paper, asset, short=True)


@pytest.mark.parametrize("symbol", ["AAPL", "SPY"])
def test_paper_fractional_share_quantity_round_trip(paper, symbol):
    _require_equity_core_session()
    _qualify_market_round_trip(paper, Asset(symbol), quantity=Decimal("0.25"))


def _qualify_market_round_trip(paper, asset, *, short=False, quantity=1):
    broker, observer, account = paper
    strategy = Strategy(broker=broker, name="webull_market_qualification", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)

    def holdings():
        rows = request_json("independent share positions", observer.account_v2.get_account_position, account)
        return sum((Decimal(row["quantity"]) for row in rows if row["symbol"] == asset.symbol), Decimal(0))

    baseline = holdings()
    entry_side, exit_side = ("sell", "buy") if short else ("buy", "sell")
    if short and asset.asset_type == Asset.AssetType.STOCK:
        entry_side, exit_side = "sell_short", "buy_to_cover"
    opening_wire_side = {"sell_short": "SHORT"}.get(entry_side, entry_side.upper())
    closing_wire_side = {"buy_to_cover": "BUY"}.get(exit_side, exit_side.upper())
    opened = strategy.create_order(asset, quantity, entry_side, order_type="market", time_in_force="day")
    result = _submit_and_observe_terminal(strategy, observer, account, opened)
    bought = Decimal(result.get("filled_quantity") or 0)
    try:
        assert result["status"] == "FILLED" and bought == quantity
        assert result["symbol"] == asset.symbol and result["side"] == opening_wire_side
        assert Decimal(result["filled_price"]) > 0
        assert holdings() - baseline == (-bought if short else bought)
    finally:
        if bought:
            closed = strategy.create_order(asset, bought, exit_side, order_type="market", time_in_force="day")
            final = _submit_and_observe_terminal(strategy, observer, account, closed)
            assert final["status"] == "FILLED" and Decimal(final["filled_quantity"]) == bought
            assert final["symbol"] == asset.symbol and final["side"] == closing_wire_side
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
        return _cancel_then_read(strategy, observer, account, order)


def _cancel_then_read(strategy, observer, account, order):
    terminal = {"FILLED", "CANCELED", "CANCELLED", "EXPIRED", "REJECTED", "FAILED"}
    for pause in (0, 2, 5, 10, 20, 30, 30, 60, 60):
        time.sleep(pause)
        current = _detail(observer, account, order.identifier, lambda row: bool(row.get("status")))
        if current["status"] in terminal:
            return current
        try:
            strategy.cancel_order(order)
        except WebullAPIError as exc:
            if (current["status"] != "PENDING" or exc.code not in
                    {"OPENAPI_ORDER_CAN_NOT_BE_CANCEL_FOR_PENDING_SUBMIT", "OPENAPI_ORDER_CAN_NOT_BE_CANCEL"}):
                raise
            continue
        return _detail(observer, account, order.identifier, lambda row: row.get("status") in terminal,
                       pauses=(0, 1, 2, 3, 5, 10, 20, 30))
    pytest.fail("Webull still shows this submitted order working; reconcile its original identifier")


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
    reference = max(Decimal(str(value)) for value in (quote.ask, quote.price) if value)
    stop = (reference * Decimal("1.01")).quantize(Decimal("0.01"))
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


@pytest.mark.parametrize("winner", ["target", "stop", "resting"])
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
    cent = Decimal("0.01")
    ask, bid = Decimal(str(quote.ask)), Decimal(str(quote.bid))
    assert ask >= bid > 0
    cap = (ask * Decimal("1.0015")).quantize(cent) if winner == "resting" else (ask + cent).quantize(cent)
    target = cap + cent if winner == "target" else (ask * Decimal("1.015")).quantize(cent)
    last = Decimal(str(quote.price))
    stop_reference = min(bid, last)
    stop = (stop_reference * (Decimal("0.9995") if winner == "stop" else Decimal("0.985"))).quantize(cent)
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
        return _native_group(observer, account, family)

    def qty(row):
        return Decimal(row.get("filled_quantity") or 0)

    try:
        assert result is not None and result.returncode == 0 and "BRACKET_ACKNOWLEDGED" in output
        deadline = time.monotonic() + 90
        while True:
            rows = group()
            if winner == "resting" and rows[entry.identifier]["status"] == "FILLED":
                active = {"SUBMITTED", "WORKING", "OPEN"}
                children = [rows[c.identifier] for c in entry.child_orders]
                if all(r["status"] in active and qty(r) == 0 for r in children):
                    assert holding() == baseline + 1
                    time.sleep(5)
                    repeated = group()
                    for row in children:
                        same = repeated[row["client_order_id"]]
                        assert same["order_id"] == row["order_id"] and same["status"] in active and qty(same) == 0
                    return
            if all(row["status"] in terminal for row in rows.values()) or time.monotonic() >= deadline:
                break
            time.sleep(12)
        assert all(row.get("order_id") for row in rows.values())
        assert qty(rows[entry.identifier]) == 1
        assert winner != "resting", "Two working exits after process shutdown were not observed"
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
@pytest.mark.parametrize("package", ["single", "put", "vertical", "credit_vertical", "put_vertical", "put_credit_vertical"])
def test_paper_option_marketable_limit_round_trip(paper, package):
    """Open and close a listed contract or vertical atomically with explicit limits."""
    from decimal import ROUND_CEILING

    broker, observer, account = paper
    strategy = Strategy(broker=broker, name="webull_option_round_trip", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    spread = "vertical" in package
    right = "PUT" if "put" in package else "CALL"
    assets = _listed_test_calls(broker, minimum_width=5 if spread else 0, right=right)[:2 if spread else 1]

    if right == "PUT" and spread:
        assets.reverse()
    credit_entry = "credit" in package

    def positions():
        return request_json("independent option positions", observer.account_v2.get_account_position, account)

    assert not positions(), "Use a flat, dedicated options test account"
    assert not request_json("open orders", observer.order_v3.list_order_open, account)["data"]
    quotes = [broker.data_source.get_quote(asset) for asset in assets]
    assert all(q.bid is not None and q.ask is not None and 0 < q.bid <= q.ask for q in quotes)
    # A nickel conforms to both penny and nickel series for the selected underlying.
    tick = Decimal("0.05")
    opening_limit = Decimal(str(quotes[0].ask))
    if spread:
        opening_limit -= Decimal(str(quotes[1].bid))
    opening_limit = (opening_limit / tick).to_integral_value(rounding=ROUND_CEILING) * tick + tick
    if spread:
        width = abs(Decimal(str(assets[1].strike)) - Decimal(str(assets[0].strike)))
        assert width > tick
        # Independently quoted legs can have a wider displayed net than the
        # strikes. Place a bounded debit and observe the actual package book.
        opening_limit = min(width - tick, max(tick, opening_limit))

    def submit(closing=False):
        if credit_entry:
            sides = ["buy_to_close", "sell_to_close"] if closing else ["sell_to_open", "buy_to_open"]
            limit = width - tick if closing else tick
        else:
            sides = ["sell_to_close", "buy_to_close"] if closing else ["buy_to_open", "sell_to_open"]
            limit = tick if closing else opening_limit
        if not spread:
            order = strategy.create_order(assets[0], 1, sides[0], limit_price=limit, time_in_force="day")
            strategy.submit_order(order)
        else:
            legs = [strategy.create_order(asset, 1, side, time_in_force="day") for asset, side in zip(assets, sides)]
            [order] = strategy.submit_orders(legs, is_multileg=True, order_type="credit" if closing != credit_entry else "debit",
                                             price=limit, duration="day")
        assert order.status != order.OrderStatus.ERROR, order.error_message
        return order, _observe_terminal_or_cancel(strategy, observer, account, order)

    def verify_legs(row, closing):
        assert row["instrument_type"] == "OPTION" and row["status"] == "FILLED"
        assert Decimal(row["filled_quantity"]) == 1 and Decimal(row["filled_price"]) > 0
        terms = {(leg["option_expire_date"], Decimal(leg.get("strike_price") or leg["option_exercise_price"]),
                  leg["option_type"], leg["side"]) for leg in row["legs"]}
        sides = ("SELL", "BUY") if closing != credit_entry else ("BUY", "SELL")
        assert terms == {(a.expiration.isoformat(), Decimal(str(a.strike)), right, side)
                         for a, side in zip(assets, sides)}

    opening_order, opening = submit()
    acquired = Decimal(opening.get("filled_quantity") or 0)
    try:
        verify_legs(opening, closing=False)
        broker._refresh_tracked_order(opening_order)
        broker._refresh_tracked_order(opening_order)
        assert opening_order.is_filled() and len(opening_order.transactions) == 1
        assert Decimal(str(opening_order.transactions[0].quantity)) == acquired
        assert Decimal(str(opening_order.transactions[0].price)) == Decimal(opening["filled_price"])
        assert positions(), "Broker reported fills without the expected options position"
        # Pull holdings afresh from the venue, as a newly started strategy does.
        units = [-1, 1] if credit_entry else [1, -1]
        actual = broker._pull_positions(strategy)
        assert {p.asset: p.quantity for p in actual} == dict(zip(assets, units))
    finally:
        if acquired:
            assert acquired == 1, "Inspect the confirmed leg quantities before close"
            closing_order, closed = submit(closing=True)
            verify_legs(closed, closing=True)
            broker._refresh_tracked_order(closing_order)
            assert closing_order.is_filled()
    assert not positions()
    assert not request_json("final open orders", observer.order_v3.list_order_open, account)["data"]


@pytest.mark.parametrize("paper", ["INDIVIDUAL_MARGIN"], indirect=True)
@pytest.mark.parametrize("shape", ["STRADDLE", "STRANGLE"])
def test_paper_call_put_package_round_trip(paper, shape):
    from decimal import ROUND_CEILING

    _require_equity_core_session()
    broker, observer, account = paper
    strategy = Strategy(broker=broker, name="webull_call_put_package", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    assert not request_json("initial package holdings", observer.account_v2.get_account_position, account)
    low_call, high_call = _listed_test_calls(broker, minimum_width=2)
    call = high_call if shape == "STRANGLE" else low_call
    expiry = call.expiration.isoformat()
    put_rows = broker.data_source.option_contracts(
        Asset(call.symbol), root_symbol=call.symbol, option_type="PUT", start_date=expiry, end_date=expiry,
        strike_price_gte=str(low_call.strike), strike_price_lte=str(low_call.strike))
    match, = [r for r in put_rows if r["expiration_date"] == expiry and Decimal(r["strike_price"]) == low_call.strike]
    put = Asset(call.symbol, "option", right="PUT", strike=low_call.strike, expiration=call.expiration,
                multiplier=float(Decimal(match["multiplier"])))
    assert call.multiplier == put.multiplier
    quotes = [broker.data_source.get_quote(a) for a in (call, put)]
    assert all(q.ask >= q.bid > 0 for q in quotes)
    tick = Decimal("0.05")
    offers = sum(Decimal(str(q.ask)) for q in quotes)
    cap = (offers / tick).to_integral_value(rounding=ROUND_CEILING) * tick + Decimal("0.50")

    def execute(closing):
        side = "sell_to_close" if closing else "buy_to_open"
        legs = [strategy.create_order(a, 1, side, time_in_force="day") for a in (call, put)]
        parent, = strategy.submit_orders(legs, is_multileg=True, order_type="credit" if closing else "debit",
                                         price=tick if closing else cap, duration="day")
        assert parent.status != parent.OrderStatus.ERROR, parent.error_message
        return _observe_terminal_or_cancel(strategy, observer, account, parent)

    def verify(row, closing):
        assert row["status"] == "FILLED" and row["option_strategy"] == shape
        assert Decimal(row["filled_quantity"]) == 1
        price = abs(Decimal(row["filled_price"]))
        assert (price >= tick if closing else 0 < price <= cap)
        assert len(row["legs"]) == 2
        assert {(r["option_type"], Decimal(r["strike_price"]), r["option_expire_date"], r["side"],
                 Decimal(r["quantity"])) for r in row["legs"]} == {
                     (a.right, Decimal(str(a.strike)), expiry, "SELL" if closing else "BUY", Decimal(1))
                     for a in (call, put)}

    opened = execute(False)
    try:
        verify(opened, False)
        signed = request_json("independent held package", observer.account_v2.get_account_position, account)
        assert len(signed) == 1 and signed[0]["option_strategy"] == shape and Decimal(signed[0]["quantity"]) == 1
        assert {p.asset: p.quantity for p in broker._pull_positions(strategy)} == {call: 1, put: 1}
    finally:
        if Decimal(opened.get("filled_quantity") or 0):
            verify(execute(True), True)
    assert not request_json("final package holdings", observer.account_v2.get_account_position, account)
    assert not request_json("final package orders", observer.order_v3.list_order_open, account)["data"]


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


@pytest.mark.parametrize("paper", ["FUTURES"], indirect=True)
@pytest.mark.parametrize("kind,wire_type", [("stop", "STOP_LOSS"), ("stop_limit", "STOP_LOSS_LIMIT"),
                                            ("trailing_stop", "TRAILING_STOP_LOSS")])
def test_paper_dated_future_conditional_orders(paper, kind, wire_type):
    broker, observer, account = paper
    dated = _listed_micro_future(broker.data_source)
    asset = Asset(dated["symbol"], asset_type="future", multiplier=dated["size"])
    tick = Decimal(str(dated["min_tick"]))
    price = Decimal(str(broker.data_source.get_quote(asset).price))
    stop = (price * Decimal("1.03") / tick).quantize(Decimal(1)) * tick
    terms = {"trail_price": 40 * tick} if kind == "trailing_stop" else {"stop_price": stop}
    if kind == "stop_limit":
        terms["stop_limit_price"] = stop + tick
    strategy = Strategy(broker=broker, name="webull_futures_conditional", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    order = strategy.create_order(asset, 1, "buy", time_in_force="day", **terms)
    try:
        strategy.submit_order(order)
        assert order.status != order.OrderStatus.ERROR, order.error_message
        placed = _detail(observer, account, order.identifier, lambda row: bool(row.get("order_id")))
        assert placed["symbol"] == asset.symbol and placed["order_type"] == wire_type
        if kind == "trailing_stop":
            assert Decimal(placed["trailing_stop_step"]) == terms["trail_price"]
        else:
            assert Decimal(placed["stop_price"]) == stop
        if kind == "stop_limit":
            assert Decimal(placed["limit_price"]) == terms["stop_limit_price"]
    finally:
        if order.was_transmitted():
            strategy.cancel_order(order)
            row = _detail(observer, account, order.identifier, lambda r: r["status"] in
                          {"FILLED", "CANCELED", "CANCELLED", "EXPIRED"})
            quantity = Decimal(row.get("filled_quantity") or 0)
            if quantity:
                close = strategy.create_order(asset, quantity, "sell", time_in_force="day")
                sold = _submit_and_observe_terminal(strategy, observer, account, close)
                assert Decimal(sold["filled_quantity"]) == quantity


@pytest.mark.parametrize("paper,futures,kind", [
    pytest.param("FUTURES", True, "stop", id="stop-future"),
    pytest.param("FUTURES", True, "stop_limit", id="stop_limit-future"),
    pytest.param("FUTURES", True, "trailing_stop", id="trailing_stop-future"),
    pytest.param("INDIVIDUAL_MARGIN", False, "stop", id="stop-call"),
    pytest.param("INDIVIDUAL_MARGIN", False, "stop_limit", id="stop_limit-call"),
], indirect=["paper"])
def test_paper_protective_exit_triggers_on_held_contract(paper, futures, kind):
    """Read the triggered sell against a real one-contract position and reconcile any remainder."""
    broker, observer, account = paper
    strategy = Strategy(broker=broker, name="webull_protective_exit", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    positions = observer.account_v2.get_account_position
    assert not request_json("independent initial positions", positions, account)
    if futures:
        dated = _listed_micro_future(broker.data_source)
        from datetime import date
        asset = Asset(dated["symbol"], "future", expiration=date.fromisoformat(dated["last_trading_date"]),
                      multiplier=dated["size"])
        tick = Decimal(str(dated["min_tick"]))
    else:
        _require_equity_core_session()
        asset = _listed_test_calls(broker)[0]
        tick = Decimal("0.05")
    enter, exit_side = ("buy", "sell") if futures else ("buy_to_open", "sell_to_close")
    entry = strategy.create_order(asset, 1, enter, order_type="market", time_in_force="day")
    opened = _submit_and_observe_terminal(strategy, observer, account, entry)
    exit_order = None
    try:
        assert opened["status"] == "FILLED" and Decimal(opened["filled_quantity"]) == 1
        holding, = request_json("independent opened position", positions, account)
        assert Decimal(holding["quantity"]) == 1
        # A market buy can execute at the offer. Webull validates a sell stop
        # against its market reference, which can already be below that fill.
        reference = Decimal(opened["filled_price"])
        if not futures:
            quote = broker.data_source.get_quote(asset)
            observed = [Decimal(str(p)) for p in (quote.bid, quote.price) if p and p > 0]
            reference = min([reference, *observed])
        stop = (reference // tick - 2) * tick
        assert stop > tick
        terms = {"trail_price": 40 * tick} if kind == "trailing_stop" else {"stop_price": stop}
        if kind == "stop_limit":
            terms["stop_limit_price"] = max(tick, stop - 8 * tick)
        exit_order = strategy.create_order(asset, 1, exit_side, order_type=kind, time_in_force="day", **terms)
        strategy.submit_order(exit_order)
        assert exit_order.status != exit_order.OrderStatus.ERROR, exit_order.error_message
        final = _detail(observer, account, exit_order.identifier, lambda row: row["status"] == "FILLED",
                        pauses=(0, 3, 5, 10, 15, 30, 30))
        assert final["side"] == "SELL" and Decimal(final["filled_quantity"]) == 1
        assert final["order_type"] == {"stop": "STOP_LOSS", "stop_limit": "STOP_LOSS_LIMIT",
                                       "trailing_stop": "TRAILING_STOP_LOSS"}[kind]
        if kind == "trailing_stop":
            assert final["trailing_type"] == "AMOUNT" and Decimal(final["trailing_stop_step"]) == terms["trail_price"]
        else:
            assert Decimal(final["stop_price"]) == stop
        if kind == "stop_limit":
            assert Decimal(final["limit_price"]) == terms["stop_limit_price"]
            assert Decimal(final["filled_price"]) >= terms["stop_limit_price"]
        assert not request_json("independent position after trigger", positions, account)
    finally:
        if exit_order is not None and exit_order.was_transmitted():
            _cancel_then_read(strategy, observer, account, exit_order)
        working = request_json("independent working orders before cleanup", observer.order_v3.list_order_open, account)
        assert not working["data"] and not working.get("pagination_key")
        remaining = request_json("independent residual positions", positions, account)
        if remaining:
            row, = remaining
            parsed = broker._pull_positions(strategy)
            assert len(parsed) == 1 and parsed[0].asset == asset and parsed[0].quantity == Decimal(row["quantity"]) == 1
            close = strategy.create_order(asset, 1, exit_side, order_type="market", time_in_force="day")
            closed = _submit_and_observe_terminal(strategy, observer, account, close)
            assert closed["status"] == "FILLED" and Decimal(closed["filled_quantity"]) == 1
        assert not request_json("independent final positions", positions, account)


@pytest.mark.parametrize("kind", ["stop", "stop_limit"])
def test_paper_equity_stop_triggers_and_closes(paper, kind):
    _require_equity_core_session()
    broker, observer, account = paper
    asset = Asset("AAPL")
    strategy = Strategy(broker=broker, name="webull_stop_execution", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    quote = broker.data_source.get_quote(asset)
    cent = Decimal("0.01")
    reference = max(Decimal(str(value)) for value in (quote.price, quote.ask) if value)
    # Allow for the quote changing during transmission while keeping the
    # condition close enough to observe an actual trigger in a liquid session.
    stop = (reference * Decimal("1.0005")).quantize(cent)
    terms = {"stop_price": stop}
    if kind == "stop_limit":
        terms["stop_limit_price"] = stop + Decimal("0.15")
    order = strategy.create_order(asset, 1, "buy", time_in_force="day", **terms)
    strategy.submit_order(order)
    assert order.status != order.OrderStatus.ERROR, order.error_message
    try:
        final = _detail(observer, account, order.identifier,
                        lambda r: r["status"] in {"FILLED", "CANCELLED", "FAILED", "EXPIRED"},
                        pauses=(0, 3, 5, 10, 15, 30, 30, 30))
    except pytest.fail.Exception:
        final = _cancel_then_read(strategy, observer, account, order)
    qty = Decimal(final.get("filled_quantity") or 0)
    try:
        assert Decimal(final["stop_price"]) == stop
        assert final["status"] == "FILLED" and qty == 1, "Stop did not fill during this observation"
        if kind == "stop_limit":
            assert Decimal(final["limit_price"]) == terms["stop_limit_price"]
            assert Decimal(final["filled_price"]) <= terms["stop_limit_price"]
    finally:
        if qty:
            close = strategy.create_order(asset, qty, "sell", time_in_force="day")
            closed = _submit_and_observe_terminal(strategy, observer, account, close)
            assert Decimal(closed["filled_quantity"]) == qty


@pytest.mark.parametrize("symbol,duration", [("AAPL", "day"), ("SPY", "gtc")])
def test_paper_extended_equity_marketable_limit_round_trip(paper, symbol, duration):
    import pandas as pd
    from lumibot.tools.helpers import get_trading_days

    now = pd.Timestamp.now(tz="America/New_York")
    sessions = get_trading_days("NYSE", now.date().isoformat(), (now + pd.Timedelta(days=1)).date().isoformat())
    if sessions.empty or sessions.iloc[0]["market_open"].date() != now.date():
        pytest.skip("An eligible extended US equity session is required")
    core = sessions.iloc[0]
    start, end = ((now.normalize() + pd.Timedelta(hours=4), core.market_open)
                  if now < core.market_open else (core.market_close, now.normalize() + pd.Timedelta(hours=20)))
    if not start <= now < end:
        pytest.skip("Run during the premarket or postmarket to qualify an extended-session execution")
    broker, observer, account = paper
    asset = Asset(symbol)
    strategy = Strategy(broker=broker, name="webull_extended_limit", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)

    def submit(side, quantity):
        quote = broker.data_source.get_quote(asset)
        price = quote.ask if side == "buy" else quote.bid
        assert price and price > 0
        allowance = Decimal("1.005") if side == "buy" else Decimal("0.995")
        limit = (Decimal(str(price)) * allowance).quantize(Decimal("0.01"))
        order = strategy.create_order(asset, quantity, side, limit_price=limit, time_in_force=duration,
                                      custom_params={"support_trading_session": "ALL"})
        return _submit_and_observe_terminal(strategy, observer, account, order)

    def verify_session(result):
        assert result["support_trading_session"] == "ALL" and result["time_in_force"] == duration.upper()
        stamp = pd.Timestamp(result["filled_time_at"])
        assert stamp.tzinfo is not None and start <= stamp < end

    initial = broker._pull_position(strategy, asset)
    result = submit("buy", 1)
    quantity = Decimal(result.get("filled_quantity") or 0)
    try:
        assert result["status"] == "FILLED" and quantity == 1
        verify_session(result)
        current = broker._pull_position(strategy, asset)
        assert current.quantity == (initial.quantity if initial else 0) + quantity
    finally:
        if quantity:
            final = submit("sell", quantity)
            assert final["status"] == "FILLED" and Decimal(final["filled_quantity"]) == quantity
            verify_session(final)
    assert (broker._pull_position(strategy, asset) or initial) == initial


@pytest.mark.parametrize("product", ["stock", "etf", "future", "gold", "option", "crypto", "ether"])
def test_webull_minute_history_replays_twice_without_network(paper, monkeypatch, product):
    """Select actual vendor bars, preload once and compare two ordinary strategy runs."""
    from datetime import date
    import pandas as pd
    import pandas_market_calendars as mcal
    import requests
    from lumibot.backtesting import PandasDataBacktesting
    from lumibot.entities import Data

    broker, _, _ = paper
    source = broker.data_source
    now = pd.Timestamp.now(tz="America/New_York")
    sessions = mcal.get_calendar("NYSE").schedule(now.date() - pd.Timedelta(days=12), now.date())
    start = sessions.loc[sessions.market_close < now, "market_open"].iloc[-1] + pd.Timedelta(minutes=30)
    end = start + pd.Timedelta(hours=1)
    quote = Asset("USD", "forex")
    if product in {"future", "gold"}:
        terms = _listed_micro_future(source, "MGC" if product == "gold" else "MES")
        asset = Asset(terms["symbol"], "future", expiration=date.fromisoformat(terms["last_trading_date"]),
                      multiplier=terms["size"])
    elif product == "option":
        asset = _listed_test_calls(broker, underlying_symbol="SPY", minimum_days=1)[0]
    elif product in {"crypto", "ether"}:
        asset = Asset("ETH" if product == "ether" else "BTC", "crypto")
        # The PaperTrade crypto endpoint returns the latest N minutes. Select
        # a recently completed interval inside that retention window.
        end = now.floor("min") - pd.Timedelta(minutes=5)
        start = end - pd.Timedelta(minutes=60)
    else:
        asset = Asset("SPY" if product == "etf" else "AAPL")
    bars = source.get_minute_range(asset, start, end, quote=quote)
    assert bars is not None and len(bars) > 5 and bars.index.is_unique and bars.index.is_monotonic_increasing
    assert bars.index[0] >= start and bars.index[-1] + pd.Timedelta(minutes=1) <= end
    assert bars[["open", "high", "low", "close"]].gt(0).all().all()

    class MinuteReplay(Strategy):
        def initialize(self):
            self.sleeptime = "1M"
            if product in {"crypto", "ether"}:
                self.set_market("24/7")
            self.steps = 0

        def on_trading_iteration(self):
            if self.steps < 2:
                side = ("buy", "sell")[self.steps]
                self.submit_order(self.create_order(asset, 1, side, quote=quote))
                self.steps += 1

    def reject_network(*args, **kwargs):
        pytest.fail("Unexpected network request during minute replay")

    def replay():
        _, strategy = MinuteReplay.run_backtest(
            PandasDataBacktesting, (bars.index[1]).to_pydatetime(), bars.index[-1].to_pydatetime(),
            pandas_data={(asset, quote): Data(asset, bars.copy(), timestep="minute", quote=quote)},
            budget=100_000, benchmark_asset=None, risk_free_rate=0, analyze_backtest=False, save_stats_file=False,
            save_tearsheet=False, show_plot=False, show_progress_bar=False, show_tearsheet=False,
        )
        orders = strategy.broker.get_all_orders()
        assert len(orders) == 2 and all(o.is_filled() and o.avg_fill_price > 0 for o in orders)
        return [(o.side, o.quantity, o.avg_fill_price) for o in orders], strategy.get_cash()

    with monkeypatch.context() as offline:
        offline.setattr(requests.Session, "send", reject_network)
        assert replay() == replay()



@pytest.mark.parametrize("paper", ["INDIVIDUAL_MARGIN"], indirect=True)
@pytest.mark.parametrize("symbol", ["AAPL", "SPY"])
def test_paper_equity_short_sale_and_cover(paper, symbol):
    _require_equity_core_session()
    _qualify_market_round_trip(paper, Asset(symbol), short=True)


@pytest.mark.parametrize("paper", ["INDIVIDUAL_MARGIN"], indirect=True)
@pytest.mark.parametrize("right", ["CALL", "PUT"])
def test_paper_listed_option_market_entry_and_exit(paper, right):
    _require_equity_core_session()
    broker, observer, account = paper
    strategy = Strategy(broker=broker, name="option_market_round_trip", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    assert not request_json("independent positions", observer.account_v2.get_account_position, account)
    asset = _listed_test_calls(broker, right=right)[0]
    entry = strategy.create_order(asset, 1, "buy_to_open", order_type="market", time_in_force="day")
    opened = _submit_and_observe_terminal(strategy, observer, account, entry)
    acquired = Decimal(opened.get("filled_quantity") or 0)
    try:
        assert opened["status"] == "FILLED" and acquired == 1
        assert opened["legs"][0]["option_type"] == right
        assert opened["order_type"] == "MARKET" and Decimal(opened["filled_price"]) > 0
        assert broker._pull_position(strategy, asset).quantity == acquired
    finally:
        if acquired:
            exit_order = strategy.create_order(asset, acquired, "sell_to_close", order_type="market",
                                               time_in_force="day")
            strategy.submit_order(exit_order)
            if exit_order.status == exit_order.OrderStatus.ERROR:
                # A definitively rejected close leaves the test contract open.
                # Sell only that confirmed holding with an explicit marketable limit.
                calls = broker._pull_position(strategy, asset)
                assert calls is not None and calls.quantity == acquired
                fallback = strategy.create_order(asset, acquired, "sell_to_close", limit_price=Decimal("0.05"),
                                                 time_in_force="day")
                finished = _submit_and_observe_terminal(strategy, observer, account, fallback)
                assert Decimal(finished["filled_quantity"]) == acquired
                pytest.fail(f"Option market close rejected: {exit_order.error_message}")
            closed = _observe_terminal_or_cancel(strategy, observer, account, exit_order)
            assert closed["status"] == "FILLED" and Decimal(closed["filled_quantity"]) == acquired
    assert not request_json("final independent positions", observer.account_v2.get_account_position, account)


@pytest.mark.parametrize("paper", ["INDIVIDUAL_MARGIN"], indirect=True)
@pytest.mark.parametrize("exit_method", ["package", "legs"])
def test_paper_listed_call_butterfly_ratio_round_trip(paper, exit_method):
    from datetime import date

    broker, observer, account = paper
    strategy = Strategy(broker=broker, name="webull_butterfly", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    assert not request_json("independent initial positions", observer.account_v2.get_account_position, account)
    underlying = Asset("AAPL")
    reference = _listed_test_calls(broker)[0]
    spot = Decimal(str(broker.data_source.get_quote(underlying).price))
    expiry = reference.expiration.isoformat()
    chain = broker.data_source.option_contracts(underlying, root_symbol=underlying.symbol, option_type="CALL",
                                               start_date=expiry, end_date=expiry)
    strikes = {Decimal(row["strike_price"]) for row in chain if row["expiration_date"] == expiry}
    wings = [(k - w, k, k + w) for k in strikes for w in (Decimal("5"), Decimal("2.5"))
             if k - w in strikes and k + w in strikes]
    assert wings, "No symmetric listed strikes for the selected expiry"
    low, middle, high = min(wings, key=lambda s: (abs(s[1] - spot), s[2] - s[0]))
    assets = [Asset(underlying.symbol, "option", right="CALL", expiration=date.fromisoformat(expiry),
                    strike=float(k), multiplier=reference.multiplier) for k in (low, middle, high)]

    def place(closing):
        sides = ("sell_to_close", "buy_to_close", "sell_to_close") if closing else (
            "buy_to_open", "sell_to_open", "buy_to_open")
        legs = [strategy.create_order(asset, qty, side, time_in_force="day", custom_params={"option_strategy": "BUTTERFLY"})
                for asset, qty, side in zip(assets, (1, 2, 1), sides)]
        [parent] = strategy.submit_orders(legs, is_multileg=True, order_type="credit" if closing else "debit",
                                          price=Decimal("0.01") if closing else middle - low - Decimal("0.05"),
                                          duration="day")
        assert parent.status != parent.OrderStatus.ERROR, parent.error_message
        if closing:
            try:
                return _detail(observer, account, parent.identifier, lambda row: row.get("status") == "FILLED",
                               pauses=(0, 3, 5, 10, 15, 20, 30, 30))
            except pytest.fail.Exception:
                return _cancel_then_read(strategy, observer, account, parent)
        return _observe_terminal_or_cancel(strategy, observer, account, parent)

    def verify(row, closing):
        assert row["status"] == "FILLED" and row["option_strategy"] == "BUTTERFLY"
        assert Decimal(row["filled_quantity"]) == 1 and len(row["legs"]) == 3
        observed = sorted((Decimal(leg["strike_price"]), leg["side"], Decimal(leg["quantity"])) for leg in row["legs"])
        assert observed == [(low, "SELL" if closing else "BUY", 1), (middle, "BUY" if closing else "SELL", 2),
                            (high, "SELL" if closing else "BUY", 1)]

    def exit_legs():
        # Repurchase the two short middle contracts first. Webull can then
        # present each remaining long wing as a separately owned SINGLE.
        for index, qty, side in [(1, 2, "buy_to_close"), (0, 1, "sell_to_close"), (2, 1, "sell_to_close")]:
            assert not request_json("working orders before leg close", observer.order_v3.list_order_open, account)["data"]
            positions = request_json("independent holding before leg close", observer.account_v2.get_account_position,
                                     account)
            def matches(leg):
                return (leg["symbol"] == underlying.symbol and leg["option_expire_date"] == expiry
                        and leg["option_type"] == "CALL" and Decimal(leg["option_exercise_price"]) == assets[index].strike)
            selected = [row for row in positions if any(matches(leg) for leg in row["legs"])]
            assert len(selected) == 1
            row = selected[0]
            terms = {}
            if row["option_strategy"] != "SINGLE":
                terms = {"position_id": row["position_id"], "custom_params": {"leg_in_or_out": "LEG_OUT"}}
            else:
                assert Decimal(row["quantity"]) == (qty if side == "sell_to_close" else -qty)
            order = strategy.create_order(assets[index], qty, side, order_type="market", time_in_force="day", **terms)
            result = _submit_and_observe_terminal(strategy, observer, account, order)
            assert result["status"] == "FILLED" and Decimal(result["filled_quantity"]) == qty

    opened = place(False)
    acquired = Decimal(opened.get("filled_quantity") or 0)
    try:
        verify(opened, False)
        rows = request_json("independent package holdings", observer.account_v2.get_account_position, account)
        assert len(rows) == 1 and Decimal(rows[0]["quantity"]) == 1
        assert len(rows[0]["legs"]) == 3
        assert {p.asset: p.quantity for p in broker._pull_positions(strategy)} == dict(zip(assets, (1, -2, 1)))
    finally:
        if acquired:
            assert acquired == 1
            if exit_method == "legs":
                exit_legs()
            else:
                closed = place(True)
                if closed["status"] in {"CANCELLED", "CANCELED", "EXPIRED"} and not Decimal(closed["filled_quantity"]):
                    exit_legs()
                    pytest.fail("The native package close did not fill. Confirmed holdings were closed by leg.")
                verify(closed, True)
    assert not request_json("independent final positions", observer.account_v2.get_account_position, account)


@pytest.mark.parametrize("call_strike", [340, 345], ids=["straddle", "strangle"])
def test_webull_daily_call_put_package_replay(paper, monkeypatch, call_strike):
    """Real per-contract bars, atomic fills, independent premium arithmetic and offline replay."""
    from datetime import date, datetime
    from functools import lru_cache
    from zoneinfo import ZoneInfo

    import requests
    from webull.trade.trade_client import TradeClient

    from lumibot.backtesting import WebullHistoryBacktesting

    source = paper[0].data_source
    monkeypatch.delenv("BACKTESTING_DATA_SOURCE", raising=False)
    expiry = date(2026, 11, 27)
    assets = [Asset("AAPL", "option", right=right, strike=k, expiration=expiry)
              for right, k in (("CALL", call_strike), ("PUT", 340))]
    usd = Asset("USD", "forex")
    ny = ZoneInfo("America/New_York")

    class PackageReplay(Strategy):
        def initialize(self):
            self.sleeptime = "1D"
            self.packages = []

        def on_trading_iteration(self):
            if len(self.packages) == 2 or (self.packages and not self.packages[0].is_filled()):
                return
            closing = bool(self.packages)
            side = "sell_to_close" if closing else "buy_to_open"
            legs = [self.create_order(asset, 1, side, quote=usd) for asset in assets]
            parent, = self.submit_orders(legs, is_multileg=True, order_type="credit" if closing else "debit",
                                         price=0.05 if closing else 30, duration="gtc")
            self.packages.append(parent)

    def reject_network(*args, **kwargs):
        pytest.fail("Unexpected network access during replay")

    def reuse_frame(reader):
        cached = lru_cache(maxsize=None)(reader)

        def read(*args):
            frame = cached(*args)
            assert not frame.empty
            return frame.copy()
        return read

    def replay():
        _, strategy = PackageReplay.run_backtest(
            WebullHistoryBacktesting, datetime(2026, 11, 16, tzinfo=ny), datetime(2026, 11, 21, tzinfo=ny),
            config={"data_source": source}, budget=25_000, benchmark_asset=None, risk_free_rate=0,
            analyze_backtest=False, save_stats_file=False, save_tearsheet=False,
            show_plot=False, show_progress_bar=False, show_tearsheet=False,
        )
        assert len(strategy.packages) == 2
        log = strategy.broker._trade_event_log_df
        journal = []
        cash = Decimal(25_000)
        for parent, side in zip(strategy.packages, ("buy_to_open", "sell_to_close")):
            assert parent.is_filled() and {leg.asset for leg in parent.child_orders} == set(assets)
            assert len(parent.child_orders) == 2
            prices = []
            for leg in parent.child_orders:
                assert leg.is_filled() and leg.quantity == 1 and leg.side == side and leg.avg_fill_price > 0
                premium = Decimal(str(leg.avg_fill_price)) * Decimal(str(leg.asset.multiplier))
                cash += premium if side == "sell_to_close" else -premium
                prices.append((str(leg.asset), side, leg.avg_fill_price))
            ids = [leg.identifier for leg in parent.child_orders]
            stamps = log.loc[log["identifier"].isin(ids) & (log["status"] == "fill"), "time"]
            assert len(stamps) == 2 and stamps.nunique() == 1
            journal.append((stamps.iloc[0], sorted(prices)))
        assert journal[0][0] < journal[1][0]
        assert all(p.quantity == 0 for p in strategy.get_positions() if p.asset in assets)
        assert abs(Decimal(str(strategy.get_cash())) - cash) < Decimal("0.01")
        return journal, strategy.get_cash(), strategy.get_portfolio_value()

    monkeypatch.setattr(TradeClient, "__init__", reject_network)
    for method in ("get_option_daily_range", "get_equity_daily_range"):
        monkeypatch.setattr(source, method, reuse_frame(getattr(source, method)))
    cold = replay()
    with monkeypatch.context() as offline:
        offline.setattr(requests.Session, "send", reject_network)
        assert replay() == cold


@pytest.mark.parametrize("unit,step", [("AMOUNT", "0.05"), ("PERCENTAGE", "0.01")])
def test_paper_equity_day_trailing_terms_and_terminal_state(paper, unit, step):
    """Prove the native unit and amount. The terminal read identifies a fill or a canceled resting trail."""
    _require_equity_core_session()
    broker, observer, account = paper
    strategy = Strategy(broker=broker, name="webull_trailing_terms", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    asset = Asset("AAPL")
    positions = observer.account_v2.get_account_position
    assert not request_json("initial positions for trailing test", positions, account)
    entry = strategy.create_order(asset, 1, "buy", order_type="market", time_in_force="day")
    opened = _submit_and_observe_terminal(strategy, observer, account, entry)
    trail = None
    try:
        assert opened["status"] == "FILLED" and Decimal(opened["filled_quantity"]) == 1
        terms = {"trail_price" if unit == "AMOUNT" else "trail_percent": Decimal(step)}
        trail = strategy.create_order(asset, 1, "sell", order_type="trailing_stop", time_in_force="day", **terms)
        strategy.submit_order(trail)
        assert trail.status != trail.OrderStatus.ERROR, trail.error_message
        observed = _detail(observer, account, trail.identifier, lambda r: bool(r.get("order_id")))
        assert observed["order_type"] == "TRAILING_STOP_LOSS" and observed["time_in_force"] == "DAY"
        assert observed["trailing_type"] == unit and Decimal(observed["trailing_stop_step"]) == Decimal(step)
        terminal = _observe_terminal_or_cancel(strategy, observer, account, trail)
        assert terminal["status"] in {"FILLED", "CANCELED", "CANCELLED"}
        if terminal["status"] == "FILLED":
            assert Decimal(terminal["filled_quantity"]) == 1 and Decimal(terminal["filled_price"]) > 0
            assert not request_json("position after trailing fill", positions, account)
        else:
            assert Decimal(terminal.get("filled_quantity") or 0) == 0
    finally:
        if trail is not None and trail.was_transmitted():
            _cancel_then_read(strategy, observer, account, trail)
        working = request_json("working orders after trailing test", observer.order_v3.list_order_open, account)
        assert not working["data"] and not working.get("pagination_key")
        remaining = request_json("remaining position after trailing test", positions, account)
        if remaining:
            row, = remaining
            assert row["symbol"] == asset.symbol and Decimal(row["quantity"]) == 1
            closed = _submit_and_observe_terminal(strategy, observer, account, strategy.create_order(
                asset, 1, "sell", order_type="market", time_in_force="day"))
            assert closed["status"] == "FILLED" and Decimal(closed["filled_quantity"]) == 1
        assert not request_json("final positions after trailing test", positions, account)


def test_paper_stock_market_buy_by_cash_amount(paper):
    """The venue derives shares from dollars, and the strategy closes those actual shares."""
    _require_equity_core_session()
    broker, observer, account = paper
    strategy = Strategy(broker=broker, name="webull_stock_amount", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    asset = Asset("AAPL")
    amount = Decimal("25")
    positions = observer.account_v2.get_account_position
    assert not request_json("independent initial positions", positions, account)

    entry = strategy.create_order(asset, 0, "buy", order_type="market", time_in_force="day",
                                  custom_params={"total_cash_amount": amount})
    result = _submit_and_observe_terminal(strategy, observer, account, entry)
    shares = Decimal(result.get("filled_quantity") or 0)
    try:
        assert result["status"] == "FILLED" and result["entrust_type"] == "AMOUNT"
        assert 0 < shares < 1 and Decimal(result["total_quantity"]) == shares
        price = Decimal(result["filled_price"])
        cost = shares * price
        # The simulator allocates fractional shares in four-decimal units.
        # Allocation rounding is measured in shares, then converted to cash.
        assert price > 0 and abs(cost - amount) <= price * Decimal("0.0001")
        holding, = request_json("independent fractional holding", positions, account)
        assert holding["symbol"] == asset.symbol and Decimal(holding["quantity"]) == shares
        broker._refresh_tracked_order(entry)
        broker._refresh_tracked_order(entry)
        assert entry.is_filled() and entry.quantity == entry.filled_quantity == shares
        assert sum(Decimal(str(t.quantity)) for t in entry.transactions) == shares
    finally:
        if shares:
            exit_order = strategy.create_order(asset, shares, "sell", order_type="market", time_in_force="day")
            cleared = _submit_and_observe_terminal(strategy, observer, account, exit_order)
            assert cleared["status"] == "FILLED" and Decimal(cleared["filled_quantity"]) == shares
        assert not request_json("independent final positions", positions, account)
        working = request_json("independent final working orders", observer.order_v3.list_order_open, account)
        assert not working["data"] and not working.get("pagination_key")


def _read_dated_future_in_new_process():
    import sys

    config, serialized = json.load(sys.stdin)
    assert config["PAPER"] is True
    fresh = Webull(config, connect_stream=False, start_orders_thread=False)
    try:
        strategy = Strategy(broker=fresh, name="webull_future_identity", risk_free_rate=0,
                            synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                            should_send_summary_to_discord=False)
        asset = Asset.from_dict(serialized)
        position = strategy.get_position(asset)
        assert position is not None and position.asset == asset and position.quantity == 1
        assert position.asset.multiplier == asset.multiplier
        symbol_only = Asset(asset.symbol, "future")
        assert strategy.get_position(symbol_only, broker_refresh=False) is position
    finally:
        fresh.cleanup_streams()


@pytest.mark.parametrize("paper", ["FUTURES"], indirect=True)
@pytest.mark.parametrize("product", ["MES", "MGC"])
def test_paper_dated_future_lookup_after_restart(paper, product):
    import subprocess
    import sys

    broker, observer, account = paper
    terms = _listed_micro_future(broker.data_source, product)
    asset = Asset(terms["symbol"], "future", expiration=terms["last_trading_date"], multiplier=terms["size"])
    positions = observer.account_v2.get_account_position
    assert not request_json("initial futures positions", positions, account)
    strategy = Strategy(broker=broker, name="webull_future_identity", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    entry = strategy.create_order(asset, 1, "buy", time_in_force="day")
    placed = _submit_and_observe_terminal(strategy, observer, account, entry)
    quantity = Decimal(placed.get("filled_quantity") or 0)
    try:
        assert placed["status"] == "FILLED" and quantity == 1
        held, = request_json("independent futures position", positions, account)
        assert held["symbol"] == asset.symbol and Decimal(held["quantity"]) == quantity
        child = subprocess.run([
            sys.executable, "-P", "-c",
            'import runpy,sys; runpy.run_path(sys.argv[1])["_read_dated_future_in_new_process"]()',
            str(Path(__file__).resolve()),
        ], input=json.dumps((broker._webull_config, asset.to_dict()), default=str),
            text=True, capture_output=True, timeout=45)
        assert child.returncode == 0, "Restarted strategy did not recover the exact dated contract"
    finally:
        if quantity:
            close = strategy.create_order(asset, quantity, "sell", time_in_force="day")
            closed = _submit_and_observe_terminal(strategy, observer, account, close)
            assert closed["status"] == "FILLED" and Decimal(closed["filled_quantity"]) == quantity
        assert not request_json("final futures positions", positions, account)


@pytest.mark.parametrize("paper", ["EVENTS_CASH"], indirect=True)
@pytest.mark.parametrize("mode", ["limit_amount", "best_amount", "limit_contract"])
def test_paper_event_fill_or_kill(paper, mode):
    broker, observer, account = paper
    asset, _ = _quoted_event_contract(broker.data_source.client, "no", require_two_sided=True)
    positions = observer.account_v2.get_account_position
    assert not request_json("event starting positions", positions, account)
    strategy = Strategy(broker=broker, name="webull_event_fill_or_kill", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    def buying_power():
        balance = request_json("independent event buying power", observer.account_v2.get_account_balance, account)
        return next(Decimal(r["buying_power"]) for r in balance["account_currency_assets"] if r["currency"] == "USD")

    def fees(row):
        items = (row.get("fees") or []) + [row.get("commission") or {}]
        return sum(Decimal(i.get("actual_value") or 0) for i in items)

    before_cash = buying_power()
    by_amount = mode != "limit_contract"
    params = {"total_cash_amount": "1.00"} if by_amount else {}
    limit = Decimal("0.90") if by_amount else Decimal("0.01")
    if mode == "best_amount":
        params["event_trade_mode"] = "TRADE_IN_AMOUNT"
        limit = None
    order = strategy.create_order(asset, 0 if by_amount else 1, "buy", order_type="limit",
                                  limit_price=limit, time_in_force="fok", custom_params=params)
    strategy.submit_order(order)
    assert order.status != order.OrderStatus.ERROR, order.error_message
    # No client cancellation can be used as evidence of venue FOK behavior.
    venue_finished = True
    try:
        terminal = _detail(observer, account, order.identifier, lambda row: row["status"] in
                           {"FILLED", "CANCELLED", "CANCELED", "EXPIRED", "FAILED", "REJECTED"})
    except pytest.fail.Exception:
        venue_finished = False
        terminal = _cancel_then_read(strategy, observer, account, order)
    quantity = Decimal(terminal.get("filled_quantity") or 0)
    try:
        assert venue_finished, "Venue FOK did not reach a terminal state before cleanup"
        assert terminal["symbol"] == asset.symbol and terminal["event_outcome"].lower() == asset.outcome
        assert terminal["time_in_force"] == "FOK" and terminal["order_type"] == "LIMIT"
        assert terminal["entrust_type"] == ("AMOUNT" if by_amount else "QTY")
        assert terminal["status"] in {"FILLED", "CANCELLED", "CANCELED", "EXPIRED"}
        if by_amount and terminal.get("total_cash_amount"):
            assert Decimal(terminal["total_cash_amount"]) == 1
        if mode == "best_amount":
            assert terminal["event_trade_mode"] == "TRADE_IN_AMOUNT"
        if terminal["status"] == "FILLED":
            assert quantity > 0
            if by_amount:
                assert quantity * Decimal(terminal["filled_price"]) + fees(terminal) <= Decimal("1.00")
            if not by_amount:
                assert quantity == 1
            if limit is not None:
                assert Decimal(terminal["filled_price"]) <= limit
            holding, = request_json("signed event position", positions, account)
            assert holding["symbol"] == asset.symbol and Decimal(holding["quantity"]) == quantity
            recovered = strategy.get_position(asset)
            assert recovered is not None and Decimal(str(recovered.quantity)) == quantity
        else:
            assert quantity == 0
    finally:
        if quantity:
            close = strategy.create_order(asset, quantity, "sell", limit_price=Decimal("0.01"), time_in_force="gtc")
            closed = _submit_and_observe_terminal(strategy, observer, account, close)
            assert closed["status"] == "FILLED" and Decimal(closed["filled_quantity"]) == quantity
        assert not request_json("independent event final positions", positions, account)
        assert not request_json("independent event working orders", observer.order_v3.list_order_open, account)["data"]
    expected_change = Decimal(0)
    if quantity:
        expected_change = quantity * (Decimal(closed["filled_price"]) - Decimal(terminal["filled_price"]))
        expected_change -= fees(terminal) + fees(closed)
    assert abs(buying_power() - before_cash - expected_change) <= Decimal("0.01")


@pytest.mark.parametrize("paper,product", [("INDIVIDUAL_CASH", "stock"), ("INDIVIDUAL_MARGIN", "option")],
                         indirect=["paper"], ids=["stock", "option"])
def test_paper_smart_limit_with_two_sided_quote(paper, product):
    from lumibot.entities import SmartLimitConfig, SmartLimitPreset

    _require_equity_core_session()
    broker, observer, account = paper
    asset = Asset("AAPL") if product == "stock" else _listed_test_calls(broker)[0]
    positions = observer.account_v2.get_account_position
    assert not request_json("initial smart-limit positions", positions, account)
    strategy = Strategy(broker=broker, name="webull_smart_limit", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    quoted = strategy.get_quote(asset)
    assert quoted.ask >= quoted.bid > 0
    side = "buy" if product == "stock" else "buy_to_open"
    cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST, final_price_pct=1.0, final_hold_seconds=30)
    target = strategy.create_order(asset, 1, side, order_type="smart_limit", smart_limit=cfg, time_in_force="day")
    sent = strategy.submit_order(target)
    assert sent is not None and sent.status != sent.OrderStatus.ERROR, target.error_message
    observed_prices = []
    try:
        for _ in range(12):
            row = _detail(observer, account, sent.identifier, lambda item: bool(item["order_id"]))
            if row.get("limit_price"):
                observed_prices.append(Decimal(row["limit_price"]))
            if row["status"] in {"FILLED", "CANCELLED", "CANCELED", "EXPIRED", "REJECTED", "FAILED"}:
                break
            strategy.get_order(sent.identifier, broker_refresh=True)
            strategy._executor._process_smart_limit_orders()
            time.sleep(1)
    finally:
        terminal = _cancel_then_read(strategy, observer, account, sent)
        quantity = Decimal(terminal.get("filled_quantity") or 0)
        if quantity:
            close_side = "sell" if product == "stock" else "sell_to_close"
            close = strategy.create_order(asset, quantity, close_side, order_type="market", time_in_force="day")
            closed = _submit_and_observe_terminal(strategy, observer, account, close)
            assert closed["status"] == "FILLED" and Decimal(closed["filled_quantity"]) == quantity
        assert not request_json("final smart-limit positions", positions, account)
        assert not request_json("final smart-limit open orders", observer.order_v3.list_order_open, account)["data"]
    assert terminal["order_type"] == "LIMIT" and observed_prices
    assert terminal["status"] == "FILLED" and quantity == 1
    assert Decimal(terminal["filled_price"]) <= Decimal(terminal["limit_price"])


@pytest.mark.parametrize("algorithm", ["TWAP", "VWAP", "POV"])
def test_paper_equity_algorithm_retains_venue_terms(paper, algorithm):
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    _require_equity_core_session()
    broker, observer, account = paper
    asset = Asset("F")
    positions = observer.account_v2.get_account_position
    assert not request_json("initial equity algorithm positions", positions, account)
    strategy = Strategy(broker=broker, name="webull_equity_algorithm", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    now = datetime.now(ZoneInfo("America/New_York"))
    close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    if now + timedelta(minutes=4) >= close:
        pytest.skip("The intraday execution window needs four minutes before the core close")
    rate_field = "target_vol_percent" if algorithm == "POV" else "max_target_percent"
    params = {"algo_type": algorithm, rate_field: "10",
              "algo_start_time": (now + timedelta(minutes=1)).strftime("%H:%M:%S"),
              "algo_end_time": min(now + timedelta(minutes=10), close).strftime("%H:%M:%S")}
    quote = strategy.get_quote(asset)
    assert quote.bid and quote.bid > 0
    limit = (Decimal(str(quote.bid)) * Decimal("0.97")).quantize(Decimal("0.01"))
    # Webull publishes a 1,000-share and $5,000 minimum for equity algorithms.
    # Use a passive, future-starting order in PaperTrade and bound its test size.
    if not Decimal("5") <= limit <= Decimal("30"):
        pytest.skip("The qualification symbol is outside this test's paper notional range")
    target = strategy.create_order(asset, 1000, "buy", limit_price=limit, time_in_force="day", custom_params=params)
    strategy.submit_order(target)
    assert target.status != target.OrderStatus.ERROR, target.error_message
    try:
        placed = _detail(observer, account, target.identifier, lambda row: bool(row.get("order_id")))
        assert placed.get("algo_type") == algorithm, "Venue detail did not confirm the requested algorithm"
        assert placed["symbol"] == asset.symbol and Decimal(placed["limit_price"]) == limit
    finally:
        terminal = _cancel_then_read(strategy, observer, account, target)
        quantity = Decimal(terminal.get("filled_quantity") or 0)
        if quantity:
            close_order = strategy.create_order(asset, quantity, "sell", order_type="market", time_in_force="day")
            closed = _submit_and_observe_terminal(strategy, observer, account, close_order)
            assert closed["status"] == "FILLED" and Decimal(closed["filled_quantity"]) == quantity
        assert not request_json("final algorithm positions", positions, account)
        assert not request_json("final algorithm working", observer.order_v3.list_order_open, account)["data"]


def test_paper_twap_fills_and_cancel_reconciles_executed_shares(paper):
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    _require_equity_core_session()
    broker, observer, account = paper
    positions = observer.account_v2.get_account_position
    assert not request_json("initial algorithm positions", positions, account)
    asset = Asset("F")
    strategy = Strategy(broker=broker, name="webull_algo_execution", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    now = datetime.now(ZoneInfo("America/New_York"))
    start = now + timedelta(seconds=20)
    end = start + timedelta(minutes=5)
    if end >= now.replace(hour=16, minute=0, second=0, microsecond=0):
        pytest.skip("Need the algorithm and cleanup window inside the core session")
    ask = strategy.get_quote(asset).ask
    assert ask and ask > 0
    limit = (Decimal(str(ask)) * Decimal("1.02")).quantize(Decimal("0.01"))
    if not Decimal("5") <= limit <= Decimal("30"):
        pytest.skip("The symbol is outside this test's PaperTrade notional range")
    params = {"algo_type": "TWAP", "algo_start_time": start.strftime("%H:%M:%S"),
              "algo_end_time": end.strftime("%H:%M:%S"), "max_target_percent": "10"}
    test_order = strategy.create_order(asset, 1000, "buy", limit_price=limit, time_in_force="day", custom_params=params)
    strategy.submit_order(test_order)
    assert test_order.status != test_order.OrderStatus.ERROR, test_order.error_message
    try:
        observed = _detail(observer, account, test_order.identifier,
                           lambda row: Decimal(row.get("filled_quantity") or 0) > 0,
                           pauses=(0, 8, 12, 16, 20, 24, 30, 30))
        assert observed["algo_type"] == "TWAP" and Decimal(observed["filled_price"]) <= limit
    finally:
        terminal = _cancel_then_read(strategy, observer, account, test_order)
        executed = Decimal(terminal.get("filled_quantity") or 0)
        try:
            if executed:
                row, = request_json("holding after algorithm stopped", positions, account)
                assert row["symbol"] == asset.symbol and Decimal(row["quantity"]) == executed
                for _ in range(2):
                    tracked = strategy.get_order(test_order.identifier, broker_refresh=True, broker_refresh_ttl_seconds=0)
                assert tracked.filled_quantity == executed
                assert sum(Decimal(str(t.quantity)) for t in tracked.transactions) == executed
        finally:
            if executed:
                close = strategy.create_order(asset, executed, "sell", order_type="market", time_in_force="day")
                closed = _submit_and_observe_terminal(strategy, observer, account, close)
                assert closed["status"] == "FILLED" and Decimal(closed["filled_quantity"]) == executed
            assert not request_json("final algorithm positions", positions, account)
            assert not request_json("final working algorithms", observer.order_v3.list_order_open, account)["data"]
    assert executed > 0


@pytest.mark.parametrize("paper", ["INDIVIDUAL_MARGIN"], indirect=True)
@pytest.mark.parametrize("shape", ["COVERED_STOCK", "COLLAR_WITH_STOCK"])
def test_paper_stock_option_package_while_held_and_after_close(paper, shape):
    from datetime import date, timedelta

    _require_equity_core_session()
    broker, observer, account = paper
    strategy = Strategy(broker=broker, name="webull_stock_option_package", risk_free_rate=0,
                        synchronize_broker_on_start=False, should_backup_variables_to_database=False,
                        should_send_summary_to_discord=False)
    positions = observer.account_v2.get_account_position
    assert not request_json("initial mixed positions", positions, account)
    assert not request_json("initial mixed orders", observer.order_v3.list_order_open, account)["data"]
    root = Asset("AAPL")
    spot = Decimal(str(strategy.get_quote(root).price))
    today = date.today()
    def contract(right, target, expiry=None):
        rows = broker.data_source.option_contracts(
            root, root_symbol=root.symbol, option_type=right, strike_price_gte=str(spot - 18),
            strike_price_lte=str(spot + 18), start_date=(expiry or today + timedelta(days=2)).isoformat(),
            end_date=(expiry or today + timedelta(days=12)).isoformat())
        assert rows, "No eligible listed contracts in the requested window"
        nearest = min(row["expiration_date"] for row in rows)
        row = min((row for row in rows if row["expiration_date"] == nearest),
                  key=lambda row: abs(Decimal(row["strike_price"]) - target))
        return Asset(root.symbol, "option", right=right, strike=float(row["strike_price"]),
                     expiration=date.fromisoformat(nearest), multiplier=float(row["multiplier"]))

    call = contract("CALL", spot + 3)
    assert call.multiplier == 100
    legs = [(root, 100, "buy", "sell"), (call, -1, "sell_to_open", "buy_to_close")]
    if shape == "COLLAR_WITH_STOCK":
        put = contract("PUT", spot - 3, call.expiration)
        assert put.multiplier == call.multiplier and put.strike < call.strike
        legs.append((put, 1, "buy_to_open", "sell_to_close"))

    def submit(closing):
        orders, net = [], Decimal(0)
        for asset, qty, entry_side, exit_side in legs:
            side = exit_side if closing else entry_side
            quote = strategy.get_quote(asset)
            assert quote.ask >= quote.bid > 0
            value = quote.ask if side.startswith("buy") else quote.bid
            net += Decimal(str(value)) * (1 if qty > 0 else -1)
            orders.append(strategy.create_order(asset, abs(qty), side, time_in_force="day",
                                                custom_params={"option_strategy": shape}))
        allowance = spot * Decimal("0.015")
        price = (net - allowance if closing else net + allowance).quantize(Decimal("0.01"))
        assert price > 0
        group, = strategy.submit_orders(orders, is_multileg=True, order_type="credit" if closing else "debit",
                                        price=price, duration="day")
        assert group.status != group.OrderStatus.ERROR, group.error_message
        return _observe_terminal_or_cancel(strategy, observer, account, group)

    opened = submit(False)
    try:
        assert opened["status"] == "FILLED" and opened["option_strategy"] == shape
        assert Decimal(opened["filled_quantity"]) == 1
        row, = request_json("independent mixed holding", positions, account)
        assert row["option_strategy"] == shape and Decimal(row["quantity"]) == 1
        expected = {asset: qty for asset, qty, _, _ in legs}
        fresh = Webull(broker._webull_config, connect_stream=False, start_orders_thread=False)
        try:
            assert {p.asset: p.quantity for p in fresh._pull_positions(strategy)} == expected
        finally:
            fresh.cleanup_streams()
        assert sorted(Decimal(leg["quantity"]) for leg in opened["legs"]) == sorted(abs(q) for q in expected.values())
    finally:
        if Decimal(opened.get("filled_quantity") or 0):
            closed = submit(True)
            assert closed["status"] == "FILLED" and Decimal(closed["filled_quantity"]) == 1, (
                "Native close did not fill. Reconcile this test's grouped position before any further order.")
        assert not request_json("final mixed positions", positions, account)
        assert not request_json("final mixed orders", observer.order_v3.list_order_open, account)["data"]


@pytest.mark.parametrize("paper", ["EVENTS_CASH"], indirect=True)
def test_paper_event_good_till_date_terms(paper):
    from datetime import datetime, timedelta, timezone

    broker, observer, account = paper
    asset, _ = _quoted_event_contract(broker.data_source.client, "no")
    strategy = Strategy(broker=broker, name="webull_event_gtd", risk_free_rate=0, synchronize_broker_on_start=False,
                        should_backup_variables_to_database=False, should_send_summary_to_discord=False)
    assert not request_json("starting GTD holdings", observer.account_v2.get_account_position, account)
    expiry = (datetime.now(timezone.utc) + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    order = strategy.create_order(asset, 1, "buy", limit_price=Decimal("0.01"), time_in_force="gtd", good_till_date=expiry)
    strategy.submit_order(order)
    assert order.status != order.OrderStatus.ERROR, order.error_message
    try:
        row = _detail(observer, account, order.identifier, lambda item: bool(item.get("order_id")))
        assert row["time_in_force"] == "GTD" and row["expire_date"] == expiry.date().isoformat()
    finally:
        final = _cancel_then_read(strategy, observer, account, order)
        quantity = Decimal(final.get("filled_quantity") or 0)
        if quantity:
            close = strategy.create_order(asset, quantity, "sell", limit_price=Decimal("0.01"), time_in_force="gtc")
            closed = _submit_and_observe_terminal(strategy, observer, account, close)
            assert Decimal(closed["filled_quantity"]) == quantity
        assert not request_json("final GTD holdings", observer.account_v2.get_account_position, account)
        assert not request_json("final GTD working orders", observer.order_v3.list_order_open, account)["data"]
