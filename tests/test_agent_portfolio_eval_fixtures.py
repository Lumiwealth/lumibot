"""Native broker state, rather than fabricated tool replies, scores basket cases."""
from types import SimpleNamespace

import pytest

from lumibot.entities import Asset
from scripts.agent_eval_production_fixture import ProductionFixture
from scripts.run_agent_evals import build_fixture, load_cases, score_machine_contract


@pytest.mark.parametrize("count,quantity", [(9, 110), (25, 39)])
def test_native_portfolio_fixture_fills_all_requested_positions(count, quantity):
    fixture = build_fixture(f"portfolio_initial_{count}")
    production = ProductionFixture(fixture)
    try:
        for number in range(1, count + 1):
            order = production.strategy.create_order(Asset(f"QZX{number:02d}"), quantity, "buy")
            production.strategy.submit_order(order)
            production.settle()
            assert order.is_filled()
            assert order.avg_fill_price == 100
        production.capture(SimpleNamespace(tool_calls=[], tool_results=[]))
        assert len(fixture.positions) == count
        assert fixture.final_account["cash"] == 100000 - count * quantity * 100
        assert len([x for x in fixture.broker_trade_events if x["status"] == "fill"]) == count
    finally:
        production.close()


def test_actual_six_fills_cannot_satisfy_complete_portfolio_contract():
    fixture = build_fixture("portfolio_initial_25")
    production = ProductionFixture(fixture)
    try:
        for number in range(1, 7):
            production.strategy.submit_order(production.strategy.create_order(Asset(f"QZX{number:02d}"), 39, "buy"))
            production.settle()
        production.capture(SimpleNamespace(tool_calls=[], tool_results=[]))
        case = load_cases({"stock_complete_portfolio_25"})[0]
        transcript = {"fixture_calls": [], "tool_calls": [], "tool_results": [], "submissions": [],
                      "final_positions": fixture.positions, "final_account": fixture.final_account,
                      "broker_trade_events": fixture.broker_trade_events}
        score = score_machine_contract(case, transcript)
        assert score["pass"] is False
        assert any("final broker positions" in failure for failure in score["failures"])
        assert fixture.final_account["cash"] == 76600
    finally:
        production.close()
def test_quote_sizing_fixture_distinguishes_every_possible_price_basis():
    production = ProductionFixture(build_fixture("portfolio_quote_sizing"))
    try:
        asset = Asset("QZX01")
        quote = production.strategy.get_quote(asset)
        history = production.broker.data_source.get_historical_prices(asset, 2, timestep="day").df
        prices = (history.iloc[-1].close, quote.price, quote.bid, quote.ask)
        assert prices == (150, 99, 98, 100)
        assert len(set(prices)) == 4
    finally:
        production.close()
