import inspect

import lumibot.example_strategies.ai_trading_team as example
from lumibot.example_strategies.agent_cycle import run_cycle, trader_prompt
from lumibot.example_strategies.ai_trading_team import AITradingTeamStrategy
from lumibot.example_strategies.ai_trading_team_bill_ackman_concentrated import (
    AITradingTeamBillAckmanConcentratedStrategy,
)
from lumibot.example_strategies.ai_trading_team_bull_bear_large_cap_stocks import (
    AITradingTeamBullBearLargeCapStocksStrategy,
)
from lumibot.example_strategies.ai_trading_team_bull_bear_leveraged_etf import (
    AITradingTeamBullBearLeveragedETFStrategy,
)
from lumibot.example_strategies.ai_trading_team_citadel_sector_pods import (
    AITradingTeamCitadelSectorPodsStrategy,
)
from lumibot.example_strategies.ai_trading_team_ray_dalio_idea_meritocracy import (
    AITradingTeamRayDalioIdeaMeritocracyStrategy,
)
from lumibot.example_strategies.ai_trading_team_warren_buffett_value import (
    AITradingTeamWarrenBuffettValueStrategy,
)


def test_ai_trading_team_is_bare_bones():
    assert list(AITradingTeamStrategy.parameters) == ["universe", "max_position_pct"]
    assert AITradingTeamStrategy is AITradingTeamBullBearLeveragedETFStrategy
    assert not hasattr(example, "MODEL")
    assert not hasattr(example, "UNIVERSE")
    assert not hasattr(AITradingTeamStrategy, "rotate_portfolio")


def test_ai_trading_team_uses_leveraged_etfs():
    universe = set(AITradingTeamStrategy.parameters["universe"])

    assert {"TQQQ", "SQQQ", "SOXL", "SOXS"}.issubset(universe)


def test_ai_trading_team_avoids_example_knobs():
    source = inspect.getsource(example)

    assert "@agent_tool" not in source
    assert "benchmark_asset" not in source
    assert "TradingFee" not in source
    assert "budget=" not in source
    assert "quiet_logs" not in source


def test_ai_trading_team_uses_agent_run_keywords():
    source = inspect.getsource(AITradingTeamStrategy.on_trading_iteration)

    assert '.run("' not in source
    assert "run_cycle(" in source
    assert "task_prompt=" in inspect.getsource(run_cycle)


def test_ai_trading_team_variants_import_and_define_universes():
    variants = {
        AITradingTeamBullBearLargeCapStocksStrategy: {"AAPL", "MSFT", "NVDA", "GOOGL"},
        AITradingTeamRayDalioIdeaMeritocracyStrategy: {"SPY", "QQQ", "TLT", "GLD"},
        AITradingTeamWarrenBuffettValueStrategy: {"AAPL", "KO", "AXP", "COST"},
        AITradingTeamBillAckmanConcentratedStrategy: {"GOOGL", "CMG", "HLT", "QSR"},
        AITradingTeamCitadelSectorPodsStrategy: {"XLK", "XLF", "XLV", "XLE"},
    }

    for strategy_class, expected_symbols in variants.items():
        universe = set(strategy_class.parameters["universe"])
        assert expected_symbols.issubset(universe)
        assert hasattr(strategy_class, "initialize")
        assert hasattr(strategy_class, "on_trading_iteration")


def test_ai_trading_team_variants_keep_one_trading_agent():
    variant_classes = {
        AITradingTeamBullBearLargeCapStocksStrategy: 4,
        AITradingTeamRayDalioIdeaMeritocracyStrategy: 4,
        AITradingTeamWarrenBuffettValueStrategy: 4,
        AITradingTeamBillAckmanConcentratedStrategy: 4,
        AITradingTeamCitadelSectorPodsStrategy: 6,
    }

    for strategy_class, read_only_count in variant_classes.items():
        source = inspect.getsource(strategy_class.initialize)
        assert source.count("allow_trading=True") == 1
        assert source.count("allow_trading=False") == read_only_count


def test_final_trading_agents_own_risk_instead_of_spending_nearly_all_cash():
    strategy_classes = (
        AITradingTeamBullBearLeveragedETFStrategy,
        AITradingTeamBullBearLargeCapStocksStrategy,
        AITradingTeamWarrenBuffettValueStrategy,
        AITradingTeamBillAckmanConcentratedStrategy,
    )

    prompt = trader_prompt(book_rule="book", exit_rule="exit").lower()
    assert "risk" in prompt
    assert "account" in prompt
    assert "positions" in prompt
    assert "open orders" in prompt
    assert "outside this book, drop that weight and rescale" in prompt

    for strategy_class in strategy_classes:
        initialize_source = inspect.getsource(strategy_class.initialize).lower()
        iteration_source = inspect.getsource(strategy_class.on_trading_iteration).lower()
        assert "weight only symbols in the universe" in initialize_source
        assert "nearly all cash" not in initialize_source
        assert "nearly all available cash" not in iteration_source
        assert "trader_prompt(" in initialize_source
        assert "max_position_pct" in strategy_class.parameters


def test_trader_never_parks_the_book_outside_the_universe_or_at_a_stale_price():
    prompt = " ".join(trader_prompt(book_rule="book", exit_rule="exit").split()).lower()
    assert "cash, treasury, or money-market funds" in prompt
    assert "never an allowed trade" in prompt
    assert "at the session open the last price can still be the prior close" in prompt
    assert "a limit exactly at the last price fills only if the next price reaches it" in prompt
    assert "use a market order or a buy limit slightly above" in prompt
    assert "use the current price, not a prior close" not in prompt


def test_trader_rebalances_once_within_a_tolerance_instead_of_churning():
    prompt = " ".join(trader_prompt(book_rule="book", exit_rule="exit").split()).lower()
    assert "plan every order from one read of the account" in prompt
    assert "within 2 percentage points of its target weight" in prompt
    assert "never buy and sell the same symbol in the same session" in prompt
    assert "once every holding is within that tolerance, stop" in prompt


def test_daily_bull_bear_books_rebalance_instead_of_selling_everything_each_morning():
    for strategy_class in (
        AITradingTeamBullBearLargeCapStocksStrategy,
        AITradingTeamBullBearLeveragedETFStrategy,
    ):
        trader = _created_prompts(strategy_class)["trader"].lower()
        assert "opened on an earlier session, sell it" not in trader
        assert "sell a holding" in trader
        assert "no longer" in trader
        assert "keep a holding that today's weights still include" in trader


def test_daily_bull_bear_trade_task_does_not_tell_the_trader_to_exit_the_whole_book():
    import inspect

    for strategy_class in (
        AITradingTeamBullBearLargeCapStocksStrategy,
        AITradingTeamBullBearLeveragedETFStrategy,
    ):
        source = inspect.getsource(inspect.getmodule(strategy_class))
        assert "Exit yesterday's book first" not in source


def test_leveraged_etf_book_never_holds_a_long_and_its_inverse_on_one_index():
    trader = _created_prompts(AITradingTeamBullBearLeveragedETFStrategy)["trader"].lower()
    assert "never hold a long etf and its inverse on the same index" in trader
    assert "sell the whole opposite side before buying" in trader


def test_trader_rescales_weights_a_book_rule_removed_instead_of_skipping():
    prompt = " ".join(trader_prompt(book_rule="book", exit_rule="exit").split()).lower()
    assert "when a book rule drops or nets away weight" in prompt
    assert "a conflict between these rules is never a reason to skip the rebalance" in prompt


def test_trader_never_buys_more_than_the_cash_it_has():
    prompt = " ".join(trader_prompt(book_rule="book", exit_rule="exit").split()).lower()
    assert "total cost of new buys must stay below cash plus the proceeds of this session's sells" in prompt
    assert "never let cash go negative" in prompt


def _created_prompts(strategy_class):
    from types import SimpleNamespace

    created = {}

    class _Agents(dict):
        def create(self, **kwargs):
            created[kwargs["name"]] = " ".join(kwargs["system_prompt"].split())

    strategy_class.initialize(SimpleNamespace(agents=_Agents(), parameters=dict(strategy_class.parameters)))
    return created


def test_buffett_team_values_the_business_before_judging_the_margin_of_safety():
    prompts = _created_prompts(AITradingTeamWarrenBuffettValueStrategy)
    researcher = prompts["researcher"].lower()
    interpreter = prompts["interpreter"].lower()

    for measure in ("earnings yield", "free cash flow yield", "net debt"):
        assert measure in researcher
    assert "as of the current date" in researcher
    assert "do not require a full intrinsic value model" in interpreter
    assert "weights sum near 100%" in interpreter
    assert "weight only symbols in the universe" in interpreter


def test_bull_bear_code_runs_the_two_sides_together_then_an_interpreter():
    from lumibot.example_strategies import agent_cycle

    cycle_source = inspect.getsource(agent_cycle.run_cycle)
    assert "run_together" in cycle_source
    for strategy_class in (
        AITradingTeamBullBearLargeCapStocksStrategy,
        AITradingTeamBullBearLeveragedETFStrategy,
    ):
        source = inspect.getsource(strategy_class)
        assert "run_cycle(" in source
        assert '"interpreter"' in source
