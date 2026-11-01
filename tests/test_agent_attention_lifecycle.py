"""Native example wiring, with model responses standing in only at the AI boundary.

These checks establish scheduling, answer handoff and error propagation. They
are not real-model evals, option fills or historical strategy results.
"""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from lumibot.components.agents import AgentRunResult
from lumibot.components.agents.attention import AttentionMonitor
from lumibot.example_strategies.ai_iron_condor import AIIronCondorStrategy
from lumibot.example_strategies.ai_bull_call_spread import AIBullCallSpreadStrategy
from lumibot.example_strategies.ai_call_stock_replacement import AICallStockReplacementStrategy
from lumibot.strategies._strategy import Vars


def example(strategy_class):
    strategy = object.__new__(strategy_class)
    strategy.parameters = {'symbol': 'XYZ', 'model': 'chatgpt/gpt-6-luna'}
    strategy.vars = Vars()
    strategy._is_backtesting = True
    strategy.now = datetime(2030, 6, 3, 19, 45, tzinfo=timezone.utc)
    strategy.get_datetime = lambda: strategy.now
    created, calls, handles = [], [], {}
    manager = SimpleNamespace(attention=AttentionMonitor(strategy))

    def create(**settings):
        created.append(settings)
        def run(**kwargs):
            calls.append((settings['name'], kwargs))
            return AgentRunResult(summary='Dated research answer.', model=settings['model'], events=[],
                payload={'execution_outcome': {'decision_completed': True}})
        handles[settings['name']] = SimpleNamespace(run=run)
    manager.create = create
    class Agents:
        attention = manager.attention
        def create(self, **kwargs):
            return create(**kwargs)
        def __getitem__(self, name):
            return handles[name]
    strategy.agents = Agents()
    strategy.initialize()
    return strategy, created, calls, handles


@pytest.fixture(params=[AIIronCondorStrategy, AIBullCallSpreadStrategy, AICallStockReplacementStrategy])
def strategy_class(request):
    return request.param


def test_example_registers_research_and_one_trading_owner(strategy_class):
    strategy, created, calls, handles = example(strategy_class)
    assert {(entry['name'], entry['allow_trading']) for entry in created} == {
        ('researcher', False), ('trader', True)}
    assert all('tools' not in entry for entry in created)


def test_quiet_minutes_make_no_model_calls_and_initial_schedule_runs_once(strategy_class):
    strategy, _, calls, _ = example(strategy_class)
    for _ in range(390):
        strategy.on_trading_iteration()
    assert calls == []
    strategy.before_market_closes()
    assert [name for name, _ in calls] == ['researcher', 'trader']
    for _ in range(390):
        strategy.now += timedelta(minutes=1)
        strategy.on_trading_iteration()
    strategy.before_market_closes()
    assert len(calls) == 2
    strategy.on_strategy_end()


def test_research_tool_returns_the_actual_answer_without_interpreting_it(strategy_class):
    strategy, _, calls, _ = example(strategy_class)
    answer = strategy.refresh_research('Review this opportunity')
    assert answer == 'Dated research answer.'
    assert strategy.vars.get('research') == {'at': strategy.now.isoformat(), 'answer': answer}
    assert [name for name, _ in calls] == ['researcher']


def test_management_reuses_research_and_explicit_research_watch_refreshes_both_roles(strategy_class):
    strategy, _, calls, _ = example(strategy_class)
    strategy.before_market_closes()
    for identity in ['management', 'research_review']:
        strategy.agents.attention.watch(owner='trader', watch_id=identity, operation_id='a',
            metric='time', comparison='gte', value=strategy.now.isoformat(), reason='Agent-selected review')
        strategy.on_trading_iteration()
    assert [name for name, _ in calls] == ['researcher', 'trader', 'trader', 'researcher', 'trader']


def test_incomplete_model_result_retains_original_attention(strategy_class):
    strategy, _, calls, handles = example(strategy_class)
    handles['trader'].run = lambda **kwargs: AgentRunResult(summary=None,
        model='chatgpt/gpt-6-luna', events=[], payload={'execution_outcome': {'decision_completed': False}})
    with pytest.raises(RuntimeError, match='AI decision incomplete'):
        strategy.before_market_closes()
    assert len(strategy.agents.attention.pending()) == 1
    assert strategy.vars.get('attention_started') is True
