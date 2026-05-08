from datetime import datetime, timezone

from lumibot.components.agents import AgentManager, AgentRunResult, AgentTraceEvent


class _Vars(dict):
    def get(self, key, default=None):
        return super().get(key, default)

    def set(self, key, value):
        self[key] = value


class _Strategy:
    is_backtesting = True
    parameters = {}
    vars = _Vars()

    def get_datetime(self):
        return datetime(2026, 1, 2, tzinfo=timezone.utc)

    def log_message(self, *args, **kwargs):
        return None


class _Runtime:
    last_request = None

    def run(self, request):
        from lumibot.components.agents.runtime import _wrap_tool_callable

        type(self).last_request = request
        tool_map = {tool.name: _wrap_tool_callable(tool) for tool in request.bound_tools}
        assert "orders_submit_order" not in tool_map
        memory_result = tool_map["search_memory"](query="AAPL", limit=1)
        notify_result = tool_map["notify_user"](title="Test", message="Backtest dry run")
        events = [
            AgentTraceEvent(kind="tool_call", tool_name="search_memory", payload={"query": "AAPL"}),
            AgentTraceEvent(kind="tool_result", tool_name="search_memory", payload=memory_result),
            AgentTraceEvent(kind="tool_call", tool_name="notify_user", payload={"title": "Test"}),
            AgentTraceEvent(kind="tool_result", tool_name="notify_user", payload=notify_result),
            AgentTraceEvent(kind="text", text="Research completed without trading tools."),
        ]
        return AgentRunResult(summary="Research completed without trading tools.", model=request.model, events=events)


class _LongSummaryRuntime:
    requests = []

    def run(self, request):
        type(self).requests.append(request)
        summary = "x" * 5000
        return AgentRunResult(
            summary=summary,
            model=request.model,
            events=[AgentTraceEvent(kind="text", text=summary)],
        )


def test_agent_allow_trading_false_removes_only_mutating_order_tools():
    strategy = _Strategy()
    manager = AgentManager(strategy)

    agent = manager.create(name="researcher", model="openai/gpt-5.4-mini", allow_trading=False)
    tool_names = {tool.name for tool in agent._ensure_bound_tools()}

    assert "orders_submit_order" not in tool_names
    assert "orders_cancel_order" not in tool_names
    assert "orders_modify_order" not in tool_names
    assert "orders_open_orders" in tool_names
    assert "account_positions" in tool_names
    assert "get_income_statement" in tool_names
    assert "get_indicator" in tool_names
    assert "list_fred_series" in tool_names
    assert "get_fred_series" in tool_names
    assert "get_fred_latest" in tool_names
    assert "get_fred_snapshot" in tool_names
    assert agent.default_model == "openai/gpt-5.4-mini"


def test_agent_allow_trading_true_keeps_mutating_order_tools():
    strategy = _Strategy()
    manager = AgentManager(strategy)

    agent = manager.create(name="trader", model="openai/gpt-5.5", allow_trading=True)
    tool_names = {tool.name for tool in agent._ensure_bound_tools()}

    assert "orders_submit_order" in tool_names
    assert "orders_cancel_order" in tool_names
    assert "orders_modify_order" in tool_names
    assert "orders_open_orders" in tool_names
    assert agent.default_model == "openai/gpt-5.5"


def test_read_only_agent_runtime_can_use_non_trading_tools(tmp_path):
    from lumibot.components.memory import MemoryStore
    from lumibot.components.notifications import NotificationManager

    strategy = _Strategy()
    strategy.is_backtesting = False
    strategy.memory = MemoryStore(strategy, root_dir=tmp_path)
    strategy.notifications = NotificationManager(strategy)
    strategy.notify = lambda title, message, **kwargs: strategy.notifications.notify(title, message, **kwargs)
    manager = AgentManager(strategy)
    runtime = _Runtime()

    agent = manager.create(
        name="researcher_runtime",
        model="openai/gpt-5.4-mini",
        allow_trading=False,
        _runtime=runtime,
    )
    result = agent.run(task_prompt="Research without trading.")

    assert result.summary == "Research completed without trading tools."
    assert _Runtime.last_request.model == "openai/gpt-5.4-mini"
    assert any(event.tool_name == "search_memory" for event in result.tool_calls)
    assert any(event.tool_name == "notify_user" for event in result.tool_calls)


def test_agent_runtime_memory_notes_are_compacted(monkeypatch, tmp_path):
    from lumibot.components.memory import MemoryStore
    from lumibot.components.notifications import NotificationManager

    monkeypatch.setenv("LUMIBOT_AGENT_MEMORY_NOTE_MAX_CHARS", "500")
    strategy = _Strategy()
    strategy.vars = _Vars()
    strategy.is_backtesting = False
    strategy.memory = MemoryStore(strategy, root_dir=tmp_path)
    strategy.notifications = NotificationManager(strategy)
    manager = AgentManager(strategy)
    runtime = _LongSummaryRuntime()
    _LongSummaryRuntime.requests = []

    agent = manager.create(
        name="compact_memory",
        model="openai/gpt-5.4-mini",
        allow_trading=False,
        _runtime=runtime,
    )
    agent.run(task_prompt="first long summary")
    agent.run(task_prompt="second should receive compact prior summary")

    assert len(_LongSummaryRuntime.requests) == 2
    prior_notes = _LongSummaryRuntime.requests[1].memory_notes
    assert len(prior_notes) == 1
    assert len(prior_notes[0]["summary"]) == 500
    assert prior_notes[0]["summary"].endswith("...")
