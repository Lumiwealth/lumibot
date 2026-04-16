import logging
import json
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from lumibot.backtesting import PandasDataBacktesting
from lumibot.components.agents import AgentRunResult
from lumibot.entities import Asset, Data
from lumibot.strategies import Strategy


def _utc_iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _event(kind: str, *, text: str | None = None, tool_name: str | None = None, payload: dict | None = None):
    from lumibot.components.agents import AgentTraceEvent

    return AgentTraceEvent(
        kind=kind,
        text=text,
        tool_name=tool_name,
        payload=payload,
        timestamp=_utc_iso_timestamp(),
    )


def _invoke_tool(request, events, tool_name: str, **kwargs):
    from lumibot.components.agents.runtime import _wrap_tool_callable

    tool_map = {tool.name: _wrap_tool_callable(tool) for tool in request.bound_tools}
    events.append(_event("tool_call", tool_name=tool_name, payload=kwargs))
    result = tool_map[tool_name](**kwargs)
    payload = result if isinstance(result, dict) else {"value": result}
    events.append(_event("tool_result", tool_name=tool_name, payload=payload))
    return result


class StockPlanRuntime:
    call_count = 0

    def run(self, request):
        type(self).call_count += 1
        events = [_event("thinking", text="Inspecting current stock state.")]
        positions = _invoke_tool(request, events, "account_positions")
        table = _invoke_tool(
            request,
            events,
            "market_load_history_table",
            symbol=request.context["symbol"],
            length=request.context["length"],
            timestep=request.context["timestep"],
            asset_type=request.context.get("asset_type", "stock"),
            table_name="stock_history",
        )
        _invoke_tool(
            request,
            events,
            "duckdb_query",
            sql=f"SELECT AVG(close) AS avg_close, MAX(close) AS max_close FROM {table['table_name']}",
        )
        has_position = any(
            pos.get("asset", {}).get("symbol") == request.context["symbol"] and float(pos.get("quantity") or 0) > 0
            for pos in positions["positions"]
            if isinstance(pos, dict) and isinstance(pos.get("asset"), dict)
        )
        if not has_position:
            _invoke_tool(
                request,
                events,
                "orders_submit_order",
                symbol=request.context["symbol"],
                quantity=1,
                side="buy",
                asset_type="stock",
                order_type="market",
            )
            summary = "Bought the stock after inspecting DuckDB history."
        else:
            summary = "Held the current stock position."
        events.append(_event("text", text=summary))
        return AgentRunResult(summary=summary, model=request.model, events=events)


class OptionPlanRuntime:
    call_count = 0

    def run(self, request):
        type(self).call_count += 1
        events = [_event("thinking", text="Inspecting current option state.")]
        positions = _invoke_tool(request, events, "account_positions")
        table = _invoke_tool(
            request,
            events,
            "market_load_history_table",
            symbol=request.context["symbol"],
            length=request.context["length"],
            timestep=request.context["timestep"],
            asset_type="option",
            expiration=request.context["expiration"],
            strike=request.context["strike"],
            right=request.context["right"],
            table_name="option_history",
        )
        _invoke_tool(
            request,
            events,
            "duckdb_query",
            sql=f"SELECT COUNT(*) AS rows_seen, MIN(close) AS min_close, MAX(close) AS max_close FROM {table['table_name']}",
        )
        has_position = any(
            pos.get("asset", {}).get("asset_type") == "option" and float(pos.get("quantity") or 0) > 0
            for pos in positions["positions"]
            if isinstance(pos, dict)
        )
        if not has_position:
            _invoke_tool(
                request,
                events,
                "orders_submit_order",
                symbol=request.context["symbol"],
                quantity=1,
                side="buy",
                asset_type="option",
                expiration=request.context["expiration"],
                strike=request.context["strike"],
                right=request.context["right"],
                order_type="market",
            )
            summary = "Bought the option contract after checking history."
        else:
            summary = "Held the current option position."
        events.append(_event("text", text=summary))
        return AgentRunResult(summary=summary, model=request.model, events=events)


class MinuteStressRuntime:
    call_count = 0

    def run(self, request):
        type(self).call_count += 1
        events = [_event("thinking", text="Stress-testing minute DuckDB history refresh.")]
        positions = _invoke_tool(request, events, "account_positions")
        table = _invoke_tool(
            request,
            events,
            "market_load_history_table",
            symbol=request.context["symbol"],
            length=request.context["length"],
            timestep=request.context["timestep"],
            asset_type="stock",
            table_name="minute_window",
        )
        _invoke_tool(
            request,
            events,
            "duckdb_query",
            sql=f"SELECT COUNT(*) AS rows_seen, AVG(close) AS avg_close, MAX(close) AS max_close FROM {table['table_name']}",
        )
        has_position = any(
            pos.get("asset", {}).get("symbol") == request.context["symbol"] and float(pos.get("quantity") or 0) > 0
            for pos in positions["positions"]
            if isinstance(pos, dict) and isinstance(pos.get("asset"), dict)
        )
        if not has_position:
            _invoke_tool(
                request,
                events,
                "orders_submit_order",
                symbol=request.context["symbol"],
                quantity=1,
                side="buy",
                asset_type="stock",
                order_type="market",
            )
            summary = "Bought once during minute stress test."
        else:
            summary = "Held during minute stress test."
        events.append(_event("text", text=summary))
        return AgentRunResult(summary=summary, model=request.model, events=events)


class PromptCaptureRuntime:
    call_count = 0
    last_request = None

    def run(self, request):
        type(self).call_count += 1
        type(self).last_request = request
        summary = "Captured runtime context for inspection."
        return AgentRunResult(
            summary=summary,
            model=request.model,
            events=[_event("text", text=summary)],
        )


class FutureTimestampRuntime:
    call_count = 0

    def run(self, request):
        type(self).call_count += 1
        events = [
            _event("tool_call", tool_name="fred_get_series", payload={"series_id": "CPIAUCSL"}),
            _event(
                "tool_result",
                tool_name="fred_get_series",
                payload={"observations": [{"date": "2025-01-08", "realtime_end": "2025-01-08T00:00:00Z"}]},
            ),
            _event("text", text="Used macro data to inspect inflation."),
        ]
        return AgentRunResult(summary="Used macro data to inspect inflation.", model=request.model, events=events)


class InvalidAssetTypeRuntime:
    call_count = 0

    def run(self, request):
        type(self).call_count += 1
        events = [_event("thinking", text="Testing invalid asset type handling.")]
        result = _invoke_tool(
            request,
            events,
            "market_load_history_table",
            symbol=request.context["symbol"],
            length=request.context["length"],
            timestep=request.context["timestep"],
            asset_type=request.context["asset_type"],
            table_name="invalid_history",
        )
        assert result.get("tool_error") is True
        summary = "Tool rejected an invalid asset_type without crashing the strategy."
        events.append(_event("text", text=summary))
        return AgentRunResult(summary=summary, model=request.model, events=events)


class AgentStockBacktestStrategy(Strategy):
    runtime_class = StockPlanRuntime

    def initialize(self):
        self.sleeptime = "1M"
        self.asset = Asset("AGST", Asset.AssetType.STOCK)
        self.agents.create(
            name="research",
            system_prompt="Use DuckDB and buy once if no position exists.",
            default_model="stub-stock",
            tools=[
                self._builtin_positions(),
                self._builtin_portfolio(),
                self._builtin_history(),
                self._builtin_query(),
                self._builtin_submit(),
            ],
            _runtime=self.runtime_class(),
        )

    def _builtin_positions(self):
        from lumibot.components.agents import BuiltinTools

        return BuiltinTools.account.positions()

    def _builtin_portfolio(self):
        from lumibot.components.agents import BuiltinTools

        return BuiltinTools.account.portfolio()

    def _builtin_history(self):
        from lumibot.components.agents import BuiltinTools

        return BuiltinTools.market.load_history_table()

    def _builtin_query(self):
        from lumibot.components.agents import BuiltinTools

        return BuiltinTools.duckdb.query()

    def _builtin_submit(self):
        from lumibot.components.agents import BuiltinTools

        return BuiltinTools.orders.submit()

    def on_trading_iteration(self):
        self.agents["research"].run(
            context={
                "symbol": self.asset.symbol,
                "length": 3,
                "timestep": "minute",
                "asset_type": self.parameters.get("asset_type", "stock"),
            }
        )


class AgentOptionBacktestStrategy(Strategy):
    runtime_class = OptionPlanRuntime
    expiration = "2025-01-17"
    strike = 100.0
    right = "CALL"

    def initialize(self):
        self.sleeptime = "1M"
        self.option_asset = Asset(
            "AGOP",
            asset_type=Asset.AssetType.OPTION,
            expiration=date(2025, 1, 17),
            strike=self.strike,
            right=self.right,
        )
        from lumibot.components.agents import BuiltinTools

        self.agents.create(
            name="research",
            system_prompt="Use DuckDB and buy the fixed option contract once if no position exists.",
            default_model="stub-option",
            tools=[
                BuiltinTools.account.positions(),
                BuiltinTools.market.load_history_table(),
                BuiltinTools.duckdb.query(),
                BuiltinTools.orders.submit(),
            ],
            _runtime=self.runtime_class(),
        )

    def on_trading_iteration(self):
        self.agents["research"].run(
            context={
                "symbol": self.option_asset.symbol,
                "length": 3,
                "timestep": "minute",
                "expiration": self.expiration,
                "strike": self.strike,
                "right": self.right,
            }
        )


class AgentMinuteStressBacktestStrategy(Strategy):
    runtime_class = MinuteStressRuntime

    def initialize(self):
        self.sleeptime = "1M"
        self.asset = Asset("AGMS", Asset.AssetType.STOCK)
        from lumibot.components.agents import BuiltinTools

        self.agents.create(
            name="research",
            system_prompt="Stress test minute DuckDB history refreshes and only buy once.",
            default_model="stub-minute-stress",
            tools=[
                BuiltinTools.account.positions(),
                BuiltinTools.market.load_history_table(),
                BuiltinTools.duckdb.query(),
                BuiltinTools.orders.submit(),
            ],
            _runtime=self.runtime_class(),
        )

    def on_trading_iteration(self):
        self.agents["research"].run(
            context={
                "symbol": self.asset.symbol,
                "length": 120,
                "timestep": "minute",
            }
        )


class PromptCaptureStrategy(Strategy):
    runtime_class = PromptCaptureRuntime

    def log_message(self, message, *args, **kwargs):
        logs = list(self.vars.get("captured_logs", []) or [])
        logs.append(str(message))
        self.vars.captured_logs = logs
        return super().log_message(message, *args, **kwargs)

    def initialize(self):
        self.sleeptime = "1D"
        from lumibot.components.agents import BuiltinTools

        self.agents.create(
            name="research",
            system_prompt="Review the current state before deciding whether to trade.",
            default_model="stub-prompt-capture",
            tools=[BuiltinTools.account.positions(), BuiltinTools.account.portfolio()],
            _runtime=self.runtime_class(),
        )

    def on_trading_iteration(self):
        self.agents["research"].run(context={"symbol": "AGST"})


class FutureTimestampWarningStrategy(Strategy):
    runtime_class = FutureTimestampRuntime

    def log_message(self, message, *args, **kwargs):
        logs = list(self.vars.get("warning_logs", []) or [])
        logs.append(str(message))
        self.vars.warning_logs = logs
        return super().log_message(message, *args, **kwargs)

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="research",
            system_prompt="Inspect macro data before deciding whether to trade.",
            default_model="stub-future-warning",
            tools=[],
            _runtime=self.runtime_class(),
        )

    def on_trading_iteration(self):
        result = self.agents["research"].run(context={"target_symbol": "TQQQ"})
        self.vars.result_summary = result.summary
        self.vars.result_warnings = result.warnings


class InvalidAssetTypeStrategy(Strategy):
    runtime_class = InvalidAssetTypeRuntime

    def log_message(self, message, *args, **kwargs):
        logs = list(self.vars.get("invalid_asset_logs", []) or [])
        logs.append(str(message))
        self.vars.invalid_asset_logs = logs
        return super().log_message(message, *args, **kwargs)

    def initialize(self):
        self.sleeptime = "1D"
        from lumibot.components.agents import BuiltinTools

        self.agents.create(
            name="research",
            system_prompt="Use the history tool carefully and surface invalid arguments clearly.",
            default_model="stub-invalid-asset",
            tools=[BuiltinTools.market.load_history_table()],
            _runtime=self.runtime_class(),
        )

    def on_trading_iteration(self):
        result = self.agents["research"].run(
            context={
                "symbol": "AGST",
                "length": 3,
                "timestep": "minute",
                "asset_type": "equity",
            }
        )
        self.vars.result_summary = result.summary
        self.vars.result_warnings = result.warnings


def _build_stock_pandas_data():
    index = pd.date_range("2025-01-06 09:30", periods=6, freq="min", tz="America/New_York")
    df = pd.DataFrame(
        {
            "open": [100.0, 101.0, 102.5, 103.0, 104.0, 104.5],
            "high": [101.0, 103.0, 103.5, 104.0, 105.0, 105.5],
            "low": [99.0, 100.5, 101.5, 102.0, 103.5, 104.0],
            "close": [100.5, 102.0, 103.0, 103.5, 104.5, 105.0],
            "volume": [1000, 1100, 1200, 1300, 1400, 1500],
        },
        index=index,
    )
    asset = Asset("AGST", Asset.AssetType.STOCK)
    return {asset: Data(asset, df, timestep="minute")}


def _build_option_pandas_data():
    index = pd.date_range("2025-01-06 09:30", periods=6, freq="min", tz="America/New_York")
    underlying = Asset("AGOP", Asset.AssetType.STOCK)
    option = Asset(
        "AGOP",
        asset_type=Asset.AssetType.OPTION,
        expiration=date(2025, 1, 17),
        strike=100.0,
        right="CALL",
    )
    quote = Asset("USD", Asset.AssetType.FOREX)
    underlying_df = pd.DataFrame(
        {
            "open": [99.0, 100.0, 101.0, 101.5, 102.0, 102.5],
            "high": [100.0, 101.0, 102.0, 102.5, 103.0, 103.5],
            "low": [98.5, 99.5, 100.5, 101.0, 101.5, 102.0],
            "close": [99.5, 100.5, 101.5, 102.0, 102.5, 103.0],
            "volume": [1500, 1500, 1500, 1500, 1500, 1500],
        },
        index=index,
    )
    option_df = pd.DataFrame(
        {
            "open": [3.0, 3.2, 3.5, 3.7, 3.9, 4.0],
            "high": [3.2, 3.5, 3.8, 4.0, 4.2, 4.3],
            "low": [2.8, 3.0, 3.3, 3.5, 3.7, 3.8],
            "close": [3.1, 3.4, 3.7, 3.9, 4.1, 4.2],
            "volume": [100, 110, 120, 130, 140, 150],
        },
        index=index,
    )
    return {
        underlying: Data(underlying, underlying_df, timestep="minute"),
        option: Data(option, option_df, quote=quote, timestep="minute"),
    }


def _build_minute_stress_pandas_data():
    day_one = pd.date_range("2025-01-06 09:30", periods=390, freq="min", tz="America/New_York")
    day_two = pd.date_range("2025-01-07 09:30", periods=390, freq="min", tz="America/New_York")
    index = day_one.append(day_two)
    base = [100.0 + i * 0.02 for i in range(len(index))]
    df = pd.DataFrame(
        {
            "open": base,
            "high": [value + 0.15 for value in base],
            "low": [value - 0.15 for value in base],
            "close": [value + 0.05 for value in base],
            "volume": [1000 + (i % 100) for i in range(len(index))],
        },
        index=index,
    )
    asset = Asset("AGMS", Asset.AssetType.STOCK)
    return {asset: Data(asset, df, timestep="minute")}


@pytest.mark.usefixtures("disable_datasource_override")
def test_agent_runtime_stock_backtest_replays_from_cache(monkeypatch, tmp_path):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    StockPlanRuntime.call_count = 0
    params = dict(
        datasource_class=PandasDataBacktesting,
        backtesting_start=datetime(2025, 1, 6, 9, 30),
        backtesting_end=datetime(2025, 1, 6, 9, 35),
        pandas_data=_build_stock_pandas_data(),
        benchmark_asset=None,
        analyze_backtest=False,
        show_plot=False,
        save_tearsheet=False,
        show_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=False,
        quiet_logs=True,
    )

    _, strategy_first = AgentStockBacktestStrategy.run_backtest(**params)
    first_calls = StockPlanRuntime.call_count
    assert first_calls > 0
    assert strategy_first.get_position(Asset("AGST", Asset.AssetType.STOCK)) is not None
    assert strategy_first.agents.duckdb.get_metrics()["history_load_calls"] >= 1
    assert strategy_first.agents.duckdb.get_metrics()["history_bind_calls"] == 1
    assert strategy_first.agents.duckdb.get_metrics()["history_visible_refresh_calls"] >= 1
    state = strategy_first.vars.get("_agent_runtime_state", {})
    assert "research" in state
    assert state["research"]["memory_notes"]

    _, strategy_second = AgentStockBacktestStrategy.run_backtest(**params)
    assert StockPlanRuntime.call_count == first_calls
    assert strategy_second.get_position(Asset("AGST", Asset.AssetType.STOCK)) is not None
    second_state = strategy_second.vars.get("_agent_runtime_state", {})
    assert second_state["research"]["runs"][-1]["cache_hit"] is True


@pytest.mark.usefixtures("disable_datasource_override")
def test_agent_runtime_option_backtest_executes_option_order(monkeypatch, tmp_path):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    OptionPlanRuntime.call_count = 0
    _, strategy = AgentOptionBacktestStrategy.run_backtest(
        datasource_class=PandasDataBacktesting,
        backtesting_start=datetime(2025, 1, 6, 9, 30),
        backtesting_end=datetime(2025, 1, 6, 9, 35),
        pandas_data=_build_option_pandas_data(),
        benchmark_asset=None,
        analyze_backtest=False,
        show_plot=False,
        save_tearsheet=False,
        show_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=False,
        quiet_logs=True,
    )
    assert OptionPlanRuntime.call_count > 0
    positions = strategy.get_positions(include_cash_positions=True)
    option_positions = [position for position in positions if position.asset.asset_type == Asset.AssetType.OPTION]
    assert option_positions
    assert strategy.agents.duckdb.get_metrics()["history_load_calls"] >= 1
    assert strategy.agents.duckdb.get_metrics()["history_bind_calls"] == 1


@pytest.mark.usefixtures("disable_datasource_override")
def test_agent_runtime_minute_duckdb_stress_binds_once(monkeypatch, tmp_path):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    MinuteStressRuntime.call_count = 0
    _, strategy = AgentMinuteStressBacktestStrategy.run_backtest(
        datasource_class=PandasDataBacktesting,
        backtesting_start=datetime(2025, 1, 6, 9, 30),
        backtesting_end=datetime(2025, 1, 7, 15, 59),
        pandas_data=_build_minute_stress_pandas_data(),
        benchmark_asset=None,
        analyze_backtest=False,
        show_plot=False,
        save_tearsheet=False,
        show_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=False,
        quiet_logs=True,
    )
    metrics = strategy.agents.duckdb.get_metrics()
    assert MinuteStressRuntime.call_count >= 700
    assert metrics["history_load_calls"] >= 700
    assert metrics["history_bind_calls"] == 1
    assert metrics["history_bind_cache_hits"] >= 700


@pytest.mark.usefixtures("disable_datasource_override")
def test_agent_runtime_injects_base_prompt_runtime_context_and_default_summary_log(monkeypatch, tmp_path):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    PromptCaptureRuntime.call_count = 0
    PromptCaptureRuntime.last_request = None
    _, strategy = PromptCaptureStrategy.run_backtest(
        datasource_class=PandasDataBacktesting,
        backtesting_start=datetime(2025, 1, 6),
        backtesting_end=datetime(2025, 1, 7),
        pandas_data=_build_stock_pandas_data(),
        benchmark_asset=None,
        analyze_backtest=False,
        show_plot=False,
        save_tearsheet=False,
        show_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=False,
        quiet_logs=True,
    )
    request = PromptCaptureRuntime.last_request
    assert request is not None
    assert request.runtime_context["mode"] == "backtesting"
    assert request.runtime_context["current_datetime"]
    assert request.runtime_context["timezone"]
    assert "positions" in request.runtime_context
    assert "account" in request.runtime_context
    assert "recent_orders" in request.runtime_context
    assert "recent_trades" in request.runtime_context
    assert "BACKTESTING SAFETY RULES" in request.system_prompt
    assert "Look-ahead bias" in request.system_prompt
    assert "Current datetime:" in request.system_prompt
    assert "DEFAULT INVESTOR POLICY" in request.system_prompt
    assert "Do not trade for the sake of activity." in request.system_prompt
    assert "Do not resist intentional concentration" in request.system_prompt
    assert "Avoid leaving raw cash idle unless there is a specific reason" in request.system_prompt
    summary_logs = [line for line in strategy.vars.captured_logs if line.startswith("[agents] name=research")]
    assert summary_logs
    assert "cache_hit=False" in summary_logs[-1]
    assert "tool_calls=0" in summary_logs[-1]
    summary_file = Path(tmp_path / "cache" / "agent_runtime" / "agent_run_summaries.jsonl")
    assert summary_file.exists()
    records = [json.loads(line) for line in summary_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert records
    assert records[-1]["agent_name"] == "research"
    assert records[-1]["trace_relative_path"].startswith("agent_runtime/traces/")


@pytest.mark.usefixtures("disable_datasource_override")
def test_agent_runtime_replays_market_priming_builtins_on_cache(monkeypatch, tmp_path):
    from lumibot.components.agents import BuiltinTools

    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    StockPlanRuntime.call_count = 0
    _, strategy = AgentStockBacktestStrategy.run_backtest(
        datasource_class=PandasDataBacktesting,
        backtesting_start=datetime(2025, 1, 6),
        backtesting_end=datetime(2025, 1, 7),
        pandas_data=_build_stock_pandas_data(),
        benchmark_asset=None,
        analyze_backtest=False,
        show_plot=False,
        save_tearsheet=False,
        show_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=False,
        quiet_logs=True,
    )
    market_last_price = BuiltinTools.market.last_price().binder(strategy, strategy.agents)
    market_load_history = BuiltinTools.market.load_history_table().binder(strategy, strategy.agents)
    assert market_last_price.metadata.get("replay_on_cache") is True
    assert market_load_history.metadata.get("replay_on_cache") is True


def test_builtin_market_history_and_duckdb_descriptions_include_schema_hints():
    from lumibot.components.agents import BuiltinTools

    _, strategy = PromptCaptureStrategy.run_backtest(
        datasource_class=PandasDataBacktesting,
        backtesting_start=datetime(2025, 1, 6),
        backtesting_end=datetime(2025, 1, 7),
        pandas_data=_build_stock_pandas_data(),
        benchmark_asset=None,
        analyze_backtest=False,
        show_plot=False,
        save_tearsheet=False,
        show_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=False,
        quiet_logs=True,
    )
    history_tool = BuiltinTools.market.load_history_table().binder(strategy, strategy.agents)
    query_tool = BuiltinTools.duckdb.query().binder(strategy, strategy.agents)

    assert "datetime" in history_tool.description
    assert "close" in history_tool.description
    assert "datetime" in query_tool.description
    assert "close" in query_tool.description


@pytest.mark.usefixtures("disable_datasource_override")
def test_run_backtest_explicit_quiet_logs_false_emits_agent_logs(monkeypatch, tmp_path, caplog):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    monkeypatch.delenv("BACKTESTING_QUIET_LOGS", raising=False)
    monkeypatch.delenv("BACKTESTING_SHOW_PROGRESS_BAR", raising=False)
    PromptCaptureRuntime.call_count = 0
    PromptCaptureRuntime.last_request = None
    caplog.set_level(logging.INFO, logger="lumibot")

    PromptCaptureStrategy.run_backtest(
        datasource_class=PandasDataBacktesting,
        backtesting_start=datetime(2025, 1, 6),
        backtesting_end=datetime(2025, 1, 7),
        pandas_data=_build_stock_pandas_data(),
        benchmark_asset=None,
        analyze_backtest=False,
        show_plot=False,
        save_tearsheet=False,
        show_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=False,
        quiet_logs=False,
    )

    assert any("[agents] name=research" in record.getMessage() for record in caplog.records)


@pytest.mark.usefixtures("disable_datasource_override")
def test_run_backtest_explicit_quiet_logs_true_suppresses_agent_logs(monkeypatch, tmp_path, caplog):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    monkeypatch.delenv("BACKTESTING_QUIET_LOGS", raising=False)
    monkeypatch.delenv("BACKTESTING_SHOW_PROGRESS_BAR", raising=False)
    PromptCaptureRuntime.call_count = 0
    PromptCaptureRuntime.last_request = None
    caplog.set_level(logging.INFO, logger="lumibot")

    PromptCaptureStrategy.run_backtest(
        datasource_class=PandasDataBacktesting,
        backtesting_start=datetime(2025, 1, 6),
        backtesting_end=datetime(2025, 1, 7),
        pandas_data=_build_stock_pandas_data(),
        benchmark_asset=None,
        analyze_backtest=False,
        show_plot=False,
        save_tearsheet=False,
        show_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=False,
        quiet_logs=True,
    )

    assert not any("[agents] name=research" in record.getMessage() for record in caplog.records)


@pytest.mark.usefixtures("disable_datasource_override")
def test_agent_runtime_emits_future_timestamp_warning_without_blocking(monkeypatch, tmp_path):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    FutureTimestampRuntime.call_count = 0
    _, strategy = FutureTimestampWarningStrategy.run_backtest(
        datasource_class=PandasDataBacktesting,
        backtesting_start=datetime(2025, 1, 6),
        backtesting_end=datetime(2025, 1, 7),
        pandas_data=_build_stock_pandas_data(),
        benchmark_asset=None,
        analyze_backtest=False,
        show_plot=False,
        save_tearsheet=False,
        show_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=False,
        quiet_logs=True,
    )
    assert strategy.vars.result_summary == "Used macro data to inspect inflation."
    assert any(warning.get("kind") == "future_timestamp" for warning in strategy.vars.result_warnings)
    warning_logs = list(strategy.vars.warning_logs or [])
    assert any("[agents][observability_warning]" in line for line in warning_logs)


@pytest.mark.usefixtures("disable_datasource_override")
def test_agent_runtime_invalid_asset_type_emits_observability_warning(monkeypatch, tmp_path):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    InvalidAssetTypeRuntime.call_count = 0
    _, strategy = InvalidAssetTypeStrategy.run_backtest(
        datasource_class=PandasDataBacktesting,
        backtesting_start=datetime(2025, 1, 6),
        backtesting_end=datetime(2025, 1, 7),
        pandas_data=_build_stock_pandas_data(),
        benchmark_asset=None,
        analyze_backtest=False,
        show_plot=False,
        save_tearsheet=False,
        show_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=False,
        quiet_logs=True,
    )
    assert strategy.vars.result_summary == "Tool rejected an invalid asset_type without crashing the strategy."
    assert any(warning.get("kind") == "tool_error" for warning in strategy.vars.result_warnings)
    invalid_asset_logs = list(strategy.vars.invalid_asset_logs or [])
    assert any("[agents][observability_warning]" in line for line in invalid_asset_logs)


def test_builtin_docs_search_returns_local_doc_snippets(monkeypatch, tmp_path):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    _, strategy = PromptCaptureStrategy.run_backtest(
        datasource_class=PandasDataBacktesting,
        backtesting_start=datetime(2025, 1, 6),
        backtesting_end=datetime(2025, 1, 7),
        pandas_data=_build_stock_pandas_data(),
        benchmark_asset=None,
        analyze_backtest=False,
        show_plot=False,
        save_tearsheet=False,
        show_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=False,
        quiet_logs=True,
    )
    from lumibot.components.agents import BuiltinTools

    docs_tool = BuiltinTools.docs.search().binder(strategy, strategy.agents)
    result = docs_tool.function(query="benchmark_asset SPY", max_results=3)
    assert result["result_count"] >= 1
    assert any("SPY" in item["snippet"] or "benchmark" in item["snippet"].lower() for item in result["results"])


def test_agent_runtime_wrap_tool_callable_sanitizes_nan_payloads():
    from lumibot.components.agents.runtime import _wrap_tool_callable
    from lumibot.components.agents.schemas import BoundTool

    def nan_result_tool():
        return {
            "price": float("nan"),
            "nested": {"change": float("inf"), "ok": 1.25},
            "rows": [1.0, float("-inf")],
        }

    wrapped = _wrap_tool_callable(
        BoundTool(
            name="nan_result_tool",
            description="Return NaN values for regression testing.",
            function=nan_result_tool,
        )
    )
    result = wrapped()
    assert result["price"] is None
    assert result["nested"]["change"] is None
    assert result["nested"]["ok"] == 1.25
    assert result["rows"] == [1.0, None]
