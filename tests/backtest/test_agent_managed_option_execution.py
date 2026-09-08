"""Real Strategy/ADK/gateway/builtin/broker path; only inference and market data are fixtures."""
import json
from datetime import date, datetime, timezone

import pandas as pd
import pytest

from lumibot.backtesting import PandasDataBacktesting
from lumibot.components.agents.managed_gateway import BotSpotManagedLlm
from lumibot.entities import Asset, Data
from lumibot.strategies import Strategy


class ManagedSpreadStrategy(Strategy):
    def initialize(self):
        self.set_market("24/7")
        self.sleeptime = "1M"
        self.agents.create(name="trader", model="google/gemini-flash-lite", allow_trading=True,
                           system_prompt="Execute the specified synthetic spread once.")

    def on_trading_iteration(self):
        if not self.vars.get("ran"):
            self.vars.ran = True
            self.vars.summary = self.agents["trader"].run(task_prompt="Inspect and submit the spread.").summary


def option_data():
    index = pd.date_range("2026-08-20T14:30:00Z", periods=5, freq="min")
    assets = [Asset("QQQ"), *[Asset("QQQ", asset_type="option", expiration=date(2026, 9, 18),
                                  strike=k, right="put") for k in (520, 525)]]
    result = {}
    for asset, price in zip(assets, (530, .5, 1)):
        frame = pd.DataFrame({"open": price, "high": price + .1, "low": price - .1,
                              "close": price, "volume": 1000, "bid": price - .05,
                              "ask": price + .05}, index=index)
        result[asset] = Data(asset, frame, timestep="minute")
    return result


@pytest.mark.usefixtures("disable_datasource_override")
def test_managed_native_tool_chain_reaches_atomic_option_submission_and_simulated_fill(monkeypatch, tmp_path):
    legs = json.dumps([dict(symbol="QQQ", expiration="2026-09-18", strike=k, right="put",
                            quantity=1, side=side) for k, side in [(520, "buy_to_open"), (525, "sell_to_open")]])
    trajectory = [
        ("account_portfolio", {}), ("account_positions", {}),
        ("market_last_price", {"symbol": "QQQ"}),
        ("options_get_chain", {"symbol": "QQQ", "include_strikes": True}),
        ("options_calculate_multileg_price", {"legs_json": legs}),
        ("orders_submit_multileg", {"legs_json": legs, "net_limit_price": -.5}),
    ]
    requests = []

    def post(url, token, payload):
        step = len(requests)
        requests.append(payload)
        if step:
            parts = [part for message in payload["messages"] for part in message["parts"]]
            previous_call = next(p for p in parts if p.get("type") == "function_call" and p.get("id") == f"call-{step-1}")
            assert previous_call["thoughtSignature"] == "c2lnbmF0dXJl"
            result = next(p for p in parts if p.get("type") == "function_response" and p.get("id") == f"call-{step-1}")
            assert result["name"] == trajectory[step-1][0]
            assert not result.get("response", {}).get("tool_error"), result
            assert "error" not in result.get("response", {}), result
            assert payload["model"] == "gemini-3.5-flash-lite"
        parts = ([{"type": "function_call", "name": trajectory[step][0], "arguments": trajectory[step][1],
                   "id": f"call-{step}", "thoughtSignature": "c2lnbmF0dXJl"}]
                 if step < len(trajectory) else [{"type": "text", "text": "Spread submitted; fills are broker-observed."}])
        return 200, {"resolvedModel": "gemini-3.5-flash-lite", "model": "gemini-3.5-flash-lite",
                     "parts": parts, "usage": {"inputTokens": 10, "outputTokens": 5}}

    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    monkeypatch.setenv("LUMIBOT_AI_GATEWAY_URL", "https://gateway.example.test")
    monkeypatch.setenv("LUMIBOT_AI_GATEWAY_TOKEN", "synthetic-bound-token")
    for key in ("GEMINI_API_KEY", "GOOGLE_API_KEY"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr("lumibot.components.agents.managed_gateway.managed_gateway_model", lambda model: BotSpotManagedLlm(
        model=model, gateway_url="https://gateway.example.test", access_token="synthetic-bound-token", post=post))
    _, strategy = ManagedSpreadStrategy.run_backtest(
        datasource_class=PandasDataBacktesting,
        backtesting_start=datetime(2026, 8, 20, 14, 30, tzinfo=timezone.utc),
        backtesting_end=datetime(2026, 8, 20, 14, 34, tzinfo=timezone.utc),
        pandas_data=option_data(), benchmark_asset=None, analyze_backtest=False,
        show_plot=False, save_tearsheet=False, show_tearsheet=False, show_indicators=False,
        save_logfile=False, show_progress_bar=False, quiet_logs=True)
    assert len(requests) == len(trajectory) + 1
    positions = {p.asset.strike: float(p.quantity) for p in strategy.get_positions() if p.asset.asset_type == "option"}
    assert positions == {520: 1, 525: -1}
    detail = pd.read_parquet(strategy.parameters["agent_trader_detail_parquet"])
    calls = detail.loc[detail.event_kind == "tool_call", "tool_name"].tolist()
    assert calls == [name for name, _ in trajectory]
