from datetime import datetime, timezone

import pandas as pd
import pytest

from lumibot.backtesting import PandasDataBacktesting
from lumibot.components.agents.managed_gateway import BotSpotManagedLlm
from lumibot.entities import Asset, Data
from lumibot.strategies import Strategy


class ManagedGatewayBitcoinStrategy(Strategy):
    def initialize(self):
        self.set_market("24/7")
        self.sleeptime = "1M"
        self.agents.create(
            name="btc_research",
            system_prompt="Return a short BTC market observation.",
            default_model="openai/gpt-5.6-luna",
            include_builtin_tools=False,
            include_builtin_skills=False,
        )

    def on_trading_iteration(self):
        if self.vars.get("agent_ran"):
            return
        result = self.agents["btc_research"].run(
            task="Observe BTC/USD without placing an order.",
            context={"symbol": "BTC", "quote": "USD", "market": "24/7"},
        )
        self.vars.agent_ran = True
        self.vars.agent_summary = result.summary


def _bitcoin_data():
    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    index = pd.date_range("2026-08-20T00:00:00Z", periods=3, freq="min")
    frame = pd.DataFrame(
        {
            "open": [60000.0, 60010.0, 60020.0],
            "high": [60020.0, 60030.0, 60040.0],
            "low": [59990.0, 60000.0, 60010.0],
            "close": [60010.0, 60020.0, 60030.0],
            "volume": [1.0, 1.1, 1.2],
        },
        index=index,
    )
    return {base: Data(base, frame, timestep="minute", quote=quote)}


@pytest.mark.usefixtures("disable_datasource_override")
def test_24_7_bitcoin_strategy_uses_real_agent_runtime_and_managed_gateway(monkeypatch, tmp_path):
    gateway_calls = []

    def post(url, token, payload):
        gateway_calls.append((url, token, payload))
        return 200, {
            "model": payload["model"],
            "parts": [{"type": "text", "text": "BTC observation complete."}],
            "usage": {"inputTokens": 11, "cachedInputTokens": 3, "outputTokens": 4},
        }

    def managed_model(model):
        return BotSpotManagedLlm(
            model=model,
            gateway_url="https://gateway.example.test",
            access_token="deployment-bound-token",
            post=post,
        )

    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "synthetic-test-key")
    monkeypatch.setenv("LUMIBOT_AI_GATEWAY_URL", "https://gateway.example.test")
    monkeypatch.setenv("LUMIBOT_AI_GATEWAY_TOKEN", "deployment-bound-token")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setattr(
        "lumibot.components.agents.managed_gateway.managed_gateway_model",
        managed_model,
    )

    _, strategy = ManagedGatewayBitcoinStrategy.run_backtest(
        datasource_class=PandasDataBacktesting,
        backtesting_start=datetime(2026, 8, 20, 0, 0, tzinfo=timezone.utc),
        backtesting_end=datetime(2026, 8, 20, 0, 2, tzinfo=timezone.utc),
        pandas_data=_bitcoin_data(),
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

    assert strategy.vars.agent_summary == "BTC observation complete."
    assert len(gateway_calls) == 1
    url, token, payload = gateway_calls[0]
    assert url == "https://gateway.example.test/v2/inference"
    assert token == "deployment-bound-token"
    assert payload["provider"] == "openai"
    assert payload["model"] == "openai/gpt-5.6-luna"
    assert "OPENAI_API_KEY" not in payload
    assert strategy.parameters["agent_btc_research_input_tokens"] == 11
    assert strategy.parameters["agent_btc_research_cached_input_tokens"] == 3
    assert strategy.parameters["agent_btc_research_output_tokens"] == 4
    detail = pd.read_parquet(strategy.parameters["agent_btc_research_detail_parquet"])
    summaries = detail[detail["event_kind"] == "call_summary"]
    assert set(summaries["ai_access_mode"]) == {"botspot_managed"}
    assert set(summaries["gateway_protocol_version"]) == {2}
    assert summaries["gateway_component_sha256"].str.fullmatch(r"[0-9a-f]{64}").all()
