from datetime import datetime, timezone

from lumibot.components.agents import AgentManager, BuiltinTools
from lumibot.sentiment import AdanosMarketSentiment


class _Strategy:
    """Strategy test fixture with a fixed backtest datetime and log capture."""

    is_backtesting = True
    broker = None

    def __init__(self):
        self.log_messages = []
        self.sentiment = AdanosMarketSentiment(self, base_url="https://adanos.test", timeout=3)

    def get_datetime(self):
        return datetime(2026, 5, 20, 14, 30, tzinfo=timezone.utc)

    def log_message(self, message, color=None):
        self.log_messages.append((message, color))


class _Response:
    """Lightweight HTTP response stub returning the stored JSON payload."""

    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def test_adanos_client_fetches_stock_sentiment_with_strategy_date_bound(monkeypatch):
    calls = []

    def fake_get(url, *, headers, params, timeout):
        calls.append({"url": url, "headers": headers, "params": params, "timeout": timeout})
        return _Response({"ticker": "AAPL", "sentiment_score": 0.42})

    monkeypatch.setenv("ADANOS_API_KEY", "test-key")
    monkeypatch.setattr("lumibot.sentiment.adanos.requests.get", fake_get)

    client = AdanosMarketSentiment(_Strategy(), base_url="https://adanos.test", timeout=3)
    result = client.get_stock_sentiment("aapl", sources="reddit,news", days=5)

    assert result["symbol"] == "AAPL"
    assert result["end"] == "2026-05-20"
    assert result["sources"] == ["reddit", "news"]
    assert result["results"]["reddit"]["sentiment_score"] == 0.42
    assert len(calls) == 2
    assert calls[0]["url"] == "https://adanos.test/reddit/stocks/v1/stock/AAPL"
    assert calls[0]["headers"] == {"X-API-Key": "test-key"}
    assert calls[0]["params"] == {"days": 5, "to": "2026-05-20"}
    assert calls[0]["timeout"] == 3
    assert calls[1]["url"] == "https://adanos.test/news/stocks/v1/stock/AAPL"
    assert calls[1]["headers"] == {"X-API-Key": "test-key"}
    assert calls[1]["params"] == {"days": 5, "to": "2026-05-20"}
    assert calls[1]["timeout"] == 3


def test_adanos_client_rejects_empty_sources():
    client = AdanosMarketSentiment(_Strategy(), api_key="test-key", base_url="https://adanos.test")

    try:
        client.get_stock_sentiment("AAPL", sources="")
    except ValueError as exc:
        assert "No Adanos sources provided" in str(exc)
    else:
        raise AssertionError("Expected ValueError for empty Adanos sources")


def test_adanos_client_requires_api_key(monkeypatch):
    monkeypatch.delenv("ADANOS_API_KEY", raising=False)
    client = AdanosMarketSentiment(_Strategy(), api_key="", base_url="https://adanos.test")

    result = client.get_stock_sentiment("MSFT", sources="x")

    assert result["results"] == {}
    assert "ADANOS_API_KEY is required" in result["errors"]["x"]


def test_builtin_adanos_tool_is_disabled_without_api_key(monkeypatch):
    monkeypatch.delenv("ADANOS_API_KEY", raising=False)

    strategy = _Strategy()
    tool = BuiltinTools.sentiment.adanos_market_sentiment().binder(strategy, None)
    result = tool.function(symbol="AAPL")

    assert tool.metadata["disabled"] is True
    assert tool.metadata["disabled_reason"] == "missing ADANOS_API_KEY"
    assert strategy.log_messages
    assert result["ok"] is False
    assert result["tool_error"] is True
    assert result["results"] == {}


def test_builtin_adanos_tool_fetches_market_sentiment(monkeypatch):
    calls = []

    def fake_get(url, *, headers, params, timeout):
        calls.append({"url": url, "headers": headers, "params": params, "timeout": timeout})
        return _Response({"overall_sentiment": "bullish"})

    monkeypatch.setenv("ADANOS_API_KEY", "test-key")
    monkeypatch.setattr("lumibot.sentiment.adanos.requests.get", fake_get)

    tool = BuiltinTools.sentiment.adanos_market_sentiment().binder(_Strategy(), None)
    result = tool.function(mode="market", sources="polymarket", days=3)

    assert result["ok"] is True
    assert result["results"]["polymarket"]["overall_sentiment"] == "bullish"
    assert calls[0]["url"] == "https://adanos.test/polymarket/stocks/v1/market-sentiment"
    assert calls[0]["params"] == {"days": 3, "to": "2026-05-20"}


def test_agent_manager_omits_unavailable_adanos_tool(monkeypatch):
    monkeypatch.delenv("ADANOS_API_KEY", raising=False)

    strategy = _Strategy()
    manager = AgentManager(strategy)
    handle = manager.create(name="test_agent", tools=[])
    tool_names = {tool.name for tool in handle._ensure_bound_tools()}

    assert "adanos_market_sentiment" not in tool_names
