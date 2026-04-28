from datetime import datetime, timezone

from lumibot.components.agents import BuiltinTools


class _Strategy:
    def get_datetime(self):
        return datetime(2026, 4, 28, 15, 30, tzinfo=timezone.utc)


class _Response:
    def raise_for_status(self):
        return None

    def json(self):
        return {
            "news": [
                {
                    "id": 123,
                    "headline": "AAPL reports earnings",
                    "summary": "Apple reported results after the bell.",
                    "source": "benzinga",
                    "created_at": "2026-04-28T14:00:00Z",
                    "updated_at": "2026-04-28T14:05:00Z",
                    "url": "https://example.com/aapl",
                    "symbols": ["AAPL"],
                    "content": "x" * 2500,
                }
            ],
            "next_page_token": "next",
        }


def test_builtin_alpaca_news_uses_byok_and_bounds_default_end(monkeypatch):
    calls = []

    def fake_get(url, *, headers, params, timeout):
        calls.append({"url": url, "headers": headers, "params": params, "timeout": timeout})
        return _Response()

    monkeypatch.setenv("ALPACA_API_KEY", "key")
    monkeypatch.setenv("ALPACA_API_SECRET", "secret")
    monkeypatch.setattr("lumibot.components.agents.builtins.requests.get", fake_get)

    tool = BuiltinTools.news.alpaca_news().binder(_Strategy(), None)
    result = tool.function(symbols="AAPL", limit=99, include_content=True)

    assert result["ok"] is True
    assert result["count"] == 1
    assert result["window_end"] == "2026-04-28T15:30:00+00:00"
    assert calls[0]["params"]["limit"] == 50
    assert calls[0]["params"]["symbols"] == "AAPL"
    assert calls[0]["headers"]["APCA-API-KEY-ID"] == "key"
    assert len(result["articles"][0]["content"]) == 2000
    assert result["articles"][0]["content_truncated"] is True


def test_builtin_alpaca_news_missing_credentials_is_tool_error(monkeypatch):
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_API_SECRET", raising=False)
    monkeypatch.delenv("APCA_API_KEY_ID", raising=False)
    monkeypatch.delenv("APCA_API_SECRET_KEY", raising=False)

    tool = BuiltinTools.news.alpaca_news().binder(_Strategy(), None)
    result = tool.function()

    assert result["ok"] is False
    assert result["tool_error"] is True
    assert result["articles"] == []
