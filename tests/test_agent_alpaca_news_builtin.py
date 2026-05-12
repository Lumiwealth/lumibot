from datetime import UTC, datetime

from lumibot.components.agents import BuiltinTools


class _Strategy:
    is_backtesting = True

    def get_datetime(self):
        return datetime(2026, 4, 28, 15, 30, tzinfo=UTC)


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


def _install_fake_alpaca(monkeypatch):
    calls = []

    def fake_get(url, *, headers, params, timeout):
        calls.append({"url": url, "headers": headers, "params": params, "timeout": timeout})
        return _Response()

    monkeypatch.setenv("ALPACA_API_KEY", "key")
    monkeypatch.setenv("ALPACA_API_SECRET", "secret")
    monkeypatch.setattr("lumibot.components.agents.builtins.requests.get", fake_get)
    return calls


def test_builtin_alpaca_news_uses_byok_default_scan_mode_and_bounds_default_end(monkeypatch):
    calls = _install_fake_alpaca(monkeypatch)

    tool = BuiltinTools.news.alpaca_news().binder(_Strategy(), None)
    result = tool.function(symbols="AAPL")

    assert result["ok"] is True
    assert result["count"] == 1
    assert result["window_end"] == "2026-04-28T15:30:00+00:00"
    assert result["query_symbols"] == "AAPL"
    assert result["include_content"] is False
    assert result["content_included"] is False
    assert result["content_available_count"] == 1
    assert result["summary_available_count"] == 1
    assert result["articles"][0]["content_available"] is True
    assert "content" not in result["articles"][0]
    assert calls[0]["params"]["limit"] == 30
    assert calls[0]["params"]["symbols"] == "AAPL"
    assert calls[0]["params"]["include_content"] is False
    assert calls[0]["headers"]["APCA-API-KEY-ID"] == "key"


def test_builtin_alpaca_news_full_content_is_not_truncated_unless_requested(monkeypatch):
    calls = _install_fake_alpaca(monkeypatch)

    tool = BuiltinTools.news.alpaca_news().binder(_Strategy(), None)
    result = tool.function(symbols="AAPL", limit=99, include_content=True)

    assert calls[0]["params"]["limit"] == 50
    assert calls[0]["params"]["include_content"] is True
    assert result["include_content"] is True
    assert result["content_included"] is True
    assert len(result["articles"][0]["content"]) == 2500
    assert result["articles"][0]["content_original_length"] == 2500
    assert result["articles"][0]["content_truncated"] is False


def test_builtin_alpaca_news_truncates_only_with_content_max_chars(monkeypatch):
    _install_fake_alpaca(monkeypatch)

    tool = BuiltinTools.news.alpaca_news().binder(_Strategy(), None)
    result = tool.function(symbols="AAPL", include_content=True, content_max_chars=100)

    assert len(result["articles"][0]["content"]) == 100
    assert result["articles"][0]["content_original_length"] == 2500
    assert result["articles"][0]["content_truncated"] is True
    assert result["articles"][0]["content_max_chars"] == 100


def test_builtin_alpaca_news_passes_page_token_and_clamps_lookahead(monkeypatch):
    calls = _install_fake_alpaca(monkeypatch)

    tool = BuiltinTools.news.alpaca_news().binder(_Strategy(), None)
    result = tool.function(
        symbols="SPY,QQQ",
        start="2026-04-28T00:00:00Z",
        end="2026-04-29T00:00:00Z",
        page_token="abc123",
        sort="asc",
    )

    assert result["requested_end"] == "2026-04-29T00:00:00Z"
    assert result["effective_end"] == "2026-04-28T15:30:00+00:00"
    assert result["lookahead_clamped"] is True
    assert calls[0]["params"]["end"] == "2026-04-28T15:30:00+00:00"
    assert calls[0]["params"]["page_token"] == "abc123"
    assert calls[0]["params"]["sort"] == "asc"


def test_builtin_alpaca_news_description_teaches_scan_deep_read_and_no_keyword_search():
    tool_def = BuiltinTools.news.alpaca_news()

    assert "not keyword search" in tool_def.description
    assert "include_content=False" in tool_def.description
    assert "include_content=True" in tool_def.description
    assert "limit=30-50" in tool_def.description
    assert "page_token" in tool_def.description
    assert "Full content is not truncated unless you explicitly set content_max_chars" in tool_def.description
    assert "SPY,QQQ,DIA,IWM" in tool_def.description
    assert "XLK,SMH" in tool_def.description
    assert "look-ahead bias" in tool_def.description


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
