from datetime import datetime, timezone

import pytest

from lumibot.components.agents import AgentManager, BuiltinTools


class _Strategy:
    is_backtesting = True
    broker = None

    def __init__(self):
        self.log_messages = []

    def get_datetime(self):
        return datetime(2026, 4, 28, 15, 30, tzinfo=timezone.utc)

    def log_message(self, message, color=None):
        self.log_messages.append((message, color))


class _Broker:
    name = "alpaca"

    def __init__(self, *, oauth_token="", api_key="", api_secret=""):
        self.oauth_token = oauth_token
        self.api_key = api_key
        self.api_secret = api_secret


class _StrategyWithBroker(_Strategy):
    def __init__(self, broker):
        self.broker = broker


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


def _install_fake_alpaca(monkeypatch, *, set_news_env=True):
    calls = []

    def fake_get(url, *, headers, params, timeout):
        calls.append({"url": url, "headers": headers, "params": params, "timeout": timeout})
        return _Response()

    if set_news_env:
        monkeypatch.setenv("ALPACA_NEWS_API_KEY", "key")
        monkeypatch.setenv("ALPACA_NEWS_API_SECRET", "secret")
    else:
        monkeypatch.delenv("ALPACA_NEWS_API_KEY", raising=False)
        monkeypatch.delenv("ALPACA_NEWS_API_SECRET", raising=False)
    monkeypatch.setattr("lumibot.components.agents.builtins.requests.get", fake_get)
    return calls


def test_builtin_alpaca_news_uses_byok_news_key_default_scan_mode_and_bounds_default_end(monkeypatch):
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
    assert result["credential_source"] == "byok_alpaca_news_env"


def test_builtin_alpaca_news_falls_back_to_standard_alpaca_env(monkeypatch):
    calls = _install_fake_alpaca(monkeypatch, set_news_env=False)
    monkeypatch.setenv("ALPACA_API_KEY", "standard-key")
    monkeypatch.setenv("ALPACA_API_SECRET", "standard-secret")

    tool = BuiltinTools.news.alpaca_news().binder(_Strategy(), None)
    result = tool.function(symbols="AAPL")

    assert tool.metadata.get("disabled") is not True
    assert calls[0]["headers"]["APCA-API-KEY-ID"] == "standard-key"
    assert calls[0]["headers"]["APCA-API-SECRET-KEY"] == "standard-secret"
    assert result["credential_source"] == "alpaca_api_env"


def test_builtin_alpaca_news_prefers_news_env_over_standard_alpaca_env(monkeypatch):
    calls = _install_fake_alpaca(monkeypatch)
    monkeypatch.setenv("ALPACA_API_KEY", "standard-key")
    monkeypatch.setenv("ALPACA_API_SECRET", "standard-secret")

    tool = BuiltinTools.news.alpaca_news().binder(_Strategy(), None)
    result = tool.function(symbols="AAPL")

    assert calls[0]["headers"]["APCA-API-KEY-ID"] == "key"
    assert result["credential_source"] == "byok_alpaca_news_env"


def test_builtin_alpaca_news_prefers_news_env_over_active_alpaca_broker_oauth(monkeypatch):
    calls = _install_fake_alpaca(monkeypatch)

    tool = BuiltinTools.news.alpaca_news().binder(
        _StrategyWithBroker(_Broker(oauth_token="oauth-token", api_key="broker-key", api_secret="broker-secret")),
        None,
    )
    result = tool.function(symbols="AAPL")

    assert calls[0]["headers"]["APCA-API-KEY-ID"] == "key"
    assert calls[0]["headers"]["APCA-API-SECRET-KEY"] == "secret"
    assert result["credential_source"] == "byok_alpaca_news_env"


def test_builtin_alpaca_news_falls_back_to_active_alpaca_broker_oauth(monkeypatch):
    calls = _install_fake_alpaca(monkeypatch, set_news_env=False)

    tool = BuiltinTools.news.alpaca_news().binder(
        _StrategyWithBroker(_Broker(oauth_token="oauth-token", api_key="broker-key", api_secret="broker-secret")),
        None,
    )
    result = tool.function(symbols="AAPL")

    assert calls[0]["headers"] == {"Authorization": "Bearer oauth-token"}
    assert result["credential_source"] == "alpaca_broker_oauth"


def test_builtin_alpaca_news_uses_active_alpaca_broker_api_keys(monkeypatch):
    calls = _install_fake_alpaca(monkeypatch, set_news_env=False)

    tool = BuiltinTools.news.alpaca_news().binder(
        _StrategyWithBroker(_Broker(api_key="broker-key", api_secret="broker-secret")),
        None,
    )
    result = tool.function(symbols="AAPL")

    assert calls[0]["headers"]["APCA-API-KEY-ID"] == "broker-key"
    assert calls[0]["headers"]["APCA-API-SECRET-KEY"] == "broker-secret"
    assert result["credential_source"] == "alpaca_broker_api_key"


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


def _install_article_page(monkeypatch, articles):
    calls = _install_fake_alpaca(monkeypatch)
    monkeypatch.setattr(_Response, "json", lambda self: {"news": articles, "next_page_token": "next"})
    return calls


def _historical_article(**changes):
    article = _Response().json()["news"][0]
    return {**article, **changes}


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"created_at": "2026-04-28T15:30:01Z", "updated_at": "2026-04-28T15:30:01Z"}, "published_after_as_of"),
        ({"updated_at": "2026-04-28T15:30:01Z"}, "version_after_as_of"),
        ({"created_at": None}, "unverified_availability"),
        ({"created_at": "invalid"}, "unverified_availability"),
        ({"created_at": "2026-04-28T14:00:00"}, "unverified_availability"),
        ({"updated_at": None}, "unverified_availability"),
        ({"updated_at": "invalid"}, "unverified_availability"),
        ({"updated_at": "2026-04-28T14:05:00"}, "unverified_availability"),
        ({"updated_at": "2026-04-28T13:00:00Z"}, "unverified_availability"),
    ],
)
def test_backtest_news_excludes_unavailable_or_later_article_versions(monkeypatch, changes, reason):
    # Request bounds do not establish what an upstream page actually returned.
    article = _historical_article(**changes)
    _install_article_page(monkeypatch, [article])

    tool = BuiltinTools.news.alpaca_news().binder(_Strategy(), None)
    result = tool.function(include_content=True)

    assert result["ok"] is True
    assert result["articles"] == []
    assert result["count"] == 0
    assert result["content_available_count"] == 0
    assert result["summary_available_count"] == 0
    integrity = result["historical_integrity"]
    assert integrity["excluded_counts"][reason] == 1
    assert sum(integrity["excluded_counts"].values()) == 1
    assert integrity["upstream_article_count"] == 1
    assert integrity["original_version_history_available"] is False
    assert integrity["coverage_complete"] is False
    assert integrity["limitations"]
    assert result["next_page_token"] == "next"


def test_backtest_news_keeps_utc_boundary_version_and_full_body(monkeypatch):
    article = _historical_article(
        created_at="2026-04-28T11:30:00-04:00",
        updated_at="2026-04-28T17:30:00+02:00",
    )
    _install_article_page(monkeypatch, [article])

    tool = BuiltinTools.news.alpaca_news().binder(_Strategy(), None)
    result = tool.function(include_content=True)

    assert result["articles"][0]["content"] == article["content"]
    assert result["count"] == 1
    assert result["historical_integrity"]["as_of"] == "2026-04-28T15:30:00+00:00"
    assert sum(result["historical_integrity"]["excluded_counts"].values()) == 0


def test_backtest_news_filters_versions_at_an_explicit_earlier_end(monkeypatch):
    _install_article_page(monkeypatch, [_historical_article()])

    tool = BuiltinTools.news.alpaca_news().binder(_Strategy(), None)
    result = tool.function(end="2026-04-28T14:02:00Z", include_content=True)

    assert result["articles"] == []
    assert result["historical_integrity"]["as_of"] == "2026-04-28T14:02:00+00:00"
    assert result["historical_integrity"]["excluded_counts"]["version_after_as_of"] == 1


def test_backtest_news_rejects_an_unparseable_end_before_provider_access(monkeypatch):
    calls = _install_fake_alpaca(monkeypatch)

    tool = BuiltinTools.news.alpaca_news().binder(_Strategy(), None)
    result = tool.function(end="unknown", include_content=True)

    assert result["ok"] is False
    assert result["tool_error"] is True
    assert result["error"]["type"] == "InvalidHistoricalWindow"
    assert result["articles"] == []
    assert calls == []


def test_backtest_headline_scan_excludes_later_mutable_versions(monkeypatch):
    _install_article_page(monkeypatch, [_historical_article(updated_at="2026-04-28T15:31:00Z")])

    tool = BuiltinTools.news.alpaca_news().binder(_Strategy(), None)
    result = tool.function(include_content=False)

    assert result["articles"] == []
    assert result["historical_integrity"]["excluded_counts"]["version_after_as_of"] == 1


def test_backtest_news_filters_the_complete_page_and_counts_only_visible_bodies(monkeypatch):
    visible = _historical_article()
    future = _historical_article(id=456, updated_at="2026-04-29T12:00:00Z")
    _install_article_page(monkeypatch, [visible, future, None])

    tool = BuiltinTools.news.alpaca_news().binder(_Strategy(), None)
    result = tool.function(include_content=True)

    assert [article["id"] for article in result["articles"]] == [123]
    assert result["content_available_count"] == 1
    assert result["summary_available_count"] == 1
    assert result["historical_integrity"]["upstream_article_count"] == 3
    assert result["historical_integrity"]["excluded_counts"] == {
        "published_after_as_of": 0,
        "version_after_as_of": 1,
        "unverified_availability": 1,
    }


def test_live_news_preserves_provider_rows_without_historical_filtering(monkeypatch):
    class _LiveStrategy(_Strategy):
        is_backtesting = False

    articles = [
        _historical_article(),
        _historical_article(id=456, created_at=None, updated_at=None),
        _historical_article(id=789, updated_at="2026-04-29T12:00:00Z"),
    ]
    _install_article_page(monkeypatch, articles)

    tool = BuiltinTools.news.alpaca_news().binder(_LiveStrategy(), None)
    result = tool.function(include_content=True)

    assert [article["id"] for article in result["articles"]] == [123, 456, 789]
    assert all(article["content"] == "x" * 2500 for article in result["articles"])
    assert "historical_integrity" not in result


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


def test_builtin_alpaca_news_missing_credentials_disables_tool_and_warns(monkeypatch):
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_API_SECRET", raising=False)
    monkeypatch.delenv("APCA_API_KEY_ID", raising=False)
    monkeypatch.delenv("APCA_API_SECRET_KEY", raising=False)
    monkeypatch.delenv("ALPACA_NEWS_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_NEWS_API_SECRET", raising=False)

    strategy = _Strategy()
    tool = BuiltinTools.news.alpaca_news().binder(strategy, None)
    result = tool.function()

    assert tool.metadata["disabled"] is True
    assert strategy.log_messages
    assert result["ok"] is False
    assert result["tool_error"] is True
    assert result["articles"] == []


def test_agent_manager_omits_unavailable_alpaca_news(monkeypatch):
    strategy = _Strategy()
    # Constructing the manager imports credentials, which reloads .env.local
    # and can put ALPACA_API_KEY back. Clear news keys and the regular Alpaca
    # fallback after that import, before tools are bound.
    manager = AgentManager(strategy)
    for name in (
        "ALPACA_API_KEY",
        "ALPACA_API_SECRET",
        "APCA_API_KEY_ID",
        "APCA_API_SECRET_KEY",
        "ALPACA_NEWS_API_KEY",
        "ALPACA_NEWS_API_SECRET",
    ):
        monkeypatch.delenv(name, raising=False)

    handle = manager.create(name="test_agent", tools=[])
    tool_names = {tool.name for tool in handle._ensure_bound_tools()}

    assert "alpaca_news" not in tool_names


def test_news_proof_accepts_pruned_full_content_from_regular_keys(tmp_path):
    """Agent detail rows shorten large news payloads. The proof still has to
    see a regular-key fallback and a full article from that shortened row."""
    import json

    import pandas as pd

    from scripts.run_alpaca_news_ai_proof import _summarize_and_assert

    scan_excerpt = (
        '{"count": 21, "credential_source": "alpaca_api_env", "include_content": false, '
        '"articles": [{"content_available": false, "headline": "Fed holds rates"}]}'
    )
    full_excerpt = (
        '{"count": 3, "credential_source": "alpaca_api_env", "include_content": true, '
        '"articles": [{"content_available": true, "content_original_length": 15177, '
        '"content_truncated": false, "headline": "US stocks"}]}'
    )
    rows = [
        {
            "event_kind": "tool_call",
            "tool_name": "alpaca_news",
            "event_payload_json": '{"include_content": false, "symbols": "SPY"}',
            "call_input_tokens": 0,
            "call_output_tokens": 0,
            "call_thinking_tokens": 0,
            "call_cached_input_tokens": 0,
            "call_latency_ms": 0,
        },
        {
            "event_kind": "tool_result",
            "tool_name": "alpaca_news",
            "event_payload_json": json.dumps({
                "lumibot_tool_result_pruned": True,
                "excerpt": scan_excerpt,
                "original_chars": 13993,
            }),
            "call_input_tokens": 0,
            "call_output_tokens": 0,
            "call_thinking_tokens": 0,
            "call_cached_input_tokens": 0,
            "call_latency_ms": 0,
        },
        {
            "event_kind": "tool_call",
            "tool_name": "alpaca_news",
            "event_payload_json": '{"include_content": true, "symbols": "SPY"}',
            "call_input_tokens": 0,
            "call_output_tokens": 0,
            "call_thinking_tokens": 0,
            "call_cached_input_tokens": 0,
            "call_latency_ms": 0,
        },
        {
            "event_kind": "tool_result",
            "tool_name": "alpaca_news",
            "event_payload_json": json.dumps({
                "lumibot_tool_result_pruned": True,
                "excerpt": full_excerpt,
                "original_chars": 28033,
            }),
            "call_input_tokens": 0,
            "call_output_tokens": 0,
            "call_thinking_tokens": 0,
            "call_cached_input_tokens": 0,
            "call_latency_ms": 0,
        },
        {
            "event_kind": "call_summary",
            "tool_name": "",
            "event_payload_json": "{}",
            "call_input_tokens": 10,
            "call_output_tokens": 2,
            "call_thinking_tokens": 0,
            "call_cached_input_tokens": 0,
            "call_latency_ms": 5,
        },
    ]
    path = tmp_path / "detail.parquet"
    pd.DataFrame(rows).to_parquet(path)

    summary = _summarize_and_assert(path)

    assert summary["credential_sources"] == ["alpaca_api_env"]
    assert summary["full_content_articles"] == 1
    assert summary["max_full_article_chars"] == 15177
