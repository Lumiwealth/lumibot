import os
from datetime import datetime, timezone

import pytest

from lumibot.components.agents import BuiltinTools

pytestmark = [pytest.mark.apitest, pytest.mark.alpaca]


class _LiveNewsStrategy:
    is_backtesting = True

    def get_datetime(self):
        return datetime(2024, 8, 6, 16, 0, tzinfo=timezone.utc)


class _LivePaginationStrategy:
    is_backtesting = True

    def get_datetime(self):
        return datetime(2025, 4, 22, 16, 0, tzinfo=timezone.utc)


def _require_alpaca_news_creds(monkeypatch: pytest.MonkeyPatch) -> None:
    api_key = os.environ.get("ALPACA_NEWS_API_KEY") or os.environ.get("ALPACA_TEST_API_KEY")
    api_secret = os.environ.get("ALPACA_NEWS_API_SECRET") or os.environ.get("ALPACA_TEST_API_SECRET")
    if not (api_key and api_secret):
        pytest.skip("Missing Alpaca news credentials")
    monkeypatch.setenv("ALPACA_NEWS_API_KEY", api_key)
    monkeypatch.setenv("ALPACA_NEWS_API_SECRET", api_secret)


def test_live_alpaca_news_known_market_event_scan_and_full_content(monkeypatch):
    """Smoke-test real Alpaca/Benzinga historical news quality.

    The 2024-08-05 market selloff is a known broad-market news day. This verifies
    the tool retrieves relevant historical articles by symbol/date window, keeps
    scan mode light, and can fetch full article bodies when explicitly requested.
    """
    _require_alpaca_news_creds(monkeypatch)

    tool = BuiltinTools.news.alpaca_news().binder(_LiveNewsStrategy(), None)
    scan = tool.function(
        symbols="SPY,QQQ,DIA,IWM",
        start="2024-08-05T00:00:00Z",
        end="2024-08-05T23:59:59Z",
        limit=20,
        include_content=False,
        sort="asc",
    )

    assert scan["ok"] is True
    assert scan["count"] >= 5
    assert scan["query_symbols"] == "SPY,QQQ,DIA,IWM"
    assert scan["lookahead_clamped"] is False
    assert scan["include_content"] is False
    assert scan["content_included"] is False
    assert all("content" not in article for article in scan["articles"])
    assert all(str(article.get("created_at") or "") <= "2024-08-05T23:59:59Z" for article in scan["articles"])

    combined_scan_text = " ".join(
        f"{article.get('headline') or ''} {article.get('summary') or ''}".lower()
        for article in scan["articles"]
    )
    assert any(
        keyword in combined_scan_text
        for keyword in ("vix", "selloff", "recession", "global", "plunge", "volatility")
    )

    full = tool.function(
        symbols="SPY,QQQ,DIA,IWM",
        start="2024-08-05T00:00:00Z",
        end="2024-08-05T23:59:59Z",
        limit=5,
        include_content=True,
        exclude_contentless=True,
        sort="asc",
    )

    full_content_articles = [article for article in full["articles"] if article.get("content")]
    assert full["ok"] is True
    assert full["include_content"] is True
    assert full["content_included"] is True
    assert full["content_available_count"] >= 1
    assert full_content_articles
    assert max(len(str(article["content"])) for article in full_content_articles) > 1000
    assert all(article.get("content_truncated") is False for article in full_content_articles)
    assert all(
        article.get("content_original_length") == len(str(article.get("content") or ""))
        for article in full_content_articles
    )


def test_live_alpaca_news_pagination_fetches_distinct_second_page(monkeypatch):
    """Verify real Alpaca pagination, not just unit-level page_token forwarding."""
    _require_alpaca_news_creds(monkeypatch)

    tool = BuiltinTools.news.alpaca_news().binder(_LivePaginationStrategy(), None)
    first_page = tool.function(
        symbols="SPY,QQQ,DIA,IWM",
        start="2025-04-21T00:00:00Z",
        end="2025-04-21T23:59:59Z",
        limit=5,
        include_content=False,
        sort="asc",
    )

    assert first_page["ok"] is True
    assert first_page["count"] == 5
    assert first_page["next_page_token"]

    second_page = tool.function(
        symbols="SPY,QQQ,DIA,IWM",
        start="2025-04-21T00:00:00Z",
        end="2025-04-21T23:59:59Z",
        limit=5,
        include_content=False,
        sort="asc",
        page_token=first_page["next_page_token"],
    )

    assert second_page["ok"] is True
    assert second_page["count"] >= 1
    first_ids = {article.get("id") for article in first_page["articles"]}
    second_ids = {article.get("id") for article in second_page["articles"]}
    assert first_ids.isdisjoint(second_ids)
