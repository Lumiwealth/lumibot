import os
from datetime import datetime, timezone

import pytest

from lumibot.components.agents import BuiltinTools


pytestmark = pytest.mark.apitest


class _LiveNewsStrategy:
    is_backtesting = True

    def get_datetime(self):
        return datetime(2024, 8, 6, 16, 0, tzinfo=timezone.utc)


def _require_alpaca_news_creds() -> None:
    has_key = bool(os.environ.get("ALPACA_API_KEY") or os.environ.get("APCA_API_KEY_ID"))
    has_secret = bool(os.environ.get("ALPACA_API_SECRET") or os.environ.get("APCA_API_SECRET_KEY"))
    if not (has_key and has_secret):
        pytest.skip("Missing Alpaca news credentials")


def test_live_alpaca_news_known_market_event_scan_and_full_content():
    """Smoke-test real Alpaca/Benzinga historical news quality.

    The 2024-08-05 market selloff is a known broad-market news day. This verifies
    the tool retrieves relevant historical articles by symbol/date window, keeps
    scan mode light, and can fetch full article bodies when explicitly requested.
    """
    _require_alpaca_news_creds()

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
    assert any(keyword in combined_scan_text for keyword in ("vix", "selloff", "recession", "global", "plunge", "volatility"))

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
    assert all(article.get("content_original_length") == len(str(article.get("content") or "")) for article in full_content_articles)
