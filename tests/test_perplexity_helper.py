from __future__ import annotations

from types import SimpleNamespace
from typing import Any, ClassVar

import pytest

from lumibot.components import perplexity_helper


class _FakeOpenAI:
    last: ClassVar[_FakeOpenAI | None] = None

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.calls: list[dict[str, Any]] = []
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self.create))
        _FakeOpenAI.last = self

    def create(self, **payload: Any) -> Any:
        self.calls.append(payload)
        schema = payload.get("response_format", {}).get("json_schema", {}).get("schema", {})
        required = schema.get("required", []) if isinstance(schema, dict) else []
        if "analysis_summary" in required:
            return [
                SimpleNamespace(
                    choices=[
                        SimpleNamespace(
                            delta=SimpleNamespace(content='{"query": "q", "analysis_summary": "ok", "items": []}')
                        )
                    ]
                )
            ]
        if payload.get("stream"):
            return [
                SimpleNamespace(choices=[SimpleNamespace(delta=SimpleNamespace(content='{"query": "q", '))]),
                SimpleNamespace(choices=[SimpleNamespace(delta=SimpleNamespace(content='"response_summary": "ok", '))]),
                SimpleNamespace(choices=[SimpleNamespace(delta=SimpleNamespace(content='"symbols": []}'))]),
            ]
        return SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        content='{"query": "q", "analysis_summary": "ok", "items": []}',
                    )
                )
            ]
        )


@pytest.fixture(autouse=True)
def fake_openai(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeOpenAI.last = None
    monkeypatch.setattr(perplexity_helper, "_openai_client_class", lambda: _FakeOpenAI)


def test_execute_general_query_uses_streaming_and_response_format() -> None:
    helper = perplexity_helper.PerplexityHelper("test-key")

    result = helper.execute_general_query("q")

    assert result == {"query": "q", "response_summary": "ok", "symbols": []}
    assert _FakeOpenAI.last is not None
    payload = _FakeOpenAI.last.calls[-1]
    assert payload["stream"] is True
    assert payload["max_tokens"] == 8000
    assert payload["response_format"]["type"] == "json_schema"


def test_execute_financial_news_query_non_streaming_parse() -> None:
    helper = perplexity_helper.PerplexityHelper("test-key")

    result = helper.execute_financial_news_query("q")

    assert result == {"query": "q", "analysis_summary": "ok", "items": []}


def test_post_process_financial_news_data_coerces_numbers() -> None:
    helper = perplexity_helper.PerplexityHelper("test-key")
    data = {
        "items": [
            {
                "confidence": "9",
                "sentiment_score": "-2",
                "popularity_metric": "12",
                "magnitude": "bad",
                "volume_of_messages": "4",
                "price_targets": {"low": "10.5", "high": "bad", "average": 15},
            }
        ]
    }

    helper._post_process_data(data)

    item = data["items"][0]
    assert item["confidence"] == 9
    assert item["sentiment_score"] == -2
    assert item["popularity_metric"] == 12
    assert item["magnitude"] == 0
    assert item["volume_of_messages"] == 4
    assert item["price_targets"] == {"low": 10.5, "high": None, "average": 15.0}


def test_non_object_json_returns_error_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    class ListJsonOpenAI(_FakeOpenAI):
        def create(self, **payload: Any) -> Any:
            self.calls.append(payload)
            return [SimpleNamespace(choices=[SimpleNamespace(delta=SimpleNamespace(content='["not", "object"]'))])]

    monkeypatch.setattr(perplexity_helper, "_openai_client_class", lambda: ListJsonOpenAI)
    helper = perplexity_helper.PerplexityHelper("test-key")

    result = helper.execute_general_query("q")

    assert result["response_summary"] == "Error: LLM output was not a JSON object."
