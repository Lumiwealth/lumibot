"""Neutral external-search contracts; no model, broker or network calls."""
import pytest
from lumibot.components.agents import builtins


@pytest.mark.parametrize('backend', ['duckduckgo', 'brave,duckduckgo'])
def test_agent_can_choose_a_documented_search_backend_without_changing_region(monkeypatch, backend):
    calls = []
    def external_search(**kwargs):
        calls.append(kwargs)
        # DDGS text-result fields, as returned by the installed provider.
        return [{'title': 'Public documentation', 'href': 'https://example.test/docs', 'body': 'Reference.'}]
    monkeypatch.setattr(builtins, '_web_search_backend', lambda: external_search)
    result = builtins._run_web_search(query='public documentation', max_results=2,
                                     region='fr-fr', backend=backend)
    assert calls == [{'query': 'public documentation', 'max_results': 2,
                      'region': 'fr-fr', 'backend': backend}]
    assert result['count'] == 1
    assert result['results'][0]['url'] == 'https://example.test/docs'


def test_existing_default_search_call_stays_compatible(monkeypatch):
    calls = []
    def external_search(**kwargs):
        calls.append(kwargs)
        return []
    monkeypatch.setattr(builtins, '_web_search_backend', lambda: external_search)
    assert builtins._run_web_search(query='public documentation')['count'] == 0
    assert calls == [{'query': 'public documentation', 'region': 'wt-wt', 'max_results': 10}]


def test_bound_agent_tool_forwards_backend_and_preserves_provider_errors(monkeypatch):
    calls = []
    error = RuntimeError('Provider backend unavailable')
    def external_search(**kwargs):
        calls.append(kwargs)
        raise error
    monkeypatch.setattr(builtins, '_web_search_backend', lambda: external_search)
    tool = builtins._bind_web_search(None, None)
    with pytest.raises(RuntimeError) as observed:
        tool.function(query='public documentation', backend='duckduckgo')
    assert observed.value is error
    assert calls[0]['backend'] == 'duckduckgo'
