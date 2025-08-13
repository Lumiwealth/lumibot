import os
import pytest

from lumibot.ai.model_client import ProviderRouter


def _has(key):
    return os.environ.get(key) not in (None, "")


@pytest.mark.skipif(not _has("OPENAI_API_KEY"), reason="OPENAI_API_KEY missing; skipping live OpenAI test")
def test_openai_gpt5_nano_search_live():
    router = ProviderRouter(provider="openai", model="gpt-5-nano")
    system = "Use web_search for time-sensitive facts and include citations. Return strict JSON with fields actions, notes, confidence."
    user = "What moved the S&P 500 today? Produce {\"actions\":[],\"notes\":string,\"confidence\": float}."
    decision, diags = router.complete_json(system, user, json_schema={}, search=True)
    assert decision is not None
    assert isinstance(decision, dict)
    assert "notes" in decision
    # Diagnostics
    assert diags.latency_ms >= 0
    assert diags.provider == "openai"
    # If search is enabled, we expect either citations or search_used True
    assert diags.search_used is not False


@pytest.mark.skipif(not (_has("XAI_API_KEY") or _has("GROK_API_KEY")), reason="XAI/GROK API key missing; skipping live xAI test")
def test_xai_grok_search_live():
    router = ProviderRouter(provider="xai", model="grok-4")
    system = "Use Live Search and include citations. Return JSON: actions(list), notes(str), confidence(float)."
    user = "Summarize top AI policy headlines today with sources."
    decision, diags = router.complete_json(system, user, json_schema={}, search=True)
    assert decision is not None
    assert isinstance(decision, dict)
    assert "notes" in decision
    assert diags.provider == "xai"
    assert diags.latency_ms >= 0


@pytest.mark.skipif(not _has("ANTHROPIC_API_KEY"), reason="ANTHROPIC_API_KEY missing; skipping live Anthropic test")
def test_anthropic_search_live():
    # Accept model from env else default in router
    router = ProviderRouter(provider="anthropic")
    system = "Use web search for current facts and add citations. Return JSON with actions, notes, confidence."
    user = "What changed in the Fed's latest statement?"
    decision, diags = router.complete_json(system, user, json_schema={}, search=True)
    assert decision is not None
    assert isinstance(decision, dict)
    assert "notes" in decision
    assert diags.provider == "anthropic"
    assert diags.latency_ms >= 0


@pytest.mark.skipif(not (_has("PPLX_API_KEY") or _has("PERPLEXITY_API_KEY")), reason="Perplexity API key missing; skipping live Perplexity test")
def test_perplexity_search_live():
    router = ProviderRouter(provider="perplexity", model="sonar")
    system = "Be precise. Cite sources. Return JSON with actions, notes, confidence."
    user = "What did the ECB say today?"
    decision, diags = router.complete_json(system, user, json_schema={}, search=True)
    assert decision is not None
    assert isinstance(decision, dict)
    assert "notes" in decision
    assert diags.provider == "perplexity"
    assert diags.latency_ms >= 0
