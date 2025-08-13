from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


@dataclass
class LLMDiagnostics:
    provider: str
    model: str
    latency_ms: int
    tokens_in: Optional[int] = None
    tokens_out: Optional[int] = None
    tokens_total: Optional[int] = None
    search_used: Optional[bool] = None
    citations_count: Optional[int] = None
    raw_preview_on_error: Optional[str] = None


class LLMClient:
    def complete_json(
        self,
        system: str,
        user: str,
        json_schema: Dict[str, Any],
        max_tokens: int = 800,
        temperature: float = 0.2,
        timeout_s: int = 20,
        search: bool = True,
    ) -> Tuple[Optional[Dict[str, Any]], LLMDiagnostics]:
        raise NotImplementedError


class OpenAIClient(LLMClient):
    def __init__(self, api_key: str, model: str):
        # Lazy import
        from openai import OpenAI  # type: ignore

        self._client = OpenAI(api_key=api_key)
        self._model = model

    def complete_json(self, system, user, json_schema, max_tokens=800, temperature=0.2, timeout_s=20, search=True):
        t0 = time.perf_counter()
        usage = None
        citations = []
        search_used = False
        try:
            tools = [{"type": "web_search"}] if search else []
            resp = self._client.responses.create(
                model=self._model,
                instructions=system,
                input=user,
                tools=tools,
                max_output_tokens=max_tokens,
                temperature=temperature,
                timeout=timeout_s,
                response_format={"type": "json_object"},
            )
            dt = time.perf_counter() - t0
            # Extract JSON text safely
            text = getattr(resp, "output_text", None)
            result = None
            if text:
                try:
                    result = json.loads(text)
                except Exception:
                    result = None

            # Citations via annotations if present
            output = getattr(resp, "output", None) or []
            for block in output:
                if getattr(block, "type", None) == "message":
                    for part in getattr(block, "content", []) or []:
                        anns = getattr(part, "annotations", []) or []
                        for ann in anns:
                            if getattr(ann, "type", None) == "url_citation":
                                citations.append({"title": getattr(ann, "title", None), "url": getattr(ann, "url", None)})
            if citations:
                search_used = True

            usage = getattr(resp, "usage", None)
            diags = LLMDiagnostics(
                provider="openai",
                model=self._model,
                latency_ms=int(dt * 1000),
                tokens_in=getattr(usage, "input_tokens", None) if usage else None,
                tokens_out=getattr(usage, "output_tokens", None) if usage else None,
                tokens_total=getattr(usage, "total_tokens", None) if usage else None,
                search_used=search_used,
                citations_count=len(citations) if citations else 0,
            )
            return result, diags
        except Exception as e:
            dt = time.perf_counter() - t0
            diags = LLMDiagnostics(
                provider="openai",
                model=self._model,
                latency_ms=int(dt * 1000),
                raw_preview_on_error=str(e)[:4000],
            )
            return None, diags


class AnthropicClient(LLMClient):
    def __init__(self, api_key: str, model: str):
        import anthropic  # type: ignore

        self._client = anthropic.Anthropic(api_key=api_key)
        self._model = model

    def complete_json(self, system, user, json_schema, max_tokens=800, temperature=0.2, timeout_s=20, search=True):
        import anthropic  # type: ignore

        t0 = time.perf_counter()
        citations = []
        search_used = False
        try:
            tools = [{"type": "web_search_20250305", "name": "web_search"}] if search else []
            resp = self._client.messages.create(
                model=self._model,
                system=system,
                messages=[{"role": "user", "content": user}],
                max_tokens=max_tokens,
                temperature=temperature,
                tools=tools,
                tool_choice={"type": "auto"} if search else None,
            )
            dt = time.perf_counter() - t0

            # Concatenate text parts
            text_parts = [b.text for b in resp.content if getattr(b, "type", None) == "text"]
            text = "".join(text_parts) if text_parts else None
            result = None
            if text:
                try:
                    result = json.loads(text)
                except Exception:
                    result = None

            # Citations on text blocks
            for b in resp.content:
                if getattr(b, "type", None) == "text" and getattr(b, "citations", None):
                    for c in b.citations:
                        src = getattr(c, "source", None)
                        if src:
                            url = getattr(src, "uri", None) or getattr(src, "url", None)
                            citations.append({"title": getattr(src, "display_name", None), "url": url})
            if citations or (getattr(getattr(resp, "usage", None), "server_tool_use", None) and getattr(resp.usage.server_tool_use, "web_search_requests", 0) > 0):
                search_used = True

            usage = getattr(resp, "usage", None)
            diags = LLMDiagnostics(
                provider="anthropic",
                model=self._model,
                latency_ms=int(dt * 1000),
                tokens_in=getattr(usage, "input_tokens", None) if usage else None,
                tokens_out=getattr(usage, "output_tokens", None) if usage else None,
                tokens_total=None,
                search_used=search_used,
                citations_count=len(citations),
            )
            return result, diags
        except Exception as e:
            dt = time.perf_counter() - t0
            diags = LLMDiagnostics(
                provider="anthropic",
                model=self._model,
                latency_ms=int(dt * 1000),
                raw_preview_on_error=str(e)[:4000],
            )
            return None, diags


class XAIClient(LLMClient):
    def __init__(self, api_key: str, model: str):
        # xAI is OpenAI-compatible at base_url https://api.x.ai
        from openai import OpenAI  # type: ignore

        self._client = OpenAI(api_key=api_key, base_url="https://api.x.ai")
        self._model = model

    def complete_json(self, system, user, json_schema, max_tokens=800, temperature=0.2, timeout_s=20, search=True):
        t0 = time.perf_counter()
        try:
            resp = self._client.chat.completions.create(
                model=self._model,
                messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
                temperature=temperature,
                timeout=timeout_s,
                search_parameters={
                    "mode": "on" if search else "off",
                    "return_citations": True,
                    "max_search_results": 20,
                    "sources": [{"type": "web"}, {"type": "news"}, {"type": "x"}],
                } if search else None,
            )
            dt = time.perf_counter() - t0
            text = resp.choices[0].message.content if resp.choices else None
            result = None
            if text:
                try:
                    result = json.loads(text)
                except Exception:
                    result = None

            citations = getattr(resp, "citations", None) or []
            usage = getattr(resp, "usage", None)
            diags = LLMDiagnostics(
                provider="xai",
                model=self._model,
                latency_ms=int(dt * 1000),
                tokens_in=getattr(usage, "prompt_tokens", None) if usage else None,
                tokens_out=getattr(usage, "completion_tokens", None) if usage else None,
                tokens_total=getattr(usage, "total_tokens", None) if usage else None,
                search_used=True if citations else False,
                citations_count=len(citations) if citations else 0,
            )
            return result, diags
        except Exception as e:
            dt = time.perf_counter() - t0
            diags = LLMDiagnostics(
                provider="xai",
                model=self._model,
                latency_ms=int(dt * 1000),
                raw_preview_on_error=str(e)[:4000],
            )
            return None, diags


class PerplexityClient(LLMClient):
    def __init__(self, api_key: str, model: str):
        import requests  # noqa: F401  # ensure installed
        self._api_key = api_key
        self._model = model

    def complete_json(self, system, user, json_schema, max_tokens=800, temperature=0.2, timeout_s=20, search=True):
        import requests
        t0 = time.perf_counter()
        try:
            headers = {"Authorization": f"Bearer {self._api_key}", "Content-Type": "application/json"}
            body = {
                "model": self._model,
                "messages": [
                    {"role": "system", "content": system + " Return strict JSON."},
                    {"role": "user", "content": user},
                ],
                "temperature": temperature,
                "search_mode": "web" if search else "off",
            }
            r = requests.post("https://api.perplexity.ai/chat/completions", headers=headers, json=body, timeout=timeout_s)
            dt = time.perf_counter() - t0
            r.raise_for_status()
            data = r.json()
            text = data.get("choices", [{}])[0].get("message", {}).get("content")
            result = None
            if text:
                try:
                    result = json.loads(text)
                except Exception:
                    result = None
            citations = data.get("search_results", [])
            usage = data.get("usage", {})
            diags = LLMDiagnostics(
                provider="perplexity",
                model=self._model,
                latency_ms=int(dt * 1000),
                tokens_in=usage.get("prompt_tokens"),
                tokens_out=usage.get("completion_tokens"),
                tokens_total=usage.get("total_tokens"),
                search_used=True if citations else False,
                citations_count=len(citations),
            )
            return result, diags
        except Exception as e:
            dt = time.perf_counter() - t0
            diags = LLMDiagnostics(
                provider="perplexity",
                model=self._model,
                latency_ms=int(dt * 1000),
                raw_preview_on_error=str(e)[:4000],
            )
            return None, diags


class ProviderRouter(LLMClient):
    """Factory router to instantiate a concrete client based on env or overrides."""

    def __init__(self, provider: Optional[str] = None, model: Optional[str] = None, api_key: Optional[str] = None):
        self.provider = (provider or os.environ.get("AI_PROVIDER") or "openai").lower()
        if self.provider == "grok":
            self.provider = "xai"

        # Defaults
        if self.provider == "openai":
            default_model = "gpt-5-nano"
            key = api_key or os.environ.get("OPENAI_API_KEY")
            self._client = OpenAIClient(api_key=key, model=model or default_model)
        elif self.provider == "anthropic":
            default_model = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")
            key = api_key or os.environ.get("ANTHROPIC_API_KEY")
            self._client = AnthropicClient(api_key=key, model=model or default_model)
        elif self.provider == "xai":
            default_model = os.environ.get("XAI_MODEL", "grok-4")
            key = api_key or os.environ.get("XAI_API_KEY") or os.environ.get("GROK_API_KEY")
            self._client = XAIClient(api_key=key, model=model or default_model)
        elif self.provider == "perplexity":
            default_model = os.environ.get("PPLX_MODEL", "sonar")
            key = api_key or os.environ.get("PPLX_API_KEY") or os.environ.get("PERPLEXITY_API_KEY")
            self._client = PerplexityClient(api_key=key, model=model or default_model)
        else:
            raise ValueError(f"Unsupported AI provider: {self.provider}")

    def complete_json(self, *args, **kwargs):
        return self._client.complete_json(*args, **kwargs)
