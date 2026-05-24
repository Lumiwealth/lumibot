#!/usr/bin/env python3
"""Run real provider calls to verify prompt-cache telemetry.

This intentionally bypasses LumiBot's replay cache and calls GoogleADKRuntime
directly. It sends the same large static instruction prefix multiple times with
small changing tasks at the end, then prints provider-reported cached-token
counts and latency. It does not estimate pricing.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

os.environ.setdefault("LUMIBOT_LOG_LEVEL", "WARNING")

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lumibot.components.agents.manager import _usage_breakdown
from lumibot.components.agents.runtime import GoogleADKRuntime, RuntimeRequest


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and value and key not in os.environ:
            os.environ[key] = value


def _load_known_env_files() -> None:
    for path in (
        REPO_ROOT / ".env.local",
        Path("/Users/robertgrzesik/Development/botspot_node/.env-local"),
        Path("/Users/robertgrzesik/Development/lumibot_strategy_tester/.env"),
    ):
        _load_env_file(path)
    if not os.environ.get("GOOGLE_API_KEY") and os.environ.get("GEMINI_API_KEY"):
        os.environ["GOOGLE_API_KEY"] = os.environ["GEMINI_API_KEY"]
    if not os.environ.get("XAI_API_KEY") and os.environ.get("GROK_API_KEY"):
        os.environ["XAI_API_KEY"] = os.environ["GROK_API_KEY"]


def _require_key_for_model(model: str) -> None:
    lower = model.lower()
    if lower.startswith("gemini-") or lower.startswith("models/gemini"):
        keys = ("GEMINI_API_KEY", "GOOGLE_API_KEY")
    elif lower.startswith("openai/"):
        keys = ("OPENAI_API_KEY",)
    elif lower.startswith("xai/"):
        keys = ("XAI_API_KEY", "GROK_API_KEY")
    elif lower.startswith("anthropic/"):
        keys = ("ANTHROPIC_API_KEY",)
    elif lower.startswith("deepseek/"):
        keys = ("DEEPSEEK_API_KEY",)
    else:
        keys = ()
    if keys and not any(os.environ.get(key) for key in keys):
        raise RuntimeError(f"Missing provider API key for model {model}: expected one of {', '.join(keys)}")


def _static_prompt(repetitions: int) -> str:
    paragraph = (
        "You are a LumiBot cache probe. This static instruction block is intentionally long, "
        "repeated, and identical across calls. The only changing information should appear in "
        "the user task after this instruction. Do not call tools. Reply with a single sentence "
        "starting with RESULT: and include the requested call number. "
    )
    return "\n".join([paragraph] * repetitions)


def _run_probe(model: str, calls: int, repetitions: int) -> dict[str, Any]:
    runtime = GoogleADKRuntime()
    system_prompt = _static_prompt(repetitions)
    cache_key = f"lumibot-cache-probe:{model}:{repetitions}"
    rows: list[dict[str, Any]] = []
    for idx in range(1, calls + 1):
        started = time.perf_counter()
        result = runtime.run(
            RuntimeRequest(
                agent_name="cache_probe",
                model=model,
                system_prompt=system_prompt,
                task_prompt=f"Return RESULT: cache probe call {idx}.",
                context={"call_index": idx},
                runtime_context={"mode": "backtesting", "current_datetime": "2025-04-21T09:30:00-04:00"},
                memory_notes=[],
                bound_tools=[],
                provider_prompt_cache_key=cache_key,
            )
        )
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        usage = _usage_breakdown(result.usage or {}, cache_hit=False)
        rows.append(
            {
                "call_index": idx,
                "input_tokens": usage["input_tokens"],
                "cached_input_tokens": usage["cached_input_tokens"],
                "uncached_input_tokens": usage["uncached_input_tokens"],
                "cache_write_input_tokens": usage["cache_write_input_tokens"],
                "output_tokens": usage["output_tokens"],
                "thinking_tokens": usage["thinking_tokens"],
                "total_tokens": usage["total_tokens"],
                "latency_ms": result.latency_ms if result.latency_ms is not None else elapsed_ms,
                "first_event_latency_ms": result.first_event_latency_ms,
                "summary": result.summary or result.text,
            }
        )
    return {
        "model": model,
        "calls": calls,
        "static_prompt_chars": len(system_prompt),
        "provider_prompt_cache_key": cache_key,
        "rows": rows,
        "cached_tokens_observed": any(row["cached_input_tokens"] > 0 for row in rows[1:]),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default=os.environ.get("AGENT_MODEL", "gemini-3.1-flash-lite-preview"))
    parser.add_argument("--calls", type=int, default=3)
    parser.add_argument("--repetitions", type=int, default=120)
    args = parser.parse_args()

    _load_known_env_files()
    _require_key_for_model(args.model)
    summary = _run_probe(args.model, calls=args.calls, repetitions=args.repetitions)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
