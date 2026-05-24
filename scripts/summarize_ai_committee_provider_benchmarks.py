"""Summarize AI Investment Committee provider benchmark artifacts.

The paid benchmark runner writes one ``result.json`` per model. This helper
turns those artifacts into a compact JSON and Markdown report for review.
"""

import argparse
import json
from pathlib import Path
from typing import Any


MODEL_PRICE_PER_M_TOKEN: dict[str, dict[str, float]] = {
    "openai/gpt-5.4-mini": {"input": 0.75, "output": 4.50, "cached_input": 0.075},
    "gemini-3.5-flash": {"input": 1.50, "output": 9.00, "cached_input": 0.15},
    "gemini-3-flash-preview": {"input": 0.50, "output": 3.00, "cached_input": 0.05},
    "gemini-3.1-flash-lite": {"input": 0.25, "output": 1.50, "cached_input": 0.025},
    "deepseek/deepseek-v4-flash": {"input": 0.14, "output": 0.28, "cached_input": 0.0028},
    "deepseek/deepseek-v4-pro": {"input": 0.435, "output": 0.87, "cached_input": 0.003625},
    "together_ai/moonshotai/Kimi-K2.5": {"input": 0.50, "output": 2.80},
    "together_ai/Qwen/Qwen3-235B-A22B-Instruct-2507-tput": {"input": 0.20, "output": 0.60},
    "cerebras/gpt-oss-120b": {"input": 0.25, "output": 0.69},
}


def _load_results(root: Path) -> list[dict[str, Any]]:
    results = []
    for result_path in sorted(root.glob("*/result.json")):
        try:
            payload = json.loads(result_path.read_text(encoding="utf-8"))
        except Exception as exc:
            payload = {
                "model": result_path.parent.name,
                "status": "unreadable",
                "error": {"type": exc.__class__.__name__, "message": str(exc)},
            }
        payload["_result_path"] = str(result_path.resolve())
        results.append(payload)
    return results


def _seconds(value: Any) -> float | None:
    try:
        return round(float(value) / 1000, 1)
    except (TypeError, ValueError):
        return None


def _pct(value: Any) -> float | None:
    try:
        return round(float(value) * 100, 3)
    except (TypeError, ValueError):
        return None


def _estimate_cost(model: str | None, usage: dict[str, Any]) -> dict[str, Any] | None:
    if not model:
        return None
    prices = MODEL_PRICE_PER_M_TOKEN.get(model)
    if not prices:
        return None
    input_tokens = int(usage.get("input_tokens") or 0)
    output_tokens = int(usage.get("output_tokens") or 0)
    cached_input_tokens = int(usage.get("cached_input_tokens") or 0)
    uncached_input_tokens = int(usage.get("uncached_input_tokens") or 0)
    if input_tokens <= 0 and (cached_input_tokens > 0 or uncached_input_tokens > 0):
        input_tokens = cached_input_tokens + uncached_input_tokens
    if uncached_input_tokens <= 0:
        uncached_input_tokens = max(input_tokens - cached_input_tokens, 0)
    no_cache = (input_tokens / 1_000_000) * prices["input"] + (output_tokens / 1_000_000) * prices["output"]
    cache_hit_rate = cached_input_tokens / input_tokens if input_tokens > 0 else None
    payload = {
        "estimated_usd": round(no_cache, 6),
        "cache_hit_rate": round(cache_hit_rate, 6) if cache_hit_rate is not None else None,
        "price_source": "summary_static_2026_05_24_provider_docs",
        "uncached_input_tokens": uncached_input_tokens,
        "cached_input_tokens": cached_input_tokens,
    }
    if "cached_input" in prices:
        cache_adjusted = (
            (uncached_input_tokens / 1_000_000) * prices["input"]
            + (cached_input_tokens / 1_000_000) * prices["cached_input"]
            + (output_tokens / 1_000_000) * prices["output"]
        )
        payload["cache_adjusted_estimated_usd"] = round(cache_adjusted, 6)
    return payload


def _compact_result(payload: dict[str, Any]) -> dict[str, Any]:
    backtest = payload.get("backtest_result") or {}
    drawdown = backtest.get("max_drawdown") or {}
    detail = payload.get("agent_detail") or {}
    usage = detail.get("usage") or {}
    cost = _estimate_cost(payload.get("model"), usage) or payload.get("cost") or {}
    error = payload.get("error") or {}
    return {
        "model": payload.get("model"),
        "status": payload.get("status"),
        "window": payload.get("window"),
        "wall_seconds": _seconds(payload.get("wall_ms")),
        "total_return_pct": _pct(backtest.get("total_return")),
        "max_drawdown_pct": _pct(drawdown.get("drawdown")),
        "sharpe": backtest.get("sharpe"),
        "call_count": detail.get("call_count"),
        "tool_call_count": detail.get("tool_call_count"),
        "input_tokens": usage.get("input_tokens"),
        "output_tokens": usage.get("output_tokens"),
        "cached_input_tokens": usage.get("cached_input_tokens"),
        "uncached_input_tokens": usage.get("uncached_input_tokens"),
        "cache_write_input_tokens": usage.get("cache_write_input_tokens"),
        "cache_hit_rate": cost.get("cache_hit_rate"),
        "estimated_usd": cost.get("estimated_usd"),
        "cache_adjusted_estimated_usd": cost.get("cache_adjusted_estimated_usd"),
        "cost_source": cost.get("price_source"),
        "positions": payload.get("positions"),
        "artifact_dir": payload.get("artifact_dir"),
        "result_path": payload.get("_result_path"),
        "error_type": error.get("type"),
        "error_message": error.get("message"),
    }


def _format_value(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.6g}"
    return str(value)


def _write_markdown(compact: list[dict[str, Any]], path: Path) -> None:
    lines = ["# AI Committee Provider Benchmark Summary", ""]
    for item in compact:
        lines.extend(
            [
                f"## {item.get('model')}",
                "",
                f"- Status: {_format_value(item.get('status'))}",
                f"- Window: {_format_value((item.get('window') or {}).get('start'))} to {_format_value((item.get('window') or {}).get('end'))}",
                f"- Wall time: {_format_value(item.get('wall_seconds'))} seconds",
                f"- Return: {_format_value(item.get('total_return_pct'))}%",
                f"- Max drawdown: {_format_value(item.get('max_drawdown_pct'))}%",
                f"- Sharpe: {_format_value(item.get('sharpe'))}",
                f"- Agent calls: {_format_value(item.get('call_count'))}",
                f"- Tool calls: {_format_value(item.get('tool_call_count'))}",
                f"- Tokens: input {_format_value(item.get('input_tokens'))}, output {_format_value(item.get('output_tokens'))}, cached input {_format_value(item.get('cached_input_tokens'))}, uncached input {_format_value(item.get('uncached_input_tokens'))}",
                f"- Cache hit rate: {_format_value(item.get('cache_hit_rate'))}",
                f"- Estimated cost: ${_format_value(item.get('estimated_usd'))}",
                f"- Cache-adjusted cost: ${_format_value(item.get('cache_adjusted_estimated_usd'))}",
            ]
        )
        if item.get("error_type"):
            lines.append(f"- Error: {item.get('error_type')}: {item.get('error_message')}")
        lines.extend(
            [
                f"- Artifact: `{item.get('artifact_dir')}`",
                "",
            ]
        )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize AI committee provider benchmark artifacts.")
    parser.add_argument(
        "artifact_roots",
        nargs="+",
        help="Benchmark artifact directories containing per-model result.json files.",
    )
    parser.add_argument("--json-out", help="Optional compact JSON output path.")
    parser.add_argument("--markdown-out", help="Optional Markdown output path.")
    args = parser.parse_args()

    roots = [Path(root) for root in args.artifact_roots]
    results = []
    for root in roots:
        results.extend(_load_results(root))
    compact = [_compact_result(result) for result in results]
    output = {
        "artifact_roots": [str(root.resolve()) for root in roots],
        "result_count": len(compact),
        "results": compact,
    }
    print(json.dumps(output, indent=2, sort_keys=True))
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(output, indent=2, sort_keys=True), encoding="utf-8")
    if args.markdown_out:
        _write_markdown(compact, Path(args.markdown_out))


if __name__ == "__main__":
    main()
