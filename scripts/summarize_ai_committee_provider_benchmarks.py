"""Summarize AI Investment Committee provider benchmark artifacts.

The paid benchmark runner writes one ``result.json`` per model. This helper
turns those artifacts into a compact JSON and Markdown report for review.
"""

import argparse
import json
from pathlib import Path
from typing import Any


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


def _compact_result(payload: dict[str, Any]) -> dict[str, Any]:
    backtest = payload.get("backtest_result") or {}
    drawdown = backtest.get("max_drawdown") or {}
    detail = payload.get("agent_detail") or {}
    cost = payload.get("cost") or {}
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
        "input_tokens": (detail.get("usage") or {}).get("input_tokens"),
        "output_tokens": (detail.get("usage") or {}).get("output_tokens"),
        "cached_input_tokens": (detail.get("usage") or {}).get("cached_input_tokens"),
        "estimated_usd": cost.get("estimated_usd"),
        "cache_adjusted_estimated_usd": cost.get("cache_adjusted_estimated_usd"),
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
                f"- Tokens: input {_format_value(item.get('input_tokens'))}, output {_format_value(item.get('output_tokens'))}, cached input {_format_value(item.get('cached_input_tokens'))}",
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
    parser.add_argument("artifact_root", help="Benchmark artifact directory containing per-model result.json files.")
    parser.add_argument("--json-out", help="Optional compact JSON output path.")
    parser.add_argument("--markdown-out", help="Optional Markdown output path.")
    args = parser.parse_args()

    root = Path(args.artifact_root)
    results = _load_results(root)
    compact = [_compact_result(result) for result in results]
    output = {
        "artifact_root": str(root.resolve()),
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
