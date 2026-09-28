"""Reproducible local benchmark for LumiBot's stateful browser runtime."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import math
import platform
import statistics
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

from .browser_tools import BrowserSessionManager, CamoufoxEngine, PatchrightEngine


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return 0.0
    position = (len(ordered) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def summarize_samples(samples: list[dict[str, Any]]) -> dict[str, Any]:
    latencies = [float(sample["open_seconds"]) for sample in samples]
    errors = [str(sample["error"]) for sample in samples if sample.get("error")]
    passed = sum(sample.get("ok") is True for sample in samples)
    count = len(samples)
    return {
        "iterations": count,
        "passed": passed,
        "failed": count - passed,
        "crash_rate": (count - passed) / count if count else 0.0,
        "open_seconds": {
            "min": round(min(latencies), 6) if latencies else 0.0,
            "p50": round(statistics.median(latencies), 6) if latencies else 0.0,
            "p95": round(_percentile(latencies, 0.95), 6),
            "max": round(max(latencies), 6) if latencies else 0.0,
        },
        "peak_rss_bytes": max((int(sample.get("rss_bytes") or 0) for sample in samples), default=0),
        "errors": sorted(set(errors)),
    }


def _process_tree_rss_bytes() -> int:
    try:
        import psutil

        process = psutil.Process()
        processes = [process, *process.children(recursive=True)]
        return sum(item.memory_info().rss for item in processes if item.is_running())
    except Exception:
        return 0


def _package_version(name: str) -> str | None:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return None


def candidate_inventory() -> dict[str, dict[str, Any]]:
    return {
        "patchright": {
            "installed_version": _package_version("patchright"),
            "role": "browser_engine",
            "license": "Apache-2.0",
        },
        "camoufox": {
            "installed_version": _package_version("camoufox"),
            "role": "browser_engine_candidate",
            "license": "MPL-2.0",
        },
        "browser-use": {
            "installed_version": _package_version("browser-use"),
            "role": "orchestration_reference_not_nested_agent",
            "license": "MIT",
        },
    }


def create_engine(name: str):
    normalized = str(name).strip().lower()
    if normalized == "patchright":
        return PatchrightEngine()
    if normalized == "camoufox":
        return CamoufoxEngine()
    raise ValueError(f"Unsupported browser benchmark engine {name!r}.")


def anti_detection_verdict(fingerprint: dict[str, Any] | None) -> dict[str, Any]:
    values = fingerprint or {}
    failures: list[str] = []
    if values.get("webdriver") is not False:
        failures.append("navigator.webdriver exposed")
    if "headless" in str(values.get("userAgent") or "").lower():
        failures.append("headless user agent")
    if int(values.get("plugins") or 0) == 0:
        failures.append("no browser plugins")
    return {"passed": not failures, "failures": failures}


def run_browser_benchmark(*, engine_name: str, iterations: int, state_root: Path) -> dict[str, Any]:
    manager = BrowserSessionManager(engine=create_engine(engine_name), state_root=state_root)
    samples: list[dict[str, Any]] = []
    fingerprint_html = """<!doctype html><pre id='fingerprint'></pre><script>
      document.querySelector('#fingerprint').textContent = JSON.stringify({
        webdriver: navigator.webdriver,
        userAgent: navigator.userAgent,
        languages: navigator.languages,
        plugins: navigator.plugins.length
      });
    </script>"""
    fingerprint_url = f"data:text/html,{quote(fingerprint_html)}"
    fingerprint = None
    for iteration in range(max(int(iterations), 1)):
        started = time.perf_counter()
        session_id = None
        sample: dict[str, Any] = {"iteration": iteration + 1, "ok": False}
        try:
            opened = manager.open(profile="benchmark", headless=True)
            session_id = opened["session_id"]
            sample["open_seconds"] = time.perf_counter() - started
            manager.navigate(session_id, fingerprint_url)
            values = manager.extract(session_id, selector="#fingerprint")["values"]
            if values:
                fingerprint = json.loads(values[0])
            sample["rss_bytes"] = _process_tree_rss_bytes()
            sample["ok"] = True
        except Exception as exc:
            sample.setdefault("open_seconds", time.perf_counter() - started)
            sample["rss_bytes"] = _process_tree_rss_bytes()
            sample["error"] = f"{type(exc).__name__}: {exc}"
        finally:
            if session_id is not None:
                try:
                    manager.close(session_id)
                except Exception as exc:
                    sample["ok"] = False
                    sample["error"] = f"{type(exc).__name__}: {exc}"
            sample["cycle_seconds"] = time.perf_counter() - started
            samples.append(sample)
    return {
        "schema_version": 1,
        "engine": engine_name,
        "host": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python": platform.python_version(),
            "exact_linux_arm64_container": platform.system() == "Linux"
            and platform.machine().lower() in {"arm64", "aarch64"},
        },
        "candidates": candidate_inventory(),
        "fingerprint_probe": fingerprint,
        "anti_detection": anti_detection_verdict(fingerprint),
        "summary": summarize_samples(samples),
        "samples": samples,
    }


def run_patchright_benchmark(*, iterations: int, state_root: Path) -> dict[str, Any]:
    """Backward-compatible wrapper for the original benchmark entry point."""
    return run_browser_benchmark(engine_name="patchright", iterations=iterations, state_root=state_root)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=("patchright", "camoufox"), default="patchright")
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--state-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = run_browser_benchmark(
        engine_name=args.engine,
        iterations=args.iterations,
        state_root=args.state_root,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result["summary"], indent=2, sort_keys=True))
    return 0 if result["summary"]["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
