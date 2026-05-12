#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REQUIRED_INTERVALS = ("minute", "hour", "day")
REQUIRED_IBKR_INDEXES = ("VIX", "SPX", "NDX", "RUT")
REQUIRED_IBKR_STOCKS = ("SPY", "AAPL", "TSLA", "REM", "MOBX")
REQUIRED_IBKR_CRYPTO = ("BTC", "ETH")
REQUIRED_IBKR_SYMBOLS = REQUIRED_IBKR_INDEXES + REQUIRED_IBKR_STOCKS + REQUIRED_IBKR_CRYPTO
REQUIRED_YAHOO_SYMBOLS = REQUIRED_IBKR_INDEXES


def _iso_now() -> str:
    return datetime.now(UTC).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected JSON object in {path}")
    return payload


def _resolve_report_artifact_path(report_dir: Path, raw_path: Any) -> str:
    value = str(raw_path or "").strip()
    if not value:
        return ""

    candidate = Path(value)
    if candidate.exists():
        return str(candidate.resolve())
    if not candidate.is_absolute():
        relative_candidate = (report_dir / candidate).resolve()
        if relative_candidate.exists():
            return str(relative_candidate)

    parts = candidate.parts
    if "artifacts" not in parts:
        return value

    artifacts_index = parts.index("artifacts")
    relocated_candidate = report_dir / Path(*parts[artifacts_index:])
    if relocated_candidate.exists():
        return str(relocated_candidate.resolve())
    return value


def _normalize_report_paths(report: dict[str, Any], report_path: Path) -> dict[str, Any]:
    normalized = dict(report)
    report_dir = report_path.resolve().parent
    path_fields = ("artifact_path", "metadata_path", "validation_log_path")

    def _normalize_rows(rows: Any) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            item = dict(row)
            for field in path_fields:
                if field in item:
                    item[field] = _resolve_report_artifact_path(report_dir, item.get(field))
            out.append(item)
        return out

    normalized["lumibot_matrix"] = _normalize_rows(report.get("lumibot_matrix"))
    normalized["cases"] = _normalize_rows(report.get("cases"))
    normalized["unsupported_limits"] = _normalize_rows(report.get("unsupported_limits"))
    normalized["failure_buckets"] = _normalize_rows(report.get("failure_buckets"))
    return normalized


def _write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    rows = list(rows)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _row_key(provider: str, symbol: str, interval: str) -> tuple[str, str, str]:
    return provider.strip().lower(), symbol.strip().upper(), interval.strip().lower()


def _merge_lumibot_reports(reports: list[tuple[Path, dict[str, Any]]]) -> dict[str, Any]:
    merged_rows: dict[tuple[str, str, str], dict[str, Any]] = {}
    merged_cases: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    failure_buckets: list[dict[str, Any]] = []
    unsupported_limits: dict[tuple[str, str, str], dict[str, Any]] = {}
    lumibot_file: str | None = None

    for report_path, raw_report in reports:
        report = _normalize_report_paths(raw_report, report_path)
        if lumibot_file is None and report.get("lumibot_file"):
            lumibot_file = str(report["lumibot_file"])
        for row in report.get("lumibot_matrix") or []:
            if not isinstance(row, dict):
                continue
            merged_rows[_row_key(str(row.get("provider") or ""), str(row.get("symbol") or ""), str(row.get("interval") or ""))] = dict(row)
        for case in report.get("cases") or []:
            if not isinstance(case, dict):
                continue
            key = (
                str(case.get("provider") or "").strip().lower(),
                str(case.get("symbol") or "").strip().upper(),
                str(case.get("interval") or "").strip().lower(),
                str(case.get("window") or ""),
            )
            merged_cases[key] = dict(case)
        for item in report.get("failure_buckets") or []:
            if not isinstance(item, dict):
                continue
            enriched = dict(item)
            enriched["source"] = "lumibot"
            failure_buckets.append(enriched)
        for row in report.get("unsupported_limits") or []:
            if not isinstance(row, dict):
                continue
            unsupported_limits[_row_key(str(row.get("provider") or ""), str(row.get("symbol") or ""), str(row.get("interval") or ""))] = dict(row)

    return {
        "generated_at": _iso_now(),
        "lumibot_file": lumibot_file,
        "lumibot_matrix": sorted(
            merged_rows.values(),
            key=lambda row: (str(row.get("provider") or ""), str(row.get("symbol") or ""), str(row.get("interval") or "")),
        ),
        "cases": sorted(
            merged_cases.values(),
            key=lambda case: (
                str(case.get("provider") or ""),
                str(case.get("symbol") or ""),
                str(case.get("interval") or ""),
                str(case.get("window") or ""),
            ),
        ),
        "failure_buckets": failure_buckets,
        "unsupported_limits": sorted(
            unsupported_limits.values(),
            key=lambda row: (str(row.get("provider") or ""), str(row.get("symbol") or ""), str(row.get("interval") or "")),
        ),
    }


def _required_downloader_keys() -> set[tuple[str, str, str]]:
    return {_row_key("ibkr", symbol, interval) for symbol in REQUIRED_IBKR_SYMBOLS for interval in REQUIRED_INTERVALS}


def _required_lumibot_keys() -> set[tuple[str, str, str]]:
    keys = {_row_key("ibkr", symbol, interval) for symbol in REQUIRED_IBKR_SYMBOLS for interval in REQUIRED_INTERVALS}
    keys.update({_row_key("yahoo", symbol, interval) for symbol in REQUIRED_YAHOO_SYMBOLS for interval in REQUIRED_INTERVALS})
    return keys


def _row_is_green(row: dict[str, Any] | None) -> bool:
    if not row:
        return False
    return str(row.get("status") or "").strip().lower() in {"pass", "support-limit"}


def _artifact_present(row: dict[str, Any] | None) -> bool:
    path = str((row or {}).get("artifact_path") or "").strip()
    return bool(path) and Path(path).exists()


def _compute_release_readiness(
    *,
    downloader_rows: list[dict[str, Any]],
    lumibot_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    downloader_by_key = {_row_key(str(row.get("provider") or ""), str(row.get("symbol") or ""), str(row.get("interval") or "")): row for row in downloader_rows}
    lumibot_by_key = {_row_key(str(row.get("provider") or ""), str(row.get("symbol") or ""), str(row.get("interval") or "")): row for row in lumibot_rows}

    required_downloader = _required_downloader_keys()
    required_lumibot = _required_lumibot_keys()

    downloader_green = sum(1 for key in required_downloader if _row_is_green(downloader_by_key.get(key)))
    lumibot_green = sum(1 for key in required_lumibot if _row_is_green(lumibot_by_key.get(key)))

    missing_downloader = [key for key in sorted(required_downloader) if not _row_is_green(downloader_by_key.get(key))]
    missing_lumibot = [key for key in sorted(required_lumibot) if not _row_is_green(lumibot_by_key.get(key))]

    missing_index_artifacts = []
    for symbol in REQUIRED_IBKR_INDEXES:
        for interval in REQUIRED_INTERVALS:
            key = _row_key("ibkr", symbol, interval)
            row = lumibot_by_key.get(key)
            if not _artifact_present(row):
                missing_index_artifacts.append(key)

    missing_representative_stock_artifacts = []
    for interval in REQUIRED_INTERVALS:
        if not any(
            _artifact_present(lumibot_by_key.get(_row_key("ibkr", symbol, interval)))
            for symbol in REQUIRED_IBKR_STOCKS
        ):
            missing_representative_stock_artifacts.append(interval)

    missing_representative_crypto_artifacts = []
    for interval in REQUIRED_INTERVALS:
        if not any(
            _artifact_present(lumibot_by_key.get(_row_key("ibkr", symbol, interval)))
            for symbol in REQUIRED_IBKR_CRYPTO
        ):
            missing_representative_crypto_artifacts.append(interval)

    blockers: list[str] = []
    if missing_downloader:
        blockers.append(f"downloader_required_cells_not_green={len(missing_downloader)}")
    if missing_lumibot:
        blockers.append(f"lumibot_required_cells_not_green={len(missing_lumibot)}")
    if missing_index_artifacts:
        blockers.append(f"missing_ibkr_index_artifacts={len(missing_index_artifacts)}")
    if missing_representative_stock_artifacts:
        blockers.append(f"missing_representative_stock_artifacts={','.join(missing_representative_stock_artifacts)}")
    if missing_representative_crypto_artifacts:
        blockers.append(f"missing_representative_crypto_artifacts={','.join(missing_representative_crypto_artifacts)}")

    return {
        "north_star_metric": {
            "numerator": lumibot_green,
            "denominator": len(required_lumibot),
            "value_pct": round((lumibot_green / len(required_lumibot)) * 100.0, 2) if required_lumibot else 0.0,
        },
        "downloader": {
            "green": downloader_green,
            "required": len(required_downloader),
            "missing_or_non_green": missing_downloader,
        },
        "lumibot": {
            "green": lumibot_green,
            "required": len(required_lumibot),
            "missing_or_non_green": missing_lumibot,
        },
        "artifacts": {
            "missing_ibkr_index_artifacts": missing_index_artifacts,
            "missing_representative_stock_artifacts": missing_representative_stock_artifacts,
            "missing_representative_crypto_artifacts": missing_representative_crypto_artifacts,
        },
        "blockers": blockers,
        "ready_to_think_about_deployment": not blockers,
    }


def _build_matrix_markdown(title: str, rows: list[dict[str, Any]], *, include_metadata_paths: bool = False) -> str:
    lines = [f"# {title}", ""]
    if not rows:
        lines.append("_No rows._")
        return "\n".join(lines) + "\n"

    header = [
        "Provider",
        "Symbol",
        "Asset",
        "Interval",
        "Status",
        "Backend",
        "Class",
        "Cache",
        "Max Reach",
        "First Support Limit",
        "Rows",
        "First Timestamp",
        "Last Timestamp",
        "Quality Issues",
        "Failure Bucket",
        "Confidence",
        "Artifact Path",
    ]
    if include_metadata_paths:
        header.extend(["Metadata Path", "Validation Log"])
    lines.append("| " + " | ".join(header) + " |")
    lines.append("| " + " | ".join(["---"] * len(header)) + " |")
    for row in rows:
        values = [
            str(row.get("provider") or ""),
            str(row.get("symbol") or ""),
            str(row.get("asset_type") or ""),
            str(row.get("interval") or ""),
            str(row.get("status") or ""),
            str(row.get("backend") or ""),
            str(row.get("classification") or ""),
            str(row.get("cache_write_policy") or ""),
            str(row.get("max_reachable_window") or ""),
            str(row.get("first_support_limit_window") or ""),
            str(row.get("row_count") or 0),
            str(row.get("first_timestamp") or ""),
            str(row.get("last_timestamp") or ""),
            str(row.get("quality_issues") or "").replace("|", "/"),
            str(row.get("failure_bucket") or ""),
            str(row.get("confidence") or ""),
            str(row.get("artifact_path") or ""),
        ]
        if include_metadata_paths:
            values.extend([
                str(row.get("metadata_path") or ""),
                str(row.get("validation_log_path") or ""),
            ])
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines) + "\n"


def _build_failure_buckets_markdown(rows: list[dict[str, Any]]) -> str:
    lines = ["# Failure Buckets", ""]
    if not rows:
        lines.append("_No failure buckets._")
        return "\n".join(lines) + "\n"
    lines.append("| Source | Provider | Symbol | Interval | Window | Outcome | Failure Bucket | Error | Quality Issues |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for row in rows:
        lines.append(
            "| {source} | {provider} | {symbol} | {interval} | {window} | {outcome} | {failure_bucket} | {error} | {quality_issues} |".format(
                source=row.get("source") or "",
                provider=row.get("provider") or "",
                symbol=row.get("symbol") or "",
                interval=row.get("interval") or "",
                window=row.get("window") or "",
                outcome=row.get("outcome") or "",
                failure_bucket=row.get("failure_bucket") or "",
                error=str(row.get("error") or "").replace("|", "/"),
                quality_issues=str(row.get("quality_issues") or "").replace("|", "/"),
            )
        )
    return "\n".join(lines) + "\n"


def _build_unsupported_limits_markdown(rows: list[dict[str, Any]]) -> str:
    lines = ["# Unsupported Limits", ""]
    if not rows:
        lines.append("_No unsupported limits._")
        return "\n".join(lines) + "\n"
    lines.append("| Provider | Symbol | Asset | Interval | Max Reach | First Support Limit | Status | Confidence |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
    for row in rows:
        lines.append(
            "| {provider} | {symbol} | {asset_type} | {interval} | {max_reachable_window} | {first_support_limit_window} | {status} | {confidence} |".format(
                provider=row.get("provider") or "",
                symbol=row.get("symbol") or "",
                asset_type=row.get("asset_type") or "",
                interval=row.get("interval") or "",
                max_reachable_window=row.get("max_reachable_window") or "",
                first_support_limit_window=row.get("first_support_limit_window") or "",
                status=row.get("status") or "",
                confidence=row.get("confidence") or "",
            )
        )
    return "\n".join(lines) + "\n"


def _build_release_readiness_markdown(readiness: dict[str, Any]) -> str:
    lines = ["# Release Readiness", ""]
    north_star = readiness["north_star_metric"]
    lines.append(f"- North Star: `{north_star['numerator']}/{north_star['denominator']}` required LumiBot cells green (`{north_star['value_pct']}%`).")
    lines.append(f"- Downloader green: `{readiness['downloader']['green']}/{readiness['downloader']['required']}`")
    lines.append(f"- LumiBot green: `{readiness['lumibot']['green']}/{readiness['lumibot']['required']}`")
    lines.append(
        f"- Ready to think about deployment: `{'yes' if readiness['ready_to_think_about_deployment'] else 'no'}`"
    )
    lines.append("")
    lines.append("## Blockers")
    lines.append("")
    if readiness["blockers"]:
        for blocker in readiness["blockers"]:
            lines.append(f"- {blocker}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("## Missing Downloader Or Non-Green Cells")
    lines.append("")
    if readiness["downloader"]["missing_or_non_green"]:
        for provider, symbol, interval in readiness["downloader"]["missing_or_non_green"]:
            lines.append(f"- {provider}:{symbol}:{interval}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("## Missing LumiBot Or Non-Green Cells")
    lines.append("")
    if readiness["lumibot"]["missing_or_non_green"]:
        for provider, symbol, interval in readiness["lumibot"]["missing_or_non_green"]:
            lines.append(f"- {provider}:{symbol}:{interval}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("## Missing Required IBKR Index Artifacts")
    lines.append("")
    if readiness["artifacts"]["missing_ibkr_index_artifacts"]:
        for provider, symbol, interval in readiness["artifacts"]["missing_ibkr_index_artifacts"]:
            lines.append(f"- {provider}:{symbol}:{interval}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("## Missing Representative Stock Artifacts")
    lines.append("")
    if readiness["artifacts"]["missing_representative_stock_artifacts"]:
        for interval in readiness["artifacts"]["missing_representative_stock_artifacts"]:
            lines.append(f"- {interval}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("## Missing Representative Crypto Artifacts")
    lines.append("")
    if readiness["artifacts"]["missing_representative_crypto_artifacts"]:
        for interval in readiness["artifacts"]["missing_representative_crypto_artifacts"]:
            lines.append(f"- {interval}")
    else:
        lines.append("- none")
    lines.append("")
    return "\n".join(lines) + "\n"


def _write_release_package(
    *,
    outdir: Path,
    downloader_report: dict[str, Any],
    lumibot_report: dict[str, Any],
    readiness: dict[str, Any],
) -> None:
    outdir.mkdir(parents=True, exist_ok=True)

    downloader_rows = list(downloader_report.get("downloader_matrix") or [])
    lumibot_rows = list(lumibot_report.get("lumibot_matrix") or [])
    combined_failure_buckets = []
    for row in downloader_report.get("failure_buckets") or []:
        if not isinstance(row, dict):
            continue
        enriched = dict(row)
        enriched["source"] = "downloader"
        enriched.setdefault("provider", "ibkr")
        combined_failure_buckets.append(enriched)
    for row in lumibot_report.get("failure_buckets") or []:
        if not isinstance(row, dict):
            continue
        enriched = dict(row)
        enriched["source"] = "lumibot"
        combined_failure_buckets.append(enriched)

    combined_unsupported_limits = list(downloader_report.get("unsupported_limits") or []) + list(lumibot_report.get("unsupported_limits") or [])

    (outdir / "downloader_matrix.json").write_text(
        json.dumps(
            {
                "generated_at": _iso_now(),
                "downloader_matrix": downloader_rows,
                "cases": downloader_report.get("cases") or [],
                "failure_buckets": downloader_report.get("failure_buckets") or [],
                "unsupported_limits": downloader_report.get("unsupported_limits") or [],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    _write_csv(outdir / "downloader_matrix.csv", downloader_rows)
    (outdir / "downloader_matrix.md").write_text(
        _build_matrix_markdown("Downloader Matrix", downloader_rows),
        encoding="utf-8",
    )

    (outdir / "lumibot_matrix.json").write_text(
        json.dumps(lumibot_report, indent=2) + "\n",
        encoding="utf-8",
    )
    _write_csv(outdir / "lumibot_matrix.csv", lumibot_rows)
    (outdir / "lumibot_matrix.md").write_text(
        _build_matrix_markdown("LumiBot Matrix", lumibot_rows, include_metadata_paths=True),
        encoding="utf-8",
    )

    (outdir / "failure_buckets.md").write_text(
        _build_failure_buckets_markdown(combined_failure_buckets),
        encoding="utf-8",
    )
    (outdir / "unsupported_limits.md").write_text(
        _build_unsupported_limits_markdown(combined_unsupported_limits),
        encoding="utf-8",
    )
    (outdir / "release_readiness.md").write_text(
        _build_release_readiness_markdown(readiness),
        encoding="utf-8",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Assemble the final LumiBot deployment-readiness package from downloader and LumiBot matrix reports.")
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--downloader-json", required=True)
    parser.add_argument("--lumibot-json", nargs="+", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    downloader_report = _read_json(Path(args.downloader_json))
    lumibot_reports = [(Path(path), _read_json(Path(path))) for path in args.lumibot_json]
    merged_lumibot = _merge_lumibot_reports(lumibot_reports)
    readiness = _compute_release_readiness(
        downloader_rows=list(downloader_report.get("downloader_matrix") or []),
        lumibot_rows=list(merged_lumibot.get("lumibot_matrix") or []),
    )
    _write_release_package(
        outdir=Path(args.outdir),
        downloader_report=downloader_report,
        lumibot_report=merged_lumibot,
        readiness=readiness,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
