#!/usr/bin/env python3
"""Read-only structural audit for local or configured S3 IBKR cache objects."""

from __future__ import annotations

import argparse
import io
import json
import os
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

# Audits must not discover project dotenv files or initialize a configured live
# broker while importing LumiBot cache utilities.
os.environ.setdefault("LUMIBOT_DISABLE_DOTENV", "1")

from lumibot.constants import LUMIBOT_CACHE_FOLDER
from lumibot.tools.backtest_cache import CacheMode, get_backtest_cache
from lumibot.tools.ibkr_history_health import audit_ibkr_cache_frame


AuditRecord = tuple[str, pd.DataFrame | None, str | None]


def _local_records(root: Path, limit: int | None) -> Iterable[AuditRecord]:
    paths = sorted((root / "ibkr").glob("*.parquet"))
    for path in paths[:limit]:
        try:
            yield path.as_posix(), pd.read_parquet(path), None
        except Exception as exc:
            yield path.as_posix(), None, str(exc)[:500]


def _remote_keys(client: Any, *, bucket: str, prefix: str) -> Iterable[str]:
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        response = client.list_objects_v2(**kwargs)
        for item in response.get("Contents") or []:
            key = str(item.get("Key") or "")
            if key.endswith(".parquet"):
                yield key
        if not response.get("IsTruncated"):
            break
        token = response.get("NextContinuationToken")


def _remote_records(limit: int | None) -> Iterable[AuditRecord]:
    manager = get_backtest_cache()
    if not manager.enabled or manager.mode is not CacheMode.S3_READONLY:
        raise RuntimeError("Remote audit requires LUMIBOT_CACHE_MODE=s3_readonly")
    settings = manager._settings
    if settings is None or not settings.bucket:
        raise RuntimeError("S3 cache settings are incomplete")
    prefix = "/".join(
        component
        for component in (settings.prefix.strip("/"), settings.version.strip("/"), "ibkr")
        if component
    )
    client = manager._get_client()
    for index, key in enumerate(_remote_keys(client, bucket=settings.bucket, prefix=prefix)):
        if limit is not None and index >= limit:
            break
        try:
            response = client.get_object(Bucket=settings.bucket, Key=key)
            body = response["Body"]
            try:
                payload = body.read()
            finally:
                close = getattr(body, "close", None)
                if callable(close):
                    close()
            yield key, pd.read_parquet(io.BytesIO(payload)), None
        except Exception as exc:
            yield key, None, str(exc)[:500]


def run_audit(*, remote: bool, limit: int | None = None) -> dict[str, Any]:
    records = _remote_records(limit) if remote else _local_records(Path(LUMIBOT_CACHE_FOLDER), limit)
    objects = []
    errors = []
    for identifier, frame, read_error in records:
        if read_error is not None or frame is None:
            errors.append({"object": identifier, "error": read_error or "empty dataframe"})
            continue
        try:
            objects.append({"object": identifier, **audit_ibkr_cache_frame(frame)})
        except Exception as exc:
            errors.append({"object": identifier, "error": str(exc)[:500]})
    return {
        "mode": "s3_readonly" if remote else "local",
        "objects": len(objects),
        "read_errors": errors,
        "placeholder_objects": sum(value["placeholder_rows"] > 0 for value in objects),
        "all_placeholder_objects": sum(value["all_placeholder"] for value in objects),
        "duplicate_timestamp_objects": sum(value["duplicate_timestamps"] > 0 for value in objects),
        "non_monotonic_objects": sum(not value["monotonic"] for value in objects),
        "real_ohlc_null_objects": sum(value["real_ohlc_null_rows"] > 0 for value in objects),
        "details": objects,
    }


def _nonnegative_int(value: str) -> int:
    result = int(value)
    if result < 0:
        raise argparse.ArgumentTypeError("--limit must be nonnegative")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--remote", action="store_true", help="Audit configured S3 cache read-only")
    parser.add_argument("--limit", type=_nonnegative_int, default=None)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--quiet", action="store_true", help="Do not print the JSON report")
    args = parser.parse_args()
    result = run_audit(remote=args.remote, limit=args.limit)
    encoded = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded + "\n", encoding="utf-8")
    if not args.quiet:
        print(encoded)
    return 0 if not result["read_errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
