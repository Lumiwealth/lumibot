"""Bounded observations of actual framework inputs, independent of strategy charts.

This recorder neither fetches data nor changes simulation decisions. It preserves
market columns and observed times, including missing or future rows if an API
actually returned them. Its manifest states the scope and any incomplete capture.
"""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from threading import RLock

import pandas as pd

MARKET_COLUMNS = {"open", "high", "low", "close", "volume", "dividend", "dividends",
                  "stock_splits", "split", "vwap", "bid", "ask"}


def _scalar(value):
    if value is None or pd.isna(value):
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    return value if isinstance(value, (str, int, float, bool)) else str(value)


def asset_identity(asset, quote=None):
    if isinstance(asset, tuple):
        asset, quote = asset
    result = {"symbol": str(getattr(asset, "symbol", asset))}
    for field, label in (("asset_type", "assetType"), ("expiration", "expiration"),
                         ("strike", "strike"), ("right", "right"), ("multiplier", "multiplier")):
        value = getattr(asset, field, None)
        if value is not None and (field != "multiplier" or value != 1):
            result[label] = str(getattr(value, "value", value))
    if quote is not None:
        result["quote"] = asset_identity(quote)
    return result


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


class MarketInputEvidence:
    def __init__(self, *, max_rows=50_000, max_observations=4096):
        self.max_rows = max_rows
        self.max_observations = max_observations
        self.rows = {}
        self.observations = []
        self.seen = set()
        self.incomplete = set()
        self.lock = RLock()

    def record(self, *, asset, timestep, when, frame, purpose, quote=None):
        with self.lock:
            if len(self.observations) >= self.max_observations:
                self.incomplete.add("observation_limit")
                return
            if frame is not None and len(frame) > self.max_rows:
                self.incomplete.add("row_limit")
                return  # Bound conversion memory as well as retained rows.
            columns = [c for c in frame.columns if c in MARKET_COLUMNS] if frame is not None else []
            if frame is not None and (not isinstance(frame.index, pd.DatetimeIndex) or not frame.index.is_unique):
                self.incomplete.add("unsupported_index")
                return
            if frame is not None and not frame.empty and not columns:
                self.incomplete.add("no_market_columns")
                return
            identity = _encoded(asset_identity(asset, quote))
            observed = when.isoformat()
            values = [] if frame is None else [
                [index.isoformat(), *(_scalar(v) for v in row)]
                for index, *row in frame[columns].itertuples(name=None)]
            frame_hash = hashlib.sha256(_encoded({"columns": columns, "rows": values}).encode()).hexdigest()
            context = {"asset": identity, "timestep": str(timestep), "columns": _encoded(columns)}
            observation = {**context, "purpose": purpose, "observedAt": observed,
                "frameSha256": frame_hash, "returnedRows": len(values),
                "windowStart": values[0][0] if values else None,
                "windowEnd": values[-1][0] if values else None}
            observation_key = _encoded(observation)
            if observation_key in self.seen:
                return
            sequence = len(self.observations)
            for row in values:
                key = hashlib.sha256(_encoded({**context, "values": row}).encode()).hexdigest()
                if key not in self.rows:
                    if len(self.rows) >= self.max_rows:
                        self.incomplete.add("row_limit")
                        continue
                    self.rows[key] = {**context, "rowSha256": key, "barTime": row[0],
                        "firstObservedAt": observed, "observedSequence": sequence,
                        **dict(zip(columns, row[1:]))}
            self.seen.add(observation_key)
            self.observations.append({**observation, "sequence": sequence})


def record_market_input(*, source, asset, timestep, when, frame, purpose, quote=None):
    recorder = getattr(source, "_market_input_evidence", None)
    try:
        if recorder is None:
            recorder = MarketInputEvidence()
            source._market_input_evidence = recorder
        recorder.record(asset=asset, timestep=timestep, when=when, frame=frame,
                        purpose=purpose, quote=quote)
    except Exception:
        # Diagnostics must not change orders or data validation. Failure is
        # explicit when a recorder exists, never silently labeled complete.
        if recorder is not None:
            with recorder.lock:
                recorder.incomplete.add("capture_error")


def write_market_input_evidence(source, directory, *, prefix=""):
    recorder = getattr(source, "_market_input_evidence", None)
    if recorder is None:
        return
    if prefix not in {"", "option_"}:
        raise ValueError("Unsupported input artifact prefix")
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    with recorder.lock:
        files = {}
        for name, rows in (("market_inputs.csv", list(recorder.rows.values())),
                           ("market_input_observations.csv", recorder.observations)):
            name = prefix + name
            path = directory / name
            pd.DataFrame(rows).to_csv(path, index=False)
            files[name] = {"rows": len(rows), "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
        manifest = {"schemaVersion": 1, "producer": "lumibot-framework",
            "scope": ("Observed market columns from history and indicator inputs; "
                      "not quotes, broker fills or raw custom data accesses."),
            "complete": not recorder.incomplete, "incompleteReasons": sorted(recorder.incomplete),
            "rowLimit": recorder.max_rows, "observationLimit": recorder.max_observations,
            "files": files}
        manifest_path = directory / (prefix + "market_input_manifest.json")
        manifest_path.write_text(_encoded(manifest) + "\n", encoding="utf-8")
