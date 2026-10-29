"""Streaming observations of actual framework inputs, independent of strategy charts.

This recorder neither fetches data nor changes simulation decisions. It preserves
market columns and observed times, including missing or future rows if an API
actually returned them. Its manifest states the scope and any incomplete capture.
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import sqlite3
import tempfile
import weakref
from collections import OrderedDict
from collections.abc import Mapping, Sequence
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


def _close_spool(connection, directory):
    try:
        connection.close()
    finally:
        directory.cleanup()


class _InputRows(Mapping):
    def __init__(self, recorder):
        self.recorder = recorder

    def __len__(self):
        return self.recorder._row_count

    def __iter__(self):
        for row in self.recorder._read_records("input_rows"):
            yield row["rowSha256"]

    def __getitem__(self, key):
        with self.recorder.lock:
            row = self.recorder._connection.execute(
                "SELECT payload FROM input_rows WHERE fingerprint = ?", (key,)
            ).fetchone()
        if row is None:
            raise KeyError(key)
        return json.loads(row[0])

    def values(self):
        return self.recorder._read_records("input_rows")


class _Observations(Sequence):
    def __init__(self, recorder):
        self.recorder = recorder

    def __len__(self):
        return self.recorder._observation_count

    def __iter__(self):
        return self.recorder._read_records("observations")

    def __getitem__(self, index):
        if isinstance(index, slice):
            return [self[i] for i in range(*index.indices(len(self)))]
        if index < 0:
            index += len(self)
        if index < 0 or index >= len(self):
            raise IndexError(index)
        with self.recorder.lock:
            row = self.recorder._connection.execute(
                "SELECT payload FROM observations WHERE sequence = ?", (index,)
            ).fetchone()
        if row is None:
            raise IndexError(index)
        return json.loads(row[0])


class MarketInputEvidence:
    """Spool complete history to disk with a bounded serialization cache.

    Optional capture ceilings remain available to an explicit caller. They never
    limit the underlying strategy or turn incomplete evidence into a full capture.
    """

    def __init__(self, *, max_rows=None, max_observations=None, max_cache_rows=50_000):
        for name, value in (("max_rows", max_rows), ("max_observations", max_observations),
                            ("max_cache_rows", max_cache_rows)):
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError(f"{name} must be a nonnegative integer or None")
        if max_cache_rows is None:
            raise ValueError("max_cache_rows must be bounded")
        self.max_rows = max_rows
        self.max_observations = max_observations
        self.max_cache_rows = min(max_cache_rows, max_rows) if max_rows is not None else max_cache_rows
        self._serialized_rows = OrderedDict()
        self.incomplete = set()
        self.lock = RLock()
        self._row_count = 0
        self._observation_count = 0
        self._directory = tempfile.TemporaryDirectory(prefix="lumibot-market-input-")
        self.spool_directory = Path(self._directory.name)
        try:
            self._connection = sqlite3.connect(self.spool_directory / "capture.sqlite", check_same_thread=False)
            self._connection.execute("PRAGMA journal_mode = WAL")
            self._connection.execute("PRAGMA synchronous = NORMAL")
            self._connection.execute("PRAGMA cache_size = -2048")
            self._connection.execute("PRAGMA temp_store = FILE")
            self._connection.execute("PRAGMA wal_autocheckpoint = 128")
            self._connection.execute("CREATE TABLE input_rows (fingerprint TEXT PRIMARY KEY, payload TEXT NOT NULL)")
            self._connection.execute("CREATE TABLE observations (fingerprint TEXT UNIQUE, sequence INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
            self._connection.commit()
        except Exception:
            if hasattr(self, "_connection"):
                self._connection.close()
            self._directory.cleanup()
            raise
        self._finalizer = weakref.finalize(self, _close_spool, self._connection, self._directory)
        self.rows = _InputRows(self)
        self.observations = _Observations(self)

    def close(self):
        with self.lock:
            self._finalizer()
            self._serialized_rows.clear()

    def _read_records(self, table):
        # Table names are internal constants, never caller input.
        if table not in {"input_rows", "observations"}:
            raise ValueError("Unknown capture table")
        with self.lock:
            cursor = self._connection.execute(f"SELECT payload FROM {table} ORDER BY rowid")
            try:
                while records := cursor.fetchmany(256):
                    for row in records:
                        yield json.loads(row[0])
            finally:
                cursor.close()

    def record(self, *, asset, timestep, when, frame, purpose, quote=None):
        with self.lock:
            if self.max_observations is not None and self._observation_count >= self.max_observations:
                self.incomplete.add("observation_limit")
                return
            if frame is not None and self.max_rows is not None and len(frame) > self.max_rows:
                self.incomplete.add("row_limit")
                return
            columns = [c for c in frame.columns if c in MARKET_COLUMNS] if frame is not None else []
            if frame is not None and (not isinstance(frame.index, pd.DatetimeIndex) or not frame.index.is_unique):
                self.incomplete.add("unsupported_index")
                return
            if frame is not None and not frame.empty and not columns:
                self.incomplete.add("no_market_columns")
                return
            identity = _encoded(asset_identity(asset, quote))
            observed = when.isoformat()
            context = {"asset": identity, "timestep": str(timestep), "columns": _encoded(columns)}
            timezone_name = str(frame.index.tz) if frame is not None else ""
            digest = hashlib.sha256(('{"columns":' + context["columns"] + ',"rows":[').encode())
            count, first, last, added_rows = 0, None, None, 0
            sequence = self._observation_count
            try:
                with self._connection:
                    if frame is not None:
                        for index, *row in frame[columns].itertuples(name=None):
                            signature = (identity, str(timestep), tuple(columns), timezone_name, index.value,
                                         tuple((type(v), v.hex() if isinstance(v, float) else v) for v in row))
                            try:
                                cached = self._serialized_rows.get(signature)
                            except TypeError:
                                signature, cached = None, None
                            if cached is None:
                                converted = [index.isoformat(), *(_scalar(v) for v in row)]
                                key = hashlib.sha256(_encoded({**context, "values": converted}).encode()).hexdigest()
                                cached = [converted, key, _encoded(converted), False]
                                if signature is not None and self.max_cache_rows:
                                    self._serialized_rows[signature] = cached
                                    if len(self._serialized_rows) > self.max_cache_rows:
                                        self._serialized_rows.popitem(last=False)
                            elif signature is not None:
                                self._serialized_rows.move_to_end(signature)
                            converted, key, encoded, persisted = cached
                            digest.update(("," if count else "").encode())
                            digest.update(encoded.encode())
                            first = converted[0] if first is None else first
                            last = converted[0]
                            count += 1
                            if persisted:
                                continue  # This bounded cache entry already exists in the committed spool.
                            if self.max_rows is not None and self._row_count + added_rows >= self.max_rows:
                                exists = self._connection.execute(
                                    "SELECT 1 FROM input_rows WHERE fingerprint = ?", (key,)
                                ).fetchone()
                                if exists is None:
                                    self.incomplete.add("row_limit")
                                else:
                                    cached[3] = True
                                continue
                            record = {**context, "rowSha256": key, "barTime": converted[0],
                                      "firstObservedAt": observed, "observedSequence": sequence,
                                      **dict(zip(columns, converted[1:]))}
                            cursor = self._connection.execute(
                                "INSERT OR IGNORE INTO input_rows (fingerprint, payload) VALUES (?, ?)",
                                (key, _encoded(record)),
                            )
                            added_rows += cursor.rowcount
                            cached[3] = True
                    digest.update(b"]}")
                    observation = {**context, "purpose": purpose, "observedAt": observed,
                                   "frameSha256": digest.hexdigest(), "returnedRows": count,
                                   "windowStart": first, "windowEnd": last}
                    inserted = self._connection.execute(
                        "INSERT OR IGNORE INTO observations (fingerprint, sequence, payload) VALUES (?, ?, ?)",
                        (_encoded(observation), sequence, _encoded({**observation, "sequence": sequence})),
                    ).rowcount
            except Exception:
                # An uncommitted row must not survive as a persisted cache hit.
                self._serialized_rows.clear()
                raise
            self._row_count += added_rows
            self._observation_count += inserted


def record_market_input(*, source, asset, timestep, when, frame, purpose, quote=None):
    recorder = getattr(source, "_market_input_evidence", None)
    try:
        if recorder is None:
            recorder = MarketInputEvidence()
            source._market_input_evidence = recorder
        recorder.record(asset=asset, timestep=timestep, when=when, frame=frame,
                        purpose=purpose, quote=quote)
    except Exception:
        if recorder is not None:
            with recorder.lock:
                recorder.incomplete.add("capture_error")


def _write_csv(path, fields, rows):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", newline="", encoding="utf-8", dir=path.parent,
                                         prefix=path.name + ".", suffix=".tmp", delete=False) as handle:
            temporary = Path(handle.name)
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return {"bytes": path.stat().st_size, "sha256": digest.hexdigest()}


def write_market_input_evidence(source, directory, *, prefix="", finalize=False):
    recorder = getattr(source, "_market_input_evidence", None)
    if recorder is None:
        return
    try:
        if prefix not in {"", "option_"}:
            raise ValueError("Unsupported input artifact prefix")
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        with recorder.lock:
            manifest_path = directory / (prefix + "market_input_manifest.json")
            manifest_path.unlink(missing_ok=True)  # A failed replacement is never an old complete snapshot.
            context_fields = ["asset", "timestep", "columns"]
            files = {}
            records = (
                ("market_inputs.csv", context_fields + ["rowSha256", "barTime", "firstObservedAt", "observedSequence"]
                 + sorted(MARKET_COLUMNS), recorder.rows.values(), len(recorder.rows)),
                ("market_input_observations.csv", context_fields + ["purpose", "observedAt", "frameSha256", "returnedRows",
                 "windowStart", "windowEnd", "sequence"], iter(recorder.observations), len(recorder.observations)),
            )
            for name, fields, rows, count in records:
                name = prefix + name
                files[name] = {"rows": count, **_write_csv(directory / name, fields, rows)}
            manifest = {"schemaVersion": 1, "producer": "lumibot-framework",
                        "scope": ("Observed market columns from history and indicator inputs; "
                                  "not quotes, broker fills or raw custom data accesses."),
                        "complete": not recorder.incomplete, "incompleteReasons": sorted(recorder.incomplete),
                        "rowLimit": recorder.max_rows, "observationLimit": recorder.max_observations,
                        "serializationCacheLimit": recorder.max_cache_rows, "storage": "disk-spool",
                        "files": files}
            manifest_path.write_text(_encoded(manifest) + "\n", encoding="utf-8")
    finally:
        if finalize:
            recorder.close()
