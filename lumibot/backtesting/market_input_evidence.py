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

import numpy as np
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


def _frame_rows(frame, columns):
    """``frame[columns].itertuples(name=None)`` without building a Series per column.

    For plain numpy columns, ``ndarray.tolist()`` gives the same Python scalars that
    iterating the Series gives (both use ``ndarray.item``), and iterating the index gives the
    same Timestamps. Duplicate column names and extension dtypes (nullable, categorical,
    datetime) keep the itertuples path, so every row is exactly what it was.
    """
    if not columns or not frame.columns.is_unique:
        return frame[columns].itertuples(name=None)
    values = []
    for column in columns:
        array = frame[column]._values
        if not isinstance(array, np.ndarray) or array.dtype.kind not in "biufcO":
            return frame[columns].itertuples(name=None)
        values.append(array.tolist())
    return zip(frame.index, *values)


def _plain_arrays(frame, columns):
    """The numpy arrays ``_frame_rows`` reads directly, or None when it would use itertuples.

    Object columns are excluded too: comparing them element by element is not cheaper than
    serializing them.
    """
    if not columns or not frame.columns.is_unique:
        return None
    arrays = []
    for column in columns:
        array = frame[column]._values
        if not isinstance(array, np.ndarray) or array.dtype.kind not in "biuf":
            return None
        arrays.append(array)
    return arrays


def _bits(array):
    """Exact identity view: floats compare by bit pattern, so -0.0, 0.0 and NaN stay distinct."""
    if array.dtype.kind == "f":
        return array.view(f"u{array.dtype.itemsize}")
    return array


class _SeenRows:
    """Rows one stream already serialized and committed, kept as compact bytes.

    ``blob`` is the comma-joined encoded rows exactly as they enter a frame digest;
    ``starts``/``ends`` locate each row in it. Values are compared by bit pattern before any
    row is reused, so a reused row is the same row, byte for byte.
    """

    __slots__ = ("rows", "index_dtype", "index", "dtypes", "arrays", "blob", "starts", "ends", "digest")

    def __init__(self, frame, arrays, blob, starts, ends, digest):
        self.rows = len(frame)
        self.index_dtype = frame.index.dtype
        self.index = frame.index.asi8.copy()
        self.dtypes = tuple(array.dtype for array in arrays)
        self.arrays = [_bits(array).copy() for array in arrays]
        # Frame digest after all of these rows, before it closes; a frame that starts with
        # exactly these rows (a growing history) continues it instead of rehashing them.
        self.blob, self.starts, self.ends, self.digest = blob, starts, ends, digest

    def overlap(self, frame, arrays):
        """``(k, n)``: frame rows ``[0, n)`` equal this stream's rows ``[k, k + n)``, else None.

        The overlap reaches the end of the frame or the end of the stored rows, so every frame
        row after it is new to this stream.
        """
        if frame.index.dtype != self.index_dtype or self.dtypes != tuple(array.dtype for array in arrays):
            return None
        index = frame.index.asi8
        k = int(np.searchsorted(self.index, index[0]))
        if k >= self.rows or self.index[k] != index[0]:
            return None
        n = min(self.rows - k, len(frame))
        if not np.array_equal(index[:n], self.index[k:k + n]):
            return None
        if not all(np.array_equal(_bits(array)[:n], stored[k:k + n]) for array, stored in zip(arrays, self.arrays)):
            return None
        return k, n

    def piece(self, k, n):
        origin = self.starts[k]
        return self.blob[origin:self.ends[k + n - 1]], self.starts[k:k + n] - origin, self.ends[k:k + n] - origin


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

    # Indicator inputs hand over the whole visible history on every call, one bar longer (or
    # one bar later) each time. Rows this stream already serialized and committed are reused
    # instead of being re-read in Python. Off only for reference tests.
    incremental = True
    max_prefix_states = 32
    max_prefix_states_per_stream = 4
    max_prefix_rows = 1_000_000

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
        self._prefix_states = OrderedDict()
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
            stream = arrays = reused = open_digest = None
            new_encoded = []
            if frame is not None and not frame.empty and self.incremental and self.max_rows is None:
                arrays = _plain_arrays(frame, columns)
                if arrays is not None:
                    stream = (identity, str(timestep), tuple(columns), timezone_name)
                    for state in self._prefix_states.get(stream, ()):
                        span = state.overlap(frame, arrays)
                        if span is not None:
                            reused = (state, *state.piece(*span), sum(span) >= state.rows and len(frame) >= state.rows)
                            # These rows are already in the committed spool; only the digest needs them.
                            if span == (0, state.rows):
                                digest = state.digest.copy()
                            else:
                                digest.update(reused[1])
                            count = span[1]
                            first, last = frame.index[0].isoformat(), frame.index[count - 1].isoformat()
                            break
            try:
                with self._connection:
                    if frame is not None:
                        unread = frame.iloc[count:] if count else frame
                        for index, *row in _frame_rows(unread, columns):
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
                            if stream is not None:
                                new_encoded.append(encoded)
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
                    if stream is not None:
                        open_digest = digest.copy()
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
            if stream is not None:
                self._remember_rows(stream, frame, arrays, reused, new_encoded, open_digest)

    def _remember_rows(self, stream, frame, arrays, reused, new_encoded, open_digest):
        candidates = self._prefix_states.pop(stream, [])
        if len(frame) > self.max_prefix_rows:
            if candidates:
                self._prefix_states[stream] = candidates
            return
        lengths = np.fromiter((len(text) for text in new_encoded), dtype=np.int64, count=len(new_encoded))
        added = ",".join(new_encoded).encode()  # json.dumps output is ASCII: one byte per character
        if reused is None:
            blob, starts, ends = b"", np.empty(0, np.int64), np.empty(0, np.int64)
        else:
            state, blob, starts, ends, replaces = reused
            if replaces:  # the new entry is at least as long and continues past the old one
                candidates = [entry for entry in candidates if entry is not state]
        if added:
            offset = len(blob) + 1 if blob else 0
            blob = blob + b"," + added if blob else added
            new_starts = offset + np.concatenate(([0], np.cumsum(lengths[:-1] + 1))).astype(np.int64)
            starts = np.concatenate((starts, new_starts))
            ends = np.concatenate((ends, new_starts + lengths))
        candidates.insert(0, _SeenRows(frame, arrays, blob, starts, ends, open_digest))
        states = self._prefix_states
        states[stream] = candidates[: self.max_prefix_states_per_stream]
        total = sum(state.rows for entries in states.values() for state in entries)
        while len(states) > self.max_prefix_states or total > self.max_prefix_rows:
            _, dropped = states.popitem(last=False)
            total -= sum(state.rows for state in dropped)


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
