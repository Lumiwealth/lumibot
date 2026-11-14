"""Shared frame metadata: same content, never rebuilt when pandas derives a frame."""
import copy
import json
import pickle

import pandas as pd
import pytest

from lumibot.tools.frame_metadata import SharedFrameMapping, SharedFrameSequence, share_frame_attrs


def _frame():
    frame = pd.DataFrame({"open": [1.0, 2.0, 3.0], "close": [1.5, 2.5, 3.5]},
                         index=pd.date_range("2025-01-02", periods=3, freq="D", tz="UTC"))
    frame.attrs["ledger"] = {"schema": "v1", "events": [["2024-03-15", 1.5, 0.0], ["2024-06-21", 1.6, 0.0]]}
    frame.attrs["label"] = "kept as is"
    return frame


def test_plain_nested_attrs_are_rebuilt_on_every_derived_frame():
    frame = _frame()
    assert frame[["close"]].attrs["ledger"] is not frame.attrs["ledger"]


def test_shared_attrs_keep_content_and_are_not_rebuilt():
    frame = _frame()
    expected = copy.deepcopy(frame.attrs)
    assert share_frame_attrs(frame) is frame
    ledger = frame.attrs["ledger"]

    assert frame.attrs == expected
    assert isinstance(ledger, dict) and isinstance(ledger["events"], list)
    assert json.loads(json.dumps(frame.attrs)) == expected
    for derived in (frame[["close"]], frame["close"], frame.iloc[1:], frame.loc[frame.index[:2]],
                    frame.copy(), frame.tail(1), frame.dropna()):
        assert derived.attrs["ledger"] is ledger
        assert derived.attrs["label"] == "kept as is"


def test_share_frame_attrs_is_idempotent_and_ignores_empty_attrs():
    frame = share_frame_attrs(_frame())
    ledger = frame.attrs["ledger"]
    share_frame_attrs(frame)
    assert frame.attrs["ledger"] is ledger
    empty = pd.DataFrame({"close": [1.0]})
    assert share_frame_attrs(empty).attrs == {}


def test_shared_metadata_is_read_only_but_replaceable_on_the_frame():
    frame = share_frame_attrs(_frame())
    ledger = frame.attrs["ledger"]
    with pytest.raises(TypeError):
        ledger["schema"] = "v2"
    with pytest.raises(TypeError):
        ledger["events"].append(["2024-09-20", 1.7, 0.0])
    with pytest.raises(TypeError):
        ledger["events"][0][1] = 9.9
    frame.attrs["ledger"] = {"schema": "v2"}
    assert frame.attrs["ledger"] == {"schema": "v2"}


def test_pickle_and_shallow_copy_give_ordinary_independent_containers():
    frame = share_frame_attrs(_frame())
    ledger = frame.attrs["ledger"]
    restored = pickle.loads(pickle.dumps(frame))
    assert restored.attrs == frame.attrs
    plain = copy.copy(ledger)
    assert type(plain) is dict and plain == ledger
    plain["schema"] = "changed"
    assert ledger["schema"] == "v1"
    assert isinstance(ledger, SharedFrameMapping) and isinstance(ledger["events"], SharedFrameSequence)
