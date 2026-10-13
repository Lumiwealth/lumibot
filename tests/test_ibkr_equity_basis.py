"""Price-basis provenance must never be inferred from price shape."""

from dataclasses import FrozenInstanceError
import json

import pandas as pd
import pytest

from lumibot.tools.ibkr_equity_basis import (
    EquityBasisError,
    EquityBasisSnapshot,
    assert_same_basis,
    decorate_adjusted_frame,
)


def actions(rows=()):
    return pd.DataFrame(
        [(dividend, split) for _, dividend, split in rows],
        index=pd.DatetimeIndex([date for date, _, _ in rows]),
        columns=["Dividends", "Stock Splits"],
    )


def snapshot(rows=(), **kwargs):
    return EquityBasisSnapshot.from_verified_actions(
        actions(rows),
        as_of=kwargs.get("as_of", "2024-11-10T12:00:00Z"),
        coverage_through=kwargs.get("coverage_through", "2024-11-08"),
    )


def prices():
    return pd.DataFrame({"open": [100.0, 1000.0], "close": [120.0, 2.0],
                         "volume": [10.0, -1.0], "dividend": [0.0, 0.5]},
                        index=pd.date_range("2024-06-07", periods=2, tz="UTC"))


def decorated(basis=None):
    return decorate_adjusted_frame(prices(), basis or snapshot(), provider_prices_verified=True)


def test_dividend_changes_do_not_invalidate_split_basis():
    assert snapshot().split_fingerprint == snapshot([("2024-06-11", 0.01, 0)]).split_fingerprint
    first = snapshot([("2024-06-10", 0, 10), ("2024-06-11", 0.01, 0)])
    corrected = snapshot([("2024-06-10", 0, 10), ("2024-06-11", 0.02, 0)])
    assert first.split_fingerprint == corrected.split_fingerprint
    assert first.events != corrected.events


def test_forward_reverse_and_removed_events_change_basis():
    empty = snapshot()
    forward = snapshot([("2024-06-10", 0, 10)])
    reverse = snapshot([("2024-11-07", 0, 0.2)])
    both = snapshot([("2024-06-10", 0, 10), ("2024-11-07", 0, 0.2)])
    assert len({item.split_fingerprint for item in (empty, forward, reverse, both)}) == 4
    assert reverse.events == (("2024-11-07", 0.0, 0.2),)


def test_future_events_do_not_enter_effective_ledger_or_fingerprint():
    basis = snapshot([("2024-12-01", 1.0, 2.0)])
    assert basis.events == ()
    assert basis.split_fingerprint == snapshot().split_fingerprint


def test_row_order_and_no_op_rows_do_not_change_fingerprint():
    rows = [("2024-06-10", 0, 10), ("2024-11-07", 0, 0.2)]
    assert snapshot(rows).split_fingerprint == snapshot(rows[::-1]).split_fingerprint
    assert snapshot().split_fingerprint == snapshot([("2024-06-10", 0, 1)]).split_fingerprint


def test_snapshot_is_immutable_and_serialization_detached():
    basis = snapshot([("2024-06-10", 0, 10)])
    with pytest.raises(FrozenInstanceError):
        basis.coverage_through = "2099-01-01"
    saved = basis.to_dict()
    assert json.loads(json.dumps(saved))["split_fingerprint"] == basis.split_fingerprint
    assert "provider_prices_verified" not in saved
    saved["events"][0][2] = 99
    assert basis.events[0][2] == 10


def test_direct_constructor_cannot_smuggle_mutable_events():
    with pytest.raises(EquityBasisError):
        EquityBasisSnapshot("2024-11-10T12:00:00+00:00", "2024-11-08", [["2024-06-10", 0, 10]])


def test_direct_constructor_cannot_smuggle_future_events():
    with pytest.raises(EquityBasisError):
        EquityBasisSnapshot("2024-11-10T12:00:00+00:00", "2024-11-08", (("2024-12-10", 0, 10),))


def test_integer_and_float_ratios_identify_same_basis():
    direct = EquityBasisSnapshot("2024-11-10T12:00:00+00:00", "2024-11-08", (("2024-06-10", 0, 10),))
    assert direct.split_fingerprint == snapshot([("2024-06-10", 0, 10)]).split_fingerprint


def test_actions_frame_roundtrip_and_input_mutation_are_isolated():
    supplied = actions([("2024-06-10", 0, 10), ("2024-06-11", 0.01, 0)])
    basis = EquityBasisSnapshot.from_verified_actions(supplied, as_of="2024-11-10T12:00:00Z", coverage_through="2024-11-08")
    supplied.iloc[0, 1] = 200
    frame = basis.actions_frame()
    assert frame.loc[pd.Timestamp("2024-06-10", tz="America/New_York"), "Stock Splits"] == 10
    frame.iloc[0, 1] = 300
    assert basis.actions_frame().iloc[0, 1] == 10


@pytest.mark.parametrize("column", ["Dividends", "Stock Splits"])
@pytest.mark.parametrize("bad", [-1, float("nan"), float("inf"), -float("inf"), True, "bad"])
def test_invalid_actions_rejected_including_future_rows(column, bad):
    frame = actions([("2024-12-01", 0, 2)])
    frame[column] = pd.Series([bad], index=frame.index, dtype=object)
    with pytest.raises(EquityBasisError):
        EquityBasisSnapshot.from_verified_actions(frame, as_of="2024-11-10T12:00:00Z", coverage_through="2024-11-08")


def test_duplicate_session_dates_rejected():
    with pytest.raises(EquityBasisError, match="unique"):
        snapshot([("2024-06-10 00:00", 0, 10), ("2024-06-10 12:00", 1, 0)])


@pytest.mark.parametrize("kwargs", [{"coverage_through": None}, {"coverage_through": "2024-11-11"}, {"as_of": "NaT"}])
def test_invalid_explicit_coverage_rejected(kwargs):
    with pytest.raises(EquityBasisError):
        snapshot(**kwargs)


def test_event_outside_claimed_coverage_rejected():
    with pytest.raises(EquityBasisError, match="coverage"):
        snapshot([("2024-11-08", 0, 2)], coverage_through="2024-11-07")


def test_decoration_preserves_real_jumps_and_input_frame():
    original = prices()
    result = decorate_adjusted_frame(original, snapshot(), provider_prices_verified=True)
    pd.testing.assert_frame_equal(result[original.columns], original)
    assert "_split_adjusted" not in original
    assert result["_split_adjusted"].all()
    assert result["_ibkr_split_basis"].nunique() == 1
    assert result.attrs["ibkr_equity_basis"]["split_fingerprint"] == snapshot().split_fingerprint


@pytest.mark.parametrize("attestation", [False, None, 1, "true"])
def test_event_snapshot_does_not_certify_provider_prices(attestation):
    with pytest.raises(EquityBasisError, match="provider"):
        decorate_adjusted_frame(prices(), snapshot(), provider_prices_verified=attestation)


def test_equal_basis_survives_attrs_loss():
    first, second = decorated(), decorated()
    second.attrs = {}
    assert_same_basis(first, second)


def test_different_basis_and_unknown_legacy_merge_rejected():
    first = decorated()
    with pytest.raises(EquityBasisError):
        assert_same_basis(first, decorated(snapshot([("2024-06-10", 0, 10)])))
    for left, right in [(first, prices()), (prices(), first)]:
        with pytest.raises(EquityBasisError):
            assert_same_basis(left, right)
    assert_same_basis(prices(), prices())


@pytest.mark.parametrize("bad", [None, "different", ""])
def test_mixed_or_partial_row_basis_rejected(bad):
    first = decorated()
    second = decorated()
    second.loc[second.index[-1], "_ibkr_split_basis"] = bad
    with pytest.raises(EquityBasisError):
        assert_same_basis(first, second)


def test_explicit_missing_placeholder_can_lack_basis():
    first, second = decorated(), decorated()
    second["missing"] = [False, True]
    second.loc[second.index[-1], "_ibkr_split_basis"] = None
    assert_same_basis(first, second)


def test_truthy_string_is_not_a_missing_placeholder():
    first, second = decorated(), decorated()
    second["missing"] = [False, "false"]
    second.loc[second.index[-1], "_ibkr_split_basis"] = None
    with pytest.raises(EquityBasisError):
        assert_same_basis(first, second)


def test_conflicting_attrs_and_row_metadata_rejected():
    first = decorated()
    first.attrs["ibkr_equity_basis"]["split_fingerprint"] = "conflicting"
    with pytest.raises(EquityBasisError):
        assert_same_basis(first, decorated())


def test_decoration_cannot_relabel_existing_different_basis():
    with pytest.raises(EquityBasisError):
        decorate_adjusted_frame(decorated(), snapshot([("2024-06-10", 0, 10)]), provider_prices_verified=True)
