"""Exercise real cache IO with the provider's recorded aggregate response shape."""

from datetime import date, datetime, timedelta, timezone
from unittest.mock import Mock

import pandas as pd
import pytest

from lumibot.entities import Asset
from lumibot.tools import polygon_helper as ph


@pytest.fixture
def history(monkeypatch, tmp_path):
    asset = Asset("SYNTH")
    start = datetime(2024, 8, 23, tzinfo=timezone.utc)
    end = datetime(2026, 1, 28, 23, 59, tzinfo=timezone.utc)
    dates = ph.get_trading_dates(asset, start, end)
    monkeypatch.setattr(ph, "LUMIBOT_CACHE_FOLDER", tmp_path)
    validation = Mock(side_effect=lambda forced, *args: forced)
    monkeypatch.setattr(ph, "validate_cache", validation)
    client = Mock()
    monkeypatch.setattr(ph.PolygonClient, "create", Mock(return_value=client))
    progress = Mock()
    monkeypatch.setattr(ph, "tqdm", progress)

    def aggregates(selected, timespan):
        # o/h/l/c/v/t is the native aggregate shape consumed by update_polygon_data.
        rows = []
        for day in selected:
            for minute in range(2 if timespan == "minute" else 1):
                stamp = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
                stamp += timedelta(hours=14, minutes=30 + minute)
                price = day.toordinal() % 100 + minute
                rows.append(
                    {
                        "o": price,
                        "h": price + 1,
                        "l": price - 1,
                        "c": price,
                        "v": 100,
                        "t": int(stamp.timestamp() * 1000),
                    }
                )
        return rows

    def provider(**kwargs):
        selected = [d for d in dates if kwargs["from_"] <= d <= kwargs["to"]]
        return aggregates(selected, kwargs["timespan"])

    client.get_aggs.side_effect = provider

    def seed(selected, timespan, unavailable=()):
        frame = ph.update_polygon_data(None, aggregates(selected, timespan))
        ph.update_cache(ph.build_cache_filename(asset, timespan), frame, unavailable)

    def run(timespan="day", **kwargs):
        return ph.get_price_data_from_polygon("synthetic", asset, start, end, timespan, max_workers=1, **kwargs)

    def ranges():
        return [(c.kwargs["from_"], c.kwargs["to"]) for c in client.get_aggs.call_args_list]

    return asset, dates, client, progress, validation, aggregates, seed, run, ranges


@pytest.mark.parametrize("timespan", ["day", "minute"])
def test_cached_middle_chunks_are_not_downloaded(history, timespan):
    asset, dates, client, progress, _, aggregates, seed, run, ranges = history
    seed([d for d in dates if date(2024, 9, 9) <= d <= date(2025, 11, 5)], timespan)

    result = run(timespan)

    assert ranges() == [
        (date(2024, 8, 23), date(2024, 9, 22)),
        (date(2025, 10, 31), date(2025, 11, 30)),
        (date(2025, 12, 1), date(2025, 12, 31)),
        (date(2026, 1, 1), date(2026, 1, 28)),
    ]
    expected = ph.update_polygon_data(None, aggregates(dates, timespan))
    pd.testing.assert_frame_equal(result, expected)
    assert progress.call_args.kwargs["total"] == 4
    assert progress.return_value.update.call_count == 4
    assert all(c.kwargs["limit"] == 50000 for c in client.get_aggs.call_args_list)
    client.get_aggs.reset_mock()
    pd.testing.assert_frame_equal(run(timespan), result)
    client.get_aggs.assert_not_called()


def test_missing_chunk_end_and_next_chunk_start_are_both_requested(history):
    _, dates, _, progress, _, _, seed, run, ranges = history
    missing = {dates[0], date(2025, 2, 24), date(2025, 2, 25), dates[-1]}
    assert missing <= set(dates)
    seed([d for d in dates if d not in missing], "day")

    result = run()

    assert ranges() == [
        (date(2024, 8, 23), date(2024, 9, 22)),
        (date(2025, 1, 25), date(2025, 2, 24)),
        (date(2025, 2, 25), date(2025, 3, 27)),
        (date(2026, 1, 1), date(2026, 1, 28)),
    ]
    assert set(result.index.date) == set(dates)
    assert progress.call_args.kwargs["total"] == 4


@pytest.mark.parametrize("cache_mode", ["cold", "forced", "split_invalidated"])
def test_cold_and_invalidated_caches_keep_all_download_chunks(history, cache_mode):
    _, dates, client, progress, validation, _, seed, run, ranges = history
    if cache_mode != "cold":
        seed(dates, "day")
    if cache_mode == "split_invalidated":
        validation.side_effect = None
        validation.return_value = True

    result = run(force_cache_update=cache_mode == "forced")

    requests = ranges()
    assert len(requests) == 17
    assert requests[0][0] == dates[0]
    assert requests[-1][1] == dates[-1]
    assert all(b[0] == a[1] + timedelta(days=1) for a, b in zip(requests, requests[1:]))
    assert all((end - start).days <= ph.MAX_POLYGON_DAYS for start, end in requests)
    assert set(result.index.date) == set(dates)
    assert progress.call_args.kwargs["total"] == client.get_aggs.call_count


def test_unavailable_dates_stay_checked_without_losing_supported_history(history):
    asset, dates, client, _, _, _, seed, run, ranges = history
    sentinel = date(2025, 1, 15)
    cached = [d for d in dates if date(2024, 9, 9) <= d <= date(2025, 11, 5) and d != sentinel]
    seed(cached, "day", unavailable=[sentinel])
    provider = client.get_aggs.side_effect

    def partial_provider(**kwargs):
        if kwargs["from_"] == dates[0]:
            # A provider-denied slice must not prevent the supported slices returning.
            raise RuntimeError("Recorded provider refusal: data timeframe not authorized")
        return provider(**kwargs)

    client.get_aggs.side_effect = partial_provider
    result = run()

    assert len(ranges()) == 4
    assert sentinel not in set(result.index.date)
    assert date(2025, 12, 15) in set(result.index.date)
    assert set(cached) <= set(result.index.date)
    stored = ph.load_cache(ph.build_cache_filename(asset, "day"))
    assert stored.loc[stored.index.date == sentinel, "missing"].all()
    assert not result.get("missing", pd.Series(False, index=result.index)).fillna(False).any()
    client.get_aggs.reset_mock()
    pd.testing.assert_frame_equal(run(), result)
    client.get_aggs.assert_not_called()


def test_fully_cached_history_never_constructs_a_provider_request(history):
    _, dates, client, progress, _, _, seed, run, _ = history
    seed(dates, "day")
    result = run()
    assert set(result.index.date) == set(dates)
    client.get_aggs.assert_not_called()
    progress.assert_not_called()


@pytest.mark.parametrize("timespan", ["day", "minute"])
@pytest.mark.parametrize("all_unavailable", [False, True], ids=["partial", "all"])
def test_fully_cached_reads_exclude_unavailable_rows(history, timespan, all_unavailable):
    asset, dates, client, progress, _, aggregates, seed, run, _ = history
    unavailable = dates if all_unavailable else [date(2025, 1, 15)]
    supported = [d for d in dates if d not in unavailable]
    seed(supported, timespan, unavailable=unavailable)

    result = run(timespan)

    assert set(result.index.date) == set(supported)
    if supported:
        expected = ph.update_polygon_data(None, aggregates(supported, timespan))
        pd.testing.assert_frame_equal(result[expected.columns], expected, check_dtype=False)
    else:
        assert result.empty
    stored = ph.load_cache(ph.build_cache_filename(asset, timespan))
    assert set(stored.index.date) == set(dates)
    assert stored.loc[[d in unavailable for d in stored.index.date], "missing"].all()
    client.get_aggs.assert_not_called()
    progress.assert_not_called()
