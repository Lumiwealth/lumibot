"""Provider paging, cache extension and frozen split provenance use one basis."""
from copy import deepcopy
from types import SimpleNamespace
import pandas as pd
import pytest
from lumibot.entities import Asset
from lumibot.tools import ibkr_helper as h
from lumibot.tools.ibkr_equity_basis import EquityBasisSnapshot, EquityBasisError, decorate_adjusted_frame


def snapshot(ratio=0):
    actions = pd.DataFrame({"Dividends": [0.], "Stock Splits": [float(ratio)]},
                           index=pd.DatetimeIndex(["2024-06-10"], tz="America/New_York"))
    return EquityBasisSnapshot.from_verified_actions(actions, as_of=pd.Timestamp("2024-06-11T22:00Z"), coverage_through="2024-06-11")


@pytest.fixture
def isolated(monkeypatch, tmp_path):
    basis = snapshot()
    monkeypatch.setattr(h, "_get_current_equity_basis", lambda _: basis)
    monkeypatch.setattr(h, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
    monkeypatch.setattr(h, "get_backtest_cache", lambda: SimpleNamespace(ensure_local_file=lambda *a, **k: None, on_local_update=lambda *a, **k: None))
    monkeypatch.setattr(h, "_resolve_conid", lambda **_: 4815747)
    return basis


def args():
    return dict(asset=Asset("NVDA"), quote=Asset("USD", asset_type="forex"), timestep="minute",
                start_dt=pd.Timestamp("2024-06-07T13:30Z").to_pydatetime(),
                end_dt=pd.Timestamp("2024-06-07T13:32Z").to_pydatetime(), exchange="SMART", include_after_hours=False, source="Trades")


def payload(start=0, end=3, price=120.):
    return {"volumeFactor": 1, "data": [{"t": int((pd.Timestamp("2024-06-07T13:30Z") + pd.Timedelta(minutes=i)).timestamp()*1000),
        "o": price, "h": price+1, "l": price-1, "c": price, "v": 100} for i in range(start,end)]}


def test_extension_and_warm_repeat_keep_basis_without_rewrite(isolated, monkeypatch):
    calls=[]
    monkeypatch.setattr(h, "_ibkr_history_request", lambda **_: calls.append(1) or payload())
    first_args=args(); first_args["end_dt"]=pd.Timestamp("2024-06-07T13:31Z").to_pydatetime()
    first=h._get_cached_bars_for_source(**first_args)
    assert first._ibkr_split_basis.eq(isolated.split_fingerprint).all()
    path=h._cache_file_for(**{k:v for k,v in args().items() if k not in {"start_dt","end_dt"}}, equity_basis=isolated.split_fingerprint)
    before=path.stat().st_mtime_ns
    repeat=h._get_cached_bars_for_source(**first_args)
    pd.testing.assert_frame_equal(first,repeat)
    assert len(calls)==1 and path.stat().st_mtime_ns==before
    # Force an actual disjoint extension beyond the small tail tolerance.
    later=args(); later["start_dt"]=pd.Timestamp("2024-06-07T13:40Z").to_pydatetime(); later["end_dt"]=pd.Timestamp("2024-06-07T13:42Z").to_pydatetime()
    monkeypatch.setattr(h,"_ibkr_history_request",lambda **_: calls.append(1) or payload(10,13))
    extended=h._get_cached_bars_for_source(**later)
    assert len(calls)==2 and len(extended)==3
    persisted=pd.read_parquet(path)
    assert len(persisted)==6 and persisted._ibkr_split_basis.eq(isolated.split_fingerprint).all()


def test_new_split_refetches_separate_cache_and_preserves_old(isolated, monkeypatch):
    calls=[]
    monkeypatch.setattr(h,"_ibkr_history_request",lambda **_: calls.append(1) or payload())
    first=h._get_cached_bars_for_source(**args())
    changed=snapshot(10)
    monkeypatch.setattr(h,"_get_current_equity_basis",lambda _: changed)
    monkeypatch.setattr(h,"_ibkr_history_request",lambda **_: calls.append(1) or payload(price=12.))
    after=h._get_cached_bars_for_source(**args())
    assert len(calls)==2 and first.close.tolist()==[120.]*3 and after.close.tolist()==[12.]*3
    old=h._cache_file_for(**{k:v for k,v in args().items() if k not in {"start_dt","end_dt"}},equity_basis=isolated.split_fingerprint)
    assert pd.read_parquet(old).close.tolist()==[120.]*3
    with pytest.raises(EquityBasisError): h._merge_frames(first,after)


def test_unmarked_legacy_rows_are_refetched_not_relabelled(isolated, monkeypatch):
    frame=pd.DataFrame({"close":[120.]},index=pd.DatetimeIndex(["2024-06-07T13:30Z"]))
    assert h._verified_equity_cache(frame,isolated).empty
    tagged=decorate_adjusted_frame(frame,isolated,provider_prices_verified=True)
    mixed=pd.concat([tagged,frame])
    assert h._verified_equity_cache(mixed,isolated).empty


def test_page_checkpoint_contains_basis_before_publication(isolated, monkeypatch):
    monkeypatch.setattr(h,"IBKR_PAGE_CHECKPOINT_EVERY",1)
    monkeypatch.setattr(h,"_ibkr_history_request",lambda **_:payload())
    published=[]
    value=h._fetch_history_between_dates(**args(),source_was_explicit=True,_page_checkpoint=published.append)
    assert len(published)==1
    assert published[0]._ibkr_split_basis.eq(isolated.split_fingerprint).all()
    pd.testing.assert_frame_equal(value,published[0])


def test_current_actions_use_latest_session_on_weekend(monkeypatch):
    monkeypatch.setattr(h,"_IBKR_EQUITY_BASIS_CACHE",{})
    monkeypatch.setattr(h,"_ibkr_history_now_utc",lambda:pd.Timestamp("2026-10-10T17:00Z").to_pydatetime())
    calls=[]
    def actions(symbol,last_needed_datetime):
        calls.append(last_needed_datetime.date().isoformat())
        frame=pd.DataFrame(columns=["Dividends","Stock Splits"])
        frame.attrs["coverage_through"]="2026-10-09"
        return frame
    monkeypatch.setattr(h,"_get_cached_equity_actions",actions)
    basis=h._get_current_equity_basis("SPY")
    assert calls==["2026-10-09"] and basis.coverage_through=="2026-10-09"
    assert h._get_current_equity_basis("SPY") is basis and len(calls)==1


def test_missing_split_morning_metadata_preserves_native_price_basis(monkeypatch):
    monkeypatch.setattr(h,"_IBKR_EQUITY_BASIS_CACHE",{})
    monkeypatch.setattr(h,"_ibkr_history_now_utc",lambda:pd.Timestamp("2024-06-10T13:00Z").to_pydatetime())
    frame=pd.DataFrame(columns=["Dividends","Stock Splits"]);frame.attrs["coverage_through"]="2024-06-07"
    monkeypatch.setattr(h,"_get_cached_equity_actions",lambda *a,**k:frame)
    # 2026-10-21: a missing event ledger is not a price-data failure. IBKR's
    # native prices are already split-adjusted; unknown events stay unverified.
    basis = h._get_current_equity_basis("NVDA")
    assert basis.actions_verified is False and basis.coverage_through is None
    assert basis.events == ()


def test_placeholder_merge_does_not_invent_or_require_price_basis(isolated):
    real=decorate_adjusted_frame(pd.DataFrame({"close":[120.]},index=pd.DatetimeIndex(["2024-06-07T13:30Z"])),isolated,provider_prices_verified=True)
    placeholder=pd.DataFrame({"close":[float("nan")],"missing":[True]},index=pd.DatetimeIndex(["2024-06-07T13:31Z"]))
    combined=h._merge_frames(real,placeholder)
    assert combined.close.iloc[0]==120. and pd.isna(combined.close.iloc[1])
    assert len(h._verified_equity_cache(combined,isolated))==2
    unmarked=placeholder.copy();unmarked["missing"]=False
    with pytest.raises(EquityBasisError):h._merge_frames(real,unmarked)


@pytest.mark.parametrize("symbol", ["SPY", "XLF"])
@pytest.mark.parametrize("wall_clock", ["2026-10-21T06:30Z", "2026-10-22T00:42Z"])
def test_native_stock_prices_survive_optional_action_outage(monkeypatch, tmp_path, symbol, wall_clock):
    """Missing auxiliary metadata must not prevent a supported IBKR price request."""
    monkeypatch.setattr(h, "_IBKR_EQUITY_BASIS_CACHE", {})
    monkeypatch.setattr(h, "_ibkr_history_now_utc", lambda: pd.Timestamp(wall_clock).to_pydatetime())
    monkeypatch.setattr(h, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
    monkeypatch.setattr(h, "get_backtest_cache", lambda: SimpleNamespace(
        ensure_local_file=lambda *a, **k: None, on_local_update=lambda *a, **k: None))
    monkeypatch.setattr(h, "_resolve_conid", lambda **_: 12345)
    action_calls, price_calls = [], []

    def unavailable(*a, **k):
        action_calls.append(1)
        raise RuntimeError("corporate_actions_unavailable: source unavailable")

    monkeypatch.setattr(h, "_get_cached_equity_actions", unavailable)
    monkeypatch.setattr(h, "_ibkr_history_request", lambda **_: price_calls.append(1) or payload())
    request = args()
    request["asset"] = Asset(symbol)
    first = h._get_cached_bars_for_source(**request)
    repeat = h._get_cached_bars_for_source(**request)
    assert first.close.tolist() == [120.] * 3
    pd.testing.assert_frame_equal(first, repeat)
    assert len(price_calls) == 1 and len(action_calls) == 1
    from lumibot.tools.ibkr_equity_basis import equity_basis_from_frame
    basis = equity_basis_from_frame(first)
    assert basis.actions_verified is False and basis.coverage_through is None
    assert basis.events == ()
    verified_empty = snapshot()
    assert basis.split_fingerprint != verified_empty.split_fingerprint
    verified = decorate_adjusted_frame(pd.DataFrame({"close": [120.]}, index=first.index[:1]),
                                      verified_empty, provider_prices_verified=True)
    with pytest.raises(EquityBasisError):
        h._merge_frames(first, verified)
