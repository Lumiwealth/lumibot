"""Input-observation unit fixtures, never strategy execution or eval proof."""
import json
from types import SimpleNamespace

import pandas as pd

from lumibot.entities import Asset


def test_framework_input_rows_preserve_time_identity_and_changed_values(tmp_path):
    from lumibot.backtesting.market_input_evidence import record_market_input, write_market_input_evidence
    source = SimpleNamespace()
    frame = pd.DataFrame({"open": [9., 10.], "close": [10., 11.]},
        index=pd.date_range("2025-01-01", periods=2, tz="America/New_York"))
    kwargs = dict(source=source, asset=Asset("SPY"), timestep="day", when=frame.index[-1], purpose="history")
    record_market_input(frame=frame, **kwargs)
    record_market_input(frame=frame, **kwargs)  # Repeated reads are not duplicated.
    changed = frame.copy()
    changed.iloc[1, changed.columns.get_loc("close")] = 12.
    record_market_input(frame=changed, **kwargs)
    write_market_input_evidence(source, tmp_path)
    proof = json.loads((tmp_path / "market_input_manifest.json").read_text())
    data = pd.read_csv(tmp_path / "market_inputs.csv")
    observations = pd.read_csv(tmp_path / "market_input_observations.csv")
    assert proof["complete"] is True
    assert len(data) == 3
    assert len(observations) == 2
    assert set(data["close"]) == {10., 11., 12.}
    assert all("-05:00" in value for value in data["barTime"])
    assert len(set(observations["frameSha256"])) == 2
    assert all(json.loads(value)["assetType"] == "stock" and json.loads(value)["symbol"] == "SPY"
               for value in data["asset"])


def test_capture_limit_is_explicit_and_does_not_change_input_data(tmp_path):
    from lumibot.backtesting.market_input_evidence import MarketInputEvidence, write_market_input_evidence
    recorder = MarketInputEvidence(max_rows=1)
    source = SimpleNamespace(_market_input_evidence=recorder)
    frame = pd.DataFrame({"close": [10., 11.]}, index=pd.date_range("2025-01-01", periods=2, tz="UTC"))
    original = frame.copy()
    recorder.record(asset=Asset("SPY"), timestep="day", when=frame.index[-1], frame=frame, purpose="history")
    write_market_input_evidence(source, tmp_path)
    assert frame.equals(original)
    proof = json.loads((tmp_path / "market_input_manifest.json").read_text())
    assert proof["complete"] is False
    assert "row_limit" in proof["incompleteReasons"]


def test_option_and_crypto_quote_identities_never_collapse():
    from lumibot.backtesting.market_input_evidence import asset_identity
    call = Asset("SPY", "option", strike=500, right="call")
    put = Asset("SPY", "option", strike=500, right="put")
    assert asset_identity(call) != asset_identity(put)
    btc = Asset("BTC", "crypto")
    assert asset_identity((btc, Asset("USD", "forex"))) != asset_identity((btc, Asset("USDT", "crypto")))


def test_missing_rows_and_input_window_remain_visible(tmp_path):
    from lumibot.backtesting.market_input_evidence import record_market_input, write_market_input_evidence
    source = SimpleNamespace()
    frame = pd.DataFrame({"close": [10., float("nan")]}, index=pd.date_range("2025-01-01", periods=2, tz="UTC"))
    record_market_input(source=source, asset=Asset("SPY"), timestep="day", when=frame.index[0],
        frame=frame, purpose="history")  # Preserve a future row if an API actually returned one.
    write_market_input_evidence(source, tmp_path)
    data = pd.read_csv(tmp_path / "market_inputs.csv")
    observations = pd.read_csv(tmp_path / "market_input_observations.csv")
    assert data["close"].isna().sum() == 1
    assert observations.iloc[0]["observedAt"] < observations.iloc[0]["windowEnd"]
    assert observations.iloc[0]["returnedRows"] == 2
