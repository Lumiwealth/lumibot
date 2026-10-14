from types import SimpleNamespace

from lumibot.data_sources.alpaca_data import AlpacaData
from lumibot.entities import Asset


def _data_source():
    return AlpacaData({"API_KEY": "test", "API_SECRET": "test"})


def test_get_chain_full_info_preserves_native_quote_trade_and_greeks():
    data_source = _data_source()
    data_source._option_client = SimpleNamespace(
        get_option_chain=lambda request: {
            "KVYO260821P00015000": SimpleNamespace(
                latest_quote=SimpleNamespace(bid_price=0.77, ask_price=0.83, bid_size=12, ask_size=8),
                latest_trade=SimpleNamespace(price=0.80, size=10),
                implied_volatility=0.81,
                greeks=SimpleNamespace(delta=-0.2439, gamma=0.0654, rho=-0.012, theta=-0.0193, vega=0.0176),
            ),
            "KVYO260821C00015000": SimpleNamespace(
                latest_quote=SimpleNamespace(bid_price=5.1, ask_price=5.4, bid_size=2, ask_size=3),
                latest_trade=SimpleNamespace(price=5.2, size=1),
                implied_volatility=0.72,
                greeks=SimpleNamespace(delta=0.76, gamma=0.06, rho=0.03, theta=-0.02, vega=0.02),
            ),
        }
    )

    result = data_source.get_chain_full_info(
        Asset("KVYO", asset_type="stock"),
        "2026-08-21",
        strike_min=15,
        strike_max=15,
    )

    put = result[result["option_type"] == "PUT"].iloc[0]
    assert put["symbol"] == "KVYO260821P00015000"
    assert put["bid"] == 0.77
    assert put["ask"] == 0.83
    assert put["last"] == 0.80
    assert put["bidsize"] == 12
    assert put["asksize"] == 8
    assert put["greeks.delta"] == -0.2439
    assert put["implied_volatility"] == 0.81


def test_get_chain_full_info_keeps_quote_midpoint_separate_from_missing_trade():
    data_source = _data_source()
    data_source._option_client = SimpleNamespace(
        get_option_chain=lambda request: {
            "snapshots": {
                "SPY260821P00500000": {
                    "latestQuote": {"bp": 4.0, "ap": 4.4, "bs": 5, "as": 7},
                    "latestTrade": None,
                    "impliedVolatility": 0.2,
                    "greeks": {"delta": -0.25},
                }
            }
        }
    )

    result = data_source.get_chain_full_info(Asset("SPY", asset_type="stock"), "2026-08-21")

    row = result.iloc[0]
    assert row["bid"] == 4.0
    assert row["ask"] == 4.4
    assert row["last"] is None or __import__("pandas").isna(row["last"])
    assert row["mid"] == 4.2
    assert row["greeks.delta"] == -0.25


def test_get_chain_full_info_returns_empty_when_alpaca_has_no_native_snapshots():
    data_source = _data_source()
    captured = {}

    def get_option_chain(request):
        captured["request"] = request
        return {}

    data_source._option_client = SimpleNamespace(get_option_chain=get_option_chain)

    result = data_source.get_chain_full_info(
        Asset("SPY", asset_type="stock"),
        "2026-08-21",
        strike_min=490,
        strike_max=510,
    )

    assert result.empty
    assert captured["request"].underlying_symbol == "SPY"
    assert captured["request"].expiration_date.isoformat() == "2026-08-21"
    assert captured["request"].strike_price_gte == 490
    assert captured["request"].strike_price_lte == 510


def test_get_chains_still_parses_nested_snapshot_symbols():
    data_source = _data_source()
    data_source._option_client = SimpleNamespace(
        get_option_chain=lambda request: {
            "option_chains": {
                "SPY260821P00500000": {},
                "SPY260821C00510000": {},
                "not-an-option": {},
            }
        }
    )

    result = data_source.get_chains(Asset("SPY", asset_type="stock"))

    assert result["Chains"]["PUT"]["2026-08-21"] == [500.0]
    assert result["Chains"]["CALL"]["2026-08-21"] == [510.0]


def test_option_quote_uses_latest_snapshot_after_close():
    from datetime import date
    from lumibot.entities import Asset
    data_source = _data_source()
    symbol = "SPY261120P00500000"
    data_source._option_client = SimpleNamespace(get_option_snapshot=lambda request: {symbol: SimpleNamespace(
        latest_quote=SimpleNamespace(bid_price=4.0, ask_price=4.4, bid_size=2, ask_size=3, timestamp="2026-10-13T20:00:00Z"),
        latest_trade=SimpleNamespace(price=4.1, timestamp="2026-10-13T19:58:00Z"),
        greeks=SimpleNamespace(delta=-0.25,gamma=0.01,theta=-0.03,vega=0.08,rho=-0.02),
    )})
    quote = data_source.get_quote(Asset("SPY",asset_type="option",expiration=date(2026,11,20),strike=500,right="PUT"))
    assert quote.bid == 4.0
    assert quote.ask == 4.4
    assert quote.price == 4.1
    assert quote.last_price == 4.1
    assert str(quote.trade_time).startswith("2026-10-13 19:58:00")
    assert quote.raw_data["last_price"] == 4.1
    assert quote.raw_data["greeks"]["delta"] == -0.25
    assert quote.raw_data["trade_time"] == "2026-10-13T19:58:00Z"
