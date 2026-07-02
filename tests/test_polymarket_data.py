from urllib.parse import parse_qs, urlparse

from lumibot.data_sources.polymarket_data import PolymarketData
from lumibot.entities import Asset, Bars, Quote


class FakeHttpClient:
    def __init__(self):
        self.calls = []

    def get(self, url, params=None):
        self.calls.append((url, params or {}))
        path = urlparse(url).path
        params = params or {}
        if path == "/book":
            return {
                "bids": [{"price": "0.40", "size": "10"}, {"price": "0.42", "size": "5"}],
                "asks": [{"price": "0.45", "size": "7"}, {"price": "0.47", "size": "9"}],
                "tick_size": "0.01",
                "min_order_size": "1",
            }
        if path == "/last-trade-price":
            return {"price": "0.44"}
        if path == "/prices-history":
            return {"history": [{"t": 1_782_800_000, "p": "0.43"}, {"t": 1_782_800_060, "p": "0.44"}]}
        if path == "/tick-size":
            return {"minimum_tick_size": "0.01"}
        if path == "/spread":
            return {"spread": "0.03"}
        if path == "/midpoint":
            return {"mid": "0.435"}
        if path in {"/markets", "/markets/slug/test-market", "/markets/token/111"}:
            query = parse_qs(urlparse(url).query)
            slug = params.get("slug") or query.get("slug", ["test-market"])[0]
            market = {
                "id": "m1",
                "slug": slug,
                "question": "Will this test pass?",
                "description": "Unit-test market",
                "resolutionSource": "Unit test fixture",
                "conditionId": "0x" + "1" * 64,
                "outcomes": '["Yes","No"]',
                "clobTokenIds": '["111","222"]',
                "orderPriceMinTickSize": 0.01,
                "orderMinSize": 5,
                "umaResolutionStatus": "pending",
                "acceptingOrders": True,
                "enableOrderBook": True,
                "closed": False,
                "tokens": [{"token_id": "111", "outcome": "Yes", "winner": False}],
            }
            if path == "/markets":
                return [market]
            return market
        if path == "/public-search":
            return {
                "markets": [
                    {
                        "id": "m1",
                        "slug": "test-market",
                        "outcomes": '["Yes","No"]',
                        "clobTokenIds": '["111","222"]',
                    }
                ],
                "events": [
                    {
                        "id": "e1",
                        "slug": "event-slug",
                        "title": "Event title",
                        "markets": [
                            {
                                "id": "m2",
                                "slug": "event-market",
                                "outcomes": '["Up","Down"]',
                                "clobTokenIds": '["333","444"]',
                            }
                        ],
                    }
                ],
            }
        if path == "/events/slug/event-slug":
            return {"id": "e1", "slug": "event-slug", "title": "Event title", "markets": []}
        if path == "/trades":
            return [{"asset": "111", "conditionId": "0x" + "1" * 64, "price": "0.44", "size": "10"}]
        if path == "/oi":
            return [{"market": "0x" + "1" * 64, "value": 123.45}]
        if path == "/holders":
            return [{"token": "111", "holders": [{"proxyWallet": "0xwallet", "amount": 10}]}]
        raise AssertionError(f"Unexpected URL: {url} params={params}")


def test_polymarket_quote_from_order_book():
    data = PolymarketData(client=FakeHttpClient())
    asset = Asset("111", asset_type=Asset.AssetType.PREDICTION_CONTRACT)

    quote = data.get_quote(asset)

    assert isinstance(quote, Quote)
    assert quote.bid == 0.42
    assert quote.ask == 0.45
    assert quote.mid_price == 0.435
    assert quote.bid_size == 5.0
    assert quote.ask_size == 7.0


def test_polymarket_last_price_uses_last_trade_endpoint():
    data = PolymarketData(client=FakeHttpClient())
    asset = Asset("111", asset_type=Asset.AssetType.PREDICTION_CONTRACT)

    assert data.get_last_price(asset) == 0.44


def test_polymarket_history_returns_bars():
    data = PolymarketData(client=FakeHttpClient())
    asset = Asset("111", asset_type=Asset.AssetType.PREDICTION_CONTRACT)

    bars = data.get_historical_prices(asset, 2, timestep="minute")

    assert isinstance(bars, Bars)
    assert list(bars.df["close"]) == [0.43, 0.44]


def test_polymarket_resolve_market_and_contract():
    data = PolymarketData(client=FakeHttpClient())

    market = data.resolve_market(slug="test-market")
    asset = data.resolve_contract(market, outcome="No")

    assert market["outcomes"] == ["Yes", "No"]
    assert asset.symbol == "222"
    assert asset.asset_type == Asset.AssetType.PREDICTION_CONTRACT


def test_polymarket_prediction_market_metadata_methods():
    data = PolymarketData(client=FakeHttpClient())
    market = data.get_market_metadata(slug="test-market")
    asset = data.resolve_contract(market, outcome="Yes")

    rules = data.get_market_rules(market)
    resolution = data.get_resolution_status(market)
    search = data.search_markets("test", limit=5)
    event = data.get_event(slug="event-slug")
    trades = data.get_recent_trades(market)
    holders = data.get_holders(market)

    assert rules["question"] == "Will this test pass?"
    assert rules["tick_size"] == 0.01
    assert rules["min_order_size"] == 5
    assert rules["accepting_orders"] is True
    assert resolution["status"] == "pending"
    assert resolution["resolved"] is False
    assert len(search) == 2
    assert event["title"] == "Event title"
    assert trades[0]["asset"] == "111"
    assert holders[0]["token"] == "111"
    assert data.get_open_interest(market) == 123.45
    assert data.get_spread(asset) == 0.03
    assert data.get_midpoint(asset) == 0.435


def test_polymarket_market_stream_list_event_updates_quote_cache():
    data = PolymarketData(client=FakeHttpClient())

    data.apply_market_event(
        [
            {
                "event_type": "best_bid_ask",
                "asset_id": "111",
                "best_bid": "0.41",
                "best_ask": "0.44",
                "price": "0.425",
            }
        ]
    )

    cached = data._quote_cache["111"]
    assert cached.bid == 0.41
    assert cached.ask == 0.44
    assert cached.price == 0.425


def test_polymarket_market_stream_nested_price_changes_update_quote_cache():
    data = PolymarketData(client=FakeHttpClient())

    data.apply_market_event(
        {
            "event_type": "price_change",
            "market": "0xmarket",
            "price_changes": [
                {
                    "asset_id": "111",
                    "price": "0.42",
                    "best_bid": "0.42",
                    "best_ask": "0.45",
                }
            ],
        }
    )

    cached = data._quote_cache["111"]
    assert cached.bid == 0.42
    assert cached.ask == 0.45
    assert cached.price == 0.42
