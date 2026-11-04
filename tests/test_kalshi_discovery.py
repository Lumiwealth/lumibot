"""Kalshi discovery uses recorded public API shapes and neutral instruments."""

from copy import deepcopy

import httpx
import pytest

from lumibot.data_sources import KalshiData
from lumibot.entities import Asset
from lumibot.tools.kalshi_client import KalshiAPIError, KalshiClient


def market(ticker="SYNTHETIC-EVENT-YES", title="Will the synthetic observation exceed its threshold?"):
    # Shape from Kalshi's public demo /markets response, with neutral values.
    return {
        "ticker": ticker,
        "event_ticker": "SYNTHETIC-EVENT",
        "title": title,
        "status": "active",
        "rules_primary": "Yes if the published observation is greater than 10.",
        "rules_secondary": "Use the first published value; later revisions do not change the result.",
        "open_time": "2026-01-01T00:00:00Z",
        "close_time": "2026-01-02T00:00:00Z",
        "expiration_time": "2026-01-09T00:00:00Z",
        "price_ranges": [{"start": "0.0000", "end": "1.0000", "step": "0.0100"}],
    }


@pytest.fixture
def venue():
    requests = []
    rows = [
        {"ticker": "UNRELATED", "title": "Unrelated weather", "volume_fp": "10000"},
        {"ticker": "SYNTHETIC", "title": "Synthetic observation", "tags": ["Observation"], "volume_fp": "200"},
        {"ticker": "OLD", "title": "Synthetic observation", "volume_fp": "0"},
    ]

    def handler(request):
        requests.append(request)
        path = request.url.path.removeprefix("/trade-api/v2")
        params = request.url.params
        if path == "/series":
            return httpx.Response(200, json={"series": deepcopy(rows)})
        if path == "/series/SYNTHETIC":
            return httpx.Response(
                200,
                json={
                    "series": {
                        **rows[1],
                        "settlement_sources": [
                            {"name": "Public observation source", "url": "https://example.com/data"}
                        ],
                        "contract_terms_url": "https://example.com/terms",
                    }
                },
            )
        if path == "/events/SYNTHETIC-EVENT":
            event = {"event_ticker": "SYNTHETIC-EVENT", "series_ticker": "SYNTHETIC", "title": "Synthetic observation"}
            if params.get("exclude_markets") == "true":
                return httpx.Response(200, json={"event": event, "markets": []})
            return httpx.Response(200, json={"event": event, "markets": [market()]})
        if path == "/markets/SYNTHETIC-EVENT-YES":
            return httpx.Response(200, json={"market": market()})
        if path == "/markets":
            if params.get("series_ticker") == "OLD":
                return httpx.Response(200, json={"markets": [], "cursor": ""})
            if params.get("cursor") == "next":
                return httpx.Response(200, json={"markets": [market("SYNTHETIC-EVENT-SECOND")], "cursor": ""})
            return httpx.Response(200, json={"markets": [market()], "cursor": "next"})
        return httpx.Response(404, json={"error": {"code": "not_found"}})

    http = httpx.Client(transport=httpx.MockTransport(handler))
    client = KalshiClient({"IS_DEMO": True}, http_client=http)
    data = KalshiData(client=client)
    yield data, requests, handler
    data.shutdown()
    http.close()


def test_query_discovers_native_series_then_pages_markets_with_outcome_assets(venue):
    data, requests, _ = venue
    found = data.search_markets("synthetic observation", limit=2)
    assert [row["ticker"] for row in found] == ["SYNTHETIC-EVENT-YES", "SYNTHETIC-EVENT-SECOND"]
    assert found[0]["rules_secondary"] == market()["rules_secondary"]
    assets = [Asset.from_dict(value) for value in found[0]["outcome_assets"]]
    assert [(asset.symbol, asset.asset_type, asset.outcome) for asset in assets] == [
        ("SYNTHETIC-EVENT-YES", "prediction_contract", "yes"),
        ("SYNTHETIC-EVENT-YES", "prediction_contract", "no"),
    ]
    assert all(request.url.host == "external-api.demo.kalshi.co" for request in requests)
    assert all("KALSHI-ACCESS-KEY" not in request.headers for request in requests)
    calls = [request for request in requests if request.url.path.endswith("/markets")]
    assert [request.url.params["series_ticker"] for request in calls] == ["SYNTHETIC", "OLD", "SYNTHETIC"]
    assert calls[0].url.params["status"] == "open"
    assert calls[2].url.params["cursor"] == "next"
    assert not any("query" in request.url.params or "q" in request.url.params for request in requests)


def test_browse_preserves_provider_filters_and_stops_at_requested_limit(venue):
    data, requests, _ = venue
    found = data.search_markets(limit=1, event_ticker="SYNTHETIC-EVENT", status="settled")
    assert len(found) == 1
    assert len(requests) == 1
    assert requests[0].url.params["event_ticker"] == "SYNTHETIC-EVENT"
    assert requests[0].url.params["status"] == "settled"


def test_no_matching_series_is_empty_without_unrelated_market_fallback(venue):
    data, requests, _ = venue
    assert data.search_markets("unavailable topic") == []
    assert len(requests) == 1


def test_topic_search_does_not_hide_other_matching_series_behind_one_empty_book(venue):
    data, _, handler = venue

    def answer(request):
        if request.url.path.endswith("/series"):
            return httpx.Response(
                200,
                json={
                    "series": [
                        {"ticker": "SYNTHETIC", "title": "Synthetic observation", "volume_fp": "200"},
                        {"ticker": "SECOND", "title": "Synthetic observation", "volume_fp": "100"},
                    ]
                },
            )
        if request.url.path.endswith("/markets") and request.url.params.get("series_ticker") == "SECOND":
            return httpx.Response(200, json={"markets": [market("SECOND-EVENT")], "cursor": ""})
        return handler(request)

    with httpx.Client(transport=httpx.MockTransport(answer)) as http:
        data = KalshiData(client=KalshiClient(http_client=http))
        assert [m["ticker"] for m in data.search_markets("synthetic", limit=2)] == [
            "SYNTHETIC-EVENT-YES",
            "SECOND-EVENT",
        ]


def test_event_keeps_nested_market_identity_and_native_event_ticker(venue):
    data, requests, _ = venue
    event = data.get_event(event_id="SYNTHETIC-EVENT")
    assert event["event_ticker"] == "SYNTHETIC-EVENT"
    assert event["series_ticker"] == "SYNTHETIC"
    assert event["markets"][0]["ticker"] == "SYNTHETIC-EVENT-YES"
    assert event["markets"][0]["outcomes"] == ["yes", "no"]
    assert requests[0].url.params.get("with_nested_markets") == "true"


def test_rules_include_secondary_payout_terms_and_distinct_close_expiration(venue):
    data, _, _ = venue
    rules = data.get_market_rules(Asset("SYNTHETIC-EVENT-YES", "prediction_contract", outcome="no"))
    assert rules["rules_primary"] == market()["rules_primary"]
    assert rules["rules_secondary"] == market()["rules_secondary"]
    assert rules["rules_primary"] in rules["description"] and rules["rules_secondary"] in rules["description"]
    assert rules["end_date"] == market()["close_time"]
    assert rules["expiration_time"] == market()["expiration_time"]
    assert rules["settlement_sources"][0]["name"] == "Public observation source"
    assert rules["contract_terms_url"] == "https://example.com/terms"
    assert rules["price_ranges"] == market()["price_ranges"]
    assert rules["outcomes"] == ["yes", "no"]


def test_metadata_accepts_existing_market_dict_without_network_or_mutating_it(venue):
    data, requests, _ = venue
    row = market()
    assert data.get_market_metadata(row)["ticker"] == row["ticker"]
    assert requests == []
    assert "outcomes" not in row


def test_rules_preserve_available_criteria_when_partial_metadata_has_no_event(venue):
    data, requests, _ = venue
    row = {"ticker": "SYNTHETIC", "rules_primary": "Use the published value."}
    rules = data.get_market_rules(row)
    assert rules["rules_primary"] == row["rules_primary"]
    assert rules["settlement_sources"] == []
    assert rules["expiration_time"] is None
    assert requests == []


def test_malformed_series_identity_is_a_provider_error():
    response = {"series": [{"title": "Synthetic observation"}]}
    with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, json=response))) as http:
        data = KalshiData(client=KalshiClient(http_client=http))
        with pytest.raises(KalshiAPIError, match="series"):
            data.search_markets("Synthetic")


@pytest.mark.parametrize("limit", [-1, True, 1.5])
def test_invalid_search_limit_does_not_make_provider_requests(venue, limit):
    data, requests, _ = venue
    with pytest.raises(ValueError, match="limit"):
        data.search_markets(limit=limit)
    assert requests == []


def test_zero_search_limit_is_an_empty_bounded_read(venue):
    data, requests, _ = venue
    assert data.search_markets(limit=0) == []
    assert requests == []


def test_provider_error_remains_visible_in_discovery():
    with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(403, json={}))) as http:
        data = KalshiData(client=KalshiClient(http_client=http))
        with pytest.raises(KalshiAPIError, match="HTTP 403"):
            data.search_markets("Synthetic")


def test_malformed_catalog_is_not_reported_as_no_results():
    with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, json={"series": {}}))) as http:
        data = KalshiData(client=KalshiClient(http_client=http))
        with pytest.raises(KalshiAPIError, match="series"):
            data.search_markets("Synthetic")


def test_market_pagination_repeated_cursor_is_visible(venue):
    data, _, handler = venue

    def repeat(request):
        response = handler(request)
        if request.url.path.endswith("/markets"):
            return httpx.Response(200, json={"markets": [market()], "cursor": "same"})
        return response

    data._client._http = httpx.Client(transport=httpx.MockTransport(repeat))
    try:
        with pytest.raises(KalshiAPIError, match="cursor"):
            data.search_markets(limit=5)
    finally:
        data._client._http.close()
