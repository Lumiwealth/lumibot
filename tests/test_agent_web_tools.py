import json
from datetime import datetime, timezone

import httpx
import pytest

from lumibot.components.agents.builtins import BuiltinTools
from lumibot.components.agents.web_tools import CredentialProfile, WebClient

PUBLIC_IP = "93.184.216.34"


def _resolver(hostname):
    return [PUBLIC_IP]


def _original_url(request):
    # WebClient pins each request to the validated IP, so the transport sees the IP in
    # request.url and the real hostname in the Host header.
    return f"{request.url.scheme}://{request.headers['host']}{request.url.raw_path.decode()}"


def test_http_request_supports_all_standard_methods_and_body_types():
    requests = []

    def handler(request):
        body = request.read().decode()
        requests.append((request.method, str(request.url), body, request.headers.get("content-type")))
        return httpx.Response(200, json={"method": request.method, "body": body})

    client = WebClient(transport=httpx.MockTransport(handler), resolver=_resolver)

    for method in ("GET", "HEAD", "OPTIONS", "POST", "PUT", "PATCH", "DELETE"):
        kwargs = {"json_body": {"side": "buy"}} if method in {"POST", "PUT", "PATCH", "DELETE"} else {}
        result = client.request(method, "https://example.test/orders", **kwargs)
        assert result["ok"] is True
        assert result["status_code"] == 200

    assert [entry[0] for entry in requests] == ["GET", "HEAD", "OPTIONS", "POST", "PUT", "PATCH", "DELETE"]
    assert json.loads(requests[3][2]) == {"side": "buy"}


def test_http_request_and_rss_results_carry_stable_availability_provenance():
    fetched_at = datetime(2026, 9, 20, 12, 0, tzinfo=timezone.utc)
    feed = b"""<rss><channel><title>Filings</title><item><guid>filing-1</guid>
      <title>New filing</title><link>https://example.test/filing-1</link>
      <pubDate>Sun, 20 Sep 2026 11:30:00 GMT</pubDate></item></channel></rss>"""

    def handler(request):
        if request.url.path == "/feed":
            return httpx.Response(
                200,
                content=feed,
                headers={"content-type": "application/rss+xml", "date": "Sun, 20 Sep 2026 12:00:00 GMT"},
            )
        return httpx.Response(
            200,
            json={"ok": True},
            headers={"date": "Sun, 20 Sep 2026 11:59:00 GMT"},
        )

    client = WebClient(
        transport=httpx.MockTransport(handler),
        resolver=_resolver,
        clock=lambda: fetched_at,
    )

    response = client.request("GET", "https://example.test/data?secret=redacted")
    feed_result = client.fetch_feed("https://example.test/feed")

    assert response["id"] == response["content_sha256"]
    assert response["source"] == "https://example.test/data"
    assert response["published_at"] == "2026-09-20T11:59:00+00:00"
    assert response["fetched_at"] == fetched_at.isoformat()
    assert feed_result["source"] == "https://example.test/feed"
    assert feed_result["fetched_at"] == fetched_at.isoformat()
    assert feed_result["entries"][0] == {
        "id": "filing-1",
        "title": "New filing",
        "link": "https://example.test/filing-1",
        "published_at": "2026-09-20T11:30:00+00:00",
        "fetched_at": fetched_at.isoformat(),
        "source": "https://example.test/feed",
        "summary": None,
    }


def test_http_request_supports_form_raw_multipart_and_binary_downloads():
    requests = []

    def handler(request):
        requests.append((request.headers.get("content-type"), request.read()))
        if request.url.path == "/binary":
            return httpx.Response(200, content=b"\x00\x01", headers={"content-type": "application/octet-stream"})
        return httpx.Response(200, text="ok")

    client = WebClient(transport=httpx.MockTransport(handler), resolver=_resolver)

    client.request("POST", "https://example.test/form", form_body={"side": "buy"})
    client.request("POST", "https://example.test/raw", raw_body="exact body")
    client.request(
        "POST",
        "https://example.test/upload",
        files={"report": {"filename": "report.txt", "content": "proof", "content_type": "text/plain"}},
    )
    binary = client.request("GET", "https://example.test/binary")

    assert requests[0][1] == b"side=buy"
    assert requests[1][1] == b"exact body"
    assert b'filename="report.txt"' in requests[2][1]
    assert b"proof" in requests[2][1]
    assert binary["body_base64"] == "AAE="


def test_http_request_returns_pdf_text_instead_of_base64(monkeypatch):
    pdf = b"%PDF-1.4\n" + b"0" * 32

    def handler(request):
        return httpx.Response(200, content=pdf, headers={"content-type": "application/pdf"})

    monkeypatch.setattr(
        "lumibot.components.house_ptr.pdf_bytes_to_text",
        lambda raw: "Nancy Pelosi GOOGL purchase " + ("x" * 20_000),
    )
    client = WebClient(transport=httpx.MockTransport(handler), resolver=_resolver)

    result = client.request("GET", "https://example.test/filing.pdf")

    assert result["text"].startswith("Nancy Pelosi GOOGL purchase")
    assert len(result["text"]) == 12_000
    assert result["text_truncated"] is True
    assert "body_base64" not in result


def test_http_request_supports_basic_api_key_custom_header_cookie_and_client_cert_profiles():
    seen = []

    def handler(request):
        seen.append(dict(request.headers))
        return httpx.Response(200, text="ok")

    profiles = {
        "basic": CredentialProfile.basic(
            name="basic", username="agent", password="secret", allowed_hosts=["example.test"]
        ),
        "api": CredentialProfile.api_key(
            name="api", key="api-secret", header="X-API-Key", allowed_hosts=["example.test"]
        ),
        "custom": CredentialProfile(
            name="custom",
            allowed_hosts=("example.test",),
            headers={"X-Custom-Auth": "custom-secret"},
            cookies={"session": "cookie-secret"},
        ),
    }
    client = WebClient(credential_profiles=profiles, transport=httpx.MockTransport(handler), resolver=_resolver)

    for profile_name in profiles:
        result = client.request("GET", "https://example.test/private", credential_profile=profile_name)
        assert all(secret not in json.dumps(result) for secret in ("secret", "api-secret", "custom-secret"))

    assert seen[0]["authorization"].startswith("Basic ")
    assert seen[1]["x-api-key"] == "api-secret"
    assert seen[2]["x-custom-auth"] == "custom-secret"
    assert seen[2]["cookie"] == "session=cookie-secret"
    mapped = CredentialProfile.from_mapping(
        "cert", {"type": "headers", "allowed_hosts": ["example.test"], "client_cert": ["cert.pem", "key.pem"]}
    )
    assert mapped.client_cert == ("cert.pem", "key.pem")


def test_http_request_scopes_and_redacts_credentials():
    seen_authorization = []

    def handler(request):
        seen_authorization.append(request.headers.get("authorization"))
        return httpx.Response(200, text="ok")

    profile = CredentialProfile.bearer(
        name="research-api",
        token="super-secret-token",
        allowed_hosts=["api.example.test"],
    )
    client = WebClient(
        credential_profiles={profile.name: profile},
        transport=httpx.MockTransport(handler),
        resolver=_resolver,
    )

    result = client.request("GET", "https://api.example.test/data", credential_profile="research-api")

    assert seen_authorization == ["Bearer super-secret-token"]
    assert "super-secret-token" not in json.dumps(result)
    with pytest.raises(ValueError, match="is not allowed for host"):
        client.request("GET", "https://evil.example.test/data", credential_profile="research-api")


def test_profile_cookies_do_not_leak_to_another_host():
    seen = []

    def handler(request):
        seen.append((request.headers.get("host"), request.headers.get("cookie")))
        return httpx.Response(200, text="ok")

    profile = CredentialProfile(
        name="portal",
        allowed_hosts=("api.example.test",),
        cookies={"session": "host-secret"},
    )
    client = WebClient(
        credential_profiles={profile.name: profile},
        transport=httpx.MockTransport(handler),
        resolver=_resolver,
    )

    client.request("GET", "https://api.example.test/private", credential_profile="portal")
    client.request("GET", "https://other.example.test/public")

    assert seen == [
        ("api.example.test", "session=host-secret"),
        ("other.example.test", None),
    ]


def test_http_request_persists_cookies_and_rechecks_redirect_targets():
    request_count = 0

    def handler(request):
        nonlocal request_count
        request_count += 1
        if request.url.path == "/login":
            return httpx.Response(200, headers={"set-cookie": "session=abc; Path=/"}, text="logged in")
        if request.url.path == "/me":
            return httpx.Response(200, json={"cookie": request.headers.get("cookie")})
        return httpx.Response(302, headers={"location": "http://169.254.169.254/latest/meta-data/"})

    client = WebClient(transport=httpx.MockTransport(handler), resolver=_resolver)
    client.request("POST", "https://example.test/login")
    me = client.request("GET", "https://example.test/me")

    assert me["json"]["cookie"] == "session=abc"
    with pytest.raises(ValueError, match="private, loopback, link-local, or reserved"):
        client.request("GET", "https://example.test/redirect")
    assert request_count == 3


def test_http_request_enforces_response_limit_and_parses_rss_with_cache_validators():
    feed = b"""<?xml version='1.0'?>
    <rss version='2.0'><channel><title>Trades</title>
      <item><guid>1</guid><title>New filing</title><link>https://example.test/1</link>
      <pubDate>Fri, 18 Sep 2026 12:00:00 GMT</pubDate><description>Details</description></item>
    </channel></rss>"""

    def handler(request):
        if request.url.path == "/large":
            return httpx.Response(200, content=b"x" * 32)
        return httpx.Response(
            200,
            content=feed,
            headers={
                "content-type": "application/rss+xml",
                "etag": '"v1"',
                "last-modified": "Fri, 18 Sep 2026 12:00:00 GMT",
            },
        )

    client = WebClient(transport=httpx.MockTransport(handler), resolver=_resolver, max_response_bytes=16)

    with pytest.raises(ValueError, match="exceeded the 16 byte limit"):
        client.request("GET", "https://example.test/large")

    feed_result = client.fetch_feed("https://example.test/feed", max_response_bytes=1024)
    assert feed_result["feed"]["title"] == "Trades"
    assert feed_result["entries"][0]["id"] == "1"
    assert feed_result["etag"] == '"v1"'
    assert feed_result["last_modified"] == "Fri, 18 Sep 2026 12:00:00 GMT"


def test_rss_fetch_sends_validators_handles_304_and_rejects_malformed_xml():
    requests = []
    feed = b"<rss><channel><title>Updates</title><item><guid>1</guid></item></channel></rss>"

    def handler(request):
        requests.append(dict(request.headers))
        if request.url.path == "/malformed":
            return httpx.Response(200, text="<rss>", headers={"content-type": "application/rss+xml"})
        if len(requests) == 1:
            return httpx.Response(
                200,
                content=feed,
                headers={"content-type": "application/rss+xml", "etag": '"feed-v1"'},
            )
        return httpx.Response(304)

    client = WebClient(transport=httpx.MockTransport(handler), resolver=_resolver)

    first = client.fetch_feed("https://example.test/feed")
    second = client.fetch_feed("https://example.test/feed")

    assert first["entry_count"] == 1
    assert requests[1]["if-none-match"] == '"feed-v1"'
    assert second["ok"] is True
    assert second["not_modified"] is True
    assert second["url"] == "https://example.test/feed"
    assert second["entries"] == []
    assert second["source"] == "https://example.test/feed"
    assert second["published_at"] is None
    assert second["fetched_at"].endswith("+00:00")
    assert len(second["id"]) == 64
    with pytest.raises(ValueError, match="Invalid RSS/Atom XML"):
        client.fetch_feed("https://example.test/malformed")


def test_http_request_returns_a_structured_redacted_transport_error():
    def handler(request):
        raise httpx.ReadTimeout("timed out with secret-query", request=request)

    client = WebClient(transport=httpx.MockTransport(handler), resolver=_resolver)

    result = client.request("GET", "https://example.test/data?token=secret-query")

    assert result["ok"] is False
    assert result["status_code"] is None
    assert result["method"] == "GET"
    assert result["url"] == "https://example.test/data"
    assert result["source"] == "https://example.test/data"
    assert result["published_at"] is None
    assert result["fetched_at"]
    assert result["id"]
    assert result["redirects"] == []
    assert result["error"] == {"type": "ReadTimeout", "message": "HTTP transport failed."}


def test_web_tools_are_exposed_as_agent_builtins():
    names = {definition.name for definition in BuiltinTools.all()}
    assert {"http_request", "rss_fetch"}.issubset(names)


def test_web_types_are_public_agent_exports():
    from lumibot.components.agents import CredentialProfile as ExportedProfile
    from lumibot.components.agents import WebClient as ExportedClient

    assert ExportedProfile is CredentialProfile
    assert ExportedClient is WebClient


def test_fetch_feed_sends_the_sec_user_agent_only_to_sec_hosts():
    # The SEC contact User-Agent must go to sec.gov and its subdomains only. A plain
    # substring check also matched hosts such as "sec.gov.example.test" and URLs that
    # merely mention sec.gov in a path or query (CodeQL: incomplete URL sanitization).
    from lumibot.fundamentals.sec import DEFAULT_SEC_USER_AGENT

    seen = {}
    feed = b"<rss><channel><title>t</title></channel></rss>"

    def handler(request):
        seen[_original_url(request)] = request.headers.get("user-agent")
        return httpx.Response(200, content=feed, headers={"content-type": "application/rss+xml"})

    client = WebClient(transport=httpx.MockTransport(handler), resolver=_resolver)
    sec_urls = ["https://www.sec.gov/feed", "https://sec.gov/feed"]
    other_urls = [
        "https://sec.gov.example.test/feed",
        "https://example.test/sec.gov/feed",
        "https://example.test/feed?next=sec.gov",
        "https://notsec.gov/feed",
    ]
    for url in sec_urls + other_urls:
        client.fetch_feed(url)

    for url in sec_urls:
        assert seen[url] == DEFAULT_SEC_USER_AGENT
    for url in other_urls:
        assert seen[url] != DEFAULT_SEC_USER_AGENT


def test_http_request_stops_reading_an_oversized_body_at_the_limit():
    # A URL the agent picks can return a body of many gigabytes. The size limit must
    # stop reading once it is exceeded instead of loading the whole body into memory
    # and checking afterward (CodeRabbit finding on the 4.5.92 release).
    chunk_size = 8
    total_chunks = 10_000
    consumed = {"chunks": 0}

    def endless_body():
        for _ in range(total_chunks):
            consumed["chunks"] += 1
            yield b"x" * chunk_size

    def handler(request):
        return httpx.Response(200, content=endless_body(), headers={"content-type": "text/plain"})

    client = WebClient(transport=httpx.MockTransport(handler), resolver=_resolver, max_response_bytes=16)

    with pytest.raises(ValueError, match="exceeded the 16 byte limit"):
        client.request("GET", "https://example.test/huge")

    # 16 bytes is two 8-byte chunks; the third chunk crosses the limit. Nothing past it.
    assert consumed["chunks"] <= 3


def test_http_request_streams_normal_bodies_within_the_limit():
    def handler(request):
        if request.url.path == "/json":
            return httpx.Response(200, content=iter([b'{"a"', b": 1}"]), headers={"content-type": "application/json"})
        return httpx.Response(
            200,
            content=iter(["café ".encode("latin-1"), b"ok"]),
            headers={"content-type": "text/plain; charset=latin-1"},
        )

    client = WebClient(transport=httpx.MockTransport(handler), resolver=_resolver, max_response_bytes=64)

    assert client.request("GET", "https://example.test/json")["json"] == {"a": 1}
    text_result = client.request("GET", "https://example.test/text")
    assert text_result["text"] == "café ok"
    assert text_result["content_length"] == len("café ok".encode("latin-1"))


def _rebinding_resolver(calls):
    # Answers a public address the first time each host is resolved and a loopback
    # address every time after, like an attacker-controlled DNS rebinding domain.
    def resolve(hostname):
        calls.append(hostname)
        if calls.count(hostname) == 1:
            return [PUBLIC_IP]
        return ["127.0.0.1"]

    return resolve


def test_http_request_pins_the_connection_to_the_validated_address():
    # SSRF via DNS rebinding: the host is validated against one DNS answer, so the
    # connection must go to that exact address instead of resolving the name again.
    seen = []

    def handler(request):
        seen.append(
            (request.url.host, request.headers.get("host"), request.extensions.get("sni_hostname"), request.url.path)
        )
        return httpx.Response(200, text="ok")

    resolver_calls = []
    client = WebClient(transport=httpx.MockTransport(handler), resolver=_rebinding_resolver(resolver_calls))

    result = client.request("GET", "https://example.test:8443/data?x=1")

    assert seen == [(PUBLIC_IP, "example.test:8443", "example.test", "/data")]
    assert result["url"] == "https://example.test:8443/data"
    assert resolver_calls == ["example.test"]


def test_http_request_pins_every_redirect_hop_and_never_reaches_a_rebound_address():
    seen = []

    def handler(request):
        seen.append((request.url.host, request.headers.get("host"), request.extensions.get("sni_hostname")))
        if request.headers.get("host") == "start.test":
            return httpx.Response(302, headers={"location": "https://rebind.test/next"})
        if request.headers.get("host") == "rebind.test":
            return httpx.Response(302, headers={"location": "http://final.test/done"})
        return httpx.Response(200, text="done")

    resolver_calls = []
    client = WebClient(transport=httpx.MockTransport(handler), resolver=_rebinding_resolver(resolver_calls))

    result = client.request("GET", "https://start.test/begin")

    assert result["ok"] is True
    assert result["url"] == "http://final.test/done"
    assert [hop["url"] for hop in result["redirects"]] == ["https://rebind.test/next", "http://final.test/done"]
    assert all(host != "127.0.0.1" for host, _, _ in seen)
    assert seen == [
        (PUBLIC_IP, "start.test", "start.test"),
        (PUBLIC_IP, "rebind.test", "rebind.test"),
        (PUBLIC_IP, "final.test", None),
    ]


def test_http_request_pins_ipv6_addresses_and_keeps_cookies_scoped_to_the_hostname():
    ipv6 = "2606:4700:4700::1111"
    seen = []

    def handler(request):
        seen.append((request.url.host, request.headers.get("host"), request.headers.get("cookie")))
        if request.url.path == "/login":
            return httpx.Response(200, headers={"set-cookie": "session=abc; Path=/"}, text="ok")
        return httpx.Response(200, text="ok")

    client = WebClient(transport=httpx.MockTransport(handler), resolver=lambda hostname: [ipv6])

    client.request("POST", "https://a.example.test/login")
    client.request("GET", "https://a.example.test/me")
    # Another hostname on the same shared address (a CDN, say) must not get a.example.test's cookie.
    client.request("GET", "https://b.example.test/me")

    assert seen == [
        (ipv6, "a.example.test", None),
        (ipv6, "a.example.test", "session=abc"),
        (ipv6, "b.example.test", None),
    ]
