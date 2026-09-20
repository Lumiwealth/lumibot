import json

import httpx
import pytest

from lumibot.components.agents.builtins import BuiltinTools
from lumibot.components.agents.web_tools import CredentialProfile, WebClient

PUBLIC_IP = "93.184.216.34"


def _resolver(hostname):
    return [PUBLIC_IP]


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
        seen.append((request.url.host, request.headers.get("cookie")))
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
    assert second == {
        "ok": True,
        "not_modified": True,
        "url": "https://example.test/feed",
        "entries": [],
    }
    with pytest.raises(ValueError, match="Invalid RSS/Atom XML"):
        client.fetch_feed("https://example.test/malformed")


def test_http_request_returns_a_structured_redacted_transport_error():
    def handler(request):
        raise httpx.ReadTimeout("timed out with secret-query", request=request)

    client = WebClient(transport=httpx.MockTransport(handler), resolver=_resolver)

    result = client.request("GET", "https://example.test/data?token=secret-query")

    assert result == {
        "ok": False,
        "status_code": None,
        "method": "GET",
        "url": "https://example.test/data",
        "redirects": [],
        "error": {"type": "ReadTimeout", "message": "HTTP transport failed."},
    }


def test_web_tools_are_exposed_as_agent_builtins():
    names = {definition.name for definition in BuiltinTools.all()}
    assert {"http_request", "rss_fetch"}.issubset(names)


def test_web_types_are_public_agent_exports():
    from lumibot.components.agents import CredentialProfile as ExportedProfile
    from lumibot.components.agents import WebClient as ExportedClient

    assert ExportedProfile is CredentialProfile
    assert ExportedClient is WebClient
