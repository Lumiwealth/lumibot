from __future__ import annotations

import base64
import hashlib
import ipaddress
import json
import socket
import xml.etree.ElementTree as ET
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Iterable
from urllib.parse import urljoin, urlsplit, urlunsplit

import httpx

_ALLOWED_METHODS = {"GET", "HEAD", "OPTIONS", "POST", "PUT", "PATCH", "DELETE"}
_REDIRECT_STATUSES = {301, 302, 303, 307, 308}
_SAFE_RESPONSE_HEADERS = {"content-type", "content-length", "date", "etag", "last-modified", "location", "retry-after"}
# Request extension that carries the address WebClient validated for this request's host.
_PINNED_ADDRESS_EXTENSION = "lumibot_pinned_address"
# Upper bound on per-host connection pools kept open by one client.
_MAX_PINNED_HOST_POOLS = 32


def _default_resolver(hostname: str) -> list[str]:
    return sorted({entry[4][0] for entry in socket.getaddrinfo(hostname, None, type=socket.SOCK_STREAM)})


def _host_matches(hostname: str, pattern: str) -> bool:
    hostname = hostname.lower().rstrip(".")
    pattern = pattern.lower().rstrip(".")
    if pattern.startswith("*."):
        suffix = pattern[1:]
        return hostname.endswith(suffix) and hostname != suffix[1:]
    return hostname == pattern


def _safe_url(url: str) -> str:
    parsed = urlsplit(url)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


@dataclass(frozen=True)
class CredentialProfile:
    """Credentials injected by the host without exposing their values to an agent."""

    name: str
    allowed_hosts: tuple[str, ...]
    headers: dict[str, str] = field(default_factory=dict)
    cookies: dict[str, str] = field(default_factory=dict)
    basic_username: str | None = None
    basic_password: str | None = None
    client_cert: str | tuple[str, str] | None = None

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("Credential profile name must not be empty.")
        if not self.allowed_hosts:
            raise ValueError("Credential profiles must declare at least one allowed host.")

    @classmethod
    def bearer(cls, *, name: str, token: str, allowed_hosts: Iterable[str]) -> CredentialProfile:
        return cls(name=name, allowed_hosts=tuple(allowed_hosts), headers={"Authorization": f"Bearer {token}"})

    @classmethod
    def basic(
        cls,
        *,
        name: str,
        username: str,
        password: str,
        allowed_hosts: Iterable[str],
    ) -> CredentialProfile:
        return cls(
            name=name,
            allowed_hosts=tuple(allowed_hosts),
            basic_username=username,
            basic_password=password,
        )

    @classmethod
    def api_key(
        cls,
        *,
        name: str,
        key: str,
        header: str,
        allowed_hosts: Iterable[str],
    ) -> CredentialProfile:
        return cls(name=name, allowed_hosts=tuple(allowed_hosts), headers={header: key})

    @classmethod
    def from_mapping(cls, name: str, value: dict[str, Any]) -> CredentialProfile:
        profile_type = str(value.get("type") or "headers").strip().lower()
        allowed_hosts = tuple(str(host) for host in value.get("allowed_hosts") or ())
        client_cert = value.get("client_cert")
        if isinstance(client_cert, list):
            client_cert = tuple(str(item) for item in client_cert)
        if profile_type == "bearer":
            return cls.bearer(name=name, token=str(value.get("token") or ""), allowed_hosts=allowed_hosts)
        if profile_type == "basic":
            return cls.basic(
                name=name,
                username=str(value.get("username") or ""),
                password=str(value.get("password") or ""),
                allowed_hosts=allowed_hosts,
            )
        if profile_type == "api_key":
            return cls.api_key(
                name=name,
                key=str(value.get("key") or ""),
                header=str(value.get("header") or "X-API-Key"),
                allowed_hosts=allowed_hosts,
            )
        if profile_type not in {"headers", "cookies"}:
            raise ValueError(f"Unsupported credential profile type {profile_type!r}.")
        return cls(
            name=name,
            allowed_hosts=allowed_hosts,
            headers={str(key): str(item) for key, item in dict(value.get("headers") or {}).items()},
            cookies={str(key): str(item) for key, item in dict(value.get("cookies") or {}).items()},
            client_cert=client_cert,
        )

    def ensure_host_allowed(self, hostname: str) -> None:
        if not any(_host_matches(hostname, pattern) for pattern in self.allowed_hosts):
            raise ValueError(f"Credential profile {self.name!r} is not allowed for host {hostname!r}.")



def _is_sec_host(url: str) -> bool:
    """True only when the URL's host is sec.gov or one of its subdomains.

    The SEC asks for a contact User-Agent, and that header must not be sent to
    other hosts that merely mention sec.gov in their name, path, or query.
    """
    host = (urlsplit(str(url)).hostname or "").lower().rstrip(".")
    return host == "sec.gov" or host.endswith(".sec.gov")


class _PinnedAddressTransport(httpx.BaseTransport):
    """Connects each request to the exact address WebClient already validated.

    Why: WebClient checks that a hostname resolves to a public address before it
    sends anything. If the connection then resolved the hostname again, a DNS
    rebinding domain could answer with a public address for the check and with
    127.0.0.1 or the cloud metadata address (169.254.169.254) for the connection.

    Invariants:
    - A request without a validated address is refused (fail closed).
    - The outgoing URL uses the validated IP, the original Host header is kept,
      and https requests set ``sni_hostname`` so TLS still verifies the real name.
    - httpx replaces ``response.request`` with the original request after this
      transport returns, so cookies, redirects, and reported URLs stay keyed to
      the hostname, never to a shared IP address.
    - Without an injected transport, each hostname gets its own connection pool.
      Pools are keyed by IP, so a shared pool could reuse a TLS session made for
      one hostname to send another hostname's request on a shared address.
    """

    def __init__(
        self,
        *,
        transport: httpx.BaseTransport | None = None,
        cert: str | tuple[str, str] | None = None,
    ) -> None:
        self._shared_transport = transport
        self._cert = cert
        self._ssl_context: Any = None
        self._host_transports: OrderedDict[str, httpx.BaseTransport] = OrderedDict()

    def _transport_for(self, hostname: str) -> httpx.BaseTransport:
        if self._shared_transport is not None:
            return self._shared_transport
        transport = self._host_transports.get(hostname)
        if transport is not None:
            self._host_transports.move_to_end(hostname)
            return transport
        if self._ssl_context is None:
            context = httpx.create_ssl_context(verify=True, trust_env=True)
            if isinstance(self._cert, str):
                context.load_cert_chain(self._cert)
            elif self._cert:
                context.load_cert_chain(*self._cert)
            self._ssl_context = context
        transport = httpx.HTTPTransport(verify=self._ssl_context)
        self._host_transports[hostname] = transport
        while len(self._host_transports) > _MAX_PINNED_HOST_POOLS:
            _, evicted = self._host_transports.popitem(last=False)
            evicted.close()
        return transport

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        address = request.extensions.get(_PINNED_ADDRESS_EXTENSION)
        if not address:
            raise RuntimeError("Refusing to send an HTTP request without a validated address.")
        hostname = request.url.host
        extensions = {key: value for key, value in request.extensions.items() if key != _PINNED_ADDRESS_EXTENSION}
        if request.url.scheme == "https":
            extensions["sni_hostname"] = hostname
        pinned = httpx.Request(
            request.method,
            request.url.copy_with(host=address),
            headers=request.headers,
            stream=request.stream,
            extensions=extensions,
        )
        return self._transport_for(hostname).handle_request(pinned)

    def close(self) -> None:
        for transport in self._host_transports.values():
            transport.close()
        self._host_transports.clear()
        if self._shared_transport is not None:
            self._shared_transport.close()


class WebClient:
    """Stateful public-web HTTP/RSS transport with credential scoping and SSRF protection."""

    def __init__(
        self,
        *,
        credential_profiles: dict[str, CredentialProfile] | None = None,
        trusted_private_hosts: Iterable[str] = (),
        timeout_seconds: float = 30.0,
        max_response_bytes: int = 2_000_000,
        max_redirects: int = 10,
        transport: httpx.BaseTransport | None = None,
        resolver: Callable[[str], list[str]] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.credential_profiles = dict(credential_profiles or {})
        self.trusted_private_hosts = tuple(str(host) for host in trusted_private_hosts)
        self.timeout_seconds = max(float(timeout_seconds), 0.1)
        self.max_response_bytes = max(int(max_response_bytes), 1)
        self.max_redirects = max(int(max_redirects), 0)
        self._resolver = resolver or _default_resolver
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._transport = transport
        self._client = httpx.Client(
            timeout=self.timeout_seconds,
            follow_redirects=False,
            transport=_PinnedAddressTransport(transport=transport),
        )
        self._certificate_clients: dict[str, httpx.Client] = {}
        self._feed_validators: dict[str, dict[str, str]] = {}

    def close(self) -> None:
        self._client.close()
        for client in self._certificate_clients.values():
            client.close()
        self._certificate_clients.clear()

    def _request_client(self, profile: CredentialProfile | None) -> httpx.Client:
        if profile is None or profile.client_cert is None:
            return self._client
        client = self._certificate_clients.get(profile.name)
        if client is None:
            # The client certificate lives on the pinned transport: httpx ignores
            # ``cert=`` on a Client once a custom transport is supplied.
            client = httpx.Client(
                timeout=self.timeout_seconds,
                follow_redirects=False,
                transport=_PinnedAddressTransport(transport=self._transport, cert=profile.client_cert),
            )
            self._certificate_clients[profile.name] = client
        return client

    def _validate_url(self, url: str) -> str:
        return self._validate_and_pin(url)[0]

    def _validate_and_pin(self, url: str) -> tuple[str, str]:
        """Validate ``url`` and return ``(hostname, address)`` to connect to.

        The hostname is resolved exactly once. The returned address is one the
        checks below approved, and the request is pinned to it so the connection
        cannot resolve the name again (DNS rebinding).
        """
        parsed = urlsplit(str(url).strip())
        if parsed.scheme not in {"http", "https"}:
            raise ValueError("Only http and https URLs are supported.")
        if not parsed.hostname:
            raise ValueError("URL must include a hostname.")
        if parsed.username or parsed.password:
            raise ValueError("Credentials in URLs are not supported; use a credential profile.")
        hostname = parsed.hostname.lower().rstrip(".")
        trusted = any(_host_matches(hostname, pattern) for pattern in self.trusted_private_hosts)
        try:
            literal_address = ipaddress.ip_address(hostname)
        except ValueError:
            literal_address = None
        addresses = [str(literal_address)] if literal_address is not None else self._resolver(hostname)
        if not addresses:
            raise ValueError(f"Could not resolve host {hostname!r}.")
        if not trusted:
            for address in addresses:
                ip = ipaddress.ip_address(address)
                if (
                    ip.is_private
                    or ip.is_loopback
                    or ip.is_link_local
                    or ip.is_reserved
                    or ip.is_unspecified
                    or ip.is_multicast
                ):
                    raise ValueError(
                        f"Host {hostname!r} resolved to a private, loopback, link-local, or reserved address."
                    )
        return hostname, str(ipaddress.ip_address(addresses[0]))

    def _profile(self, name: str | None, hostname: str) -> CredentialProfile | None:
        if not name:
            return None
        profile = self.credential_profiles.get(name)
        if profile is None:
            available = sorted(self.credential_profiles)
            raise ValueError(f"Unknown credential profile {name!r}. Available profiles: {available}")
        profile.ensure_host_allowed(hostname)
        return profile

    @staticmethod
    def _mapping(value: str | dict[str, Any] | None, field_name: str) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return dict(value)
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{field_name} must be a JSON object.") from exc
        if not isinstance(parsed, dict):
            raise ValueError(f"{field_name} must be a JSON object.")
        return parsed

    def request(
        self,
        method: str,
        url: str,
        *,
        query: str | dict[str, Any] | None = None,
        headers: str | dict[str, Any] | None = None,
        json_body: Any | None = None,
        form_body: str | dict[str, Any] | None = None,
        raw_body: str | bytes | None = None,
        files: str | dict[str, Any] | None = None,
        credential_profile: str | None = None,
        max_response_bytes: int | None = None,
    ) -> dict[str, Any]:
        normalized_method = str(method).upper().strip()
        fetched_at = self._clock().astimezone(timezone.utc).isoformat()
        if normalized_method not in _ALLOWED_METHODS:
            raise ValueError(f"Unsupported HTTP method {normalized_method!r}.")
        body_count = sum(value is not None for value in (json_body, form_body, raw_body, files))
        if body_count > 1:
            raise ValueError("Supply only one of json_body, form_body, raw_body, or files.")

        current_url = str(url)
        current_method = normalized_method
        current_json = json_body
        current_form = self._mapping(form_body, "form_body") if form_body is not None else None
        current_content = raw_body
        current_files = self._prepare_files(files)
        request_headers = {str(key): str(value) for key, value in self._mapping(headers, "headers").items()}
        if not any(str(key).lower() == "user-agent" for key in request_headers):
            request_headers["User-Agent"] = "Lumiwealth research botspot.trade"
        params = self._mapping(query, "query") if query is not None else None
        redirects = []
        response = None

        for redirect_index in range(self.max_redirects + 1):
            # Every hop, including redirect targets, is validated once and pinned.
            hostname, address = self._validate_and_pin(current_url)
            profile = self._profile(credential_profile, hostname)
            request_client = self._request_client(profile)
            effective_headers = dict(request_headers)
            auth = None
            if profile is not None:
                effective_headers.update(profile.headers)
                if profile.cookies:
                    profile_cookie = "; ".join(f"{key}={value}" for key, value in profile.cookies.items())
                    existing_cookie = effective_headers.get("Cookie") or effective_headers.get("cookie")
                    effective_headers["Cookie"] = (
                        f"{existing_cookie}; {profile_cookie}" if existing_cookie else profile_cookie
                    )
                if profile.basic_username is not None:
                    auth = httpx.BasicAuth(profile.basic_username, profile.basic_password or "")

            try:
                outgoing = request_client.build_request(
                    current_method,
                    current_url,
                    params=params,
                    headers=effective_headers,
                    json=current_json,
                    data=current_form,
                    content=current_content,
                    files=current_files,
                    extensions={_PINNED_ADDRESS_EXTENSION: address},
                )
                # Stream so the body is never read in full before the size limit applies.
                response = request_client.send(outgoing, auth=auth, stream=True)
            except httpx.RequestError as exc:
                return self._transport_error(exc, current_method, current_url, fetched_at, redirects)
            params = None
            if response.status_code not in _REDIRECT_STATUSES or "location" not in response.headers:
                break
            response.close()
            if redirect_index >= self.max_redirects:
                raise ValueError(f"HTTP request exceeded {self.max_redirects} redirects.")
            next_url = urljoin(str(response.url), response.headers["location"])
            redirects.append({"status_code": response.status_code, "url": _safe_url(next_url)})
            if response.status_code == 303 or (response.status_code in {301, 302} and current_method == "POST"):
                current_method = "GET"
                current_json = None
                current_form = None
                current_content = None
                current_files = None
            current_url = next_url

        if response is None:
            raise RuntimeError("HTTP request did not produce a response.")
        limit = self.max_response_bytes if max_response_bytes is None else max(int(max_response_bytes), 1)
        try:
            content = self._read_limited(response, limit)
        except httpx.RequestError as exc:
            return self._transport_error(exc, current_method, current_url, fetched_at, redirects)
        finally:
            response.close()
        content_type = response.headers.get("content-type", "")
        response_url = _safe_url(str(response.url))
        published_at = None
        if response.headers.get("date"):
            try:
                published_at = parsedate_to_datetime(response.headers["date"]).astimezone(timezone.utc).isoformat()
            except (TypeError, ValueError, OverflowError):
                published_at = None
        content_sha256 = hashlib.sha256(content).hexdigest()
        result: dict[str, Any] = {
            "id": content_sha256,
            "ok": response.is_success,
            "status_code": response.status_code,
            "method": current_method,
            "url": response_url,
            "source": response_url,
            "published_at": published_at,
            "fetched_at": fetched_at,
            "headers": {
                key.lower(): value for key, value in response.headers.items() if key.lower() in _SAFE_RESPONSE_HEADERS
            },
            "redirects": redirects,
            "content_length": len(content),
            "content_sha256": content_sha256,
        }
        if content:
            # The body was streamed, so decode it here instead of response.json()/.text.
            text_encoding = response.encoding or "utf-8"
            if "json" in content_type:
                try:
                    result["json"] = json.loads(content)
                except ValueError:
                    result["text"] = content.decode(text_encoding, errors="replace")
            elif content_type.startswith("text/") or "xml" in content_type or "html" in content_type:
                result["text"] = content.decode(text_encoding, errors="replace")
            elif "pdf" in content_type.lower() or content.startswith(b"%PDF"):
                from lumibot.components.house_ptr import pdf_bytes_to_text, reflow_ptr_text

                extracted = reflow_ptr_text(pdf_bytes_to_text(content)).strip()
                if len(extracted) > 12_000:
                    result["text"] = extracted[:12_000]
                    result["text_truncated"] = True
                else:
                    result["text"] = extracted
            else:
                result["body_base64"] = base64.b64encode(content).decode("ascii")
        return result

    @staticmethod
    def _read_limited(response: httpx.Response, limit: int) -> bytes:
        """Read a streamed body, stopping as soon as it passes ``limit`` bytes.

        Why: the URL is chosen by an agent, and a multi-gigabyte body read into
        memory before the size check could take down a live trading process.
        """
        chunks = []
        total = 0
        for chunk in response.iter_bytes():
            total += len(chunk)
            if total > limit:
                raise ValueError(f"HTTP response exceeded the {limit} byte limit.")
            chunks.append(chunk)
        return b"".join(chunks)

    @staticmethod
    def _transport_error(
        exc: Exception,
        method: str,
        url: str,
        fetched_at: str,
        redirects: list[dict[str, Any]],
    ) -> dict[str, Any]:
        safe_url = _safe_url(url)
        return {
            "id": hashlib.sha256(f"{method}|{safe_url}|{type(exc).__name__}".encode()).hexdigest(),
            "ok": False,
            "status_code": None,
            "method": method,
            "url": safe_url,
            "source": safe_url,
            "published_at": None,
            "fetched_at": fetched_at,
            "redirects": redirects,
            "error": {"type": type(exc).__name__, "message": "HTTP transport failed."},
        }

    def _prepare_files(self, value: str | dict[str, Any] | None) -> dict[str, Any] | None:
        if value is None:
            return None
        prepared: dict[str, Any] = {}
        for field_name, item in self._mapping(value, "files").items():
            if not isinstance(item, dict):
                raise ValueError("Each files entry must contain filename and content or content_base64.")
            filename = str(item.get("filename") or field_name)
            content_type = str(item.get("content_type") or "application/octet-stream")
            if item.get("content_base64") is not None:
                content = base64.b64decode(str(item["content_base64"]), validate=True)
            else:
                content = str(item.get("content") or "").encode()
            prepared[str(field_name)] = (filename, content, content_type)
        return prepared

    def fetch_feed(
        self,
        url: str,
        *,
        credential_profile: str | None = None,
        max_entries: int = 100,
        max_response_bytes: int | None = None,
    ) -> dict[str, Any]:
        validators = self._feed_validators.get(url, {})
        headers = {}
        if _is_sec_host(url):
            from lumibot.fundamentals.sec import DEFAULT_SEC_USER_AGENT

            headers["User-Agent"] = DEFAULT_SEC_USER_AGENT
            headers["Accept"] = "application/atom+xml, application/rss+xml, application/xml"
        if validators.get("etag"):
            headers["If-None-Match"] = validators["etag"]
        if validators.get("last_modified"):
            headers["If-Modified-Since"] = validators["last_modified"]
        response = self.request(
            "GET",
            url,
            headers=headers,
            credential_profile=credential_profile,
            max_response_bytes=max_response_bytes,
        )
        if response["status_code"] == 304:
            return {
                "id": response["id"],
                "ok": True,
                "not_modified": True,
                "url": response["url"],
                "source": response["source"],
                "published_at": response["published_at"],
                "fetched_at": response["fetched_at"],
                "entries": [],
            }
        text = response.get("text")
        if not isinstance(text, str):
            raise ValueError("Feed response was not text or XML.")
        parsed = self._parse_feed(
            text,
            max_entries=max_entries,
            source=response["source"],
            fetched_at=response["fetched_at"],
        )
        etag = response["headers"].get("etag")
        last_modified = response["headers"].get("last-modified")
        self._feed_validators[url] = {
            key: value for key, value in {"etag": etag, "last_modified": last_modified}.items() if value
        }
        return {
            "id": response["id"],
            "ok": response["ok"],
            "not_modified": False,
            "url": response["url"],
            "source": response["source"],
            "published_at": response["published_at"],
            "fetched_at": response["fetched_at"],
            "etag": etag,
            "last_modified": last_modified,
            **parsed,
        }

    @staticmethod
    def _parse_feed(text: str, *, max_entries: int, source: str, fetched_at: str) -> dict[str, Any]:
        try:
            root = ET.fromstring(text)
        except ET.ParseError as exc:
            raise ValueError(f"Invalid RSS/Atom XML: {exc}") from exc

        def local_name(element: ET.Element) -> str:
            return element.tag.rsplit("}", 1)[-1].lower()

        def child_text(element: ET.Element, *names: str) -> str | None:
            wanted = {name.lower() for name in names}
            for child in list(element):
                if local_name(child) in wanted and child.text:
                    return child.text.strip()
            return None

        is_atom = local_name(root) == "feed"
        container = root if is_atom else next((child for child in root if local_name(child) == "channel"), root)
        feed = {
            "title": child_text(container, "title"),
            "link": child_text(container, "link"),
            "description": child_text(container, "description", "subtitle"),
        }
        entries = []
        entry_name = "entry" if is_atom else "item"
        for element in container.iter():
            if local_name(element) != entry_name:
                continue
            link = child_text(element, "link")
            if is_atom:
                link_element = next((child for child in element if local_name(child) == "link"), None)
                if link_element is not None:
                    link = link_element.attrib.get("href") or link
            published = child_text(element, "pubDate", "published", "updated")
            published_at = published
            if published:
                try:
                    published_at = parsedate_to_datetime(published).isoformat()
                except (TypeError, ValueError, OverflowError):
                    pass
            entries.append(
                {
                    "id": child_text(element, "guid", "id") or link,
                    "title": child_text(element, "title"),
                    "link": link,
                    "published_at": published_at,
                    "fetched_at": fetched_at,
                    "source": source,
                    "summary": child_text(element, "description", "summary", "content"),
                }
            )
            if len(entries) >= max(int(max_entries), 1):
                break
        return {"feed": feed, "entries": entries, "entry_count": len(entries)}
