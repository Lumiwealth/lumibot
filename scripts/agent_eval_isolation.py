"""Release-eval process isolation, below the unchanged production tool surface."""

import os
from contextlib import contextmanager
from urllib.parse import urlsplit


def configure_fixture_environment(root):
    """Import only the approved inference credential, never a developer broker."""
    from dotenv import dotenv_values

    key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not key:
        values = {**dotenv_values(root / ".env"), **dotenv_values(root / ".env.local")}
        key = values.get("GEMINI_API_KEY") or values.get("GOOGLE_API_KEY")
    if not key:
        raise RuntimeError("An approved Gemini eval credential is required.")
    retained = {
        name: value
        for name, value in os.environ.items()
        if name
        in {
            "PATH",
            "HOME",
            "TMPDIR",
            "TEMP",
            "TMP",
            "LANG",
            "LC_ALL",
            "TZ",
            "SYSTEMROOT",
            "SSL_CERT_FILE",
            "SSL_CERT_DIR",
            "REQUESTS_CA_BUNDLE",
        }
    }
    os.environ.clear()
    os.environ.update(retained)
    os.environ.update(GOOGLE_API_KEY=key, GEMINI_API_KEY=key, LUMIBOT_DISABLE_DOTENV="true", IS_BACKTESTING="true")


def assert_fixture_request(url, method):
    target = urlsplit(str(url))
    allowed_paths = (":generateContent", ":streamGenerateContent", ":countTokens")
    if (
        target.scheme != "https"
        or target.hostname != "generativelanguage.googleapis.com"
        or target.port not in (None, 443)
        or target.username
        or target.password
        or method.upper() != "POST"
        or not target.path.endswith(allowed_paths)
    ):
        # Do not echo URLs: an accidental SDK request may contain a credential.
        raise RuntimeError("Eval external boundary rejected a non-inference request; use fixture data.")


@contextmanager
def fixture_network_boundary():
    """Permit billed inference/counting only; fixture tools cannot reach brokers."""
    from unittest.mock import patch

    import httpx
    import requests

    requests_send = requests.sessions.Session.send
    sync_send, async_send = httpx.Client.send, httpx.AsyncClient.send

    def requests_only(session, request, **kwargs):
        assert_fixture_request(request.url, request.method)
        return requests_send(session, request, **kwargs)

    def sync_only(client, request, **kwargs):
        assert_fixture_request(request.url, request.method)
        return sync_send(client, request, **kwargs)

    async def async_only(client, request, **kwargs):
        assert_fixture_request(request.url, request.method)
        return await async_send(client, request, **kwargs)

    with patch.object(requests.sessions.Session, "send", requests_only), patch.object(
        httpx.Client, "send", sync_only
    ), patch.object(httpx.AsyncClient, "send", async_only):
        yield
