"""Release-eval process isolation, below the unchanged production tool surface."""

import os
from contextlib import contextmanager
from urllib.parse import urlsplit


def configure_fixture_environment(root, *, artifact_root=None):
    """Keep only the explicitly selected subscription profile and OS settings."""
    retained = {
        name: value for name, value in os.environ.items() if name in {
            "PATH", "HOME", "TMPDIR", "TEMP", "TMP", "LANG", "LC_ALL", "TZ", "SYSTEMROOT",
            "SSL_CERT_FILE", "SSL_CERT_DIR", "REQUESTS_CA_BUNDLE",
            "LUMIBOT_CHATGPT_HOME", "LUMIBOT_CHATGPT_PROFILE", "LUMIBOT_CHATGPT_PROFILE_PATH",
        }
    }
    os.environ.clear()
    os.environ.update(retained)
    os.environ.update(LUMIBOT_DISABLE_DOTENV="true", IS_BACKTESTING="true", LITELLM_LOCAL_MODEL_COST_MAP="True")
    if artifact_root is not None:
        # Each producer's owned output holds traces and caches, never the developer's shared cache.
        from pathlib import Path
        os.environ["LUMIBOT_CACHE_FOLDER"] = str(Path(artifact_root).resolve() / "runtime_cache")


_INFERENCE_ENDPOINTS = {
    "api.openai.com": lambda path: path == "/v1/responses",
}


def assert_fixture_request(url, method):
    target = urlsplit(str(url))
    path_allowed = _INFERENCE_ENDPOINTS.get(target.hostname or "")
    refresh = target.hostname == "auth.openai.com" and target.path == "/api/accounts/oauth/token" and method.upper() == "POST"
    catalog = target.hostname == "api.openai.com" and target.path == "/v1/models" and method.upper() == "GET"
    if (
        target.scheme != "https"
        or (path_allowed is None and not refresh and not catalog)
        or target.port not in (None, 443)
        or target.username
        or target.password
        or (method.upper() != "POST" and not catalog)
        or (not refresh and not catalog and not path_allowed(target.path))
    ):
        # Do not echo URLs: an accidental SDK request may contain a credential.
        raise RuntimeError("Eval external boundary rejected a non-inference request; use fixture data.")


@contextmanager
def fixture_network_boundary():
    """Permit subscription Responses/auth only; fixture tools cannot reach brokers."""
    from unittest.mock import patch

    import httpx
    import requests

    requests_send = requests.sessions.Session.send
    sync_send, async_send = httpx.Client.send, httpx.AsyncClient.send

    def requests_only(session, request, **kwargs):
        assert_fixture_request(request.url, request.method)
        return requests_send(session, request, **kwargs)

    def serves_fixture_data(client):
        # A MockTransport answers from memory; the request never leaves the process.
        transport = getattr(client, "_transport", None)
        return isinstance(transport, httpx.MockTransport) or isinstance(
            getattr(transport, "_shared_transport", None), httpx.MockTransport
        )

    def sync_only(client, request, **kwargs):
        if not serves_fixture_data(client):
            assert_fixture_request(request.url, request.method)
        return sync_send(client, request, **kwargs)

    async def async_only(client, request, **kwargs):
        assert_fixture_request(request.url, request.method)
        return await async_send(client, request, **kwargs)

    with patch.object(requests.sessions.Session, "send", requests_only), patch.object(
        httpx.Client, "send", sync_only
    ), patch.object(httpx.AsyncClient, "send", async_only):
        yield
