"""Release-eval process isolation, below the unchanged production tool surface."""

import os
from contextlib import contextmanager
from urllib.parse import urlsplit


def configure_fixture_environment(root):
    """Import only the approved inference credential, never a developer broker."""
    from dotenv import dotenv_values

    # GPT-6 Luna is the default eval model and needs OPENAI_API_KEY. Gemini is
    # an explicit opt-in and keeps its key only when one is present.
    values = None

    def lookup(*names):
        nonlocal values
        for name in names:
            if os.environ.get(name):
                return os.environ[name]
        if values is None:
            values = {**dotenv_values(root / ".env"), **dotenv_values(root / ".env.local")}
        for name in names:
            if values.get(name):
                return values[name]
        return None

    openai_key = lookup("OPENAI_API_KEY")
    gemini_key = lookup("GEMINI_API_KEY", "GOOGLE_API_KEY")
    if not openai_key and not gemini_key:
        raise RuntimeError("An approved eval inference credential (OPENAI_API_KEY) is required.")
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
    # litellm >= 1.102 downloads its model price map from GitHub on import;
    # the fixture boundary rejects that request, so use the bundled map.
    os.environ.update(
        LUMIBOT_DISABLE_DOTENV="true", IS_BACKTESTING="true", LITELLM_LOCAL_MODEL_COST_MAP="True"
    )
    if openai_key:
        os.environ["OPENAI_API_KEY"] = openai_key
    if gemini_key:
        os.environ.update(GOOGLE_API_KEY=gemini_key, GEMINI_API_KEY=gemini_key)


_INFERENCE_ENDPOINTS = {
    "generativelanguage.googleapis.com": lambda path: path.endswith(
        (":generateContent", ":streamGenerateContent", ":countTokens")
    ),
    # GPT-6 Luna runs through the OpenAI Responses API (Chat Completions when
    # reasoning is off). Nothing else on the OpenAI API is reachable.
    "api.openai.com": lambda path: path in {"/v1/responses", "/v1/chat/completions"},
}


def assert_fixture_request(url, method):
    target = urlsplit(str(url))
    path_allowed = _INFERENCE_ENDPOINTS.get(target.hostname or "")
    if (
        target.scheme != "https"
        or path_allowed is None
        or target.port not in (None, 443)
        or target.username
        or target.password
        or method.upper() != "POST"
        or not path_allowed(target.path)
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
