"""Exercise real JWT/JWK verification with synthetic keys and offline transports."""

import io
import json
import time
from urllib.parse import parse_qs, urlencode, urlsplit
from uuid import uuid4

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from lumibot.components.agents import chatgpt_auth as auth


@pytest.fixture(scope="module")
def signing_keys():
    return [rsa.generate_private_key(public_exponent=65537, key_size=2048) for _ in range(2)]


@pytest.mark.parametrize("failure", [None, "signature", "algorithm", "issuer", "audience", "expired", "nonce", "subject",
                                     "missing_exp", "missing_iss", "missing_aud", "missing_sub", "missing_nonce"])
def test_login_real_signed_identity_checks_preserve_existing_grant(tmp_path, monkeypatch, signing_keys, failure):
    profile_id = str(uuid4())
    profile = tmp_path / "profiles" / f"{profile_id}.json"
    previous = {"status": "connected", "subject": "synthetic-subject", "client_id": "synthetic-client",
                "scopes": [auth.PLAN_SCOPE], "access_token": "old-synthetic", "refresh_token": "old-refresh",
                "saved_at": time.time(), "expires_in": 3600}
    auth.save_private(profile, previous)
    authorization = {}

    class OfflineCallbackServer:
        def __init__(self, address, handler):
            self.server_port = 12345
            self.handler = handler

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def handle_request(self):
            url = json.loads((tmp_path / "pending-browser.json").read_text())["url"]
            authorization.update(parse_qs(urlsplit(url).query))
            # Invoke the real callback handler without opening a browser/socket.
            handler = object.__new__(self.handler)
            handler.path = "/auth/callback?" + urlencode({"code": "synthetic-code", "state": authorization["state"][0]})
            handler.send_response = lambda *args: None
            handler.send_header = lambda *args: None
            handler.end_headers = lambda: None
            handler.wfile = io.BytesIO()
            handler.do_GET()

    def respond(request):
        assert request.url.host == "auth.openai.com"
        if request.method == "GET":
            return httpx.Response(200, json={"jwks_uri": auth.AUTH + "/synthetic-jwks"})
        claims = {"sub": "synthetic-subject", "nonce": authorization["nonce"][0],
                  "iss": auth.AUTH, "aud": "synthetic-client", "exp": int(time.time()) + 300}
        if failure == "issuer":
            claims["iss"] = "https://wrong.invalid"
        elif failure == "audience":
            claims["aud"] = "another-client"
        elif failure == "expired":
            claims["exp"] = int(time.time()) - 60
        elif failure in {"nonce", "subject"}:
            claims["nonce" if failure == "nonce" else "sub"] = "different"
        elif failure and failure.startswith("missing_"):
            del claims[failure.removeprefix("missing_")]
        key = signing_keys[1] if failure == "signature" else signing_keys[0]
        algorithm = "HS256" if failure == "algorithm" else "RS256"
        token = jwt.encode(claims, "synthetic-symmetric-key-long-enough" if algorithm == "HS256" else key,
                           algorithm=algorithm, headers={"kid": "synthetic-key"})
        return httpx.Response(200, json={"id_token": token, "access_token": "new-synthetic",
                                        "refresh_token": "new-refresh", "expires_in": 3600, "scope": auth.PLAN_SCOPE})

    public_jwk = jwt.algorithms.RSAAlgorithm.to_jwk(signing_keys[0].public_key(), as_dict=True)
    public_jwk.update(kid="synthetic-key", use="sig", alg="RS256")
    monkeypatch.setattr(jwt.PyJWKClient, "fetch_data", lambda self: {"keys": [public_jwk]})
    monkeypatch.setattr(auth, "HTTPServer", OfflineCallbackServer)
    client_type = httpx.Client
    monkeypatch.setattr(auth.httpx, "Client", lambda **kwargs: client_type(transport=httpx.MockTransport(respond), **kwargs))
    if failure:
        with pytest.raises((jwt.PyJWTError, ValueError)):
            auth.login(root=tmp_path, profile_id=profile_id, open_browser=False, timeout_seconds=2)
        assert auth.read_private(profile) == previous
    else:
        auth.login(root=tmp_path, profile_id=profile_id, open_browser=False, timeout_seconds=2)
        saved = auth.read_private(profile)
        assert saved["subject"] == previous["subject"]
        assert saved["status"] == "connected"
        assert saved["access_token"] == "new-synthetic"
    assert not (tmp_path / "pending-browser.json").exists()
