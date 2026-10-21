import http.server
import json
import threading
import time

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import HTTPException
from pydantic import ValidationError

from slurm_monitor.api.routes import verify_token
from slurm_monitor.app_settings import AppSettings, OAuthSettings


@pytest.fixture
def jwks_server():
    """
    Serve the public part of a fresh RSA key as JWKS on localhost, standing in
    for the identity provider's internal address.
    """
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(private_key.public_key()))
    jwk.update({"kid": "test-key", "use": "sig", "alg": "RS256"})
    body = json.dumps({"keys": [jwk]}).encode()

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}/certs", private_key
    server.shutdown()


def make_token(private_key, issuer: str, aud: str | list[str] = "account") -> str:
    now = int(time.time())
    payload = {
        "exp": now + 300,
        "iat": now,
        "jti": "test-jti",
        "iss": issuer,
        "aud": aud,
        "sub": "test-sub",
        "typ": "Bearer",
        "azp": "slurm-monitor",
        "sid": "test-sid",
        "acr": 1,
        "realm_access": {"roles": []},
        "resource_access": {"account": {"roles": []}},
        "scope": "email profile",
        "email_verified": False,
        "name": "Test User",
        "preferred_username": "test-user",
        "given_name": "Test",
        "family_name": "User",
        "email": "test-user@xyz.com",
    }
    return jwt.encode(payload, private_key, algorithm="RS256", headers={"kid": "test-key"})


def use_oauth(monkeypatch, jwks_url: str, **kwargs) -> OAuthSettings:
    oauth = OAuthSettings(required=True, url="https://public.invalid", realm="test-realm", jwks=jwks_url, **kwargs)
    monkeypatch.setattr(AppSettings, "_instance", AppSettings(oauth=oauth), raising=False)
    return oauth


@pytest.mark.asyncio
async def test_token_validated_with_keys_from_jwks_url(monkeypatch, jwks_server):
    jwks_url, private_key = jwks_server
    # tokens are issued for a public url the api server cannot reach, so the
    # signing keys have to be fetched from elsewhere
    oauth = use_oauth(monkeypatch, jwks_url)

    token_payload = await verify_token(make_token(private_key, oauth.issuer))
    assert token_payload.preferred_username == "test-user"


@pytest.mark.asyncio
async def test_token_for_another_audience_is_rejected(monkeypatch, jwks_server):
    jwks_url, private_key = jwks_server
    oauth = use_oauth(monkeypatch, jwks_url)

    with pytest.raises(HTTPException) as e:
        await verify_token(make_token(private_key, oauth.issuer, aud="some-other-client"))
    assert e.value.status_code == 401


@pytest.mark.asyncio
async def test_configured_audience_replaces_default(monkeypatch, jwks_server):
    jwks_url, private_key = jwks_server
    oauth = use_oauth(monkeypatch, jwks_url, audience="slurm-monitor")

    token_payload = await verify_token(make_token(private_key, oauth.issuer, aud="slurm-monitor"))
    assert token_payload.aud == "slurm-monitor"

    # a sign-in token for any other client of the realm carries "account"
    with pytest.raises(HTTPException) as e:
        await verify_token(make_token(private_key, oauth.issuer, aud="account"))
    assert e.value.status_code == 401


@pytest.mark.asyncio
async def test_token_with_several_audiences(monkeypatch, jwks_server):
    jwks_url, private_key = jwks_server
    oauth = use_oauth(monkeypatch, jwks_url, audience="slurm-monitor, account")

    token_payload = await verify_token(make_token(private_key, oauth.issuer, aud=["other-client", "slurm-monitor"]))
    assert token_payload.aud == ["other-client", "slurm-monitor"]

    with pytest.raises(HTTPException):
        await verify_token(make_token(private_key, oauth.issuer, aud=["other-client", "third-client"]))


def test_oauth_audience_from_env(monkeypatch):
    assert AppSettings().oauth.audiences == ["account"]

    monkeypatch.setenv("SLURM_MONITOR_OAUTH_AUDIENCE", " slurm-monitor ,account,")
    assert AppSettings().oauth.audiences == ["slurm-monitor", "account"]


@pytest.mark.parametrize("audience", ["", " , "])
def test_oauth_audience_must_not_be_empty(audience):
    with pytest.raises(ValidationError, match="at least one audience"):
        OAuthSettings(audience=audience)


def test_oauth_jwks_from_env(monkeypatch):
    monkeypatch.setenv("SLURM_MONITOR_OAUTH_URL", "https://public.invalid")
    monkeypatch.setenv("SLURM_MONITOR_OAUTH_REALM", "test-realm")

    settings = AppSettings()
    assert settings.oauth.jwks_url == "https://public.invalid/realms/test-realm/protocol/openid-connect/certs"

    monkeypatch.setenv("SLURM_MONITOR_OAUTH_JWKS", "http://keycloak:8080/certs")
    settings = AppSettings()
    assert settings.oauth.issuer == "https://public.invalid/realms/test-realm"
    assert settings.oauth.jwks_url == "http://keycloak:8080/certs"
