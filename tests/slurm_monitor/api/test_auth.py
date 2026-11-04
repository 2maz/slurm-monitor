import http.server
import json
import threading
import time

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import HTTPException
from pydantic import ValidationError

from slurm_monitor.api.routes import NoneForUserWithRealmRoles, Role, verify_token
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


def make_token(private_key, issuer: str, aud: str | list[str] = "account", drop: tuple[str, ...] = ()) -> str:
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
    for claim in drop:
        payload.pop(claim)
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


# What Keycloak's token exchange (RFC 8693) for the slurm-monitor audience
# leaves out of the token: the roles of other clients, and here the profile
# claims of a user without an e-mail address or full name.
EXCHANGED_OUT = ("resource_access", "email", "email_verified", "name", "given_name", "family_name", "sid", "acr")


@pytest.mark.asyncio
async def test_exchanged_token_is_accepted(monkeypatch, jwks_server):
    jwks_url, private_key = jwks_server
    oauth = use_oauth(monkeypatch, jwks_url, audience="slurm-monitor")

    token = make_token(private_key, oauth.issuer, aud="slurm-monitor", drop=EXCHANGED_OUT)
    token_payload = await verify_token(token)

    assert token_payload.preferred_username == "test-user"
    assert token_payload.resource_access.account.roles == []


@pytest.mark.asyncio
async def test_admin_role_of_an_exchanged_token_is_kept(monkeypatch, jwks_server):
    jwks_url, private_key = jwks_server
    oauth = use_oauth(monkeypatch, jwks_url)
    now = int(time.time())
    payload = {
        "exp": now + 300,
        "iat": now,
        "iss": oauth.issuer,
        "aud": "account",
        "sub": "admin-sub",
        "preferred_username": "admin-user",
        "realm_access": {"roles": ["admin"]},
    }
    token = jwt.encode(payload, private_key, algorithm="RS256", headers={"kid": "test-key"})

    token_payload = await verify_token(token)

    # None: an admin is not limited to their own data
    assert NoneForUserWithRealmRoles([Role.ADMIN])(token_payload) is None


@pytest.mark.asyncio
async def test_token_without_roles_sees_only_its_own(monkeypatch, jwks_server):
    jwks_url, private_key = jwks_server
    oauth = use_oauth(monkeypatch, jwks_url)

    token_payload = await verify_token(make_token(private_key, oauth.issuer, drop=("realm_access", "resource_access")))

    assert NoneForUserWithRealmRoles([Role.ADMIN])(token_payload) == "test-user"


@pytest.mark.asyncio
@pytest.mark.parametrize("claim", ["preferred_username", "sub"])
async def test_token_without_identity_is_rejected(monkeypatch, jwks_server, claim):
    jwks_url, private_key = jwks_server
    oauth = use_oauth(monkeypatch, jwks_url)

    with pytest.raises(HTTPException) as e:
        await verify_token(make_token(private_key, oauth.issuer, drop=(claim,)))
    assert e.value.status_code == 401


@pytest.mark.asyncio
async def test_claims_are_not_taken_from_the_environment(monkeypatch, jwks_server):
    jwks_url, private_key = jwks_server
    oauth = use_oauth(monkeypatch, jwks_url)
    monkeypatch.setenv("PREFERRED_USERNAME", "root")
    monkeypatch.setenv("ROLES", '["admin"]')

    with pytest.raises(HTTPException):
        await verify_token(make_token(private_key, oauth.issuer, drop=("preferred_username",)))

    token_payload = await verify_token(make_token(private_key, oauth.issuer, drop=("realm_access",)))
    assert token_payload.realm_access.roles == []
