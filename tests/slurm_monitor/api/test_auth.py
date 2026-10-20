import http.server
import json
import threading
import time

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

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


@pytest.mark.asyncio
async def test_token_validated_with_keys_from_jwks_url(monkeypatch, jwks_server):
    jwks_url, private_key = jwks_server
    # tokens are issued for a public url the api server cannot reach, so the
    # signing keys have to be fetched from elsewhere
    oauth = OAuthSettings(required=True, url="https://public.invalid", realm="test-realm", jwks=jwks_url)
    monkeypatch.setattr(AppSettings, "_instance", AppSettings(oauth=oauth), raising=False)

    now = int(time.time())
    payload = {
        "exp": now + 300,
        "iat": now,
        "jti": "test-jti",
        "iss": oauth.issuer,
        "aud": "account",
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
    token = jwt.encode(payload, private_key, algorithm="RS256", headers={"kid": "test-key"})

    token_payload = await verify_token(token)
    assert token_payload.preferred_username == "test-user"


def test_oauth_jwks_from_env(monkeypatch):
    monkeypatch.setenv("SLURM_MONITOR_OAUTH_URL", "https://public.invalid")
    monkeypatch.setenv("SLURM_MONITOR_OAUTH_REALM", "test-realm")

    settings = AppSettings()
    assert settings.oauth.jwks_url == "https://public.invalid/realms/test-realm/protocol/openid-connect/certs"

    monkeypatch.setenv("SLURM_MONITOR_OAUTH_JWKS", "http://keycloak:8080/certs")
    settings = AppSettings()
    assert settings.oauth.issuer == "https://public.invalid/realms/test-realm"
    assert settings.oauth.jwks_url == "http://keycloak:8080/certs"
