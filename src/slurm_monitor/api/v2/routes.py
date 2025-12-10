from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.security import OAuth2PasswordBearer, HTTPBearer, HTTPAuthorizationCredentials
from fastapi_cache.decorator import cache
from pydantic import Field
from pydantic_settings import BaseSettings
import functools

from typing import Annotated

from logging import getLogger, Logger
import jwt

from slurm_monitor.app_settings import AppSettings
from slurm_monitor.utils import utcnow

logger: Logger = getLogger(__name__)

api_router = APIRouter(
#    prefix="",
    tags=["v2"]
)

oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl="token",
    scopes={
        "user.read": "Read information about the current user.",
        "jobs.all": "Read all items."
    }
)

class Roles(BaseSettings):
    roles: list[str] = Field(default=[])

class Account(BaseSettings):
    account: Roles

class TokenPayload(BaseSettings):
    exp: int # time in s
    iat: int
    jti: str
    iss: str # issuer
    aud: str # audience
    sub: str # subject
    typ: str # type: Bearer
    azp: str # client name
    sid: str # 
    acr: int

    allowed_origins: list[str] = Field(alias='allowed-origins', default=[])
    realm_access: Roles
    resource_access: Account
    scope: str
    email_verified: bool
    name: str
    preferred_username: str
    given_name: str
    family_name: str
    email: str


def validate_interval(end_time_in_s: float | None, start_time_in_s: float | None, resolution_in_s: int | None):
    if end_time_in_s is None:
        now = utcnow()
        end_time_in_s = now.timestamp()

    # Default 1h interval
    if start_time_in_s is None:
        start_time_in_s = end_time_in_s - 60 * 60.0

    if resolution_in_s is None:
        resolution_in_s = max(60, int((end_time_in_s - start_time_in_s) / 120))

    if end_time_in_s < start_time_in_s:
        raise HTTPException(
            status_code=500,
            detail=f"ValueError: {end_time_in_s=} cannot be smaller than {start_time_in_s=}",
        )

    if (end_time_in_s - start_time_in_s) > 3600*24*14:
        raise HTTPException(
            status_code=500,
            detail=f"""ValueError: query timeframe cannot exceed 14 days (job length), but was
                {(end_time_in_s - start_time_in_s) / (3600*24):.2f} days""",
        )

    if resolution_in_s <= 0:
        raise HTTPException(
            status_code=500,
            detail=f"ValueError: {resolution_in_s=} must be >= 1 and <= 24*60*60",
        )

    return start_time_in_s, end_time_in_s, resolution_in_s

# Dependency to validate JWT token
async def verify_token(token: str) -> TokenPayload:
    """
    Verify and decode JWT token
    """
    app_settings = AppSettings.get_instance()
    try:
        # Get the signing key from JWKS
        signing_key = app_settings.oauth.jwks_client.get_signing_key_from_jwt(token)

        # Decode and verify the token
        payload = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            audience="account",  # Default Keycloak audience
            issuer=app_settings.oauth.issuer,
            options={
                "verify_signature": True,
                "verify_exp": True,
                "verify_iat": True,
                "verify_aud": True,
                "verify_iss": True,
            }
        )
        logger.info(f"Token validated for user: {payload.get('preferred_username')}")
        return TokenPayload(**payload)

    except jwt.ExpiredSignatureError:
        logger.error("Token has expired")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.InvalidTokenError as e:
        logger.error(f"Invalid token: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Could not validate credentials: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except Exception as e:
        logger.error(f"Token verification failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

async def get_token_payload(request: Request) -> TokenPayload:
    app_settings = AppSettings.get_instance()

    if app_settings.oauth.required:
        oauth2: str = Depends(oauth2_scheme)
        token = await oauth2.dependency(request)

        logger.info(f"Retrieved token: {token}")
        return await verify_token(token)

    logger.info("get_token_payload: authentication disabled")
    return None
