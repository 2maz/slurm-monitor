from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer
from fastapi_pagination import Page

from enum import Enum

from pydantic import Field
from pydantic_settings import BaseSettings

from logging import getLogger, Logger
import jwt
from typing import Annotated, Generic, Sequence, TypeVar

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

class Role(str, Enum):
    ADMIN = "admin"

class Roles(BaseSettings):
    roles: list[str] = Field(default=[])

    def has_role(self, role: str) -> bool:
        return role in self.roles

class Account(BaseSettings):
    account: Roles

    def has_role(self, role: str) -> bool:
        return self.account.has_role(role)

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

    auth_time: int | None = Field(default=None)
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


class RequiredPermissions:
    required_roles: list[str]

    def __init__(self, roles: list[str]) -> None:
        app_settings = AppSettings.initialize()

        if not app_settings.oauth.required:
            self.required_roles = []
        else:
            self.required_roles = roles

    def __call__(self, token_payload: Annotated[TokenPayload, Depends(get_token_payload)]) -> None:
        if token_payload:
            logger.info(f"Required roles: {self.required_roles} - available roles: {token_payload.resource_access.account.roles}")

        for role in self.required_roles:
            if not token_payload.resource_access.has_role(role):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Permission/role(s) '{','.join(self.required_roles)}' required"
                )
        return token_payload

class NoneForUserWithResourceRoles:
    optional_roles: list[Role]

    def __init__(self, roles: list[Role]) -> None:
        app_settings = AppSettings.initialize()

        if not app_settings.oauth.required:
            self.optional_roles = []
        else:
            self.optional_roles = roles

    def __call__(self, token_payload: Annotated[TokenPayload, Depends(get_token_payload)]) -> None:
        if token_payload:
            logger.info(f"Optional roles: {self.optional_roles} - available roles: {token_payload.resource_access.account.roles}")

        for role in self.optional_roles:
            if not token_payload.resource_access.has_role(role.value):
                return token_payload.preferred_username

        return None

class NoneForUserWithRealmRoles:
    optional_roles: list[Role]

    def __init__(self, roles: list[Role]) -> None:
        app_settings = AppSettings.initialize()

        if not app_settings.oauth.required:
            self.optional_roles = []
        else:
            self.optional_roles = roles

    def __call__(self, token_payload: Annotated[TokenPayload, Depends(get_token_payload)]) -> None:
        if token_payload:
            logger.info(f"Optional roles: {self.optional_roles} - available roles: {token_payload.realm_access.roles}")

        for role in self.optional_roles:
            if not token_payload.realm_access.has_role(role.value):
                return token_payload.preferred_username

        return None



T = TypeVar("T")
def create_custom_page(items_alias: str):
    class CustomPage(Page[T], Generic[T]):
        items: Sequence[T] = Field(alias=items_alias)
        model_config = { 'populate_by_name': True }

    return CustomPage
