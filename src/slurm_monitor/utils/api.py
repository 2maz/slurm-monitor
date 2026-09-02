import fastapi
from fastapi import FastAPI

from slurm_monitor.app_settings import AppSettings

# see https://fastapi.tiangolo.com/how-to/extending-openapi
def createFastAPI(**kwargs):
    title = "slurm-monitor"
    if "title" in kwargs:
        title = kwargs["title"]
    else:
        kwargs["title"] = title

    version="0.3.0"
    if "version" in kwargs:
        version = kwargs["version"]
    else:
        kwargs["version"] = version

    root_path = "/"
    if "root_path" in kwargs:
        root_path = kwargs["root_path"]

    app = FastAPI(
        **kwargs
    )

    app_settings = AppSettings.initialize(db_schema_version="v2")
    def custom_openapi():
        if app.openapi_schema:
            return app.openapi_schema

        openapi_schema = fastapi.openapi.utils.get_openapi(
            title=title,
            version=version,
            routes=app.routes,
        )

        openapi_schema["servers"] = [{ 'url': root_path}]

        if app_settings.oauth.required:
            app.openapi_schema = openapi_schema
            if "components" not in openapi_schema:
                openapi_schema["components"] = {}

            openapi_schema["components"]["securitySchemes"] = {
                "BearerAuth": {
                    # https://fastapi.tiangolo.com/reference/openapi/models/?h=securityschemetype#fastapi.openapi.models.SecuritySchemeType
                    "type": "http",
                    # https://fastapi.tiangolo.com/reference/openapi/models/?h=securityschemetype#fastapi.openapi.models.HTTPBearer
                    "scheme": "bearer",
                    "bearerFormat": "JWT",
                }
            }

            openapi_schema["security"] = [{"BearerAuth": []}]
        app.openapi_schema = openapi_schema
        return app.openapi_schema

    app.openapi = custom_openapi
    return app


def flatten_router_routes(routes):
    """Recursively resolve FastAPI's lazy ``_IncludedRouter`` wrappers (as
    produced by ``APIRouter.include_router``) into concrete route-like
    objects exposing ``.path``/``.name``/``.endpoint``.

    Newer FastAPI versions no longer flatten an included router's routes
    into the parent router's `.routes` list; instead each `include_router()`
    call appends a single internal `_IncludedRouter` wrapper that resolves
    its effective routes lazily via `.effective_candidates()`. This walks
    that structure so callers can keep iterating a flat list of routes. We
    duck-type on `effective_candidates` rather than importing the private
    `_IncludedRouter` class, since its exact type is a FastAPI internal.

    Args:
        routes: Routes as found on a FastAPI/APIRouter `.routes` attribute.

    Returns:
        A flat list of route-like objects with resolved `.path`, `.name`,
        and `.endpoint`.
    """
    flattened = []
    for route in routes:
        effective_candidates = getattr(route, "effective_candidates", None)
        if callable(effective_candidates):
            flattened.extend(flatten_router_routes(effective_candidates()))
        else:
            flattened.append(route)
    return flattened


def find_endpoint_by_name(app: FastAPI, name: str, prefix: str = "api/v2"):
    router = [x for x in app.routes if x.name == prefix][0]
    matching_routes = [
        x for x in flatten_router_routes(router.routes) if x.name == name
    ]
    if not matching_routes:
        raise KeyError(f"find_endpoint_by_name: could not find route {name}")

    return matching_routes[0].endpoint
