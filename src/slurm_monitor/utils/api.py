from fastapi import FastAPI

def find_endpoint_by_name(app: FastAPI, name: str, prefix: str = "api/v2"):
    router = [x for x in app.routes if x.name == prefix][0]
    matching_routes = [x for x in router.routes if x.name == name]
    if not matching_routes:
        raise KeyError(f"find_endpoint_by_name: could not find route {name}")

    return matching_routes[0].endpoint
