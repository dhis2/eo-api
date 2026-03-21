"""Routes exposing generated pygeoapi documents from publication truth."""

from fastapi import APIRouter

from .pygeoapi import build_pygeoapi_config, build_pygeoapi_openapi, write_generated_pygeoapi_documents

router = APIRouter()


@router.get("/pygeoapi/config")
def get_generated_pygeoapi_config() -> dict[str, object]:
    """Return generated pygeoapi config from backend publication truth."""
    return build_pygeoapi_config()


@router.get("/pygeoapi/openapi")
def get_generated_pygeoapi_openapi() -> dict[str, object]:
    """Return generated pygeoapi OpenAPI projection from backend publication truth."""
    return build_pygeoapi_openapi()


@router.post("/pygeoapi/materialize")
def materialize_generated_pygeoapi_documents() -> dict[str, str]:
    """Write generated pygeoapi documents to disk for runtime wiring."""
    config_path, openapi_path = write_generated_pygeoapi_documents()
    return {
        "config_path": str(config_path),
        "openapi_path": str(openapi_path),
    }
