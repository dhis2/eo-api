import json
import os
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


FEDERATED_PREFIX = "ext"
SUPPORTED_PROXY_OPERATIONS = {"coverage", "position", "area"}


@dataclass(frozen=True)
class ExternalOGCProvider:
    id: str
    url: str
    title: str | None = None
    headers: dict[str, str] | None = None
    api_key_env: str | None = None
    auth_scheme: str = "Bearer"
    timeout_seconds: float = 20.0
    retries: int = 0
    operations: tuple[str, ...] | None = None


def federated_collection_id(provider_id: str, collection_id: str) -> str:
    return f"{FEDERATED_PREFIX}:{provider_id}:{collection_id}"


def parse_federated_collection_id(collection_id: str) -> tuple[str, str] | None:
    parts = collection_id.split(":", 2)
    if len(parts) != 3 or parts[0] != FEDERATED_PREFIX:
        return None
    return (parts[1], parts[2])


def _provider_headers(provider: ExternalOGCProvider | None) -> dict[str, str]:
    headers = {"Accept": "application/json"}
    if provider is None:
        return headers

    for key, value in (provider.headers or {}).items():
        if key and value:
            headers[str(key)] = str(value)

    if provider.api_key_env:
        api_key = os.getenv(provider.api_key_env, "").strip()
        if api_key:
            scheme = provider.auth_scheme.strip() if provider.auth_scheme else "Bearer"
            if scheme.lower() == "none":
                headers["Authorization"] = api_key
            else:
                headers["Authorization"] = f"{scheme} {api_key}"

    return headers


def _fetch_json(url: str, provider: ExternalOGCProvider | None = None) -> dict[str, Any]:
    request = Request(url=url, method="GET")
    for key, value in _provider_headers(provider).items():
        request.add_header(key, value)

    timeout = provider.timeout_seconds if provider is not None else 20.0
    retries = provider.retries if provider is not None else 0
    attempts = max(1, retries + 1)

    for attempt in range(attempts):
        try:
            with urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, json.JSONDecodeError):
            if attempt + 1 >= attempts:
                return {}

    return {}


def load_external_providers() -> list[ExternalOGCProvider]:
    raw = os.getenv("EOAPI_EXTERNAL_OGC_SERVICES", "").strip()
    if not raw:
        return []

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return []

    if not isinstance(payload, list):
        return []

    providers: list[ExternalOGCProvider] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        provider_id = str(item.get("id", "")).strip()
        url = str(item.get("url", "")).strip().rstrip("/")
        if not provider_id or not url:
            continue
        title = str(item.get("title", "")).strip() or None
        raw_headers = item.get("headers")
        headers = None
        if isinstance(raw_headers, dict):
            headers = {str(key): str(value) for key, value in raw_headers.items() if key and value is not None}

        api_key_env = str(item.get("apiKeyEnv", "")).strip() or None
        auth_scheme = str(item.get("authScheme", "Bearer")).strip() or "Bearer"

        timeout_seconds = item.get("timeoutSeconds", 20)
        try:
            timeout_seconds = float(timeout_seconds)
        except (TypeError, ValueError):
            timeout_seconds = 20.0
        if timeout_seconds <= 0:
            timeout_seconds = 20.0

        retries = item.get("retries", 0)
        try:
            retries = int(retries)
        except (TypeError, ValueError):
            retries = 0
        if retries < 0:
            retries = 0

        operations = None
        raw_operations = item.get("operations")
        if isinstance(raw_operations, list):
            normalized = sorted({str(value).strip().lower() for value in raw_operations if str(value).strip()})
            if any(value not in SUPPORTED_PROXY_OPERATIONS for value in normalized):
                continue
            operations = tuple(normalized)

        providers.append(
            ExternalOGCProvider(
                id=provider_id,
                url=url,
                title=title,
                headers=headers,
                api_key_env=api_key_env,
                auth_scheme=auth_scheme,
                timeout_seconds=timeout_seconds,
                retries=retries,
                operations=operations,
            )
        )

    return providers


def get_external_provider(provider_id: str) -> ExternalOGCProvider | None:
    return next((item for item in load_external_providers() if item.id == provider_id), None)


def get_external_provider_for_collection_id(collection_id: str) -> tuple[ExternalOGCProvider, str] | None:
    parsed = parse_federated_collection_id(collection_id)
    if parsed is None:
        return None

    provider_id, source_collection_id = parsed
    provider = get_external_provider(provider_id)
    if provider is None:
        return None

    return (provider, source_collection_id)


def is_external_operation_enabled(collection_id: str, operation: str) -> bool | None:
    resolved = get_external_provider_for_collection_id(collection_id)
    if resolved is None:
        return None

    provider, _ = resolved
    if provider.operations is None:
        return True

    return operation in provider.operations


def list_external_collections() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for provider in load_external_providers():
        payload = _fetch_json(f"{provider.url}/collections", provider=provider)
        collections = payload.get("collections", []) if isinstance(payload, dict) else []
        if not isinstance(collections, list):
            continue

        for collection in collections:
            if not isinstance(collection, dict):
                continue
            raw_id = str(collection.get("id", "")).strip()
            if not raw_id:
                continue
            items.append(
                {
                    **collection,
                    "id": federated_collection_id(provider.id, raw_id),
                    "federation": {
                        "providerId": provider.id,
                        "providerTitle": provider.title,
                        "providerUrl": provider.url,
                        "sourceCollectionId": raw_id,
                    },
                }
            )

    return items


def get_external_collection(collection_id: str) -> dict[str, Any] | None:
    resolved = get_external_provider_for_collection_id(collection_id)
    if resolved is None:
        return None

    provider, source_collection_id = resolved

    encoded_id = quote(source_collection_id, safe="")
    payload = _fetch_json(f"{provider.url}/collections/{encoded_id}", provider=provider)
    if not isinstance(payload, dict) or not payload.get("id"):
        return None

    return {
        **payload,
        "id": federated_collection_id(provider.id, source_collection_id),
        "federation": {
            "providerId": provider.id,
            "providerTitle": provider.title,
            "providerUrl": provider.url,
            "sourceCollectionId": source_collection_id,
        },
    }


def proxy_external_collection_request(
    collection_id: str,
    operation: str,
    query_params: list[tuple[str, str]],
) -> dict[str, Any] | None:
    resolved = get_external_provider_for_collection_id(collection_id)
    if resolved is None:
        return None

    provider, source_collection_id = resolved

    encoded_id = quote(source_collection_id, safe="")
    filtered_query = [(key, value) for key, value in query_params if key]
    query_string = urlencode(filtered_query, doseq=True)
    url = f"{provider.url}/collections/{encoded_id}/{operation}"
    if query_string:
        url = f"{url}?{query_string}"

    payload = _fetch_json(url, provider=provider)
    if not isinstance(payload, dict) or not payload:
        return None

    return {
        **payload,
        "federation": {
            "providerId": provider.id,
            "providerTitle": provider.title,
            "providerUrl": provider.url,
            "sourceCollectionId": source_collection_id,
            "operation": operation,
        },
    }
