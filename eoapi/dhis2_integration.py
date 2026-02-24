import json
import os
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def _base_url() -> str | None:
    raw = os.getenv("EOAPI_DHIS2_BASE_URL", "").strip().rstrip("/")
    return raw or None


def _auth_token() -> str | None:
    raw = os.getenv("EOAPI_DHIS2_TOKEN", "").strip()
    return raw or None


def _basic_credentials() -> tuple[str, str] | None:
    username = os.getenv("EOAPI_DHIS2_USERNAME", "").strip()
    password = os.getenv("EOAPI_DHIS2_PASSWORD", "").strip()
    if username and password:
        return (username, password)
    return None


def dhis2_configured() -> bool:
    return _base_url() is not None and (_auth_token() is not None or _basic_credentials() is not None)


def _timeout() -> float:
    raw = os.getenv("EOAPI_DHIS2_TIMEOUT_SECONDS", "20").strip()
    try:
        timeout = float(raw)
    except ValueError:
        timeout = 20.0
    return timeout if timeout > 0 else 20.0


def _auth_headers() -> dict[str, str]:
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    token = _auth_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
        return headers

    credentials = _basic_credentials()
    if credentials is None:
        return headers

    import base64

    encoded = base64.b64encode(f"{credentials[0]}:{credentials[1]}".encode("utf-8")).decode("ascii")
    headers["Authorization"] = f"Basic {encoded}"
    return headers


def _request_json(method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any] | None:
    base = _base_url()
    if base is None:
        return None

    request = Request(
        url=f"{base}{path}",
        method=method,
        data=json.dumps(payload).encode("utf-8") if payload is not None else None,
    )
    for key, value in _auth_headers().items():
        request.add_header(key, value)

    try:
        with urlopen(request, timeout=_timeout()) as response:
            body = response.read().decode("utf-8")
            if not body:
                return {}
            parsed = json.loads(body)
            return parsed if isinstance(parsed, dict) else {}
    except (HTTPError, URLError, json.JSONDecodeError):
        return None


def _extract_import_counts(payload: dict[str, Any] | None, dry_run: bool, fallback_count: int) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {
            "imported": fallback_count,
            "updated": 0,
            "ignored": 0,
            "deleted": 0,
            "dryRun": dry_run,
            "source": "simulated",
        }

    counts = payload.get("response", {}).get("importCount")
    if not isinstance(counts, dict):
        counts = payload.get("importCount")

    if not isinstance(counts, dict):
        return {
            "imported": fallback_count,
            "updated": 0,
            "ignored": 0,
            "deleted": 0,
            "dryRun": dry_run,
            "source": "simulated",
        }

    return {
        "imported": int(counts.get("imported", 0)),
        "updated": int(counts.get("updated", 0)),
        "ignored": int(counts.get("ignored", 0)),
        "deleted": int(counts.get("deleted", 0)),
        "dryRun": dry_run,
        "source": "dhis2",
    }


def import_data_values_to_dhis2(data_values: list[dict[str, Any]], *, dry_run: bool) -> dict[str, Any]:
    if dry_run:
        return {
            "imported": 0,
            "updated": 0,
            "ignored": len(data_values),
            "deleted": 0,
            "dryRun": True,
            "source": "dry-run",
        }

    if not data_values:
        return {
            "imported": 0,
            "updated": 0,
            "ignored": 0,
            "deleted": 0,
            "dryRun": False,
            "source": "dhis2",
        }

    if not dhis2_configured():
        return {
            "imported": len(data_values),
            "updated": 0,
            "ignored": 0,
            "deleted": 0,
            "dryRun": False,
            "source": "simulated",
        }

    request_payload = {
        "dataValues": data_values,
    }
    response_payload = _request_json(
        "POST",
        f"/api/dataValueSets?importStrategy=CREATE_AND_UPDATE&dryRun={'true' if dry_run else 'false'}&preheatCache=true&skipAudit=true&async=false",
        payload=request_payload,
    )
    return _extract_import_counts(response_payload, dry_run=dry_run, fallback_count=len(data_values))


def _to_feature(ou_id: str, name: str, level: int, geometry: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(geometry, dict) or "type" not in geometry or "coordinates" not in geometry:
        return None

    return {
        "type": "Feature",
        "id": ou_id,
        "geometry": geometry,
        "properties": {
            "name": name,
            "level": level,
        },
    }


def fetch_org_units_from_dhis2(level: int) -> list[dict[str, Any]]:
    if not dhis2_configured():
        return []

    params = urlencode(
        {
            "fields": "id,name,level,geometry",
            "filter": f"level:eq:{level}",
            "paging": "false",
        }
    )
    payload = _request_json("GET", f"/api/organisationUnits?{params}")
    if not isinstance(payload, dict):
        return []

    units = payload.get("organisationUnits", [])
    if not isinstance(units, list):
        return []

    features: list[dict[str, Any]] = []
    for item in units:
        if not isinstance(item, dict):
            continue
        ou_id = str(item.get("id", "")).strip()
        name = str(item.get("name", "")).strip() or ou_id
        if not ou_id:
            continue

        geometry = item.get("geometry")
        feature = _to_feature(ou_id, name, level, geometry)
        if feature is not None:
            features.append(feature)

    return features


def iso_to_dhis2_period(datetime_value: str) -> str:
    normalized = datetime_value.strip()
    try:
        if normalized.endswith("Z"):
            parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
        else:
            parsed = datetime.fromisoformat(normalized)
        return parsed.astimezone(UTC).strftime("%Y%m%d")
    except ValueError:
        return normalized[:10].replace("-", "")
