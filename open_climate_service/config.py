"""Instance configuration loaded from CLIMATE_SERVICE_CONFIG."""

import os
import re
from pathlib import Path
from typing import Any

import yaml

_MISSING = object()


def _substitute_env_vars(text: str) -> str:
    """Replace ${VAR:-default} patterns with values from the environment."""

    def _replace(match: re.Match[str]) -> str:
        var, _, default = match.group(1).partition(":-")
        return os.environ.get(var, default)

    return re.sub(r"\$\{([^}]+)\}", _replace, text)


def get_config_path() -> Path | None:
    """Return the resolved Path of CLIMATE_SERVICE_CONFIG, or None if unset."""
    raw = os.environ.get("CLIMATE_SERVICE_CONFIG")
    return Path(raw).resolve() if raw else None


def get_config() -> dict[str, Any]:
    """Load and return the instance config from CLIMATE_SERVICE_CONFIG.

    Results are cached for the lifetime of the process; the config file is
    read once and reused on subsequent calls. Returns an empty dict if
    CLIMATE_SERVICE_CONFIG is not set. Raises FileNotFoundError if the path is
    set but does not exist.
    """
    return _load_config()


# Module-level cache — reset between tests via monkeypatch on _cache.
_cache: dict[str, Any] | None = None


def _load_config() -> dict[str, Any]:
    global _cache
    if _cache is not None:
        return _cache
    path = get_config_path()
    if path is None:
        return {}
    if not path.exists():
        raise FileNotFoundError(f"CLIMATE_SERVICE_CONFIG not found: {path}")
    text = _substitute_env_vars(path.read_text(encoding="utf-8"))
    loaded = yaml.safe_load(text)
    if loaded is not None and not isinstance(loaded, dict):
        raise ValueError(f"CLIMATE_SERVICE_CONFIG must be a YAML mapping at the top level: {path}")
    _cache = dict(loaded or {})
    return _cache


DEFAULT_CRS = "EPSG:4326"
DEFAULT_NAME = "Open Climate Service"
DEFAULT_ID = "open-climate-service"  # operators should always set id: in climate-service.yaml


def get_id() -> str:
    """Return the instance identifier from CLIMATE_SERVICE_CONFIG.

    Set `id: sierra-leone-climate-service` in climate-service.yaml to give this
    instance a unique id used as the STAC catalog id. Should be lowercase,
    hyphen-separated, and unique across all deployed instances (e.g.
    nepal-climate-service, kenya-climate-service). Defaults to
    'open-climate-service'.
    """
    raw = get_config().get("id")
    if raw is None:
        return DEFAULT_ID
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"id in CLIMATE_SERVICE_CONFIG must be a non-empty string, got {type(raw).__name__}")
    return raw.strip()


def get_name() -> str:
    """Return the instance display name from CLIMATE_SERVICE_CONFIG.

    Set `name: My Climate Service` in climate-service.yaml to customise the title
    shown in the web UI. Defaults to 'Open Climate Service' when unset.
    """
    raw = get_config().get("name")
    if raw is None:
        return DEFAULT_NAME
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"name in CLIMATE_SERVICE_CONFIG must be a non-empty string, got {type(raw).__name__}")
    return raw.strip()


def get_crs() -> str:
    """Return the instance CRS from CLIMATE_SERVICE_CONFIG, defaulting to EPSG:4326.

    Set `crs: EPSG:25833` in climate-service.yaml to store all GeoZarr files in a
    national projection. All datasets within one instance share the same CRS.
    """
    from pyproj import CRS
    from pyproj.exceptions import CRSError

    raw = get_config().get("crs")
    if raw is None:
        return DEFAULT_CRS
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"crs in CLIMATE_SERVICE_CONFIG must be a non-empty string, got {type(raw).__name__}")
    crs = raw.strip()
    try:
        CRS.from_user_input(crs)
    except CRSError as exc:
        raise ValueError(f"crs '{crs}' in CLIMATE_SERVICE_CONFIG is not a valid CRS: {exc}") from exc
    return crs


def get_data_dir() -> Path | None:
    """Return the data directory declared in CLIMATE_SERVICE_CONFIG.

    Returns None when CLIMATE_SERVICE_CONFIG is unset or points to a file that does
    not exist (e.g. CI environments where the config is gitignored).

    Raises ValueError if the config file exists but data_dir is not set, so
    misconfigured instances fail fast at startup rather than silently sharing
    a default directory with other instances.

    """
    config_path = get_config_path()
    if config_path is None or not config_path.exists():
        return None

    config = get_config()
    raw = config.get("data_dir", _MISSING)
    if raw is _MISSING:
        raise ValueError(
            "data_dir is required in CLIMATE_SERVICE_CONFIG when a config file is present. "
            "Set it to the directory where downloaded data should be stored, "
            "e.g. data_dir: ./data"
        )
    if not isinstance(raw, (str, Path)):
        raise ValueError(f"data_dir in CLIMATE_SERVICE_CONFIG must be a path string, got {type(raw).__name__}")
    return (config_path.parent / raw).resolve()
