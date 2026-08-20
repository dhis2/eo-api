"""Portable on-disk representation for artifact store paths.

Artifact records are persisted to ``{data_dir}/artifacts/records.json``, which lives
inside the very directory the paths point into. Storing absolute paths there pins the
data directory to the machine and mount point that produced it: a directory ingested in
a container at ``/app/data`` is unreadable from the host, from a restored backup, or
from any deployment that mounts it elsewhere.

Records are therefore written relative to the data root and resolved back to absolute
when loaded, so everything downstream of :func:`decode_record_paths` keeps seeing the
absolute paths it always did.
"""

import re
from pathlib import Path, PurePosixPath
from typing import Any

from open_climate_service import config as api_config

_URI_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*://")

_PATH_FIELD = "path"
_PATH_LIST_FIELD = "asset_paths"


def _is_windows_absolute(raw: str) -> bool:
    """Return whether raw is a drive-letter path, which urlparse misreads as a URI scheme."""
    return len(raw) >= 3 and raw[0].isalpha() and raw[1] == ":" and raw[2] in ("\\", "/")


def _is_remote(raw: str) -> bool:
    """Return whether raw addresses a remote store rather than a local filesystem path."""
    if _is_windows_absolute(raw):
        return False
    return bool(_URI_SCHEME.match(raw)) and not raw.startswith("file://")


def to_portable(raw: str) -> str:
    """Return raw relative to the data root when it points inside it.

    Anything else - a remote URI, an already-relative path, or a store deliberately
    kept outside the data directory - is returned untouched.
    """
    if not raw or _is_remote(raw):
        return raw
    candidate = Path(raw)
    if not candidate.is_absolute():
        return raw
    root = api_config.get_data_root()
    try:
        relative = candidate.resolve(strict=False).relative_to(root.resolve(strict=False))
    except ValueError:
        return raw
    return relative.as_posix()


def to_absolute(raw: str) -> str:
    """Return raw as an absolute path against the current data root.

    Relative paths are the portable form and are simply rejoined. Absolute paths are
    legacy records written before this module existed; they are re-rooted onto the
    current data root when the original no longer resolves but a re-rooted suffix does.
    """
    if not raw or _is_remote(raw):
        return raw
    root = api_config.get_data_root()
    candidate = Path(raw)
    if not candidate.is_absolute():
        return str((root / PurePosixPath(raw)).resolve(strict=False))
    return str(_rebase_legacy(candidate, root))


def _rebase_legacy(candidate: Path, root: Path) -> Path:
    """Re-root a legacy absolute path onto root, preferring the longest matching suffix.

    Only a suffix that actually exists is accepted, so a store intentionally held
    outside the data directory keeps its recorded path rather than being silently
    redirected at a lookalike inside it.
    """
    resolved_root = root.resolve(strict=False)
    try:
        candidate.resolve(strict=False).relative_to(resolved_root)
    except ValueError:
        pass
    else:
        return candidate
    if candidate.exists():
        return candidate
    parts = candidate.parts
    for index in range(1, len(parts)):
        rebased = resolved_root.joinpath(*parts[index:])
        if rebased.exists():
            return rebased
    return candidate


def _map_record_paths(item: dict[str, Any], convert: Any) -> dict[str, Any]:
    """Apply convert to every path-bearing field of a serialized artifact record."""
    value = item.get(_PATH_FIELD)
    if isinstance(value, str):
        item[_PATH_FIELD] = convert(value)
    assets = item.get(_PATH_LIST_FIELD)
    if isinstance(assets, list):
        item[_PATH_LIST_FIELD] = [convert(entry) if isinstance(entry, str) else entry for entry in assets]
    return item


def decode_record_paths(item: dict[str, Any]) -> dict[str, Any]:
    """Resolve a record's stored paths to absolute, in place."""
    return _map_record_paths(item, to_absolute)


def encode_record_paths(item: dict[str, Any]) -> dict[str, Any]:
    """Rewrite a record's paths into their portable form, in place."""
    return _map_record_paths(item, to_portable)
