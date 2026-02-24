import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any


STATE_DIR_ENV = "EOAPI_STATE_DIR"
STATE_PERSIST_ENV = "EOAPI_STATE_PERSIST"


def _state_enabled() -> bool:
    raw = os.getenv(STATE_PERSIST_ENV, "true").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _state_dir() -> Path:
    return Path(os.getenv(STATE_DIR_ENV, ".cache/state"))


def _state_file(name: str) -> Path:
    return _state_dir() / f"{name}.json"


def load_state_map(name: str) -> dict[str, dict[str, Any]]:
    if not _state_enabled():
        return {}

    path = _state_file(name)
    if not path.exists():
        return {}

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}

    if not isinstance(payload, dict):
        return {}

    return {str(key): value for key, value in payload.items() if isinstance(value, dict)}


def save_state_map(name: str, payload: dict[str, dict[str, Any]]) -> None:
    if not _state_enabled():
        return

    state_dir = _state_dir()
    try:
        state_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        return

    path = _state_file(name)

    try:
        with NamedTemporaryFile("w", delete=False, dir=state_dir, encoding="utf-8") as tmp:
            json.dump(payload, tmp, ensure_ascii=False)
            tmp_path = Path(tmp.name)
        tmp_path.replace(path)
    except OSError:
        return
