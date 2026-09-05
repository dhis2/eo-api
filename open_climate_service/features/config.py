"""Feature templates: what an instance's named geometry sets are (CLIM-926).

A template declares one feature set — its metadata and, where it is maintained by a provider, how
to fetch it. Templates are `features/*.yaml`, discovered exactly like `datasets/*.yaml`:

    plugins/features/
      districts.yaml          # this template
      dhis2_hierarchy.py      # the @feature_provider it names

The pairing mirrors `plugins/datasets/`, which holds `chirps3.yaml` beside `chirps3.py`, and the
same three-tier precedence applies: built-in, then installed plugin package, then `plugins_dir`
overriding both.

Metadata lives here rather than in a runtime sidecar because it is *authored*, not observed. A
licence, an attribution string and a description are facts about the source that someone writes
down once; the version and fetch time are facts about the last refresh. Keeping them apart is what
lets a set be described in the catalogue without having been fetched yet.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from open_climate_service import config as api_config

logger = logging.getLogger(__name__)

DEFAULT_TTL_SECONDS = 24 * 60 * 60
"""How long a provider-maintained set is reused before the provider is asked again.

A day, because org-unit hierarchies change on the order of months and the cost of being one day
stale is far below the cost of refetching on every scheduled run — or of the pipeline failing
whenever the upstream server is briefly unreachable.
"""


class FeatureTemplate(BaseModel):
    """One named feature set this instance offers."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1)
    name: str | None = None
    description: str | None = None

    # Provenance and terms of use. `license` is an SPDX id or a URI, the same shape CLIM-888
    # introduces for dataset templates — features must not invent a second vocabulary for the
    # question, since both end up in the same catalogue.
    license: str | None = None
    attribution: str | None = None
    source: str | None = None
    source_url: str | None = None
    keywords: list[str] = Field(default_factory=list)

    # How the set is maintained. Omit `provider` for a collection an admin places in the store by
    # hand: the template then carries only metadata, and nothing refreshes the file.
    provider: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    ttl_seconds: int | None = Field(default=None, ge=0)
    id_property: str | None = None
    """Column or property whose value becomes each feature's id.

    The id becomes the geometry-dimension label an export writes against, so for a DHIS2-destined
    set this must name the org-unit UID column. Providers that already return correct ids — the
    DHIS2 one does — leave it unset.
    """

    @property
    def effective_ttl(self) -> int:
        return DEFAULT_TTL_SECONDS if self.ttl_seconds is None else self.ttl_seconds

    @property
    def is_provider_backed(self) -> bool:
        return self.provider is not None

    def metadata(self) -> dict[str, Any]:
        """The authored fields, for the catalogue. Empty ones are omitted rather than reported null."""
        fields = ("name", "description", "license", "attribution", "source", "source_url")
        described = {key: getattr(self, key) for key in fields if getattr(self, key) is not None}
        if self.keywords:
            described["keywords"] = self.keywords
        return described


class FeatureTemplates(BaseModel):
    """Every feature template this instance can see."""

    model_config = ConfigDict(extra="forbid")

    templates: list[FeatureTemplate] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_unique_ids(self) -> "FeatureTemplates":
        ids = [template.id for template in self.templates]
        duplicates = sorted({value for value in ids if ids.count(value) > 1})
        if duplicates:
            raise ValueError(f"feature template ids must be unique: {duplicates}")
        return self

    def get(self, feature_id: str) -> FeatureTemplate:
        """The template for ``feature_id``, or a ValueError naming what is declared."""
        for template in self.templates:
            if template.id == feature_id:
                return template
        available = ", ".join(sorted(t.id for t in self.templates)) or "none"
        raise ValueError(f"Unknown feature id {feature_id!r}. Declared: {available}")

    def find(self, feature_id: str) -> FeatureTemplate | None:
        """The template for ``feature_id`` if there is one — a collection may have none."""
        return next((t for t in self.templates if t.id == feature_id), None)


def _parse(raw: Any, source: str) -> list[dict[str, Any]]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValueError(f"{source} must contain a list of feature templates")
    return raw


def _from_builtin() -> list[dict[str, Any]]:
    import importlib.resources

    pkg = importlib.resources.files("open_climate_service") / "plugins" / "features"
    found: list[dict[str, Any]] = []
    try:
        for resource in pkg.iterdir():
            if not resource.name.endswith((".yaml", ".yml")):
                continue
            found.extend(_parse(yaml.safe_load(resource.read_text(encoding="utf-8")), resource.name))
    except (FileNotFoundError, NotADirectoryError):
        pass
    return found


def _from_plugin_packages() -> list[dict[str, Any]]:
    from open_climate_service.plugin_discovery import iter_plugin_subdirs

    found: list[dict[str, Any]] = []
    for plugin_name, _package, features_res in iter_plugin_subdirs("features"):
        try:
            for resource in features_res.iterdir():
                if not resource.name.endswith((".yaml", ".yml")):
                    continue
                found.extend(
                    _parse(yaml.safe_load(resource.read_text(encoding="utf-8")), f"{plugin_name} ({resource.name})")
                )
        except Exception:
            logger.exception("Error loading feature templates from plugin '%s'", plugin_name)
            raise
    return found


def _from_instance() -> list[dict[str, Any]]:
    config = api_config.get_config()
    plugins_dir_raw = config.get("plugins_dir") if config else None
    if not plugins_dir_raw:
        return []
    config_path = api_config.get_config_path()
    base = config_path.parent if config_path else Path()
    features_dir = (base / plugins_dir_raw).resolve() / "features"
    if not features_dir.is_dir():
        return []
    found: list[dict[str, Any]] = []
    for path in sorted(features_dir.glob("*.y*ml")):
        found.extend(_parse(yaml.safe_load(path.read_text(encoding="utf-8")), str(path)))
    return found


def get_feature_templates() -> FeatureTemplates:
    """Load every feature template, later tiers overriding earlier ones by id.

    Precedence matches the other plugin folders — built-in, then installed plugin package, then
    ``plugins_dir`` — so an instance can replace a template a plugin ships without forking it.
    """
    merged: dict[str, dict[str, Any]] = {}
    for entry in [*_from_builtin(), *_from_plugin_packages(), *_from_instance()]:
        if not isinstance(entry, dict) or "id" not in entry:
            raise ValueError(f"Feature template must be a mapping with an 'id': {entry!r}")
        if entry["id"] in merged:
            logger.info("Feature template '%s' overridden by a higher-precedence definition", entry["id"])
        merged[entry["id"]] = entry
    return FeatureTemplates.model_validate({"templates": list(merged.values())})
