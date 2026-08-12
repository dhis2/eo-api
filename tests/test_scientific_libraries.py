"""Smoke tests for the scientific libraries process authors are told they can import.

`docs/extensibility.md` promises these are available to any `@process` function, including
in a downstream instance that installs this package as a dependency. That promise only
holds while each one is *declared* in the `[server]` extra — a component satisfied only
transitively disappears without warning when the chain shifts.

This matters more than a normal dependency check because plugin modules are imported
**lazily, at ingest or process-execution time**: a missing library leaves startup clean and
`/collections` fully populated, then fails mid-run. These tests move that failure to CI.
"""

from __future__ import annotations

import importlib

import pytest

# Kept in step with the `[server]` extra in pyproject.toml and the table in
# docs/extensibility.md. Adding a library to either without adding it here (or vice versa)
# should be a deliberate act, not an oversight.
_PROMISED_MODULES = [
    "earthkit.data",
    "earthkit.meteo",
    "earthkit.meteo.thermo",
    "earthkit.transforms",
    "earthkit.transforms.climatology",
    "earthkit.transforms.temporal",
    "earthkit.utils",
    "metpy",
    "numpy",
    "pandas",
    "rioxarray",
    "xarray",
    "xclim",
]


@pytest.mark.parametrize("module_name", _PROMISED_MODULES)
def test_promised_library_is_importable(module_name: str) -> None:
    assert importlib.import_module(module_name) is not None


def test_earthkit_transforms_climatology_entry_points_exist() -> None:
    """The functions our normals/anomaly processes call, pinned against a major upgrade.

    earthkit-transforms 1.0 moved `climatology` to a top-level module and left
    `aggregate.climatology` as a deprecation shim; we import the new path.
    """
    from earthkit.transforms import climatology

    for name in ("daily_mean", "monthly_mean", "daily_std", "monthly_std", "anomaly"):
        assert callable(getattr(climatology, name)), name


def test_earthkit_transforms_exposes_deaccumulation() -> None:
    """Used in place of hand-rolled ERA5 deaccumulation — see CLIM-682."""
    from earthkit.transforms import temporal

    assert callable(temporal.deaccumulate)
    assert callable(temporal.accumulation_to_rate)
