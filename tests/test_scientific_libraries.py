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
import re
import tomllib
from pathlib import Path

import pytest

_PYPROJECT = Path(__file__).resolve().parent.parent / "pyproject.toml"


def _requirement_name(requirement: str) -> str:
    """Return the PEP 503-normalised distribution name from a PEP 508 requirement string.

    Only the leading name is read, so every version operator (`>=`, `<`, `~=`, `!=`, `==`),
    extras, and environment markers are ignored rather than having to be enumerated.
    """
    match = re.match(r"\s*([A-Za-z0-9][A-Za-z0-9._-]*)", requirement)
    return re.sub(r"[-_.]+", "-", match.group(1)).lower() if match else ""


# Kept in step with the `[server]` extra in pyproject.toml and the table in
# docs/extensibility.md. Adding a library to either without adding it here (or vice versa)
# should be a deliberate act, not an oversight.
_PROMISED_MODULES = [
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


def test_earthkit_data_is_an_extra_and_not_part_of_the_server_install() -> None:
    """`earthkit-data` is the only dependency here that cannot be installed on Windows.

    It requires `eccodeslib` and `eckit`, both of which require `eckitlib`, and none of the
    three has ever published a Windows wheel (ecmwf/earthkit-data#1105). Every other package
    in the resolved tree is pure-Python or ships `win_amd64`, so moving this one dependency
    into the `grib` extra is what makes `uv sync --extra server` resolve on Windows.

    Nothing in this package imports `earthkit.data`. Adding it back to `[server]` — or to the
    base dependencies — would re-break Windows silently, on a platform CI does not build, so
    this test asserts the placement rather than trusting the comment in pyproject.toml.
    """
    project = tomllib.loads(_PYPROJECT.read_text(encoding="utf-8"))["project"]
    extras = project["optional-dependencies"]

    def declares_earthkit_data(requirements: list[str]) -> list[str]:
        return [r for r in requirements if _requirement_name(r) == "earthkit-data"]

    assert declares_earthkit_data(extras["grib"]), "the `grib` extra must declare earthkit-data"
    for name, requirements in (("dependencies", project["dependencies"]), *extras.items()):
        if name == "grib":
            continue
        assert not declares_earthkit_data(requirements), (
            f"earthkit-data is declared in `{name}`, which makes a Windows install unresolvable "
            "(ecmwf/earthkit-data#1105). It belongs only in the `grib` extra."
        )


@pytest.mark.parametrize(
    "requirement",
    [
        "earthkit-data",
        "earthkit-data>=1.1",
        "earthkit-data==1.1.0",
        "earthkit-data<2",
        "earthkit-data~=1.1",
        "earthkit-data!=1.0",
        "earthkit-data[all]>=1.1",
        "earthkit-data >= 1.1",
        # The plausible wrong fix for Windows: a marker rather than a separate extra. It would
        # still put the dependency in the default install, so the guard above must see it.
        'earthkit-data; sys_platform != "win32"',
        # PEP 503 treats these as the same distribution, so the guard must too.
        "Earthkit_Data>=1.1",
        "EARTHKIT.DATA",
    ],
)
def test_the_placement_guard_recognises_every_spelling_of_the_dependency(requirement: str) -> None:
    """The guard above is only as good as its name extraction.

    Matching on a hand-stripped prefix missed `<`, `~=`, `!=`, markers and non-normalised
    names, which would have let earthkit-data back into `[server]` unnoticed — so the
    extraction reads the leading PEP 508 name and normalises it per PEP 503 instead.
    """
    assert _requirement_name(requirement) == "earthkit-data"
