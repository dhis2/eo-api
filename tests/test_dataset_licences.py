"""Dataset licences: declaration, publication and propagation (CLIM-946).

The failure this guards against is silent. OCS published the constant `various` on every
collection, so a water mask derived from CC-BY-NC imagery was published as though it carried no
restriction — on the default path, with nothing to catch it.
"""

import glob
import pathlib
from typing import TYPE_CHECKING

import pytest
import yaml

from open_climate_service.shared.licences import (
    STAC_LICENSE_OTHER,
    UNDECLARED,
    most_restrictive,
    parse_licence,
    refuses_publication,
)

if TYPE_CHECKING:
    import xarray as xr

_BUILTIN_TEMPLATES = sorted(glob.glob("open_climate_service/plugins/datasets/*.yaml"))


# -- parsing ----------------------------------------------------------------------------


def test_spdx_identifier_is_recognised() -> None:
    licence = parse_licence("CC-BY-4.0")
    assert licence.identifier == "CC-BY-4.0"
    assert licence.commercial_use is True
    assert licence.stac_license == "CC-BY-4.0"


@pytest.mark.parametrize("identifier", ["CC-BY-NC-4.0", "CC-BY-NC-SA-4.0", "CC-BY-NC-ND-4.0", "cc-by-nc-3.0"])
def test_every_non_commercial_cc_licence_is_caught(identifier: str) -> None:
    """Matched on the CC `NC` element rather than an enumeration, so the whole family is
    covered — including a lower-cased identifier, which YAML makes easy to write."""
    assert parse_licence(identifier).commercial_use is False


def test_a_named_licence_without_spdx_is_accepted() -> None:
    """The Copernicus licence has no SPDX identifier. Forcing it into a near-miss one would be
    a false statement about what a user may do."""
    licence = parse_licence(
        {
            "name": "Licence to Use Copernicus Products",
            "url": "https://apps.ecmwf.int/datasets/licences/copernicus/",
            "commercial_use": True,
        }
    )
    assert licence.identifier is None
    assert licence.name == "Licence to Use Copernicus Products"
    assert licence.commercial_use is True
    # STAC 1.1 has a defined value for "not SPDX", and it is not `various`.
    assert licence.stac_license == STAC_LICENSE_OTHER


def test_an_unknown_identifier_is_unknown_not_permissive() -> None:
    """The safe answer for an identifier nobody has checked is "we do not know"."""
    assert parse_licence("SomeVendorLicence-2.0").commercial_use is None


@pytest.mark.parametrize("bad", [None, "", "   ", 42, [], {}, {"url": "https://x"}])
def test_unusable_declarations_degrade_to_undeclared(bad: object) -> None:
    """A malformed licence must not take down the catalogue; the validator warns separately."""
    assert parse_licence(bad) is UNDECLARED


def test_undeclared_publishes_as_other() -> None:
    assert UNDECLARED.stac_license == STAC_LICENSE_OTHER
    assert UNDECLARED.commercial_use is None


# -- comparison -------------------------------------------------------------------------


def test_non_commercial_beats_permissive() -> None:
    nc = parse_licence("CC-BY-NC-4.0")
    assert most_restrictive([parse_licence("CC0-1.0"), nc]) is nc


def test_unknown_beats_permissive() -> None:
    """Deriving from an unlabelled input yields unknown, not permissive. Assuming permissive is
    exactly the laundering this exists to prevent."""
    assert most_restrictive([parse_licence("CC0-1.0"), UNDECLARED]) is UNDECLARED


def test_non_commercial_beats_unknown() -> None:
    nc = parse_licence("CC-BY-NC-4.0")
    assert most_restrictive([UNDECLARED, nc]) is nc


def test_a_single_input_is_returned_unchanged() -> None:
    cc = parse_licence("CC-BY-4.0")
    assert most_restrictive([cc]) is cc


# -- the rule that matters ----------------------------------------------------------------


def test_a_derived_product_may_not_be_more_permissive_than_its_input() -> None:
    """The headline case: a water mask derived from CC-BY-NC imagery, published as CC0."""
    refusal = refuses_publication(parse_licence("CC0-1.0"), [parse_licence("CC-BY-NC-4.0")])
    assert refusal is not None
    assert "CC-BY-NC-4.0" in refusal
    assert "derivative work" in refusal


def test_a_derived_product_may_match_its_input() -> None:
    nc = parse_licence("CC-BY-NC-4.0")
    assert refuses_publication(nc, [nc]) is None


def test_a_derived_product_may_be_more_restrictive_than_its_input() -> None:
    assert refuses_publication(parse_licence("CC-BY-NC-4.0"), [parse_licence("CC0-1.0")]) is None


def test_claiming_permissive_from_an_unlabelled_input_is_refused() -> None:
    """Unknown outranks permissive, so this is refused rather than waved through."""
    assert refuses_publication(parse_licence("CC0-1.0"), [UNDECLARED]) is not None


def test_no_inputs_means_nothing_to_check() -> None:
    """A dataset ingested directly from a source has no derivation to constrain it."""
    assert refuses_publication(parse_licence("CC0-1.0"), []) is None


# -- every built-in declares one ------------------------------------------------------------


@pytest.mark.parametrize("path", _BUILTIN_TEMPLATES, ids=lambda p: pathlib.Path(p).name)
def test_every_builtin_template_declares_a_licence(path: str) -> None:
    """No built-in may rely on the default. A user seeing a layer should be able to see what
    they may do with it, and `other` is what OCS says when nobody has said anything."""
    undeclared = [
        t.get("id")
        for t in yaml.safe_load(pathlib.Path(path).read_text(encoding="utf-8"))
        if isinstance(t, dict) and t.get("license") is None
    ]
    assert not undeclared, f"templates without a licence: {undeclared}"


@pytest.mark.parametrize("path", _BUILTIN_TEMPLATES, ids=lambda p: pathlib.Path(p).name)
def test_every_builtin_licence_parses(path: str) -> None:
    """A declared licence that does not parse is worse than none: it looks handled."""
    for template in yaml.safe_load(pathlib.Path(path).read_text(encoding="utf-8")):
        if not isinstance(template, dict):
            continue
        assert parse_licence(template.get("license")) is not UNDECLARED, (
            f"{template.get('id')} declares an unparseable licence {template.get('license')!r}"
        )


def test_derived_builtins_are_no_more_permissive_than_their_source() -> None:
    """The normals and anomalies are derivative works of CHIRPS3 and ERA5-Land, and were the
    templates the first pass missed — they carry no `source_url`, so an insertion anchored on
    that skipped every one of them."""
    by_id = {}
    for path in _BUILTIN_TEMPLATES:
        for template in yaml.safe_load(pathlib.Path(path).read_text(encoding="utf-8")):
            if isinstance(template, dict) and template.get("id"):
                by_id[template["id"]] = parse_licence(template.get("license"))

    for derived_id, source_id in (
        ("chirps3_precipitation_monthly_normal_1991_2020", "chirps3_precipitation_monthly"),
        ("chirps3_precipitation_monthly_anomaly_1991_2020", "chirps3_precipitation_monthly"),
        ("era5land_temperature_monthly_normal_1991_2020", "era5land_temperature_monthly"),
        ("era5land_temperature_monthly_anomaly_1991_2020", "era5land_temperature_monthly"),
    ):
        assert refuses_publication(by_id[derived_id], [by_id[source_id]]) is None, (
            f"{derived_id} is more permissive than {source_id}"
        )


# -- the derive path, end to end -----------------------------------------------------------


def _cube(variable: str) -> "xr.Dataset":
    """A real one-variable cube. The derived-template builder computes a display range from the
    values, so stubbing far enough to satisfy it costs more than just handing it a small array.
    """
    import numpy as np
    import xarray as xr

    return xr.Dataset(
        {variable: (("y", "x"), np.array([[1.0, 2.0], [3.0, 4.0]], dtype="float32"))},
        coords={"y": [1.0, 0.0], "x": [0.0, 1.0]},
    )


def _derive(options: dict, source_template: dict | None) -> dict:
    """Run the real derived-template builder, not a reimplementation of its licence rule."""
    from open_climate_service.openeo import jobs

    return jobs._derive_managed_dataset_template(_cube(str(options["variable"])), options, source_template, None)


_NC_SOURCE = {
    "id": "vantor_flood_rgb_2026",
    "name": "Flood true colour",
    "variable": "true_colour",
    "license": "CC-BY-NC-4.0",
    "providers": [{"name": "Vantor", "roles": ["producer"]}],
}


def test_a_product_derived_from_a_non_commercial_source_inherits_the_restriction() -> None:
    """The acceptance case from CLIM-946: a water mask computed from CC-BY-NC imagery.

    Before this, the derived collection published `various` — indistinguishable from an openly
    licensed one, on the default path, with nothing to catch it.
    """
    template = _derive(
        {"dataset_id": "flood_water_mask", "variable": "water", "source_dataset_id": "vantor_flood_rgb_2026"},
        _NC_SOURCE,
    )
    assert parse_licence(template["license"]).commercial_use is False
    # Attribution is a licence condition under CC-BY-NC, so it has to travel too.
    assert template["providers"] == _NC_SOURCE["providers"]


def test_publishing_a_derived_product_as_permissive_is_refused() -> None:
    """Refused, not warned about: the output is a derivative work and a log line is not a
    defence."""
    with pytest.raises(ValueError, match="more permissive"):
        _derive(
            {
                "dataset_id": "flood_water_mask",
                "variable": "water",
                "source_dataset_id": "vantor_flood_rgb_2026",
                "license": "CC0-1.0",
            },
            _NC_SOURCE,
        )


def test_a_derived_product_may_declare_a_stricter_licence_than_its_source() -> None:
    template = _derive(
        {
            "dataset_id": "derived",
            "variable": "value",
            "source_dataset_id": "chirps3_precipitation_monthly",
            "license": "CC-BY-NC-4.0",
        },
        {"id": "chirps3_precipitation_monthly", "variable": "precip", "license": "CC0-1.0"},
    )
    assert template["license"] == "CC-BY-NC-4.0"


def test_deriving_from_an_unlabelled_source_leaves_the_output_unlabelled() -> None:
    """Not permissive by omission. An unlabelled input yields an unlabelled output, which
    publishes as `other` rather than as something reassuring."""
    template = _derive(
        {"dataset_id": "derived", "variable": "value", "source_dataset_id": "mystery"},
        {"id": "mystery", "variable": "value"},
    )
    assert parse_licence(template.get("license")) is UNDECLARED
