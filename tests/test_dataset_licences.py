"""Dataset licences: declaration, publication and propagation (CLIM-946).

The failure this guards against is silent. OCS published the constant `various` on every
collection, so a water mask derived from CC-BY-NC imagery was published as though it carried no
restriction — on the default path, with nothing to catch it.
"""

import glob
import logging
import pathlib
import re
from typing import TYPE_CHECKING

import pytest
import yaml

from open_climate_service.shared.licences import (
    ATTRIBUTION,
    NO_DERIVATIVES,
    NON_COMMERCIAL,
    STAC_LICENSE_OTHER,
    UNDECLARED,
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
    assert licence.obligations == frozenset({ATTRIBUTION})
    assert licence.commercial_use is True
    assert licence.stac_license == "CC-BY-4.0"


def test_spdx_matching_is_case_insensitive_but_output_is_canonical() -> None:
    """`cc-by-4.0` is easy to write in YAML; the published identifier must still be canonical,
    because `license` is a constrained STAC field."""
    assert parse_licence("cc-by-4.0").stac_license == "CC-BY-4.0"


def test_an_unrecognised_string_is_a_name_not_an_spdx_identifier() -> None:
    """Emitting a vendor string verbatim would produce a collection that does not validate.

    It is kept as a name — so nothing is lost — and published as `other`.
    """
    licence = parse_licence("SomeVendorLicence-2.0")
    assert licence.identifier is None
    assert licence.name == "SomeVendorLicence-2.0"
    assert licence.stac_license == STAC_LICENSE_OTHER
    assert licence.known is False


@pytest.mark.parametrize("identifier", ["CC-BY-NC-4.0", "CC-BY-NC-SA-4.0", "CC-BY-NC-ND-4.0", "cc-by-nc-3.0"])
def test_every_non_commercial_cc_licence_is_caught(identifier: str) -> None:
    assert NON_COMMERCIAL in parse_licence(identifier).obligations
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
    assert licence.obligations == frozenset({ATTRIBUTION})
    # STAC 1.1 has a defined value for "not SPDX", and it is not `various`.
    assert licence.stac_license == STAC_LICENSE_OTHER


def test_an_unknown_identifier_is_unknown_not_permissive() -> None:
    """The safe answer for a licence nobody has checked is "we do not know"."""
    assert parse_licence("SomeVendorLicence-2.0").commercial_use is None


def test_explicit_obligations_describe_a_licence_in_neither_table() -> None:
    licence = parse_licence(
        {"name": "Vendor Terms", "url": "https://x", "obligations": ["attribution", "non-commercial"]}
    )
    assert licence.known is True
    assert licence.commercial_use is False


@pytest.mark.parametrize("bad", [None, "", "   ", 42, [], {}, {"url": "https://x"}])
def test_unusable_declarations_degrade_to_undeclared(bad: object) -> None:
    """A malformed licence must not take down the catalogue; the validator warns separately."""
    assert parse_licence(bad) is UNDECLARED


def test_undeclared_publishes_as_other() -> None:
    assert UNDECLARED.stac_license == STAC_LICENSE_OTHER
    assert UNDECLARED.commercial_use is None
    assert UNDECLARED.known is False


# -- comparison -------------------------------------------------------------------------
#
# `most_restrictive` used to live here and has been removed. It ordered licences by how many
# obligations they carried, which is not an ordering: CC-BY-SA and CC-BY-NC both carry two, so
# it returned whichever came first and silently dropped the other's requirement. Nothing in
# production ever called it. `refuses_publication` compares against every input in turn, which
# handles incomparable sets correctly — see the two tests below. If multi-input propagation
# ever needs a single inherited licence, it needs a union-preserving representation, not a
# cardinality comparison.


def test_incomparable_licences_refuse_in_both_directions() -> None:
    """CC-BY-SA and CC-BY-NC each carry an obligation the other lacks, so neither can cover
    both. Checking per input catches this; picking a winner by obligation count does not."""
    share_alike = parse_licence("CC-BY-SA-4.0")
    non_commercial = parse_licence("CC-BY-NC-4.0")
    assert refuses_publication(share_alike, [share_alike, non_commercial]) is not None
    assert refuses_publication(non_commercial, [share_alike, non_commercial]) is not None


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


def test_dropping_attribution_is_refused_even_though_both_allow_commercial_use() -> None:
    """CC0 from CC-BY-4.0: both permit commercial use, but the derived product would shed
    WorldPop's attribution requirement."""
    refusal = refuses_publication(parse_licence("CC0-1.0"), [parse_licence("CC-BY-4.0")])
    assert refusal is not None
    assert "attribution" in refusal


def test_dropping_share_alike_is_refused() -> None:
    refusal = refuses_publication(parse_licence("CC-BY-4.0"), [parse_licence("CC-BY-SA-4.0")])
    assert refusal is not None
    assert "share-alike" in refusal


def test_an_unparseable_declared_licence_is_refused_not_waved_through() -> None:
    """Fails closed: if either side is not understood the pair is incomparable."""
    assert refuses_publication(parse_licence("MysteryTerms"), [parse_licence("CC-BY-4.0")]) is not None


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


def _builtin_licences() -> dict:
    licences = {}
    for path in _BUILTIN_TEMPLATES:
        for template in yaml.safe_load(pathlib.Path(path).read_text(encoding="utf-8")):
            if isinstance(template, dict) and template.get("id"):
                licences[template["id"]] = parse_licence(template.get("license"))
    return licences


def _derived_source_pairs() -> list[tuple[str, str]]:
    """Every derived built-in paired with the dataset it is computed from.

    Derived from the ids rather than listed by hand: an earlier version enumerated four pairs
    and left twelve unchecked, so a licence change in any of those twelve could have loosened
    silently. Suffix-stripping means a new normal or anomaly is covered the day it is added.
    """
    ids = set(_builtin_licences())
    pairs = []
    for dataset_id in sorted(ids):
        base = re.sub(r"_(relative_)?(normal|anomaly)_\d{4}_\d{4}$", "", dataset_id)
        if base != dataset_id and base in ids:
            pairs.append((dataset_id, base))
    return pairs


def test_the_derived_pairs_are_actually_found() -> None:
    """Guards the guard: a suffix change would silently empty the parametrisation below and
    the whole check would pass by testing nothing."""
    assert len(_derived_source_pairs()) >= 12


@pytest.mark.parametrize(("derived_id", "source_id"), _derived_source_pairs(), ids=lambda v: v)
def test_no_derived_builtin_is_more_permissive_than_its_source(derived_id: str, source_id: str) -> None:
    """The normals and anomalies are derivative works of CHIRPS3 and ERA5-Land.

    They carry no `source_url`, so a first pass that anchored insertion on that field skipped
    every one of them.
    """
    licences = _builtin_licences()
    assert refuses_publication(licences[derived_id], [licences[source_id]]) is None


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
    with pytest.raises(ValueError, match="drops"):
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


# -- the ND prohibition ------------------------------------------------------------------


def test_no_derivatives_is_recorded_as_a_prohibition() -> None:
    assert NO_DERIVATIVES in parse_licence("CC-BY-NC-ND-4.0").obligations


def test_nothing_may_be_derived_from_an_nd_input_not_even_under_the_same_licence() -> None:
    """ND forbids distributing adapted material at all, so this is not a matter of matching
    obligations — there is no licence under which the derived product may be published."""
    nd = parse_licence("CC-BY-NC-ND-4.0")
    refusal = refuses_publication(nd, [nd])
    assert refusal is not None
    assert "prohibits derivative works" in refusal


_ND_SOURCE = {
    "id": "nd_imagery",
    "name": "Restricted imagery",
    "variable": "rgb",
    "license": "CC-BY-ND-4.0",
}


def test_saying_nothing_does_not_publish_what_naming_the_licence_would_refuse() -> None:
    """The implicit-inheritance path has to clear the ND bar too.

    `license: CC-BY-ND-4.0` was refused while omitting `license` inherited the same licence
    and published — so the check was reachable only by declaring what it would reject. The
    unit test above passed throughout: it drove `refuses_publication` directly, and this path
    never called it.
    """
    with pytest.raises(ValueError, match="prohibits derivative works"):
        _derive({"dataset_id": "mask", "variable": "water", "source_dataset_id": "nd_imagery"}, _ND_SOURCE)


def test_a_preregistered_template_cannot_receive_nd_data_by_declaring_nothing() -> None:
    """The same hole on the loaded-template path."""
    from open_climate_service.openeo import jobs

    with pytest.raises(ValueError, match="prohibits derivative works"):
        jobs._enforce_licence_on_loaded_template("mask", {"id": "mask"}, _ND_SOURCE)


def test_inheriting_an_unparseable_source_licence_is_not_refused() -> None:
    """`refuses_derivation` must not fail closed the way `refuses_publication` does.

    Carrying a licence forward verbatim cannot launder it, so refusing here would block every
    derived product whose source licence OCS cannot parse — a licence-metadata gap turned into
    an ingest failure. Template validation warns about the gap instead.
    """
    template = _derive(
        {"dataset_id": "derived", "variable": "value", "source_dataset_id": "vendor"},
        {"id": "vendor", "variable": "value", "license": "Some Vendor Terms"},
    )
    assert template["license"] == "Some Vendor Terms"


# -- the SPDX table is authoritative ------------------------------------------------------


def test_explicit_obligations_cannot_override_a_recognised_spdx_identifier() -> None:
    """`{id: CC-BY-NC-4.0, obligations: []}` would otherwise publish as CC-BY-NC while every
    compatibility check saw no restrictions — laundering by configuration."""
    licence = parse_licence({"id": "CC-BY-NC-4.0", "obligations": []})
    assert licence.stac_license == "CC-BY-NC-4.0"
    assert licence.commercial_use is False
    assert NON_COMMERCIAL in licence.obligations


def test_explicit_obligations_cannot_override_a_recognised_name() -> None:
    licence = parse_licence({"name": "Licence to Use Copernicus Products", "url": "https://x", "obligations": []})
    assert licence.obligations == frozenset({ATTRIBUTION})


def test_explicit_obligations_are_used_when_nothing_else_supplies_them() -> None:
    """The legitimate case: a licence in neither table, whose terms someone has read."""
    licence = parse_licence({"name": "Vendor Terms", "url": "https://x", "obligations": ["attribution"]})
    assert licence.known is True
    assert licence.obligations == frozenset({ATTRIBUTION})


# -- providers on a derived product --------------------------------------------------------


def test_derived_providers_keep_the_source_licensor() -> None:
    """Attribution is a licence condition, so dropping the source's licensor breaches it even
    when the obligation check passes. An explicit list adds to the source's, never replaces."""
    template = _derive(
        {
            "dataset_id": "d",
            "variable": "water",
            "source_dataset_id": "vantor_flood_rgb_2026",
            "providers": [{"name": "DHIS2"}],
        },
        _NC_SOURCE,
    )
    names = [p["name"] for p in template["providers"]]
    assert names == ["Vantor", "DHIS2"]


def test_an_empty_providers_option_cannot_erase_the_source_licensor() -> None:
    template = _derive(
        {"dataset_id": "d", "variable": "water", "source_dataset_id": "vantor_flood_rgb_2026", "providers": []},
        _NC_SOURCE,
    )
    assert [p["name"] for p in template["providers"]] == ["Vantor"]


def test_duplicate_providers_are_not_doubled() -> None:
    template = _derive(
        {
            "dataset_id": "d",
            "variable": "water",
            "source_dataset_id": "vantor_flood_rgb_2026",
            "providers": [{"name": "vantor"}],
        },
        _NC_SOURCE,
    )
    assert len(template["providers"]) == 1


# -- valid SPDX identifiers OCS has not reviewed -------------------------------------------


def test_a_valid_spdx_identifier_is_preserved_even_when_unreviewed() -> None:
    """Downgrading BSD-3-Clause to a free-form name and publishing `other` threw away
    information the catalogue already had. The identifier is kept; only the propagation
    semantics are marked unknown."""
    licence = parse_licence("BSD-3-Clause")
    assert licence.stac_license == "BSD-3-Clause"
    assert licence.known is False


def test_deriving_from_an_unreviewed_spdx_licence_fails_closed() -> None:
    """Publishing the identifier is safe; reasoning about derivation from it is not."""
    bsd = parse_licence("BSD-3-Clause")
    assert refuses_publication(bsd, [bsd]) is not None


# -- share-alike is licence identity, not an obligation flag --------------------------------


def test_another_share_alike_licence_is_not_an_acceptable_substitute() -> None:
    """ODbL-1.0 and CC-BY-SA-4.0 both reduce to {attribution, share-alike}, so a subset check
    accepts each in place of the other. That is relicensing, which share-alike forbids."""
    refusal = refuses_publication(parse_licence("ODbL-1.0"), [parse_licence("CC-BY-SA-4.0")])
    assert refusal is not None
    assert "share-alike" in refusal


def test_adding_non_commercial_to_a_share_alike_input_is_still_relicensing() -> None:
    """CC-BY-NC-SA's obligations are a superset of CC-BY-SA's, so the subset check passed it."""
    assert refuses_publication(parse_licence("CC-BY-NC-SA-4.0"), [parse_licence("CC-BY-SA-4.0")]) is not None


def test_the_same_share_alike_licence_is_accepted() -> None:
    sa = parse_licence("CC-BY-SA-4.0")
    assert refuses_publication(sa, [sa]) is None


def test_a_share_alike_version_upgrade_the_licence_permits_is_accepted() -> None:
    """CC-BY-SA-3.0 explicitly allows relicensing under 4.0."""
    assert refuses_publication(parse_licence("CC-BY-SA-4.0"), [parse_licence("CC-BY-SA-3.0")]) is None


# -- lineage that does not resolve ----------------------------------------------------------


def test_an_unresolvable_source_dataset_id_is_an_error_not_absent_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A typo would otherwise mean "not derived from anything", disabling propagation and
    letting a permissive output through with no error anywhere."""
    from open_climate_service.data_registry.services import datasets as reg
    from open_climate_service.openeo import jobs

    monkeypatch.setattr(reg, "get_dataset", lambda _: None)
    with pytest.raises(ValueError, match="not a registered dataset template"):
        jobs._resolve_source_template({"source_dataset_id": "chirps3_typooo"})


# -- an already-registered output template --------------------------------------------------


def test_a_preregistered_permissive_template_cannot_receive_restricted_data() -> None:
    """The path CLIM-946 recommends. The licence rule ran only while *synthesising* a template,
    so pre-registering one — which the ticket advises, to control the display — skipped the
    check entirely and a CC0 template could publish CC-BY-NC-derived data."""
    from open_climate_service.openeo import jobs

    with pytest.raises(ValueError, match="cannot receive data derived from"):
        jobs._enforce_licence_on_loaded_template(
            "flood_water_mask", {"id": "flood_water_mask", "license": "CC0-1.0"}, _NC_SOURCE
        )


def test_a_preregistered_undeclared_template_inherits_rather_than_staying_undeclared() -> None:
    """Migrates a template left by an older run, instead of leaving it unlabelled forever."""
    from open_climate_service.openeo import jobs

    updated = jobs._enforce_licence_on_loaded_template("flood_water_mask", {"id": "flood_water_mask"}, _NC_SOURCE)
    assert updated["license"] == "CC-BY-NC-4.0"
    assert [p["name"] for p in updated["providers"]] == ["Vantor"]


def test_a_matching_preregistered_licence_still_has_to_credit_the_licensor() -> None:
    """A compatible licence identifier is not the whole licence.

    `CC-BY-NC-4.0` on both sides passes the obligation check, so this template used to be
    returned untouched — and it names nobody, so the derived product published without the
    attribution CC-BY-NC requires. The check compared identifiers while the condition it was
    enforcing lives in the providers.
    """
    from open_climate_service.openeo import jobs

    updated = jobs._enforce_licence_on_loaded_template(
        "flood_water_mask", {"id": "flood_water_mask", "license": "CC-BY-NC-4.0"}, _NC_SOURCE
    )
    assert [p["name"] for p in updated["providers"]] == ["Vantor"]


def test_a_preregistered_templates_own_providers_are_kept_alongside_the_sources() -> None:
    from open_climate_service.openeo import jobs

    updated = jobs._enforce_licence_on_loaded_template(
        "flood_water_mask",
        {"id": "flood_water_mask", "license": "CC-BY-NC-4.0", "providers": [{"name": "DHIS2"}]},
        _NC_SOURCE,
    )
    assert [p["name"] for p in updated["providers"]] == ["Vantor", "DHIS2"]


def test_a_template_needing_no_change_is_returned_untouched() -> None:
    """No write, no copy: nothing to persist when the registered metadata is already right."""
    from open_climate_service.openeo import jobs

    template = {
        "id": "flood_water_mask",
        "license": "CC-BY-NC-4.0",
        "providers": [{"name": "Vantor", "roles": ["producer"]}],
    }
    assert jobs._enforce_licence_on_loaded_template("flood_water_mask", template, _NC_SOURCE) is template


def test_the_enforced_licence_reaches_the_registry_not_just_this_publish(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    """STAC builds the collection from the on-disk template, so an in-memory fix publishes the
    same wrong licence it did before. `build_collection` reads the registry by dataset id and
    never sees the object this function returns."""
    from open_climate_service.data_registry.services import datasets as reg
    from open_climate_service.openeo import jobs

    monkeypatch.setattr(reg, "CONFIGS_DIR", tmp_path)
    # Shaped like a real pre-registered workflow output, so it survives template validation.
    jobs._enforce_licence_on_loaded_template(
        "flood_water_mask",
        {"id": "flood_water_mask", "variable": "water", "sync": {"kind": "static"}},
        _NC_SOURCE,
    )

    written = yaml.safe_load((tmp_path / "flood_water_mask.yaml").read_text())[0]
    assert written["license"] == "CC-BY-NC-4.0"
    assert [p["name"] for p in written["providers"]] == ["Vantor"]


def test_a_registry_that_cannot_be_written_does_not_fail_the_publish(monkeypatch: pytest.MonkeyPatch) -> None:
    """The licence is already correct on the returned template, so a read-only templates
    directory should not abort an otherwise valid publish."""
    from open_climate_service.data_registry.services import datasets as reg
    from open_climate_service.openeo import jobs

    def refuse(*_args: object, **_kwargs: object) -> None:
        raise OSError("read-only file system")

    monkeypatch.setattr(reg, "write_dataset_template", refuse)

    # Attached to the module logger directly: `open_climate_service` does not propagate to
    # root, so caplog never sees the record, and its handler holds the pre-capture stderr.
    records: list[logging.LogRecord] = []

    class Collect(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    handler = Collect()
    jobs.logger.addHandler(handler)
    try:
        updated = jobs._enforce_licence_on_loaded_template("flood_water_mask", {"id": "flood_water_mask"}, _NC_SOURCE)
    finally:
        jobs.logger.removeHandler(handler)

    assert updated["license"] == "CC-BY-NC-4.0"
    assert any("Could not persist licence metadata" in r.getMessage() for r in records)


def test_no_source_means_a_preregistered_template_is_untouched() -> None:
    from open_climate_service.openeo import jobs

    template = {"id": "standalone", "license": "CC0-1.0"}
    assert jobs._enforce_licence_on_loaded_template("standalone", template, None) is template
