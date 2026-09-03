"""Dataset licences: declaration and publication (CLIM-946).

The failure this guards against is silent. OCS published the constant `various` on every
collection, so a CC-BY-NC source was published as though it carried no restriction.

Propagation to derived products is deliberately not here. It is a separate change with a
single enforcement point, because splitting the decision across the synthesise and reload
paths produced four rounds of contradictory fixes.
"""

import glob
import pathlib
import re

import pytest
import yaml

from open_climate_service.shared.licences import (
    ATTRIBUTION,
    NON_COMMERCIAL,
    STAC_LICENSE_OTHER,
    UNDECLARED,
    parse_licence,
)

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
def test_no_derived_builtin_declares_away_its_sources_obligations(derived_id: str, source_id: str) -> None:
    """The normals and anomalies are derivative works of CHIRPS3 and ERA5-Land.

    Stated as a set comparison rather than by calling the production predicate: a test that
    asks the code under test whether the code under test is right passes when both are wrong.

    They carry no `source_url`, so a first pass that anchored insertion on that field skipped
    every one of them.
    """
    licences = _builtin_licences()
    derived, source = licences[derived_id], licences[source_id]
    assert derived.known and source.known, "a shipped licence OCS cannot parse"
    assert source.obligations <= derived.obligations, (
        f"{derived_id} drops {sorted(source.obligations - derived.obligations)} required by {source_id}"
    )


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


# -- valid SPDX identifiers OCS has not reviewed -------------------------------------------


def test_a_valid_spdx_identifier_is_preserved_even_when_unreviewed() -> None:
    """Downgrading BSD-3-Clause to a free-form name and publishing `other` threw away
    information the catalogue already had. The identifier is kept; only the propagation
    semantics are marked unknown."""
    licence = parse_licence("BSD-3-Clause")
    assert licence.stac_license == "BSD-3-Clause"
    assert licence.known is False


# -- a name must be the licence, not merely start like it ----------------------------------


def test_a_qualified_name_does_not_inherit_a_known_licences_terms() -> None:
    """`startswith` matching classified this as NASA's unrestricted terms, so a
    non-commercial variant read as free for commercial use — the exact laundering the
    unknown-is-not-permissive rule exists to prevent."""
    licence = parse_licence("NASA Earth Science Data - Non-Commercial Terms")
    assert licence.known is False
    assert licence.commercial_use is None


def test_an_exact_known_name_still_resolves() -> None:
    assert parse_licence("NASA Earth Science Data").obligations == frozenset()
    assert parse_licence("Licence to Use Copernicus Products").obligations == frozenset({ATTRIBUTION})


@pytest.mark.parametrize("name", ["Licence to Use Copernicus Products v1.2", "Licence to Use Copernicus Products 2"])
def test_a_trailing_version_is_still_the_same_licence(name: str) -> None:
    """A version number cannot change what a licence requires, so these stay recognised."""
    assert parse_licence(name).obligations == frozenset({ATTRIBUTION})


# -- an identifier and a name that disagree -------------------------------------------------


def test_an_identifier_contradicted_by_a_known_name_is_undeclared() -> None:
    """`license` is read far more often than the licence link is fetched, so publishing
    `CC0-1.0` beside a link to terms requiring attribution is worse than publishing nothing.
    There is no basis for choosing between them."""
    licence = parse_licence({"id": "CC0-1.0", "name": "Licence to Use Copernicus Products", "url": "https://x"})
    assert licence is UNDECLARED


def test_an_identifier_with_a_consistent_name_and_url_is_kept() -> None:
    """The ordinary spelling of one licence in all three fields has to keep working — the URL
    is what the `rel: license` link needs."""
    licence = parse_licence(
        {
            "id": "CC-BY-4.0",
            "name": "Creative Commons Attribution 4.0 International",
            "url": "https://creativecommons.org/licenses/by/4.0/",
        }
    )
    assert licence.stac_license == "CC-BY-4.0"
    assert licence.url == "https://creativecommons.org/licenses/by/4.0/"
