"""Dataset licences: declare them, compare them, and refuse to launder them (CLIM-946).

OCS had no per-dataset licence concept — every collection published the constant `various`,
whether the data came from ERA5-Land or a non-commercial imagery release. That was tolerable
while every source was openly licensed. It stopped being tolerable when the August 2026 Nepal
flood produced the only usable imagery of the event under CC-BY-NC-4.0.

The decision recorded in CLIM-946 is to *allow* non-commercial sources, labelled, rather than
refuse them: declining the only imagery of a disaster is the opposite of the point. But
allowing them unlabelled is worse than either, because a derived product is a derivative work.
A water mask computed from CC-BY-NC imagery inherits the restriction, and publishing it as
`various` is licence laundering by accident — on the default path, with nothing to catch it.

## Why this is not just a string field

Two licences must be *comparable*, or propagation cannot be checked. So a declaration is
parsed into a `DatasetLicence` carrying one decisive fact: whether commercial use is allowed.
Three states, not two — `None` means "we do not know", which is different from "yes" and must
not be treated as it.

## SPDX where it exists, a name and URL where it does not

The three built-in sources happen to cover every case:

    CHIRPS3     CC0-1.0                              SPDX, public domain
    WorldPop    CC-BY-4.0                            SPDX
    ERA5-Land   Licence to Use Copernicus Products   bespoke, no SPDX identifier

Forcing the Copernicus licence into a near-miss SPDX identifier would be a false statement
about what a user may do, so the field accepts a name plus a URL as well.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

# STAC 1.1 wants an SPDX identifier or the literal "other"; it deprecated "proprietary" and
# never defined "various", which is what OCS was publishing on every collection.
STAC_LICENSE_OTHER = "other"

# What a licence requires of anyone redistributing the data, or a work derived from it. A
# derived product may add obligations; it may never drop one.
ATTRIBUTION = "attribution"
SHARE_ALIKE = "share-alike"
NON_COMMERCIAL = "non-commercial"

# SPDX identifiers whose terms have been read, with the obligations each imposes. Deliberately
# a small checked table rather than an attempt at the full SPDX register: an identifier absent
# here is not an SPDX identifier as far as OCS is concerned, which keeps an unvalidated string
# out of the published `license` field.
#
# Commercial use is one obligation among several, not the whole model. Comparing on it alone
# would let CC0 be derived from CC-BY-4.0 — both permit commercial use — silently dropping
# WorldPop's attribution requirement.
_SPDX_OBLIGATIONS: dict[str, frozenset[str]] = {
    "CC0-1.0": frozenset(),
    "PDDL-1.0": frozenset(),
    "MIT": frozenset({ATTRIBUTION}),
    "Apache-2.0": frozenset({ATTRIBUTION}),
    "CC-BY-3.0": frozenset({ATTRIBUTION}),
    "CC-BY-4.0": frozenset({ATTRIBUTION}),
    "OGL-UK-3.0": frozenset({ATTRIBUTION}),
    "ODbL-1.0": frozenset({ATTRIBUTION, SHARE_ALIKE}),
    "CC-BY-SA-3.0": frozenset({ATTRIBUTION, SHARE_ALIKE}),
    "CC-BY-SA-4.0": frozenset({ATTRIBUTION, SHARE_ALIKE}),
    "CC-BY-NC-3.0": frozenset({ATTRIBUTION, NON_COMMERCIAL}),
    "CC-BY-NC-4.0": frozenset({ATTRIBUTION, NON_COMMERCIAL}),
    "CC-BY-NC-SA-3.0": frozenset({ATTRIBUTION, SHARE_ALIKE, NON_COMMERCIAL}),
    "CC-BY-NC-SA-4.0": frozenset({ATTRIBUTION, SHARE_ALIKE, NON_COMMERCIAL}),
    "CC-BY-NC-ND-4.0": frozenset({ATTRIBUTION, NON_COMMERCIAL}),
}
_SPDX_BY_LOWER = {identifier.lower(): identifier for identifier in _SPDX_OBLIGATIONS}

# Licences with no SPDX identifier, whose terms have been read. Without this a template author
# has to restate the obligations by hand for every one, and an assertion repeated in five
# templates is one that will eventually be wrong in a sixth.
#
# Note what this is NOT: a default. An unrecognised licence resolves to "obligations unknown",
# never to "no obligations". Defaulting to permissive would mean an author who forgets to
# describe a restrictive source publishes it as freely reusable, which is the laundering this
# module exists to prevent.
_KNOWN_NAMED_LICENCES: dict[str, frozenset[str]] = {
    # "free of charge, worldwide, non-exclusive, royalty free and perpetual", for "any purpose
    # in so far as it is lawful", with clear attribution to Copernicus required.
    "licence to use copernicus products": frozenset({ATTRIBUTION}),
    # Free and open including commercial reuse, subject to the Notice's conditions.
    "copernicus sentinel data legal notice": frozenset({ATTRIBUTION}),
    # NASA Earth science data carries no copyright and no use restrictions.
    "nasa earth science data": frozenset(),
}


@dataclass(frozen=True)
class DatasetLicence:
    """A licence declaration, parsed into something two datasets can be compared on."""

    identifier: str | None
    """Canonical SPDX identifier, or None when the licence is named rather than identified."""

    name: str | None
    url: str | None

    obligations: frozenset[str]
    """What the licence requires. Meaningless unless `known` is true."""

    known: bool
    """Whether the obligations were actually determined, rather than defaulted."""

    @property
    def stac_license(self) -> str:
        """The value for a STAC collection's `license` field.

        A canonical SPDX identifier when there is one, otherwise the literal `other`, which
        STAC 1.1 defines for exactly this case. Never a free-form string: `license` is a
        constrained field, and emitting an unvalidated vendor name there produces a collection
        that does not validate. Never `various` either, which is not a STAC value at all and
        reads as "no restrictions worth mentioning".
        """
        return self.identifier or STAC_LICENSE_OTHER

    @property
    def label(self) -> str:
        return self.identifier or self.name or "not declared"

    @property
    def commercial_use(self) -> bool | None:
        """Whether commercial use is permitted, or None when the licence is not understood."""
        if not self.known:
            return None
        return NON_COMMERCIAL not in self.obligations

    def drops_obligations_of(self, other: DatasetLicence) -> frozenset[str]:
        """Obligations of `other` that this licence does not carry."""
        return other.obligations - self.obligations


UNDECLARED = DatasetLicence(identifier=None, name=None, url=None, obligations=frozenset(), known=False)
"""A template with no `license`, or one that could not be parsed. Published as `other`."""


def _obligations_for_name(name: str | None) -> frozenset[str] | None:
    """Obligations for a licence known by name rather than SPDX identifier.

    Matched on a normalised prefix so a template may add a version or qualifier — "Licence to
    Use Copernicus Products v1.2" resolves the same as the bare name — without a new entry.
    """
    if not name:
        return None
    normalised = " ".join(name.lower().split())
    for known, obligations in _KNOWN_NAMED_LICENCES.items():
        if normalised.startswith(known):
            return obligations
    return None


def _canonical_spdx(value: str) -> str | None:
    """The canonical SPDX identifier for `value`, or None if it is not one we recognise.

    Case-insensitive, because `cc-by-4.0` is an easy thing to write in YAML and rejecting it
    outright would be unhelpful. An unrecognised string is not treated as SPDX at all: it
    becomes a licence *name*, so the collection publishes `other` rather than an identifier no
    STAC client can resolve.
    """
    return _SPDX_BY_LOWER.get(value.strip().lower())


def parse_licence(declared: Any) -> DatasetLicence:
    """Parse a template's `license` field.

    Accepts either form, because the sources demand both:

        license: CC-BY-4.0
        license:
          name: Licence to Use Copernicus Products
          url: https://apps.ecmwf.int/datasets/licences/copernicus/

    A string that is not a recognised SPDX identifier is kept as a *name*, not passed through
    to STAC as though it were one. Anything unparseable resolves to `UNDECLARED` rather than
    raising: a malformed licence should degrade to "unknown, published as other", not take down
    the catalogue, and the template validator warns about it separately.
    """
    if isinstance(declared, str):
        text = declared.strip()
        if not text:
            return UNDECLARED
        spdx = _canonical_spdx(text)
        if spdx is not None:
            return DatasetLicence(identifier=spdx, name=None, url=None, obligations=_SPDX_OBLIGATIONS[spdx], known=True)
        named = _obligations_for_name(text)
        return DatasetLicence(
            identifier=None,
            name=text,
            url=None,
            obligations=named if named is not None else frozenset(),
            known=named is not None,
        )

    if isinstance(declared, dict):
        raw_id = declared.get("id") or declared.get("spdx")
        identifier = _canonical_spdx(str(raw_id)) if isinstance(raw_id, str) and raw_id.strip() else None
        raw_name = declared.get("name")
        name = str(raw_name).strip() if isinstance(raw_name, str) and raw_name.strip() else None
        raw_url = declared.get("url")
        url = str(raw_url).strip() if isinstance(raw_url, str) and raw_url.strip() else None

        if identifier is None and name is None:
            return UNDECLARED

        # An explicit `obligations` list wins, for a licence whose terms someone has read and
        # which is in neither table.
        raw_obligations = declared.get("obligations")
        if isinstance(raw_obligations, list):
            obligations = frozenset(str(o).strip().lower() for o in raw_obligations if str(o).strip())
            known = True
        elif identifier is not None:
            obligations, known = _SPDX_OBLIGATIONS[identifier], True
        else:
            named = _obligations_for_name(name)
            obligations, known = (named, True) if named is not None else (frozenset(), False)

        return DatasetLicence(identifier=identifier, name=name, url=url, obligations=obligations, known=known)

    return UNDECLARED


def most_restrictive(licences: list[DatasetLicence]) -> DatasetLicence:
    """The licence a product derived from these inputs must carry.

    "Most restrictive" is the one carrying the most obligations, with an unknown licence
    winning outright — a licence nobody has described might require anything.
    """
    if not licences:
        return UNDECLARED
    unknown = [licence for licence in licences if not licence.known]
    if unknown:
        return unknown[0]
    return max(licences, key=lambda licence: len(licence.obligations))


def refuses_publication(declared: DatasetLicence, inputs: list[DatasetLicence]) -> str | None:
    """Why publishing `declared` for a product derived from `inputs` must be refused, or None.

    A derived product is a derivative work: it may add obligations, never drop one. Refused
    rather than warned about, because the failure is silent and a log line is not a defence.

    Fails closed on anything not understood. If either side's obligations are unknown the pair
    is incomparable, and the safe answer is to refuse rather than assume they are compatible —
    which is how an unlabelled restrictive source would otherwise be laundered.
    """
    for candidate in inputs:
        if not candidate.known:
            return (
                f"Derived dataset declares licence {declared.label!r}, but its input "
                f"{candidate.label!r} has no licence OCS understands, so the two cannot be "
                f"compared. Declare the input's licence, or the derived product's terms are a "
                f"guess."
            )
        if not declared.known:
            return (
                f"Derived dataset declares licence {declared.label!r}, which OCS does not "
                f"understand, so it cannot be checked against its input {candidate.label!r}. "
                f"Use an SPDX identifier, or a licence name OCS knows, or list `obligations`."
            )
        dropped = declared.drops_obligations_of(candidate)
        if dropped:
            return (
                f"Derived dataset declares licence {declared.label!r}, which drops "
                f"{sorted(dropped)} required by its input {candidate.label!r}. A derived "
                f"product is a derivative work and inherits its inputs' obligations."
            )
    return None
