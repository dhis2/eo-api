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

# SPDX identifiers known to permit commercial use. Deliberately a small, checked list rather
# than an attempt at the full SPDX register: an identifier absent here is reported as unknown,
# which is the safe answer, and adding one is a one-line change with a citation.
_COMMERCIAL_USE_ALLOWED = frozenset(
    {
        "CC0-1.0",
        "CC-BY-4.0",
        "CC-BY-3.0",
        "CC-BY-SA-4.0",
        "ODbL-1.0",
        "PDDL-1.0",
        "Apache-2.0",
        "MIT",
        "OGL-UK-3.0",
    }
)

# Creative Commons marks a non-commercial licence with an `NC` element, so this catches the
# whole family — CC-BY-NC-4.0, CC-BY-NC-SA-4.0, CC-BY-NC-ND-4.0 — without enumerating it.
_NON_COMMERCIAL_MARKER = "-NC"

# Licences with no SPDX identifier, whose terms have been read. Without this a template author
# has to assert `commercial_use` by hand for every one of them, and an assertion repeated in
# five templates is an assertion that will eventually be wrong in one of them.
#
# Note what this is NOT: a default. An unrecognised licence still resolves to "unknown", never
# to "commercial use allowed". Defaulting to allowed would mean an author who forgets the flag
# publishes a restrictive source as permissive — precisely the laundering this module exists to
# prevent. Each entry here is a licence someone read, not a guess applied wholesale.
_KNOWN_NAMED_LICENCES: dict[str, bool] = {
    # "free of charge, worldwide, non-exclusive, royalty free and perpetual", for "any purpose
    # in so far as it is lawful", with attribution required.
    "licence to use copernicus products": True,
    # Free and open including commercial reuse, subject to the Notice's conditions.
    "copernicus sentinel data legal notice": True,
    # NASA Earth science data carries no copyright and no use restrictions.
    "nasa earth science data": True,
}

# How restrictive a licence is, for picking the one a derived product must carry. Unknown
# ranks above permissive on purpose: a licence nobody has declared might forbid anything, so
# inheriting "unknown" from an unlabelled input is honest, and the alternative — assuming
# permissive — is exactly the laundering this module exists to prevent.
_RANK_PERMISSIVE = 0
_RANK_UNKNOWN = 1
_RANK_NON_COMMERCIAL = 2


@dataclass(frozen=True)
class DatasetLicence:
    """A licence declaration, parsed into something two datasets can be compared on."""

    identifier: str | None
    """SPDX identifier, or None when the licence is named rather than identified."""

    name: str | None
    """Human-readable name, for a licence with no SPDX identifier."""

    url: str | None

    commercial_use: bool | None
    """True, False, or None for "not known" — which is not the same as True."""

    @property
    def stac_license(self) -> str:
        """The value for a STAC collection's `license` field.

        An SPDX identifier when there is one, otherwise the literal `other`, which STAC 1.1
        defines for exactly this case. Never `various`: that is not a STAC value at all, and it
        reads as "no restrictions worth mentioning" when the truth may be the opposite.
        """
        return self.identifier or STAC_LICENSE_OTHER

    @property
    def label(self) -> str:
        """A short human label — the identifier, the name, or an explicit "not declared"."""
        return self.identifier or self.name or "not declared"

    @property
    def restrictiveness(self) -> int:
        if self.commercial_use is False:
            return _RANK_NON_COMMERCIAL
        if self.commercial_use is None:
            return _RANK_UNKNOWN
        return _RANK_PERMISSIVE

    def is_at_least_as_restrictive_as(self, other: DatasetLicence) -> bool:
        return self.restrictiveness >= other.restrictiveness


UNDECLARED = DatasetLicence(identifier=None, name=None, url=None, commercial_use=None)
"""What a template without a `license` field resolves to. Published as `other`."""


def _commercial_use_for_name(name: str | None) -> bool | None:
    """Commercial-use status for a licence known by name rather than SPDX identifier.

    Matched on a normalised prefix so a template may add a version or a qualifier — "Licence to
    Use Copernicus Products v1.2" resolves the same as the bare name — without a new entry.
    """
    if not name:
        return None
    normalised = " ".join(name.lower().split())
    for known, commercial in _KNOWN_NAMED_LICENCES.items():
        if normalised.startswith(known):
            return commercial
    return None


def _commercial_use_for(identifier: str) -> bool | None:
    upper = identifier.upper()
    if _NON_COMMERCIAL_MARKER in upper:
        return False
    if identifier in _COMMERCIAL_USE_ALLOWED:
        return True
    return None


def parse_licence(declared: Any) -> DatasetLicence:
    """Parse a template's `license` field.

    Accepts either form, because the sources demand both:

        license: CC-BY-4.0
        license:
          name: Licence to Use Copernicus Products
          url: https://apps.ecmwf.int/datasets/licences/copernicus/

    An `id` key is treated as an SPDX identifier, so a bespoke licence can still carry one if
    it ever gets registered. Anything unparseable resolves to `UNDECLARED` rather than raising:
    a malformed licence should degrade to "unknown, published as other", not take down the
    catalogue — and the template validator warns about it separately.
    """
    if isinstance(declared, str):
        spdx = declared.strip()
        if not spdx:
            return UNDECLARED
        return DatasetLicence(
            identifier=spdx,
            name=None,
            url=None,
            commercial_use=_commercial_use_for(spdx),
        )

    if isinstance(declared, dict):
        raw_id = declared.get("id") or declared.get("spdx")
        identifier: str | None = str(raw_id).strip() if isinstance(raw_id, str) and raw_id.strip() else None
        raw_name = declared.get("name")
        name = str(raw_name).strip() if isinstance(raw_name, str) and raw_name.strip() else None
        raw_url = declared.get("url")
        url = str(raw_url).strip() if isinstance(raw_url, str) and raw_url.strip() else None

        if identifier is None and name is None:
            return UNDECLARED

        # An explicit `commercial_use` overrides the lookup, for a licence whose terms we have
        # actually read. The Copernicus licence is the live example: no SPDX identifier, so the
        # lookup can only say "unknown", but its text plainly permits commercial use.
        declared_commercial = declared.get("commercial_use")
        if isinstance(declared_commercial, bool):
            commercial_use: bool | None = declared_commercial
        elif identifier is not None:
            commercial_use = _commercial_use_for(identifier)
        else:
            commercial_use = _commercial_use_for_name(name)

        return DatasetLicence(identifier=identifier, name=name, url=url, commercial_use=commercial_use)

    return UNDECLARED


def most_restrictive(licences: list[DatasetLicence]) -> DatasetLicence:
    """The licence a product derived from these inputs must carry.

    Ties keep the first, so a single-input derivation returns that input's licence unchanged
    rather than a synthesised equivalent.
    """
    if not licences:
        return UNDECLARED
    return max(licences, key=lambda licence: licence.restrictiveness)


def refuses_publication(declared: DatasetLicence, inputs: list[DatasetLicence]) -> str | None:
    """Why publishing `declared` for a product derived from `inputs` must be refused, or None.

    The rule from CLIM-946: a derived product may not claim a licence more permissive than any
    of its inputs. Refused rather than warned about, because the failure is silent and the
    output is a derivative work — a warning in a log is not a defence.
    """
    if not inputs:
        return None
    strictest = most_restrictive(inputs)
    if declared.is_at_least_as_restrictive_as(strictest):
        return None
    return (
        f"Derived dataset declares licence {declared.label!r} (commercial use: "
        f"{_describe(declared.commercial_use)}), which is more permissive than its input "
        f"{strictest.label!r} (commercial use: {_describe(strictest.commercial_use)}). "
        f"A derived product is a derivative work and inherits the most restrictive licence "
        f"among its inputs."
    )


def _describe(commercial_use: bool | None) -> str:
    if commercial_use is None:
        return "not known"
    return "allowed" if commercial_use else "not allowed"
