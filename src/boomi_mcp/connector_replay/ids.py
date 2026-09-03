"""Identifier grammars for the connector-replay evidence registry.

This module OWNS the identifier languages the registry validates and the capture
tooling produces. It is deliberately the lowest layer of ``connector_replay`` and
imports nothing from ``boomi_mcp.compiler`` or ``boomi_mcp.categories`` at module
load, so the registry can be packaged and loaded without dragging the authoring
stack in behind it.

Ownership matters more than convenience here. The Boomi component-id language was
spelled out by hand in two places before this module existed — once with a
lowercase class plus ``re.IGNORECASE`` and once with an explicit ``a-fA-F`` class —
two spellings of exactly the same language, each free to drift from the other. A
third spelling added here would have made it three. Both existing sites now import
:data:`BOOMI_COMPONENT_ID_PATTERN` from this module, which is why the import arrow
points *into* the authoring stack and never back out of it.

The execution-id grammar earns its strictness from two negatives the platform
itself supplies, and both are the kind a hand-written fixture reaches for first:

* ``execution-110b23f4-567a-8d90-1234-56789e0b123d`` is the platform's own
  documented example for ``ExecutionRecord.executionId`` — and it is UNDATED.
  No real execution id looks like this; every one of the 57 captured under this
  issue carries a trailing ``-YYYY.MM.DD``. A fixture pasted from the API
  documentation would therefore be accepted by any grammar loose enough to make
  the date optional, and would prove nothing about the ids the platform emits.
* ``executionrecord-110b23f4-...`` is a DIFFERENT object's id that shares the
  leading characters. A prefix test written as ``startswith("execution")``
  accepts it.

Both are rejected here, and the test module pins both directions.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Final

__all__ = [
    "BOOMI_COMPONENT_ID_PATTERN",
    "BOOMI_COMPONENT_ID_RE",
    "EVIDENCE_COMPONENT_ID_PATTERN",
    "EVIDENCE_COMPONENT_ID_RE",
    "is_evidence_component_id",
    "EXECUTION_ID_PATTERN",
    "EXECUTION_ID_RE",
    "is_boomi_component_id",
    "is_execution_id",
]


#: Matched with ``fullmatch``, not ``match``. A trailing ``$`` is NOT a whole-string
#: anchor in Python: it also matches immediately before a final newline, so
#: ``match`` accepted an otherwise-valid id with a newline glued to it — a value
#: that is not the grammar this module documents, and one an evidence row could
#: then cite. The anchors are kept as well, so the pattern remains correct for a
#: caller that uses it directly.
#:
#: The Boomi component-id language: a lowercase-or-uppercase hex UUID in canonical
#: 8-4-4-4-12 form. Anchored at both ends — an id is the whole string, never a
#: substring of one, so a value carrying trailing whitespace or an appended
#: fragment is rejected rather than silently truncated to its leading match.
#: TWO grammars, because two consumers ask different questions of the same shape.
#:
#: This one — the general Boomi component-id language — is CASE-INSENSITIVE. It
#: answers "is this a well-formed component reference?", which is what a caller
#: supplying a certificate or profile reference needs, and UUIDs are case-insensitive
#: by definition. A previous review round established that explicitly, with a test.
#:
#: A single lowercase-only pattern was tried here and was WRONG: the evidence behind
#: it — every id in the captured archive is lowercase — comes from the replay
#: archive, which is a different consumer with a different question. Tightening the
#: shared pattern on one consumer's evidence broke the other, and the suite caught
#: it. One consumer's evidence is not automatically another's contract.
BOOMI_COMPONENT_ID_PATTERN: Final[str] = (
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

#: The STRICTER grammar, for what an evidence row may CITE. Not "is this a valid
#: id?" but "is this an id the platform was OBSERVED to mint?" — a narrower question
#: with a fail-closed answer, because a citation is a claim about something that
#: happened. All 72 archived ids are lowercase, with zero exceptions.
EVIDENCE_COMPONENT_ID_PATTERN: Final[str] = (
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)

EVIDENCE_COMPONENT_ID_RE: Final[re.Pattern[str]] = re.compile(EVIDENCE_COMPONENT_ID_PATTERN)

BOOMI_COMPONENT_ID_RE: Final[re.Pattern[str]] = re.compile(BOOMI_COMPONENT_ID_PATTERN)


#: The Boomi execution-id language: the literal ``execution-``, a hex UUID, then a
#: dotted calendar date. The date is REQUIRED — see the module docstring for why
#: making it optional would admit the platform's own documentation example. The
#: leading segment is matched as ``execution-`` immediately followed by a hex
#: digit, which is what separates it from ``executionrecord-``: that value's next
#: character after the shared prefix is ``r``, not a hex digit.
#:
#: The date is validated as a REAL CALENDAR DAY, and this reverses an earlier
#: decision recorded here. That decision argued a recogniser should not assert an
#: authority it lacks, and would accept 2026.02.30 on the grounds that the platform
#: mints these strings and this module only reads them.
#:
#: The argument was wrong for this particular grammar, for two reasons. The plan of
#: record requires the identifier formats to be CLOSED and evidence-derived, and the
#: evidence is unanimous: all 72 archived execution ids carry a real calendar day.
#: And the asymmetry runs the other way here — this grammar guards what an evidence
#: row may CITE, so accepting a value the platform cannot mint admits a fabricated
#: citation, which is worse than rejecting a real id that has never been observed to
#: exist.
EXECUTION_ID_PATTERN: Final[str] = (
    r"^execution-"
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    r"-(?P<year>[0-9]{4})\.(?P<month>[0-9]{2})\.(?P<day>[0-9]{2})$"
)

EXECUTION_ID_RE: Final[re.Pattern[str]] = re.compile(EXECUTION_ID_PATTERN)


def is_boomi_component_id(value: object) -> bool:
    """Return True when ``value`` is a Boomi component id.

    Non-strings are False rather than a TypeError: this is a recogniser used to
    classify values arriving from JSON and from the platform, where a null or a
    number is a thing that happens and is simply not an id.
    """
    return isinstance(value, str) and BOOMI_COMPONENT_ID_RE.fullmatch(value) is not None


def is_evidence_component_id(value: object) -> bool:
    """Whether an EVIDENCE ROW may cite this component id.

    Stricter than :func:`is_boomi_component_id` on purpose — see
    :data:`EVIDENCE_COMPONENT_ID_PATTERN`. A reference a caller supplies is
    validated for well-formedness; a citation in an evidence row is validated
    against what the platform has been observed to produce.
    """
    return isinstance(value, str) and EVIDENCE_COMPONENT_ID_RE.fullmatch(value) is not None


def is_execution_id(value: object) -> bool:
    """Return True when ``value`` is a Boomi execution id.

    See :data:`EXECUTION_ID_PATTERN` — the trailing date is required, so the
    platform's undated documentation example is rejected here on purpose.
    """
    if not isinstance(value, str):
        return False
    match = EXECUTION_ID_RE.fullmatch(value)
    if match is None:
        return False
    try:
        date(int(match["year"]), int(match["month"]), int(match["day"]))
    except ValueError:
        return False
    return True


#: One grammar-safe identifier segment. The alphabet is closed and lowercase, so
#: a segment can never carry the delimiter and an identifier can never be
#: ambiguous about where its parts divide.
CONTRACT_ID_SEGMENT: Final[str] = r"[a-z0-9_]+"

#: The canonical idempotency-contract identifier:
#: ``icv1:<family_id>:<action_id>:<semantics_id>:<revision>``.
#:
#: The version tag is part of the identifier rather than metadata beside it,
#: because a consumer holding only the string must be able to tell which grammar
#: produced it. The revision excludes zero and leading zeros so one revision has
#: exactly one spelling — otherwise ``:1`` and ``:01`` are two references to one
#: contract, and a set of references stops being a set.
#:
#: DERIVED FROM ABSTRACT SEMANTICS ONLY: no account, no component GUID, no
#: capture identity appears in any segment, so the same abstract contract derives
#: the same identifier in every account. That is what lets a relocatable artifact
#: carry a reference at all.
CANONICAL_CONTRACT_ID_PREFIX: Final[str] = "icv1"
CANONICAL_CONTRACT_ID_BODY: Final[str] = (
    rf"{CANONICAL_CONTRACT_ID_PREFIX}:{CONTRACT_ID_SEGMENT}:{CONTRACT_ID_SEGMENT}"
    rf":{CONTRACT_ID_SEGMENT}:[1-9][0-9]*"
)
CANONICAL_CONTRACT_ID_PATTERN: Final[str] = rf"^{CANONICAL_CONTRACT_ID_BODY}(?![\s\S])"
_CANONICAL_CONTRACT_ID_RE: Final = re.compile(CANONICAL_CONTRACT_ID_PATTERN)


def is_canonical_contract_id(value: object) -> bool:
    """Whether ``value`` is a well-formed bare canonical contract identifier."""
    return isinstance(value, str) and _CANONICAL_CONTRACT_ID_RE.fullmatch(value) is not None


def canonical_contract_id(
    family_id: str, action_id: str, semantics_id: str, revision: int
) -> str:
    """Build the canonical identifier, or refuse.

    THE ONLY CONSTRUCTOR. Every surface that needs one calls this rather than
    formatting the parts itself, because a second formatter is how a mint and a
    validator come to disagree about a string they both claim to own — and a
    reference that mints but does not validate is a contract nobody can use.

    Refuses rather than emitting something the grammar would reject: a segment
    that is not grammar-safe here becomes an unparseable reference downstream,
    where the part that produced it is no longer in view.
    """
    parts = {"family_id": family_id, "action_id": action_id,
             "semantics_id": semantics_id}
    for name, value in parts.items():
        if not isinstance(value, str) or not re.fullmatch(CONTRACT_ID_SEGMENT, value):
            raise ValueError(
                f"{name}={value!r} is not a grammar-safe contract identifier "
                f"segment ({CONTRACT_ID_SEGMENT}); a fold that produces one must "
                "be recorded in the registry vocabulary, never applied here"
            )
    if not isinstance(revision, int) or isinstance(revision, bool) or revision < 1:
        raise ValueError(
            f"revision={revision!r} must be a positive integer; the grammar "
            "admits no zero and no leading zero, so one revision has one spelling"
        )
    built = f"{CANONICAL_CONTRACT_ID_PREFIX}:{family_id}:{action_id}:{semantics_id}:{revision}"
    # The constructor grades its own output against the published rule. Without
    # this the two could drift apart silently on any edit to either.
    assert is_canonical_contract_id(built), built
    return built


def authored_contract_ref(
    family_id: str, action_id: str, semantics_id: str, revision: int
) -> str:
    """The authored ``$ref:`` form of the canonical identifier."""
    return AUTHORED_CONTRACT_REF_TOKEN_PREFIX + canonical_contract_id(
        family_id, action_id, semantics_id, revision
    )


#: The authored form of an idempotency-contract reference: the ``$ref:`` token
#: followed by the canonical identifier. Owned here, beside the other identifier
#: grammars, because several consumers need it — the authoring model that accepts
#: documents, the compiler symbol that resolves them, the registry mint, the
#: compile-time grant check and the discovery surface — and a second hand-written
#: copy is how an authoring surface and its compiler drift into disagreeing about
#: what a reference IS.
AUTHORED_CONTRACT_REF_TOKEN_PREFIX: Final[str] = "$ref:"
# Ends with a negative lookahead rather than `$`. Measured against a real
# Draft 2020-12 validator: a `$`-anchored pattern ACCEPTS "$ref:ABC\n",
# because `$` may match before a final line terminator — so a client whose
# input validated against the served schema was then refused at parse time.
# `(?![\s\S])` means "no character follows" in both ECMA-262 and Python, so
# the published rule and the enforced one accept the same strings.
AUTHORED_CONTRACT_REF_PATTERN: Final[str] = rf"^\$ref:{CANONICAL_CONTRACT_ID_BODY}(?![\s\S])"
_AUTHORED_CONTRACT_REF_RE: Final = re.compile(AUTHORED_CONTRACT_REF_PATTERN)


def is_authored_contract_ref(value: object) -> bool:
    """Whether ``value`` is a well-formed authored contract reference."""
    # `fullmatch`, not `match`: `$` matches before a trailing newline, so a
    # value ending in one would be accepted here while the served JSON-Schema
    # pattern still declares it invalid — the exact trap this module documents
    # for its other identifier grammars.
    return isinstance(value, str) and _AUTHORED_CONTRACT_REF_RE.fullmatch(value) is not None


#: A fixed vocabulary the grammar's BEHAVIOUR is fingerprinted over. The pattern
#: string alone is not the contract: a change from `match` to `fullmatch` altered
#: what the server accepts while every revision hash stood still, because the
#: string had not moved. These probes exercise the discriminating shapes, so a
#: change to HOW the pattern is applied moves the revision even when the pattern
#: itself does not.
AUTHORED_CONTRACT_REF_PROBES: Final[tuple] = (
    "$ref:OK",
    "$ref:a.b-c_d",
    "$ref:ABC\n",
    "$ref:ABC\r\n",
    "$ref:has:colon",
    "$ref:has space",
    "$ref:",
    "literal-component-id",
    "",
)


def authored_contract_ref_behaviour() -> tuple:
    """The grammar's verdict on each probe, as fingerprintable data."""
    return tuple(
        (probe, is_authored_contract_ref(probe)) for probe in AUTHORED_CONTRACT_REF_PROBES
    )

