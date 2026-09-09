"""Typed governance intent and retirement vocabulary (issue #157 / M12.19).

Two things live here, both deliberately OUTSIDE ``ProcessIRV1``, which stays a
semantic process graph and nothing else (ADR-001; #153 kept that boundary and
#157 extends the ENVELOPE, never the IR):

* **Recorded-not-wired declarations.** A caller may state an operational
  intent — a watermark strategy, a runtime hint — that this server records and
  serves back but does not wire into any emitted byte or platform mutation.
  :class:`RecordedIntentV1` is that record. Its ``status`` is a literal, so a
  record claiming to be wired cannot be constructed; the authoring workflow
  keeps every record OUTSIDE the semantic/plan/compile hashes and outside every
  mutation payload, and a paired test pins that only the served record moves.

* **Retired legacy spellings.** The M12 triage (plan §4) retires the inert
  legacy metadata spellings — the ones that never affected an emitted byte, a
  fingerprint, a mutation or a validation verdict on the legacy chain. Each
  retirement is proven by a frozen baseline pair under
  ``tests/fixtures/governance/issue_157/retirements/`` and then REFUSED on the
  typed surface with a named diagnostic, rather than silently accepted and
  ignored. :data:`RETIRED_SPELLINGS` is the single in-code authority for that
  vocabulary; a test pins it equal to the fixture index in both directions.

The watermark declaration carries the M18 RULE half only: the consistency rules
the legacy archetype enforced (a watermark-sourced query parameter needs a
declaration; the tracked field must be a declared source field) stay
validation-bearing. The BINDING half — wiring the watermark into a REST query
parameter — stays deferred past M12 cutover and is owned by neither #155 nor
this issue.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Annotated, Any, Literal, Mapping, Optional, Tuple, Union

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator
from pydantic_core import PydanticCustomError

from .process_ir import ComponentRefV1

NonEmptyString = Annotated[str, StringConstraints(min_length=1)]

#: Fields safe to show in a repr: discriminators and closed literals only. A
#: recorded intent carries caller-authored strings (a hint value, an initial
#: watermark value) and a repr that echoed them could put an authored value in
#: a traceback or a served diagnostic.
_REPR_SAFE_FIELDS = frozenset(
    {"declaration_kind", "status", "kind", "persistence", "hint_kind", "process_key"}
)


class _GovernanceModel(BaseModel):
    """Strict, frozen, repr-redacted — the same posture as the envelope models."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    def __repr_args__(self):  # noqa: D105 — pydantic hook
        for key, value in super().__repr_args__():
            if key in _REPR_SAFE_FIELDS:
                yield key, value
            else:
                yield key, "..."


def _require_unpadded(value: str, what: str) -> str:
    if not value or value != value.strip():
        raise PydanticCustomError(
            "governance_recorded_intent_invalid",
            "{what} must be non-blank and carry no surrounding whitespace",
            {"what": what},
        )
    return value


# ---------------------------------------------------------------------------
# Recorded-not-wired declarations
# ---------------------------------------------------------------------------


class WatermarkDeclarationV1(_GovernanceModel):
    """The typed watermark declaration — the SOLE accepted watermark spelling.

    RULE half of M18 only. ``persistence`` names the mechanism the canonical
    graph already owns (``SetDppNodeV1.persist`` persists a dynamic process
    property across executions); this declaration does not insert that node, and
    it binds no REST query parameter. ``query_parameter_refs`` are names the
    caller intends to bind LATER — recorded so the consistency rule can be
    checked, never wired.
    """

    declaration_kind: Literal["watermark"] = "watermark"
    source_profile_ref: ComponentRefV1
    field: NonEmptyString
    kind: Literal["timestamp", "sequence"]
    persistence: Literal["process_property"] = "process_property"
    initial_value: Optional[str] = None
    query_parameter_refs: Tuple[NonEmptyString, ...] = ()

    @field_validator("field")
    @classmethod
    def _check_field(cls, value: str) -> str:
        return _require_unpadded(value, "watermark field")

    @field_validator("query_parameter_refs")
    @classmethod
    def _check_refs(cls, value: Tuple[str, ...]) -> Tuple[str, ...]:
        for item in value:
            _require_unpadded(item, "watermark query parameter name")
        if len(set(value)) != len(value):
            raise PydanticCustomError(
                "governance_recorded_intent_invalid",
                "query_parameter_refs lists the same name more than once",
            )
        return value


class RuntimeHintDeclarationV1(_GovernanceModel):
    """A deliberate, non-executable runtime hint (atom selection, environment tag).

    The typed successor of the archetype's free-form ``naming.runtime_hints``
    echo: a CLOSED kind plus one value, recorded and served back, never read by
    any builder. Secret-shaped content is refused before the record is made.
    """

    declaration_kind: Literal["runtime_hint"] = "runtime_hint"
    hint_kind: Literal["atom_selection", "environment_tag", "note"]
    value: NonEmptyString

    @field_validator("value")
    @classmethod
    def _check_value(cls, value: str) -> str:
        return _require_unpadded(value, "runtime hint value")


RecordedIntentDeclarationV1 = Annotated[
    Union[WatermarkDeclarationV1, RuntimeHintDeclarationV1],
    Field(discriminator="declaration_kind"),
]

#: DERIVED from the union, never hand-listed, so a served list of declaration
#: kinds cannot drift from what the union accepts.
RECORDED_INTENT_DECLARATION_KINDS: Tuple[str, ...] = tuple(
    member.model_fields["declaration_kind"].default
    for member in (WatermarkDeclarationV1, RuntimeHintDeclarationV1)
)

RECORDED_NOT_WIRED = "recorded_not_wired"


class RecordedIntentV1(_GovernanceModel):
    """One served, recorded-not-wired declaration bound to one process root.

    ``status`` is a single-valued literal on purpose: there is no ``wired``
    state a record could be promoted to. When a declaration becomes executable
    it becomes a ProcessIR node or an envelope binding, not a status flip here.
    """

    intent_id: NonEmptyString
    process_key: NonEmptyString
    declaration: RecordedIntentDeclarationV1
    status: Literal["recorded_not_wired"] = RECORDED_NOT_WIRED

    @field_validator("intent_id", "process_key")
    @classmethod
    def _check_ids(cls, value: str, info) -> str:
        return _require_unpadded(value, str(info.field_name))

    @property
    def sort_key(self) -> Tuple[str, str]:
        return (self.process_key, self.intent_id)


# ---------------------------------------------------------------------------
# Retired legacy spellings
# ---------------------------------------------------------------------------

#: Retired spelling -> retirement record id. THE authority for the vocabulary
#: the typed surface refuses. Each id names a record under
#: ``tests/fixtures/governance/issue_157/retirements/<id>.json`` carrying the
#: legacy-baseline mutation proof (equal emitted bytes, fingerprints, mutation
#: trace and verdict for a pair differing only in that field). A test pins this
#: mapping equal to the fixture index in both directions, so a spelling cannot
#: be refused without its proof and a proof cannot exist for an unrefused
#: spelling.
#:
#: Keys are the LEGACY spellings as authored on the archetype contract or the
#: primitive fragment; values are stable retirement ids. The typed surface
#: never carries these keys, so the check is a refusal at intake, not a
#: mapping into anything.
RETIRED_SPELLINGS: Mapping[str, str] = MappingProxyType(
    {
        # DB connection create settings echoed but never read by any builder.
        "jdbc_options": "RET-157-01",
        # DB read operation knobs recorded as deferred intent, never emitted.
        "fetch_size": "RET-157-02",
        "link_element": "RET-157-03",
        # DB read parameter typing recorded as deferred intent, never emitted.
        # (`direction` is NOT here: measured, it sets the read profile's
        # `mappable` flag and moves the emitted profile XML — RETAINED.)
        "sql_type": "RET-157-04",
        # Intent-only scheduling: a cron echo that activates no Boomi schedule.
        # Schedule activation is a future topology capability.
        "cron": "RET-157-06",
        "run_metadata": "RET-157-07",
        # The archetype's free-form runtime hints, echoed verbatim under
        # spec.runtime and read by nothing; the typed successor is the
        # recorded-not-wired RuntimeHintDeclarationV1.
        "runtime_hints": "RET-157-09",
        # The legacy watermark FRAGMENT's persistence spellings. The watermark
        # itself is SPLIT, not retired: WatermarkDeclarationV1 is the sole
        # accepted spelling and SetDppNodeV1.persist the executable mechanism.
        "dpp_name": "RET-157-10",
        "store_ref": "RET-157-10",
    }
)


def retired_spellings_in(value: Any, *, path: str = "") -> Tuple[Tuple[str, str], ...]:
    """Every retired spelling used as a KEY anywhere in ``value``.

    Returns ``(json_pointer_path, spelling)`` pairs in document order. Walks
    mappings and sequences; values are never inspected, only key names, so a
    caller's authored text cannot trip it.
    """
    found = []
    if isinstance(value, Mapping):
        for key, sub in value.items():
            here = "{0}/{1}".format(path, key)
            if isinstance(key, str) and key in RETIRED_SPELLINGS:
                found.append((here, key))
            found.extend(retired_spellings_in(sub, path=here))
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            found.extend(retired_spellings_in(item, path="{0}/{1}".format(path, index)))
    return tuple(found)


def reject_retired_spellings(value: Any, *, path: str = "") -> None:
    """Raise the named refusal when ``value`` carries a retired spelling as a key."""
    found = retired_spellings_in(value, path=path)
    if found:
        raise PydanticCustomError(
            "governance_retired_spelling",
            "retired legacy metadata spelling at {path}; it never affected an "
            "emitted byte, a fingerprint, a mutation or a validation verdict, "
            "and the typed surface refuses it rather than ignoring it",
            {"path": found[0][0]},
        )


__all__ = [
    "RECORDED_INTENT_DECLARATION_KINDS",
    "RECORDED_NOT_WIRED",
    "RETIRED_SPELLINGS",
    "RecordedIntentDeclarationV1",
    "RecordedIntentV1",
    "RuntimeHintDeclarationV1",
    "WatermarkDeclarationV1",
    "reject_retired_spellings",
    "retired_spellings_in",
]
