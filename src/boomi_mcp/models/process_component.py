"""Typed per-root process component contracts (issue #153 / M12.15).

A ``ProcessIRV1`` is a semantic process graph and nothing else — it carries no
name, no folder, no conflict policy and no raw XML (ADR-001; #153 keeps that
boundary). Everything apply needs to turn one compiled root into a real Boomi
``Component`` therefore lives here, in a typed envelope that travels ALONGSIDE
the IR rather than inside it.

The cardinality is exactly one envelope per root and one root per unit
(:class:`ProcessAuthoringUnitV1`): no root applies without an envelope, and no
envelope covers more than one root.

**Strictness.** These models are strict and frozen (``extra="forbid"``,
``frozen=True``), unlike the legacy, mutable, extra-tolerant
:class:`~boomi_mcp.models.integration_models.IntegrationSpecV1`. They are a new
surface with no existing callers, so there is no compatibility argument for
tolerance, and an unknown key here would silently drop a caller's override
declaration — the failure mode ``PROCESS_EXTENSIONS_INVALID`` already exists to
prevent on the legacy side.

**Relationship to the legacy extension reader.** The validation rules below
mirror ``process_flow_builder._extract_process_extension_connections`` exactly,
because that function's output is the byte-authority for
``<bns:processOverrides>`` and this slice must stay byte-identical to it (the
legacy path remains the parity oracle until #160). Two of its asymmetries are
deliberate and are reproduced verbatim:

* ``id`` is canonicalized (stripped) because it is structural; ``label`` is NOT
  stripped, because a leading/trailing space is cosmetic and the value is
  XML-escaped on emission anyway.
* ``xpath`` is mandatory ONLY for an explicitly ``database`` connection
  override, which is xpath-keyed. An id-keyed (REST) override is valid without
  it, and without declaring ``connector_type`` at all.

Where the two differ is whitespace TOLERANCE, and only at the outer edge: the
legacy reader accepts ``" $ref:X "`` and strips it, while
:data:`~boomi_mcp.models.process_ir.ComponentRefV1` rejects surrounding
whitespace outright. That is intentional — the typed surface is strict and the
legacy adapter normalizes before constructing these models, so no legacy input
changes behaviour.
"""

from __future__ import annotations

from typing import Literal, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_core import PydanticCustomError

from .process_ir import ComponentRefV1, ProcessIRV1
from .recipe_contributions import RecipeComponentKey

#: Fields safe to expose in a repr: discriminators only. Mirrors
#: ``models.process_ir._REPR_SAFE_FIELDS`` and exists for the same reason — an
#: envelope carries caller-authored strings (name, description, override labels)
#: and a repr that echoed them could surface authored values in a traceback or a
#: served diagnostic. Secrets/security is a blocking class in this repository.
_REPR_SAFE_FIELDS = frozenset({"action", "version", "component_key"})


class _ProcessComponentModel(BaseModel):
    """Shared strict base: unknown fields rejected, authored values suppressed."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    def __repr_args__(self):  # noqa: D105 — pydantic hook
        for key, value in super().__repr_args__():
            if key in _REPR_SAFE_FIELDS:
                yield key, value
            else:
                yield key, "..."


def _require_unpadded(value: str, what: str) -> str:
    """Reject a blank or whitespace-padded structural string."""
    if not value or value != value.strip():
        raise PydanticCustomError(
            "process_component_value_invalid",
            "{what} must be non-blank and carry no surrounding whitespace",
            {"what": what},
        )
    return value


class ProcessOverrideFieldV1(_ProcessComponentModel):
    """One overrideable connection field in a process extension declaration."""

    id: str
    label: str
    xpath: Optional[str] = None

    @field_validator("id")
    @classmethod
    def _check_id(cls, value: str) -> str:
        # Structural: canonicalized by the legacy reader, so require the
        # canonical form here rather than silently accepting a padded variant
        # that would compare unequal to the same field authored cleanly.
        return _require_unpadded(value, "field id")

    @field_validator("label")
    @classmethod
    def _check_label(cls, value: str) -> str:
        # Deliberately NOT stripped — see the module docstring. Only a blank
        # label is refused, exactly as the legacy reader refuses it.
        if not value.strip():
            raise PydanticCustomError(
                "process_component_value_invalid",
                "field label must not be blank",
            )
        return value

    @field_validator("xpath")
    @classmethod
    def _check_xpath(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _require_unpadded(value, "field xpath")


class ProcessConnectionOverrideV1(_ProcessComponentModel):
    """Environment-extension overrides declared for ONE connection."""

    connection_id: ComponentRefV1
    connector_type: Optional[str] = None
    fields: Tuple[ProcessOverrideFieldV1, ...]

    @field_validator("connector_type")
    @classmethod
    def _check_connector_type(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        _require_unpadded(value, "connector_type")
        # Case-folded for canonical comparison. Byte-safe: `connector_type` is
        # carried for downstream tooling and is NEVER emitted into the override
        # declaration (Boomi keys overrides by connection id + field id), so
        # folding it cannot move a single emitted byte — it only makes the
        # database rule below, and the plan fingerprint, insensitive to spelling.
        return value.lower()

    @field_validator("fields")
    @classmethod
    def _check_fields_present(
        cls, value: Tuple[ProcessOverrideFieldV1, ...]
    ) -> Tuple[ProcessOverrideFieldV1, ...]:
        if not value:
            raise PydanticCustomError(
                "process_component_cardinality_invalid",
                "a connection override must declare at least one field",
            )
        # Field ORDER is preserved, never sorted: `_emit_process_overrides`
        # emits fields in input order, so a reorder here would move emitted
        # bytes. Duplicate ids are likewise NOT rejected — the legacy renderer
        # preserves them and parity outranks tidiness.
        return value

    @model_validator(mode="after")
    def _check_database_xpath(self) -> "ProcessConnectionOverrideV1":
        if self.connector_type != "database":
            return self
        missing = [field.id for field in self.fields if field.xpath is None]
        if missing:
            raise PydanticCustomError(
                "process_component_value_invalid",
                "a database connection override is xpath-keyed, so every field "
                "requires an xpath; missing on: {missing}",
                {"missing": ", ".join(missing)},
            )
        return self


class ProcessExtensionBindingsV1(_ProcessComponentModel):
    """Typed process-extension / override bindings for one process root."""

    connections: Tuple[ProcessConnectionOverrideV1, ...] = ()

    @field_validator("connections")
    @classmethod
    def _check_connections(
        cls, value: Tuple[ProcessConnectionOverrideV1, ...]
    ) -> Tuple[ProcessConnectionOverrideV1, ...]:
        # Connection ORDER is preserved for the same byte-parity reason as
        # field order. An empty tuple is a valid no-op, matching the legacy
        # reader's treatment of an explicitly empty `connections` list.
        return value


class ProcessComponentEnvelopeV1(_ProcessComponentModel):
    """Everything apply needs about a process root that is not its semantics.

    ``name`` and ``action`` are REQUIRED caller-supplied fields in this slice,
    with no defaults: #157 later adds prefix-derived default names, and until it
    does, guessing either one would let a caller create or overwrite a component
    they did not name. There is deliberately no ``folder_id`` — placement is
    authored by NAME and resolved to an account-bound id at apply time, so the
    public contract stays relocatable.
    """

    component_key: RecipeComponentKey
    name: str
    action: Literal["create", "update"]
    component_id: Optional[str] = None
    description: str = ""
    folder_name: Optional[str] = None
    depends_on: Tuple[RecipeComponentKey, ...] = ()
    process_extensions: ProcessExtensionBindingsV1 = Field(
        default_factory=ProcessExtensionBindingsV1
    )

    @field_validator("name")
    @classmethod
    def _check_name(cls, value: str) -> str:
        # The legacy assembler already refuses a blank process name
        # (PROCESS_XML_VALIDATION_FAILED); refusing it here means the caller
        # learns at authoring time instead of mid-apply.
        #
        # ...and padding is refused on the same footing as every other
        # structural string in this module (§6 AR2-06). It previously accepted
        # `"  N  "` verbatim, which is worse than either alternative: the name
        # is fingerprint-covered AND emitted into the component XML, so two
        # spellings the plan defines as ONE canonical envelope minted different
        # plan fingerprints and different submitted bytes. Rejecting rather
        # than silently trimming is the recorded deviation from the plan's
        # "trim once" letter — see the ledger row: it is the module's
        # established idiom, fail-closed, and every accepted value therefore
        # already equals its trimmed form.
        return _require_unpadded(value, "process component name")

    @field_validator("component_id", "folder_name")
    @classmethod
    def _check_optional_unpadded(cls, value: Optional[str], info) -> Optional[str]:
        if value is None:
            return None
        return _require_unpadded(value, str(info.field_name))

    @field_validator("depends_on")
    @classmethod
    def _check_depends_on(cls, value: Tuple[str, ...]) -> Tuple[str, ...]:
        if len(set(value)) != len(value):
            duplicates = sorted({key for key in value if value.count(key) > 1})
            raise PydanticCustomError(
                "process_component_duplicate_dependency",
                "depends_on lists the same key more than once: {duplicates}",
                {"duplicates": ", ".join(duplicates)},
            )
        # Dependency declaration is SET semantics — the topological sorter reads
        # it as an unordered edge set — so canonicalize to sorted order. That is
        # what makes a permuted `depends_on` fingerprint-equivalent instead of
        # minting a different plan for the same graph.
        return tuple(sorted(value))

    @model_validator(mode="after")
    def _check_no_self_dependency(self) -> "ProcessComponentEnvelopeV1":
        if self.component_key in self.depends_on:
            raise PydanticCustomError(
                "process_component_self_dependency",
                "process '{key}' cannot depend on itself",
                {"key": self.component_key},
            )
        return self


class ProcessAuthoringUnitV1(_ProcessComponentModel):
    """Exactly ONE process root plus exactly ONE envelope describing it.

    The invariant is carried by the shape rather than by a check: there is no
    root list and no optional envelope, so a unit cannot express "no envelope"
    or "two roots" in the first place.
    """

    envelope: ProcessComponentEnvelopeV1
    process_ir: ProcessIRV1


__all__ = [
    "ProcessAuthoringUnitV1",
    "ProcessComponentEnvelopeV1",
    "ProcessConnectionOverrideV1",
    "ProcessExtensionBindingsV1",
    "ProcessOverrideFieldV1",
]
