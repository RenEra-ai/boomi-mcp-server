"""M11.1 (issue #120, epic #118): typed cache/property authoring vocabulary.

Single source of truth for the cache/property/enrichment intent terms that
M11 children make executable incrementally: #121 (M11.2) ships the DDP/DPP
Set Properties emitters, #131 (M11.7) the Process Property component builder,
#122 (M11.3) the Document Cache put/get/join authoring, #123 (M11.4) the
lineage validation that consumes these models.

This module deliberately contains NO emitters and NO Boomi mutation paths —
declaring a term here does not make it executable. Executability is
advertised per-term by the ``cache_property_authoring`` schema surface in
``meta_tools.py`` and enforced by the process/component builders.

Field contracts are grounded in the #119 census captures
(``tests/fixtures/live_xml/m11/``): ``static``/``current``/``profile``/
``ddp``/``dpp`` source shapes are live-verified; ``definedparameter`` (read a
Process Property component field) is companion-documented only and stays
gated until a verified wire shape exists.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Literal, Mapping, Optional

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, model_validator

# Where a property value lives at runtime: per-document (DDP), per-execution
# (DPP), or in a standalone Process Property component (deploy-time defaults,
# runtime-readable).
PropertyScope = Literal["ddp", "dpp", "processproperty"]

# Property-side authoring intents. set_ddp / set_dpp lower to the Set
# Properties (documentproperties) shape (#121); get_property reads a property
# value into a flow (map function today; Set Properties definedparameter
# source when verified); set_process_property writes INTO a Process Property
# component slot at runtime (companion-documented propertyId
# 'definedprocess.<componentId>@<propertyKey>' — gated until verified).
PropertyOperationKind = Literal[
    "set_ddp",
    "set_dpp",
    "get_property",
    "set_process_property",
]

# Cache-side authoring intents. cache_put lowers to doccacheload (#122),
# cache_get to doccacheretrieve (#122), cache_join to a map-time
# DocumentCacheJoins entry (#122, live-captured shape).
CacheOperationKind = Literal["cache_put", "cache_get", "cache_join"]

PropertySourceValueType = Literal[
    "static",
    "current",
    "profile",
    "ddp",
    "dpp",
    "definedparameter",
]

# value_type -> (required fields, allowed optional fields). Kept as data so
# validators and schema surfaces can render the contract without duplicating
# it.
PROPERTY_SOURCE_FIELD_CONTRACT = {
    "static": (("value",), ()),
    "current": ((), ()),
    "profile": (
        ("element_id", "element_name", "profile_id", "profile_type"),
        (),
    ),
    "ddp": (("property_name",), ("default_value",)),
    "dpp": (("property_name",), ("default_value",)),
    "definedparameter": (
        ("component_id", "property_key"),
        ("component_name", "property_label"),
    ),
}


class PropertySourceValue(BaseModel):
    """One source value feeding a property assignment or cache key.

    Mirrors one ``<parametervalue valueType="...">`` entry. Exactly the
    fields required by ``value_type`` may be set (see
    :data:`PROPERTY_SOURCE_FIELD_CONTRACT`); everything else must stay None.
    """

    model_config = ConfigDict(extra="forbid")

    value_type: PropertySourceValueType = Field(
        ..., description="Source kind (parametervalue valueType)"
    )
    # static
    value: Optional[str] = Field(
        default=None, description="static: literal value (may be empty)"
    )
    # profile
    element_id: Optional[str] = Field(
        default=None, description="profile: profileelement elementId"
    )
    element_name: Optional[str] = Field(
        default=None, description="profile: profileelement elementName"
    )
    profile_id: Optional[str] = Field(
        default=None,
        description="profile: profile componentId literal or '$ref:KEY' token",
    )
    profile_type: Optional[str] = Field(
        default=None, description="profile: profileType (e.g. profile.json)"
    )
    # ddp / dpp
    property_name: Optional[str] = Field(
        default=None,
        description="ddp/dpp: property name WITHOUT the dynamicdocument./process. prefix",
    )
    default_value: Optional[str] = Field(
        default=None, description="ddp/dpp: optional read default"
    )
    # definedparameter (gated: companion-documented wire shape)
    component_id: Optional[str] = Field(
        default=None,
        description="definedparameter: processproperty componentId literal or '$ref:KEY'",
    )
    component_name: Optional[str] = Field(
        default=None, description="definedparameter: component display name"
    )
    property_key: Optional[str] = Field(
        default=None,
        description="definedparameter: the definedProcessProperty key UUID",
    )
    property_label: Optional[str] = Field(
        default=None, description="definedparameter: the property label"
    )

    @model_validator(mode="after")
    def _enforce_field_contract(self) -> "PropertySourceValue":
        required, optional = PROPERTY_SOURCE_FIELD_CONTRACT[self.value_type]
        allowed = set(required) | set(optional)
        for field_name in (
            "value",
            "element_id",
            "element_name",
            "profile_id",
            "profile_type",
            "property_name",
            "default_value",
            "component_id",
            "component_name",
            "property_key",
            "property_label",
        ):
            field_value = getattr(self, field_name)
            if field_name in required and field_value is None:
                raise ValueError(
                    f"value_type='{self.value_type}' requires '{field_name}'"
                )
            if field_name not in allowed and field_value is not None:
                raise ValueError(
                    f"value_type='{self.value_type}' does not accept '{field_name}'"
                )
        return self


class DocumentCacheKeyValue(BaseModel):
    """One cache key binding for a keyed cache operation.

    ``cache_key_id`` references the Document Cache component's ``cacheKey``
    ``id`` attribute (1-based on the wire; 0 silently fails at runtime, hence
    the strict positive bound).
    """

    model_config = ConfigDict(extra="forbid")

    cache_key_id: StrictInt = Field(..., gt=0, description="cacheKey id (non-zero)")
    source: PropertySourceValue = Field(
        ..., description="Value source for this cache key"
    )


class PropertyAssignment(BaseModel):
    """A declared DDP/DPP write intent (one documentproperty entry).

    ``name`` is the bare property name — the emitter owns the
    ``dynamicdocument.`` / ``process.`` prefix and display-name convention.
    ``persist`` is meaningful for DPP only (DDPs always emit persist=false).
    """

    model_config = ConfigDict(extra="forbid")

    scope: PropertyScope = Field(..., description="ddp or dpp (processproperty writes are gated)")
    name: str = Field(..., min_length=1, description="Bare property name (no prefix)")
    source_values: list[PropertySourceValue] = Field(
        ..., min_length=1, description="Ordered source values (concatenated by Boomi)"
    )
    persist: Optional[StrictBool] = Field(
        default=None, description="DPP only: persist the value at atom level"
    )

    @model_validator(mode="after")
    def _persist_only_for_dpp(self) -> "PropertyAssignment":
        if self.persist is not None and self.scope != "dpp":
            raise ValueError("persist is only valid for scope='dpp'")
        if self.scope == "processproperty":
            raise ValueError(
                "scope='processproperty' writes are gated (no verified wire shape); "
                "declare a processproperty component and use map functions to write it"
            )
        return self


# ---------------------------------------------------------------------------
# Process-property scope descriptor (#146 amendment)
# ---------------------------------------------------------------------------

#: The visibility model for a PROCESS PROPERTY, stated here and nowhere else.
#:
#: Kept OUT of ``semantic_validation.lineage.STATE_VISIBILITY_V1`` deliberately.
#: That module models the state a flow establishes and reads DURING an
#: execution — dynamic document properties, dynamic process properties, and the
#: document cache — and a process property is none of those: it is a component
#: with deploy-time defaults, read at runtime through map functions. Pinning it
#: against the lineage traversal would assert an ownership that does not exist,
#: and the resulting parity test would pass by agreeing with a claim neither
#: side actually enforces.
#:
#: The field names match the lineage descriptor's so a caller reads one shape
#: across all four scopes; the VALUES are what differ, and that is the point.
PROCESS_PROPERTY_SCOPE_V1: "Mapping[str, object]" = MappingProxyType(
    {
        "state_scope": "processproperty",
        "scope": "component",
        "lifetime": "deployment",
        # A component-backed value is not carried on the document at all, so
        # copying documents into Branch legs neither preserves nor discards it.
        "survives_branch_leg_entry": True,
        "visible_across_sibling_paths": True,
        # There is no flow-established write to converge, so convergence does
        # not apply — stated explicitly rather than left blank, because an
        # absent field reads as an oversight.
        "convergence": "not_applicable",
        # Writes from inside a flow are gated (no verified wire shape); the
        # value comes from the component's deploy-time configuration instead, so
        # a read never depends on an earlier in-flow write.
        "read_before_write": "not_applicable",
        "authored_writes": "gated",
    }
)


def process_property_scope_row() -> "Mapping[str, object]":
    """The process-property scope descriptor, for the #146 projection."""
    return PROCESS_PROPERTY_SCOPE_V1
