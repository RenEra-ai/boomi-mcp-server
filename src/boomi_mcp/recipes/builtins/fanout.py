"""The built-in DB -> transform -> REST fan-out recipe (issue #145 M12.10).

The native form of what ``compose_archetypes`` builds: one database source, one
shared transform, and 2-25 REST targets behind a terminal Branch, with an
optional Document Cache handoff.

**The cache reference is the whole point of this recipe existing natively.** The
legacy ``flow_sequence`` adapter mints an occurrence-scoped alias per reference,
so the staging ``cache_put`` and each consuming ``cache_get`` end up naming
DIFFERENT symbols — the strict lineage validator then cannot see the writer from
the reader, and the dialect needs a registered ``STANDALONE_CACHE_READ``
exemption to compile at all. This recipe uses ONE stable ``$ref:KEY`` across the
put and every get, so the same strict validator sees the sequential writer and
passes with ``validation_policy=None``. That asymmetry is measured, not assumed:
``tests/patterns/test_recipe_preset_parity.py`` asserts the legacy arm still
fails strictly while this one passes.
"""

from __future__ import annotations

import re
from typing import Optional, Tuple

from pydantic import Field, model_validator
from pydantic_core import PydanticCustomError

from ...models.process_ir import ProcessIRV1
from ...models.recipe_contributions import (
    ComponentContributionV1,
    ConstraintRequirementV1,
    ProcessIRPatchV1,
    RecipeComponentKey,
    RecipeSemanticId,
    RequireCapabilityV1,
    RequireComponentV1,
    RequireProcessV1,
    SetProcessRootV1,
)
from ..contracts import RecipeInputBase
from .sync import SyncComponentSlotV1

_VERSION = "1"

#: Branch cardinality is 2-25 in ProcessIR. A cache handoff consumes one leg for
#: the target-less staging put, so a composition with any cache target can carry
#: at most 24 REST targets. Both bounds are re-enforced by ``BranchNodeV1``; they
#: are stated here too so the failure names the INPUT rather than surfacing as an
#: opaque cardinality error from deep inside the compiler.
_MAX_TARGETS = 25
_MAX_CACHE_TARGETS = 24


class FanoutTargetV1(RecipeInputBase):
    """One REST target leg, in caller order."""

    target_id: RecipeSemanticId
    connection_ref: str = Field(min_length=1)
    operation_ref: str = Field(min_length=1)
    handoff: str = Field(pattern=r"^(document_stream|document_cache)$")
    label: Optional[str] = None


class ComposeDbRestFanoutInputV1(RecipeInputBase):
    """The SAFE projection of a composition's parameters."""

    version: str = Field(pattern=r"^1$")
    process_key: RecipeComponentKey
    source_connection_ref: str = Field(min_length=1)
    source_operation_ref: str = Field(min_length=1)
    map_ref: str = Field(min_length=1)
    map_label: Optional[str] = None
    branch_label: Optional[str] = None
    cache_put_label: Optional[str] = None
    cache_get_label: Optional[str] = None
    cache_ref: Optional[str] = None
    targets: Tuple[FanoutTargetV1, ...] = Field(min_length=2, max_length=_MAX_TARGETS)
    component_slots: Tuple[SyncComponentSlotV1, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _cache_rules(self) -> "ComposeDbRestFanoutInputV1":
        cached = [t for t in self.targets if t.handoff == "document_cache"]
        if cached and self.cache_ref is None:
            raise PydanticCustomError(
                "fanout_cache_ref_required",
                "a document_cache handoff requires cache_ref",
            )
        if not cached and self.cache_ref is not None:
            raise PydanticCustomError(
                "fanout_cache_ref_unused",
                "cache_ref is only valid when a target uses a document_cache handoff",
            )
        if cached and len(self.targets) > _MAX_CACHE_TARGETS:
            raise PydanticCustomError(
                "fanout_cache_target_limit",
                "a cache handoff consumes one Branch leg, so at most 24 targets",
            )
        if len({t.target_id for t in self.targets}) != len(self.targets):
            raise PydanticCustomError(
                "fanout_duplicate_target_id", "target_id must be unique"
            )
        return self


def _root(inp: ComposeDbRestFanoutInputV1) -> ProcessIRV1:
    """source -> map_ref -> terminal Branch, legs in caller target order.

    The staging leg is inserted immediately before the FIRST cache consumer, not
    at position 0: Branch legs run sequentially, so a staging put anywhere before
    the first read satisfies write-before-read, and putting it exactly there is
    what keeps a mixed-mode composition's leg numbering identical to the legacy
    arm's.
    """
    legs: list = []
    staged = False
    for target in inp.targets:
        if target.handoff == "document_cache":
            if not staged:
                legs.append(
                    {
                        "steps": [],
                        "terminal": {
                            "kind": "cache_put",
                            "cache_ref": inp.cache_ref,
                            "label": inp.cache_put_label,
                        },
                    }
                )
                staged = True
            legs.append(
                {
                    "steps": [
                        {
                            "kind": "cache_get",
                            "cache_ref": inp.cache_ref,
                            "label": inp.cache_get_label,
                        }
                    ],
                    "terminal": {
                        "kind": "target",
                        "connection_ref": target.connection_ref,
                        "operation_ref": target.operation_ref,
                        "label": target.label,
                    },
                }
            )
        else:
            legs.append(
                {
                    "steps": [],
                    "terminal": {
                        "kind": "target",
                        "connection_ref": target.connection_ref,
                        "operation_ref": target.operation_ref,
                        "label": target.label,
                    },
                }
            )

    return ProcessIRV1.model_validate(
        {
            "version": "1",
            "body": {
                "kind": "sequence",
                "steps": [
                    {
                        "kind": "source",
                        "connection_ref": inp.source_connection_ref,
                        "operation_ref": inp.source_operation_ref,
                        "label": None,
                    },
                    {
                        "kind": "map_ref",
                        "map_ref": inp.map_ref,
                        "label": inp.map_label,
                    },
                    {"kind": "branch", "label": inp.branch_label, "legs": legs},
                ],
            },
        }
    )


def emit_db_rest_fanout(inp: ComposeDbRestFanoutInputV1) -> Tuple[object, ...]:
    contributions: list = [
        ComponentContributionV1(
            contribution_kind="component_contribution",
            version=_VERSION,
            contribution_id=slot.contribution_id,
            component_key=slot.component_key,
            component_type=slot.component_type,
            materialization_mode=slot.materialization_mode,
            materializer_slot=slot.materializer_slot,
        )
        for slot in inp.component_slots
    ]
    contributions.append(
        ProcessIRPatchV1(
            contribution_kind="process_ir_patch",
            version=_VERSION,
            process_key=inp.process_key,
            operations=(
                SetProcessRootV1(
                    operation_id="op.root",
                    op="set_process_root",
                    slot="root",
                    root=_root(inp),
                ),
            ),
        )
    )
    contributions.append(
        ConstraintRequirementV1(
            contribution_kind="constraint_requirement",
            version=_VERSION,
            requirement_id="req.process",
            requirement=RequireProcessV1(kind="process", process_key=inp.process_key),
        )
    )
    for slot in inp.component_slots:
        contributions.append(
            ConstraintRequirementV1(
                contribution_kind="constraint_requirement",
                version=_VERSION,
                requirement_id=f"req.component.{slot.contribution_id}",
                requirement=RequireComponentV1(
                    kind="component",
                    component_key=slot.component_key,
                    component_type=slot.component_type,
                ),
            )
        )
    contributions.append(
        ConstraintRequirementV1(
            contribution_kind="constraint_requirement",
            version=_VERSION,
            requirement_id="req.capability.branch",
            requirement=RequireCapabilityV1(
                kind="capability",
                authority="process_emitter",
                subject="branch",
                required_state="supported",
            ),
        )
    )
    if inp.cache_ref is not None:
        contributions.append(
            ConstraintRequirementV1(
                contribution_kind="constraint_requirement",
                version=_VERSION,
                requirement_id="req.capability.cache",
                requirement=RequireCapabilityV1(
                    kind="capability",
                    authority="process_emitter",
                    subject="doccacheload",
                    required_state="supported",
                ),
            )
        )
    return tuple(contributions)




# =============================================================================
# Fan-out 2.0.0 (issue #159 S1) — typed assembly with a component-material channel
# =============================================================================
#
# Registered BESIDE 1.0.0 (which stays the default and the compatibility
# adapter's exact target through S2). It lives in THIS module on purpose: the
# registry hashes the defining module's whole source into every registration's
# ``implementation_sha256``, so the v2 input model, the component-material model,
# the preparer and the executor are all covered by one digest. The consequence is
# stated rather than hidden: editing this module moves 1.0.0's digest too, even
# though 1.0.0's behaviour above is byte-identical.
#
# **Two halves, one trust split (architect design C).**
#
# * :func:`prepare_db_rest_fanout_v2` — server-side, TRUSTED. It receives the
#   validated recipe input AND the caller's closed component material
#   (connection settings, SQL, base URLs, paths, mapping operations) and
#   generates every supporting component spec through the surviving primitives
#   (``db_extract``, ``field_map``, ``rest_send``). Material-aware composition
#   checks run here, BEFORE any component is built, and fail value-free.
# * :func:`emit_db_rest_fanout_v2` — the PURE executor. It receives ONLY the
#   closed, secret-free recipe input and emits one ``ProcessUnitContributionV1``
#   plus its requirements and the recorded watermark intent. It never sees
#   material: the root and envelope reference supporting components by the key
#   both halves derive from :func:`_fanout_keys_v2`.


from dataclasses import dataclass as _dataclass
from types import MappingProxyType as _MappingProxyType
from typing import Annotated as _Annotated
from typing import Any as _Any
from typing import Dict as _Dict
from typing import List as _List
from typing import Literal as _Literal
from typing import Mapping as _Mapping
from typing import Union as _Union

from pydantic import BaseModel as _BaseModel
from pydantic import ConfigDict as _ConfigDict
from pydantic import StrictBool as _StrictBool
from pydantic import StrictFloat as _StrictFloat
from pydantic import StrictInt as _StrictInt
from pydantic import StrictStr as _StrictStr
from pydantic import ValidationError as _ValidationError

from ...errors import (
    COMPOSITION_COMPONENT_KEY_COLLISION,
    COMPOSITION_CONTRACT_MISMATCH,
    COMPOSITION_UNSUPPORTED_TOPOLOGY,
    GOVERNANCE_WATERMARK_INCONSISTENT,
    RECIPE_CONSTRAINT_FAILED,
    RECIPE_INPUT_INVALID,
)
from ...models.governance_intent import (
    WatermarkDeclarationV1,
    reject_retired_spellings,
)
from ...models.derived_flows import (
    DerivedCompositionPartV1,
    RecipeOperationHintV1,
    RecipeTargetFactV1,
    RecipeUnitProjectionFactsV1,
)
from ...models.integration_models import IntegrationComponentSpec
from ...models.process_component import (
    ProcessAuthoringUnitV1,
    ProcessComponentEnvelopeV1,
    ProcessConnectionOverrideV1,
    ProcessExtensionBindingsV1,
    ProcessOverrideFieldV1,
)
from ...models.recipe_contributions import (
    ProcessUnitContributionV1,
    RecordedIntentContributionV1,
)
from ...patterns.archetype_parameters import (
    DBResultField,
    DbCreateSettings,
    JSONPayloadProfile,
    RestCreateSettings,
    RestQueryParameter,
    RestSendRequest,
    _flatten_payload_profile_leaves,
)
from ...patterns.base import PrimitiveBuildContext
from ..errors import RecipeError, recipe_diagnostic


def _rt():
    """The surviving primitives and builder authorities, imported at CALL time.

    The recipe layer must load under the ``src.boomi_mcp`` namespace (the
    registry's source digest and ``test_the_recipe_layer_itself_loads_under_the_
    src_namespace`` both import it that way), and the primitives and component
    builders reach ``boomi_mcp`` by ABSOLUTE import. So they are imported where
    they are used, never at module scope. Nothing here is state: the same
    modules, and so the same objects, every call.
    """
    from types import SimpleNamespace

    from ...categories.components.builders.connector_builder import (
        BuilderValidationError,
    )
    from ...categories.components.builders.json_profile_builder import (
        JSONGeneratedProfileBuilder,
    )
    from ...categories.components.builders.map_function_registry import (
        FUNCTION_FAMILIES,
    )
    from ...categories.components.builders.profile_generation import (
        profile_from_json_schema,
    )
    from ...categories.components.builders.transform_map_validation import (
        required_target_coverage_gaps,
    )
    from ...patterns import archetype_assembly as assembly
    from ...patterns.primitives import _helpers as helpers
    from ...patterns.primitives import db_extract, field_map, rest_send

    return SimpleNamespace(
        BuilderValidationError=BuilderValidationError,
        JSONGeneratedProfileBuilder=JSONGeneratedProfileBuilder,
        FUNCTION_FAMILIES=FUNCTION_FAMILIES,
        profile_from_json_schema=profile_from_json_schema,
        required_target_coverage_gaps=required_target_coverage_gaps,
        REST_CREATE_AUTH_MAP=assembly._REST_CREATE_AUTH_MAP,
        SOURCE_PREFIX=assembly._SOURCE_PREFIX,
        TARGET_PREFIX=assembly._TARGET_PREFIX,
        TRANSFORM_PREFIX=assembly._TRANSFORM_PREFIX,
        ROLE_DB_CONNECTION=helpers.ROLE_DB_CONNECTION,
        ROLE_DB_GET_OPERATION=helpers.ROLE_DB_GET_OPERATION,
        ROLE_DB_READ_PROFILE=helpers.ROLE_DB_READ_PROFILE,
        ROLE_REST_CONNECTION=helpers.ROLE_REST_CONNECTION,
        ROLE_REST_OPERATION=helpers.ROLE_REST_OPERATION,
        ROLE_TARGET_PROFILE=helpers.ROLE_TARGET_PROFILE,
        ROLE_TRANSFORM_MAP=helpers.ROLE_TRANSFORM_MAP,
        primitive_component_key=helpers.primitive_component_key,
        slugify=helpers.slugify,
        DbExtractParameters=db_extract.DbExtractParameters,
        DbExtractPrimitive=db_extract.DbExtractPrimitive,
        db_connection_extension_fields=db_extract.db_connection_extension_fields,
        FieldMapParameters=field_map.FieldMapParameters,
        FieldMapPrimitive=field_map.FieldMapPrimitive,
        RestSendWithRetryParameters=rest_send.RestSendWithRetryParameters,
        RestSendWithRetryPrimitive=rest_send.RestSendWithRetryPrimitive,
        rest_connection_extension_fields=rest_send.rest_connection_extension_fields,
    )


#: The composition-owned Document Cache key for staged handoffs. A port of
#: ``patterns.composition._HANDOFF_CACHE_KEY`` (scheduled for deletion and
#: import-blocked at replay); ``tests/test_recipe_fanout_v2.py`` pins the two
#: equal while the legacy module exists, and the frozen flagship oracle pins it
#: after.
_HANDOFF_CACHE_KEY_V2 = "handoff_document_cache"


def _reserved_prefixes_v2() -> frozenset:
    """Key prefixes owned by the base assembly. DERIVED from the neutral
    assembly constants, never re-typed."""
    rt = _rt()
    return frozenset({rt.SOURCE_PREFIX, rt.TRANSFORM_PREFIX, rt.TARGET_PREFIX})


#: Fixed spellings the legacy composition emitted on the composed root and its
#: components. Pinned against the frozen flagship oracle and the legacy arm's
#: root by the v2 witness tests.
_MAP_LABEL_V2 = "Shared transform"
#: The served transform flow row's default name (the legacy DB-source sync row).
_TRANSFORM_ROW_NAME_V2 = "Map source to JSON payload"
_BRANCH_LABEL_V2 = "Fan out"
_STAGE_LABEL_V2 = "Stage mapped documents"
_CONSUME_LABEL_V2 = "Consume staged documents"
_PROCESS_NAME_SUFFIX_V2 = "DB to API Sync"
_CACHE_NAME_SUFFIX_V2 = "Handoff Cache"

#: DERIVED from the neutral parameter models — the authorities for these value
#: vocabularies — so the v2 surface cannot drift from them.
_RestMethodV2 = RestSendRequest.model_fields["method"].annotation
_DEFAULT_STATUS_CODES_V2: Tuple[int, ...] = tuple(
    RestSendRequest.model_fields["expected_status_codes"].default_factory()
)
_DbAuthModeV2 = DbCreateSettings.model_fields["auth_mode"].annotation
_DbDriverV2 = DbCreateSettings.model_fields["driver"].annotation
_DEFAULT_DB_PORT_V2: int = DbCreateSettings.model_fields["port"].default
_DbFieldTypeV2 = DBResultField.model_fields["data_type"].annotation

def _v2_function_families() -> Tuple[str, ...]:
    """The map-function families the v2 transform may use.

    DERIVED from the ``FunctionFamily`` registry (the parameter authority),
    restricted to the side-effect-free families: a fan-out's shared transform
    writes payload leaves, never process or document properties.
    """
    return tuple(
        sorted(
            name
            for name, family in _rt().FUNCTION_FAMILIES.items()
            if getattr(family, "effect_kind", None) == "pure"
        )
    )


# ---------------------------------------------------------------------------
# v2 recipe input — closed and secret-free (what the PURE executor receives)
# ---------------------------------------------------------------------------


class FanoutSourceV2(RecipeInputBase):
    """The database source part: identity and binding classification only."""

    part_key: RecipeSemanticId
    binding_mode: _Literal["create", "reuse"]
    #: The create-mode connection's auth mode. It decides whether the source
    #: connection's credential fields become per-environment override points.
    auth_mode: Optional[_DbAuthModeV2] = None
    #: An EXISTING base component (a ``base_components`` entry of the intent)
    #: this part binds to, by key. When set, the component material carries no
    #: connection for this part.
    connection_key: Optional[RecipeComponentKey] = None

    @model_validator(mode="after")
    def _auth_mode_matches_binding(self) -> "FanoutSourceV2":
        if (self.binding_mode == "create") != (self.auth_mode is not None):
            raise PydanticCustomError(
                "fanout_source_auth_mode",
                "auth_mode is required for a create-mode source and refused otherwise",
            )
        return self


class FanoutTransformV2(RecipeInputBase):
    """The shared transform part."""

    part_key: RecipeSemanticId
    #: The NAME of the served transform flow row. The composed root's
    #: ``map_ref`` step label is the legacy constant (``_MAP_LABEL_V2``) and is
    #: not an input: the legacy composition fixed it, and a name that also moved
    #: the step made the legacy step-label/row-name pair unreachable.
    map_label: Optional[str] = Field(
        default=None,
        min_length=1,
        description=(
            "The name of the served transform flow row. Defaults to the legacy "
            "row name. The process map step's label is fixed and is not set by "
            "this field."
        ),
    )


class FanoutTargetV2(RecipeInputBase):
    """One REST target part, in Branch order."""

    part_key: RecipeSemanticId
    label: Optional[str] = Field(default=None, min_length=1)
    binding_mode: _Literal["create", "reuse"]
    connection_key: Optional[RecipeComponentKey] = None
    method: _RestMethodV2 = "POST"  # type: ignore[valid-type]
    expected_status_codes: Tuple[int, ...] = _DEFAULT_STATUS_CODES_V2


class FanoutHandoffV2(RecipeInputBase):
    """How documents move along one link.

    The keyed-retrieval fields exist only so they can be REFUSED by name with
    ``COMPOSITION_UNSUPPORTED_TOPOLOGY`` (no live-captured wire shape, #119).
    """

    mode: _Literal["document_stream", "document_cache"] = "document_stream"
    doc_cache_index: Optional[int] = None
    cache_key_values: Optional[Tuple[str, ...]] = None


class FanoutLinkV2(RecipeInputBase):
    from_part: str = Field(min_length=1)
    to_part: str = Field(min_length=1)
    handoff: FanoutHandoffV2 = Field(default_factory=FanoutHandoffV2)


class FanoutNamingV2(RecipeInputBase):
    component_prefix: str = Field(min_length=1)
    folder_name: Optional[str] = Field(default=None, min_length=1)
    process_name: Optional[str] = Field(default=None, min_length=1)


class FanoutWatermarkV2(RecipeInputBase):
    """The typed watermark intent. Recorded, never wired (M18 rule half)."""

    field: str = Field(min_length=1)
    kind: _Literal["timestamp", "sequence"]
    initial_value: Optional[str] = None
    query_parameter_refs: Tuple[str, ...] = ()


class ComposeDbRestFanoutInputV2(RecipeInputBase):
    """The v2 recipe input: topology, identities and binding classifications.

    Carries NO connection material. Hosts, credentials, SQL, URLs, paths and
    mapping operations travel in the separate, closed component material
    (:class:`FanoutComponentMaterialV2`), which only the trusted preparer reads.
    """

    version: _Literal["2"]
    process_key: RecipeComponentKey
    #: Optional namespace for every DERIVED component key, so two v2 invocations
    #: in one request do not collide. Omitted: the legacy keys exactly.
    key_namespace: Optional[str] = Field(default=None, pattern=r"^[a-z][a-z0-9_]*$")
    naming: FanoutNamingV2
    source: FanoutSourceV2
    transform: FanoutTransformV2
    targets: Tuple[FanoutTargetV2, ...] = Field(min_length=2, max_length=_MAX_TARGETS)
    #: Omitted: the star is inferred, every transform -> target link streaming.
    #: Given: it must describe exactly that star (an empty tuple does not).
    links: Optional[Tuple[FanoutLinkV2, ...]] = None
    watermark: Optional[FanoutWatermarkV2] = None


# ---------------------------------------------------------------------------
# v2 component material — closed, strict, repr-redacted (preparer only)
# ---------------------------------------------------------------------------


class _FanoutMaterialModel(_BaseModel):
    """Strict, frozen, and repr-REDACTED: every value renders as ``...``.

    Material carries connection settings, SQL and URLs. A repr reaches logs and
    tracebacks, so no field value — not even a structural one — is rendered.
    Retired #157 spellings are refused by name before validation.
    """

    model_config = _ConfigDict(extra="forbid", frozen=True)

    def __repr_args__(self):  # noqa: D105 — pydantic hook
        for key, _value in super().__repr_args__():
            yield key, "..."

    @model_validator(mode="before")
    @classmethod
    def _refuse_retired_spellings(cls, value: _Any) -> _Any:
        if isinstance(value, _Mapping):
            reject_retired_spellings(value, path="")
        return value


class FanoutDbCreateMaterialV2(_FanoutMaterialModel):
    """Create a database connection. Value rules are ``DbCreateSettings``'s."""

    mode: _Literal["create"]
    driver: _DbDriverV2  # type: ignore[valid-type]
    host: str = Field(min_length=1)
    port: int = Field(default=_DEFAULT_DB_PORT_V2, ge=1, le=65535)
    database: str = Field(min_length=1)
    username: Optional[str] = None
    #: An OPAQUE secret-store reference — never the secret itself.
    credential_ref: Optional[str] = None


class FanoutConnectionReuseMaterialV2(_FanoutMaterialModel):
    """Reference an existing connection by exactly one selector."""

    mode: _Literal["reuse"]
    component_id: Optional[str] = Field(default=None, min_length=1)
    component_name: Optional[str] = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _exactly_one_selector(self) -> "FanoutConnectionReuseMaterialV2":
        if (self.component_id is None) == (self.component_name is None):
            raise PydanticCustomError(
                "fanout_reuse_selector",
                "a reuse binding needs exactly one of component_id or component_name",
            )
        return self


FanoutDbConnectionMaterialV2 = _Annotated[
    _Union[FanoutDbCreateMaterialV2, FanoutConnectionReuseMaterialV2],
    Field(discriminator="mode"),
]


class FanoutResultFieldV2(_FanoutMaterialModel):
    name: str = Field(min_length=1)
    data_type: _DbFieldTypeV2  # type: ignore[valid-type]
    required: bool = False
    description: Optional[str] = None


class FanoutBindParameterV2(_FanoutMaterialModel):
    name: str = Field(min_length=1)
    direction: _Literal["in", "out"] = "in"


class FanoutDbReadMaterialV2(_FanoutMaterialModel):
    sql: str = Field(min_length=1)
    result_fields: Tuple[FanoutResultFieldV2, ...] = Field(min_length=1)
    bind_parameters: Tuple[FanoutBindParameterV2, ...] = ()
    batch_size: Optional[int] = Field(default=None, ge=1)
    max_rows: Optional[int] = Field(default=None, ge=1)


class FanoutDbMaterialV2(_FanoutMaterialModel):
    #: Absent exactly when the recipe input names a base ``connection_key``.
    connection: Optional[FanoutDbConnectionMaterialV2] = None
    read: FanoutDbReadMaterialV2


class FanoutDirectOperationV2(_FanoutMaterialModel):
    operation_type: _Literal["direct"]
    source_field: str = Field(min_length=1)
    target_path: str = Field(min_length=1)
    documentation_hint: Optional[str] = None


class FanoutFunctionParameterV2(_FanoutMaterialModel):
    """One map-function parameter: a name and a SCALAR value — never a mapping."""

    name: str = Field(min_length=1)
    value: _Union[_StrictStr, _StrictBool, _StrictInt, _StrictFloat]


class FanoutFunctionOperationV2(_FanoutMaterialModel):
    """A map-function operation. Families and parameters come from ``FunctionFamily``."""

    operation_type: _Literal["map_function"]
    function_type: str = Field(min_length=1)
    inputs: Tuple[str, ...] = ()
    target_path: str = Field(min_length=1)
    parameters: Tuple[FanoutFunctionParameterV2, ...] = ()
    documentation_hint: Optional[str] = None

    @model_validator(mode="after")
    def _check_against_the_family(self) -> "FanoutFunctionOperationV2":
        if self.function_type not in _v2_function_families():
            raise PydanticCustomError(
                "fanout_function_family_unsupported",
                "function_type is not a side-effect-free registered map-function family",
            )
        family = _rt().FUNCTION_FAMILIES[self.function_type]
        names = [parameter.name for parameter in self.parameters]
        allowed = set(family.required_parameters) | set(family.optional_parameters)
        if len(set(names)) != len(names) or not set(names) <= allowed:
            raise PydanticCustomError(
                "fanout_function_parameter_unknown",
                "parameters must be unique names the family declares",
            )
        if not set(family.required_parameters) <= set(names):
            raise PydanticCustomError(
                "fanout_function_parameter_missing",
                "a required parameter of the family is missing",
            )
        for parameter in self.parameters:
            validator = family.parameter_validators.get(parameter.name)
            if validator is not None and validator(parameter.value) is not None:
                raise PydanticCustomError(
                    "fanout_function_parameter_invalid",
                    "a parameter value fails the family's own validator",
                )
        return self


class FanoutScriptReferenceOperationV2(_FanoutMaterialModel):
    """A map-script operation that REFERENCES a script component by key.

    The script is authored as a canonical supporting component — an intent
    ``base_components`` entry of type ``script.mapping`` — never inline here.
    """

    operation_type: _Literal["map_script"]
    script_component_key: RecipeComponentKey
    inputs: Tuple[str, ...] = Field(min_length=1)
    outputs: Tuple[str, ...] = Field(min_length=1)
    documentation_hint: Optional[str] = None


FanoutTransformOperationV2 = _Annotated[
    _Union[
        FanoutDirectOperationV2,
        FanoutFunctionOperationV2,
        FanoutScriptReferenceOperationV2,
    ],
    Field(discriminator="operation_type"),
]


class FanoutTransformMaterialV2(_FanoutMaterialModel):
    operations: Tuple[FanoutTransformOperationV2, ...] = Field(min_length=1)


class FanoutRestCreateMaterialV2(_FanoutMaterialModel):
    """Create an UNAUTHENTICATED REST connection (secured auth must be reused)."""

    mode: _Literal["create"]
    base_url: str = Field(min_length=1)


FanoutRestConnectionMaterialV2 = _Annotated[
    _Union[FanoutRestCreateMaterialV2, FanoutConnectionReuseMaterialV2],
    Field(discriminator="mode"),
]


class FanoutQueryParameterV2(_FanoutMaterialModel):
    name: str = Field(min_length=1)
    value_source: _Literal["literal", "watermark"]
    literal_value: Optional[str] = None


class FanoutRestOperationMaterialV2(_FanoutMaterialModel):
    path: str = Field(min_length=1)
    query_parameters: Tuple[FanoutQueryParameterV2, ...] = ()


class FanoutTargetMaterialV2(_FanoutMaterialModel):
    part_key: str = Field(min_length=1)
    #: Absent exactly when the recipe input names a base ``connection_key``.
    connection: Optional[FanoutRestConnectionMaterialV2] = None
    operation: FanoutRestOperationMaterialV2
    #: Optional assertion of this target's expected leaf contract. Compared by
    #: path, data type and requiredness against the ONE shared payload profile.
    expected_payload_profile: Optional[JSONPayloadProfile] = None


class FanoutComponentMaterialV2(_FanoutMaterialModel):
    """The closed component material the fan-out 2.0.0 preparer consumes.

    Accepted ONLY on an invocation whose resolved registration declares it.
    It never reaches the recipe executor, a contribution, provenance, or a
    diagnostic: the preparer turns it into supporting component specs.
    """

    version: _Literal["2"]
    source: FanoutDbMaterialV2
    transform: FanoutTransformMaterialV2
    #: The ONE shared payload profile every target leg sends.
    payload_profile: JSONPayloadProfile
    targets: Tuple[FanoutTargetMaterialV2, ...] = Field(min_length=2, max_length=_MAX_TARGETS)


# ---------------------------------------------------------------------------
# Shared key derivation (preparer AND executor)
# ---------------------------------------------------------------------------


@_dataclass(frozen=True)
class _FanoutKeysV2:
    """Every component key the v2 route names. Derived once, used by both halves."""

    db_connection: str
    db_read_profile: str
    db_get_operation: str
    target_profile: str
    transform_map: str
    cache: str
    #: ``(prefix, connection key, operation key)`` per target, in Branch order.
    targets: Tuple[Tuple[str, str, str], ...]
    #: Which connection keys are DERIVED (generated) rather than base keys.
    derived_connections: frozenset


def _namespaced_v2(inp: ComposeDbRestFanoutInputV2, prefix: str) -> str:
    return prefix if inp.key_namespace is None else f"{inp.key_namespace}_{prefix}"


def _target_prefix_v2(part_key: str, index: int) -> str:
    """The legacy prefix rule: the first target is the base ``target`` pair."""
    rt = _rt()
    return rt.TARGET_PREFIX if index == 0 else f"{rt.TARGET_PREFIX}_{rt.slugify(part_key)}"


def _fanout_keys_v2(inp: ComposeDbRestFanoutInputV2) -> _FanoutKeysV2:
    source_prefix = _namespaced_v2(inp, _rt().SOURCE_PREFIX)
    transform_prefix = _namespaced_v2(inp, _rt().TRANSFORM_PREFIX)
    derived = set()
    db_connection = inp.source.connection_key or _rt().primitive_component_key(
        source_prefix, _rt().ROLE_DB_CONNECTION
    )
    if inp.source.connection_key is None:
        derived.add(db_connection)
    targets = []
    for index, target in enumerate(inp.targets):
        prefix = _namespaced_v2(inp, _target_prefix_v2(target.part_key, index))
        connection = target.connection_key or _rt().primitive_component_key(
            prefix, _rt().ROLE_REST_CONNECTION
        )
        if target.connection_key is None:
            derived.add(connection)
        targets.append(
            (prefix, connection, _rt().primitive_component_key(prefix, _rt().ROLE_REST_OPERATION))
        )
    return _FanoutKeysV2(
        db_connection=db_connection,
        db_read_profile=_rt().primitive_component_key(source_prefix, _rt().ROLE_DB_READ_PROFILE),
        db_get_operation=_rt().primitive_component_key(source_prefix, _rt().ROLE_DB_GET_OPERATION),
        target_profile=_rt().primitive_component_key(transform_prefix, _rt().ROLE_TARGET_PROFILE),
        transform_map=_rt().primitive_component_key(transform_prefix, _rt().ROLE_TRANSFORM_MAP),
        cache=_namespaced_v2(inp, _HANDOFF_CACHE_KEY_V2),
        targets=tuple(targets),
        derived_connections=frozenset(derived),
    )


def _derived_label_v2(target: FanoutTargetV2) -> str:
    """Port of ``composition._derived_label``: explicit label, else a humanized key."""
    if target.label and target.label.strip():
        return target.label.strip()
    return re.sub(r"[_\-]+", " ", target.part_key).strip().title()


def _handoff_modes_v2(inp: ComposeDbRestFanoutInputV2) -> _Dict[str, str]:
    """Per-target handoff mode. Omitted links: every target streams."""
    modes = {target.part_key: "document_stream" for target in inp.targets}
    for link in inp.links or ():
        if link.from_part == inp.transform.part_key and link.to_part in modes:
            modes[link.to_part] = link.handoff.mode
    return modes


# ---------------------------------------------------------------------------
# Value-free refusals
# ---------------------------------------------------------------------------


def _refusal(
    cause: str,
    path: str,
    *,
    phase: str,
    code: str = RECIPE_CONSTRAINT_FAILED,
    target: Optional[str] = None,
) -> RecipeError:
    """One value-free diagnostic: a static message, a cause code and a POINTER.

    The pointer is structural (positions and field names from the caller's own
    request). No value — not a key, not a label, not a setting — is rendered.
    """
    return RecipeError(
        (
            recipe_diagnostic(
                code,
                phase=phase,  # type: ignore[arg-type]
                path=path,
                target=target,
                cause_codes=(cause,),
            ),
        )
    )


def _validate_fanout_topology_v2(inp: ComposeDbRestFanoutInputV2, *, phase: str) -> None:
    """The material-free composition rules, run BEFORE anything is built.

    A port of ``patterns.composition.validate_composition`` steps 1-3 (counts are
    enforced by the model's bounds) plus the connection-key sharing rule. Shared
    by the preparer's preflight and the executor, so neither half can emit a
    component or a unit for a topology the other would refuse.
    """
    parts = [("/raw_input/source/part_key", inp.source.part_key)]
    parts.append(("/raw_input/transform/part_key", inp.transform.part_key))
    parts.extend(
        (f"/raw_input/targets/{index}/part_key", target.part_key)
        for index, target in enumerate(inp.targets)
    )
    seen: set = set()
    for pointer, key in parts:
        if key in seen:
            raise _refusal(COMPOSITION_COMPONENT_KEY_COLLISION, pointer, phase=phase)
        seen.add(key)

    seen_prefixes: set = set()
    for index, target in enumerate(inp.targets):
        if index == 0:
            continue
        prefix = _target_prefix_v2(target.part_key, index)
        if prefix in _reserved_prefixes_v2() or prefix in seen_prefixes:
            raise _refusal(
                COMPOSITION_COMPONENT_KEY_COLLISION,
                f"/raw_input/targets/{index}/part_key",
                phase=phase,
            )
        seen_prefixes.add(prefix)

    seen_labels: set = set()
    for index, target in enumerate(inp.targets):
        label = _derived_label_v2(target)
        if label in seen_labels:
            raise _refusal(
                COMPOSITION_COMPONENT_KEY_COLLISION,
                f"/raw_input/targets/{index}/label",
                phase=phase,
            )
        seen_labels.add(label)

    # Explicit links describe EXACTLY the star (port of `_validate_links`).
    if inp.links is not None:
        source_key, transform_key = inp.source.part_key, inp.transform.part_key
        target_keys = [target.part_key for target in inp.targets]
        declared_parts = {source_key, transform_key, *target_keys}
        for index, link in enumerate(inp.links):
            for name in ("from_part", "to_part"):
                if getattr(link, name) not in declared_parts:
                    raise _refusal(
                        COMPOSITION_UNSUPPORTED_TOPOLOGY,
                        f"/raw_input/links/{index}/{name}",
                        phase=phase,
                    )
            for gated in ("doc_cache_index", "cache_key_values"):
                if getattr(link.handoff, gated) is not None:
                    raise _refusal(
                        COMPOSITION_UNSUPPORTED_TOPOLOGY,
                        f"/raw_input/links/{index}/handoff/{gated}",
                        phase=phase,
                    )
            if link.handoff.mode == "document_cache" and not (
                link.from_part == transform_key and link.to_part in target_keys
            ):
                raise _refusal(
                    COMPOSITION_UNSUPPORTED_TOPOLOGY,
                    f"/raw_input/links/{index}/handoff/mode",
                    phase=phase,
                )
        expected = {(source_key, transform_key)} | {
            (transform_key, key) for key in target_keys
        }
        declared = [(link.from_part, link.to_part) for link in inp.links]
        if len(set(declared)) != len(declared) or set(declared) != expected:
            raise _refusal(COMPOSITION_UNSUPPORTED_TOPOLOGY, "/raw_input/links", phase=phase)
        modes = _handoff_modes_v2(inp)
        if (
            any(mode == "document_cache" for mode in modes.values())
            and len(inp.targets) > _MAX_CACHE_TARGETS
        ):
            raise _refusal(COMPOSITION_UNSUPPORTED_TOPOLOGY, "/raw_input/links", phase=phase)

    # A base connection key shared by several parts is one connection: the
    # parts must agree on how it is bound, or its environment-extension
    # declaration would be two different things for one component.
    bindings: _Dict[str, str] = {}
    bound = [("/raw_input/source/connection_key", inp.source.connection_key, inp.source.binding_mode)]
    bound.extend(
        (f"/raw_input/targets/{index}/connection_key", target.connection_key, target.binding_mode)
        for index, target in enumerate(inp.targets)
    )
    for pointer, key, mode in bound:
        if key is None:
            continue
        if bindings.setdefault(key, mode) != mode:
            raise _refusal(COMPOSITION_UNSUPPORTED_TOPOLOGY, pointer, phase=phase)
    if inp.source.connection_key is not None and any(
        target.connection_key == inp.source.connection_key for target in inp.targets
    ):
        raise _refusal(
            COMPOSITION_UNSUPPORTED_TOPOLOGY, "/raw_input/source/connection_key", phase=phase
        )

    # The root's own key may not be any component key this route names.
    keys = _fanout_keys_v2(inp)
    named = {
        keys.db_connection,
        keys.db_read_profile,
        keys.db_get_operation,
        keys.target_profile,
        keys.transform_map,
        keys.cache,
    }
    for _prefix, connection, operation in keys.targets:
        named.update((connection, operation))
    if inp.process_key in named:
        raise _refusal(COMPOSITION_COMPONENT_KEY_COLLISION, "/raw_input/process_key", phase=phase)


# ---------------------------------------------------------------------------
# The preparer (trusted, server-side)
# ---------------------------------------------------------------------------


@_dataclass(frozen=True, repr=False)
class PreparedFanoutV2:
    """What the preparer hands the workflow. NEVER reaches a recipe executor.

    ``supporting_components`` are the generated specs, in the legacy component
    order. ``selected_base_keys`` name intent ``base_components`` entries the
    generated specs reference (a base-key connection, a referenced script) —
    the workflow adds those catalog entries to the run exactly once.
    ``deferred_query_parameter_names`` maps the FIRST target's operation key to
    the watermark-sourced query parameter names the preparer validated: they
    are recorded intent, kept OUT of the emitted operation config, and handed
    to governance's watermark rule scoped to that operation and its root.

    The repr names counts and keys only; component configs carry material.
    """

    supporting_components: Tuple[IntegrationComponentSpec, ...]
    selected_base_keys: Tuple[str, ...] = ()
    deferred_query_parameter_names: Tuple[Tuple[str, Tuple[str, ...]], ...] = ()
    #: #159 S1 (C8): the trusted facts the #157 projection reads for this
    #: invocation's unit. Their presence is what admits the unit's recipe-route
    #: rows. Keys and classifications only, EXCEPT the transform operations'
    #: documentation hints — caller content from the material, served on the
    #: transform row exactly where the legacy row served it (repr-redacted).
    projection_facts: Optional[RecipeUnitProjectionFactsV1] = None

    def __repr__(self) -> str:  # noqa: D105
        return (
            "PreparedFanoutV2("
            f"supporting_keys={tuple(c.key for c in self.supporting_components)!r}, "
            f"selected_base_keys={self.selected_base_keys!r})"
        )


def _material_failure(path: str, error: BaseException) -> RecipeError:
    """A value-free refusal for a value rule an authority model enforced."""
    cause = getattr(error, "error_code", None) or RECIPE_INPUT_INVALID
    return _refusal(str(cause), path, phase="input", code=RECIPE_INPUT_INVALID)


def _authority_check(pointer: str, model: _Any, /, **values: _Any) -> _Any:
    """Validate ``values`` through a neutral authority model, value-free on failure."""
    try:
        return model(**values)
    except _ValidationError:
        raise _material_failure(pointer, ValueError()) from None


def _builder_check(path: str, build):
    """Run a primitive build; a builder refusal becomes a value-free diagnostic."""
    try:
        return build()
    except _rt().BuilderValidationError as exc:
        raise _refusal(
            str(getattr(exc, "error_code", "") or RECIPE_CONSTRAINT_FAILED),
            path,
            phase="input",
        ) from None
    except _ValidationError:
        raise _material_failure(path, ValueError()) from None


def _check_base_component(
    base_catalog: _Mapping[str, IntegrationComponentSpec],
    key: str,
    *,
    path: str,
    component_type: str,
    connector_type: Optional[str] = None,
    binding_mode: Optional[str] = None,
) -> None:
    component = base_catalog.get(key)
    if component is None or component.type != component_type:
        raise _refusal(COMPOSITION_CONTRACT_MISMATCH, path, phase="input")
    config = component.config or {}
    if connector_type is not None and config.get("connector_type") != connector_type:
        raise _refusal(COMPOSITION_CONTRACT_MISMATCH, path, phase="input")
    if binding_mode is not None and bool(config.get("reference_only")) != (
        binding_mode == "reuse"
    ):
        raise _refusal(COMPOSITION_CONTRACT_MISMATCH, path, phase="input")


def _connection_presence(
    raw_key: Optional[str], material: _Any, binding_mode: str, *, path: str
) -> None:
    """Material carries a connection exactly when no base key is named, in the
    binding mode the recipe input declares."""
    if raw_key is not None:
        if material is not None:
            raise _refusal(COMPOSITION_CONTRACT_MISMATCH, path, phase="input")
        return
    if material is None or material.mode != binding_mode:
        raise _refusal(COMPOSITION_CONTRACT_MISMATCH, path, phase="input")


def _leaf_contract(profile: JSONPayloadProfile) -> Tuple[_Dict[str, str], set]:
    leaves = _flatten_payload_profile_leaves(profile)
    index = _rt().profile_from_json_schema(profile)["field_index_by_path"]
    return leaves, set(_rt().required_target_coverage_gaps(index, ()))


def prepare_db_rest_fanout_v2(
    inp: ComposeDbRestFanoutInputV2,
    material: FanoutComponentMaterialV2,
    *,
    base_catalog: _Mapping[str, IntegrationComponentSpec],
) -> PreparedFanoutV2:
    """Generate every supporting component spec for one v2 invocation.

    Order is load-bearing, and every refusal is value-free:

    1. the material-free topology rules (shared with the executor);
    2. material <-> input agreement (one material entry per target part key,
       connection presence and binding mode, base components by key and type);
    3. value rules through the neutral authority models;
    4. the producer -> consumer contract (source fields, transform operations,
       the shared payload profile, each target's asserted leaf contract);
    5. the first-target watermark rule;
    6. ONLY THEN the components, through ``db_extract``, ``field_map`` and
       ``rest_send`` — the DB connection/profile/operation, the shared JSON
       profile and map, one REST connection/operation pair per target, and the
       staged-handoff cache when any link stages.
    """
    _validate_fanout_topology_v2(inp, phase="input")
    keys = _fanout_keys_v2(inp)
    naming = inp.naming
    selected: _List[str] = []

    # -- 2. agreement ------------------------------------------------------
    by_part: _Dict[str, Tuple[int, FanoutTargetMaterialV2]] = {}
    for index, entry in enumerate(material.targets):
        if entry.part_key in by_part:
            raise _refusal(
                COMPOSITION_CONTRACT_MISMATCH,
                f"/component_material/targets/{index}/part_key",
                phase="input",
            )
        by_part[entry.part_key] = (index, entry)
    input_parts = {target.part_key for target in inp.targets}
    for index, entry in enumerate(material.targets):
        if entry.part_key not in input_parts:
            raise _refusal(
                COMPOSITION_CONTRACT_MISMATCH,
                f"/component_material/targets/{index}/part_key",
                phase="input",
            )
    if set(by_part) != input_parts:
        raise _refusal(COMPOSITION_CONTRACT_MISMATCH, "/component_material/targets", phase="input")

    _connection_presence(
        inp.source.connection_key,
        material.source.connection,
        inp.source.binding_mode,
        path="/component_material/source/connection",
    )
    if inp.source.connection_key is not None:
        _check_base_component(
            base_catalog,
            inp.source.connection_key,
            path="/raw_input/source/connection_key",
            component_type="connector-settings",
            connector_type="database",
            binding_mode=inp.source.binding_mode,
        )
        selected.append(inp.source.connection_key)
    ordered_material: _List[Tuple[int, FanoutTargetMaterialV2]] = []
    for index, target in enumerate(inp.targets):
        material_index, entry = by_part[target.part_key]
        ordered_material.append((material_index, entry))
        _connection_presence(
            target.connection_key,
            entry.connection,
            target.binding_mode,
            path=f"/component_material/targets/{material_index}/connection",
        )
        if target.connection_key is not None:
            _check_base_component(
                base_catalog,
                target.connection_key,
                path=f"/raw_input/targets/{index}/connection_key",
                component_type="connector-settings",
                connector_type="rest",
                binding_mode=target.binding_mode,
            )
            if target.connection_key not in selected:
                selected.append(target.connection_key)

    # -- 3. value rules through the authorities ----------------------------
    db_connection = material.source.connection
    if isinstance(db_connection, FanoutDbCreateMaterialV2):
        _authority_check(
            "/component_material/source/connection",
            DbCreateSettings,
            driver=db_connection.driver,
            auth_mode=inp.source.auth_mode,
            host=db_connection.host,
            port=db_connection.port,
            database=db_connection.database,
            username=db_connection.username,
            credential_ref=db_connection.credential_ref,
        )
    for field_index, field in enumerate(material.source.read.result_fields):
        _authority_check(
            f"/component_material/source/read/result_fields/{field_index}",
            DBResultField,
            name=field.name,
            data_type=field.data_type,
            required=field.required,
            description=field.description,
        )
    result_names = [field.name for field in material.source.read.result_fields]
    if len(set(result_names)) != len(result_names):
        raise _refusal(
            COMPOSITION_CONTRACT_MISMATCH,
            "/component_material/source/read/result_fields",
            phase="input",
        )
    for (material_index, entry), target in zip(ordered_material, inp.targets):
        base = f"/component_material/targets/{material_index}"
        if isinstance(entry.connection, FanoutRestCreateMaterialV2):
            # `RestCreateSettings` is the URL-shape authority; created REST is
            # always unauthenticated (the auth vocabulary's ONE create mode).
            _authority_check(
                base + "/connection",
                RestCreateSettings,
                base_url=entry.connection.base_url,
                auth_mode="none",
            )
        for qp_index, parameter in enumerate(entry.operation.query_parameters):
            _authority_check(
                f"{base}/operation/query_parameters/{qp_index}",
                RestQueryParameter,
                name=parameter.name,
                value_source=parameter.value_source,
                literal_value=parameter.literal_value,
            )
        _authority_check(
            base + "/operation",
            RestSendRequest,
            method=target.method,
            path=entry.operation.path,
            expected_status_codes=list(target.expected_status_codes),
        )

    # -- 4. producer -> consumer contract ----------------------------------
    source_fields = {field.name: field for field in material.source.read.result_fields}
    shared_leaves, shared_required = _leaf_contract(material.payload_profile)
    bound_paths: set = set()
    for op_index, operation in enumerate(material.transform.operations):
        pointer = f"/component_material/transform/operations/{op_index}"
        if isinstance(operation, FanoutDirectOperationV2):
            inputs, outputs = [operation.source_field], [operation.target_path]
        elif isinstance(operation, FanoutFunctionOperationV2):
            inputs, outputs = list(operation.inputs), [operation.target_path]
        else:
            inputs, outputs = list(operation.inputs), list(operation.outputs)
            _check_base_component(
                base_catalog,
                operation.script_component_key,
                path=pointer + "/script_component_key",
                component_type="script.mapping",
            )
            if operation.script_component_key not in selected:
                selected.append(operation.script_component_key)
        if any(name not in source_fields for name in inputs):
            raise _refusal(COMPOSITION_CONTRACT_MISMATCH, pointer, phase="input")
        for output in outputs:
            if output not in shared_leaves:
                raise _refusal(COMPOSITION_CONTRACT_MISMATCH, pointer, phase="input")
            bound_paths.add(output)
    index = _rt().profile_from_json_schema(material.payload_profile)["field_index_by_path"]
    if _rt().required_target_coverage_gaps(index, bound_paths):
        raise _refusal(
            COMPOSITION_CONTRACT_MISMATCH, "/component_material/payload_profile", phase="input"
        )
    for material_index, entry in ordered_material:
        if entry.expected_payload_profile is None:
            continue
        leaves, required = _leaf_contract(entry.expected_payload_profile)
        if leaves != shared_leaves or required != shared_required:
            raise _refusal(
                COMPOSITION_CONTRACT_MISMATCH,
                f"/component_material/targets/{material_index}/expected_payload_profile",
                phase="input",
            )

    # -- 5. the first-target watermark rule --------------------------------
    deferred: Tuple[Tuple[str, Tuple[str, ...]], ...] = ()
    for position, (material_index, entry) in enumerate(ordered_material):
        names = tuple(
            parameter.name
            for parameter in entry.operation.query_parameters
            if parameter.value_source == "watermark"
        )
        if position > 0 and names:
            first = next(
                qp_index
                for qp_index, parameter in enumerate(entry.operation.query_parameters)
                if parameter.value_source == "watermark"
            )
            raise _refusal(
                COMPOSITION_UNSUPPORTED_TOPOLOGY,
                f"/component_material/targets/{material_index}/operation/query_parameters/{first}",
                phase="input",
            )
        if position == 0 and names:
            if inp.watermark is None:
                raise _refusal(
                    GOVERNANCE_WATERMARK_INCONSISTENT,
                    f"/component_material/targets/{material_index}/operation/query_parameters",
                    phase="input",
                )
            deferred = ((keys.targets[0][2], names),)
    if inp.watermark is not None:
        first_material_index, first_entry = ordered_material[0]
        first_names = {parameter.name for parameter in first_entry.operation.query_parameters}
        # A watermark-sourced parameter is never emitted; the declaration's refs
        # are the ONLY place it is recorded. One the refs omit would vanish —
        # refused rather than silently dropped.
        for qp_index, parameter in enumerate(first_entry.operation.query_parameters):
            if (
                parameter.value_source == "watermark"
                and parameter.name not in inp.watermark.query_parameter_refs
            ):
                raise _refusal(
                    GOVERNANCE_WATERMARK_INCONSISTENT,
                    f"/component_material/targets/{first_material_index}"
                    f"/operation/query_parameters/{qp_index}",
                    phase="input",
                )
        for ref_index, name in enumerate(inp.watermark.query_parameter_refs):
            if name not in first_names:
                raise _refusal(
                    GOVERNANCE_WATERMARK_INCONSISTENT,
                    f"/raw_input/watermark/query_parameter_refs/{ref_index}",
                    phase="input",
                )

    # -- 6. components -----------------------------------------------------
    context = PrimitiveBuildContext(
        integration_name=naming.component_prefix,
        component_prefix=naming.component_prefix,
        folder_path=naming.folder_name,
    )
    components: _List[IntegrationComponentSpec] = []
    components.extend(_emit_db_components_v2(inp, material, keys, context))
    components.extend(_emit_transform_components_v2(inp, material, keys, context))

    modes = _handoff_modes_v2(inp)
    staged = any(mode == "document_cache" for mode in modes.values())
    target_pairs: _List[_List[IntegrationComponentSpec]] = []
    for index, (target, (material_index, entry)) in enumerate(
        zip(inp.targets, ordered_material)
    ):
        target_pairs.append(
            _emit_target_components_v2(
                target, entry, keys.targets[index], keys, context, material_index
            )
        )
    # Legacy order: the base (first) pair, then the staged cache, then the rest.
    components.extend(target_pairs[0])
    if staged:
        profile = next(c for c in components if c.key == keys.target_profile)
        components.append(_emit_handoff_cache_v2(keys.cache, naming, profile))
    for pair in target_pairs[1:]:
        components.extend(pair)

    return PreparedFanoutV2(
        supporting_components=tuple(components),
        selected_base_keys=tuple(selected),
        deferred_query_parameter_names=deferred,
        projection_facts=_projection_facts_v2(inp, keys, material),
    )


#: The route's topology spelling. Pinned against the legacy composition's
#: served summary by ``test_the_ported_constants_are_pinned_to_the_legacy_composition``.
_TOPOLOGY_V2 = "db_source->transform->rest_fanout"


def _projection_facts_v2(
    inp: ComposeDbRestFanoutInputV2,
    keys: _FanoutKeysV2,
    material: FanoutComponentMaterialV2,
) -> RecipeUnitProjectionFactsV1:
    """The trusted facts the #157 projection needs and the root cannot carry.

    Part identities and kinds, which components each part owns (from the SAME
    key derivation the executor uses), the declared status-code expectations,
    and each transform operation's ``documentation_hint`` — caller content the
    legacy flows row served but the legacy (and so the v2) map component never
    carried; keeping it OFF the map config keeps the generated components
    byte-identical to the legacy outcome. Leg counts, ordinals, handoff and
    binding modes are NOT here: the projection derives them from the composed
    root and components.
    """
    return RecipeUnitProjectionFactsV1(
        process_key=inp.process_key,
        topology=_TOPOLOGY_V2,
        parts=(
            DerivedCompositionPartV1(key=inp.source.part_key, kind="db_source"),
            DerivedCompositionPartV1(key=inp.transform.part_key, kind="transform"),
            *(
                DerivedCompositionPartV1(key=target.part_key, kind="rest_target")
                for target in inp.targets
            ),
        ),
        source_connection_key=keys.db_connection,
        source_operation_key=keys.db_get_operation,
        transform_row_name=inp.transform.map_label or _TRANSFORM_ROW_NAME_V2,
        targets=tuple(
            RecipeTargetFactV1(
                part_key=target.part_key,
                connection_key=connection,
                operation_key=operation,
                expected_status_codes=tuple(target.expected_status_codes),
            )
            for target, (_prefix, connection, operation) in zip(inp.targets, keys.targets)
        ),
        documentation_hints=tuple(
            RecipeOperationHintV1(
                operation_type=operation.operation_type,
                target_path=target_path,
                documentation_hint=operation.documentation_hint,
            )
            for operation in material.transform.operations
            if operation.documentation_hint is not None
            for target_path in (
                operation.outputs
                if isinstance(operation, FanoutScriptReferenceOperationV2)
                else (operation.target_path,)
            )
        ),
    )


def _emit_db_components_v2(
    inp: ComposeDbRestFanoutInputV2,
    material: FanoutComponentMaterialV2,
    keys: _FanoutKeysV2,
    context: PrimitiveBuildContext,
) -> _List[IntegrationComponentSpec]:
    """Port of ``_build_db_extract_params`` + ``_rt().DbExtractPrimitive``."""
    read = material.source.read
    connection = material.source.connection
    if isinstance(connection, FanoutDbCreateMaterialV2):
        connection_params: _Dict[str, _Any] = {
            "mode": "create",
            "driver_id": connection.driver,
            "auth_mode": inp.source.auth_mode,
            "host": connection.host,
            "port": connection.port,
            "dbname": connection.database,
        }
        if connection.username is not None:
            connection_params["username"] = connection.username
        if connection.credential_ref is not None:
            connection_params["credential_ref"] = connection.credential_ref
    elif isinstance(connection, FanoutConnectionReuseMaterialV2):
        connection_params = {"mode": "reuse"}
        if connection.component_id:
            connection_params["component_id"] = connection.component_id
        if connection.component_name:
            connection_params["component_name"] = connection.component_name
    else:
        # A base-key binding: the connection exists already; only its key is
        # read. The placeholder selector never reaches a spec (see below).
        connection_params = {"mode": "reuse", "component_id": keys.db_connection}

    read_profile: _Dict[str, _Any] = {
        "query": read.sql,
        "output_fields": [
            {"name": field.name, "data_type": field.data_type, "mandatory": field.required}
            for field in read.result_fields
        ],
    }
    if read.bind_parameters:
        read_profile["parameters"] = [
            {"name": parameter.name, "mappable": parameter.direction == "in"}
            for parameter in read.bind_parameters
        ]
    operation: _Dict[str, _Any] = {}
    if read.batch_size is not None:
        operation["batch_count"] = read.batch_size
    if read.max_rows is not None:
        operation["max_rows"] = read.max_rows

    source_prefix = _namespaced_v2(inp, _rt().SOURCE_PREFIX)
    params = _builder_check(
        "/component_material/source",
        lambda: _rt().DbExtractParameters.model_validate(
            {
                "key_prefix": source_prefix,
                "connection": connection_params,
                "read_profile": read_profile,
                "operation": operation,
                "component_names": {},
            }
        ),
    )
    if inp.source.connection_key is None:
        return _builder_check(
            "/component_material/source",
            lambda: _rt().DbExtractPrimitive.emit_components(context, params),
        )
    folder = context.folder_path
    return [
        _builder_check(
            "/component_material/source/read",
            lambda: _rt().DbExtractPrimitive._emit_read_profile(
                context, params, keys.db_read_profile, folder
            ),
        ),
        _builder_check(
            "/component_material/source/read",
            lambda: _rt().DbExtractPrimitive._emit_get_operation(
                context,
                params,
                keys.db_get_operation,
                keys.db_connection,
                keys.db_read_profile,
                folder,
            ),
        ),
    ]


def _emit_transform_components_v2(
    inp: ComposeDbRestFanoutInputV2,
    material: FanoutComponentMaterialV2,
    keys: _FanoutKeysV2,
    context: PrimitiveBuildContext,
) -> _List[IntegrationComponentSpec]:
    """Port of ``_build_field_map_params`` + ``_rt().FieldMapPrimitive``.

    Map scripts take the primitive's script-component REFERENCE path: the
    script is a base component, referenced by ``$ref``, never inlined.
    """
    read = material.source.read
    direct: _List[_Dict[str, _Any]] = []
    functions: _List[_Dict[str, _Any]] = []
    scripts: _List[_Dict[str, _Any]] = []
    for operation in material.transform.operations:
        if isinstance(operation, FanoutDirectOperationV2):
            direct.append(
                {"source_field": operation.source_field, "target_path": operation.target_path}
            )
        elif isinstance(operation, FanoutFunctionOperationV2):
            entry: _Dict[str, _Any] = {
                "function_type": operation.function_type,
                "inputs": list(operation.inputs),
                "target_path": operation.target_path,
            }
            if operation.parameters:
                entry["parameters"] = {
                    parameter.name: parameter.value for parameter in operation.parameters
                }
            functions.append(entry)
        else:
            scripts.append(
                {
                    "inputs": [
                        {"source_path": name, "input_name": name} for name in operation.inputs
                    ],
                    "outputs": [
                        {"output_name": path.rsplit("/", 1)[-1], "target_path": path}
                        for path in operation.outputs
                    ],
                    "script_component_ref": f"$ref:{operation.script_component_key}",
                }
            )
    params = _builder_check(
        "/component_material/transform",
        lambda: _rt().FieldMapParameters.model_validate(
            {
                "key_prefix": _namespaced_v2(inp, _rt().TRANSFORM_PREFIX),
                "source": {
                    "source_profile_id": f"$ref:{keys.db_read_profile}",
                    "source_profile_type": "profile.db",
                    "source_field_index": {
                        field.name: {"data_type": field.data_type, "mappable": True}
                        for field in read.result_fields
                    },
                },
                "target_payload_profile": material.payload_profile.model_dump(),
                "direct": direct,
                "map_function": functions,
                "map_script": scripts,
                "component_names": {},
            }
        ),
    )
    return _builder_check(
        "/component_material/transform",
        lambda: _rt().FieldMapPrimitive.emit_components(context, params),
    )


def _emit_target_components_v2(
    target: FanoutTargetV2,
    material: FanoutTargetMaterialV2,
    target_keys: Tuple[str, str, str],
    keys: _FanoutKeysV2,
    context: PrimitiveBuildContext,
    material_index: int,
) -> _List[IntegrationComponentSpec]:
    """Port of ``_fanout_target_params`` + ``_rt().RestSendWithRetryPrimitive``."""
    prefix, connection_key, operation_key = target_keys
    base = f"/component_material/targets/{material_index}"
    label = _derived_label_v2(target)
    connection = material.connection
    if isinstance(connection, FanoutRestCreateMaterialV2):
        connection_params: _Dict[str, _Any] = {
            "mode": "create",
            "base_url": connection.base_url,
            "auth": _rt().REST_CREATE_AUTH_MAP["none"],
        }
    elif isinstance(connection, FanoutConnectionReuseMaterialV2):
        connection_params = {"mode": "reuse"}
        if connection.component_id:
            connection_params["component_id"] = connection.component_id
        if connection.component_name:
            connection_params["component_name"] = connection.component_name
    else:
        connection_params = {"mode": "reuse", "component_id": connection_key}
    operation: _Dict[str, _Any] = {
        "method": target.method,
        "path": material.operation.path,
        # Every leg sends the ONE shared payload profile the transform produces.
        "request_profile_id": f"$ref:{keys.target_profile}",
        "request_profile_type": "json",
    }
    literal = {
        parameter.name: parameter.literal_value
        for parameter in material.operation.query_parameters
        if parameter.value_source == "literal" and parameter.literal_value is not None
    }
    if literal:
        operation["query_parameters"] = literal
    params = _builder_check(
        base,
        lambda: _rt().RestSendWithRetryParameters.model_validate(
            {
                "key_prefix": prefix,
                "connection": connection_params,
                "operation": operation,
                "component_names": {
                    "connection": f"{context.component_prefix} {label} REST Connection",
                    "operation": f"{context.component_prefix} {label} REST Send",
                },
            }
        ),
    )
    if target.connection_key is None:
        return _builder_check(
            base, lambda: _rt().RestSendWithRetryPrimitive.emit_components(context, params)
        )
    return [
        _builder_check(
            base + "/operation",
            lambda: _rt().RestSendWithRetryPrimitive._emit_operation(
                context, params, operation_key, connection_key, context.folder_path
            ),
        )
    ]


def _emit_handoff_cache_v2(
    key: str, naming: FanoutNamingV2, profile: IntegrationComponentSpec
) -> IntegrationComponentSpec:
    """Port of ``composition._emit_handoff_cache_component``.

    The cache binds to the SAME shared payload profile every leg sends; its
    index keys on the profile's first mappable simple leaf, read from the
    profile's own generated field index.
    """
    field_index = _rt().JSONGeneratedProfileBuilder.build_field_index(dict(profile.config))
    leaf = next(
        (
            entry
            for entry in field_index.values()
            if entry.get("mappable") and entry.get("kind") == "simple"
        ),
        None,
    )
    if leaf is None:  # pragma: no cover — the contract check requires a bound leaf
        raise _refusal(
            COMPOSITION_UNSUPPORTED_TOPOLOGY, "/component_material/payload_profile", phase="input"
        )
    name = f"{naming.component_prefix} {_CACHE_NAME_SUFFIX_V2}"
    config: _Dict[str, _Any] = {
        "component_type": "documentcache",
        "component_name": name,
        "profile_type": "profile.json",
        "profile_id": f"$ref:{profile.key}",
        "indexes": [
            {
                "index_id": 1,
                "index_name": f"by {leaf['name']}",
                "keys": [
                    {
                        "id": int(leaf["key"]),
                        "element_key": str(leaf["key"]),
                        "name": f"{leaf['name']} ({leaf['path']})",
                    }
                ],
            }
        ],
    }
    if naming.folder_name:
        config["folder_path"] = naming.folder_name
    return IntegrationComponentSpec(
        key=key,
        type="documentcache",
        action="create",
        name=name,
        config=config,
        depends_on=[profile.key],
    )


# ---------------------------------------------------------------------------
# The PURE executor
# ---------------------------------------------------------------------------


def _root_v2(inp: ComposeDbRestFanoutInputV2, keys: _FanoutKeysV2) -> ProcessIRV1:
    """source -> shared map -> terminal Branch; the v1 root shape over v2 keys.

    The staging leg sits immediately before the FIRST cache consumer (Branch
    legs run sequentially, so write-before-read holds), and one stable
    ``$ref`` names the cache across the put and every get.
    """
    modes = _handoff_modes_v2(inp)
    cache_ref = f"$ref:{keys.cache}"
    legs: list = []
    staged = False
    for target, (_prefix, connection, operation) in zip(inp.targets, keys.targets):
        terminal = {
            "kind": "target",
            "connection_ref": f"$ref:{connection}",
            "operation_ref": f"$ref:{operation}",
            "label": None,
        }
        if modes[target.part_key] == "document_cache":
            if not staged:
                legs.append(
                    {
                        "steps": [],
                        "terminal": {
                            "kind": "cache_put",
                            "cache_ref": cache_ref,
                            "label": _STAGE_LABEL_V2,
                        },
                    }
                )
                staged = True
            legs.append(
                {
                    "steps": [
                        {"kind": "cache_get", "cache_ref": cache_ref, "label": _CONSUME_LABEL_V2}
                    ],
                    "terminal": terminal,
                }
            )
        else:
            legs.append({"steps": [], "terminal": terminal})
    return ProcessIRV1.model_validate(
        {
            "version": "1",
            "body": {
                "kind": "sequence",
                "steps": [
                    {
                        "kind": "source",
                        "connection_ref": f"$ref:{keys.db_connection}",
                        "operation_ref": f"$ref:{keys.db_get_operation}",
                        "label": None,
                    },
                    {
                        "kind": "map_ref",
                        "map_ref": f"$ref:{keys.transform_map}",
                        "label": _MAP_LABEL_V2,
                    },
                    {"kind": "branch", "label": _BRANCH_LABEL_V2, "legs": legs},
                ],
            },
        }
    )


def _extension_bindings_v2(
    inp: ComposeDbRestFanoutInputV2, keys: _FanoutKeysV2
) -> ProcessExtensionBindingsV1:
    """Per-environment override points, by the legacy archetype policy.

    The DB source declares its credential fields when reused, or when created
    with ``username_password``; a REST target declares credentials only when
    reused (a created REST connection is always unauthenticated); endpoint
    fields stay opt-out. A connection shared by several parts is declared once.
    """
    entries: _List[Tuple[str, str, _List[_Dict[str, str]]]] = []
    if inp.source.binding_mode == "reuse" or inp.source.auth_mode == "username_password":
        entries.append(
            (
                keys.db_connection,
                "database",
                _rt().db_connection_extension_fields(credentials=True, endpoint=False),
            )
        )
    for target, (_prefix, connection, _operation) in zip(inp.targets, keys.targets):
        entries.append(
            (
                connection,
                "rest",
                _rt().rest_connection_extension_fields(
                    credentials=(target.binding_mode == "reuse"), endpoint=False
                ),
            )
        )
    connections = []
    declared: set = set()
    for key, connector_type, fields in entries:
        if not fields or key in declared:
            continue
        declared.add(key)
        connections.append(
            ProcessConnectionOverrideV1(
                connection_id=f"$ref:{key}",
                connector_type=connector_type,
                fields=tuple(ProcessOverrideFieldV1(**field) for field in fields),
            )
        )
    return ProcessExtensionBindingsV1(connections=tuple(connections))


def _process_dependencies_v2(inp: ComposeDbRestFanoutInputV2, keys: _FanoutKeysV2) -> Tuple[str, ...]:
    """The root's declared edges, in the legacy main-process order."""
    ordered = [keys.db_connection, keys.db_get_operation, keys.transform_map]
    for _prefix, connection, operation in keys.targets:
        ordered.extend((connection, operation))
    if any(mode == "document_cache" for mode in _handoff_modes_v2(inp).values()):
        ordered.append(keys.cache)
    unique: _List[str] = []
    for key in ordered:
        if key not in unique:
            unique.append(key)
    return tuple(unique)


def _required_components_v2(
    inp: ComposeDbRestFanoutInputV2, keys: _FanoutKeysV2
) -> Tuple[Tuple[str, str], ...]:
    """Every component the root needs, with its type — for RequireComponentV1.

    A requirement per key is what makes preparer/executor key drift a HARD
    failure: the executor derives these keys without seeing the preparer's
    output, and the run refuses a required key no component carries.
    """
    required: _List[Tuple[str, str]] = [
        (keys.db_connection, "connector-settings"),
        (keys.db_read_profile, "profile.db"),
        (keys.db_get_operation, "connector-action"),
        (keys.target_profile, "profile.json"),
        (keys.transform_map, "transform.map"),
    ]
    for _prefix, connection, operation in keys.targets:
        required.extend(((connection, "connector-settings"), (operation, "connector-action")))
    if any(mode == "document_cache" for mode in _handoff_modes_v2(inp).values()):
        required.append((keys.cache, "documentcache"))
    unique: _List[Tuple[str, str]] = []
    for item in required:
        if item not in unique:
            unique.append(item)
    return tuple(unique)


def emit_db_rest_fanout_v2(inp: ComposeDbRestFanoutInputV2) -> Tuple[object, ...]:
    """The PURE v2 executor: closed recipe input in, one process unit out.

    Receives no component material. Emits, in order: one
    ``ProcessUnitContributionV1`` (root + envelope, explicit dependency edges at
    ``/unit/envelope/depends_on``), the recorded watermark intent when declared,
    and the requirements — the process, every supporting component by key and
    type, and the emitter capabilities.
    """
    _validate_fanout_topology_v2(inp, phase="execution")
    keys = _fanout_keys_v2(inp)
    naming = inp.naming
    envelope = ProcessComponentEnvelopeV1(
        component_key=inp.process_key,
        name=naming.process_name or f"{naming.component_prefix} {_PROCESS_NAME_SUFFIX_V2}",
        action="create",
        folder_name=naming.folder_name,
        depends_on=_process_dependencies_v2(inp, keys),
        process_extensions=_extension_bindings_v2(inp, keys),
    )
    contributions: list = [
        ProcessUnitContributionV1(
            contribution_kind="process_unit",
            version="1",
            contribution_id="unit.process",
            unit=ProcessAuthoringUnitV1(envelope=envelope, process_ir=_root_v2(inp, keys)),
        )
    ]
    if inp.watermark is not None:
        contributions.append(
            RecordedIntentContributionV1(
                contribution_kind="recorded_intent",
                version="1",
                intent_id="watermark",
                process_key=inp.process_key,
                declaration=WatermarkDeclarationV1(
                    source_profile_ref=f"$ref:{keys.db_read_profile}",
                    field=inp.watermark.field,
                    kind=inp.watermark.kind,
                    initial_value=inp.watermark.initial_value,
                    query_parameter_refs=inp.watermark.query_parameter_refs,
                ),
                status="recorded_not_wired",
            )
        )
    # Requirement ids are namespaced with the derived keys: two invocations in
    # one request require DIFFERENT subjects, and one id naming two subjects is
    # a composition conflict. The capability requirements are identical across
    # invocations and dedupe under the declared merge rule.
    scope = "req" if inp.key_namespace is None else f"req.{inp.key_namespace}"
    contributions.append(
        ConstraintRequirementV1(
            contribution_kind="constraint_requirement",
            version=_VERSION,
            requirement_id=f"{scope}.process",
            requirement=RequireProcessV1(kind="process", process_key=inp.process_key),
        )
    )
    for position, (key, component_type) in enumerate(_required_components_v2(inp, keys)):
        contributions.append(
            ConstraintRequirementV1(
                contribution_kind="constraint_requirement",
                version=_VERSION,
                requirement_id=f"{scope}.component.{position}",
                requirement=RequireComponentV1(
                    kind="component", component_key=key, component_type=component_type
                ),
            )
        )
    contributions.append(
        ConstraintRequirementV1(
            contribution_kind="constraint_requirement",
            version=_VERSION,
            requirement_id="req.capability.branch",
            requirement=RequireCapabilityV1(
                kind="capability",
                authority="process_emitter",
                subject="branch",
                required_state="supported",
            ),
        )
    )
    if any(mode == "document_cache" for mode in _handoff_modes_v2(inp).values()):
        contributions.append(
            ConstraintRequirementV1(
                contribution_kind="constraint_requirement",
                version=_VERSION,
                requirement_id="req.capability.cache",
                requirement=RequireCapabilityV1(
                    kind="capability",
                    authority="process_emitter",
                    subject="doccacheload",
                    required_state="supported",
                ),
            )
        )
    return tuple(contributions)


__all__ = [
    "ComposeDbRestFanoutInputV1",
    "ComposeDbRestFanoutInputV2",
    "FanoutComponentMaterialV2",
    "FanoutTargetV1",
    "PreparedFanoutV2",
    "emit_db_rest_fanout",
    "emit_db_rest_fanout_v2",
    "prepare_db_rest_fanout_v2",
]
