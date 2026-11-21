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
    FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED,
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
        RestClientOperationBuilder,
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
        RestClientOperationBuilder=RestClientOperationBuilder,
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
    """Create a database connection. Value rules are ``DbCreateSettings``'s.

    The credential boundary also refuses a ``host`` carrying userinfo or a JDBC
    property, a ``database`` carrying a JDBC property, and a ``credential_ref``
    that is not an opaque ``credential://`` reference.
    """

    mode: _Literal["create"]
    driver: _DbDriverV2  # type: ignore[valid-type]
    host: str = Field(
        min_length=1,
        description="Database host. No userinfo ('@') and no JDBC property (';' or '=').",
    )
    port: int = Field(default=_DEFAULT_DB_PORT_V2, ge=1, le=65535)
    database: str = Field(
        min_length=1,
        description="Database (catalog) name. No JDBC property (';' or '=').",
    )
    username: Optional[str] = None
    credential_ref: Optional[str] = Field(
        default=None,
        description=(
            "An OPAQUE secret-store reference of the form 'credential://...', "
            "never the secret itself; any other value is refused."
        ),
    )


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


#: D12 (#159 S1, architect decision A): the ONLY names under which fan-out
#: 2.0.0 emits a literal REST query VALUE — a structured literal parameter, or
#: a query pair inside ``base_url`` / ``path``. Exact and case-sensitive, after
#: the trim for a structured name and URL-decoding for an inline one. A
#: deny-list name classifier cannot close (a provider-specific name carrying an
#: opaque value is indistinguishable from data), so the vocabulary is CLOSED.
#: Watermark-sourced names are recorded, never emitted, and are not judged here.
#: The served text below is RENDERED from this constant.
LITERAL_QUERY_NAMES_V2: Tuple[str, ...] = ("filter", "limit", "mode", "offset")

#: The value-free cause for a literal query name outside that vocabulary, or
#: inline query syntax that cannot be read unambiguously.
_QUERY_NAME_CAUSE = FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED


def _published_query_names() -> str:
    return ", ".join(LITERAL_QUERY_NAMES_V2)


#: The architect's served guarantee (D12, "Served contract text"), with the
#: published names rendered from :data:`LITERAL_QUERY_NAMES_V2`.
LITERAL_QUERY_GUARANTEE_V2 = (
    "Fan-out 2.0.0 requires opaque `credential://` references in credential "
    "fields, rejects URL userinfo and JDBC property injection in checked "
    "fields, and limits literal REST queries to the published names ({names}) "
    "and the REST builder\u2019s value rules. Usernames, SQL, map constants, URL "
    "path text, and values under permitted query names are caller-authored "
    "data that may be persisted in component configuration or XML, so supply "
    "only non-secret data in those fields."
).format(names=_published_query_names())

# The request-target GRAMMAR (#159 S1, B9 structural fix for CDX r6-01..04,
# QA r8-01/02): an ALLOWLIST of what may be EMITTED, not an enumeration of
# refused syntax. ONE function owns it (:func:`_query_value_violation` for a
# value, :func:`_judge_request_target` for a whole target), inline and
# structured values both call it, and the served sentence is rendered from
# these constants.
_URI_UNRESERVED = (
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
)
#: Every character an EMITTED request target may carry raw: RFC 3986
#: unreserved + reserved, and ``%`` only as a two-hex-digit escape. ``#`` is
#: NOT here (a fragment never reaches the server; text after it would be
#: persisted and ignored), nor whitespace or control characters.
_URI_TARGET_EXTRA = ":/?[]@!$&'()*+,;=%"
#: The characters a query VALUE may carry raw besides unreserved and a valid
#: ``%XX``: never ``+`` (form space), ``;`` (a pair separator to some servers),
#: ``&``, ``?`` or ``#``. Escaped (``%2B``, ``%3B``, ``%26``) they are data.
_QUERY_VALUE_EXTRA = "!$'()*,:@/="
_HEX_DIGITS = "0123456789abcdefABCDEF"
#: Raw characters a PATH may not carry before its query (ARCH-r1-03): either
#: would continue a stored base URL's query as a new pair.
_PATH_FORBIDDEN_RAW = "&;"
#: How the bytes of a run of ``%XX`` escapes must decode (r7-03). ONE place:
#: :func:`_decoded` and :func:`_first_undecodable` decode with these, and the
#: served sentence renders them.
_ESCAPE_ENCODING = "utf-8"
_ESCAPE_ERRORS = "strict"
_TARGET_CHARS = frozenset(_URI_UNRESERVED + _URI_TARGET_EXTRA)
_VALUE_CHARS = frozenset(_URI_UNRESERVED + _QUERY_VALUE_EXTRA)
_NAME_CHARS = frozenset(_URI_UNRESERVED + "%")

#: The ONE rendered statement of the grammar.
QUERY_GRAMMAR_SENTENCE_V2 = (
    "The effective request target (base_url followed by path, judged as "
    "joined) may carry only RFC 3986 characters, with no whitespace, control "
    "character or '#', at most one '?', and '%' only as a two-hex-digit "
    "escape whose escaped bytes decode as {errors} {encoding}; userinfo is "
    "refused in a leading authority; its query must be name=value pairs "
    "separated by '&', each name (after percent-decoding) a published name and "
    "each value only unreserved characters ({unreserved}), %XX escapes and "
    "{extra}. A path may not carry a raw {path_raw} before its query (a "
    "stored connection URL may already end in a query); escaped they are data. "
    "The stored base_url and the stored path are each judged on their "
    "own as well as joined. A structured literal value follows the same value "
    "rule. The two are SENT differently: an inline query value (in base_url or "
    "path) is sent as written, so the server decodes its escapes (%26 arrives "
    "as '&'); a structured literal value is percent-encoded by the platform, "
    "'%' included, so a %XX in a structured value is sent as those literal "
    "characters, not as the character it encodes; a structured value is still "
    "limited to the value alphabet above."
).format(
    unreserved="A-Z a-z 0-9 " + "-._~",
    extra=" ".join(_QUERY_VALUE_EXTRA),
    errors=_ESCAPE_ERRORS,
    encoding=_ESCAPE_ENCODING.upper(),
    path_raw=" or ".join("'{0}'".format(char) for char in _PATH_FORBIDDEN_RAW),
)

#: The per-field rule every query-carrying material field states.
_LITERAL_QUERY_FIELD_RULE = (
    "A literal REST query value is emitted only under the published names "
    "({names}): any other name, or query syntax outside the grammar below "
    "(including a name repeated across base_url, path and query_parameters), "
    "is refused with {cause}; a value the REST operation builder refuses is "
    "refused with PLAINTEXT_SECRET_REJECTED. "
).format(names=_published_query_names(), cause=_QUERY_NAME_CAUSE) + (
    QUERY_GRAMMAR_SENTENCE_V2 + " " + LITERAL_QUERY_GUARANTEE_V2
)


class FanoutRestCreateMaterialV2(_FanoutMaterialModel):
    """Create an UNAUTHENTICATED REST connection (secured auth must be reused)."""

    mode: _Literal["create"]
    base_url: str = Field(
        min_length=1,
        description=(
            "Absolute http(s) base URL. Userinfo ('user:password@') is refused. "
        )
        + _LITERAL_QUERY_FIELD_RULE,
    )


FanoutRestConnectionMaterialV2 = _Annotated[
    _Union[FanoutRestCreateMaterialV2, FanoutConnectionReuseMaterialV2],
    Field(discriminator="mode"),
]


class FanoutQueryParameterV2(_FanoutMaterialModel):
    name: str = Field(
        min_length=1,
        description=(
            "Compared after trimming. A literal parameter's name must be one of "
            "the published names ({names}); a watermark-sourced name is recorded "
            "and never emitted."
        ).format(names=_published_query_names()),
    )
    value_source: _Literal["literal", "watermark"]
    literal_value: Optional[str] = Field(default=None, description=_LITERAL_QUERY_FIELD_RULE)


class FanoutRestOperationMaterialV2(_FanoutMaterialModel):
    path: str = Field(
        min_length=1,
        description="Request path. Userinfo is refused. " + _LITERAL_QUERY_FIELD_RULE,
    )
    query_parameters: Tuple[FanoutQueryParameterV2, ...] = Field(
        default=(),
        description="Names are compared after trimming; a repeated name is refused.",
    )


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
# The credential boundary (#159 S1, CDX-r1-01/02 structural fix)
# ---------------------------------------------------------------------------
#
# ONE check, applied to every field of the material that can carry a secret,
# driven by ONE table that classifies EVERY field of every model reachable from
# ``FanoutComponentMaterialV2`` (including the shared ``JSONPayloadProfile``
# tree). The walk reads the table for every field it visits and FAILS CLOSED on
# a field the table does not classify, so a field added to the material later
# cannot bypass the boundary silently: ``tests/test_recipe_component_material.py``
# pins the table's keys to the set derived from the models, and the
# secret-capable subset to the enumeration below.
#
# Secret-capable (a rule that can REFUSE plaintext):
#   FanoutDbCreateMaterialV2.credential_ref   credential_reference: the opaque
#       ``credential://`` form the REST/SOAP builders already enforce.
#   FanoutDbCreateMaterialV2.host             jdbc_host: no userinfo (``@``) and
#       no JDBC property (``;`` / ``=``) — the two ways a password rides a host.
#   FanoutDbCreateMaterialV2.database         jdbc_property: no ``;`` / ``=``
#       (a ``;password=...`` suffix lands in the JDBC URL).
#   FanoutRestCreateMaterialV2.base_url       url: no userinfo; every inline query
#       pair readable, under a published name, and with a value the REST builder
#       admits (D12).
#   FanoutRestOperationMaterialV2.path        url: same rule (a path may carry a
#       query string, or an authority when written scheme-/protocol-relative).
#   FanoutQueryParameterV2.name               literal_query_name: a LITERAL
#       parameter's name must be a published name (D12).
#   FanoutQueryParameterV2.literal_value      named_literal: the value must pass
#       the REST builder's value check.
#
# Every other field is classified ``None`` with its reason in the table. There
# is no ``jdbc_options`` (or other open map) in the v2 material: the only maps it
# can reach are the closed models below.
#
# Literal REST query NAMES are a closed vocabulary (D12,
# :data:`LITERAL_QUERY_NAMES_V2`); no name classifier exists. Query VALUES are
# judged by the REST BUILDER's own authority — the customProperties check
# ``RestClientOperationBuilder`` applies to the very ``query_parameters`` these
# values become (``REST_SECRET_VALUE_FORBIDDEN``) — asked, never re-typed.

_CREDENTIAL_REFERENCE_PREFIX = "credential://"
#: The cause a boundary refusal carries: the builders' own code for a plaintext
#: secret offered where only an opaque reference may cross.
_PLAINTEXT_SECRET_CAUSE = "PLAINTEXT_SECRET_REJECTED"

_SECRET_RULES = (
    "credential_reference",
    "jdbc_host",
    "jdbc_property",
    "url",
    "literal_query_name",
    "named_literal",
)


def _material_field_classes_v2() -> _Mapping[Tuple[type, str], Optional[str]]:
    """``(model, field) -> rule`` for EVERY material field; ``None`` = not a carrier.

    The reasons for ``None`` are stated per entry. "Free text" entries CAN hold
    arbitrary characters; they are classified non-carrier because no shape rule
    can tell a secret from legitimate content there and the contract declares a
    different, checked channel for the secret (``credential_ref``), not because
    they are physically incapable of holding one.
    """
    from ...patterns.archetype_parameters import JSONProfileNode

    return _MappingProxyType(
        {
            (FanoutComponentMaterialV2, "version"): None,  # closed literal
            (FanoutComponentMaterialV2, "source"): None,  # structural (model)
            (FanoutComponentMaterialV2, "transform"): None,  # structural (model)
            (FanoutComponentMaterialV2, "payload_profile"): None,  # structural (model)
            (FanoutComponentMaterialV2, "targets"): None,  # structural (models)
            (FanoutDbMaterialV2, "connection"): None,  # structural (variant)
            (FanoutDbMaterialV2, "read"): None,  # structural (model)
            (FanoutDbCreateMaterialV2, "mode"): None,  # closed literal
            (FanoutDbCreateMaterialV2, "driver"): None,  # closed literal
            (FanoutDbCreateMaterialV2, "host"): "jdbc_host",
            (FanoutDbCreateMaterialV2, "port"): None,  # bounded integer
            (FanoutDbCreateMaterialV2, "database"): "jdbc_property",
            # An identity, emitted as the connection's user; the secret half is
            # credential_ref. Azure-style ``user@server`` identities are
            # legitimate, so no shape rule separates a smuggled secret.
            (FanoutDbCreateMaterialV2, "username"): None,
            (FanoutDbCreateMaterialV2, "credential_ref"): "credential_reference",
            (FanoutConnectionReuseMaterialV2, "mode"): None,  # closed literal
            # Selectors of an EXISTING component (platform id / display name),
            # served on the endpoint rows by design; they name, never unlock.
            (FanoutConnectionReuseMaterialV2, "component_id"): None,
            (FanoutConnectionReuseMaterialV2, "component_name"): None,
            # Free text: query material emitted into the read profile by design
            # (the addendum's D1). A literal in it cannot be told from data.
            (FanoutDbReadMaterialV2, "sql"): None,
            (FanoutDbReadMaterialV2, "result_fields"): None,  # structural (models)
            (FanoutDbReadMaterialV2, "bind_parameters"): None,  # structural (models)
            (FanoutDbReadMaterialV2, "batch_size"): None,  # bounded integer
            (FanoutDbReadMaterialV2, "max_rows"): None,  # bounded integer
            # A field name (``DBResultField`` refuses path separators).
            (FanoutResultFieldV2, "name"): None,
            (FanoutResultFieldV2, "data_type"): None,  # closed literal
            (FanoutResultFieldV2, "required"): None,  # boolean
            (FanoutResultFieldV2, "description"): None,  # free text: profile docs
            (FanoutBindParameterV2, "name"): None,  # a statement parameter NAME; no value
            (FanoutBindParameterV2, "direction"): None,  # closed literal
            (FanoutTransformMaterialV2, "operations"): None,  # structural (variants)
            (FanoutDirectOperationV2, "operation_type"): None,  # closed literal
            # Field references, each checked against a DECLARED source field or
            # payload leaf by the producer -> consumer contract.
            (FanoutDirectOperationV2, "source_field"): None,
            (FanoutDirectOperationV2, "target_path"): None,
            (FanoutDirectOperationV2, "documentation_hint"): None,  # free text: served hint
            (FanoutFunctionOperationV2, "operation_type"): None,  # closed literal
            (FanoutFunctionOperationV2, "function_type"): None,  # a registered family name
            (FanoutFunctionOperationV2, "inputs"): None,  # declared source fields
            (FanoutFunctionOperationV2, "target_path"): None,  # a declared payload leaf
            (FanoutFunctionOperationV2, "parameters"): None,  # structural (models)
            (FanoutFunctionOperationV2, "documentation_hint"): None,  # free text: served hint
            # A name the family DECLARES (closed by ``FunctionFamily``).
            (FanoutFunctionParameterV2, "name"): None,
            # A scalar transform constant emitted into the map by design; its
            # name is family-declared (never secret-shaped), so no named rule
            # applies, and a constant cannot be told from a secret by shape.
            (FanoutFunctionParameterV2, "value"): None,
            (FanoutScriptReferenceOperationV2, "operation_type"): None,  # closed literal
            (FanoutScriptReferenceOperationV2, "script_component_key"): None,  # a bounded key
            (FanoutScriptReferenceOperationV2, "inputs"): None,  # declared source fields
            (FanoutScriptReferenceOperationV2, "outputs"): None,  # declared payload leaves
            (FanoutScriptReferenceOperationV2, "documentation_hint"): None,  # free text
            (JSONPayloadProfile, "format"): None,  # closed literal
            (JSONPayloadProfile, "root"): None,  # structural (model)
            (JSONProfileNode, "name"): None,  # a schema node name
            (JSONProfileNode, "kind"): None,  # closed literal
            (JSONProfileNode, "data_type"): None,  # closed literal
            (JSONProfileNode, "required"): None,  # boolean
            (JSONProfileNode, "description"): None,  # free text: profile docs
            (JSONProfileNode, "children"): None,  # structural (models)
            (FanoutTargetMaterialV2, "part_key"): None,  # must equal a raw-input part key
            (FanoutTargetMaterialV2, "connection"): None,  # structural (variant)
            (FanoutTargetMaterialV2, "operation"): None,  # structural (model)
            (FanoutTargetMaterialV2, "expected_payload_profile"): None,  # structural (model)
            (FanoutRestCreateMaterialV2, "mode"): None,  # closed literal
            (FanoutRestCreateMaterialV2, "base_url"): "url",
            (FanoutRestOperationMaterialV2, "path"): "url",
            (FanoutRestOperationMaterialV2, "query_parameters"): None,  # structural (models)
            # A LITERAL parameter's name must be published (D12).
            (FanoutQueryParameterV2, "name"): "literal_query_name",
            (FanoutQueryParameterV2, "value_source"): None,  # closed literal
            (FanoutQueryParameterV2, "literal_value"): "named_literal",
        }
    )


#: The REST builder's own code for a query/header pair it refuses as a secret.
_REST_SECRET_CODE = "REST_SECRET_VALUE_FORBIDDEN"


def _rest_query_value_is_secret(name: str, value: str) -> bool:
    """Whether the REST builder refuses ``name=value`` — its customProperties
    validator, the check the emitted operation's ``query_parameters`` meet at
    build time, read by error CODE only. Under a published name this judges
    the VALUE (JWT, ``Bearer``/``Basic`` schemes, long base64, ``[encrypted]``)."""
    error = _rt().RestClientOperationBuilder._validate_dict_param(
        {name: value}, "query_parameters"
    )
    return getattr(error, "error_code", None) == _REST_SECRET_CODE


def _escape_ok(text: str, index: int) -> bool:
    """Whether ``text[index]`` (a ``%``) starts a valid two-hex-digit escape."""
    escape = text[index + 1:index + 3]
    return len(escape) == 2 and all(char in _HEX_DIGITS for char in escape)


def _query_value_violation(raw: str) -> Optional[int]:
    """THE value grammar: the offset of the first character a query VALUE may
    not carry raw, or ``None``. Allowed: unreserved, a valid ``%XX`` escape,
    and :data:`_QUERY_VALUE_EXTRA`. Inline and structured values both call this."""
    for index, char in enumerate(raw):
        if char == "%":
            if not _escape_ok(raw, index):
                return index
        elif char not in _VALUE_CHARS:
            return index
    return None


def _decoded(raw: str) -> Optional[str]:
    """``raw`` percent-decoded ONCE, as strict UTF-8; ``None`` if undecodable."""
    from urllib.parse import unquote

    try:
        return unquote(raw, encoding=_ESCAPE_ENCODING, errors=_ESCAPE_ERRORS)
    except UnicodeDecodeError:
        return None


def _first_undecodable(text: str) -> Optional[int]:
    """The offset of the first ``%`` whose escaped byte cannot be decoded, or
    ``None`` (r7-05: so a pointer names the field that holds the bad escape).

    Escapes are decoded per RUN of consecutive ``%XX`` (a multi-byte UTF-8
    character spans one run; any raw character ends it). Assumes every ``%``
    already starts a well-formed escape."""
    index = 0
    length = len(text)
    while index < length:
        if text[index] != "%":
            index += 1
            continue
        start = index
        data = bytearray()
        while index + 2 < length and text[index] == "%":
            data.append(int(text[index + 1:index + 3], 16))
            index += 3
        try:
            bytes(data).decode(_ESCAPE_ENCODING, errors=_ESCAPE_ERRORS)
        except UnicodeDecodeError as exc:
            return start + 3 * exc.start
    return None


def _value_is_secret(name: str, raw: str) -> bool:
    """The REST builder's value check on the raw AND the once-decoded value."""
    decoded = _decoded(raw)
    return _rest_query_value_is_secret(name, raw) or (
        decoded is not None and _rest_query_value_is_secret(name, decoded)
    )


@_dataclass(frozen=True)
class _TargetVerdict:
    """A request-target judgment: ``cause``/``offset`` of the first refusal
    (``None`` when clean) and the decoded query names it carries."""

    cause: Optional[str]
    offset: int = 0
    names: Tuple[str, ...] = ()


def _judge_request_target(text: str) -> _TargetVerdict:
    """THE request-target grammar, applied to exactly the text that is EMITTED.

    1. every character is an RFC 3986 target character (``#``, whitespace and
       control characters never are), a ``%`` starts a valid escape, at most
       one ``?``, and no userinfo before the path;
    2. the query (after the ``?``) is ``pair ("&" pair)*`` with
       ``pair = name "=" value``: a raw name of unreserved characters and
       escapes that DECODES to a published name, never repeated, and a value
       that meets :func:`_query_value_violation`; an empty query is refused;
    3. only then each value meets the REST builder's value check (raw and
       once-decoded) — ``PLAINTEXT_SECRET_REJECTED``.
    """
    for index, char in enumerate(text):
        if char not in _TARGET_CHARS:
            return _TargetVerdict(_QUERY_NAME_CAUSE, index)
        if char == "%" and not _escape_ok(text, index):
            return _TargetVerdict(_QUERY_NAME_CAUSE, index)
    undecodable = _first_undecodable(text)
    if undecodable is not None:
        return _TargetVerdict(_QUERY_NAME_CAUSE, undecodable)
    authority_start = _leading_authority_start(text)
    if authority_start is not None:
        authority_end = len(text)
        for marker in "/?":
            found = text.find(marker, authority_start)
            if found >= 0:
                authority_end = min(authority_end, found)
        at = text.find("@", authority_start, authority_end)
        if at >= 0:
            return _TargetVerdict(_PLAINTEXT_SECRET_CAUSE, at)
    mark = text.find("?")
    if mark < 0:
        return _TargetVerdict(None)
    second = text.find("?", mark + 1)
    if second >= 0:
        return _TargetVerdict(_QUERY_NAME_CAUSE, second)
    query_start = mark + 1
    if query_start == len(text):
        return _TargetVerdict(_QUERY_NAME_CAUSE, mark)
    pairs: _List[Tuple[int, str, str]] = []
    seen: set = set()
    offset = query_start
    for segment in text[query_start:].split("&"):
        if "=" not in segment:
            return _TargetVerdict(_QUERY_NAME_CAUSE, offset)
        raw_name, raw_value = segment.split("=", 1)
        if any(char not in _NAME_CHARS for char in raw_name):
            return _TargetVerdict(_QUERY_NAME_CAUSE, offset)
        name = _decoded(raw_name)
        if name is None or name not in LITERAL_QUERY_NAMES_V2 or name in seen:
            return _TargetVerdict(_QUERY_NAME_CAUSE, offset)
        seen.add(name)
        violation = _query_value_violation(raw_value)
        if violation is not None or _decoded(raw_value) is None:
            return _TargetVerdict(
                _QUERY_NAME_CAUSE, offset + len(raw_name) + 1 + (violation or 0)
            )
        pairs.append((offset, name, raw_value))
        offset += len(segment) + 1
    for pair_offset, name, raw_value in pairs:
        if _value_is_secret(name, raw_value):
            return _TargetVerdict(_PLAINTEXT_SECRET_CAUSE, pair_offset)
    return _TargetVerdict(None, names=tuple(name for _o, name, _v in pairs))


def _leading_authority_start(text: str) -> Optional[int]:
    """Where the target's LEADING authority starts, or ``None`` (r7-02).

    Only a ``scheme://`` AT THE START (RFC 3986 scheme characters) or a
    scheme-relative ``//`` at the start of a path opens an authority; a
    ``://`` later in a path is path data (``/v1/tags://alice@example.org``)."""
    if text.startswith("//"):
        return 2
    marker = text.find("://")
    if marker <= 0:
        return None
    scheme = text[:marker]
    if not scheme[0].isascii() or not scheme[0].isalpha():
        return None
    if all(char.isascii() and (char.isalnum() or char in "+-.") for char in scheme):
        return marker + 3
    return None


def _userinfo_cause(value: str) -> Optional[str]:
    """The PER-FIELD userinfo check (a field's own authority, before joining)."""
    from urllib.parse import urlsplit

    try:
        parts = urlsplit(value.strip())
    except ValueError:
        return _PLAINTEXT_SECRET_CAUSE  # unparseable: refused rather than judged clean
    return _PLAINTEXT_SECRET_CAUSE if "@" in parts.netloc else None


def _breaches(rule: str, value: _Any, owner: _Any) -> Optional[str]:
    """The refusal CAUSE for ``value`` of a field classified ``rule``, or ``None``."""
    if value is None:
        return None
    text = str(value).strip()
    if rule == "credential_reference":
        if text.startswith(_CREDENTIAL_REFERENCE_PREFIX) and len(text) > len(
            _CREDENTIAL_REFERENCE_PREFIX
        ):
            return None
        return _PLAINTEXT_SECRET_CAUSE
    if rule == "jdbc_host":
        return _PLAINTEXT_SECRET_CAUSE if any(char in text for char in "@;=") else None
    if rule == "jdbc_property":
        return _PLAINTEXT_SECRET_CAUSE if any(char in text for char in ";=") else None
    if rule == "url":
        # Per-field userinfo only: the query grammar is judged on the JOINED
        # target (base_url + path), which only the preparer can assemble.
        return _userinfo_cause(text)
    if rule == "literal_query_name":
        if getattr(owner, "value_source", None) != "literal":
            return None  # a watermark-sourced name is recorded, never emitted
        return None if text in LITERAL_QUERY_NAMES_V2 else _QUERY_NAME_CAUSE
    if rule == "named_literal":
        name = str(getattr(owner, "name", "") or "").strip()
        if getattr(owner, "value_source", None) == "literal":
            # The SAME value grammar as an inline value (QA r8-02) — only for a
            # value that is EMITTED (r7-04) — then the builder.
            if _query_value_violation(text) is not None or _decoded(text) is None:
                return _QUERY_NAME_CAUSE
            return _PLAINTEXT_SECRET_CAUSE if _value_is_secret(name, text) else None
        # A watermark-sourced parameter's literal_value is never emitted, so no
        # grammar applies; the builder's secret check still does, as before B9.
        return _PLAINTEXT_SECRET_CAUSE if _rest_query_value_is_secret(name, text) else None
    return _PLAINTEXT_SECRET_CAUSE  # an unknown rule fails closed


def _check_credential_boundary_v2(material: FanoutComponentMaterialV2) -> None:
    """Refuse, value-free, the first material field that carries a plaintext secret.

    Walks the validated material; every field visited is looked up in
    :func:`_material_field_classes_v2`. An unclassified field is refused too
    (fail closed). The pointer is built from DECLARED field names and indices
    only, so it never carries caller content.
    """
    classes = _material_field_classes_v2()

    def walk(node: _Any, pointer: str) -> None:
        if isinstance(node, _BaseModel):
            for name in type(node).model_fields:
                here = f"{pointer}/{name}"
                key = (type(node), name)
                if key not in classes:
                    raise _refusal(_PLAINTEXT_SECRET_CAUSE, here, phase="input")
                value = getattr(node, name)
                rule = classes[key]
                cause = _breaches(rule, value, node) if rule is not None else None
                if cause is not None:
                    raise _refusal(cause, here, phase="input")
                walk(value, here)
        elif isinstance(node, (tuple, list)):
            for index, item in enumerate(node):
                walk(item, f"{pointer}/{index}")

    walk(material, "/component_material")


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
    ``deferred_query_parameter_names`` maps ``(owning root, FIRST target's
    operation key)`` to the watermark-sourced query parameter names the
    preparer validated: they are recorded intent, kept OUT of the emitted
    operation config, and handed to governance's watermark rule scoped to that
    operation AND its root (ARCH-r1-04: keyed by the operation alone, another
    root that also reaches the operation could borrow them).

    The repr names counts and keys only; component configs carry material.
    """

    supporting_components: Tuple[IntegrationComponentSpec, ...]
    selected_base_keys: Tuple[str, ...] = ()
    #: B12-01: ``(base key, raw-input pointer)`` per selected base bound in
    #: CREATE mode. Whether apply really writes it depends on the component
    #: plan's account resolution (a create matched BY NAME is reused), which
    #: only the workflow holds — it rechecks each one against that authority.
    create_bound_base_keys: Tuple[Tuple[str, str], ...] = ()
    #: B15: ``(connection key, path, path pointer, binding pointer)`` for EVERY
    #: REST target, whatever its declared binding. Whether apply writes the
    #: connection (its base URL was judged here, joined) or reuses one (its
    #: STORED base URL governs) is the component plan's decision, which only
    #: the workflow holds: it judges each target apply will not write against
    #: the stored URL (:func:`judge_reused_request_target`), at compile and
    #: again at apply before the first write.
    rest_request_targets: Tuple[Tuple[str, str, str, str], ...] = ()
    deferred_query_parameter_names: Tuple[Tuple[Tuple[str, str], Tuple[str, ...]], ...] = ()
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
    if binding_mode is None:
        return
    # ARCH-r1-01: what a base IS is decided by what apply DOES with it — asked of
    # apply's own predicate (``apply_writes_component_config``) under EVERY
    # served conflict policy, never read off a truthy flag. A reuse binding
    # needs a genuinely reference-only base that apply writes under NO policy
    # (``reference_only`` the boolean True — a string ``"true"`` beside an
    # ``update`` action is WRITTEN). A create binding needs a base apply writes
    # under EVERY policy: one it would reuse (a ``component_id`` under
    # ``reuse``) stores structured auth/URL fields that describe nothing written.
    written = _base_written_under(component)
    if binding_mode == "reuse":
        if config.get("reference_only") is not True or any(written):
            raise _refusal(COMPOSITION_CONTRACT_MISMATCH, path, phase="input")
    elif bool(config.get("reference_only")) or not all(written):
        raise _refusal(COMPOSITION_CONTRACT_MISMATCH, path, phase="input")


def _served_conflict_policies() -> Tuple[str, ...]:
    """The conflict policies the authoring surface serves — DERIVED from its model."""
    from typing import get_args

    from ...models.authoring_workflow import RecipeAuthoringIntentV1

    return tuple(get_args(RecipeAuthoringIntentV1.model_fields["conflict_policy"].annotation))


def _base_written_under(component: IntegrationComponentSpec) -> Tuple[bool, ...]:
    """Per served conflict policy: does apply WRITE this base's configuration?"""
    from ...categories.integration_builder import apply_writes_component_config

    # ``existing_ids=None``: the recipe layer touches no account (like the
    # engine), so the answer is the DECLARED one — a create matched to an
    # existing component by NAME is decided at apply. That only ever counts a
    # base as written, so its config is checked; it never skips a check.
    return tuple(
        bool(apply_writes_component_config(component, policy, existing_ids=None))
        for policy in _served_conflict_policies()
    )


#: ARCH-r1-02: the config keys a selected CREATE-mode base may carry, per family,
#: each mapped to the material field whose credential-boundary rule governs it
#: (so the rule is looked up in ONE table, :func:`_material_field_classes_v2`).
#: ``None`` = a key that is not a value carrier (closed literal, identity,
#: placement, or checked by its own rule elsewhere). EVERY OTHER KEY is refused
#: (fail closed): the material cannot express it, so neither may a selected base
#: — notably the DB builder's ``additional`` (a JDBC suffix appended verbatim),
#: ``connection_url``/``custom_class_name`` (the custom-URL shape), ``pooling``,
#: and the REST builder's ``username``/``credential_ref``/``oauth2``/
#: ``default_headers`` (a created REST connection is unauthenticated).
def _selected_base_key_classes() -> _Mapping[str, _Mapping[str, Optional[Tuple[type, str]]]]:
    shared = {
        "connector_type": None,  # checked: must equal the family
        "component_name": None,  # the component's display name
        "folder_name": None,  # placement
        "folder_path": None,  # placement
        "folder_id": None,  # placement
        "description": None,  # free text: component docs
        "reference_only": None,  # checked by the binding rule
        "component_id": None,  # checked by the binding rule (apply's predicate)
    }
    return _MappingProxyType(
        {
            "database": _MappingProxyType(
                dict(
                    shared,
                    auth_mode=None,  # checked: must equal the declared auth mode
                    driver_id=(FanoutDbCreateMaterialV2, "driver"),
                    host=(FanoutDbCreateMaterialV2, "host"),
                    port=(FanoutDbCreateMaterialV2, "port"),
                    dbname=(FanoutDbCreateMaterialV2, "database"),
                    username=(FanoutDbCreateMaterialV2, "username"),
                    credential_ref=(FanoutDbCreateMaterialV2, "credential_ref"),
                )
            ),
            "rest": _MappingProxyType(
                dict(
                    shared,
                    auth=None,  # checked: must be the unauthenticated wire value
                    base_url=(FanoutRestCreateMaterialV2, "base_url"),
                )
            ),
        }
    )


def _check_selected_base_config(
    base_catalog: _Mapping[str, IntegrationComponentSpec],
    key: str,
    *,
    family: str,
    path: str,
) -> None:
    """The credential boundary over a WRITTEN selected base's config (ARCH-r1-02).

    The same rules the material walk applies (looked up in
    :func:`_material_field_classes_v2` through the key map above), plus fail
    closed on any key the material cannot express. Value-free: the pointer is
    the raw-input field that selected the base."""
    config = (base_catalog[key].config or {}) if key in base_catalog else {}
    classes = _material_field_classes_v2()
    allowed = _selected_base_key_classes()[family]
    for name, value in config.items():
        if name not in allowed:
            raise _refusal(COMPOSITION_CONTRACT_MISMATCH, path, phase="input")
        field = allowed[name]
        rule = classes.get(field) if field is not None else None
        cause = _breaches(rule, value, None) if rule is not None else None
        if cause is not None:
            raise _refusal(cause, path, phase="input")


def _selected_base_url(base_catalog: _Mapping[str, IntegrationComponentSpec], key: str) -> str:
    config = (base_catalog[key].config or {}) if key in base_catalog else {}
    value = config.get("base_url")
    return value if isinstance(value, str) else ""


def _path_continues_a_query(path: str) -> bool:
    """Whether a path's PRE-QUERY portion carries a raw pair separator
    (:data:`_PATH_FORBIDDEN_RAW`) — ARCH-r1-03. The path is appended to the
    connection URL, and a stored URL this request cannot see (a reused
    connection) may already end in a query: ``&k=v`` would then be a pair.
    Escaped (``%26``) it stays path data."""
    head = path.split("?", 1)[0]
    return any(char in head for char in _PATH_FORBIDDEN_RAW)


def _rest_request_targets_v2(
    inp: ComposeDbRestFanoutInputV2,
    ordered_material: _List[Tuple[int, FanoutTargetMaterialV2]],
    keys: _FanoutKeysV2,
) -> Tuple[Tuple[str, str, str, str], ...]:
    """Every REST target, with the pointer of the field that BINDS its
    connection (B15): the material ``connection`` (create or reuse) or the
    raw-input ``connection_key`` that selected a base. No target is skipped by
    its declaration: a connection requested in create mode is still reused when
    the component plan binds an existing same-name component."""
    found = []
    for index, (target, (material_index, entry)) in enumerate(zip(inp.targets, ordered_material)):
        binding = (
            f"/raw_input/targets/{index}/connection_key"
            if target.connection_key is not None
            else f"/component_material/targets/{material_index}/connection"
        )
        found.append(
            (
                keys.targets[index][1],
                entry.operation.path.strip(),
                f"/component_material/targets/{material_index}/operation/path",
                binding,
            )
        )
    return tuple(found)


#: Which field a reused-target refusal points at (B15-4): the connection's
#: STORED base URL (it carries a query or fragment — the binding is at fault),
#: or the authored PATH.
REUSED_TARGET_STORED_BASE = "stored_base"
REUSED_TARGET_PATH = "path"


#: B20-5: the rules for a path sent through a REUSED connection, as constants
#: the judge enforces and the served sentence renders. A stored base carrying
#: one of these is refused for reuse (the path would land inside it).
_STORED_BASE_FORBIDDEN = "?#"
#: With an UNKNOWN base the path must start with this ...
_UNKNOWN_BASE_PATH_PREFIX = "/"
#: ... and carry no such character before its query.
_UNKNOWN_BASE_FORBIDDEN_BEFORE_QUERY = "="

#: The ONE rendered statement of the reuse rules (B20-5), served in the
#: fan-out material guarantee. The joined grammar alone does not imply them: a
#: stored base ending in '?mode=full' joined with '/orders' satisfies it.
REUSED_BASE_SENTENCE_V2 = (
    "A path sent through a REUSED connection is judged against the base URL "
    "that connection stores in the account: a stored base carrying {forbidden} "
    "is refused for reuse (the path would be appended inside it); a stored "
    "base that cannot be read, is blank, is an unresolved extension "
    "placeholder, or is not an absolute http(s) URL is UNKNOWN, and then the "
    "path must start with '{prefix}' and carry no '{before}' before any '?'."
).format(
    forbidden=" or ".join("'{0}'".format(char) for char in _STORED_BASE_FORBIDDEN),
    prefix=_UNKNOWN_BASE_PATH_PREFIX,
    before=_UNKNOWN_BASE_FORBIDDEN_BEFORE_QUERY,
)


def _stored_base_is_known(base: str) -> bool:
    """Whether a stored url is a CONCRETE base (B20-1): not blank, not the
    connector builder's unresolved-endpoint placeholder (its own definition,
    ``is_unresolved_endpoint_value``), and an absolute http(s) URL with an
    authority (the route digest's own bar). Anything else addresses nothing
    knowable, so the path is judged as against an unknown base."""
    from urllib.parse import urlsplit

    from ...categories.components.builders.connector_builder import (
        is_unresolved_endpoint_value,
    )

    candidate = base.strip()
    if not candidate or is_unresolved_endpoint_value(candidate):
        return False
    try:
        parsed = urlsplit(candidate)
    except ValueError:
        return False
    return parsed.scheme.lower() in ("http", "https") and bool(parsed.netloc)


def judge_reused_request_target(
    stored_base_url: Optional[str], path: str
) -> Optional[Tuple[str, str]]:
    """``(cause, fault)`` for a path appended to a REUSED connection's stored
    base URL, or ``None`` (B14 ARCH-r1-03; B15). ``fault`` is
    :data:`REUSED_TARGET_STORED_BASE` or :data:`REUSED_TARGET_PATH`.

    With the stored base URL in hand (read from the account): a base that
    already carries a query or fragment would receive the path INSIDE it, so it
    is refused as the STORED BASE's fault, whatever the path; otherwise every
    joined target meets the grammar, judged only for what the PATH contributes
    (the stored URL is the account's, not the caller's). Without it (no
    account, an unresolved, id-less or unreadable reuse, a blank stored url) the rule FAILS CLOSED
    on the path: it must start with '/' and carry no '=' before any '?' — the
    two shapes that could complete a pair a stored query left open."""
    if stored_base_url is None or not _stored_base_is_known(stored_base_url):
        # B16-2 / B20-1: no concrete base (blank, the unresolved placeholder,
        # not an absolute http(s) URL) — unknown, so the fail-closed rule
        # judges the path.
        head = path.split("?", 1)[0]
        if (
            not path.startswith(_UNKNOWN_BASE_PATH_PREFIX)
            or _UNKNOWN_BASE_FORBIDDEN_BEFORE_QUERY in head
        ):
            return (_QUERY_NAME_CAUSE, REUSED_TARGET_PATH)
        return None
    base = stored_base_url.strip()
    if any(char in base for char in _STORED_BASE_FORBIDDEN):
        return (_QUERY_NAME_CAUSE, REUSED_TARGET_STORED_BASE)
    for joined, path_start in _joined_targets(base, path):
        verdict = _judge_request_target(joined)
        if verdict.cause is not None and verdict.offset >= path_start:
            return (verdict.cause, REUSED_TARGET_PATH)
    return None


def _joined_targets(base: str, path: str) -> Tuple[Tuple[str, int], ...]:
    """The request target(s) a base URL and a path can be joined into, each
    with the offset where the PATH's contribution starts.

    The REST Client operation path is APPENDED to the connection URL (the
    builders' own contract: "Path appended onto the connection's base_url ...
    REST Client preserves the path verbatim"); whether the runtime inserts a
    single ``/`` at the boundary is not documented, and the route digest
    (``connector_replay.digests._effective_path``) models that slash join. Both
    are judged, so a split that is clean under one join cannot smuggle a pair
    under the other."""
    if not base:
        return ((path, 0),)
    concatenated = (base + path, len(base))
    trimmed = base.rstrip("/")
    slashed = (trimmed + "/" + path.lstrip("/"), len(trimmed))
    return (concatenated,) if slashed[0] == concatenated[0] else (concatenated, slashed)


def _check_request_targets_v2(
    inp: ComposeDbRestFanoutInputV2,
    ordered_material: _List[Tuple[int, FanoutTargetMaterialV2]],
    authorized_paths: _Mapping[int, Tuple[str, str]],
    base_catalog: _Mapping[str, IntegrationComponentSpec],
) -> None:
    """Judge each target's EFFECTIVE request target (QA r8-01): the connection
    URL joined with the operation path, exactly as emitted (authority-stripped
    material; a selected create-mode base's ``base_url`` as written), AFTER
    each stored field is judged on its own (r7-01). A reused connection's URL
    is not in hand, so its path is judged alone.

    The refusal points at the field that CONTRIBUTES the offending text: the
    path when the offset falls in the path's portion, else the base URL (or,
    for a selected base, the raw-input ``connection_key`` that selected it).
    A structured parameter repeating an inline name is refused at its name.
    """
    for index, (target, (material_index, entry)) in enumerate(zip(inp.targets, ordered_material)):
        base_pointer = f"/component_material/targets/{material_index}"
        base_url, path = authorized_paths[index]
        if isinstance(entry.connection, FanoutRestCreateMaterialV2):
            base_field = base_pointer + "/connection/base_url"
        elif target.connection_key is not None and target.binding_mode == "create":
            base_url = _selected_base_url(base_catalog, target.connection_key)
            base_field = f"/raw_input/targets/{index}/connection_key"
        else:
            base_url, base_field = "", base_pointer + "/operation/path"
        # r7-01: every PERSISTED field is judged on its own first — the stored
        # base_url (material or selected base) and the stored path — because
        # each is written as-is, and appending the path can hide a base value
        # from the builder's whole-value patterns.
        if base_url:
            standalone = _judge_request_target(base_url)
            if standalone.cause is not None:
                raise _refusal(standalone.cause, base_field, phase="input")
        standalone = _judge_request_target(path)
        if standalone.cause is None and _path_continues_a_query(path):
            standalone = _TargetVerdict(_QUERY_NAME_CAUSE)
        if standalone.cause is not None:
            raise _refusal(standalone.cause, base_pointer + "/operation/path", phase="input")
        names: set = set()
        for joined, path_start in _joined_targets(base_url, path):
            verdict = _judge_request_target(joined)
            if verdict.cause is not None:
                field = (
                    base_pointer + "/operation/path"
                    if verdict.offset >= path_start
                    else base_field
                )
                raise _refusal(verdict.cause, field, phase="input")
            names.update(verdict.names)
        for qp_index, parameter in enumerate(entry.operation.query_parameters):
            if parameter.name.strip() in names:
                raise _refusal(
                    _QUERY_NAME_CAUSE,
                    f"{base_pointer}/operation/query_parameters/{qp_index}/name",
                    phase="input",
                )


def _check_base_auth(
    base_catalog: _Mapping[str, IntegrationComponentSpec],
    key: str,
    *,
    config_key: str,
    expected: str,
    path: str,
) -> None:
    """A CREATE-mode base connection's auth must be the one the recipe declares.

    The declared auth decides which credential fields become per-environment
    override points (``_extension_bindings_v2``), so a base connection whose own
    config says otherwise would ship credentials nobody can override — or
    declare override points for credentials it does not have (CDX-r1-04). The
    builders REQUIRE the auth key on a created connection (no default), so an
    absent value is a mismatch too.

    A base connection whose write submits raw ``config.xml``
    (``submits_raw_component_xml``, the predicate every write route asks) is
    REFUSED (CDX-r2-03): the route posts those bytes verbatim, so the
    structured auth field beside them describes nothing that will be written,
    and no authority in this repository reads a connection's auth out of
    submitted XML (``live_identity_from_component_xml`` reads only type,
    subtype, verb and path). Unverifiable is not verified.
    """
    from ...categories.components._shared import submits_raw_component_xml

    config = (base_catalog[key].config or {}) if key in base_catalog else {}
    if submits_raw_component_xml(config) or config.get(config_key) != expected:
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
    2. the credential boundary (one check over every secret-capable field);
    3. material <-> input agreement (one material entry per target part key,
       connection presence and binding mode, base components by key, type and
       — for a create-mode base connection — auth);
    4. value rules through the neutral authority models. The VALIDATED values
       those models return (stripped, normalized) are what every later step
       matches against and what the components carry (CDX-r1-05) — never the
       raw material, so v2 emits what the legacy route emits;
    5. the producer -> consumer contract (source fields, transform operations,
       the shared payload profile, each target's asserted leaf contract);
    6. the first-target watermark rule;
    7. ONLY THEN the components, through ``db_extract``, ``field_map`` and
       ``rest_send`` — the DB connection/profile/operation, the shared JSON
       profile and map, one REST connection/operation pair per target, and the
       staged-handoff cache when any link stages.
    """
    _validate_fanout_topology_v2(inp, phase="input")
    keys = _fanout_keys_v2(inp)
    naming = inp.naming
    selected: _List[str] = []
    create_bound: _List[Tuple[str, str]] = []

    # -- 2. the credential boundary ----------------------------------------
    _check_credential_boundary_v2(material)

    # -- 3. agreement ------------------------------------------------------
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
        if inp.source.binding_mode == "create":
            _check_base_auth(
                base_catalog,
                inp.source.connection_key,
                config_key="auth_mode",
                expected=str(inp.source.auth_mode),
                path="/raw_input/source/auth_mode",
            )
            _check_selected_base_config(
                base_catalog,
                inp.source.connection_key,
                family="database",
                path="/raw_input/source/connection_key",
            )
            create_bound.append((inp.source.connection_key, "/raw_input/source/connection_key"))
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
            if target.binding_mode == "create":
                # A created REST connection is always unauthenticated (the
                # extension policy declares no credential fields for it).
                _check_base_auth(
                    base_catalog,
                    target.connection_key,
                    config_key="auth",
                    expected=_rt().REST_CREATE_AUTH_MAP["none"],
                    path=f"/raw_input/targets/{index}/connection_key",
                )
                # The selected base's base_url is WRITTEN (the plan creates the
                # component) without the material walk ever seeing it: its own
                # userinfo check here; its query is judged JOINED with the path
                # below (D12, QA r8-01).
                # Its config meets the same credential boundary as material
                # (ARCH-r1-02): base_url's userinfo rule included.
                _check_selected_base_config(
                    base_catalog,
                    target.connection_key,
                    family="rest",
                    path=f"/raw_input/targets/{index}/connection_key",
                )
                create_bound.append(
                    (target.connection_key, f"/raw_input/targets/{index}/connection_key")
                )
            if target.connection_key not in selected:
                selected.append(target.connection_key)

    _check_request_targets_v2(
        inp,
        ordered_material,
        {
            index: (
                entry.connection.base_url.strip()
                if isinstance(entry.connection, FanoutRestCreateMaterialV2)
                else "",
                entry.operation.path.strip(),
            )
            for index, (_material_index, entry) in enumerate(ordered_material)
        },
        base_catalog,
    )

    # -- 4. value rules through the authorities ----------------------------
    authorized = _authorize_material_v2(inp, material, ordered_material, base_catalog)
    result_fields = authorized.result_fields

    # -- 5. producer -> consumer contract ----------------------------------
    source_fields = {field.name: field for field in result_fields}
    shared_leaves, shared_required = _leaf_contract(material.payload_profile)
    bound_paths: set = set()
    for op_index, operation in enumerate(authorized.operations):
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

    # -- 6. the first-target watermark rule --------------------------------
    # Matched on the AUTHORITY-validated parameters (stripped names), in the
    # material's own order, so a pointer index still addresses the request.
    deferred: Tuple[Tuple[Tuple[str, str], Tuple[str, ...]], ...] = ()
    for position, ((material_index, _entry), (_settings, request)) in enumerate(
        zip(ordered_material, authorized.targets)
    ):
        names = tuple(
            parameter.name
            for parameter in request.query_parameters
            if parameter.value_source == "watermark"
        )
        if position > 0 and names:
            first = next(
                qp_index
                for qp_index, parameter in enumerate(request.query_parameters)
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
            deferred = (((inp.process_key, keys.targets[0][2]), names),)
    if inp.watermark is not None:
        # ARCH-r1-14: a repeated ref is refused HERE, at the authored field,
        # before any component exists (the declaration model would refuse it
        # later, in the executor, without the pointer).
        seen_refs: set = set()
        for ref_index, name in enumerate(inp.watermark.query_parameter_refs):
            if name in seen_refs:
                raise _refusal(
                    GOVERNANCE_WATERMARK_INCONSISTENT,
                    f"/raw_input/watermark/query_parameter_refs/{ref_index}",
                    phase="input",
                )
            seen_refs.add(name)
        first_material_index, _first_entry = ordered_material[0]
        first_parameters = authorized.targets[0][1].query_parameters
        first_names = {parameter.name for parameter in first_parameters}
        # A watermark-sourced parameter is never emitted; the declaration's refs
        # are the ONLY place it is recorded. One the refs omit would vanish —
        # refused rather than silently dropped.
        for qp_index, parameter in enumerate(first_parameters):
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

    # -- 7. components -----------------------------------------------------
    context = PrimitiveBuildContext(
        integration_name=naming.component_prefix,
        component_prefix=naming.component_prefix,
        folder_path=naming.folder_name,
    )
    components: _List[IntegrationComponentSpec] = []
    components.extend(_emit_db_components_v2(inp, material, authorized, keys, context))
    components.extend(_emit_transform_components_v2(inp, material, authorized, keys, context))

    modes = _handoff_modes_v2(inp)
    staged = any(mode == "document_cache" for mode in modes.values())
    target_pairs: _List[_List[IntegrationComponentSpec]] = []
    for index, (target, (material_index, entry), validated) in enumerate(
        zip(inp.targets, ordered_material, authorized.targets)
    ):
        target_pairs.append(
            _emit_target_components_v2(
                target, entry, validated, keys.targets[index], keys, context, material_index
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
        create_bound_base_keys=tuple(create_bound),
        rest_request_targets=_rest_request_targets_v2(inp, ordered_material, keys),
        deferred_query_parameter_names=deferred,
        projection_facts=_projection_facts_v2(inp, keys, authorized.operations),
    )


@_dataclass(frozen=True, repr=False)
class _AuthorizedMaterialV2:
    """The material's values AS THE NEUTRAL AUTHORITIES RETURNED THEM (CDX-r1-05).

    ``db`` is the validated ``DbCreateSettings`` (create) or the validated
    ``DbConnectionBinding`` (reuse), ``None`` for a base-key binding;
    ``targets`` is, per target in Branch order, ``(connection authority or
    None, validated RestSendRequest)``. Emission reads ONLY these. The repr is
    redacted: the values are material.
    """

    db: _Any
    #: The validated ``DbReadOperation`` (statement, result schema, bind
    #: parameters, batch / row bounds).
    read: _Any
    targets: Tuple[Tuple[_Any, RestSendRequest], ...]
    #: The transform operations with their mapping names NORMALIZED by the
    #: legacy operation authorities (``DirectTransformOperation`` /
    #: ``MapFunctionTransformOperation`` strip ``source_field``, ``inputs``,
    #: ``target_path`` and the hint) — ARCH-r1-11. Preflight, emission and the
    #: projection facts read these, never the raw material.
    operations: Tuple[_Any, ...] = ()

    @property
    def result_fields(self) -> Tuple[DBResultField, ...]:
        return tuple(self.read.result_schema.fields)

    def __repr__(self) -> str:  # noqa: D105
        return "_AuthorizedMaterialV2(...)"


def _authorize_material_v2(
    inp: ComposeDbRestFanoutInputV2,
    material: FanoutComponentMaterialV2,
    ordered_material: _List[Tuple[int, FanoutTargetMaterialV2]],
    base_catalog: _Mapping[str, IntegrationComponentSpec] = _MappingProxyType({}),
) -> _AuthorizedMaterialV2:
    """Run every value rule through its neutral authority; return what came back."""
    from ...patterns.archetype_parameters import (
        DbConnectionBinding,
        DbReadOperation,
        DbReadParameter,
        RestConnectionBinding,
    )

    db_connection = material.source.connection
    db: _Any = None
    if isinstance(db_connection, FanoutDbCreateMaterialV2):
        db = _authority_check(
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
    elif isinstance(db_connection, FanoutConnectionReuseMaterialV2):
        db = _authority_check(
            "/component_material/source/connection",
            DbConnectionBinding,
            mode="reuse",
            component_id=db_connection.component_id,
            component_name=db_connection.component_name,
        )
    result_fields = tuple(
        _authority_check(
            f"/component_material/source/read/result_fields/{field_index}",
            DBResultField,
            name=field.name,
            data_type=field.data_type,
            required=field.required,
            description=field.description,
        )
        for field_index, field in enumerate(material.source.read.result_fields)
    )
    result_names = [field.name for field in result_fields]
    if len(set(result_names)) != len(result_names):
        raise _refusal(
            COMPOSITION_CONTRACT_MISMATCH,
            "/component_material/source/read/result_fields",
            phase="input",
        )
    read = material.source.read
    bind_parameters = [
        _authority_check(
            f"/component_material/source/read/bind_parameters/{bind_index}",
            DbReadParameter,
            name=parameter.name,
            direction=parameter.direction,
        )
        for bind_index, parameter in enumerate(read.bind_parameters)
    ]
    read_operation = _authority_check(
        "/component_material/source/read",
        DbReadOperation,
        sql=read.sql,
        result_schema={"fields": list(result_fields)},
        parameters=bind_parameters,
        batch_size=read.batch_size,
        max_rows=read.max_rows,
    )
    targets: _List[Tuple[_Any, RestSendRequest]] = []
    for (material_index, entry), target in zip(ordered_material, inp.targets):
        base = f"/component_material/targets/{material_index}"
        connection: _Any = None
        if isinstance(entry.connection, FanoutRestCreateMaterialV2):
            # `RestCreateSettings` is the URL-shape authority; created REST is
            # always unauthenticated (the auth vocabulary's ONE create mode).
            connection = _authority_check(
                base + "/connection",
                RestCreateSettings,
                base_url=entry.connection.base_url,
                auth_mode="none",
            )
        elif isinstance(entry.connection, FanoutConnectionReuseMaterialV2):
            connection = _authority_check(
                base + "/connection",
                RestConnectionBinding,
                mode="reuse",
                component_id=entry.connection.component_id,
                component_name=entry.connection.component_name,
            )
        parameters = [
            _authority_check(
                f"{base}/operation/query_parameters/{qp_index}",
                RestQueryParameter,
                name=parameter.name,
                value_source=parameter.value_source,
                literal_value=parameter.literal_value,
            )
            for qp_index, parameter in enumerate(entry.operation.query_parameters)
        ]
        # CDX-r2-01: names are compared AFTER the authority normalized them, so
        # ``" mode "`` and ``"mode"`` are one parameter. The emitted query is a
        # name -> value map; a second entry would silently replace the first
        # (the legacy composition's dict comprehension does exactly that), so
        # it is refused at the SECOND entry's declared position instead.
        seen_names: set = set()
        for qp_index, parameter in enumerate(parameters):
            if parameter.name in seen_names:
                raise _refusal(
                    COMPOSITION_CONTRACT_MISMATCH,
                    f"{base}/operation/query_parameters/{qp_index}",
                    phase="input",
                )
            seen_names.add(parameter.name)
        request = _authority_check(
            base + "/operation",
            RestSendRequest,
            method=target.method,
            path=entry.operation.path,
            query_parameters=parameters,
            expected_status_codes=list(target.expected_status_codes),
        )
        targets.append((connection, request))
    return _AuthorizedMaterialV2(
        db=db,
        read=read_operation,
        targets=tuple(targets),
        operations=_authorize_operations_v2(material, base_catalog),
    )


def _authorize_operations_v2(
    material: FanoutComponentMaterialV2,
    base_catalog: _Mapping[str, IntegrationComponentSpec],
) -> Tuple[_Any, ...]:
    """Each transform operation normalized through its legacy authority (ARCH-r1-11).

    A map-script operation goes through ``MapScriptTransformOperation`` (B12-03:
    it strips ``inputs``/``outputs`` and the hint). Its ``language`` is the
    REFERENCED script component's own (that component is the source of truth);
    the reference itself is checked later against the base catalog, so a
    script with no readable language is normalized by the SAME function the
    model's validators call rather than by an invented language."""
    from typing import get_args

    from ...patterns.archetype_parameters import (
        DirectTransformOperation,
        MapFunctionTransformOperation,
        MapScriptTransformOperation,
        _stripped_nonblank,
    )

    languages = get_args(MapScriptTransformOperation.model_fields["language"].annotation)

    normalized: _List[_Any] = []
    for op_index, operation in enumerate(material.transform.operations):
        pointer = f"/component_material/transform/operations/{op_index}"
        if isinstance(operation, FanoutDirectOperationV2):
            checked = _authority_check(
                pointer,
                DirectTransformOperation,
                operation_type="direct",
                source_field=operation.source_field,
                target_path=operation.target_path,
                documentation_hint=operation.documentation_hint,
            )
            normalized.append(
                operation.model_copy(
                    update={
                        "source_field": checked.source_field,
                        "target_path": checked.target_path,
                        "documentation_hint": checked.documentation_hint,
                    }
                )
            )
        elif isinstance(operation, FanoutFunctionOperationV2) and not operation.inputs:
            # A family that maps NO input (``default_value``): the legacy
            # model requires at least one input and so cannot carry it; its
            # names are normalized by the SAME function its validators call.
            try:
                target_path = _stripped_nonblank(operation.target_path)
                hint = (
                    None
                    if operation.documentation_hint is None
                    else _stripped_nonblank(operation.documentation_hint)
                )
            except ValueError:
                raise _material_failure(pointer, ValueError()) from None
            normalized.append(
                operation.model_copy(update={"target_path": target_path, "documentation_hint": hint})
            )
        elif isinstance(operation, FanoutFunctionOperationV2):
            checked = _authority_check(
                pointer,
                MapFunctionTransformOperation,
                operation_type="map_function",
                function_type=operation.function_type,
                inputs=list(operation.inputs),
                target_path=operation.target_path,
                parameters=(
                    {parameter.name: parameter.value for parameter in operation.parameters}
                    or None
                ),
                documentation_hint=operation.documentation_hint,
            )
            normalized.append(
                operation.model_copy(
                    update={
                        "inputs": tuple(checked.inputs),
                        "target_path": checked.target_path,
                        "documentation_hint": checked.documentation_hint,
                    }
                )
            )
        else:
            script = base_catalog.get(operation.script_component_key)
            language = ((script.config or {}) if script is not None else {}).get("language")
            if language in languages:
                checked = _authority_check(
                    pointer,
                    MapScriptTransformOperation,
                    operation_type="map_script",
                    script_slot=operation.script_component_key,
                    language=language,
                    inputs=list(operation.inputs),
                    outputs=list(operation.outputs),
                    script_component_ref="$ref:" + operation.script_component_key,
                    documentation_hint=operation.documentation_hint,
                )
                inputs, outputs, hint = checked.inputs, checked.outputs, checked.documentation_hint
            else:
                try:
                    inputs = [_stripped_nonblank(item) for item in operation.inputs]
                    outputs = [_stripped_nonblank(item) for item in operation.outputs]
                    hint = (
                        None
                        if operation.documentation_hint is None
                        else _stripped_nonblank(operation.documentation_hint)
                    )
                except ValueError:
                    raise _material_failure(pointer, ValueError()) from None
            normalized.append(
                operation.model_copy(
                    update={
                        "inputs": tuple(inputs),
                        "outputs": tuple(outputs),
                        "documentation_hint": hint,
                    }
                )
            )
    return tuple(normalized)


#: The route's topology spelling. Pinned against the legacy composition's
#: served summary by ``test_the_ported_constants_are_pinned_to_the_legacy_composition``.
_TOPOLOGY_V2 = "db_source->transform->rest_fanout"


def _projection_facts_v2(
    inp: ComposeDbRestFanoutInputV2,
    keys: _FanoutKeysV2,
    operations: Tuple[_Any, ...],
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
            for operation in operations
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
    authorized: _AuthorizedMaterialV2,
    keys: _FanoutKeysV2,
    context: PrimitiveBuildContext,
) -> _List[IntegrationComponentSpec]:
    """Port of ``_build_db_extract_params`` + ``_rt().DbExtractPrimitive``.

    Connection values and result fields are the AUTHORITY-validated ones."""
    read = authorized.read
    connection = authorized.db
    if isinstance(connection, DbCreateSettings):
        connection_params: _Dict[str, _Any] = {
            "mode": "create",
            "driver_id": connection.driver,
            "auth_mode": connection.auth_mode,
            "host": connection.host,
            "port": connection.port,
            "dbname": connection.database,
        }
        if connection.username is not None:
            connection_params["username"] = connection.username
        if connection.credential_ref is not None:
            connection_params["credential_ref"] = connection.credential_ref
    elif connection is not None:
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
            for field in authorized.result_fields
        ],
    }
    if read.parameters:
        read_profile["parameters"] = [
            {"name": parameter.name, "mappable": parameter.direction == "in"}
            for parameter in read.parameters
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
    authorized: _AuthorizedMaterialV2,
    keys: _FanoutKeysV2,
    context: PrimitiveBuildContext,
) -> _List[IntegrationComponentSpec]:
    """Port of ``_build_field_map_params`` + ``_rt().FieldMapPrimitive``.

    Map scripts take the primitive's script-component REFERENCE path: the
    script is a base component, referenced by ``$ref``, never inlined.
    """
    direct: _List[_Dict[str, _Any]] = []
    functions: _List[_Dict[str, _Any]] = []
    scripts: _List[_Dict[str, _Any]] = []
    for operation in authorized.operations:
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
                        for field in authorized.result_fields
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
    validated: Tuple[_Any, RestSendRequest],
    target_keys: Tuple[str, str, str],
    keys: _FanoutKeysV2,
    context: PrimitiveBuildContext,
    material_index: int,
) -> _List[IntegrationComponentSpec]:
    """Port of ``_fanout_target_params`` + ``_rt().RestSendWithRetryPrimitive``.

    The base URL, reuse selector, path and query parameters are the
    AUTHORITY-validated values (``validated``), exactly as the legacy route
    emits ``settings.base_url`` / ``send.path`` / ``qp.name`` (CDX-r1-05)."""
    prefix, connection_key, operation_key = target_keys
    base = f"/component_material/targets/{material_index}"
    label = _derived_label_v2(target)
    connection, request = validated
    if isinstance(connection, RestCreateSettings):
        connection_params: _Dict[str, _Any] = {
            "mode": "create",
            "base_url": connection.base_url,
            "auth": _rt().REST_CREATE_AUTH_MAP["none"],
        }
    elif connection is not None:
        connection_params = {"mode": "reuse"}
        if connection.component_id:
            connection_params["component_id"] = connection.component_id
        if connection.component_name:
            connection_params["component_name"] = connection.component_name
    else:
        connection_params = {"mode": "reuse", "component_id": connection_key}
    operation: _Dict[str, _Any] = {
        "method": target.method,
        "path": request.path,
        # Every leg sends the ONE shared payload profile the transform produces.
        "request_profile_id": f"$ref:{keys.target_profile}",
        "request_profile_type": "json",
    }
    literal = {
        parameter.name: parameter.literal_value
        for parameter in request.query_parameters
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
