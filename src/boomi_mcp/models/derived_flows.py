"""Typed, generator-derived, OUTPUT-ONLY flow rows (issue #157 / M12.19).

The legacy ``IntegrationSpecV1.flows`` list was a caller-authored, schema-opaque
echo that seven archetype/composition producers wrote and nothing read. Two of
those producers — the DB-source sync and the API-source sync — emitted a RICH
``transform`` row carrying the generated source/target profile artifacts, the
normalized direct mappings and the operation summaries that
``review_transformation`` consumes. That subshape is worth serving, so on the
two COMPILING intents (``process_ir`` and ``recipe``) the served preview's
``flows`` becomes a projection of exactly these typed rows, DERIVED from the
surviving profile generator and the authored map components, never authored by
the caller (a caller-supplied ``flows`` on those intents is refused with
``GOVERNANCE_FLOWS_OUTPUT_ONLY``).

The legacy field itself is neither renamed nor repurposed (frozen legacy
surface, #146 naming rule): the ``integration_spec`` intent keeps serving the
authored list verbatim, and the seven legacy producers live until #160 deletes
them.

**These rows are a served projection, not compiler input.** The map components
and profile artifacts they summarize remain the executable authorities; the
projection is excluded from every fingerprint and from every mutation payload.

**Shape fidelity.** Every field below mirrors a key the two legacy rich forms
actually served, so the ONE normalizer in ``boomi_mcp.authoring.derived_flows``
can bring both legacy forms and the canonical projection to the same
representation and compare them by full payload. The only representation
change is deliberate and recorded (ledger correction P6): a map-script body is
served as ``script_body_sha256`` + ``script_body_present`` rather than verbatim,
because ADR-001 §11 forbids executable script text on a served result.
"""

from __future__ import annotations

from typing import Annotated, Any, Dict, Literal, Mapping, Optional, Tuple, Union

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

NonEmptyString = Annotated[str, StringConstraints(min_length=1)]


class _DerivedFlowModel(BaseModel):
    """Strict and frozen: a served projection must not carry unknown keys."""

    model_config = ConfigDict(extra="forbid", frozen=True)


# ---------------------------------------------------------------------------
# profile summaries (the value-free shape descriptions the legacy rows carried)
# ---------------------------------------------------------------------------


class ProfileLeafSummaryV1(_DerivedFlowModel):
    path: NonEmptyString
    data_type: str


class PayloadProfileSummaryV1(_DerivedFlowModel):
    """A JSON payload profile, summarized by its leaves (legacy `format/root_name/leaves`)."""

    format: NonEmptyString
    root_name: NonEmptyString
    leaf_count: int = Field(ge=0)
    leaves: Tuple[ProfileLeafSummaryV1, ...] = ()


class SchemaFieldSummaryV1(_DerivedFlowModel):
    name: NonEmptyString
    data_type: str
    required: bool = False


class SourceSchemaSummaryV1(_DerivedFlowModel):
    """A DB read result schema, summarized by its fields (legacy `field_count/fields`)."""

    field_count: int = Field(ge=0)
    fields: Tuple[SchemaFieldSummaryV1, ...] = ()


class WriteProfileSummaryV1(_DerivedFlowModel):
    """A DB write profile target summary.

    Carried for the three DB-write producers' rows, which #159 discharges; the
    two rich forms this issue discharges never populate it. Kept opaque-but-
    frozen rather than typed here so this issue does not hand-model a shape it
    does not derive yet — #159 types it when it derives it.
    """

    summary: Mapping[str, Any]


# ---------------------------------------------------------------------------
# generated profile artifacts (the surviving generator's own output shape)
# ---------------------------------------------------------------------------


class FieldIndexEntryV1(_DerivedFlowModel):
    """One `field_index_by_path` entry, exactly as the generator emits it."""

    path: NonEmptyString
    name: NonEmptyString
    mappable: bool
    profile_component_type: NonEmptyString
    data_type: Optional[str] = None
    kind: Optional[str] = None
    required: Optional[bool] = None
    source: Optional[str] = None


class JsonProfileNodeV1(_DerivedFlowModel):
    """The normalized JSON profile tree node `profile_from_json_schema` returns."""

    name: NonEmptyString
    kind: Literal["simple", "object", "array"]
    required: bool = False
    data_type: Optional[str] = None
    children: Optional[Tuple["JsonProfileNodeV1", ...]] = None


class JsonProfileConfigV1(_DerivedFlowModel):
    format: Literal["json"]
    root: JsonProfileNodeV1


class DbOutputFieldV1(_DerivedFlowModel):
    name: NonEmptyString
    data_type: str
    mandatory: bool = False
    enforce_unique: bool = False


class DbReadProfileConfigV1(_DerivedFlowModel):
    output_fields: Tuple[DbOutputFieldV1, ...] = ()


class GeneratedProfileSummaryV1(_DerivedFlowModel):
    """The surviving generator's output (`profile_from_*`), typed to its own shape."""

    generation_mode: Literal["profile_from_db_read_fields", "profile_from_json_schema"]
    component_type: Literal["profile.db", "profile.json"]
    profile_type: Optional[str] = None
    component_name: Optional[str] = None
    profile_config: Union[JsonProfileConfigV1, DbReadProfileConfigV1]
    field_index_by_path: Mapping[str, FieldIndexEntryV1]
    mappable_paths: Tuple[str, ...] = ()


class DirectFieldMappingV1(_DerivedFlowModel):
    """One normalized direct mapping, as `validate_field_mappings` returns it."""

    route: Literal["direct"]
    source_path: NonEmptyString
    target_path: NonEmptyString
    source_data_type: Optional[str] = None
    target_data_type: Optional[str] = None


# ---------------------------------------------------------------------------
# operation summaries
# ---------------------------------------------------------------------------


class DirectOperationSummaryV1(_DerivedFlowModel):
    operation_type: Literal["direct"]
    target_path: NonEmptyString
    #: The DB-source form names the source column `source_field`; the
    #: API-source form serves BOTH `source_field` and `source_path` for the same
    #: leaf. Both are optional so the one normalizer can carry either form.
    source_field: Optional[str] = None
    source_path: Optional[str] = None
    future_builder_issue: Optional[str] = None
    documentation_hint: Optional[str] = None


class MapFunctionOperationSummaryV1(_DerivedFlowModel):
    operation_type: Literal["map_function"]
    function_type: NonEmptyString
    inputs: Tuple[str, ...] = ()
    input_count: int = Field(ge=0)
    #: Optional on the CANONICAL side: a map function may write a process
    #: property (`dynamic_process_property_set`) and target no profile leaf at
    #: all. The legacy rows always carried one; the projection must be total
    #: over every map the map builder accepts, never a planning failure.
    target_path: Optional[str] = None
    #: Schema-opaque by the archetype contract (the map-function registry owns
    #: the per-function parameter shape); secret-shaped keys are refused at the
    #: archetype boundary before a row is ever built.
    parameters: Optional[Dict[str, Any]] = None
    future_builder_issue: Optional[str] = None
    documentation_hint: Optional[str] = None


class MapScriptOperationSummaryV1(_DerivedFlowModel):
    operation_type: Literal["map_script"]
    language: NonEmptyString
    inputs: Tuple[str, ...] = ()
    input_count: int = Field(ge=0)
    outputs: Tuple[str, ...] = ()
    output_count: int = Field(ge=0)
    script_body_present: bool = False
    #: P6: the body's digest, never the body. Content equality across the
    #: legacy row and the canonical projection is preserved by the digest.
    script_body_sha256: Optional[str] = None
    script_slot: Optional[str] = None
    script_component_ref: Optional[str] = None
    future_builder_issue: Optional[str] = None
    documentation_hint: Optional[str] = None


TransformOperationSummaryV1 = Annotated[
    Union[
        DirectOperationSummaryV1,
        MapFunctionOperationSummaryV1,
        MapScriptOperationSummaryV1,
    ],
    Field(discriminator="operation_type"),
]


# ---------------------------------------------------------------------------
# the row
# ---------------------------------------------------------------------------


class DerivedTransformFlowV1(_DerivedFlowModel):
    """The rich `transform` row, derived from the generator and the map components."""

    key: NonEmptyString
    name: str = ""
    source: str = ""
    target: Optional[str] = None
    operation: Literal["transform"] = "transform"
    executable: Literal[False] = False
    source_schema: Union[SourceSchemaSummaryV1, PayloadProfileSummaryV1]
    target_payload_profile: Optional[PayloadProfileSummaryV1] = None
    target_write_profile: Optional[WriteProfileSummaryV1] = None
    operations: Tuple[TransformOperationSummaryV1, ...] = ()
    source_profile_generation: Optional[GeneratedProfileSummaryV1] = None
    target_profile_generation: Optional[GeneratedProfileSummaryV1] = None
    direct_field_mappings: Tuple[DirectFieldMappingV1, ...] = ()


#: The row kinds the projection can serve. One member today; the tuple exists so
#: `DERIVED_FLOW_OPERATIONS` is DERIVED from it rather than typed out.
_DERIVED_FLOW_MEMBERS: Tuple[type, ...] = (DerivedTransformFlowV1,)

DerivedFlowRowV1 = DerivedTransformFlowV1

DERIVED_FLOW_OPERATIONS: Tuple[str, ...] = tuple(
    member.model_fields["operation"].default for member in _DERIVED_FLOW_MEMBERS
)


__all__ = [
    "DERIVED_FLOW_OPERATIONS",
    "DbOutputFieldV1",
    "DbReadProfileConfigV1",
    "DerivedFlowRowV1",
    "DerivedTransformFlowV1",
    "DirectFieldMappingV1",
    "DirectOperationSummaryV1",
    "FieldIndexEntryV1",
    "GeneratedProfileSummaryV1",
    "JsonProfileConfigV1",
    "JsonProfileNodeV1",
    "MapFunctionOperationSummaryV1",
    "MapScriptOperationSummaryV1",
    "PayloadProfileSummaryV1",
    "ProfileLeafSummaryV1",
    "SchemaFieldSummaryV1",
    "SourceSchemaSummaryV1",
    "TransformOperationSummaryV1",
    "WriteProfileSummaryV1",
]
