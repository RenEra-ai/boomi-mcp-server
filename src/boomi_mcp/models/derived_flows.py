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

from typing import Annotated, Any, Dict, Literal, Mapping, Optional, Tuple, Union, get_args

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

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
    """A profile's shape, and WHICH artifact the shape came from.

    ``evidence_source`` is the field that makes this row checkable. A summary
    built by the surviving generator carries that generator's mode and the
    profile config it was built from. A summary describing an artifact this
    request will REUSE carries neither: the account supplied the field index and
    nothing supplied a generation body, and copying the candidate config beside
    the account's index served two contradictory descriptions of one component
    (architect evaluation 2, finding 2).
    """

    evidence_source: Literal["generator", "selected_artifact"] = "generator"
    generation_mode: Optional[
        Literal["profile_from_db_read_fields", "profile_from_json_schema"]
    ] = None
    component_type: Literal["profile.db", "profile.json"]
    profile_type: Optional[str] = None
    component_name: Optional[str] = None
    profile_config: Optional[Union[JsonProfileConfigV1, DbReadProfileConfigV1]] = None
    field_index_by_path: Mapping[str, FieldIndexEntryV1]
    mappable_paths: Tuple[str, ...] = ()

    @model_validator(mode="after")
    def _generation_body_matches_its_source(self):
        if self.evidence_source == "generator":
            if self.generation_mode is None or self.profile_config is None:
                raise ValueError(
                    "a generator-sourced profile summary carries its generation "
                    "mode and the profile config it was built from"
                )
        elif self.generation_mode is not None or self.profile_config is not None:
            raise ValueError(
                "a selected-artifact profile summary carries no generation body; "
                "the account supplied the field index and nothing generated it"
            )
        return self


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


# ---------------------------------------------------------------------------
# the recipe-route row kinds (#159 S1)
# ---------------------------------------------------------------------------


class DerivedConnectorFlowV1(_DerivedFlowModel):
    """A connector step of a recipe-assembled route: the extract or the send.

    Served only for a unit whose recipe supplied trusted projection facts
    (:class:`RecipeUnitProjectionFactsV1`); every field is read off the composed
    root and its components, never off the recipe input.
    """

    key: NonEmptyString
    name: str = ""
    source: str = ""
    target: Optional[str] = None
    operation: Literal["db_get", "rest_send"]
    executable: Literal[False] = False


class DerivedReliabilityFlowV1(_DerivedFlowModel):
    """The route's retry / DLQ posture. ``target`` names the DLQ when the root
    wires a catch path, and is null when it does not."""

    key: NonEmptyString
    name: str = ""
    source: str = ""
    target: Optional[str] = None
    operation: Literal["reliability"] = "reliability"
    executable: Literal[False] = False


class DerivedWatermarkFlowV1(_DerivedFlowModel):
    """Present exactly when the unit carries a recorded watermark declaration."""

    key: NonEmptyString
    name: str = ""
    source: str = ""
    target: Optional[str] = None
    operation: Literal["watermark"] = "watermark"
    executable: Literal[False] = False


class DerivedFanoutFlowV1(_DerivedFlowModel):
    """The terminal Branch. ``legs`` counts the ROOT's legs, staging leg included."""

    key: NonEmptyString
    name: str = ""
    source: str = ""
    target: Optional[str] = None
    operation: Literal["branch"] = "branch"
    executable: Literal[False] = False
    legs: int = Field(ge=2)


#: The row kinds the projection can serve. ``DERIVED_FLOW_OPERATIONS`` is
#: DERIVED from their ``operation`` discriminator literals, never typed out.
_DERIVED_FLOW_MEMBERS: Tuple[type, ...] = (
    DerivedConnectorFlowV1,
    DerivedTransformFlowV1,
    DerivedReliabilityFlowV1,
    DerivedWatermarkFlowV1,
    DerivedFanoutFlowV1,
)

DerivedFlowRowV1 = Annotated[
    Union[_DERIVED_FLOW_MEMBERS],  # type: ignore[valid-type]
    Field(discriminator="operation"),
]

DERIVED_FLOW_OPERATIONS: Tuple[str, ...] = tuple(
    operation
    for member in _DERIVED_FLOW_MEMBERS
    for operation in get_args(member.model_fields["operation"].annotation)
)


# ---------------------------------------------------------------------------
# endpoints and composition summaries (#159 S1) — served, output-only
# ---------------------------------------------------------------------------


class DerivedEndpointRowV1(_DerivedFlowModel):
    """One endpoint of a recipe-assembled route, read off its connection component.

    A created connection serves its classification (``driver`` and ``auth_mode``
    for a database, ``auth_mode`` for REST); a reused one serves the selector
    it binds by. Never a host, URL, credential or SQL.
    """

    key: NonEmptyString
    type: Literal["database", "rest"]
    direction: Literal["source", "target"]
    binding_mode: Literal["create", "reuse"]
    executable: Literal[False] = False
    method: Optional[str] = None
    driver: Optional[str] = None
    auth_mode: Optional[str] = None
    component_id: Optional[str] = None
    component_name: Optional[str] = None


class DerivedCompositionPartV1(_DerivedFlowModel):
    key: NonEmptyString
    kind: Literal["db_source", "transform", "rest_target"]


class DerivedFanoutLegV1(_DerivedFlowModel):
    """One non-first fan-out target, at its ACTUAL Branch leg position."""

    part_key: NonEmptyString
    leg: int = Field(ge=1)
    method: Optional[str] = None
    binding_mode: Literal["create", "reuse"]
    expected_status_codes: Optional[Tuple[int, ...]] = None
    handoff: Optional[Literal["document_stream", "document_cache"]] = None


class DerivedCompositionSummaryV1(_DerivedFlowModel):
    """The composition's value-free topology summary, derived from the root."""

    topology: NonEmptyString
    handoff: Literal["document_stream", "document_cache", "mixed"]
    fanout_targets: int = Field(ge=2)
    parts: Tuple[DerivedCompositionPartV1, ...]
    first_target_part: NonEmptyString
    fanout_legs: Tuple[DerivedFanoutLegV1, ...] = ()
    cache_component_key: Optional[str] = None
    staging_leg: Optional[int] = None
    handoff_modes: Optional[Mapping[str, Literal["document_stream", "document_cache"]]] = None


# ---------------------------------------------------------------------------
# trusted projection facts — INTERNAL, never served, never hashed
# ---------------------------------------------------------------------------


class RecipeTargetFactV1(_DerivedFlowModel):
    part_key: NonEmptyString
    connection_key: NonEmptyString
    operation_key: NonEmptyString
    expected_status_codes: Tuple[int, ...] = ()


class RecipeOperationHintV1(_DerivedFlowModel):
    """A caller's ``documentation_hint`` for one transform operation, by route
    and target path — served on the transform row's operation summary."""

    operation_type: Literal["direct", "map_function", "map_script"]
    target_path: NonEmptyString
    documentation_hint: str

    def __repr_args__(self):  # noqa: D105 — pydantic hook
        # The hint is caller content from the closed component material: it is
        # SERVED on the transform row (where the legacy row served it) and
        # nowhere else, so a repr reaching a log or traceback renders no value.
        for key, value in super().__repr_args__():
            yield key, ("..." if key != "operation_type" else value)


class RecipeUnitProjectionFactsV1(_DerivedFlowModel):
    """What a trusted recipe route tells the #157 projection about one unit.

    Supplied by a server-side preparer from its VALIDATED input and material —
    never by a caller and never by a recipe executor — and only the facts the composed
    root and components cannot carry: which part is which, which components a
    part owns, and the declared status-code expectations. Everything else the
    projection serves (legs, ordinals, handoff, binding modes, endpoint
    classifications) is DERIVED from the root and components. The presence of
    facts for a unit is also the TRIGGER: a unit without them is projected
    exactly as before (its transform rows only).

    Material boundary (D9): every field is a key, a classification or a status
    code EXCEPT ``documentation_hints``, which carries caller-authored hint text
    from the component material. That text is served on the transform row's
    operation summary — where the legacy row served it — and nowhere else; its
    repr is redacted.
    """

    process_key: NonEmptyString
    topology: NonEmptyString
    parts: Tuple[DerivedCompositionPartV1, ...]
    source_connection_key: NonEmptyString
    source_operation_key: NonEmptyString
    targets: Tuple[RecipeTargetFactV1, ...] = Field(min_length=1)
    documentation_hints: Tuple[RecipeOperationHintV1, ...] = ()


__all__ = [
    "DERIVED_FLOW_OPERATIONS",
    "DbOutputFieldV1",
    "DbReadProfileConfigV1",
    "DerivedCompositionPartV1",
    "DerivedCompositionSummaryV1",
    "DerivedConnectorFlowV1",
    "DerivedEndpointRowV1",
    "DerivedFanoutFlowV1",
    "DerivedFanoutLegV1",
    "DerivedFlowRowV1",
    "DerivedReliabilityFlowV1",
    "DerivedTransformFlowV1",
    "DerivedWatermarkFlowV1",
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
    "RecipeOperationHintV1",
    "RecipeTargetFactV1",
    "RecipeUnitProjectionFactsV1",
    "SchemaFieldSummaryV1",
    "SourceSchemaSummaryV1",
    "TransformOperationSummaryV1",
    "WriteProfileSummaryV1",
]
