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

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    model_serializer,
    model_validator,
)

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


class WriteTargetPathV1(_DerivedFlowModel):
    """One map-target leaf of a database write profile (``Fields/<col>`` or
    ``Conditions/<col>``), as the write profile's own field index names it."""

    path: NonEmptyString
    data_type: Optional[str] = None
    required: bool = False


class WriteProfileSummaryV1(_DerivedFlowModel):
    """A DB write profile target summary (#159 S2: typed now that it is derived).

    The shape the three DB-write legacy producers served, read off the write
    profile COMPONENT through ``DatabaseWriteProfileBuilder.build_field_index``
    — the index the map builder binds against — never off a recipe input.
    ``target_paths`` is sorted by path; the counts are the index's own
    namespaces. Value-free: no SQL, no table name.
    """

    kind: Literal["database.write"] = "database.write"
    statement_type: NonEmptyString
    field_count: int = Field(ge=0)
    condition_count: int = Field(ge=0)
    target_paths: Tuple[WriteTargetPathV1, ...] = ()


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
    #: ``db_get`` / ``rest_fetch`` / ``wss_listen`` on the source side,
    #: ``rest_send`` / ``db_write`` on the target side (#159 S2 adds the
    #: linear presets' three).
    operation: Literal["db_get", "rest_send", "rest_fetch", "db_write", "wss_listen"]
    executable: Literal[False] = False


class DerivedNoopFlowV1(_DerivedFlowModel):
    """The stub route's single row: a Stop-only root moves no document (#159 S2)."""

    key: NonEmptyString
    name: str = ""
    source: str = ""
    target: Optional[str] = None
    operation: Literal["noop"] = "noop"
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
    DerivedNoopFlowV1,
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


class _OmitUnsetModel(_DerivedFlowModel):
    """A served row whose UNSET keys are omitted from the wire (C8).

    The legacy composition producer wrote a key only when it had a value to
    say, so its served dicts omit what this model leaves unset. A key a
    projector sets — even to an explicit ``None`` — is served; one it never
    sets is not. The rule is the model's own set/unset state, not a key list,
    so a field added later follows it without an edit here."""

    @model_serializer(mode="wrap")
    def _omit_unset(self, handler):
        data = handler(self)
        if isinstance(data, dict):
            for name in tuple(data):
                if name in type(self).model_fields and name not in self.model_fields_set:
                    data.pop(name)
        return data


class DerivedEndpointRowV1(_OmitUnsetModel):
    """One endpoint of a recipe-assembled route, read off its connection component.

    A created connection serves its classification (``driver`` and ``auth_mode``
    for a database, ``auth_mode`` for REST); a reused one serves the selector
    it binds by. Never a host, URL, credential or SQL.

    #159 S2 widens it for the linear presets, every key still read off a
    component: a database WRITE target also serves its operation and the write
    profile's ``statement_type``; a WSS listener serves its computed
    ``endpoint_path`` (and ``publish_mode`` behind an API Service component); a
    REST endpoint serves ``expected_status_codes`` only when its route DECLARES
    them; and a stub endpoint is a bare labelled placeholder with no binding
    and no ``executable`` key at all — so ``binding_mode`` is required on every
    type EXCEPT ``stub``, and ``label`` is served on ``stub`` alone.
    """

    key: NonEmptyString
    type: Literal["database", "rest", "wss", "stub"]
    direction: Literal["source", "target"]
    binding_mode: Optional[Literal["create", "reuse"]] = None
    executable: Literal[False] = False
    method: Optional[str] = None
    driver: Optional[str] = None
    auth_mode: Optional[str] = None
    component_id: Optional[str] = None
    component_name: Optional[str] = None
    expected_status_codes: Optional[Tuple[int, ...]] = None
    operation: Optional[str] = None
    statement_type: Optional[str] = None
    endpoint_path: Optional[str] = None
    publish_mode: Optional[Literal["api_service"]] = None
    label: Optional[str] = None

    @model_validator(mode="after")
    def _binding_matches_the_type(self):
        if self.type == "stub":
            if self.binding_mode is not None or "executable" in self.model_fields_set:
                raise ValueError("a stub endpoint binds nothing and serves no executable flag")
        else:
            if self.binding_mode is None:
                raise ValueError("a {0} endpoint serves its binding mode".format(self.type))
            if self.label is not None:
                raise ValueError("only a stub endpoint serves a label")
        return self


class DerivedCompositionPartV1(_OmitUnsetModel):
    key: NonEmptyString
    kind: Literal["db_source", "transform", "rest_target"]


class DerivedFanoutLegV1(_OmitUnsetModel):
    """One non-first fan-out target, at its ACTUAL Branch leg position."""

    part_key: NonEmptyString
    leg: int = Field(ge=1)
    method: Optional[str] = None
    binding_mode: Literal["create", "reuse"]
    expected_status_codes: Optional[Tuple[int, ...]] = None
    handoff: Optional[Literal["document_stream", "document_cache"]] = None


class DerivedCompositionSummaryV1(_OmitUnsetModel):
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
    #: The NAME the transform row is served under. The row's name is the
    #: route's to set: the process step's label is a separate, fixed fact.
    transform_row_name: Optional[NonEmptyString] = None
    targets: Tuple[RecipeTargetFactV1, ...] = Field(min_length=1)
    documentation_hints: Tuple[RecipeOperationHintV1, ...] = ()


# ---------------------------------------------------------------------------
# linear projection facts (#159 S2) — INTERNAL, never served, never hashed
# ---------------------------------------------------------------------------


class LinearConnectorFactV1(_DerivedFlowModel):
    """Which components ONE connector step of a linear route owns.

    A typed pointer, never row text: the projector resolves both keys against
    the plan's components AND requires the root to carry a connector node whose
    ``operation_ref`` names ``operation_key``. Family, row key, row operation,
    endpoint token, binding mode, method, selector, auth classification,
    statement type and listener path are all READ off those components.

    ``connection_key`` is None exactly for an operation-only step (a WSS
    listener has no connection component).

    ``expected_status_codes`` is the route's DECLARED success-status
    expectation for a REST target. Set means served on the endpoint row; None
    means the key is absent. The DB→REST and listener→REST presets froze no
    such key and MUST leave it None, and the direct API→API inference leaves it
    None too: a route's topology declares no success-status expectation.
    """

    connection_key: Optional[NonEmptyString] = None
    operation_key: NonEmptyString
    expected_status_codes: Optional[Tuple[int, ...]] = None


class LinearTransformFactV1(_DerivedFlowModel):
    """Transform hints for the route's single ``map_ref``.

    ``detail`` is the ONE fact the root and components cannot carry: which of
    the two frozen transform-row forms the route serves.

    * ``generated_artifacts`` — the rich #157 row: source/target profile
      generation summaries, the validated direct mappings and the JSON target
      payload profile (DB→REST and API→API froze this form).
    * ``schema_summary`` — the summary row the API→DB and listener presets
      froze: source schema, operations and EITHER the JSON target payload
      profile OR the DB write-profile summary, and none of the generation
      keys (absent, not null).

    ``row_name`` overrides the map node's label as the served row name (as on
    the fan-out facts); None serves the label. ``documentation_hints`` are the
    caller's per-operation hints, served on the operation summary they annotate.
    """

    detail: Literal["generated_artifacts", "schema_summary"]
    row_name: Optional[NonEmptyString] = None
    documentation_hints: Tuple[RecipeOperationHintV1, ...] = ()


class LinearReliabilityFactV1(_DerivedFlowModel):
    """The route reports a reliability row (the DB→REST preset does, always).

    ``dlq_component_key`` points at the DLQ the catch path writes, in either
    form a ``ComponentRefV1`` takes: a plan component KEY (the root must then
    reference ``$ref:<key>``) or the literal id of an EXISTING component (the
    root must carry that literal verbatim, e.g. as a ``cache_put.cache_ref``).
    The row's ``target`` is ``dlq`` only then; None — retry only, or DLQ
    disabled — renders the canonical default row (``target`` null). A
    retry-only ``try_catch`` therefore never reports a DLQ.
    """

    dlq_component_key: Optional[NonEmptyString] = None


class LinearRouteProjectionFactsV1(_DerivedFlowModel):
    """A linear preset's facts: ``source -> map_ref -> target`` (#159 S2).

    Supplied by a server-side preparer from its VALIDATED input — never by a
    caller — exactly like :class:`RecipeUnitProjectionFactsV1`, and the
    trigger for the route rows the same way. Rows: the source connector row
    (``extract``/``fetch``/``listen``), the transform row, the target connector
    row (``send``/``write``), then ``reliability`` when declared, then
    ``watermark`` when the unit carries a recorded watermark declaration.
    Endpoints: source, then target.
    """

    shape: Literal["linear"] = "linear"
    process_key: NonEmptyString
    source: LinearConnectorFactV1
    transform: LinearTransformFactV1
    target: LinearConnectorFactV1
    reliability: Optional[LinearReliabilityFactV1] = None


class StubRouteProjectionFactsV1(_DerivedFlowModel):
    """The stub preset's facts: a Stop-only root and zero components (#159 S2).

    The projector checks the root IS Stop-only before serving the
    ``stub_noop_flow`` row and the two labelled ``stub`` endpoints.
    """

    shape: Literal["stub"] = "stub"
    process_key: NonEmptyString


#: Every linear-preset facts form, discriminated by ``shape``. The fan-out
#: facts (:class:`RecipeUnitProjectionFactsV1`) stay their own unchanged class.
LinearProjectionFactsV1 = Annotated[
    Union[LinearRouteProjectionFactsV1, StubRouteProjectionFactsV1],
    Field(discriminator="shape"),
]


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
    "DerivedNoopFlowV1",
    "DerivedReliabilityFlowV1",
    "DerivedTransformFlowV1",
    "DerivedWatermarkFlowV1",
    "DirectFieldMappingV1",
    "DirectOperationSummaryV1",
    "FieldIndexEntryV1",
    "GeneratedProfileSummaryV1",
    "JsonProfileConfigV1",
    "JsonProfileNodeV1",
    "LinearConnectorFactV1",
    "LinearProjectionFactsV1",
    "LinearReliabilityFactV1",
    "LinearRouteProjectionFactsV1",
    "LinearTransformFactV1",
    "MapFunctionOperationSummaryV1",
    "MapScriptOperationSummaryV1",
    "PayloadProfileSummaryV1",
    "ProfileLeafSummaryV1",
    "RecipeOperationHintV1",
    "RecipeTargetFactV1",
    "RecipeUnitProjectionFactsV1",
    "SchemaFieldSummaryV1",
    "SourceSchemaSummaryV1",
    "StubRouteProjectionFactsV1",
    "TransformOperationSummaryV1",
    "WriteProfileSummaryV1",
    "WriteTargetPathV1",
]
