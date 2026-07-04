"""Archetype composition (M8 / issue #14).

``compose_archetypes(parts, options)`` composes typed archetype PARTS into ONE
coherent :class:`IntegrationSpecV1` for the v1 composed topology::

    db_source -> transform -> rest_fanout (2..25 REST targets)

The base assembly (DB source, shared JSON transform, first REST target, main
process) is delegated verbatim to :class:`DatabaseToApiSyncArchetype`; every
additional ``rest_target`` part emits one more REST connection/operation pair
through :class:`RestSendWithRetryPrimitive`, and the main process config is
rewritten onto the shipped ``flow_sequence`` composition surface (#117): a
``map_ref`` step for the shared transform followed by a terminal ``branch``
(#112) with one REST target per leg.

Document handoff between parts is a first-class link property on the
``transform -> rest_target`` edges. ``document_stream`` (default) flows
documents down the single process stream; ``document_cache`` (M8.1 / issue
#132) lowers onto the M11 ``cache_put`` / ``cache_get`` steps: one
target-less staging Branch leg writes the mapped documents into an
auto-emitted in-spec Document Cache (Add to Cache consumes the stream — the
#122/#131 consumption contract), and every cache-mode target leg re-reads
them with an all-documents ``cache_get`` before its REST send. The staging
leg always precedes the first consuming leg, so the #123 write-before-read
lineage gate (run by the pre-emission self-check below) holds by
construction. Keyed retrieval stays gated (#119 census Outcome B).

Deterministic cross-part contract validation (COMPOSITION_* error codes) runs
BEFORE any spec is emitted, and the rewritten process config is re-validated
through ``ProcessFlowBuilder.validate_config`` before the spec is returned —
an invalid composition can never reach ``build_integration``, so it fails
before any Boomi mutation. This module emits JSON component specs only: no raw
XML, no Boomi calls, no credentials.

Deliberately NOT an :class:`ArchetypePattern` subclass — the pattern registry
(``PatternRegistry.from_package``) and ``list_integration_archetypes`` stay
unchanged; composition is its own function surface.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from ..categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from ..categories.components.builders.json_profile_builder import (
    JSONGeneratedProfileBuilder,
)
from ..categories.components.builders.process_flow_builder import (
    ProcessFlowBuilder,
)
from ..errors import (
    COMPOSITION_COMPONENT_KEY_COLLISION,
    COMPOSITION_CONTRACT_MISMATCH,
    COMPOSITION_UNSUPPORTED_TOPOLOGY,
)
from ..models.integration_models import (
    IntegrationComponentSpec,
    IntegrationSpecV1,
)
from .archetypes.database_to_api_sync import (
    _MAIN_PROCESS_KEY,
    _REST_CREATE_AUTH_MAP,
    _TARGET_PREFIX,
    _TRANSFORM_PREFIX,
    DatabaseSource,
    DatabaseToApiSyncArchetype,
    DirectTransformOperation,
    MapFunctionTransformOperation,
    MapScriptTransformOperation,
    RestTarget,
    TransformConfig,
    UNSUPPORTED_REST_AUTH_MODE,
    _coerce_primitive_params,
    _flatten_payload_profile_leaves,
    _required_simple_leaf_paths,
)
from .base import PrimitiveBuildContext
from .primitives._helpers import (
    ROLE_REST_CONNECTION,
    ROLE_REST_OPERATION,
    ROLE_TARGET_PROFILE,
    ROLE_TRANSFORM_MAP,
    primitive_component_key,
    slugify,
)
from .primitives.rest_send import (
    RestSendWithRetryParameters,
    RestSendWithRetryPrimitive,
    rest_connection_extension_fields,
)

# Branch legs are 2..25 (ProcessFlowBuilder._BRANCH_MAX_LEGS); every rest_target
# part becomes one leg, so the part count shares the same bounds.
_MIN_FANOUT_TARGETS = 2
_MAX_FANOUT_TARGETS = 25

_PART_KINDS = ("db_source", "transform", "rest_target")

# Reserved key prefixes owned by the base archetype assembly — a fanout part's
# derived prefix must never collide with them.
_RESERVED_PREFIXES = frozenset({"source", "transform", "target"})

_KEYED_HANDOFF_GATED_HINT = (
    "Composed document_cache handoffs retrieve ALL cached documents in v1; "
    "keyed cache retrieval has no live-captured wire shape (#119 census) — "
    "see get_schema_template(schema_name='cache_property_authoring')."
)

# The composition-owned Document Cache emitted for staged handoffs. Never
# collides with the base assembly keys (source_*/transform_*/target_*/
# main_process) or the fanout keys (target_<slug>_*: 'document' never
# slugifies out of a part key into this exact form because the emitted
# fanout keys always end in _rest_connection/_rest_operation).
_HANDOFF_CACHE_KEY = "handoff_document_cache"

# A staged handoff spends one of Boomi Branch's 25 legs on the cache_put
# staging leg, so the target budget drops by one.
_MAX_FANOUT_TARGETS_WITH_CACHE = _MAX_FANOUT_TARGETS - 1


# ---------------------------------------------------------------------------
# Input models
# ---------------------------------------------------------------------------


class CompositionPart(BaseModel):
    """One composable part: a role kind plus its archetype sub-parameters."""

    model_config = ConfigDict(extra="forbid")

    key: str = Field(
        ...,
        description=(
            "Unique part identifier (slug-safe). For fanout rest_target parts "
            "it derives the emitted component keys (target_<key>_rest_connection "
            "/ _rest_operation)."
        ),
    )
    kind: Literal["db_source", "transform", "rest_target"] = Field(
        ...,
        description=(
            "Part role: db_source (archetype 'source' parameters), transform "
            "(archetype 'transform' parameters), or rest_target (archetype "
            "'target' parameters)."
        ),
    )
    label: Optional[str] = Field(
        default=None,
        description=(
            "Optional human label; drives the emitted display names for fanout "
            "REST components ('<prefix> <label> REST Connection' / '... REST "
            "Send'). Defaults to a humanized part key."
        ),
    )
    parameters: Dict[str, Any] = Field(
        ...,
        description=(
            "Archetype sub-contract payload for this part's kind: "
            "DatabaseSource for db_source, TransformConfig for transform, "
            "RestTarget for rest_target."
        ),
    )

    @field_validator("key")
    @classmethod
    def _strip_nonblank_key(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("part key must be a non-blank string")
        return stripped


class DocumentHandoff(BaseModel):
    """How documents move from one part to the next.

    ``document_stream`` (default): documents flow down the single process
    stream (source -> map -> branch legs). ``document_cache`` (M8.1 / issue
    #132, transform -> rest_target links only): the composed Branch stages
    the mapped documents into an auto-emitted in-spec Document Cache via a
    target-less ``cache_put`` leg, and each cache-mode target leg re-reads
    them with an all-documents ``cache_get`` before its REST send. The keyed
    retrieval fields (``doc_cache_index`` / ``cache_key_values``) are
    allow-listed only so composition validation can reject them with the
    NAMED gated error (mirrors the builder's cache_get gate) — any value
    fails with COMPOSITION_UNSUPPORTED_TOPOLOGY until the #119 census
    captures a populated cacheKeyValues wire shape.
    """

    model_config = ConfigDict(extra="forbid")

    mode: Literal["document_stream", "document_cache"] = Field(
        default="document_stream",
        description=(
            "Handoff mode. 'document_stream' flows documents down the "
            "process stream; 'document_cache' stages them through an "
            "auto-emitted Document Cache (target-less cache_put staging leg "
            "+ all-documents cache_get before each consuming REST send)."
        ),
    )
    doc_cache_index: Optional[int] = Field(
        default=None,
        description=(
            "GATED (#119 census): keyed cache retrieval has no live-captured "
            "wire shape; any value is rejected at composition validation."
        ),
    )
    cache_key_values: Optional[List[Any]] = Field(
        default=None,
        description=(
            "GATED (#119 census): keyed cache retrieval has no live-captured "
            "wire shape; any value is rejected at composition validation."
        ),
    )


class CompositionLink(BaseModel):
    """Directed document handoff between two declared parts."""

    model_config = ConfigDict(extra="forbid")

    from_part: str = Field(..., description="Producer part key.")
    to_part: str = Field(..., description="Consumer part key.")
    handoff: DocumentHandoff = Field(
        default_factory=DocumentHandoff,
        description="Handoff semantics for this link (v1: document_stream).",
    )


class CompositionOptions(BaseModel):
    """Composition-level options (naming/execution mirror the archetype)."""

    model_config = ConfigDict(extra="forbid")

    naming: Dict[str, Any] = Field(
        ...,
        description=(
            "Archetype NamingConfig payload (integration_name, "
            "component_prefix, optional folder_path/component_names/"
            "convention/runtime_hints)."
        ),
    )
    execution: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Archetype ExecutionConfig payload; defaults to a manual trigger. "
            "A watermark applies to the FIRST rest_target's query parameters "
            "only (fanout targets reject watermark-sourced parameters in v1)."
        ),
    )
    links: Optional[List[CompositionLink]] = Field(
        default=None,
        description=(
            "Optional explicit handoff links. Omitted: the v1 star topology "
            "is inferred (db_source -> transform -> each rest_target, all "
            "document_stream). When given, the links must describe exactly "
            "that star."
        ),
    )


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def _topology_error(message: str, *, field: str, hint: Optional[str] = None) -> BuilderValidationError:
    return BuilderValidationError(
        message,
        error_code=COMPOSITION_UNSUPPORTED_TOPOLOGY,
        field=field,
        hint=hint
        or (
            "v1 compose_archetypes supports exactly one db_source part, one "
            "transform part, and 2..25 rest_target parts (a Branch fan-out)."
        ),
    )


def _derived_label(part: CompositionPart) -> str:
    if part.label and part.label.strip():
        return part.label.strip()
    return re.sub(r"[_\-]+", " ", part.key).strip().title()


def _fanout_prefix(part: CompositionPart) -> str:
    return f"target_{slugify(part.key)}"


def _split_parts(
    parts: List[CompositionPart],
) -> Dict[str, List[CompositionPart]]:
    grouped: Dict[str, List[CompositionPart]] = {kind: [] for kind in _PART_KINDS}
    for part in parts:
        grouped[part.kind].append(part)
    return grouped


def _validate_links(
    links: List[CompositionLink],
    db_part: CompositionPart,
    transform_part: CompositionPart,
    target_parts: List[CompositionPart],
) -> None:
    """Explicit links must describe exactly the v1 star topology."""
    part_keys = {db_part.key, transform_part.key} | {p.key for p in target_parts}
    for i, link in enumerate(links):
        for endpoint_field, endpoint in (
            ("from_part", link.from_part),
            ("to_part", link.to_part),
        ):
            if endpoint not in part_keys:
                raise _topology_error(
                    f"options.links[{i}].{endpoint_field} references an "
                    "undeclared part key.",
                    field=f"options.links[{i}].{endpoint_field}",
                    hint="Every link endpoint must be a declared parts[].key.",
                )
        for gated_field in ("doc_cache_index", "cache_key_values"):
            if getattr(link.handoff, gated_field) is not None:
                raise _topology_error(
                    f"options.links[{i}].handoff.{gated_field} is gated — "
                    "keyed cache retrieval has no live-captured wire shape "
                    "(#119 census).",
                    field=f"options.links[{i}].handoff.{gated_field}",
                    hint=_KEYED_HANDOFF_GATED_HINT,
                )
        if link.handoff.mode == "document_cache" and not (
            link.from_part == transform_part.key
            and link.to_part != db_part.key
            and link.to_part != transform_part.key
        ):
            raise _topology_error(
                f"options.links[{i}].handoff.mode 'document_cache' is only "
                "supported on transform -> rest_target links.",
                field=f"options.links[{i}].handoff.mode",
                hint=(
                    "The staged cache handoff lowers onto the Branch fan-out "
                    "(a target-less cache_put staging leg + cache_get "
                    "consuming legs); the db_source -> transform edge always "
                    "streams."
                ),
            )
    expected = {(db_part.key, transform_part.key)} | {
        (transform_part.key, p.key) for p in target_parts
    }
    declared = {(link.from_part, link.to_part) for link in links}
    if len(declared) != len(links) or declared != expected:
        raise _topology_error(
            "options.links must describe exactly the v1 star topology: "
            "db_source -> transform, then transform -> each rest_target "
            "(no duplicates, no extra or missing edges).",
            field="options.links",
        )


def _handoff_modes(
    links: Optional[List[CompositionLink]],
    transform_part: CompositionPart,
    target_parts: List[CompositionPart],
) -> Dict[str, str]:
    """Per-rest_target handoff mode map (part key -> mode).

    Omitted links mean the inferred all-``document_stream`` star. Assumes the
    link set has already passed :func:`_validate_links`.
    """
    modes = {part.key: "document_stream" for part in target_parts}
    for link in links or []:
        if link.from_part == transform_part.key and link.to_part in modes:
            modes[link.to_part] = link.handoff.mode
    return modes


def validate_composition(
    parts: List[CompositionPart], options: CompositionOptions
) -> None:
    """Deterministic pre-emission validation for the v1 composed topology.

    Raises :class:`BuilderValidationError` with a COMPOSITION_* error code so
    the authoring action layer converts it into the existing structured error
    envelope. Runs BEFORE any spec is emitted; error messages carry counts and
    part keys only — never caller-supplied field names, paths, or values
    (mirrors the archetype's secret-safe error policy).
    """
    if not parts:
        raise _topology_error(
            "parts must be a non-empty list of composition parts.",
            field="parts",
        )

    grouped = _split_parts(parts)
    db_parts = grouped["db_source"]
    transform_parts = grouped["transform"]
    target_parts = grouped["rest_target"]

    # 1. Topology counts.
    if len(db_parts) != 1 or len(transform_parts) != 1:
        raise _topology_error(
            f"v1 composition requires exactly one db_source part and one "
            f"transform part; got {len(db_parts)} db_source / "
            f"{len(transform_parts)} transform.",
            field="parts",
        )
    if not (_MIN_FANOUT_TARGETS <= len(target_parts) <= _MAX_FANOUT_TARGETS):
        raise _topology_error(
            f"v1 composition requires {_MIN_FANOUT_TARGETS}.."
            f"{_MAX_FANOUT_TARGETS} rest_target parts (one Branch leg each); "
            f"got {len(target_parts)}.",
            field="parts",
            hint=(
                "A single REST target needs no composition — use "
                "build_from_archetype(name='database_to_api_sync') instead. "
                "Boomi Branch supports up to 25 legs."
            ),
        )

    # 2. Key / derived-prefix / derived-display-name collisions.
    seen_keys: set = set()
    for part in parts:
        if part.key in seen_keys:
            raise BuilderValidationError(
                f"duplicate part key {part.key!r}.",
                error_code=COMPOSITION_COMPONENT_KEY_COLLISION,
                field="parts",
                hint="Every parts[].key must be unique.",
            )
        seen_keys.add(part.key)
    seen_prefixes: Dict[str, str] = {}
    for part in target_parts[1:]:
        prefix = _fanout_prefix(part)
        if prefix in _RESERVED_PREFIXES or prefix in seen_prefixes:
            raise BuilderValidationError(
                f"part {part.key!r} derives component-key prefix {prefix!r}, "
                "which collides with "
                + (
                    "a reserved base prefix."
                    if prefix in _RESERVED_PREFIXES
                    else f"part {seen_prefixes[prefix]!r}."
                ),
                error_code=COMPOSITION_COMPONENT_KEY_COLLISION,
                field="parts",
                hint=(
                    "Fanout component keys derive from slugified part keys; "
                    "pick part keys that slugify to distinct, non-reserved "
                    "values."
                ),
            )
        seen_prefixes[prefix] = part.key
    seen_labels: Dict[str, str] = {}
    for part in target_parts:
        label = _derived_label(part)
        if label in seen_labels:
            raise BuilderValidationError(
                f"parts {seen_labels[label]!r} and {part.key!r} derive the "
                "same display label — the emitted REST component names would "
                "collide.",
                error_code=COMPOSITION_COMPONENT_KEY_COLLISION,
                field="parts",
                hint="Give each rest_target part a distinct label (or key).",
            )
        seen_labels[label] = part.key

    db_part = db_parts[0]
    transform_part = transform_parts[0]

    # 3. Explicit links (when given) must match the inferred star.
    if options.links is not None:
        _validate_links(options.links, db_part, transform_part, target_parts)
        modes = _handoff_modes(options.links, transform_part, target_parts)
        if (
            any(mode == "document_cache" for mode in modes.values())
            and len(target_parts) > _MAX_FANOUT_TARGETS_WITH_CACHE
        ):
            raise _topology_error(
                "a document_cache handoff composes a target-less cache_put "
                "staging leg in addition to the target legs, so at most "
                f"{_MAX_FANOUT_TARGETS_WITH_CACHE} rest_target parts fit "
                f"the {_MAX_FANOUT_TARGETS}-leg Branch budget; got "
                f"{len(target_parts)}.",
                field="options.links",
                hint=(
                    f"Drop to {_MAX_FANOUT_TARGETS_WITH_CACHE} rest_target "
                    "parts, or use document_stream handoffs (no staging "
                    "leg)."
                ),
            )

    # 4. Per-part parameter validation via the archetype sub-contracts.
    #    A pydantic ValidationError propagates to the action layer, which maps
    #    it to PARAM_VALIDATION_FAILED with field_errors[].
    source_model = DatabaseSource(**db_part.parameters)
    transform_model = TransformConfig(**transform_part.parameters)
    target_models = [RestTarget(**p.parameters) for p in target_parts]

    # 5. Producer -> consumer contract checks (COMPOSITION_CONTRACT_MISMATCH).
    source_fields = {
        f.name for f in source_model.read_operation.result_schema.fields
    }
    first_leaves = _flatten_payload_profile_leaves(target_models[0].payload_profile)
    first_required = _required_simple_leaf_paths(target_models[0].payload_profile)

    unknown_source_refs = 0
    unknown_target_refs = 0
    bound_paths: set = set()
    for op in transform_model.operations:
        if isinstance(op, DirectTransformOperation):
            inputs = [op.source_field]
            outputs = [op.target_path]
        elif isinstance(op, MapFunctionTransformOperation):
            inputs = list(op.inputs)
            outputs = [op.target_path]
        elif isinstance(op, MapScriptTransformOperation):
            inputs = list(op.inputs)
            outputs = list(op.outputs)
        else:  # pragma: no cover — union is closed by TransformConfig
            continue
        unknown_source_refs += sum(1 for name in inputs if name not in source_fields)
        for out in outputs:
            if out in first_leaves:
                bound_paths.add(out)
            else:
                unknown_target_refs += 1
    unmapped_required = len(first_required - bound_paths)

    mismatches: List[str] = []
    if unknown_source_refs:
        mismatches.append(
            f"the transform part references {unknown_source_refs} source "
            f"field(s) not declared by db_source part {db_part.key!r}"
        )
    if unknown_target_refs:
        mismatches.append(
            f"the transform part writes {unknown_target_refs} target path(s) "
            f"that are not declared simple leaves of rest_target part "
            f"{target_parts[0].key!r}"
        )
    if unmapped_required:
        mismatches.append(
            f"{unmapped_required} required target leaf path(s) of rest_target "
            f"part {target_parts[0].key!r} are not produced by any transform "
            "operation"
        )
    for part, model in zip(target_parts[1:], target_models[1:]):
        if model.payload_profile.format != target_models[0].payload_profile.format:
            mismatches.append(
                f"rest_target part {part.key!r} declares payload format "
                f"{model.payload_profile.format!r} but the shared transform "
                f"produces {target_models[0].payload_profile.format!r}"
            )
            continue
        leaves = _flatten_payload_profile_leaves(model.payload_profile)
        required = _required_simple_leaf_paths(model.payload_profile)
        if leaves != first_leaves or required != first_required:
            mismatches.append(
                f"rest_target part {part.key!r} declares a payload_profile "
                f"whose leaf contract differs from part "
                f"{target_parts[0].key!r} — one shared transform feeds every "
                "fanout leg, so all target profiles must declare identical "
                "leaf paths, data types, and required flags"
            )
    if mismatches:
        raise BuilderValidationError(
            "composed part contracts do not line up: " + "; ".join(mismatches) + ".",
            error_code=COMPOSITION_CONTRACT_MISMATCH,
            field="parts",
            hint=(
                "Align the db_source result_schema, the transform operations, "
                "and every rest_target payload_profile before composing; "
                "contract validation runs before any spec is emitted."
            ),
        )

    # 6. v1 composability gates the flow_sequence surface cannot express.
    for part, model in zip(target_parts, target_models):
        if model.send_request.path_replacements:
            raise _topology_error(
                f"rest_target part {part.key!r} declares "
                "send_request.path_replacements; per-document dynamic paths "
                "are not supported inside a composed fan-out in v1.",
                field="parts",
                hint=(
                    "flow_sequence branch legs are plain REST targets (no "
                    "dynamic_path). Use static paths, or a standalone "
                    "database_to_api_sync build for the dynamic-path target."
                ),
            )
    for part, model in zip(target_parts[1:], target_models[1:]):
        if any(
            qp.value_source == "watermark"
            for qp in model.send_request.query_parameters
        ):
            raise _topology_error(
                f"rest_target part {part.key!r} declares a watermark-sourced "
                "query parameter; watermark intent is recorded for the FIRST "
                "rest_target only in v1.",
                field="parts",
                hint=(
                    "Move the watermark-sourced parameter to the first "
                    "rest_target part, or use literal query parameters on "
                    "fanout targets."
                ),
            )


# ---------------------------------------------------------------------------
# Emission helpers
# ---------------------------------------------------------------------------


def _base_parameters_payload(
    db_part: CompositionPart,
    transform_part: CompositionPart,
    first_target: CompositionPart,
    options: CompositionOptions,
    component_prefix_hint: str,
) -> Dict[str, Any]:
    """Assemble the DatabaseToApiSyncParameters payload for the base assembly.

    The first rest_target becomes the archetype's single target (Branch leg 1).
    Reliability is pinned to the archetype's no-op default — flow_sequence
    rejects Try/Catch composition in v1, and CompositionOptions exposes no
    reliability surface, so nothing the caller supplied is dropped here.
    """
    naming = dict(options.naming or {})
    # The first target's derived label (explicit label, else humanized part
    # key — the SAME default every fanout leg gets) drives the base REST
    # pair's display names, so leg 1 stays as uniquely named as legs 2..N and
    # cannot accidentally match an existing generic component under
    # conflict_policy='reuse'. Explicit caller overrides win — under EITHER
    # supported key form (_named checks the role key 'rest_connection' before
    # the prefixed emitted key 'target_rest_connection', so populating the
    # role key when only the emitted key is set would shadow the caller's
    # override).
    # Inject defaults only when component_names is absent or a real dict — a
    # PRESENT malformed value (a string, int, list, ...) must flow VERBATIM
    # into NamingConfig validation and fail as PARAM_VALIDATION_FAILED,
    # exactly as it would on the standalone archetype, never be replaced (0 /
    # []) or crash the copy ('bad' -> ARCHETYPE_BUILD_FAILED).
    raw_component_names = naming.get("component_names")
    if raw_component_names is None or isinstance(raw_component_names, dict):
        component_names = dict(raw_component_names or {})
        label = _derived_label(first_target)
        for role, default_name in (
            (ROLE_REST_CONNECTION, f"{component_prefix_hint} {label} REST Connection"),
            (ROLE_REST_OPERATION, f"{component_prefix_hint} {label} REST Send"),
        ):
            emitted_key = primitive_component_key(_TARGET_PREFIX, role)
            emitted_value = component_names.get(emitted_key)
            emitted_override = isinstance(emitted_value, str) and emitted_value.strip()
            # Never overwrite a PRESENT role-key value — even a malformed one
            # (e.g. an int) must reach NamingConfig validation. Only a
            # genuinely absent role key gets the derived default, and only
            # when no non-blank emitted-key override would be shadowed.
            if role not in component_names and not emitted_override:
                component_names[role] = default_name
        naming["component_names"] = component_names
    return {
        "naming": naming,
        "source": db_part.parameters,
        "transform": transform_part.parameters,
        "target": first_target.parameters,
        "execution": options.execution or {"trigger": {"mode": "manual"}},
        "reliability": {
            "retry": {"max_attempts": 1},
            "dlq": {"enabled": False},
            "error_classifier": {},
        },
    }


def _fanout_target_params(
    part: CompositionPart,
    target: RestTarget,
    part_index: int,
    key_prefix: str,
    conn_name: str,
    op_name: str,
) -> RestSendWithRetryParameters:
    """Mirror the archetype's ``_build_rest_send_params`` for one fanout part.

    Same create-auth gate (an executable created REST connection is always
    unauthenticated), same reuse passthrough, same generated-profile binding —
    parameterized by the part's derived key prefix and display names.
    """
    binding = target.binding
    send = target.send_request

    if binding.mode == "create":
        settings = binding.settings  # guaranteed present by the contract validator
        auth = _REST_CREATE_AUTH_MAP.get(settings.auth_mode)
        if auth is None:
            raise BuilderValidationError(
                "REST create-mode auth is not supported for executable "
                "assembly (only an unauthenticated connection can be "
                "created); use an existing connection instead.",
                error_code=UNSUPPORTED_REST_AUTH_MODE,
                field=f"parts[{part_index}].parameters.binding.settings.auth_mode",
                hint=(
                    "Set binding.mode='reuse' with an existing REST Client "
                    "connection (component_id or component_name) for secured "
                    "auth. The composition never echoes credentials."
                ),
            )
        connection: Dict[str, Any] = {
            "mode": "create",
            "base_url": settings.base_url,
            "auth": auth,
        }
    else:
        connection = {"mode": "reuse"}
        if binding.component_id:
            connection["component_id"] = binding.component_id
        if binding.component_name:
            connection["component_name"] = binding.component_name

    target_profile_key = primitive_component_key(_TRANSFORM_PREFIX, ROLE_TARGET_PROFILE)
    operation: Dict[str, Any] = {
        "method": send.method,
        "path": send.path,
        # Every fanout leg sends the SAME shared payload profile the transform
        # produces (leaf-contract equality is enforced by validate_composition).
        "request_profile_id": f"$ref:{target_profile_key}",
        "request_profile_type": "json",
    }
    literal_qp = {
        qp.name: qp.literal_value
        for qp in send.query_parameters
        if qp.value_source == "literal" and qp.literal_value is not None
    }
    if literal_qp:
        operation["query_parameters"] = literal_qp

    return _coerce_primitive_params(
        RestSendWithRetryParameters,
        {
            "key_prefix": key_prefix,
            "connection": connection,
            "operation": operation,
            "component_names": {"connection": conn_name, "operation": op_name},
        },
        field=f"parts[{part_index}]",
    )


def _leg_binding_from_keys(conn_key: str, op_key: str, method: str) -> Dict[str, Any]:
    return {
        "connector_type": "rest",
        "connection_id": f"$ref:{conn_key}",
        "operation_id": f"$ref:{op_key}",
        "action_type": method.upper(),
    }


def _emit_handoff_cache_component(
    naming: Any, profile_component: IntegrationComponentSpec
) -> IntegrationComponentSpec:
    """Emit the composition-owned Document Cache for staged handoffs.

    The cache binds (``$ref`` + ``depends_on``) to the SAME shared target
    profile every fanout leg sends, so staged and streamed documents carry
    one contract. The index key derives from the profile's own generated
    field index (first mappable simple leaf in deterministic pre-order) —
    the ``element_key`` always references a real emitted profile element,
    mirroring examples/m11/cache_property_authoring_join.integration.json.
    """
    profile_key = profile_component.key
    field_index = JSONGeneratedProfileBuilder.build_field_index(
        dict(profile_component.config)
    )
    leaf = next(
        (
            entry
            for entry in field_index.values()
            if entry.get("mappable") and entry.get("kind") == "simple"
        ),
        None,
    )
    if leaf is None:  # pragma: no cover — the composition contract requires
        # at least one transform-mapped simple leaf on the shared profile.
        raise BuilderValidationError(
            "the shared target profile declares no mappable simple leaf to "
            "key the handoff Document Cache index on.",
            error_code=COMPOSITION_UNSUPPORTED_TOPOLOGY,
            field="parts",
            hint=(
                "Declare at least one simple leaf on the rest_target "
                "payload_profile."
            ),
        )
    name = f"{naming.component_prefix} Handoff Cache"
    return IntegrationComponentSpec(
        key=_HANDOFF_CACHE_KEY,
        type="documentcache",
        action="create",
        name=name,
        config={
            "component_type": "documentcache",
            "component_name": name,
            "profile_type": "profile.json",
            "profile_id": f"$ref:{profile_key}",
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
        },
        depends_on=[profile_key],
    )


def _rewrite_process_for_fanout(
    process: IntegrationComponentSpec,
    map_key: str,
    legs: List[Dict[str, Any]],
    extra_dep_keys: List[str],
    extra_extension_connections: List[Dict[str, Any]],
) -> None:
    """Rewrite the base main process onto the flow_sequence Branch surface.

    ``legs`` are pre-built branch leg dicts (``{steps?, target?}``) so the
    caller controls the per-leg shape: plain ``{"target": ...}`` for stream
    legs, a target-less ``cache_put`` staging leg, and ``cache_get`` +
    target consuming legs for staged handoffs. The top-level ``target``
    stays as emitted — ``flow_sequence`` requires it as the default success
    terminal even though the terminal ``branch`` means it is never emitted
    as a shape (#117). The no-op reliability block is dropped (disabled DLQ
    / retry 0 emits nothing on either path), and the transform moves from
    the top-level slot into the sequence's ``map_ref`` step so the composed
    validator's passthrough gate holds.
    """
    config = process.config
    config["transform"] = {"mode": "passthrough"}
    config.pop("reliability", None)
    config["flow_sequence"] = [
        {
            "kind": "map_ref",
            "map_ref": f"$ref:{map_key}",
            "label": "Shared transform",
        },
        {
            "kind": "branch",
            "label": "Fan out",
            "legs": legs,
        },
    ]
    for dep_key in extra_dep_keys:
        if dep_key not in process.depends_on:
            process.depends_on.append(dep_key)
    if extra_extension_connections:
        extensions = config.setdefault("process_extensions", {})
        connections = extensions.setdefault("connections", [])
        connections.extend(extra_extension_connections)


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------


def compose_archetypes(
    parts: List[Any],
    options: Optional[Any] = None,
) -> IntegrationSpecV1:
    """Compose typed parts into ONE coherent ``IntegrationSpecV1``.

    ``parts`` is a list of :class:`CompositionPart` payloads (dicts or model
    instances); ``options`` is a :class:`CompositionOptions` payload. Raises a
    pydantic ``ValidationError`` for malformed inputs and a
    :class:`BuilderValidationError` (COMPOSITION_* / builder codes) for
    semantic failures — both BEFORE any spec exists, so an invalid composition
    can never reach a Boomi mutation.
    """
    part_models = [
        part
        if isinstance(part, CompositionPart)
        else CompositionPart.model_validate(part)
        for part in (parts or [])
    ]
    if options is None:
        options_model = CompositionOptions(naming={})
    elif isinstance(options, CompositionOptions):
        options_model = options
    else:
        options_model = CompositionOptions.model_validate(options)

    validate_composition(part_models, options_model)

    grouped = _split_parts(part_models)
    db_part = grouped["db_source"][0]
    transform_part = grouped["transform"][0]
    target_parts = grouped["rest_target"]

    component_prefix_hint = str(
        (options_model.naming or {}).get("component_prefix") or ""
    ).strip()
    payload = _base_parameters_payload(
        db_part, transform_part, target_parts[0], options_model, component_prefix_hint
    )
    params_obj = DatabaseToApiSyncArchetype.validate_parameters(payload)
    spec = DatabaseToApiSyncArchetype.emit_spec(params_obj)

    naming = params_obj.naming
    context = PrimitiveBuildContext(
        integration_name=naming.integration_name,
        component_prefix=naming.component_prefix,
        folder_path=naming.folder_path,
    )

    process = spec.components[-1]
    assert process.key == _MAIN_PROCESS_KEY  # archetype emission contract

    map_key = primitive_component_key(_TRANSFORM_PREFIX, ROLE_TRANSFORM_MAP)
    base_target_binding = {
        key: value
        for key, value in process.config["target"].items()
        if key in ("connector_type", "connection_id", "operation_id", "action_type")
    }
    target_bindings: List[Dict[str, Any]] = [base_target_binding]

    modes = _handoff_modes(options_model.links, transform_part, target_parts)
    staged = any(mode == "document_cache" for mode in modes.values())

    extra_components: List[IntegrationComponentSpec] = []
    extra_dep_keys: List[str] = []
    extra_extension_connections: List[Dict[str, Any]] = []
    fanout_summaries: List[Dict[str, Any]] = []
    for index, part in enumerate(target_parts[1:], start=1):
        part_index = part_models.index(part)
        target_model = RestTarget(**part.parameters)
        prefix = _fanout_prefix(part)
        label = _derived_label(part)
        conn_name = f"{naming.component_prefix} {label} REST Connection"
        op_name = f"{naming.component_prefix} {label} REST Send"
        params = _fanout_target_params(
            part, target_model, part_index, prefix, conn_name, op_name
        )
        extra_components.extend(
            RestSendWithRetryPrimitive.emit_components(context, params)
        )
        conn_key = primitive_component_key(prefix, ROLE_REST_CONNECTION)
        op_key = primitive_component_key(prefix, ROLE_REST_OPERATION)
        extra_dep_keys.extend([conn_key, op_key])
        target_bindings.append(
            _leg_binding_from_keys(
                conn_key, op_key, target_model.send_request.method
            )
        )
        # Mirror the base archetype's environment-extension policy for the
        # fanout connection: reuse-mode credentials are declared as
        # per-environment override points; create-mode REST is always
        # unauthenticated (nothing to declare), endpoint opt-in stays off.
        rest_fields = rest_connection_extension_fields(
            credentials=(target_model.binding.mode == "reuse"), endpoint=False
        )
        if rest_fields:
            extra_extension_connections.append(
                {
                    "connection_id": f"$ref:{conn_key}",
                    "connector_type": "rest",
                    "fields": rest_fields,
                }
            )

        binding_mode = target_model.binding.mode
        endpoint: Dict[str, Any] = {
            "key": f"rest_target_{slugify(part.key)}",
            "type": "rest",
            "direction": "target",
            "binding_mode": binding_mode,
            "method": target_model.send_request.method,
            "executable": False,
        }
        if binding_mode == "create" and target_model.binding.settings is not None:
            endpoint["auth_mode"] = target_model.binding.settings.auth_mode
        else:
            if target_model.binding.component_id:
                endpoint["component_id"] = target_model.binding.component_id
            if target_model.binding.component_name:
                endpoint["component_name"] = target_model.binding.component_name
        spec.endpoints.append(endpoint)

        summary: Dict[str, Any] = {
            "part_key": part.key,
            "leg": index + 1,
            "method": target_model.send_request.method,
            "binding_mode": binding_mode,
        }
        if target_model.send_request.expected_status_codes:
            summary["expected_status_codes"] = list(
                target_model.send_request.expected_status_codes
            )
        # Mirror the base archetype's _deferred_intent policy for headers the
        # builders cannot emit: record the intent (counts only — never the
        # caller-authored keys/values) instead of silently dropping it. The
        # base target's own default_headers are already recorded under
        # operational_intent.deferred by the archetype emission.
        if (
            binding_mode == "create"
            and target_model.binding.settings is not None
            and target_model.binding.settings.default_headers
        ):
            summary["deferred"] = {
                "default_headers": {
                    "count": len(target_model.binding.settings.default_headers),
                    "note": (
                        "REST default headers are not emitted onto the "
                        "created connection; use binding.mode='reuse' for a "
                        "connection needing them."
                    ),
                }
            }
        fanout_summaries.append(summary)

    # Lower per-target handoff modes onto pre-built branch legs. A staged
    # handoff inserts ONE target-less cache_put staging leg immediately
    # before the FIRST consuming leg (branch legs run sequentially, so the
    # #123 write-before-read lineage gate holds for every later cache_get),
    # and each cache-mode leg re-reads the staged documents before its send.
    cache_ref = f"$ref:{_HANDOFF_CACHE_KEY}"
    legs: List[Dict[str, Any]] = []
    leg_positions: Dict[str, int] = {}
    staging_leg_position: Optional[int] = None
    for part, binding in zip(target_parts, target_bindings):
        if modes[part.key] == "document_cache":
            if staging_leg_position is None:
                legs.append(
                    {
                        "steps": [
                            {
                                "kind": "cache_put",
                                "document_cache_id": cache_ref,
                                "label": "Stage mapped documents",
                            }
                        ]
                    }
                )
                staging_leg_position = len(legs)
            leg: Dict[str, Any] = {
                "steps": [
                    {
                        "kind": "cache_get",
                        "document_cache_id": cache_ref,
                        "label": "Consume staged documents",
                    }
                ],
                "target": binding,
            }
        else:
            leg = {"target": binding}
        legs.append(leg)
        leg_positions[part.key] = len(legs)

    if staged:
        profile_key = primitive_component_key(
            _TRANSFORM_PREFIX, ROLE_TARGET_PROFILE
        )
        profile_component = next(
            component
            for component in spec.components
            if component.key == profile_key
        )
        extra_components.insert(
            0, _emit_handoff_cache_component(naming, profile_component)
        )
        extra_dep_keys.append(_HANDOFF_CACHE_KEY)
        # Staged branch legs shift the raw positions: re-point each fanout
        # summary at its ACTUAL branch leg and record its handoff mode.
        for summary in fanout_summaries:
            summary["leg"] = leg_positions[summary["part_key"]]
            summary["handoff"] = modes[summary["part_key"]]

    _rewrite_process_for_fanout(
        process, map_key, legs, extra_dep_keys, extra_extension_connections
    )

    # Pre-emission self-check: the rewritten config must pass the SAME
    # validator build_integration(plan/apply) runs, so an incoherent
    # composition fails HERE — before any spec leaves this function, and long
    # before any Boomi mutation.
    residual = ProcessFlowBuilder.validate_config(
        process.config, depends_on=process.depends_on
    )
    if residual is not None:
        raise BuilderValidationError(
            f"composed process config failed builder validation: {residual}",
            error_code=COMPOSITION_UNSUPPORTED_TOPOLOGY,
            field=residual.field,
            hint=residual.hint,
        )

    spec.components = spec.components[:-1] + extra_components + [process]

    spec.flows.append(
        {
            "key": "fanout",
            "name": "Fan out to REST targets",
            "source": "transform",
            "target": "rest_target",
            "operation": "branch",
            "executable": False,
            "legs": len(legs),
        }
    )
    handoff_mode = "document_stream"
    if staged:
        handoff_mode = (
            "document_cache"
            if all(mode == "document_cache" for mode in modes.values())
            else "mixed"
        )
    spec.validation_rules["component_count"] = len(spec.components)
    composition_rules: Dict[str, Any] = {
        "topology": "db_source->transform->rest_fanout",
        "handoff": handoff_mode,
        "fanout_targets": len(target_parts),
        "parts": [{"key": p.key, "kind": p.kind} for p in part_models],
        "first_target_part": target_parts[0].key,
        "fanout_legs": fanout_summaries,
    }
    if staged:
        composition_rules["cache_component_key"] = _HANDOFF_CACHE_KEY
        composition_rules["staging_leg"] = staging_leg_position
        composition_rules["handoff_modes"] = dict(modes)
    spec.validation_rules["composition"] = composition_rules
    return spec
