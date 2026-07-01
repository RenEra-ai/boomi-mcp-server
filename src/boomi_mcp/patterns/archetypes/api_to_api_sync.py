"""api_to_api_sync archetype (M5.7, issue #73).

Exposes API-to-API sync as a **thin preset over the M5 ``sync_pipeline``**: a
REST fetch source, an optional transform map, and a REST send target wired as a
verified-linear ``fetch -> map -> send`` semantic stage graph. Unlike
``database_to_api_sync`` (which pre-lowers its pipeline to a
``process_kind="database_to_api_sync"`` core at emit time), this preset emits the
``main_process`` with ``config.process_kind="sync_pipeline"`` and the stage graph
INTACT, so ``build_integration`` routes it through :class:`SyncPipelineBuilder`.
That is the literal realization of the acceptance criterion "preset maps to
``sync_pipeline`` stages rather than a custom pairwise process builder".

It reuses the shipped #72 (``rest_fetch``), #27 (``field_map``), and #28
(``rest_send_with_retry``) primitives plus the JSON profile builder; the only
component it emits itself is the source response profile (the ``rest_fetch``
primitive *binds* a response profile, it does not generate one). Every byte of
XML and all structured validation are produced by the existing builders through
those primitives — this file emits JSON component specs only, never raw XML, no
payload/body templates, and never calls a live Boomi account.

Scope (M5.7): static REST fetch + static REST send. Runtime-bound query / path /
header / watermark behavior (#96 M5.4a) is OUT of scope — a dynamic ``{token}``
path is rejected at the contract layer, mirroring ``SyncPipelineBuilder``'s
rejection of ``runtime_bindings`` on a stage. Pagination, conditional requests,
retry/DLQ, and schedule activation are likewise out of scope for this thin pass.
"""

from __future__ import annotations

import re
from typing import Annotated, Any, Dict, List, Literal, Optional, Set, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    field_validator,
    model_validator,
)

from ...categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from ...categories.components.builders.json_profile_builder import (
    JSONGeneratedProfileBuilder,
)
from ...categories.components.builders.profile_generation import (
    profile_from_json_schema,
    validate_field_mappings,
)
from ...models.integration_models import (
    IntegrationComponentSpec,
    IntegrationSpecV1,
)
from ...models.pipeline_models import PipelineSpec
from ..base import (
    ArchetypePattern,
    PatternExample,
    PatternKind,
    PatternMetadata,
    PrimitiveBuildContext,
)
from ..primitives._helpers import (
    ROLE_REST_CONNECTION,
    ROLE_REST_OPERATION,
    ROLE_REST_SOURCE_CONNECTION,
    ROLE_REST_SOURCE_OPERATION,
    ROLE_SCRIPT,
    ROLE_TARGET_PROFILE,
    ROLE_TRANSFORM_MAP,
    primitive_component_key,
    raise_for_builder_error,
)
from ..primitives.field_map import FieldMapParameters, FieldMapPrimitive
from ..primitives.rest_fetch import RestFetchParameters, RestFetchPrimitive
from ..primitives.rest_send import (
    RestSendWithRetryParameters,
    RestSendWithRetryPrimitive,
)

# Reuse the sibling archetype's proven contract models + secret-safe helpers so
# the two REST-target presets stay byte-aligned and the diff stays minimal.
from .database_to_api_sync import (
    UNSUPPORTED_REST_AUTH_MODE,
    UNSUPPORTED_SCRIPT_COMPONENT_REF,
    JSONPayloadProfile,
    NamingConfig,
    RestConnectionBinding,
    _REST_CREATE_AUTH_MAP,
    _coerce_primitive_params,
    _component_names,
    _flatten_payload_profile_leaves,
    _named,
    _required_simple_leaf_paths,
    _scan_for_secret_shaped_keys,
    _stripped_nonblank,
)

# ---------------------------------------------------------------------------
# Assembly constants (issue #73)
# ---------------------------------------------------------------------------

# Stable primitive key prefixes — the emitted component keys are
# ``{prefix}_{role}`` (e.g. ``source_rest_source_connection``,
# ``transform_transform_map``, ``target_rest_operation``). The archetype assembles
# its $ref wiring from these deterministic keys, so they must stay stable.
_SOURCE_PREFIX = "source"
_TRANSFORM_PREFIX = "transform"
_TARGET_PREFIX = "target"
_MAIN_PROCESS_KEY = "main_process"
# The one component this preset emits itself: the JSON source response profile the
# rest_fetch source binds and the transform map reads (rest_fetch only *binds* a
# response profile — it does not generate one).
_SOURCE_RESPONSE_PROFILE_KEY = "source_response_profile"

# Role keys for the source response profile name override and the (out-of-scope)
# script name override.
_ROLE_SOURCE_PROFILE = "source_profile"

# A `{token}` in a fetch/send path signals a per-document dynamic path — runtime
# path binding is #96 (M5.4a) and is OUT of scope for this M5.7 preset (and the
# thin sync_pipeline stage rejects runtime_bindings). Reject it at the contract.
_DYNAMIC_PATH_TOKEN_RE = re.compile(r"\{[^{}]*\}")

# Characters not allowed in a Boomi script.mapping variable identifier
# (ScriptMappingBuilder enforces ^[A-Za-z_][A-Za-z0-9_]*$). Used to derive a
# language-safe in-script variable name from a JSON leaf path segment.
_NON_IDENTIFIER_RE = re.compile(r"[^A-Za-z0-9_]+")


def _script_var_name(path: str) -> str:
    """Derive a language-safe map_script variable name from a JSON leaf path.

    The variable is the path's FINAL segment, sanitized: every run of
    non-identifier characters collapses to ``_`` (so ``order-id`` -> ``order_id``)
    and a leading digit / empty result is prefixed with ``_`` so the result always
    matches ScriptMappingBuilder's ``^[A-Za-z_][A-Za-z0-9_]*$``. Underscores are
    identifier-safe, so leaves that are ALREADY valid identifiers are preserved
    verbatim — including leading/trailing underscores (``_id`` stays ``_id``, and
    ``_id`` vs ``id`` are distinct, not a collision). Two distinct paths can still
    derive the same variable (e.g. ``Root/a/id`` and ``Root/b/id`` -> the shared
    ``id`` namespace); the contract validator rejects that collision rather than
    letting it fail deep in the builder.
    """
    leaf = path.rsplit("/", 1)[-1]
    sanitized = _NON_IDENTIFIER_RE.sub("_", leaf)
    if not sanitized or sanitized[0].isdigit():
        sanitized = "_" + sanitized
    return sanitized

# Example payload sentinel — intentionally NOT a reusable path/payload template.
_EXAMPLE_PATH_SENTINEL = "/v1/<<source resource>>"


def _reject_dynamic_path(value: str) -> str:
    """Reject a `{token}` dynamic path (runtime path binding is #96, out of scope).

    Shared by the static-REST presets (api_to_api_sync, api_to_database_sync), so
    the message stays preset-neutral — it names no single preset/milestone.
    """
    stripped = _stripped_nonblank(value)
    if _DYNAMIC_PATH_TOKEN_RE.search(stripped):
        raise ValueError(
            "path must be static for this sync_pipeline preset; a "
            "'{token}' per-document dynamic path is runtime-bound behavior owned "
            "by #96 (M5.4a) and is not exposed here. Use a static path."
        )
    return stripped


# ---------------------------------------------------------------------------
# Source / target request contracts (static REST, no runtime binding)
# ---------------------------------------------------------------------------


class ApiFetchRequest(BaseModel):
    """Static REST GET fetch-request configuration for the source stage."""

    model_config = ConfigDict(extra="forbid")

    path: str = Field(
        ...,
        description=(
            "Static endpoint path appended to the source connection base_url "
            "(e.g. '/v1/customers'). Must be non-blank and must NOT contain a "
            "'{token}' dynamic segment — runtime path binding is #96 (M5.4a) and "
            "is out of scope for this preset."
        ),
    )
    query_parameters: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Optional static query-string parameters applied to the GET request. "
            "REST Client query parameters are static (Boomi UI verified); a "
            "per-request dynamic value is out of scope (#96)."
        ),
    )
    request_headers: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional static request headers applied to the GET request.",
    )
    follow_redirects: Optional[str] = Field(
        default=None, description="Redirect policy: NONE | STRICT | LAX."
    )
    return_application_errors: Optional[StrictBool] = Field(
        default=None,
        description="Whether the operation surfaces application-level errors instead of failing.",
    )
    track_response: Optional[StrictBool] = Field(
        default=None, description="Whether the connector tracks the response document."
    )

    @field_validator("path")
    @classmethod
    def _validate_static_path(cls, value: str) -> str:
        return _reject_dynamic_path(value)


class ApiSendRequest(BaseModel):
    """Static REST send-request configuration for the target stage."""

    model_config = ConfigDict(extra="forbid")

    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = Field(
        default="POST",
        description="HTTP method used for the REST send request.",
    )
    path: str = Field(
        ...,
        description=(
            "Static endpoint path appended to the target connection base_url "
            "(e.g. '/v1/items'). Must be non-blank and must NOT contain a "
            "'{token}' dynamic segment — runtime path binding is #96 (M5.4a) and "
            "is out of scope for this preset."
        ),
    )
    query_parameters: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional static query-string parameters applied to the send request.",
    )
    request_headers: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional static request headers applied to the send request.",
    )
    expected_status_codes: List[int] = Field(
        default_factory=lambda: [200, 201, 202],
        description=(
            "HTTP status codes considered successful for the send (recorded as "
            "metadata; routing of other codes through retry/DLQ is out of scope "
            "for this preset). Defaults to 200/201/202."
        ),
    )

    @field_validator("path")
    @classmethod
    def _validate_static_path(cls, value: str) -> str:
        return _reject_dynamic_path(value)


class ApiSource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    binding: RestConnectionBinding = Field(
        ...,
        description=(
            "How the REST SOURCE connector is materialized (create new settings "
            "or reuse an existing Boomi component). Create-mode supports auth "
            "'none' only; secured auth requires mode='reuse'."
        ),
    )
    fetch_request: ApiFetchRequest = Field(
        ...,
        description="Static REST GET fetch-request configuration for the source.",
    )
    response_profile: JSONPayloadProfile = Field(
        ...,
        description=(
            "Caller-supplied JSON profile tree describing the source API response "
            "body. The preset emits a generated JSON profile from this tree and "
            "binds it as the fetch source's output shape; transform source_path "
            "references resolve against its simple leaves."
        ),
    )


class ApiTarget(BaseModel):
    model_config = ConfigDict(extra="forbid")

    binding: RestConnectionBinding = Field(
        ...,
        description=(
            "How the REST TARGET connector is materialized (create new settings "
            "or reuse an existing Boomi component). Create-mode supports auth "
            "'none' only; secured auth requires mode='reuse'."
        ),
    )
    send_request: ApiSendRequest = Field(
        ...,
        description="Static REST send-request configuration for the target.",
    )
    payload_profile: JSONPayloadProfile = Field(
        ...,
        description=(
            "Caller-supplied JSON profile tree describing the target request "
            "body. The preset generates a JSON profile + transform map from it; "
            "only kind='simple' leaves are valid transform targets."
        ),
    )


# ---------------------------------------------------------------------------
# Transform — discriminated typed operations (source_path based, M5.7)
# ---------------------------------------------------------------------------


class _BaseApiTransformOperation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    documentation_hint: Optional[str] = Field(
        default=None,
        description=(
            "Optional non-executable human-readable note about the operation's "
            "intent. Downstream builders must not parse or execute the value."
        ),
    )

    @field_validator("documentation_hint")
    @classmethod
    def _strip_optional_hint(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _stripped_nonblank(value)


class DirectApiTransformOperation(_BaseApiTransformOperation):
    operation_type: Literal["direct"] = Field(
        ...,
        description="Discriminator: 'direct' routes to a one-to-one Boomi map step (#26).",
    )
    source_path: str = Field(
        ...,
        description=(
            "Logical leaf path inside source.response_profile (slash-separated, "
            "e.g. 'Root/id' or 'Root/items[]/sku'). Must reference a kind='simple' "
            "leaf; the cross-field validator rejects unknown paths."
        ),
    )
    target_path: str = Field(
        ...,
        description=(
            "Logical leaf path inside target.payload_profile. Must reference a "
            "kind='simple' leaf; object and array nodes cannot be transform targets."
        ),
    )

    @field_validator("source_path", "target_path")
    @classmethod
    def _strip_required(cls, value: str) -> str:
        return _stripped_nonblank(value)


class MapFunctionApiTransformOperation(_BaseApiTransformOperation):
    operation_type: Literal["map_function"] = Field(
        ...,
        description="Discriminator: 'map_function' routes to a Boomi map function step (#40).",
    )
    function_type: str = Field(
        ...,
        description=(
            "Task-authored function route name (e.g. 'trim', 'uppercase', "
            "'concat'). Surfaced verbatim; issue #40 owns the concrete allowed-set."
        ),
    )
    inputs: List[str] = Field(
        ...,
        min_length=1,
        description=(
            "One or more function inputs. Each entry must reference a kind='simple' "
            "leaf path in source.response_profile."
        ),
    )
    target_path: str = Field(
        ...,
        description="Logical leaf path inside target.payload_profile. Must reference a kind='simple' leaf.",
    )
    parameters: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Optional opaque parameter object surfaced verbatim to issue #40. "
            "The contract does not interpret keys or values."
        ),
    )

    @field_validator("function_type", "target_path")
    @classmethod
    def _strip_required(cls, value: str) -> str:
        return _stripped_nonblank(value)

    @field_validator("inputs")
    @classmethod
    def _strip_inputs(cls, value: List[str]) -> List[str]:
        cleaned: List[str] = []
        for item in value:
            if not isinstance(item, str):
                raise ValueError("inputs entries must be strings")
            cleaned.append(_stripped_nonblank(item))
        return cleaned

    @field_validator("parameters")
    @classmethod
    def _reject_plaintext_secret_keys(
        cls, value: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        # parameters is the only schema-opaque dict the archetype echoes back in
        # the emitted IntegrationSpec, so reject plaintext secret-shaped keys at
        # any nesting depth (mirrors database_to_api_sync). The offending key name
        # is not echoed — callers route secrets via the connector credential_ref.
        if value is None:
            return None
        if _scan_for_secret_shaped_keys(value):
            raise ValueError(
                "map_function.parameters contains a key whose name matches a "
                "forbidden secret-shaped substring (e.g. password / token / "
                "secret / api_key / bearer / authorization). Reference connector "
                "secrets via the connection binding's credential_ref instead; "
                "map_function.parameters is echoed back in the emitted "
                "IntegrationSpec and must not carry plaintext secrets."
            )
        return value


class MapScriptApiTransformOperation(_BaseApiTransformOperation):
    operation_type: Literal["map_script"] = Field(
        ...,
        description=(
            "Discriminator: 'map_script' routes to a Boomi map script step "
            "rendered as an in-map FunctionStep referencing a script.mapping "
            "component materialized from an inline script_body (#41)."
        ),
    )
    script_slot: str = Field(
        ...,
        description="Stable task-authored slot name identifying the script's role in the summary.",
    )
    language: Literal["groovy2", "groovy", "javascript"] = Field(
        ...,
        description="Script language (groovy2 recommended; groovy = legacy Groovy 1; javascript).",
    )
    inputs: List[str] = Field(
        ...,
        min_length=1,
        description=(
            "Source leaf paths consumed by the script (each a kind='simple' leaf "
            "in source.response_profile). The in-script variable for each path is "
            "its final segment sanitized to a language-safe identifier (e.g. "
            "'Root/order-id' -> 'order_id'); two paths that derive the same "
            "variable name (across inputs AND outputs) are rejected."
        ),
    )
    outputs: List[str] = Field(
        ...,
        min_length=1,
        description=(
            "Target leaf paths populated by the script (each a kind='simple' leaf "
            "in target.payload_profile). The in-script variable for each path is "
            "its final segment sanitized to a language-safe identifier; it must not "
            "collide with another input/output variable name."
        ),
    )
    script_component_ref: Optional[str] = Field(
        default=None,
        description=(
            "External script-component reuse is NOT supported by this preset (the "
            "referenced component is not part of the emitted spec). Provide "
            "script_body instead; #51 owns external script reuse."
        ),
    )
    script_body: Optional[str] = Field(
        default=None,
        description=(
            "Caller-authored script source materialized into an in-spec "
            "script.mapping component referenced by the transform.map."
        ),
    )

    @field_validator("script_slot")
    @classmethod
    def _strip_required(cls, value: str) -> str:
        return _stripped_nonblank(value)

    @field_validator("script_component_ref")
    @classmethod
    def _strip_optional_ref(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _stripped_nonblank(value)

    @field_validator("script_body")
    @classmethod
    def _strip_optional_body(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return _stripped_nonblank(value)

    @field_validator("inputs", "outputs")
    @classmethod
    def _strip_input_paths(cls, value: List[str]) -> List[str]:
        cleaned: List[str] = []
        for item in value:
            if not isinstance(item, str):
                raise ValueError("entries must be strings")
            cleaned.append(_stripped_nonblank(item))
        return cleaned

    @model_validator(mode="after")
    def _require_script_material(self) -> "MapScriptApiTransformOperation":
        # Issue #127 A2: a map_script op with neither script_body nor
        # script_component_ref carries no script to materialize. Reject it at
        # the contract layer so callers get a clear origin instead of the
        # downstream FieldMapPrimitive ARCHETYPE_PARAM_INVALID. (This preset
        # only supports inline script_body; script_component_ref reuse is still
        # rejected later at assembly — #51 owns external reuse.)
        if self.script_component_ref is None and self.script_body is None:
            raise ValueError(
                "map_script requires script_body when script_component_ref is "
                "absent"
            )
        return self


ApiTransformOperation = Annotated[
    Union[
        DirectApiTransformOperation,
        MapFunctionApiTransformOperation,
        MapScriptApiTransformOperation,
    ],
    Field(discriminator="operation_type"),
]


class ApiTransformConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operations: List[ApiTransformOperation] = Field(
        ...,
        min_length=1,
        description=(
            "Typed transform operations moving source response leaves into target "
            "payload leaves. Discriminated by operation_type: 'direct' (#26), "
            "'map_function' (#40), 'map_script' (#41). operation_type='xslt' is "
            "rejected with a pointer to issue #42."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _reject_xslt_with_42_pointer(cls, data: Any) -> Any:
        # mode='before' runs before the discriminator picks a variant, so an
        # explicit 'xslt' value gets a friendly #42 pointer rather than the
        # generic union_tag_invalid error. The offending index is included; no
        # caller-supplied content is echoed.
        if isinstance(data, dict):
            ops = data.get("operations")
            if isinstance(ops, list):
                for idx, op in enumerate(ops):
                    if isinstance(op, dict):
                        op_type = op.get("operation_type")
                        if (
                            isinstance(op_type, str)
                            and op_type.strip().lower() == "xslt"
                        ):
                            raise ValueError(
                                f"operations[{idx}].operation_type='xslt' is not "
                                "supported; see issue #42 for the XSLT decision."
                            )
        return data


# ---------------------------------------------------------------------------
# Top-level parameters
# ---------------------------------------------------------------------------


class ApiToApiSyncParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    naming: NamingConfig = Field(
        ...,
        description="Naming, folder, and runtime-hint configuration for the emitted integration.",
    )
    source: ApiSource = Field(
        ...,
        description="REST source: connector binding, static fetch request, and JSON response profile tree.",
    )
    target: ApiTarget = Field(
        ...,
        description="REST target: connector binding, static send request, and JSON payload profile tree.",
    )
    transform: ApiTransformConfig = Field(
        ...,
        description=(
            "Typed transform operations moving source response leaves into target "
            "payload leaves (direct/map_function/map_script; xslt rejected)."
        ),
    )

    @model_validator(mode="after")
    def _validate_transform_refs(self) -> "ApiToApiSyncParameters":
        source_leaves: Dict[str, str] = _flatten_payload_profile_leaves(
            self.source.response_profile
        )
        target_leaves: Dict[str, str] = _flatten_payload_profile_leaves(
            self.target.payload_profile
        )

        unknown_source_refs = 0
        unknown_target_refs = 0
        duplicate_target_bindings = 0
        script_var_collisions = 0
        bound_target_paths: Set[str] = set()

        def _bind(target_path: str) -> None:
            nonlocal duplicate_target_bindings
            if target_path in bound_target_paths:
                duplicate_target_bindings += 1
            else:
                bound_target_paths.add(target_path)

        for op in self.transform.operations:
            if isinstance(op, DirectApiTransformOperation):
                if op.source_path not in source_leaves:
                    unknown_source_refs += 1
                if op.target_path in target_leaves:
                    _bind(op.target_path)
                else:
                    unknown_target_refs += 1
            elif isinstance(op, MapFunctionApiTransformOperation):
                for inp in op.inputs:
                    if inp not in source_leaves:
                        unknown_source_refs += 1
                if op.target_path in target_leaves:
                    _bind(op.target_path)
                else:
                    unknown_target_refs += 1
            elif isinstance(op, MapScriptApiTransformOperation):
                for inp in op.inputs:
                    if inp not in source_leaves:
                        unknown_source_refs += 1
                for out in op.outputs:
                    if out in target_leaves:
                        _bind(out)
                    else:
                        unknown_target_refs += 1
                # The in-script variable for each path is its sanitized final
                # segment; inputs and outputs share one namespace inside the
                # script (ScriptMappingBuilder), so two paths deriving the same
                # variable name cannot be expressed. Reject here (clear, early)
                # instead of failing deep with SCRIPT_MAPPING_VARIABLE_INVALID.
                script_vars = [_script_var_name(p) for p in op.inputs] + [
                    _script_var_name(p) for p in op.outputs
                ]
                if len(set(script_vars)) != len(script_vars):
                    script_var_collisions += 1

        required_target_paths = _required_simple_leaf_paths(self.target.payload_profile)
        unmapped_required_count = len(required_target_paths - bound_target_paths)

        issues: List[str] = []
        if unknown_source_refs:
            issues.append(
                f"transform.operations contain {unknown_source_refs} reference(s) "
                "to a source path that is not a declared simple leaf in "
                "source.response_profile"
            )
        if unknown_target_refs:
            issues.append(
                f"transform.operations contain {unknown_target_refs} reference(s) "
                "to a target path that is not a declared simple leaf in "
                "target.payload_profile"
            )
        if duplicate_target_bindings:
            issues.append(
                f"transform.operations bind {duplicate_target_bindings} target "
                "leaf path(s) more than once; every leaf may be the destination of "
                "at most one direct/map_function/map_script output"
            )
        if unmapped_required_count:
            issues.append(
                f"transform.operations leave {unmapped_required_count} required "
                "target leaf path(s) unmapped; every required simple leaf in "
                "target.payload_profile must be the destination of at least one "
                "direct/map_function/map_script output"
            )
        if script_var_collisions:
            issues.append(
                f"{script_var_collisions} map_script operation(s) derive two or "
                "more identical in-script variable names from distinct paths (each "
                "variable is a path's sanitized final segment, and inputs/outputs "
                "share one namespace); rename the colliding leaves so every "
                "map_script input/output path yields a unique variable name"
            )

        if issues:
            raise ValueError(" | ".join(issues))

        return self


# ---------------------------------------------------------------------------
# Assembly helpers
# ---------------------------------------------------------------------------


def _map_rest_connection(
    binding: RestConnectionBinding, *, field: str
) -> Dict[str, Any]:
    """Map a contract RestConnectionBinding to a primitive RestConnection dict.

    Mirrors database_to_api_sync._build_rest_send_params: a create binding emits
    an unauthenticated connection (auth 'none' only — secured auth requires
    reuse, rejected with UNSUPPORTED_REST_AUTH_MODE); a reuse binding references
    an existing connection by id or name. The archetype never echoes credentials.
    """
    if binding.mode == "create":
        settings = binding.settings  # guaranteed present by the contract validator
        auth = _REST_CREATE_AUTH_MAP.get(settings.auth_mode)
        if auth is None:
            raise BuilderValidationError(
                "REST create-mode auth is not supported for executable assembly "
                "(only an unauthenticated connection can be created); use an "
                "existing connection instead.",
                error_code=UNSUPPORTED_REST_AUTH_MODE,
                field=f"{field}.binding.settings.auth_mode",
                hint=(
                    f"Set {field}.binding.mode='reuse' with an existing REST "
                    "Client connection (component_id or component_name) for "
                    "secured auth. The archetype never echoes credentials."
                ),
            )
        connection: Dict[str, Any] = {
            "mode": "create",
            "base_url": settings.base_url,
            "auth": auth,
        }
        # NOTE: settings.default_headers is intentionally NOT mapped here —
        # RestConnectionCreate has no such field. The fetch/send param builders
        # instead apply create-mode default_headers as operation-level
        # request_headers (so the caller's headers are honored, not dropped).
        return connection

    connection = {"mode": "reuse"}
    if binding.component_id:
        connection["component_id"] = binding.component_id
    if binding.component_name:
        connection["component_name"] = binding.component_name
    return connection


def _create_default_headers(binding: RestConnectionBinding) -> Dict[str, str]:
    """Return create-mode connection ``default_headers`` (empty for reuse).

    RestConnectionCreate carries no default_headers field, so these are applied as
    operation-level request_headers instead of being silently dropped. Reuse-mode
    bindings have no settings, so they contribute nothing here (an existing
    connection already carries its own configured headers).
    """
    if binding.mode == "create" and binding.settings is not None:
        return dict(binding.settings.default_headers or {})
    return {}


def _reject_case_variant_headers(headers: Dict[str, str], *, field: str) -> None:
    """Reject two keys in one header dict that differ only in letter case.

    HTTP header names are case-insensitive (RFC 7230); a single caller dict
    carrying both ``Accept`` and ``accept`` would otherwise emit two entries for
    one logical header (issue #127 A1). The cross-dict operation-wins dedupe in
    ``_merge_request_headers`` only resolves conflicts *between* the default and
    operation dicts, not *within* one. Raising (rather than silently keeping the
    last spelling, which depends on dict insertion order) surfaces the authoring
    error as a structured ``ARCHETYPE_PARAM_INVALID``. The offending key name is
    NOT echoed (defense-in-depth).
    """
    seen: Set[str] = set()
    for name in headers:
        lower = name.lower()
        if lower in seen:
            raise BuilderValidationError(
                "request headers contain two entries that differ only in "
                "letter case; HTTP header names are case-insensitive, so "
                "declare each header once.",
                error_code="ARCHETYPE_PARAM_INVALID",
                field=field,
                hint="Merge the case-variant header keys into a single entry.",
            )
        seen.add(lower)


def _merge_request_headers(
    default_headers: Dict[str, str],
    operation_headers: Optional[Dict[str, str]],
    *,
    default_field: str,
    operation_field: str,
) -> Optional[Dict[str, str]]:
    """Merge connection default_headers with operation headers (operation wins).

    Operation-level headers are more specific than connection defaults, so a
    header set in both resolves to the operation value. HTTP header names are
    case-insensitive (RFC 7230), so the conflict is resolved on the lowercased
    name — an operation ``{"accept": ...}`` overrides a default ``{"Accept": ...}``
    (emitting only the operation header, with its original spelling), rather than
    leaking two case-variant entries for the same header. Each input dict is
    first checked for case-variant duplicates *within* itself (issue #127 A1).
    Returns None when both are empty so the operation config omits
    request_headers entirely.
    """
    _reject_case_variant_headers(default_headers, field=default_field)
    operation_headers = operation_headers or {}
    _reject_case_variant_headers(operation_headers, field=operation_field)
    operation_lower = {name.lower() for name in operation_headers}
    merged = {
        name: value
        for name, value in default_headers.items()
        if name.lower() not in operation_lower
    }
    merged.update(operation_headers)
    return merged or None


def _build_rest_fetch_params(
    parameters: "ApiToApiSyncParameters",
    overrides: Dict[str, str],
    *,
    response_profile_key: str,
    source_field_index: Dict[str, Dict[str, Any]],
) -> RestFetchParameters:
    source = parameters.source
    fetch = source.fetch_request

    operation: Dict[str, Any] = {"path": fetch.path}
    # Apply create-mode connection default_headers as operation request_headers
    # (operation headers win on key conflict) so they are honored, not dropped.
    request_headers = _merge_request_headers(
        _create_default_headers(source.binding),
        fetch.request_headers,
        default_field="source.binding.settings.default_headers",
        operation_field="source.fetch_request.request_headers",
    )
    if request_headers is not None:
        operation["request_headers"] = request_headers
    for attr in (
        "query_parameters",
        "follow_redirects",
        "return_application_errors",
        "track_response",
    ):
        value = getattr(fetch, attr)
        if value is not None:
            operation[attr] = value

    component_names: Dict[str, str] = {}
    conn_name = _named(
        overrides,
        ROLE_REST_SOURCE_CONNECTION,
        primitive_component_key(_SOURCE_PREFIX, ROLE_REST_SOURCE_CONNECTION),
    )
    if conn_name:
        component_names["connection"] = conn_name
    op_name = _named(
        overrides,
        ROLE_REST_SOURCE_OPERATION,
        primitive_component_key(_SOURCE_PREFIX, ROLE_REST_SOURCE_OPERATION),
    )
    if op_name:
        component_names["operation"] = op_name

    return _coerce_primitive_params(
        RestFetchParameters,
        {
            "key_prefix": _SOURCE_PREFIX,
            "connection": _map_rest_connection(source.binding, field="source"),
            "operation": operation,
            "response": {
                "profile_id": f"$ref:{response_profile_key}",
                "profile_type": "profile.json",
                "field_index": source_field_index,
            },
            "component_names": component_names,
        },
        field="source",
    )


def _build_field_map_params(
    parameters: "ApiToApiSyncParameters",
    overrides: Dict[str, str],
    *,
    response_profile_key: str,
    source_field_index: Dict[str, Dict[str, Any]],
) -> FieldMapParameters:
    transform = parameters.transform

    direct: List[Dict[str, Any]] = []
    map_function: List[Dict[str, Any]] = []
    map_script: List[Dict[str, Any]] = []
    for op in transform.operations:
        if isinstance(op, DirectApiTransformOperation):
            direct.append({"source_field": op.source_path, "target_path": op.target_path})
        elif isinstance(op, MapFunctionApiTransformOperation):
            entry: Dict[str, Any] = {
                "function_type": op.function_type,
                "inputs": list(op.inputs),
                "target_path": op.target_path,
            }
            if op.parameters:
                entry["parameters"] = dict(op.parameters)
            map_function.append(entry)
        elif isinstance(op, MapScriptApiTransformOperation):
            # External script-component reuse would plan with a dangling
            # dependency (the component is not in the emitted spec). Reject it
            # with a clear error instead of an unplannable "executable" spec.
            if op.script_component_ref is not None:
                raise BuilderValidationError(
                    "map_script.script_component_ref is not supported by this "
                    "archetype — the referenced script component is not part of "
                    "the emitted spec, so the plan cannot resolve it.",
                    error_code=UNSUPPORTED_SCRIPT_COMPONENT_REF,
                    field="transform.operations.script_component_ref",
                    hint=(
                        "Provide the script inline via map_script.script_body so "
                        "the archetype materializes the script.mapping component "
                        "in the spec. External script-component reuse is deferred "
                        "to #51."
                    ),
                )
            # The contract's inputs/outputs are JSON leaf paths; field_map's
            # MapScriptOp needs named ports, so derive a language-safe variable
            # name from each path (sanitized final segment). Uniqueness across the
            # shared input/output namespace was already enforced by the contract
            # validator. field_map enforces that script_body is present.
            script_entry: Dict[str, Any] = {
                "inputs": [
                    {"source_path": path, "input_name": _script_var_name(path)}
                    for path in op.inputs
                ],
                "outputs": [
                    {"output_name": _script_var_name(path), "target_path": path}
                    for path in op.outputs
                ],
                "language": op.language,
            }
            if op.script_body is not None:
                script_entry["script_body"] = op.script_body
            map_script.append(script_entry)

    component_names: Dict[str, str] = {}
    target_profile_name = _named(
        overrides,
        ROLE_TARGET_PROFILE,
        primitive_component_key(_TRANSFORM_PREFIX, ROLE_TARGET_PROFILE),
    )
    if target_profile_name:
        component_names["target_profile"] = target_profile_name
    map_name = _named(
        overrides,
        ROLE_TRANSFORM_MAP,
        primitive_component_key(_TRANSFORM_PREFIX, ROLE_TRANSFORM_MAP),
    )
    if map_name:
        component_names["transform_map"] = map_name
    script_prefix = _named(overrides, ROLE_SCRIPT, f"{_TRANSFORM_PREFIX}_{ROLE_SCRIPT}")
    if script_prefix:
        component_names["script_prefix"] = script_prefix

    return _coerce_primitive_params(
        FieldMapParameters,
        {
            "key_prefix": _TRANSFORM_PREFIX,
            "source": {
                "source_profile_id": f"$ref:{response_profile_key}",
                "source_profile_type": "profile.json",
                "source_field_index": source_field_index,
            },
            "target_payload_profile": parameters.target.payload_profile.model_dump(),
            "direct": direct,
            "map_function": map_function,
            "map_script": map_script,
            "component_names": component_names,
        },
        field="transform",
    )


def _build_rest_send_params(
    parameters: "ApiToApiSyncParameters", overrides: Dict[str, str]
) -> RestSendWithRetryParameters:
    target = parameters.target
    send = target.send_request

    target_profile_key = primitive_component_key(_TRANSFORM_PREFIX, ROLE_TARGET_PROFILE)
    operation: Dict[str, Any] = {
        "method": send.method,
        "path": send.path,
        # Bind the operation request body to the generated JSON payload profile.
        "request_profile_id": f"$ref:{target_profile_key}",
        "request_profile_type": "json",
    }
    if send.query_parameters:
        operation["query_parameters"] = dict(send.query_parameters)
    # Apply create-mode connection default_headers as operation request_headers
    # (operation headers win on key conflict) so they are honored, not dropped.
    request_headers = _merge_request_headers(
        _create_default_headers(target.binding),
        send.request_headers,
        default_field="target.binding.settings.default_headers",
        operation_field="target.send_request.request_headers",
    )
    if request_headers is not None:
        operation["request_headers"] = request_headers

    component_names: Dict[str, str] = {}
    conn_name = _named(
        overrides,
        ROLE_REST_CONNECTION,
        primitive_component_key(_TARGET_PREFIX, ROLE_REST_CONNECTION),
    )
    if conn_name:
        component_names["connection"] = conn_name
    op_name = _named(
        overrides,
        ROLE_REST_OPERATION,
        primitive_component_key(_TARGET_PREFIX, ROLE_REST_OPERATION),
    )
    if op_name:
        component_names["operation"] = op_name

    return _coerce_primitive_params(
        RestSendWithRetryParameters,
        {
            "key_prefix": _TARGET_PREFIX,
            "connection": _map_rest_connection(target.binding, field="target"),
            "operation": operation,
            "component_names": component_names,
        },
        field="target",
    )


def _build_source_response_profile(
    parameters: "ApiToApiSyncParameters",
    overrides: Dict[str, str],
    folder: Optional[str],
):
    """Emit the source response profile component and return (component, field_index).

    rest_fetch only *binds* a response profile, so the preset generates one from
    the caller's source.response_profile tree (analogous to how db_extract emits
    the DB read profile). The same field index feeds both the fetch response
    binding and the field_map source binding.
    """
    naming = parameters.naming
    # Distinct default display name from field_map's "<prefix> Target Profile" so
    # a same-prefix assembly does not trip the COMPONENT_NAME_NOT_UNIQUE lint.
    profile_name = (
        _named(overrides, _ROLE_SOURCE_PROFILE)
        or f"{naming.component_prefix} Source Profile"
    )
    config: Dict[str, Any] = {
        "profile_type": "json.generated",
        "component_name": profile_name,
        "root": parameters.source.response_profile.model_dump()["root"],
    }
    if folder:
        config["folder_path"] = folder
    raise_for_builder_error(JSONGeneratedProfileBuilder.validate_config(config))
    field_index = JSONGeneratedProfileBuilder.build_field_index(config)
    component = IntegrationComponentSpec(
        key=_SOURCE_RESPONSE_PROFILE_KEY,
        type="profile.json",
        action="create",
        name=profile_name,
        config=config,
    )
    return component, field_index


def _build_pipeline_dict(parameters: "ApiToApiSyncParameters") -> Dict[str, Any]:
    """Build the verified-linear fetch -> map -> send sync_pipeline stage graph.

    The $ref tokens nested in stage config are resolved generically by
    build_integration's _resolve_dependency_tokens at apply time; at plan time
    SyncPipelineBuilder lowers this graph and the integration builder runs the
    proven $ref-reachability check on the lowered source/transform/target.
    """
    source_conn_key = primitive_component_key(_SOURCE_PREFIX, ROLE_REST_SOURCE_CONNECTION)
    source_op_key = primitive_component_key(_SOURCE_PREFIX, ROLE_REST_SOURCE_OPERATION)
    map_key = primitive_component_key(_TRANSFORM_PREFIX, ROLE_TRANSFORM_MAP)
    target_conn_key = primitive_component_key(_TARGET_PREFIX, ROLE_REST_CONNECTION)
    target_op_key = primitive_component_key(_TARGET_PREFIX, ROLE_REST_OPERATION)

    return {
        "stages": [
            {
                "key": "fetch",
                "kind": "fetch",
                "config": {
                    "primitive": "rest_fetch",
                    "connection_id": f"$ref:{source_conn_key}",
                    "operation_id": f"$ref:{source_op_key}",
                },
            },
            {
                "key": "map",
                "kind": "map",
                "config": {
                    "primitive": "map",
                    "map_ref": f"$ref:{map_key}",
                },
            },
            {
                "key": "send",
                "kind": "send",
                "config": {
                    "primitive": "rest_send",
                    "action_type": parameters.target.send_request.method,
                    "connection_id": f"$ref:{target_conn_key}",
                    "operation_id": f"$ref:{target_op_key}",
                },
            },
        ],
        "dependencies": [
            {"from_stage": "fetch", "to_stage": "map"},
            {"from_stage": "map", "to_stage": "send"},
        ],
    }


def _build_main_process(
    parameters: "ApiToApiSyncParameters",
    overrides: Dict[str, str],
    pipeline_dict: Dict[str, Any],
) -> IntegrationComponentSpec:
    naming = parameters.naming
    process_name = (
        _named(overrides, "process", _MAIN_PROCESS_KEY)
        or f"{naming.component_prefix} API to API Sync"
    )

    # process_kind="sync_pipeline" with the stage graph INTACT — build_integration
    # routes it through SyncPipelineBuilder (do NOT pre-lower). No reliability /
    # source / target / transform / dynamic_path top-level keys (the sync_pipeline
    # top-level gate rejects them); only folder_name placement is carried.
    config: Dict[str, Any] = {
        "process_kind": "sync_pipeline",
        "pipeline": pipeline_dict,
    }
    if naming.folder_path:
        config["folder_name"] = naming.folder_path

    # depends_on must contain exactly the keys referenced by $ref tokens in the
    # LOWERED process config (the source/target connection+operation and the map).
    # The two profiles are depended transitively by the operation/map components.
    depends_on = [
        primitive_component_key(_SOURCE_PREFIX, ROLE_REST_SOURCE_CONNECTION),
        primitive_component_key(_SOURCE_PREFIX, ROLE_REST_SOURCE_OPERATION),
        primitive_component_key(_TRANSFORM_PREFIX, ROLE_TRANSFORM_MAP),
        primitive_component_key(_TARGET_PREFIX, ROLE_REST_CONNECTION),
        primitive_component_key(_TARGET_PREFIX, ROLE_REST_OPERATION),
    ]

    return IntegrationComponentSpec(
        key=_MAIN_PROCESS_KEY,
        type="process",
        action="create",
        name=process_name,
        config=config,
        depends_on=depends_on,
    )


def _operation_summaries(
    parameters: "ApiToApiSyncParameters",
) -> List[Dict[str, Any]]:
    summaries: List[Dict[str, Any]] = []
    for op in parameters.transform.operations:
        if isinstance(op, DirectApiTransformOperation):
            summary: Dict[str, Any] = {
                "operation_type": "direct",
                "future_builder_issue": "#26",
                # Surface BOTH the API-oriented source_path AND the field_map
                # source_field alias so a downstream consumer can match either.
                "source_path": op.source_path,
                "source_field": op.source_path,
                "target_path": op.target_path,
            }
            if op.documentation_hint is not None:
                summary["documentation_hint"] = op.documentation_hint
            summaries.append(summary)
        elif isinstance(op, MapFunctionApiTransformOperation):
            summary = {
                "operation_type": "map_function",
                "future_builder_issue": "#40",
                "function_type": op.function_type,
                "inputs": list(op.inputs),
                "input_count": len(op.inputs),
                "target_path": op.target_path,
            }
            if op.parameters is not None:
                summary["parameters"] = dict(op.parameters)
            if op.documentation_hint is not None:
                summary["documentation_hint"] = op.documentation_hint
            summaries.append(summary)
        elif isinstance(op, MapScriptApiTransformOperation):
            summary = {
                "operation_type": "map_script",
                "future_builder_issue": "#41",
                "script_slot": op.script_slot,
                "language": op.language,
                "inputs": list(op.inputs),
                "input_count": len(op.inputs),
                "outputs": list(op.outputs),
                "output_count": len(op.outputs),
                # The in-script variable names derived from each path (so a caller
                # knows which identifiers to reference in script_body).
                "input_variables": [_script_var_name(p) for p in op.inputs],
                "output_variables": [_script_var_name(p) for p in op.outputs],
                "script_body_present": op.script_body is not None,
            }
            if op.script_body is not None:
                summary["script_body"] = op.script_body
            if op.documentation_hint is not None:
                summary["documentation_hint"] = op.documentation_hint
            summaries.append(summary)
    return summaries


# ---------------------------------------------------------------------------
# Archetype
# ---------------------------------------------------------------------------


class ApiToApiSyncArchetype(ArchetypePattern):
    metadata = PatternMetadata(
        name="api_to_api_sync",
        version="0.1.0",
        kind=PatternKind.ARCHETYPE,
        description=(
            "Archetype for replicating records from a REST API source to a REST "
            "API target as a thin preset over the M5 sync_pipeline. Validates "
            "parameters (caller-supplied JSON source response profile and target "
            "payload profile trees plus typed transform operations) and emits an "
            "executable IntegrationSpecV1 whose main process carries "
            "process_kind='sync_pipeline' with an intact fetch -> map -> send "
            "stage graph (NOT a custom pairwise process builder). Every byte of "
            "XML is produced by the existing component builders through the "
            "rest_fetch / field_map / rest_send primitives; the archetype emits "
            "JSON component specs only and never calls Boomi. Static REST only — "
            "runtime-bound path/query/header (#96), pagination, retry/DLQ, "
            "watermark, and schedule activation are out of scope for this preset."
        ),
        tags=[
            "api",
            "rest",
            "sync",
            "m5",
            "sync-pipeline",
            "no-boomi-mutation",
        ],
        use_cases=[
            "replicate records from a REST API source to a REST API target",
            "stand up a REST fetch -> transform -> REST send pipeline as a preset",
        ],
        not_for=[
            "database sources or targets (use database_to_api_sync)",
            "runtime-bound query/path/header or watermark behavior (#96 M5.4a)",
            "pagination loops, retry/DLQ, or deploying/scheduling the process",
        ],
    )
    parameters_model = ApiToApiSyncParameters

    capability_notes = [
        "Discoverable, fully-typed parameter contract for a REST -> REST sync.",
        "Strict per-field validation surfaces structured PARAM_VALIDATION_FAILED errors.",
        "Emits a main process with process_kind='sync_pipeline' and an intact fetch -> map -> send stage graph; build_integration routes it through the verified-linear SyncPipelineBuilder.",
        "Caller-supplied JSON source response profile and target payload profile trees are the source of truth; the preset generates the source profile and binds it as the fetch output shape.",
        "Emits executable component specs (REST source, JSON transform, REST target, process) for build_integration(action='plan').",
        "All XML is produced by the existing component builders through the shipped rest_fetch / field_map / rest_send primitives; the archetype emits JSON component specs only.",
        "The generated plan is inspectable through the existing MCP planning/review flows (build_integration plan, review_transformation, plan_integration_design).",
        "Sets spec.pipeline to the same semantic stage graph so the plan is inspectable as a pipeline.",
        "Credentials cross the contract only as opaque credential_ref values and are never echoed in errors.",
    ]
    limitations = [
        "Emits JSON component specs only; performs no Boomi mutation and exposes no raw XML or payload/body templates.",
        "REST source is GET-only; REST target carries the configured HTTP method.",
        "Static REST only: a '{token}' dynamic path is rejected (runtime path binding is #96 M5.4a); query parameters and headers are static.",
        "Pagination, conditional requests, retry/DLQ, watermark, and schedule activation are out of scope for this preset.",
        "REST create-mode emits only auth='none'; secured auth (basic / bearer / oauth2) requires binding.mode='reuse'.",
        "Create-mode connection default_headers are applied as operation-level request headers (operation-specific headers win on conflict); a reuse-mode connection carries its own configured headers.",
        "map_script materializes only an inline script_body; external script_component_ref reuse is rejected (#51). Each in-script variable is a path's sanitized final segment; two map_script paths that derive the same variable name are rejected (rename the colliding leaves).",
        "Does not mix map_function and map_script in one call (UNSUPPORTED_TRANSFORM_ROUTE); split into separate maps.",
        "operation_type='xslt' is rejected; the XSLT decision is owned by issue #42.",
        "credential_ref values are opaque end-to-end; the contract never resolves or validates secrets.",
    ]
    examples = [
        PatternExample(
            name="minimal_rest_to_rest_sync",
            description=(
                "Smallest valid payload: create-mode REST source with no auth and "
                "a one-leaf JSON response profile, create-mode REST target with no "
                "auth and a one-leaf JSON payload profile, a single direct "
                "transform operation. Placeholder sentinels only — not a reusable "
                "template."
            ),
            parameters={
                "naming": {
                    "integration_name": "demo-api-to-api-sync",
                    "component_prefix": "DEMO",
                },
                "source": {
                    "binding": {
                        "mode": "create",
                        "settings": {
                            "base_url": "https://source.example.com",
                            "auth_mode": "none",
                        },
                    },
                    "fetch_request": {
                        "path": _EXAMPLE_PATH_SENTINEL,
                    },
                    "response_profile": {
                        "format": "json",
                        "root": {
                            "name": "Root",
                            "kind": "object",
                            "children": [
                                {
                                    "name": "source_a",
                                    "kind": "simple",
                                    "data_type": "character",
                                },
                            ],
                        },
                    },
                },
                "target": {
                    "binding": {
                        "mode": "create",
                        "settings": {
                            "base_url": "https://target.example.com",
                            "auth_mode": "none",
                        },
                    },
                    "send_request": {
                        "method": "POST",
                        "path": "/v1/<<target resource>>",
                    },
                    "payload_profile": {
                        "format": "json",
                        "root": {
                            "name": "Root",
                            "kind": "object",
                            "children": [
                                {
                                    "name": "target_a",
                                    "kind": "simple",
                                    "data_type": "character",
                                },
                            ],
                        },
                    },
                },
                "transform": {
                    "operations": [
                        {
                            "operation_type": "direct",
                            "source_path": "Root/source_a",
                            "target_path": "Root/target_a",
                        },
                    ],
                },
            },
        ),
        PatternExample(
            name="reuse_connections_with_function",
            description=(
                "Fuller payload: reuse-mode REST source and target by component id "
                "(secured auth uses connection reuse) with nested JSON profiles "
                "and two transform operations (one direct, one map_function). "
                "Examples deliberately exclude map_script declarations to keep the "
                "published payload free of language tokens."
            ),
            parameters={
                "naming": {
                    "integration_name": "demo-api-to-api-enriched",
                    "component_prefix": "DEMO-ENR",
                    "folder_path": "Integrations/API/Sync",
                    "runtime_hints": {"atom_pool": "primary"},
                },
                "source": {
                    "binding": {
                        "mode": "reuse",
                        "component_id": "<<existing REST source connection id>>",
                    },
                    "fetch_request": {
                        "path": "/v1/<<source resource>>",
                        "query_parameters": {"limit": "100"},
                    },
                    "response_profile": {
                        "format": "json",
                        "root": {
                            "name": "Root",
                            "kind": "object",
                            "children": [
                                {
                                    "name": "id",
                                    "kind": "simple",
                                    "data_type": "character",
                                    "required": True,
                                },
                                {
                                    "name": "updated_at",
                                    "kind": "simple",
                                    "data_type": "datetime",
                                },
                            ],
                        },
                    },
                },
                "target": {
                    "binding": {
                        "mode": "reuse",
                        "component_id": "<<existing REST target connection id>>",
                    },
                    "send_request": {
                        "method": "PUT",
                        "path": "/v1/<<target resource>>",
                    },
                    "payload_profile": {
                        "format": "json",
                        "root": {
                            "name": "Root",
                            "kind": "object",
                            "children": [
                                {
                                    "name": "external_id",
                                    "kind": "simple",
                                    "data_type": "character",
                                    "required": True,
                                },
                                {
                                    "name": "modified",
                                    "kind": "simple",
                                    "data_type": "datetime",
                                },
                            ],
                        },
                    },
                },
                "transform": {
                    "operations": [
                        {
                            "operation_type": "direct",
                            "source_path": "Root/id",
                            "target_path": "Root/external_id",
                            "documentation_hint": "carry the source id verbatim",
                        },
                        {
                            "operation_type": "map_function",
                            "function_type": "date_format",
                            "inputs": ["Root/updated_at"],
                            "target_path": "Root/modified",
                            "parameters": {
                                "input_format": "<<source datetime format>>",
                                "output_format": "<<target datetime format>>",
                            },
                        },
                    ],
                },
            },
        ),
    ]

    @classmethod
    def emit_spec(cls, parameters: ApiToApiSyncParameters) -> IntegrationSpecV1:
        naming = parameters.naming
        source_binding = parameters.source.binding
        target_binding = parameters.target.binding
        source_fetch = parameters.source.fetch_request
        target_send = parameters.target.send_request
        source_profile = parameters.source.response_profile
        payload_profile = parameters.target.payload_profile

        overrides = _component_names(naming)
        context = PrimitiveBuildContext(
            integration_name=naming.integration_name,
            component_prefix=naming.component_prefix,
            folder_path=naming.folder_path,
        )

        # ---- Endpoint summaries — no resolved URLs, no payload bodies ----
        source_endpoint: Dict[str, Any] = {
            "key": "rest_source",
            "type": "rest",
            "direction": "source",
            "binding_mode": source_binding.mode,
            "method": "GET",
            "executable": False,
        }
        if source_binding.mode == "create" and source_binding.settings is not None:
            source_endpoint["auth_mode"] = source_binding.settings.auth_mode
        else:
            if source_binding.component_id:
                source_endpoint["component_id"] = source_binding.component_id
            if source_binding.component_name:
                source_endpoint["component_name"] = source_binding.component_name

        target_endpoint: Dict[str, Any] = {
            "key": "rest_target",
            "type": "rest",
            "direction": "target",
            "binding_mode": target_binding.mode,
            "method": target_send.method,
            "expected_status_codes": list(target_send.expected_status_codes),
            "executable": False,
        }
        if target_binding.mode == "create" and target_binding.settings is not None:
            target_endpoint["auth_mode"] = target_binding.settings.auth_mode
        else:
            if target_binding.component_id:
                target_endpoint["component_id"] = target_binding.component_id
            if target_binding.component_name:
                target_endpoint["component_name"] = target_binding.component_name

        # ---- Profile summaries — leaf path index + data type only ----
        source_leaves = _flatten_payload_profile_leaves(source_profile)
        source_profile_summary: Dict[str, Any] = {
            "format": source_profile.format,
            "root_name": source_profile.root.name,
            "leaf_count": len(source_leaves),
            "leaves": [
                {"path": path, "data_type": data_type}
                for path, data_type in sorted(source_leaves.items())
            ],
        }
        target_leaves = _flatten_payload_profile_leaves(payload_profile)
        target_profile_summary: Dict[str, Any] = {
            "format": payload_profile.format,
            "root_name": payload_profile.root.name,
            "leaf_count": len(target_leaves),
            "leaves": [
                {"path": path, "data_type": data_type}
                for path, data_type in sorted(target_leaves.items())
            ],
        }

        # ---- Transform-review metadata (consumed by review_transformation) ----
        src_gen = profile_from_json_schema(
            source_profile, component_name=f"{naming.component_prefix} Source Profile"
        )
        tgt_gen = profile_from_json_schema(
            payload_profile, component_name=f"{naming.component_prefix} Target Profile"
        )
        direct_field_mappings = validate_field_mappings(
            src_gen["field_index_by_path"],
            tgt_gen["field_index_by_path"],
            [
                {"source_field": op.source_path, "target_path": op.target_path}
                for op in parameters.transform.operations
                if isinstance(op, DirectApiTransformOperation)
            ],
        )

        flows: List[Dict[str, Any]] = [
            {
                "key": "fetch",
                "name": "Fetch from REST source",
                "source": "rest_source",
                "target": None,
                "operation": "rest_fetch",
                "executable": False,
            },
            {
                "key": "transform",
                "name": "Map source response to target payload",
                "source": "fetch",
                "target": None,
                "operation": "transform",
                "executable": False,
                "source_schema": source_profile_summary,
                "target_payload_profile": target_profile_summary,
                "operations": _operation_summaries(parameters),
                "source_profile_generation": src_gen,
                "target_profile_generation": tgt_gen,
                "direct_field_mappings": direct_field_mappings,
            },
            {
                "key": "send",
                "name": "Send to REST target",
                "source": "transform",
                "target": "rest_target",
                "operation": "rest_send",
                "executable": False,
            },
        ]

        naming_block: Dict[str, Any] = {
            "archetype": "api_to_api_sync",
            "integration_name": naming.integration_name,
            "component_prefix": naming.component_prefix,
            "component_names": naming.component_names or {},
        }
        if naming.convention:
            naming_block["convention"] = naming.convention
        folders_block: Dict[str, Any] = (
            {"path": naming.folder_path} if naming.folder_path else {}
        )
        runtime_block: Dict[str, Any] = dict(naming.runtime_hints or {})

        # ---- Executable component assembly ----
        # rest_fetch only BINDS a response profile, so emit the source profile
        # first and feed its field index to both the fetch and the field map.
        components: List[IntegrationComponentSpec] = []
        source_profile_component, source_field_index = _build_source_response_profile(
            parameters, overrides, naming.folder_path
        )
        components.append(source_profile_component)
        components.extend(
            RestFetchPrimitive.emit_components(
                context,
                _build_rest_fetch_params(
                    parameters,
                    overrides,
                    response_profile_key=_SOURCE_RESPONSE_PROFILE_KEY,
                    source_field_index=source_field_index,
                ),
            )
        )
        components.extend(
            FieldMapPrimitive.emit_components(
                context,
                _build_field_map_params(
                    parameters,
                    overrides,
                    response_profile_key=_SOURCE_RESPONSE_PROFILE_KEY,
                    source_field_index=source_field_index,
                ),
            )
        )
        components.extend(
            RestSendWithRetryPrimitive.emit_components(
                context, _build_rest_send_params(parameters, overrides)
            )
        )
        pipeline_dict = _build_pipeline_dict(parameters)
        components.append(_build_main_process(parameters, overrides, pipeline_dict))

        return IntegrationSpecV1(
            version="1.0",
            name=naming.integration_name,
            mode="redesign",
            components=components,
            goals=[
                "Replicate records from a REST API source to a REST API target "
                "via a static fetch -> transform -> send pipeline.",
                "Emit executable component specs whose main process is a "
                "process_kind='sync_pipeline' stage graph (fetch -> map -> send) "
                "for build_integration(action='plan'); the plan is inspectable "
                "through the existing MCP planning/review flows. Deployment, "
                "schedule activation, pagination, retry/DLQ, and runtime-bound "
                "behavior remain out of scope.",
            ],
            endpoints=[source_endpoint, target_endpoint],
            flows=flows,
            naming=naming_block,
            folders=folders_block,
            runtime=runtime_block,
            # Expose the same semantic stage graph for plan inspectability (the
            # field is inert for the builder — the main process config drives XML).
            pipeline=PipelineSpec(**pipeline_dict),
            validation_rules={
                "contract_only": False,
                "component_count": len(components),
                "raw_xml_exposed": False,
                "boomi_mutation": False,
                "metadata_version": "0.1.0",
                "process_kind": "sync_pipeline",
                "transform_review": {
                    "supported_actions": [
                        "list_fields",
                        "validate_unmapped",
                        "mapping_diff",
                        "generate_test_payload",
                        "compare_expected_actual",
                    ],
                    "recommended_before_apply": [
                        "validate_unmapped",
                        "generate_test_payload",
                    ],
                },
                "limitations": {
                    "rest_source": "GET-only static fetch; runtime-bound path/query/header is #96 (M5.4a)",
                    "rest_create_auth": "auth='none' only; secured auth requires reuse",
                    "pagination": "out of scope for this preset (#72 records pagination metadata only)",
                    "reliability": "retry/DLQ not emitted (sync_pipeline is verified-linear, M5.2)",
                    "watermark": "out of scope for this preset",
                    "schedule_activation": "M3 (deploy to a runtime first)",
                    "map_script": "inline script_body only; external script_component_ref rejected (#51)",
                },
                "profile_schema_strategy": (
                    "M5.7 uses a caller-supplied JSON response profile for the REST "
                    "source and a caller-supplied JSON payload profile for the REST "
                    "target; no API browse, schema introspection, or response "
                    "sampling is performed. The source profile is generated and "
                    "bound as the fetch output shape; metadata/sample inference is "
                    "available separately via infer_profile_fields (issue #47)."
                ),
                "transform_routes": {
                    "direct": "#26",
                    "map_function": "#40",
                    "map_script": "#41",
                    "xslt": "#42 (rejected)",
                },
            },
        )
