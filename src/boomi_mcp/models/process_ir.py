"""M12.1 (issue #136, epic #134): strict ``ProcessIRV1`` semantic process models.

Promotion of the frozen ``flow_sequence`` vocabulary (ADR-001 §4 "semantic seed",
inventory §1.4) into a strict, versioned, discriminated Pydantic model family —
NOT a parallel DSL (ADR-001 §12). The models ship DARK: nothing at runtime
constructs or consumes them yet; no MCP surface, compiler, CFG, emitter, or XML
behavior changes in #136.

Contract highlights (ADR-001 §6/§7/§9/§11):

- ``ProcessIRV1(version="1", body=SequenceNodeV1(...))`` is the semantic root;
  every authored boundary is ``extra="forbid"``.
- Callers author exactly two things: semantic nodes and opaque component
  references (``$ref:KEY`` tokens or literal component ids). No connector
  family/action metadata, CFG edges, shape/layout ids, XML, or secrets.
- Diagnostics carry a stable ``PROCESS_IR_*`` code (the shared
  ``boomi_mcp.errors`` registry), an RFC 6901 JSON pointer into the authored
  payload, and static remediation text — never authored values, never raw
  Pydantic internals.
- Serialization is canonical (defaults expanded, keys sorted, compact
  separators) so golden JSON/schema tests are byte-stable.

Structural rules encoded here are the LOCAL rules the legacy builder enforces
per steps-list (ordering, terminal position, the Add-to-Cache consume guard,
branch leg bounds). CFG-aware semantics (reachability, lineage) stay with
#137/#143 per ADR-001 §3.
"""

from __future__ import annotations

import json
from types import MappingProxyType
from typing import Any, List, Literal, Mapping, Optional, Tuple, Union

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    ValidationError,
    model_validator,
)
from pydantic_core import PydanticCustomError
from typing_extensions import Annotated

from ..errors import (
    PROCESS_IR_CAPABILITY_UNSUPPORTED,
    PROCESS_IR_REFERENCE_INVALID_FORMAT,
    PROCESS_IR_SCHEMA_INVALID,
    PROCESS_IR_SCHEMA_INVALID_CARDINALITY,
    PROCESS_IR_SCHEMA_UNKNOWN_FIELD,
    PROCESS_IR_SCHEMA_UNKNOWN_NODE,
    PROCESS_IR_SCHEMA_VERSION_UNSUPPORTED,
)

PROCESS_IR_VERSION = "1"

# ---------------------------------------------------------------------------
# Secret boundary (ADR-001 §11)
# ---------------------------------------------------------------------------

# Mirror of ProcessFlowBuilder.FORBIDDEN_SECRET_FIELDS — a COPY, not an import:
# models must not import from categories.* (builders import models; the reverse
# would cycle). tests/test_process_ir_models.py pins tuple equality with the
# builder's list so the two cannot drift silently.
_FORBIDDEN_SECRET_KEY_SUBSTRINGS: Tuple[str, ...] = (
    "password",
    "passcode",
    "secret",
    "private_key",
    "api_key",
    "apikey",
    "api-key",
    "auth_token",
    "access_token",
    "client_secret",
    "token",
    "authorization",
    "bearer",
    "credentials",
)


def _find_secret_shaped_key(payload: Any, _path: Tuple[Any, ...] = ()) -> Optional[Tuple[Any, ...]]:
    """Return the JSON path of the first secret-shaped key, or None.

    Same semantics as ProcessFlowBuilder.scan_forbidden_secret_fields:
    case-insensitive substring match on dict keys; a match flags a non-empty
    string value or any dict/list container value; empty strings and bare
    scalars (None/bool/int) are skipped; non-matching subtrees are recursed.
    """
    if isinstance(payload, dict):
        for key, value in payload.items():
            if isinstance(key, str):
                lowered = key.lower()
                if any(sub in lowered for sub in _FORBIDDEN_SECRET_KEY_SUBSTRINGS):
                    if isinstance(value, str):
                        if value:
                            return _path + (key,)
                    elif isinstance(value, (dict, list)):
                        return _path + (key,)
                    continue
            found = _find_secret_shaped_key(value, _path + (key,))
            if found is not None:
                return found
    elif isinstance(payload, list):
        for i, item in enumerate(payload):
            found = _find_secret_shaped_key(item, _path + (i,))
            if found is not None:
                return found
    return None


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------


class ProcessIRDiagnostic(BaseModel):
    """One deterministic parse/validation diagnostic (ADR-001 §7).

    ``path`` is an RFC 6901 JSON pointer into the AUTHORED payload; ``message``
    and ``remediation`` are static strings — authored values never appear.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    code: str
    path: str
    message: str
    remediation: str


class ProcessIRValidationError(Exception):
    """Raised by :func:`parse_process_ir_v1` with sorted, secret-free diagnostics."""

    def __init__(self, diagnostics: List[ProcessIRDiagnostic]) -> None:
        self.diagnostics: Tuple[ProcessIRDiagnostic, ...] = tuple(
            sorted(diagnostics, key=lambda d: (d.path, d.code))
        )
        summary = "; ".join(
            f"{d.code} at {d.path or '<root>'}" for d in self.diagnostics
        )
        super().__init__(f"ProcessIRV1 validation failed: {summary}")


def _pointer_escape(token: str) -> str:
    return token.replace("~", "~0").replace("/", "~1")


def _json_pointer(parts: Tuple[Any, ...]) -> str:
    return "".join(f"/{_pointer_escape(str(part))}" for part in parts)


# ---------------------------------------------------------------------------
# Base model + opaque references
# ---------------------------------------------------------------------------

# Fields safe to expose in reprs: discriminators and the version tag only.
_REPR_SAFE_FIELDS = frozenset({"kind", "version", "value_type", "operation"})


class _ProcessIRBase(BaseModel):
    """Shared strict base: unknown fields rejected, authored values repr-suppressed."""

    model_config = ConfigDict(extra="forbid")

    def __repr_args__(self):  # noqa: D105 — pydantic hook
        for key, value in super().__repr_args__():
            if key in _REPR_SAFE_FIELDS:
                yield key, value
            else:
                yield key, "..."


_REF_TOKEN_PREFIX = "$ref:"


def _validate_component_ref(value: str) -> str:
    """Opaque component reference: exact ``$ref:KEY`` token or literal id.

    Mirrors the legacy exactness rules (byte-0 prefix, no surrounding
    whitespace, non-empty whitespace-free key — process_flow_builder
    ``_validate_ref_reachability`` / ``_validate_processcall_entry``). A
    literal id is any other non-blank string with no surrounding whitespace.
    """
    if value != value.strip() or not value:
        raise PydanticCustomError(
            "process_ir_reference_invalid_format",
            "component reference must be a non-blank string without surrounding whitespace",
        )
    if value.startswith(_REF_TOKEN_PREFIX):
        key = value[len(_REF_TOKEN_PREFIX):]
        if not key or any(ch.isspace() for ch in key):
            raise PydanticCustomError(
                "process_ir_reference_invalid_format",
                "'$ref:' token must carry a non-empty, whitespace-free key",
            )
    return value


ComponentRefV1 = Annotated[
    str,
    AfterValidator(_validate_component_ref),
    Field(description="Opaque component reference: exact '$ref:KEY' token or literal component id"),
]


def _cardinality_error(message: str) -> PydanticCustomError:
    return PydanticCustomError("process_ir_schema_invalid_cardinality", message)  # noqa: EM101


def _capability_error(message: str) -> PydanticCustomError:
    return PydanticCustomError("process_ir_capability_unsupported", message)  # noqa: EM101


# ---------------------------------------------------------------------------
# Property sources (Set Properties source_values entries)
# ---------------------------------------------------------------------------


class StaticPropertySourceV1(_ProcessIRBase):
    value_type: Literal["static"] = "static"
    value: str = Field(..., description="Literal value (may be empty)")


class CurrentPropertySourceV1(_ProcessIRBase):
    value_type: Literal["current"] = "current"


class ProfilePropertySourceV1(_ProcessIRBase):
    value_type: Literal["profile"] = "profile"
    element_id: str = Field(..., min_length=1)
    element_name: str = Field(..., min_length=1)
    profile_ref: ComponentRefV1
    profile_type: str = Field(..., min_length=1, description="e.g. profile.json")


class DdpPropertySourceV1(_ProcessIRBase):
    value_type: Literal["ddp"] = "ddp"
    property_name: str = Field(..., min_length=1)
    default_value: Optional[str] = None


class DppPropertySourceV1(_ProcessIRBase):
    value_type: Literal["dpp"] = "dpp"
    property_name: str = Field(..., min_length=1)
    default_value: Optional[str] = None


# 'definedparameter' is deliberately absent: it is capability-gated (no
# verified wire shape — #119 census Outcome B). The parse translator maps the
# tag to PROCESS_IR_CAPABILITY_UNSUPPORTED instead of a generic unknown-node.
PropertySourceV1 = Annotated[
    Union[
        StaticPropertySourceV1,
        CurrentPropertySourceV1,
        ProfilePropertySourceV1,
        DdpPropertySourceV1,
        DppPropertySourceV1,
    ],
    Field(discriminator="value_type"),
]


# ---------------------------------------------------------------------------
# Data Process operations
# ---------------------------------------------------------------------------


class CustomScriptingOpV1(_ProcessIRBase):
    operation: Literal["custom_scripting"] = "custom_scripting"
    script: str = Field(..., min_length=1)
    language: Literal["groovy2"] = "groovy2"
    use_cache: Literal[True] = True

    @model_validator(mode="after")
    def _script_non_blank(self) -> "CustomScriptingOpV1":
        if not self.script.strip():
            raise _cardinality_error("script must be a non-blank string")
        return self


class SplitDocumentsOpV1(_ProcessIRBase):
    operation: Literal["split_documents"] = "split_documents"
    profile_type: Literal["json", "xml"]
    profile_ref: ComponentRefV1
    link_element_key: str = Field(..., min_length=1)
    link_element_name: str = Field(..., min_length=1)


class CombineDocumentsOpV1(_ProcessIRBase):
    operation: Literal["combine_documents"] = "combine_documents"
    profile_type: Literal["json", "xml"]
    profile_ref: ComponentRefV1
    link_element_key: str = Field(..., min_length=1)
    link_element_name: str = Field(..., min_length=1)
    combine_into_link_element_key: str = Field(
        default="null", min_length=1, description="'null' combines into the document root"
    )


DataProcessOperationV1 = Annotated[
    Union[CustomScriptingOpV1, SplitDocumentsOpV1, CombineDocumentsOpV1],
    Field(discriminator="operation"),
]


# ---------------------------------------------------------------------------
# Decision operands
# ---------------------------------------------------------------------------


class TrackOperandV1(_ProcessIRBase):
    value_type: Literal["track"] = "track"
    property_id: str = Field(..., min_length=1)
    property_name: Optional[str] = None
    default_value: Optional[str] = None

    @model_validator(mode="after")
    def _property_id_non_blank(self) -> "TrackOperandV1":
        if not self.property_id.strip():
            raise _cardinality_error("property_id must be a non-blank string")
        return self


class StaticOperandV1(_ProcessIRBase):
    value_type: Literal["static"] = "static"
    static_value: str = Field(..., description="Literal comparison value (may be empty)")


DecisionOperandV1 = Annotated[
    Union[TrackOperandV1, StaticOperandV1],
    Field(discriminator="value_type"),
]


# ---------------------------------------------------------------------------
# Endpoint + linear nodes
# ---------------------------------------------------------------------------


class SourceEndpointV1(_ProcessIRBase):
    """Current-parity source placeholder. Connector family/action metadata is
    NEVER authored — the compiler derives it from the component symbol table."""

    kind: Literal["source"] = "source"
    connection_ref: ComponentRefV1
    operation_ref: ComponentRefV1
    label: Optional[str] = None


class TargetEndpointV1(_ProcessIRBase):
    """Current-parity target placeholder (see SourceEndpointV1)."""

    kind: Literal["target"] = "target"
    connection_ref: ComponentRefV1
    operation_ref: ComponentRefV1
    label: Optional[str] = None


class FlowControlNodeV1(_ProcessIRBase):
    kind: Literal["flow_control"] = "flow_control"
    for_each_count: StrictInt = Field(..., gt=0)
    label: Optional[str] = None


class MessageNodeV1(_ProcessIRBase):
    kind: Literal["message"] = "message"
    text: str = Field(..., min_length=1)
    label: Optional[str] = None


class MapRefNodeV1(_ProcessIRBase):
    kind: Literal["map_ref"] = "map_ref"
    map_ref: ComponentRefV1
    label: Optional[str] = None


class DataProcessNodeV1(_ProcessIRBase):
    kind: Literal["data_process"] = "data_process"
    steps: List[DataProcessOperationV1] = Field(..., min_length=1)
    label: Optional[str] = None


class CachePutNodeV1(_ProcessIRBase):
    """Add to Cache write. CONSUMES the document stream — the containing
    sequence rules require a stream-replacing cache read right after it."""

    kind: Literal["cache_put"] = "cache_put"
    cache_ref: ComponentRefV1
    label: Optional[str] = None


class DocumentCacheRetrieveNodeV1(_ProcessIRBase):
    """Legacy all-document Document Cache Retrieve (M10.5 parity node)."""

    kind: Literal["document_cache_retrieve"] = "document_cache_retrieve"
    cache_ref: ComponentRefV1
    empty_cache_behavior: Literal["stopprocess"] = "stopprocess"
    load_all_documents: Literal[True] = True
    label: Optional[str] = None


class CacheGetNodeV1(_ProcessIRBase):
    """Authored all-document cache read; ``external_writer`` carries the
    authored lineage assertion (cache populated outside this process)."""

    kind: Literal["cache_get"] = "cache_get"
    cache_ref: ComponentRefV1
    empty_cache_behavior: Literal["stopprocess"] = "stopprocess"
    external_writer: StrictBool = False
    label: Optional[str] = None


class CacheRemoveNodeV1(_ProcessIRBase):
    kind: Literal["cache_remove"] = "cache_remove"
    cache_ref: ComponentRefV1
    remove_all_documents: Literal[True] = True
    label: Optional[str] = None


_PROPERTY_NAME_FORBIDDEN_PREFIXES = (
    "dynamicdocument.",
    "process.",
    "document.dynamic.userdefined.",
)


def _validate_bare_property_name(name: str) -> None:
    stripped = name.strip()
    if not stripped:
        raise _cardinality_error("property name must be a non-blank string")
    for prefix in _PROPERTY_NAME_FORBIDDEN_PREFIXES:
        if stripped.startswith(prefix):
            raise _capability_error(
                "property name must be bare — the emitter owns the wire prefix"
            )
    if any(ch.isspace() for ch in stripped):
        raise _cardinality_error("property name must not contain whitespace")


class SetDdpNodeV1(_ProcessIRBase):
    kind: Literal["set_ddp"] = "set_ddp"
    name: str = Field(..., min_length=1, description="Bare property name (no wire prefix)")
    source_values: List[PropertySourceV1] = Field(..., min_length=1)
    label: Optional[str] = None

    @model_validator(mode="after")
    def _name_rules(self) -> "SetDdpNodeV1":
        _validate_bare_property_name(self.name)
        return self


class SetDppNodeV1(_ProcessIRBase):
    kind: Literal["set_dpp"] = "set_dpp"
    name: str = Field(..., min_length=1, description="Bare property name (no wire prefix)")
    source_values: List[PropertySourceV1] = Field(..., min_length=1)
    persist: StrictBool = False
    label: Optional[str] = None

    @model_validator(mode="after")
    def _name_rules(self) -> "SetDppNodeV1":
        _validate_bare_property_name(self.name)
        return self


class ProcessCallNodeV1(_ProcessIRBase):
    """Standalone Process Call (wrapper parity). ``process_ref`` accepts a
    '$ref:KEY' in-spec child token or a literal existing component id."""

    kind: Literal["process_call"] = "process_call"
    process_ref: ComponentRefV1
    wait: StrictBool = True
    abort_on_error: StrictBool = False
    label: Optional[str] = None


# ---------------------------------------------------------------------------
# Terminal + control nodes
# ---------------------------------------------------------------------------


class StopNodeV1(_ProcessIRBase):
    """Successful-stop terminal (continue semantics are emitter-owned)."""

    kind: Literal["stop"] = "stop"


class ReturnDocumentsNodeV1(_ProcessIRBase):
    kind: Literal["return_documents"] = "return_documents"
    label: Optional[str] = None


class ExceptionNodeV1(_ProcessIRBase):
    """Terminal Exception throw. No ``label`` — parity with the legacy
    exception step key set (title/message_template/stop_single_document/
    parameter_source only)."""

    kind: Literal["exception"] = "exception"
    message_template: str = Field(..., min_length=1)
    title: Optional[str] = None
    stop_single_document: StrictBool = False
    parameter_source: Literal["caught_error", "current_document", "none"] = "caught_error"

    @model_validator(mode="after")
    def _placeholder_rules(self) -> "ExceptionNodeV1":
        if not self.message_template.strip():
            raise _cardinality_error("message_template must be a non-blank string")
        if self.parameter_source != "none" and "{1}" not in self.message_template:
            raise _cardinality_error(
                "message_template must contain the {1} placeholder when parameter_source binds a value"
            )
        return self


# The linear vocabulary usable inside branch legs and decision arms. NO
# process_call (wrapper-only today) and NO nested control — recursion is
# excluded by schema, not by a runtime check.
LinearNodeV1 = Annotated[
    Union[
        FlowControlNodeV1,
        MessageNodeV1,
        MapRefNodeV1,
        DataProcessNodeV1,
        CachePutNodeV1,
        DocumentCacheRetrieveNodeV1,
        CacheGetNodeV1,
        CacheRemoveNodeV1,
        SetDdpNodeV1,
        SetDppNodeV1,
    ],
    Field(discriminator="kind"),
]

_CACHE_READ_KINDS = ("cache_get", "document_cache_retrieve")


def _check_cache_put_followed_by_read(steps: List[Any], *, context: str) -> None:
    """Add to Cache consumes the stream: a mid-list cache_put must be followed
    by a stream-replacing cache read (legacy consume guard)."""
    for i, step in enumerate(steps[:-1]):
        if getattr(step, "kind", None) == "cache_put":
            if getattr(steps[i + 1], "kind", None) not in _CACHE_READ_KINDS:
                raise _cardinality_error(
                    f"cache_put in {context} must be immediately followed by "
                    "cache_get or document_cache_retrieve (Add to Cache consumes the documents)"
                )


class BranchLegV1(_ProcessIRBase):
    """One Branch leg: linear steps plus a terminal — a target endpoint, or a
    target-less staging cache_put (the live staging pattern)."""

    steps: List[LinearNodeV1] = Field(default_factory=list)
    terminal: Annotated[
        Union[TargetEndpointV1, CachePutNodeV1], Field(discriminator="kind")
    ]

    @model_validator(mode="after")
    def _leg_rules(self) -> "BranchLegV1":
        _check_cache_put_followed_by_read(self.steps, context="branch leg steps")
        if self.steps and self.steps[-1].kind == "cache_put":
            raise _cardinality_error(
                "a trailing cache_put belongs in the leg terminal (target-less staging leg), not in steps"
            )
        return self


class BranchNodeV1(_ProcessIRBase):
    kind: Literal["branch"] = "branch"
    legs: List[BranchLegV1] = Field(..., min_length=2, max_length=25)
    label: Optional[str] = None


class DecisionTrueArmV1(_ProcessIRBase):
    """TRUE (success) arm: linear steps, then target / nested branch / exception."""

    steps: List[LinearNodeV1] = Field(default_factory=list)
    terminal: Annotated[
        Union[TargetEndpointV1, BranchNodeV1, ExceptionNodeV1],
        Field(discriminator="kind"),
    ]

    @model_validator(mode="after")
    def _arm_rules(self) -> "DecisionTrueArmV1":
        _check_cache_put_followed_by_read(self.steps, context="decision true-arm steps")
        if self.steps and self.steps[-1].kind == "cache_put":
            raise _cardinality_error(
                "decision true-arm steps must not end in cache_put — the arm terminal would receive an empty stream"
            )
        return self


class DecisionFalseArmV1(_ProcessIRBase):
    """FALSE (reject) arm: linear steps, then stop / nested branch / exception.
    Legacy parity: the reject path is never a bare Stop, so an empty-steps arm
    with a stop terminal is rejected."""

    steps: List[LinearNodeV1] = Field(default_factory=list)
    terminal: Annotated[
        Union[StopNodeV1, BranchNodeV1, ExceptionNodeV1],
        Field(discriminator="kind"),
    ]

    @model_validator(mode="after")
    def _arm_rules(self) -> "DecisionFalseArmV1":
        _check_cache_put_followed_by_read(self.steps, context="decision false-arm steps")
        if not self.steps and self.terminal.kind == "stop":
            raise _cardinality_error(
                "decision false-arm steps must be non-empty when the terminal is a stop (reject path is never a bare Stop)"
            )
        if (
            self.steps
            and self.steps[-1].kind == "cache_put"
            and self.terminal.kind != "stop"
        ):
            raise _cardinality_error(
                "decision false-arm steps may end in cache_put only when the arm terminal is a stop"
            )
        return self


class DecisionNodeV1(_ProcessIRBase):
    kind: Literal["decision"] = "decision"
    comparison: Literal[
        "equals",
        "greaterthaneq",
        "lessthaneq",
        "greaterthan",
        "lessthan",
        "regex",
        "wildcard",
    ]
    left: DecisionOperandV1
    right: DecisionOperandV1
    true_arm: DecisionTrueArmV1
    false_arm: DecisionFalseArmV1
    label: Optional[str] = None


# ---------------------------------------------------------------------------
# Root sequence
# ---------------------------------------------------------------------------

ProcessNodeV1 = Annotated[
    Union[
        SourceEndpointV1,
        TargetEndpointV1,
        FlowControlNodeV1,
        MessageNodeV1,
        MapRefNodeV1,
        DataProcessNodeV1,
        CachePutNodeV1,
        DocumentCacheRetrieveNodeV1,
        CacheGetNodeV1,
        CacheRemoveNodeV1,
        SetDdpNodeV1,
        SetDppNodeV1,
        ProcessCallNodeV1,
        BranchNodeV1,
        DecisionNodeV1,
        ExceptionNodeV1,
        StopNodeV1,
        ReturnDocumentsNodeV1,
    ],
    Field(discriminator="kind"),
]

_ROOT_CONTROL_TERMINAL_KINDS = frozenset({"branch", "decision", "exception"})
_ROOT_LINEAR_KINDS = frozenset(
    {
        "flow_control",
        "message",
        "map_ref",
        "data_process",
        "cache_put",
        "document_cache_retrieve",
        "cache_get",
        "cache_remove",
        "set_ddp",
        "set_dpp",
    }
)


class SequenceNodeV1(_ProcessIRBase):
    """Ordered root sequence. Local structural rules mirror today's builder:

    - a connector flow starts with ``source`` and ends in exactly one of
      ``target``+``stop``, ``target``+``return_documents``, or a terminal
      control (``branch``/``decision``/``exception``);
    - a process-call flow contains only ``process_call`` steps plus a
      ``stop``/``return_documents`` terminal (mixed connector execution is
      capability-gated);
    - ``cache_put`` must be immediately followed by a stream-replacing cache
      read (never by the target/terminal).
    """

    kind: Literal["sequence"] = "sequence"
    steps: List[ProcessNodeV1] = Field(..., min_length=1)

    @model_validator(mode="after")
    def _sequence_rules(self) -> "SequenceNodeV1":
        kinds = [step.kind for step in self.steps]

        for i, kind in enumerate(kinds):
            if kind == "source" and i != 0:
                raise _cardinality_error("source may appear only as the first step")

        if "process_call" in kinds:
            for i, kind in enumerate(kinds[:-1]):
                if kind != "process_call":
                    raise _capability_error(
                        "a process-call sequence may mix only process_call steps with a "
                        "stop/return_documents terminal (mixed connector execution is gated)"
                    )
            if kinds[-1] not in ("stop", "return_documents"):
                if kinds[-1] != "process_call":
                    raise _capability_error(
                        "a process-call sequence must end in a stop or return_documents terminal"
                    )
                raise _cardinality_error(
                    "a process-call sequence must end in a stop or return_documents terminal"
                )
            return self

        # Connector flow: source first.
        if kinds[0] != "source":
            raise _cardinality_error(
                "a connector-flow sequence must start with the source endpoint"
            )

        body = kinds[1:]
        if not body:
            raise _cardinality_error(
                "a connector-flow sequence needs a terminal after the source"
            )

        # Determine the terminal shape. Legacy parity: the success terminal of
        # a linear connector flow is target+stop, or target+return_documents
        # (Return Documents replaces the Stop, target still emitted before it).
        if body[-1] in ("stop", "return_documents"):
            if len(body) < 2 or body[-2] != "target":
                raise _cardinality_error(
                    f"a {body[-1]} terminal must be immediately preceded by the target endpoint"
                )
            linear = body[:-2]
        elif body[-1] in _ROOT_CONTROL_TERMINAL_KINDS:
            linear = body[:-1]
        elif body[-1] == "target":
            raise _cardinality_error(
                "the target endpoint must be immediately followed by a stop or return_documents terminal"
            )
        else:
            raise _cardinality_error(
                "a connector-flow sequence must end in target+stop, target+return_documents, "
                "or a branch/decision/exception terminal"
            )

        for kind in linear:
            if kind not in _ROOT_LINEAR_KINDS:
                raise _cardinality_error(
                    f"{kind} may appear only in the terminal position of its sequence"
                )

        # The followed-by guard also rejects a cache_put feeding the terminal
        # (target/return_documents/control are not stream-replacing reads).
        _check_cache_put_followed_by_read(self.steps, context="sequence steps")
        return self


class ProcessIRV1(_ProcessIRBase):
    """The semantic root: exactly one per authored process (ADR-001 §3)."""

    version: Literal["1"]
    body: SequenceNodeV1


# ---------------------------------------------------------------------------
# Capability manifest (published, immutable — not an authored field)
# ---------------------------------------------------------------------------

PROCESS_IR_V1_CAPABILITIES: Mapping[str, str] = MappingProxyType(
    {
        "generalized_connector_call": "gated",  # #140
        "mixed_connector_execution": "gated",  # #140
        "continuation_after_branch_or_decision": "gated",  # #141
        "rich_branch_decision_bodies": "gated",  # #141
        "scoped_try_catch": "gated",  # #142
        "keyed_cache": "gated",  # no live-captured wire shape (#119 census)
        "definedparameter_property_source": "gated",  # no verified wire shape
        "joins": "gated",
        "loops": "gated",
        "caller_authored_cfg_edges": "unsupported",
        "xml_or_layout_or_shape_ids": "unsupported",
        "secret_values": "unsupported",
    }
)


# ---------------------------------------------------------------------------
# Parse entry point with deterministic diagnostics
# ---------------------------------------------------------------------------

# Extra keys that map to the NAMED capability gate instead of a generic
# unknown-field (mirrors the legacy cache_get keyed-retrieval gate).
_GATED_EXTRA_KEYS = frozenset({"doc_cache_index", "cache_key_values", "load_all_documents"})
_GATED_UNION_TAGS = frozenset({"definedparameter"})

# Discriminator tag values that pydantic injects into error locs for tagged
# unions; stripped so pointers address the AUTHORED JSON.
_DISCRIMINATOR_TAGS = frozenset(
    {
        "sequence",
        "source",
        "target",
        "flow_control",
        "message",
        "map_ref",
        "data_process",
        "cache_put",
        "document_cache_retrieve",
        "cache_get",
        "cache_remove",
        "set_ddp",
        "set_dpp",
        "process_call",
        "branch",
        "decision",
        "exception",
        "stop",
        "return_documents",
        "static",
        "current",
        "profile",
        "ddp",
        "dpp",
        "track",
        "custom_scripting",
        "split_documents",
        "combine_documents",
    }
)

_REMEDIATION = {
    PROCESS_IR_SCHEMA_UNKNOWN_NODE: (
        "Use one of the documented ProcessIRV1 node kinds / discriminator tags "
        "(see docs/architecture/PROCESS_IR_V1.md)."
    ),
    PROCESS_IR_SCHEMA_UNKNOWN_FIELD: (
        "Remove the unknown field — ProcessIRV1 nodes are strict and reject extras."
    ),
    PROCESS_IR_SCHEMA_INVALID_CARDINALITY: (
        "Fix the list bound or step ordering at the referenced path "
        "(see docs/architecture/PROCESS_IR_V1.md for the sequence rules)."
    ),
    PROCESS_IR_SCHEMA_VERSION_UNSUPPORTED: (
        "Set version to the supported ProcessIR version '1'."
    ),
    PROCESS_IR_SCHEMA_INVALID: (
        "Fix the value type/shape at the referenced path to match the ProcessIRV1 schema."
    ),
    PROCESS_IR_REFERENCE_INVALID_FORMAT: (
        "Use an exact '$ref:KEY' token (non-empty, whitespace-free key) or a literal component id."
    ),
    PROCESS_IR_CAPABILITY_UNSUPPORTED: (
        "The referenced construct is capability-gated or unsupported in ProcessIR v1; "
        "see the PROCESS_IR_V1_CAPABILITIES manifest."
    ),
}

_CUSTOM_ERROR_CODES = {
    "process_ir_reference_invalid_format": PROCESS_IR_REFERENCE_INVALID_FORMAT,
    "process_ir_capability_unsupported": PROCESS_IR_CAPABILITY_UNSUPPORTED,
    "process_ir_schema_invalid_cardinality": PROCESS_IR_SCHEMA_INVALID_CARDINALITY,
}

_MESSAGES = {
    PROCESS_IR_SCHEMA_UNKNOWN_NODE: "unknown node kind or discriminator tag",
    PROCESS_IR_SCHEMA_UNKNOWN_FIELD: "unknown field on a strict ProcessIRV1 node",
    PROCESS_IR_SCHEMA_INVALID_CARDINALITY: "list bound or step-ordering rule violated",
    PROCESS_IR_SCHEMA_VERSION_UNSUPPORTED: "unsupported or missing ProcessIR document version",
    PROCESS_IR_SCHEMA_INVALID: "value does not match the strict ProcessIRV1 schema",
    PROCESS_IR_REFERENCE_INVALID_FORMAT: "malformed opaque component reference",
    PROCESS_IR_CAPABILITY_UNSUPPORTED: "capability-gated or unsupported construct requested",
}


def _diagnostic(code: str, path: Tuple[Any, ...], *, message: Optional[str] = None) -> ProcessIRDiagnostic:
    return ProcessIRDiagnostic(
        code=code,
        path=_json_pointer(path),
        message=message or _MESSAGES[code],
        remediation=_REMEDIATION[code],
    )


# The non-list union fields whose loc is followed by a discriminator tag.
_UNION_FIELD_NAMES = frozenset({"left", "right", "terminal"})


def _loc_to_path(loc: Tuple[Any, ...], *, keep_last: bool = False) -> Tuple[Any, ...]:
    """Strip discriminator-tag loc elements so the pointer matches authored JSON.

    A tag element only ever directly follows a union position — a list index
    (``steps``/``source_values`` items) or a non-list union field (``left``/
    ``right``/``terminal``) — so only those positions are stripped; a FIELD
    that merely shares a tag's name (e.g. ``map_ref.map_ref``) is preserved.
    ``keep_last`` preserves the final element verbatim (the offending key of an
    extra_forbidden error) even when it sits in a strippable position.
    """
    body, tail = (loc[:-1], loc[-1:]) if keep_last and loc else (loc, ())
    kept = []
    for i, part in enumerate(body):
        if (
            isinstance(part, str)
            and part.lower() in _DISCRIMINATOR_TAGS
            and i > 0
            and (isinstance(body[i - 1], int) or body[i - 1] in _UNION_FIELD_NAMES)
        ):
            continue
        kept.append(part)
    return tuple(kept) + tail


def _translate_pydantic_error(error: Mapping[str, Any]) -> ProcessIRDiagnostic:
    """Map one pydantic error dict to a deterministic, secret-free diagnostic.

    Never propagates pydantic 'input'/'ctx'/'msg' content for non-custom
    errors — messages are the static table above.
    """
    err_type = str(error.get("type") or "")
    loc = tuple(error.get("loc") or ())
    path = _loc_to_path(loc, keep_last=err_type == "extra_forbidden")
    last = loc[-1] if loc else None

    if err_type in _CUSTOM_ERROR_CODES:
        code = _CUSTOM_ERROR_CODES[err_type]
        # Custom messages are static strings raised by OUR validators (never
        # authored values), so surfacing them keeps diagnostics actionable.
        message = str(error.get("msg") or _MESSAGES[code])
        return _diagnostic(code, path, message=message)

    if err_type == "extra_forbidden":
        if (
            isinstance(last, str)
            and last in _GATED_EXTRA_KEYS
            and "cache_get" in loc
        ):
            return _diagnostic(
                PROCESS_IR_CAPABILITY_UNSUPPORTED,
                path,
                message="keyed/indexed cache retrieval is capability-gated in ProcessIR v1",
            )
        return _diagnostic(PROCESS_IR_SCHEMA_UNKNOWN_FIELD, path)

    if err_type in ("union_tag_invalid", "union_tag_not_found"):
        ctx = error.get("ctx") or {}
        tag = str(ctx.get("tag") or "")
        if tag in _GATED_UNION_TAGS:
            return _diagnostic(
                PROCESS_IR_CAPABILITY_UNSUPPORTED,
                path,
                message="the requested discriminator tag is capability-gated in ProcessIR v1",
            )
        return _diagnostic(PROCESS_IR_SCHEMA_UNKNOWN_NODE, path)

    if err_type in ("too_short", "too_long"):
        return _diagnostic(PROCESS_IR_SCHEMA_INVALID_CARDINALITY, path)

    return _diagnostic(PROCESS_IR_SCHEMA_INVALID, path)


def parse_process_ir_v1(payload: Any) -> ProcessIRV1:
    """Parse an authored payload into a validated :class:`ProcessIRV1`.

    Raises :class:`ProcessIRValidationError` with deterministic, sorted,
    secret-free diagnostics on any failure. Order of gates: payload shape →
    secret scan → version → strict model validation.
    """
    if not isinstance(payload, dict):
        raise ProcessIRValidationError(
            [_diagnostic(PROCESS_IR_SCHEMA_INVALID, (), message="payload must be a JSON object")]
        )

    secret_path = _find_secret_shaped_key(payload)
    if secret_path is not None:
        raise ProcessIRValidationError(
            [
                _diagnostic(
                    PROCESS_IR_CAPABILITY_UNSUPPORTED,
                    secret_path,
                    message="secret-shaped key rejected — ProcessIR carries only opaque references",
                )
            ]
        )

    if payload.get("version") != PROCESS_IR_VERSION:
        raise ProcessIRValidationError(
            [_diagnostic(PROCESS_IR_SCHEMA_VERSION_UNSUPPORTED, ("version",))]
        )

    try:
        return ProcessIRV1.model_validate(payload)
    except ValidationError as exc:
        diagnostics = [_translate_pydantic_error(err) for err in exc.errors()]
        raise ProcessIRValidationError(diagnostics) from None


# ---------------------------------------------------------------------------
# Canonical serialization + schema
# ---------------------------------------------------------------------------


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def canonical_process_ir_json(ir: ProcessIRV1) -> str:
    """Canonical JSON: defaults and Nones included, keys sorted, list order kept."""
    return _canonical_json(ir.model_dump(mode="json"))


def process_ir_v1_json_schema() -> dict:
    """The generated JSON Schema for :class:`ProcessIRV1` (closed unions)."""
    return ProcessIRV1.model_json_schema()


def canonical_process_ir_schema_json() -> str:
    return _canonical_json(process_ir_v1_json_schema())
