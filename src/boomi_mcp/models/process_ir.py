"""M12.1 (issue #136, epic #134): strict ``ProcessIRV1`` semantic process models.

Promotion of the frozen ``flow_sequence`` vocabulary (ADR-001 §4 "semantic seed",
inventory §1.4) into a strict, versioned, discriminated Pydantic model family —
NOT a parallel DSL (ADR-001 §12).

These models were DARK in #136. They are not any more: #146 serves this schema
through ``get_schema_template(schema_name="ProcessIRV1")`` and compiles authored
documents through ``build_integration(action="plan"|"compile")``. What has NOT
changed is the authoring boundary — a caller still writes only semantic nodes and
opaque component references, and never connector metadata, CFG edges, shape or
layout ids, XML, or secrets.

Because the schema is SERVED, every ``description`` in this module is published
contract text read by an LLM caller. Two rules follow, both pinned by tests:

- No description may cite a repository artifact (a ``.codex/`` capture ledger, a
  ``docs/architecture/`` page, or "the PROCESS_IR_V1_CAPABILITIES manifest").
  None of them is fetchable through any MCP tool, so a caller sent there is sent
  nowhere. Cite a ``process_ir_authoring`` contract entry id instead; the
  evidence pointers stay in comments, which are not served.
- No description may contain a compiler-internal identifier
  (``tests/test_process_ir_compiler_surface.py::FORBIDDEN_NAMES``).

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
from typing import Any, List, Literal, Mapping, Optional, Tuple, Union, get_args

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_core import PydanticCustomError
from typing_extensions import Annotated

from ..errors import (
    PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED,
    PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY,
    PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED,
    PROCESS_IR_CAPABILITY_UNSUPPORTED,
    PROCESS_IR_REFERENCE_INVALID_FORMAT,
    PROCESS_IR_SCHEMA_BRANCH_CARDINALITY,
    PROCESS_IR_SCHEMA_INVALID,
    PROCESS_IR_SCHEMA_INVALID_CARDINALITY,
    PROCESS_IR_SCHEMA_RETRY_COUNT,
    PROCESS_IR_SCHEMA_UNKNOWN_FIELD,
    PROCESS_IR_SCHEMA_UNKNOWN_NODE,
    PROCESS_IR_SCHEMA_VERSION_UNSUPPORTED,
    PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED,
    PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED,
    PROCESS_IR_SEMANTIC_NESTING_LIMIT,
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


def _is_component_ref_field(field_info) -> bool:
    """Is this field a :data:`ComponentRefV1`? Asked of the ANNOTATION, not the name.

    The discriminator is the validator's own identity in the field's metadata.
    A name list (``*_ref``) would be a hand-model of the same fact and would go
    stale the moment a reference field is named anything else.
    """
    return any(
        isinstance(meta, AfterValidator) and meta.func is _validate_component_ref
        for meta in getattr(field_info, "metadata", ()) or ()
    )


def iter_component_refs(node: Any, path: str = ""):
    """Every COMPONENT REFERENCE in a ProcessIR document, as ``(path, ref)``.

    Walks the typed model and yields only values held by
    :data:`ComponentRefV1`-annotated fields — so what counts as a reference is
    decided by the schema, not by what a string happens to look like.

    **Why this exists.** Three separate consumers were scanning a serialized IR
    (or its emitted XML) for strings starting with ``$ref:``/``id-`` and treating
    every hit as structural. That is a text search standing in for a schema
    question, and it is wrong in both directions: authored ``message`` text,
    script bodies, static values and templates are ordinary caller content that
    can legitimately contain either token, and a caller who writes ``$ref:x`` in
    a message got ``INTEGRATION_DEPENDENCY_REQUIRED`` for a dependency the IR
    does not have. Codex review round 1 found all three.

    Yields nothing for a non-model node, so it is safe to call on any subtree.
    """
    if isinstance(node, BaseModel):
        for name, field_info in type(node).model_fields.items():
            value = getattr(node, name, None)
            child = "{0}/{1}".format(path, name)
            if _is_component_ref_field(field_info):
                # A ref field holds a string, or a collection of them; both are
                # handled so a future `Tuple[ComponentRefV1, ...]` needs no edit.
                if isinstance(value, str):
                    yield child, value
                elif isinstance(value, (list, tuple)):
                    for index, item in enumerate(value):
                        if isinstance(item, str):
                            yield "{0}/{1}".format(child, index), item
                continue
            yield from iter_component_refs(value, child)
    elif isinstance(node, (list, tuple)):
        for index, item in enumerate(node):
            yield from iter_component_refs(item, "{0}/{1}".format(path, index))
    elif isinstance(node, Mapping):
        for key, item in node.items():
            yield from iter_component_refs(item, "{0}/{1}".format(path, key))


def _validate_contract_ref(value: str) -> str:
    """#142: idempotency-contract reference — ``$ref:KEY`` token ONLY.

    DELIBERATELY STRICTER than :data:`ComponentRefV1`, which also admits a literal
    id. The issue's acceptance criterion is that idempotency evidence "is typed,
    reference-based, and cannot be satisfied by an unverified free-form Boolean".
    A literal-id escape hatch would reopen exactly that hole: any non-blank string
    would parse, so ``"yes"`` / ``"idempotent"`` / a raw key value would all read
    as evidence. Requiring the token form means the value can only ever NAME a
    contract the symbol table has to resolve — and an unresolvable name is an
    error, not a pass.
    """
    if value != value.strip() or not value:
        raise PydanticCustomError(
            "process_ir_reference_invalid_format",
            "idempotency contract reference must be a non-blank string "
            "without surrounding whitespace",
        )
    if not value.startswith(_REF_TOKEN_PREFIX):
        raise PydanticCustomError(
            "process_ir_reference_invalid_format",
            "idempotency contract reference must be an exact '$ref:KEY' token — "
            "a literal id or free-form assertion is not evidence",
        )
    key = value[len(_REF_TOKEN_PREFIX):]
    if not key or any(ch.isspace() for ch in key):
        raise PydanticCustomError(
            "process_ir_reference_invalid_format",
            "'$ref:' token must carry a non-empty, whitespace-free key",
        )
    return value


IdempotencyContractRefV1 = Annotated[
    str,
    AfterValidator(_validate_contract_ref),
    Field(description="Opaque idempotency-contract reference: exact '$ref:KEY' token"),
]


def _cardinality_error(message: str) -> PydanticCustomError:
    return PydanticCustomError("process_ir_schema_invalid_cardinality", message)  # noqa: EM101


def _capability_error(message: str) -> PydanticCustomError:
    return PydanticCustomError("process_ir_capability_unsupported", message)  # noqa: EM101


def _continuation_error(message: str) -> PydanticCustomError:
    """#141: a node authored after a Branch/Decision."""
    return PydanticCustomError(  # noqa: EM101
        "process_ir_semantic_control_continuation_unsupported", message
    )


def _body_kind_error(
    message: str, *, at: Tuple[Any, ...] = ()
) -> PydanticCustomError:
    """#141: a known node kind in a control-body slot that does not admit it.

    ``at`` behaves exactly as it does for :func:`_return_path_binding_error` — the
    offending node's position relative to the model raising this, appended by
    ``_translate_pydantic_error``. Added in #175 round 3: the mixing rule names a
    step index in its MESSAGE ("step 1") while its pointer addressed the whole
    body, so the two halves of one diagnostic disagreed about what to look at.
    """
    return PydanticCustomError(  # noqa: EM101
        "process_ir_capability_node_not_allowed_in_body",
        message,
        {"offending_path": tuple(at)} if at else None,
    )


def _return_path_binding_error(
    message: str, *, at: Tuple[Any, ...] = ()
) -> PydanticCustomError:
    """#175: a process call authored so that execution continues past it.

    Distinct from ``_body_kind_error``: the kind IS admitted here, in the
    terminal position. What is unsupported is the CONTINUATION — which needs the
    called process's return-document shapes and is gated separately.

    ``at`` carries the offending node's position RELATIVE to the model raising
    this. A model-level validator's error is located at the model, so a defect at
    ``/body/steps/1`` would otherwise be served as ``/body`` — a pointer that is
    technically valid and practically useless on a long list. Pydantic passes the
    context through untouched, and ``_translate_pydantic_error`` appends it, so
    the served diagnostic addresses the node the caller has to change.
    """
    return PydanticCustomError(  # noqa: EM101
        "process_ir_capability_process_call_return_path_binding_unsupported",
        message,
        {"offending_path": tuple(at)} if at else None,
    )


def _nesting_error(message: str) -> PydanticCustomError:
    """#141: control nesting deeper than ``PROCESS_IR_V1_MAX_CONTROL_DEPTH``."""
    return PydanticCustomError("process_ir_semantic_nesting_limit", message)  # noqa: EM101


def _error_scope_error(message: str) -> PydanticCustomError:
    """#142: an unknown error scope, or a known scope in an unverified placement."""
    return PydanticCustomError(  # noqa: EM101
        "process_ir_capability_error_scope_unsupported", message
    )


def _catch_unterminated_error(message: str) -> PydanticCustomError:
    """#142: a catch body that does not reach a terminal."""
    return PydanticCustomError("process_ir_semantic_catch_unterminated", message)  # noqa: EM101


def _keyed_cache_true_only(value: bool) -> bool:
    """Legacy parity: only the all-document form is emittable — any non-True
    value is a keyed/indexed cache request, which is capability-gated."""
    if value is not True:
        raise _capability_error("keyed/indexed cache retrieval is capability-gated in ProcessIR v1")
    return value


# Strict boolean (1/1.0 rejected) that must be True; schema keeps `const: true`.
KeyedCacheAllDocsV1 = Annotated[StrictBool, AfterValidator(_keyed_cache_true_only)]


def _use_cache_true_only(value: bool) -> bool:
    if value is not True:
        raise ValueError("use_cache must be true (script compilation caching is required)")
    return value


UseCacheTrueV1 = Annotated[StrictBool, AfterValidator(_use_cache_true_only)]


# ---------------------------------------------------------------------------
# Property sources (Set Properties source_values entries)
# ---------------------------------------------------------------------------


class StaticPropertySourceV1(_ProcessIRBase):
    """A literal string written into the property.

    The value is authored as-is and is not interpreted, templated, or resolved
    against anything at runtime. An empty string is legal and means "write an
    empty value", which is distinct from omitting the source entirely. Secrets
    must never be authored here: the whole document is scanned for
    secret-shaped keys and rejected before compilation.
    """

    value_type: Literal["static"]
    value: str = Field(..., description="Literal value (may be empty)")


class CurrentPropertySourceV1(_ProcessIRBase):
    """Re-uses the property's CURRENT value as the source value.

    Carries no payload — the value is whatever the property already holds at
    this point on the path. Combined with other sources in ``source_values``, it
    is how a set-property step appends to, rather than replaces, what an earlier
    write established. Because it READS the property, lineage validation still
    requires an earlier write on the same path to establish it.
    """

    value_type: Literal["current"]


def _require_non_blank(model: Any, *fields: str) -> None:
    """Legacy parity: required identifier fields stay non-blank after .strip()."""
    for name in fields:
        value = getattr(model, name)
        if not value.strip():
            raise _cardinality_error(f"{name} must be a non-blank string")


class ProfilePropertySourceV1(_ProcessIRBase):
    """Reads a single element out of the current document via a profile.

    The profile is named by an opaque reference; ``element_id`` and
    ``element_name`` address one element inside it. Both are required and
    non-blank — the element is addressed by id, and the name is carried so a
    reader can see which element was meant without resolving the profile.

    Because the value comes from the CURRENT DOCUMENT, this source is
    per-document: on a path where the document stream has been replaced (an
    all-document cache read, a combine operation) it reads from the new stream,
    not the old one.
    """

    value_type: Literal["profile"]
    element_id: str = Field(..., min_length=1)
    element_name: str = Field(..., min_length=1)
    profile_ref: ComponentRefV1
    profile_type: str = Field(..., min_length=1, description="e.g. profile.json")

    @model_validator(mode="after")
    def _non_blank(self) -> "ProfilePropertySourceV1":
        _require_non_blank(self, "element_id", "element_name", "profile_type")
        return self


class DdpPropertySourceV1(_ProcessIRBase):
    """Reads a DYNAMIC DOCUMENT property (per-document scope).

    ``property_name`` is the bare name — the wire prefix is owned by the
    emitter and must not be authored. A dynamic document property travels with
    its own document copy: it is visible to later steps on the same path, and it
    is NOT visible across sibling Branch legs, because each leg receives an
    independent copy of the document stream.

    ``default_value`` supplies a value when the property has not been written,
    and supplying it DISCHARGES the read-before-write rule: a defaulted read
    cannot fail, because the default establishes the value. Omit it when you
    want an unmet read reported rather than silently defaulted.
    """

    value_type: Literal["ddp"]
    property_name: str = Field(..., min_length=1)
    default_value: Optional[str] = None

    @model_validator(mode="after")
    def _non_blank(self) -> "DdpPropertySourceV1":
        _require_non_blank(self, "property_name")
        return self


class DppPropertySourceV1(_ProcessIRBase):
    """Reads a DYNAMIC PROCESS property (per-execution scope).

    ``property_name`` is the bare name — the wire prefix is owned by the
    emitter. Unlike a dynamic DOCUMENT property, a dynamic PROCESS property is
    scoped to the whole execution, so a value written in an earlier Branch leg
    IS visible in a later leg. That ordering matters: legs run in the authored
    order, so reading in leg 0 what leg 1 writes is rejected rather than
    silently reading nothing.

    ``default_value`` supplies a value when the property is unset, and — as with
    the document-scoped source — supplying it DISCHARGES the read-before-write
    rule: a defaulted read cannot fail. Omit it when you want an unmet read
    reported rather than silently defaulted.
    """

    value_type: Literal["dpp"]
    property_name: str = Field(..., min_length=1)
    default_value: Optional[str] = None

    @model_validator(mode="after")
    def _non_blank(self) -> "DppPropertySourceV1":
        _require_non_blank(self, "property_name")
        return self


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
    """A Groovy 2 custom-scripting operation inside a Data Process step.

    The script body is authored verbatim and is the one place in this contract
    where free-form code is accepted. Its state effects are therefore OPAQUE to
    validation: a script that reads or writes properties cannot be proven safe,
    so lineage reports the effect as unknown unless a typed effect contract
    declares it. Scripts must never embed credentials — the document is scanned
    for secret-shaped keys before compilation.
    """

    operation: Literal["custom_scripting"]
    script: str = Field(..., min_length=1)
    language: Literal["groovy2"] = "groovy2"
    use_cache: UseCacheTrueV1 = Field(default=True, json_schema_extra={"const": True})

    @model_validator(mode="after")
    def _script_non_blank(self) -> "CustomScriptingOpV1":
        if not self.script.strip():
            raise _cardinality_error("script must be a non-blank string")
        return self


class SplitDocumentsOpV1(_ProcessIRBase):
    """Splits each inbound document into many, at a named profile element.

    This is the EXPLICIT way to fan a document out in ProcessIR v1: splitting is
    an authored Data Process operation, never an implicit side effect of another
    node. ``link_element_key``/``link_element_name`` address the repeating
    element inside the referenced profile.

    It REPLACES the document stream: everything downstream on this path sees the
    split documents, not the originals. A per-document property written before
    the split therefore does not carry over unchanged.
    """

    operation: Literal["split_documents"]
    profile_type: Literal["json", "xml"]
    profile_ref: ComponentRefV1
    link_element_key: str = Field(..., min_length=1)
    link_element_name: str = Field(..., min_length=1)

    @model_validator(mode="after")
    def _non_blank(self) -> "SplitDocumentsOpV1":
        _require_non_blank(self, "link_element_key", "link_element_name")
        return self


class CombineDocumentsOpV1(_ProcessIRBase):
    """Combines many inbound documents into one, at a named profile element.

    The counterpart of the split operation, and likewise EXPLICIT: combining is
    something you author, not something a Branch, a Decision, or a batching flow
    control does for you. ProcessIR v1 emits no join or merge of control paths —
    combining documents and rejoining paths are different things, and only the
    first is authorable here.

    ``combine_into_link_element_key`` defaults to ``'null'``, which combines into
    the document root. It REPLACES the document stream, so downstream steps see
    the single combined document.
    """

    operation: Literal["combine_documents"]
    profile_type: Literal["json", "xml"]
    profile_ref: ComponentRefV1
    link_element_key: str = Field(..., min_length=1)
    link_element_name: str = Field(..., min_length=1)
    combine_into_link_element_key: str = Field(
        default="null", min_length=1, description="'null' combines into the document root"
    )

    @model_validator(mode="after")
    def _non_blank(self) -> "CombineDocumentsOpV1":
        _require_non_blank(
            self, "link_element_key", "link_element_name", "combine_into_link_element_key"
        )
        return self


DataProcessOperationV1 = Annotated[
    Union[CustomScriptingOpV1, SplitDocumentsOpV1, CombineDocumentsOpV1],
    Field(discriminator="operation"),
]


# ---------------------------------------------------------------------------
# Decision operands
# ---------------------------------------------------------------------------


class TrackOperandV1(_ProcessIRBase):
    """A Decision operand read from a tracked property.

    ``property_id`` names the property being compared; ``property_name`` is
    optional display text and never changes which property is read.
    ``default_value`` supplies a fallback when the property is unset, which is
    what keeps a comparison total rather than erroring on an absent value.
    """

    value_type: Literal["track"]
    property_id: str = Field(..., min_length=1)
    property_name: Optional[str] = None
    default_value: Optional[str] = None

    @model_validator(mode="after")
    def _property_id_non_blank(self) -> "TrackOperandV1":
        if not self.property_id.strip():
            raise _cardinality_error("property_id must be a non-blank string")
        return self


class StaticOperandV1(_ProcessIRBase):
    """A Decision operand that is a literal comparison value.

    The value is compared as authored; an empty string is a legal operand and
    compares as the empty value rather than as "no operand". Both sides of a
    Decision are operands, so a static-vs-static comparison is expressible and
    is constant — which is a design smell, not an error, and is not rejected.
    """

    value_type: Literal["static"]
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

    kind: Literal["source"]
    connection_ref: ComponentRefV1
    operation_ref: ComponentRefV1
    label: Optional[str] = None


class TargetEndpointV1(_ProcessIRBase):
    """Current-parity target placeholder (see SourceEndpointV1)."""

    kind: Literal["target"]
    connection_ref: ComponentRefV1
    operation_ref: ComponentRefV1
    label: Optional[str] = None


# #142 M12.7. Idempotency evidence for a RETRIED connector call.
#
# Evidence never AUTHORIZES a retry on its own — the connector registry's
# ``retry_safety`` row decides what is replayable, and an ``unverified`` or
# ``non_idempotent`` row rejects the retry no matter what evidence is attached
# (see .codex/plans/issue-142-live-captures.md §G4: no authoritative source
# classifies a stock write action as replay-safe, and the one on-point official
# statement puts the burden on the caller). Evidence only discharges the extra
# obligation a row that IS retry-safe imposes.
class VerifiedActionIdempotencyV1(_ProcessIRBase):
    """The action itself is idempotent, as classified by the connector registry.

    Carries no payload: the claim is entirely about the resolved action, which the
    compiler already knows. Authoring it is an acknowledgement, not an assertion
    the compiler trusts.
    """

    kind: Literal["verified_action"]


class KeyReferenceIdempotencyV1(_ProcessIRBase):
    """An opaque, externally-defined idempotency contract, named by reference.

    ``contract_ref`` names a contract the symbol table must resolve to the SAME
    operation this call targets. It is a name, never key material: the key value
    itself is never authored, never stored, and never reaches a diagnostic or the
    emitted document.
    """

    kind: Literal["key_reference"]
    contract_ref: IdempotencyContractRefV1


IdempotencyEvidenceV1 = Annotated[
    Union[VerifiedActionIdempotencyV1, KeyReferenceIdempotencyV1],
    Field(discriminator="kind"),
]


class ConnectorCallNodeV1(_ProcessIRBase):
    """A first-class connector call (issue #140, M12.5).

    Unlike ``SourceEndpointV1``/``TargetEndpointV1`` — which are position-bound
    placeholders carrying BOTH ids — a ConnectorCall authors only the *operation*
    symbol. The connection is derived by the compiler from its symbol-table
    resolution context, never authored (ADR-001 §6). That is not a stylistic
    choice: no connector-action component declares its connection, because Boomi
    binds the connection at the process connector step rather than in the
    operation configuration. The operation-to-connection edge is therefore a fact
    of the component plan, and letting a caller author it would recreate exactly
    the duplicate-authority split ADR-001 exists to remove.

    ``action`` is an OPTIONAL ASSERTION of caller intent. It never supplies
    emitter metadata: the family and action that reach the wire always come from
    the resolved operation symbol. A supplied value that disagrees with the
    authoritative one is an error, never an override.

    Which family/action pairs are callable, whether each consumes or produces
    documents, and whether each may sit inside a retried region are published
    per pair — fetch ``process_ir_authoring`` entries under the
    ``connector_action`` category, or ``node.connector_call`` for this node.
    """
    # Evidence: .codex/plans/issue-140-live-captures.md FINDING 1. Kept in a
    # comment, not the served description — a caller cannot fetch that path.

    kind: Literal["connector_call"]
    operation_ref: ComponentRefV1
    action: Optional[str] = None
    idempotency: Optional[IdempotencyEvidenceV1] = None
    label: Optional[str] = None

    @field_validator("action")
    @classmethod
    def _action_non_blank(cls, value: Optional[str]) -> Optional[str]:
        # Absent is fine (the symbol is authoritative); present-but-blank is not
        # an assertion at all, and silently ignoring it would let a typo read as
        # agreement.
        if value is not None and not value.strip():
            raise _cardinality_error("action, when supplied, must be a non-blank string")
        return value


class FlowControlNodeV1(_ProcessIRBase):
    """Batches the document stream into groups of ``for_each_count`` documents.

    ProcessIR v1 authors exactly ONE flow-control mode: documents-per-batch.
    ``for_each_count`` must be a positive integer and is the batch size —
    downstream steps on this path then run once per batch rather than once per
    document.

    There is no caller-configurable parallel chunking or multiprocess execution:
    those settings are fixed by emission and are not authorable fields, so a
    flow control never makes a path run concurrently. It also does not split or
    combine documents — those are explicit ``data_process`` operations
    (``split_documents`` / ``combine_documents``). Batching regroups the stream;
    it does not change document contents.

    See the ``process_ir_authoring`` entries ``node.flow_control`` and
    ``capability.flow_control_parallel_chunks`` for the published states.
    """

    kind: Literal["flow_control"]
    for_each_count: StrictInt = Field(..., gt=0)
    label: Optional[str] = None


class MessageNodeV1(_ProcessIRBase):
    """Replaces the document stream with one document built from ``text``.

    The text is authored literally. Because the step PRODUCES its own document,
    it does not read the inbound one, and everything downstream on this path
    sees the message document instead of what arrived. That makes it useful as a
    payload shaper and unsuitable as a passthrough.
    """

    kind: Literal["message"]
    text: str = Field(..., min_length=1)
    label: Optional[str] = None


class MapRefNodeV1(_ProcessIRBase):
    """Applies a map component to the document stream.

    The map is named by an opaque reference; its source and destination profiles
    live in the map component and are never authored here. A map transforms each
    document, so the stream shape is preserved while the contents change.

    Like a custom script, a map's property effects are OPAQUE unless a typed
    effect contract declares them, so lineage reports state written only inside
    a map as unknown rather than assuming it.
    """

    kind: Literal["map_ref"]
    map_ref: ComponentRefV1
    label: Optional[str] = None


class DataProcessNodeV1(_ProcessIRBase):
    """One or more explicit data-processing operations, run in authored order.

    ``steps`` is a non-empty ordered list of ``custom_scripting``,
    ``split_documents`` or ``combine_documents`` operations. This node is where
    document fan-out and fan-in are authored EXPLICITLY: nothing else in this
    contract splits or combines documents implicitly.

    The operations run in the order given, and each one's stream effect applies
    to the next — a split followed by a combine is not a no-op, because the
    combine acts on the documents the split produced.
    """

    kind: Literal["data_process"]
    steps: List[DataProcessOperationV1] = Field(..., min_length=1)
    label: Optional[str] = None


class CachePutNodeV1(_ProcessIRBase):
    """Add to Cache write. CONSUMES the document stream — the containing
    sequence rules require a stream-replacing cache read right after it."""

    kind: Literal["cache_put"]
    cache_ref: ComponentRefV1
    label: Optional[str] = None


class DocumentCacheRetrieveNodeV1(_ProcessIRBase):
    """Legacy all-document Document Cache Retrieve (M10.5 parity node)."""

    kind: Literal["document_cache_retrieve"]
    cache_ref: ComponentRefV1
    empty_cache_behavior: Literal["stopprocess"] = "stopprocess"
    load_all_documents: KeyedCacheAllDocsV1 = Field(default=True, json_schema_extra={"const": True})
    label: Optional[str] = None


class CacheGetNodeV1(_ProcessIRBase):
    """Authored all-document cache read; ``external_writer`` carries the
    authored lineage assertion (cache populated outside this process)."""

    kind: Literal["cache_get"]
    cache_ref: ComponentRefV1
    empty_cache_behavior: Literal["stopprocess"] = "stopprocess"
    external_writer: StrictBool = False
    label: Optional[str] = None


class CacheRemoveNodeV1(_ProcessIRBase):
    """Removes ALL documents from a document cache.

    Whole-cache removal is the only supported mode: ``remove_all_documents`` is
    fixed ``true``, and keyed or indexed removal is capability-gated because no
    verified wire shape exists for it (see the ``process_ir_authoring`` entry
    ``capability.keyed_cache``).

    The step operates on the cache, not on the document stream — it neither
    consumes nor replaces the documents flowing through this path.
    """

    kind: Literal["cache_remove"]
    cache_ref: ComponentRefV1
    remove_all_documents: KeyedCacheAllDocsV1 = Field(default=True, json_schema_extra={"const": True})
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
    """Writes a DYNAMIC DOCUMENT property (per-document scope).

    ``name`` is the BARE property name — the emitter owns the wire prefix, so a
    prefixed name is rejected rather than double-prefixed. ``source_values`` is
    a non-empty ordered list; the sources are concatenated in the authored
    order, which is how ``current`` composes with a literal to append.

    Scope is the document copy. A value written here is visible to later steps
    on the same path and is NOT visible in a sibling Branch leg, because each
    leg gets its own copy of the stream. Where paths converge, only state
    written on EVERY incoming path counts as established.

    See the ``process_ir_authoring`` entry ``state_visibility.ddp``.
    """

    kind: Literal["set_ddp"]
    name: str = Field(..., min_length=1, description="Bare property name (no wire prefix)")
    source_values: List[PropertySourceV1] = Field(..., min_length=1)
    label: Optional[str] = None

    @model_validator(mode="after")
    def _name_rules(self) -> "SetDdpNodeV1":
        _validate_bare_property_name(self.name)
        return self


class SetDppNodeV1(_ProcessIRBase):
    """Writes a DYNAMIC PROCESS property (per-execution scope).

    ``name`` is the BARE property name — the emitter owns the wire prefix.
    ``source_values`` is a non-empty ordered list concatenated in the authored
    order.

    Scope is the whole execution, not the document copy, so a value written in
    one Branch leg IS visible in a LATER leg. Legs run in the authored order, so
    a leg that reads what a later leg writes is reported rather than silently
    reading nothing — reordering the legs is the fix.

    ``persist`` requests that the value survive beyond the step that set it; it
    changes durability, never scope.

    See the ``process_ir_authoring`` entry ``state_visibility.dpp``.
    """

    kind: Literal["set_dpp"]
    name: str = Field(..., min_length=1, description="Bare property name (no wire prefix)")
    source_values: List[PropertySourceV1] = Field(..., min_length=1)
    persist: StrictBool = False
    label: Optional[str] = None

    @model_validator(mode="after")
    def _name_rules(self) -> "SetDppNodeV1":
        _validate_bare_property_name(self.name)
        return self


class ProcessCallNodeV1(_ProcessIRBase):
    """Invokes another process, and TERMINATES the path it is on.

    ``process_ref`` accepts a '$ref:KEY' in-spec child token or a literal
    existing component id.

    Whether execution continues past a call is not decided by this document: it
    is decided by the called process, which hands control back only through the
    return-document steps it declares. ProcessIR v1 supports the non-returning
    form, where the call is the end of its path — so a call is authored as a
    TERMINAL, with nothing after it and no trailing stop. Authoring a node after
    a call is rejected.

    Binding a RETURNING child's return paths is published separately as the
    gated capability ``process_call_return_path_binding``; see the
    ``process_ir_authoring`` capability entries.
    """

    kind: Literal["process_call"]
    process_ref: ComponentRefV1
    wait: StrictBool = True
    abort_on_error: StrictBool = False
    label: Optional[str] = None


# ---------------------------------------------------------------------------
# Terminal + control nodes
# ---------------------------------------------------------------------------


class StopNodeV1(_ProcessIRBase):
    """Successful-stop terminal (continue semantics are emitter-owned)."""

    kind: Literal["stop"]


class ReturnDocumentsNodeV1(_ProcessIRBase):
    """Terminal that returns the current document batch to the caller.

    Returns whatever documents reach it on this path and TERMINATES the path —
    nothing may follow it. It is the terminal a process invoked by another
    process uses to hand its results back; a plain ``stop`` ends the path
    without returning anything.

    It is a ROOT-sequence terminal only: it is not admitted in a Branch leg, a
    Decision arm, or a Try/Catch body.
    """

    kind: Literal["return_documents"]
    label: Optional[str] = None


class ExceptionNodeV1(_ProcessIRBase):
    """Terminal Exception throw. No ``label`` — parity with the legacy
    exception step key set (title/message_template/stop_single_document/
    parameter_source only)."""

    kind: Literal["exception"]
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


# The ROOT/legacy linear vocabulary. Deliberately UNCHANGED by #141: it is what a
# root ``SequenceNodeV1`` admits between its endpoints, and widening it would
# change legacy sequences. The richer control-body vocabularies below are
# separate unions (#141, M12.6).
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


# A MODEL DOCSTRING IS PUBLISHED. Pydantic serializes it into the generated JSON
# Schema as the model's ``description``, so a docstring is part of the exported
# surface, not an internal note. It must therefore never name emitter/wire
# vocabulary (dragpoints, coordinates, shape ids/types) — the same prohibition
# the schema itself is under, pinned by
# ``test_schema_carries_no_layout_cfg_or_open_config_vocabulary``. Cite live
# evidence by capture SECTION, never by wire attribute or shape name. Module-level
# comments like this one are not serialized and are the right home for the detail.
def _kinds_of(union_alias: Any) -> Tuple[str, ...]:
    """The ``kind`` discriminator literals of an ``Annotated[Union[...]]`` alias.

    DERIVED, never hand-listed: ``body_capabilities`` keys its registry off these
    sets, and a hand-maintained copy would drift the moment a node kind is added
    to a union — which is exactly the duplicate-authority failure ADR-001 §6
    exists to remove. A coverage test pins union membership against the registry
    in both directions.
    """
    members = get_args(get_args(union_alias)[0])
    return tuple(get_args(member.model_fields["kind"].annotation)[0] for member in members)


#: The exact kind set of :data:`LinearNodeV1`, shared by every control body.
LINEAR_BODY_KINDS: Tuple[str, ...] = _kinds_of(LinearNodeV1)

# #141 M12.6, amended by #175. The step vocabularies inside a control body. Both
# admit the linear set plus ``connector_call`` (capability
# ``connector_call_in_control_body``).
#
# ``process_call`` is deliberately ABSENT from both step sets. #141 admitted it
# as a STEP and required the body to end in a ``stop``, but that generalised past
# the evidence: the captures attest a control edge landing ON a Process Call, not
# a Process Call wired onward to a Stop. The platform keys a call's outbound
# connection on the CALLED process's return-document shapes, so a call whose
# child returns nothing is itself the end of the path — it belongs in the
# TERMINAL slot, which is where #175 moved it.
#
# Nested control is a TERMINAL, never a step: a step is by definition followed by
# something on the same path, and a control node terminalizes its path. A
# ``process_call`` is terminal for the same structural reason.
BranchLegStepV1 = Annotated[
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
        ConnectorCallNodeV1,
    ],
    Field(discriminator="kind"),
]

# The TRUE arm shares the Branch leg's step vocabulary; the FALSE arm is the
# reject route and admits no ProcessCall in EITHER slot (capture §2.2 attests
# ``decision ->true-> processcall`` only).
DecisionTrueArmStepV1 = BranchLegStepV1

DecisionFalseArmStepV1 = Annotated[
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
        ConnectorCallNodeV1,
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


def _check_process_call_terminal_form(
    steps: List[Any], terminal: Any, *, context: str
) -> None:
    """ProcessCall TERMINAL FORM (#141 PATH MODE, amended by #175).

    A process call ends its path. The platform projects a call's outbound
    connection from the CALLED process's return-document shapes, so a call whose
    child returns nothing has no outgoing edge at all — and ProcessIR v1 has no
    field, and no cross-component check, that could establish the child returns
    anything. V1 therefore admits the terminal form ONLY:

    * the call sits in the terminal slot, with an EMPTY step prefix;
    * nothing follows it — not a stop, not a nested control node.

    ``process_call_connector_mixing`` stays gated PER ROOT-TO-LEAF PATH. Moving
    the call to the terminal slot makes ``steps=[connector_call], terminal=
    process_call`` representable for the first time, so the mixing rule is
    enforced here against the terminal as well as against the steps — otherwise
    widening the slot would silently open the very hole #141 gated.

    This check is BODY-LOCAL and therefore not sufficient on its own: a body
    cannot see its ancestors. ``_walk_controls`` on the whole document enforces
    the other half — that no connector sits upstream on the same root-to-leaf
    path — and ``body_capabilities`` re-derives both from the compiler side, so a
    mutated model that never re-ran this validator is still refused.
    """
    verdict = process_call_placement_verdict(steps, terminal, context=context)
    if verdict is None:
        return
    reason, at, message = verdict
    if reason == PLACEMENT_CONNECTOR_MIXING:
        raise _body_kind_error(message, at=at)
    raise _return_path_binding_error(message, at=at)


# The three ways a body can place a ``process_call`` wrongly. Names, not bare
# strings, because both renderers branch on them.
#: The ONE wording for "a process_call sits in a step slot". Both entry points
#: serve it: `_translate_pydantic_error` for the union rejection a caller hits by
#: authoring, and `process_call_placement_verdict` for the mutable-model path.
PROCESS_CALL_STEP_FORM_MESSAGE = (
    "a process_call is the terminal of its path — author it in "
    "this body's terminal slot, with nothing after it"
)

PLACEMENT_CONNECTOR_MIXING = "connector_mixing"
PLACEMENT_STEP_FORM = "step_form"
PLACEMENT_PREFIX = "prefix"

#: The human label each body context contributes to a placement message, keyed by
#: the compiler's body-context name. ONE table: the message text is authored here,
#: and the compiler renders it, so the two entry points cannot drift to
#: "decision true-arm" and "decision true arm" for the same body.
#: ``test_every_body_context_has_a_placement_label`` derives its coverage from the
#: compiler's published context set rather than restating it.
PROCESS_CALL_PLACEMENT_CONTEXT_LABELS: Mapping[str, str] = MappingProxyType({
    "branch_leg": "branch leg",
    "decision_true_arm": "decision true-arm",
    "decision_false_arm": "decision false-arm",
    "try_body": "try body",
    "catch_body": "catch body",
})


PLACEMENT_ROOT_CONNECTOR_MIXING = "root_connector_mixing"
PLACEMENT_ROOT_SINGLETON = "root_singleton"


def process_call_root_verdict(kinds):
    """THE authority on how a ROOT sequence's ``process_call`` placement is
    diagnosed. Same contract as :func:`process_call_placement_verdict`, for the
    slot that function deliberately does not cover.

    It exists for the same reason and was found the same way. The body rule got
    one authority in round 3; the ROOT rule was left as two hand-written copies,
    and live QA's own sibling sweep measured them disagreeing on all five illegal
    root shapes — on the POINTER for `[pc, stop]`, `[set, pc]` and `[pc, set]`
    (one path names the call, the other names the node beside it), and on the
    CODE itself when a connector is present, where one path serves a capability
    refusal at `/body` and the other a return-path refusal at a step index. A
    machine consumer branching on the code took a different branch depending on
    which entry point it called.

    Returns ``(reason, at, message)`` or ``None`` when the root is legal.
    """
    if kinds == ["process_call"] or "process_call" not in kinds:
        return None
    # Connector mixing keeps its own code AND its precedence: it stays gated on
    # its own terms even once return-path binding lands, so a caller must not be
    # told to drop the trailing stop when the real obstacle is the connector.
    if any(kind in _CONNECTOR_KINDS for kind in kinds):
        return (
            PLACEMENT_ROOT_CONNECTOR_MIXING,
            (),
            "a process_call may not share a root-to-leaf path with a "
            "connector step (process_call_connector_mixing is gated)",
        )
    # The only legal process-call root is the exact singleton, so the offending
    # node is the first step that is not THE call. Written against the first
    # call's INDEX rather than as "the first non-call": an all-call chain has no
    # non-call element, and that spelling raised StopIteration straight out of
    # the validator. This form is total — the singleton returned above, so at
    # least two steps remain and an index other than the first call exists.
    first_call = kinds.index("process_call")
    offending = next(i for i in range(len(kinds)) if i != first_call)
    return (
        PLACEMENT_ROOT_SINGLETON,
        ("steps", offending),
        "a process_call is the terminal of its path — a root sequence "
        "containing one admits no other step, including a trailing stop or "
        "return_documents (step {0})".format(offending),
    )


def process_call_placement_verdict(
    steps: List[Any], terminal: Any, *, context: str
) -> Optional[Tuple[str, Tuple[Any, ...], str]]:
    """THE authority on how a body's ``process_call`` placement is diagnosed.

    Returns ``(reason, at, message)`` for the first rule this body breaks, or
    ``None`` when the placement is legal. ``at`` is the offending node's position
    relative to the body.

    **Why this exists.** Two public entry points enforce this rule — the model
    validators reached by authoring, and ``body_capabilities`` reached by handing
    a mutated ``ProcessIRV1`` straight to ``compile_process_ir_v1`` — and until
    #175 round 3 each carried its OWN copy of the decision. A sibling sweep found
    the two copies disagreeing on four documents: the prefix pointer
    (``/terminal`` vs ``/steps/0``), the connector-with-call-terminal pointer,
    and two orderings where one path reported mixing and the other reported the
    return-path rule for the same body. Neither copy was wrong in isolation;
    having two was the defect. Both now RENDER this verdict instead of deciding,
    so a divergence is unwritable rather than merely absent.

    **The order is DERIVED, not chosen.** A ``process_call`` is not a member of
    any step-slot union — #175 moved it to the terminal slot — so for a body with
    a call in ``steps`` the parser never reaches a mixing question at all: the
    union rejects the call first, and ``_translate_pydantic_error`` serves the
    return-path code at that step. Step form therefore comes FIRST here, because
    that is what the type system already decides; ordering mixing ahead of it
    would make the compiler contradict the union rather than render it. (Measured:
    ordering mixing first left ``[call, connector]`` and ``[connector, call]``
    disagreeing between the two paths in exactly the way this function exists to
    prevent.)

    Mixing then precedes the PREFIX reason, for a body whose call is the terminal.
    Both reject it, but they have different lifetimes: the prefix reason is a #175
    consequence that #176 lifts once a call can be bound to its child's
    return-document shapes, while ``process_call_connector_mixing`` stays gated
    beyond it. Reporting the prefix on a body that also mixes would tell the caller
    to drop the prefix when the pairing is gated regardless — advice that expires,
    and expires wrong.

    This check is BODY-LOCAL and not sufficient alone: a body cannot see its
    ancestors. ``_walk_controls`` and ``body_capabilities`` enforce the
    cross-nesting half — that no connector sits upstream on the same root-to-leaf
    path — which has no same-body verdict to render.
    """
    call_index = next(
        (i for i, s in enumerate(steps) if getattr(s, "kind", None) == "process_call"),
        None,
    )
    terminal_is_call = getattr(terminal, "kind", None) == "process_call"
    if call_index is None and not terminal_is_call:
        return None
    if call_index is not None:
        # The SHARED constant, not a context-specific rendering. For this reason
        # the parser never reaches this function at all — `process_call` is in no
        # step-slot union, so pydantic rejects the node and
        # `_translate_pydantic_error` serves the message. If the verdict phrased
        # it differently the two entry points would agree on code and pointer and
        # still hand the caller different words, which is what round 5 measured.
        # The step INDEX is deliberately absent: the pointer already carries it,
        # and duplicating a fact into the message is the same mechanism that
        # produced the shared-target defect earlier in this slice.
        return (PLACEMENT_STEP_FORM, ("steps", call_index), PROCESS_CALL_STEP_FORM_MESSAGE)
    connector_index = next(
        (i for i, s in enumerate(steps) if getattr(s, "kind", None) in _CONNECTOR_KINDS),
        None,
    )
    if connector_index is not None:
        return (
            PLACEMENT_CONNECTOR_MIXING,
            ("steps", connector_index),
            "a process_call may not share a root-to-leaf path with a connector "
            "step — a connector runs upstream in this {0} (step {1}; "
            "process_call_connector_mixing is gated)".format(context, connector_index),
        )
    if steps:
        return (
            PLACEMENT_PREFIX,
            ("terminal",),
            "a process_call {0} terminal admits no preceding steps — a call whose "
            "child returns no documents ends the path it is on, and a prefix before "
            "it is not attested".format(context),
        )
    return None


def _check_stop_terminal_has_work(steps: List[Any], terminal: Any, *, context: str) -> None:
    """A leg/arm that ONLY stops does nothing at all.

    Fail-closed: a control connector wired straight to a Stop with no intervening
    work is not live-attested for a Branch leg or a Decision TRUE arm (the capture
    records the empty-leg question as UNPROVEN), so V1 requires at least one step.
    The Decision FALSE arm is the documented exception — capture §2.1 shows
    ``shape16 ->false-> shape38 stop`` with zero intervening steps — and is checked
    by its own arm rules, not here.
    """
    if getattr(terminal, "kind", None) == "stop" and not steps:
        raise _cardinality_error(
            "a {0} whose terminal is a stop must contain at least one step".format(context)
        )


class BranchLegV1(_ProcessIRBase):
    """One Branch leg (#141: rich bodies).

    Steps are the linear vocabulary plus ``connector_call``; the terminal is a
    routed target endpoint, a target-less staging ``cache_put`` (the staging
    pattern), a plain ``stop``, a ``process_call``, or a nested ``decision``.

    A ``process_call`` is a TERMINAL, never a step, and admits no step prefix: a
    call ends the path it is on, because whether execution continues past it is
    determined by the called process's return-document shapes rather than by this
    document. Authoring anything after a call is rejected.

    A nested ``branch`` is deliberately ABSENT — a Branch is not a legal Branch-leg
    terminal. Only placements with attested evidence are admitted, and this one
    has none; the rule is fail-closed, so absence means rejected.

    The exact admitted set for each slot is published as the
    ``process_ir_authoring`` entries ``placement.branch_path.step`` and
    ``placement.branch_path.terminal``.
    """
    # Evidence: .codex/plans/issue-141-live-captures.md §2.1 attests
    # Branch-leg -> Decision and records no Branch-leg -> Branch anywhere.
    # Kept in a comment: a caller cannot fetch that path.

    steps: List[BranchLegStepV1] = Field(default_factory=list)
    terminal: Annotated[
        Union[
            TargetEndpointV1,
            CachePutNodeV1,
            StopNodeV1,
            ProcessCallNodeV1,
            "DecisionNodeV1",
        ],
        Field(discriminator="kind"),
    ]

    @model_validator(mode="after")
    def _leg_rules(self) -> "BranchLegV1":
        _check_cache_put_followed_by_read(self.steps, context="branch leg steps")
        if self.steps and self.steps[-1].kind == "cache_put":
            raise _cardinality_error(
                "a trailing cache_put belongs in the leg terminal (target-less staging leg), not in steps"
            )
        _check_process_call_terminal_form(
            self.steps, self.terminal,
            context=PROCESS_CALL_PLACEMENT_CONTEXT_LABELS["branch_leg"],
        )
        _check_stop_terminal_has_work(self.steps, self.terminal, context="branch leg")
        return self


class BranchNodeV1(_ProcessIRBase):
    """Runs 2–25 legs in the authored order, SEQUENTIALLY.

    Execution is ordered and sequential, one leg fully before the next — never
    parallel. That ordering is load-bearing, not incidental: state written in an
    earlier leg is visible to a later one at execution scope, so authoring a leg
    that reads what a later leg writes is an error the ordering makes real.

    Each leg receives an INDEPENDENT COPY of the document stream, so legs do not
    see each other's document-scoped state and a leg cannot consume the
    documents another leg needs.

    A Branch TERMINALIZES its path: ProcessIR v1 emits no continuation after a
    Branch, and no join or merge of the legs. Work that must happen "after the
    branch" is authored inside every leg instead.

    Nesting is bounded at two control levels — a compiler bound of this contract,
    NOT a Boomi platform limit — and a nested Branch is not a legal leg terminal
    (see ``BranchLegV1``). Deeper routing belongs in a subprocess.

    Published states for the related capabilities live in
    ``process_ir_authoring``: ``capability.continuation_after_branch_or_decision``,
    ``capability.joins``, ``capability.parallel_branch_execution``.
    """

    kind: Literal["branch"]
    legs: List[BranchLegV1] = Field(..., min_length=2, max_length=25)
    label: Optional[str] = None


class DecisionTrueArmV1(_ProcessIRBase):
    """TRUE (success) arm (#141: rich bodies).

    Steps are the linear vocabulary plus ``connector_call``; the terminal is a
    routed target, a plain ``stop``, an ``exception``, a ``process_call``, or a
    nested ``branch``/``decision``.

    A ``process_call`` is a TERMINAL, never a step, and admits no step prefix: a
    call ends the path it is on, because whether execution continues past it is
    determined by the called process's return-document shapes rather than by this
    document. Authoring anything after a call is rejected.

    This arm admits ``process_call`` and a routed target; the FALSE arm admits
    neither. The asymmetry is deliberate — each placement is admitted only where
    evidence attests it — so a body that is legal here may be rejected there.

    The admitted sets are published as the ``process_ir_authoring`` entries
    ``placement.decision_true_arm.step`` and ``placement.decision_true_arm.terminal``.
    """
    # Evidence: .codex/plans/issue-141-live-captures.md §2.1 (Decision-in-Decision,
    # and ProcessCall on TRUE outcomes only). Comment, not served text.

    steps: List[DecisionTrueArmStepV1] = Field(default_factory=list)
    terminal: Annotated[
        Union[
            TargetEndpointV1,
            StopNodeV1,
            ExceptionNodeV1,
            ProcessCallNodeV1,
            BranchNodeV1,
            "DecisionNodeV1",
        ],
        Field(discriminator="kind"),
    ]

    @model_validator(mode="after")
    def _arm_rules(self) -> "DecisionTrueArmV1":
        _check_cache_put_followed_by_read(self.steps, context="decision true-arm steps")
        if self.steps and self.steps[-1].kind == "cache_put":
            raise _cardinality_error(
                "decision true-arm steps must not end in cache_put — the arm terminal would receive an empty stream"
            )
        _check_process_call_terminal_form(
            self.steps, self.terminal,
            context=PROCESS_CALL_PLACEMENT_CONTEXT_LABELS["decision_true_arm"],
        )
        _check_stop_terminal_has_work(
            self.steps, self.terminal,
            context=PROCESS_CALL_PLACEMENT_CONTEXT_LABELS["decision_true_arm"],
        )
        return self


class DecisionFalseArmV1(_ProcessIRBase):
    """FALSE (reject) arm (#141: rich bodies).

    Steps are the linear vocabulary plus ``connector_call`` — no ``process_call``,
    and no routed target, both of which the TRUE arm admits. The terminal is a
    ``stop``, an ``exception``, or a nested ``branch``/``decision``.

    Unlike a Branch leg and the TRUE arm, this arm may be EMPTY: a Decision may
    route its false outcome straight to a ``stop`` with zero intervening steps.
    The legacy ``flow_sequence`` surface keeps rejecting that shape; only the
    direct IR accepts it.

    The admitted sets are published as the ``process_ir_authoring`` entries
    ``placement.decision_false_arm.step`` and ``placement.decision_false_arm.terminal``.
    """
    # Evidence: .codex/plans/issue-141-live-captures.md §2.1 shows a real
    # Decision routing false -> stop with no steps, which is why #141 dropped the
    # legacy "reject path is never a bare Stop" builder rule. Comment, not served.

    steps: List[DecisionFalseArmStepV1] = Field(default_factory=list)
    terminal: Annotated[
        Union[StopNodeV1, ExceptionNodeV1, BranchNodeV1, "DecisionNodeV1"],
        Field(discriminator="kind"),
    ]

    @model_validator(mode="after")
    def _arm_rules(self) -> "DecisionFalseArmV1":
        _check_cache_put_followed_by_read(self.steps, context="decision false-arm steps")
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
    """A two-way conditional: compares ``left`` to ``right`` and takes one arm.

    Exactly one of ``true_arm``/``false_arm`` runs — the arms are mutually
    exclusive, never both, and both are REQUIRED. Seven comparisons are
    authorable: ``equals``, ``greaterthaneq``, ``lessthaneq``, ``greaterthan``,
    ``lessthan``, ``regex``, ``wildcard``.

    The two arms admit DIFFERENT bodies (see ``DecisionTrueArmV1`` and
    ``DecisionFalseArmV1``) — a body that is legal on one is not automatically
    legal on the other.

    Like a Branch, a Decision TERMINALIZES its path: ProcessIR v1 emits no
    continuation after it and no join of the arms, so work that must follow the
    decision is authored inside both arms. Where the arms would conceptually
    converge, only state written on BOTH is treated as established.

    Nesting is bounded at two control levels — a compiler bound of this contract,
    not a Boomi platform limit.
    """

    kind: Literal["decision"]
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


# Resolve the #141 recursion. ``BranchLegV1`` and both Decision arms name
# ``DecisionNodeV1`` as a forward reference because a nested Decision may appear
# in a Branch leg and in either arm, while ``DecisionNodeV1`` is itself built out
# of those arms — a genuine cycle that cannot be broken by reordering. Every
# model in the cycle is rebuilt here, once, at import time; a failure to resolve
# raises now rather than at first validation.
for _model in (
    BranchLegV1,
    BranchNodeV1,
    DecisionTrueArmV1,
    DecisionFalseArmV1,
    DecisionNodeV1,
):
    _model.model_rebuild()
del _model


# ---------------------------------------------------------------------------
# #142 M12.7 — scoped error handling
# ---------------------------------------------------------------------------

# The Try/Catch step vocabulary. Both bodies share ONE step union: a caught
# document is an ordinary document, so anything legal on the protected path is
# legal on the recovery path.
#
# ABSENT ON PURPOSE (fail-closed, each for its own reason):
#   * ``branch``/``decision``/``process_call``/nested ``try_catch`` — composing
#     two Try/Catch steps silently rewrites the OUTER step's effective error
#     selection, and the rule differs depending on whether they are adjacent
#     (capture §G6). A single deterministic semantic cannot be derived from the
#     authored fields, so nesting stays out.
#   * ``flow_control``/``data_process`` — no evidence for either placement inside
#     a protected scope.
#   * ``target``/``source``/``return_documents`` — legacy position-bound
#     placeholders that have no meaning inside an error scope.
TryCatchBodyStepV1 = Annotated[
    Union[
        MessageNodeV1,
        MapRefNodeV1,
        CachePutNodeV1,
        DocumentCacheRetrieveNodeV1,
        CacheGetNodeV1,
        CacheRemoveNodeV1,
        SetDdpNodeV1,
        SetDppNodeV1,
        ConnectorCallNodeV1,
    ],
    Field(discriminator="kind"),
]

#: The exact kind set of :data:`TryCatchBodyStepV1`, shared by both bodies.
TRY_CATCH_BODY_KINDS: Tuple[str, ...] = _kinds_of(TryCatchBodyStepV1)


class RetryPolicyV1(_ProcessIRBase):
    """Bounded retry intent for a protected region.

    ``count`` is the whole policy. The platform owns everything else: the wait
    before each attempt is fixed and not authorable, and retries are skipped
    entirely in test runs. Authoring a delay, a backoff curve, or a per-attempt
    policy is therefore not a feature this version withholds — there is no field
    on the wire to carry it.
    """

    count: StrictInt = Field(
        ...,
        ge=0,
        le=5,
        description="Retry attempts for a failed document (0-5); 0 means no retry",
    )


#: The verified error scopes.
#:
#: ``process`` protects the whole flow from its entry call onward; ``connector``
#: protects one downstream call and its property preparation, leaving the
#: upstream source outside. Both are emitter-verified placements — a third value
#: would have no shape to compile to.
ErrorScopeV1 = Literal["process", "connector"]


class TryCatchTryBodyV1(_ProcessIRBase):
    """The protected path.

    Terminates on a plain ``stop`` only. An ``exception`` here would be caught by
    this very scope's own recovery path, and no evidence covers that loop; a
    staging ``cache_put`` terminal is a recovery-path shape, not a success one.
    """

    steps: List[TryCatchBodyStepV1] = Field(..., min_length=1)
    terminal: Annotated[Union[StopNodeV1], Field(discriminator="kind")]

    @model_validator(mode="after")
    def _try_body_rules(self) -> "TryCatchTryBodyV1":
        _check_cache_put_followed_by_read(self.steps, context="try body steps")
        if self.steps and self.steps[-1].kind == "cache_put":
            raise _cardinality_error(
                "a trailing cache_put in a try body must be followed by a "
                "stream-replacing cache read, not by the terminal"
            )
        return self


class TryCatchCatchBodyV1(_ProcessIRBase):
    """The recovery path for a caught document.

    MANDATORY and MUST terminate: a caught document that reaches no terminal is
    a document the process silently drops. The terminal set is the recovery
    vocabulary — stop the document, raise it as an explicit failure, or stage it
    for a downstream handler.

    Steps may be empty (a bare terminal is a meaningful recovery), but a bare
    ``stop`` with no work at all is rejected: it recovers nothing.
    """

    steps: List[TryCatchBodyStepV1] = Field(default_factory=list)
    terminal: Annotated[
        Union[StopNodeV1, ExceptionNodeV1, CachePutNodeV1],
        Field(discriminator="kind"),
    ]

    @model_validator(mode="after")
    def _catch_body_rules(self) -> "TryCatchCatchBodyV1":
        _check_cache_put_followed_by_read(self.steps, context="catch body steps")
        if self.steps and self.steps[-1].kind == "cache_put":
            raise _cardinality_error(
                "a trailing cache_put belongs in the catch terminal (staging sink), not in steps"
            )
        _check_stop_terminal_has_work(self.steps, self.terminal, context="catch body")
        return self


class TryCatchNodeV1(_ProcessIRBase):
    """Scoped error handling with bounded retry (#142, M12.7).

    Placement is part of the contract, because scope is not a free-form label —
    each value names a placement the compiler has a verified shape for:

    * ``process`` is the sole root step, and its protected path begins with the
      call that produces the flow's documents;
    * ``connector`` is the terminal step of a call sequence, and protects exactly
      one downstream call plus the property steps that prepare it.

    Nothing may follow a Try/Catch: both paths terminate independently and there
    is no join.

    Retry is bounded and additionally CONSTRAINED BY SAFETY, not just by range: a
    positive count is rejected when the protected region would re-run the flow's
    document source, and when a retried call writes without registry-backed
    replay safety. Those checks run before anything is emitted.
    """

    kind: Literal["try_catch"]
    scope: ErrorScopeV1
    try_body: TryCatchTryBodyV1
    catch_body: TryCatchCatchBodyV1
    retry: Optional[RetryPolicyV1] = None
    label: Optional[str] = None

    @property
    def retry_count(self) -> int:
        """Absent retry is exactly retry 0 — one normalization, used everywhere.

        Kept as a derived property rather than a defaulted field so the authored
        document keeps the distinction (absent vs explicit 0) while every consumer
        sees one value; the two forms compile to identical output.
        """
        return 0 if self.retry is None else self.retry.count

    @model_validator(mode="after")
    def _try_catch_rules(self) -> "TryCatchNodeV1":
        steps = self.try_body.steps
        kinds = [step.kind for step in steps]
        if self.scope == "connector":
            # The verified target-local topology: optional property preparation,
            # then exactly the one call being protected.
            if kinds[-1] != "connector_call":
                raise _error_scope_error(
                    "a connector-scoped try body must end with the connector_call it protects"
                )
            if kinds.count("connector_call") != 1:
                raise _error_scope_error(
                    "a connector-scoped try body protects exactly one connector_call"
                )
            for kind in kinds[:-1]:
                if kind not in ("set_ddp", "set_dpp"):
                    raise _error_scope_error(
                        "a connector-scoped try body may contain only set_ddp/set_dpp "
                        "steps before the connector_call it protects"
                    )
        else:  # "process"
            if kinds[0] != "connector_call":
                raise _error_scope_error(
                    "a process-scoped try body must begin with the connector_call that "
                    "produces the flow's documents"
                )
        return self


#: The ProcessIR v1 control-nesting bound (#141).
#:
#: Depth counts ONLY ``branch``/``decision`` nodes on one authored root-to-leaf
#: path: the outermost control is depth 1, a control used as its body terminal is
#: depth 2, and a third is rejected. Linear nodes, ConnectorCall, ProcessCall,
#: Stop, Target, CachePut and Exception do not increment it.
#:
#: This is a COMPILER bound chosen on test cost, NOT a Boomi platform limit — the
#: platform imposes no observed cap and real production processes exceed this one
#: (`.codex/plans/issue-141-live-captures.md` §2.1 records a Decision chain six
#: deep inside a single Branch leg). Raising it multiplies the arm/leg x
#: connector-dataflow x cache x terminal test matrix with no demonstrated
#: authoring need; a later issue may raise it deliberately.
PROCESS_IR_V1_MAX_CONTROL_DEPTH = 2


# ---------------------------------------------------------------------------
# Root sequence
# ---------------------------------------------------------------------------

ProcessNodeV1 = Annotated[
    Union[
        SourceEndpointV1,
        TargetEndpointV1,
        ConnectorCallNodeV1,
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
        TryCatchNodeV1,
        ExceptionNodeV1,
        StopNodeV1,
        ReturnDocumentsNodeV1,
    ],
    Field(discriminator="kind"),
]

# ``try_catch`` is deliberately ABSENT here: this set widens the LEGACY
# source/target flow's terminal vocabulary, and #142 adds no legacy dialect. A
# Try/Catch reaches the root only through the two placements its own rules
# define (control-only root, or terminal of a connector_call sequence).
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
      ``target``+``stop``, a standalone ``return_documents`` terminal (the
      legacy Return Documents path never emits the target), or a terminal
      control (``branch``/``decision``/``exception``);
    - a process-call root is EXACTLY one ``process_call`` and nothing else: the
      call ends its own path, so no trailing ``stop``/``return_documents`` and no
      second call may follow it (mixed connector execution stays
      capability-gated, and keeps its own diagnostic);
    - a CONTROL-ONLY root (#141) is exactly one ``branch``/``decision`` and
      nothing else;
    - ``cache_put`` must be immediately followed by a stream-replacing cache
      read (never by the target/terminal).
    """

    kind: Literal["sequence"]
    steps: List[ProcessNodeV1] = Field(..., min_length=1)

    @model_validator(mode="after")
    def _sequence_rules(self) -> "SequenceNodeV1":
        kinds = [step.kind for step in self.steps]

        # CONTROL-ONLY root (#141), checked FIRST and matched EXACTLY so no
        # existing payload can reach it: every pre-#141 root either starts with
        # `source`/`connector_call`/`process_call` or has more than one step, and
        # a lone `[branch]`/`[decision]` was rejected outright before. It models
        # the live `start -> branch` shape (capture §2.2), which ProcessIR could
        # not represent at all, and it is what makes a ProcessCall-only leg's
        # root-to-leaf path genuinely connector-free — so
        # `process_call_connector_mixing` stays honestly gated.
        if len(kinds) == 1 and kinds[0] in ("branch", "decision"):
            return self

        # CONTROL-ONLY try_catch root (#142). Same exact-match discipline: a lone
        # `[try_catch]` was rejected outright before, so no existing payload can
        # reach it. Only PROCESS scope may stand alone — a connector scope
        # protects a DOWNSTREAM call, which by definition needs something
        # upstream of it, and allowing it here would make the source-isolation
        # rule vacuous rather than enforced.
        if len(kinds) == 1 and kinds[0] == "try_catch":
            if self.steps[0].scope != "process":
                raise _error_scope_error(
                    "only a process-scoped try_catch may be the sole root step — a "
                    "connector scope protects a downstream call and must follow one"
                )
            return self

        for i, kind in enumerate(kinds):
            if kind == "source" and i != 0:
                raise _cardinality_error("source may appear only as the first step")

        # A control node anywhere but the final position is a CONTINUATION
        # request (#141). Reported with its own capability code rather than the
        # generic cardinality one: the caller has not miscounted a list, they
        # have asked for a feature ProcessIR v1 deliberately does not emit.
        for i, kind in enumerate(kinds[:-1]):
            if kind in ("branch", "decision"):
                raise _continuation_error(
                    "no step may follow a branch or decision — control nodes are "
                    "terminal fan-out in ProcessIR v1 "
                    "(continuation_after_branch_or_decision is gated)"
                )
            # #142: same rule, its own message. A Try/Catch forks into two
            # independently-terminating paths and emits no join, so a step after
            # it has no path to be on.
            if kind == "try_catch":
                raise _continuation_error(
                    "no step may follow a try_catch — both its paths terminate "
                    "independently and ProcessIR v1 emits no join"
                )

        if "process_call" in kinds:
            # PROCESS-CALL ROOT (#141, amended by #175). A call ends its path, so
            # the only supported root shape is the EXACT SINGLETON — matched
            # exactly, like the control-only roots above. Before #175 this branch
            # required a trailing stop/return_documents and accepted a chain; both
            # emitted a call wired onward with no return path declared, which the
            # platform does not honour.
            verdict = process_call_root_verdict(kinds)
            if verdict is None:
                return self
            reason, at, message = verdict
            if reason == PLACEMENT_ROOT_CONNECTOR_MIXING:
                raise _capability_error(message)
            raise _return_path_binding_error(message, at=at)
        # Connector-call flow (#140). Checked BEFORE the source/target branch so
        # the two legacy branches above and the legacy branch below keep their
        # exact behaviour on every payload that contains no connector_call.
        if "connector_call" in kinds:
            if "source" in kinds or "target" in kinds:
                raise _capability_error(
                    "a connector_call sequence may not also author the legacy source/target "
                    "endpoint placeholders — author every call as a connector_call"
                )
            if kinds[0] != "connector_call":
                raise _cardinality_error(
                    "a connector_call sequence must start with a connector_call"
                )
            # #141 widened this terminal set with ``branch``/``decision``. The
            # legacy connector flow below has ALWAYS admitted a terminal control,
            # and without the same allowance here the issue's own acceptance
            # criterion is unbuildable: a divergent fixture whose sibling legs run
            # different connector families needs one shared entry call and then a
            # fan-out, which is exactly the live shape (capture §2.1 — one entry,
            # then a Branch whose legs differ). The alternative is duplicating the
            # entry call into every leg, which is not the same process.
            #
            # ``exception`` is deliberately NOT added: it is a legacy terminal
            # control with no #141 construct behind it, and widening it here would
            # be scope this issue has no evidence for.
            #
            # #142 widened it again with ``try_catch``. This is the CONNECTOR
            # SCOPE placement: one upstream call produces the documents, then the
            # Try/Catch protects the downstream call. That upstream call is
            # exactly what keeps the source OUTSIDE the retried region, so this
            # placement is the one where a positive retry can be safe at all.
            if kinds[-1] not in (
                "stop",
                "return_documents",
                "branch",
                "decision",
                "try_catch",
            ):
                raise _cardinality_error(
                    "a connector_call sequence must end in a stop, return_documents, "
                    "branch, decision, or try_catch terminal"
                )
            # Only a CONNECTOR scope may terminate a call sequence; a process
            # scope owns the whole flow and must be the sole root step.
            if kinds[-1] == "try_catch" and self.steps[-1].scope != "connector":
                raise _error_scope_error(
                    "only a connector-scoped try_catch may terminate a connector_call "
                    "sequence — a process scope must be the sole root step"
                )
            body = kinds[:-1]
            for kind in body:
                if kind not in ("connector_call", "map_ref"):
                    raise _capability_error(
                        "a connector_call sequence may contain only connector_call and "
                        "map_ref steps before its terminal"
                    )
            # Every map must be BRACKETED by calls. A trailing or doubled map has
            # no following call, so the map's destination profile could not be
            # checked against anything — and an unbounded-on-one-side map is
            # exactly the profile-continuity hole this node kind exists to close.
            for i, kind in enumerate(body):
                if kind != "map_ref":
                    continue
                if i + 1 >= len(body) or body[i + 1] != "connector_call":
                    raise _cardinality_error(
                        "a map_ref in a connector_call sequence must be immediately "
                        "followed by a connector_call"
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
        # a linear connector flow is target+stop, OR a standalone Return
        # Documents terminal — with return_documents enabled the builder emits
        # ONLY `returndocuments` after the sequence (_target_terminal_entries);
        # the configured legacy target is dead and is not represented in IR.
        if body[-1] == "stop":
            if len(body) < 2 or body[-2] != "target":
                raise _cardinality_error(
                    "a stop terminal must be immediately preceded by the target endpoint"
                )
            linear = body[:-2]
        elif body[-1] == "return_documents" or body[-1] in _ROOT_CONTROL_TERMINAL_KINDS:
            linear = body[:-1]
        elif body[-1] == "target":
            raise _cardinality_error(
                "the target endpoint must be immediately followed by a stop terminal"
            )
        else:
            raise _cardinality_error(
                "a connector-flow sequence must end in target+stop, return_documents, "
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


def _control_depth(node: Any) -> int:
    """Deepest chain of ``branch``/``decision`` nodes rooted at ``node``.

    Counts control nodes only; every other kind contributes 0. Recursion follows
    the two authored body shapes — a Branch's legs and a Decision's two arms —
    and looks at the ``terminal`` slot only, because a control node can never be
    a ``step`` (a step is followed by something on its own path, and a control
    terminalizes its path).
    """
    kind = getattr(node, "kind", None)
    if kind == "branch":
        return 1 + max((_control_depth(leg.terminal) for leg in node.legs), default=0)
    if kind == "decision":
        return 1 + max(
            _control_depth(node.true_arm.terminal),
            _control_depth(node.false_arm.terminal),
        )
    return 0


#: Node kinds that execute a connector. A ``process_call`` may not share a
#: root-to-leaf path with any of them while ``process_call_connector_mixing`` is
#: gated.
_CONNECTOR_KINDS = frozenset({"source", "target", "connector_call"})


class ProcessIRV1(_ProcessIRBase):
    """The semantic root: exactly one per authored process (ADR-001 §3)."""

    version: Literal["1"]
    body: SequenceNodeV1

# ---------------------------------------------------------------------------
# Capability manifest (published, immutable — not an authored field)
# ---------------------------------------------------------------------------

PROCESS_IR_V1_CAPABILITIES: Mapping[str, str] = MappingProxyType(
    {
        # #140 M12.5. ``mixed_connector_execution`` was OVERLOADED across two
        # documents: ADR-001 §8 lists it as "multiple connector calls per path"
        # (which #140 ships), while PROCESS_IR_V1 §3's sequence rules used the
        # same name for mixing ``process_call`` steps with connector execution
        # (which #140 does NOT ship). Rather than silently redefine the flag to
        # whichever meaning happened to still be gated, the two constructs now
        # have two names.
        "generalized_connector_call": "supported",  # #140
        "mixed_connector_execution": "supported",  # #140 — many calls per path
        # Still GATED after #141 and #175: ProcessCall and connector execution may
        # not share one root-to-leaf path. #175 admits ProcessCall as the TERMINAL
        # of a Branch leg / Decision TRUE arm (empty step prefix), reachable only
        # under a control-only root — so no path ever mixes the two. Sibling legs
        # are independent paths, not a mix. The terminal-slot move is why the gate
        # is now enforced against the terminal as well as the steps.
        "process_call_connector_mixing": "gated",  # process_call x connector
        "connector_call_in_control_body": "supported",  # #141
        # #175 M12 defect slice. A process call ends its path: the platform
        # projects a call's outbound connection from the CALLED process's
        # return-document shapes, so a call whose child returns nothing has no
        # outgoing edge. The terminal form is what V1 emits; binding a RETURNING
        # child's return paths needs the child's compiled shapes (a cross-component
        # late binding) and is gated.
        "terminal_process_call": "supported",  # #175
        "process_call_return_path_binding": "gated",  # #175
        "continuation_after_branch_or_decision": "gated",  # #141 — terminal fan-out only
        "rich_branch_decision_bodies": "supported",  # #141
        "scoped_try_catch": "supported",  # #142
        "bounded_retry": "supported",  # #142 — 0-5, the platform's own bound
        "typed_idempotency_evidence": "supported",  # #142
        # #142 M12.7. Three UNSUPPORTED rows below mean "never", not "not yet" —
        # the same sense as ``caller_authored_cfg_edges``. Marking them gated
        # would promise research that cannot conclude:
        #   * error-type/error-code lists have NO representation on the wire at
        #     all — the emitted error-handling shape carries exactly two settings,
        #     an all-errors flag and a retry count (capture §G2);
        #   * the retry wait schedule is fixed by the platform and has no
        #     authorable field (capture §G1);
        #   * no queue component exists to model, and creating topology is
        #     explicitly out of scope (capture §G5).
        "catch_error_type_lists": "unsupported",  # #142
        "retry_backoff_authoring": "unsupported",  # #142
        "queue_topology": "unsupported",  # #142
        # GATED (a real "not yet"), each blocked on a different missing thing:
        #   * failure-trigger selection: semantics are FULLY known, but the shared
        #     renderer fixes the all-errors form, and changing it would alter
        #     already-shipped output (capture §G2/§G3). A deliberate V1 surface
        #     omission, not an unknown.
        #   * write retry safety: no authoritative classification exists for any
        #     stock write action, so none ships as replay-safe (capture §G4).
        #   * listener error scope: the fused listener start rejects reliability
        #     composition today.
        #   * nested try_catch: composition rewrites the outer step's effective
        #     error selection, adjacency-dependently (capture §G6).
        "catch_failure_trigger_selection": "gated",  # #142
        # RENAMED by #146 from ``verified_write_retry_safety``. The old spelling
        # embedded the connector model's own field name, which the served
        # surface may not carry (tests/test_process_ir_compiler_surface.py::
        # FORBIDDEN_NAMES) — and this row is now projected into the public
        # authoring contract. "replay safety" is the public term throughout.
        # Safe to rename: the manifest key had never been served.
        "verified_write_replay_safety": "gated",  # #142
        "listener_error_scope": "gated",  # #142
        "nested_try_catch": "gated",  # #142
        "keyed_cache": "gated",  # no live-captured wire shape (#119 census)
        "definedparameter_property_source": "gated",  # no verified wire shape
        # ONE authority for join AND merge. The served contract exposes "merge"
        # as a display ALIAS of this row rather than a second row: two names for
        # one construct is how a state ends up updated in one place and stale in
        # the other, which is the drift #146 exists to remove.
        "joins": "gated",
        "loops": "gated",
        "caller_authored_cfg_edges": "unsupported",
        "xml_or_layout_or_shape_ids": "unsupported",
        "secret_values": "unsupported",
        # #146 amendment. Two rows added so the served authoring contract can
        # PROJECT a state for parallelism instead of inventing one. A caller who
        # asks "do Branch legs run at once?" needs an answer with an authority
        # behind it, and until these existed the honest answer was "the manifest
        # does not say" — which the projection would then have had to decide on
        # its own, exactly the second-authority split ADR-001 §6 forbids.
        #
        # UNSUPPORTED, not gated, and the distinction is deliberate: gated means
        # "not yet", and neither of these is pending research. Branch legs are
        # ordered and sequential by construction — the ordering is what makes
        # execution-scoped state visible from an earlier leg to a later one, so
        # concurrent legs would not be the same feature with more speed, they
        # would be a different semantics. Flow control likewise exposes no
        # authorable parallel field at all.
        #
        # Both rows describe ProcessIR v1, NOT the Boomi platform: the platform
        # has parallel settings this contract does not author.
        "parallel_branch_execution": "unsupported",
        "flow_control_parallel_chunks": "unsupported",
    }
)


# ---------------------------------------------------------------------------
# Parse entry point with deterministic diagnostics
# ---------------------------------------------------------------------------

# Extra keys that map to the NAMED capability gate instead of a generic
# unknown-field (mirrors the legacy cache_get keyed-retrieval gate).
_GATED_EXTRA_KEYS = frozenset({"doc_cache_index", "cache_key_values", "load_all_documents"})
_GATED_UNION_TAGS = frozenset({"definedparameter"})

# #142: extras on a try_catch / connector_call that name a REAL construct this
# version does not author. Without this set they would all report as generic
# unknown fields, which sends a caller to delete a typo rather than to the
# manifest row that explains whether the thing is gated or impossible — and
# "unsupported queue/topology requests return an explicit capability gap" is an
# acceptance criterion, not a nicety.
_GATED_TRY_CATCH_EXTRA_KEYS = frozenset(
    {
        "catch_all",
        "failure_trigger",
        "error_types",
        "error_codes",
        "backoff",
        "retry_backoff",
        "listener_retry",
        "queue_ref",
        "dlq_ref",
        "idempotency_key",
    }
)

# #141: authored loc segments that mean "inside a control body". Used to tell a
# known kind in the WRONG SLOT (a body capability failure) from a genuinely
# unknown discriminator. #142 adds the two Try/Catch body slots.
_BODY_LOC_SEGMENTS = frozenset(
    {"legs", "true_arm", "false_arm", "try_body", "catch_body"}
)

#: Every globally valid node ``kind``, DERIVED from the root union so it cannot
#: drift from the vocabulary it is meant to describe.
_NODE_KIND_TAGS = frozenset(_kinds_of(ProcessNodeV1))

# Discriminator tag values that pydantic injects into error locs for tagged
# unions; stripped so pointers address the AUTHORED JSON.
_DISCRIMINATOR_TAGS = frozenset(
    {
        "sequence",
        "source",
        "target",
        "connector_call",
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
        "try_catch",
        "verified_action",
        "key_reference",
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

# Every remediation below is SERVED — it reaches a caller through
# ``build_integration(action="plan"|"compile")`` and through the typed request
# rejection. So none of them may point at a repository artifact: a
# ``docs/architecture/`` page and the capability manifest's Python name are both
# unfetchable through any MCP tool, and a caller sent there is sent nowhere.
# They cite ``process_ir_authoring`` entry ids instead, which resolve through
# ``get_schema_template``.
_REMEDIATION = {
    # ``category='node'`` was WRONG and shipped: a node entry is categorised by
    # what it does (control, connector, state, terminal, ...), so there is no
    # "node" category to filter on and the remediation was unfollowable. The
    # facets a bare call returns list every valid node_kind, and the code's own
    # entry always resolves — neither needs the caller to guess a value.
    PROCESS_IR_SCHEMA_UNKNOWN_NODE: (
        "Use one of the ProcessIRV1 node kinds published by "
        "get_schema_template(schema_name='ProcessIRV1'). A bare "
        "get_schema_template(schema_name='process_ir_authoring') lists every "
        "valid node_kind in its facets; pass one of those values as node_kind to "
        "read that kind's authoring rules."
    ),
    PROCESS_IR_SCHEMA_UNKNOWN_FIELD: (
        "Remove the unknown field — ProcessIRV1 nodes are strict and reject extras."
    ),
    # Cites its OWN entry id rather than a node_kind the caller has to derive.
    # Two earlier wordings failed here: one shipped a literal '<kind>'
    # placeholder, and the replacement told the caller to read the kind "from
    # the path" — but the path is a JSON pointer of field names and indices, and
    # the kind is recoverable from it in only one of the three shapes this code
    # reports. An exact id needs no derivation and always resolves.
    # The PRIMARY pointer must deliver something the caller does not already
    # have. An earlier wording cited this code's own contract entry, whose only
    # prose was this same sentence — a pointer that resolves and teaches nothing
    # is not better than one that does not resolve. The bounds and the step
    # ordering are structural, so the schema is where they actually live.
    PROCESS_IR_SCHEMA_INVALID_CARDINALITY: (
        "Fix the list bound or step ordering at the referenced path. The exact "
        "bounds are in the node's own definition in "
        "get_schema_template(schema_name='ProcessIRV1'); the behavioural rules "
        "for the node kind at that path are at get_schema_template("
        "schema_name='process_ir_authoring', category='placement')."
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
        "The referenced construct is capability-gated or unsupported in ProcessIR v1. "
        "Fetch its published state with "
        "get_schema_template(schema_name='process_ir_authoring', category='capability') — "
        "'gated' means not yet, 'unsupported' means never, and only the latter needs a "
        "different design."
    ),
    PROCESS_IR_SCHEMA_BRANCH_CARDINALITY: (
        "A Branch must declare between 2 and 25 legs (the platform's documented bound)."
    ),
    PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED: (
        "Move the steps that followed the branch/decision into every leg or arm — "
        "ProcessIR v1 emits no continuation after a control node."
    ),
    PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY: (
        "Use a node kind this body slot admits. The admitted set for each slot is "
        "published at "
        "get_schema_template(schema_name='process_ir_authoring', category='placement'); "
        "a kind absent from a slot is rejected, so absence is the rule, not an omission."
    ),
    PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED: (
        "Author the process call as the TERMINAL of its path and remove whatever "
        "followed it — a call whose child returns no documents ends the path, so a "
        "trailing stop is not needed and cannot be emitted. A call that must hand "
        "control onward needs its child's return-document shapes bound to it; that "
        "capability is published as process_call_return_path_binding at "
        "get_schema_template(schema_name='process_ir_authoring', category='capability')."
    ),
    PROCESS_IR_SEMANTIC_NESTING_LIMIT: (
        "Reduce Branch/Decision nesting to at most "
        "PROCESS_IR_V1_MAX_CONTROL_DEPTH levels, or move the deeper routing into a "
        "subprocess. This is a ProcessIR v1 compiler bound, not a Boomi platform limit."
    ),
    PROCESS_IR_SCHEMA_RETRY_COUNT: (
        "Use an integer from 0 through 5 for the retry count (the platform's own "
        "documented bound), or omit retry entirely for no retry."
    ),
    PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED: (
        "Use a supported error scope in its verified placement: a process scope as "
        "the sole root step, or a connector scope as the last step of a "
        "connector-call sequence. See "
        "get_schema_template(schema_name='process_ir_authoring', node_kind='try_catch')."
    ),
    PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED: (
        "End the catch body with a stop, an exception, or a staging cache_put — "
        "every caught document must reach a terminal."
    ),
}

_CUSTOM_ERROR_CODES = {
    "process_ir_reference_invalid_format": PROCESS_IR_REFERENCE_INVALID_FORMAT,
    "process_ir_capability_unsupported": PROCESS_IR_CAPABILITY_UNSUPPORTED,
    "process_ir_schema_invalid_cardinality": PROCESS_IR_SCHEMA_INVALID_CARDINALITY,
    # #141
    "process_ir_semantic_control_continuation_unsupported": (
        PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED
    ),
    "process_ir_capability_node_not_allowed_in_body": (
        PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY
    ),
    "process_ir_semantic_nesting_limit": PROCESS_IR_SEMANTIC_NESTING_LIMIT,
    # #142
    "process_ir_capability_error_scope_unsupported": (
        PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED
    ),
    "process_ir_semantic_catch_unterminated": PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED,
    # #175
    "process_ir_capability_process_call_return_path_binding_unsupported": (
        PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED
    ),
}

_MESSAGES = {
    PROCESS_IR_SCHEMA_UNKNOWN_NODE: "unknown node kind or discriminator tag",
    PROCESS_IR_SCHEMA_UNKNOWN_FIELD: "unknown field on a strict ProcessIRV1 node",
    PROCESS_IR_SCHEMA_INVALID_CARDINALITY: "list bound or step-ordering rule violated",
    PROCESS_IR_SCHEMA_VERSION_UNSUPPORTED: "unsupported or missing ProcessIR document version",
    PROCESS_IR_SCHEMA_INVALID: "value does not match the strict ProcessIRV1 schema",
    PROCESS_IR_REFERENCE_INVALID_FORMAT: "malformed opaque component reference",
    PROCESS_IR_CAPABILITY_UNSUPPORTED: "capability-gated or unsupported construct requested",
    PROCESS_IR_SCHEMA_BRANCH_CARDINALITY: "branch leg count is outside the 2-25 bound",
    PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED: (
        "continuation after a branch or decision is not supported"
    ),
    PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY: (
        "this node kind is not admitted in this control-body slot"
    ),
    PROCESS_IR_SEMANTIC_NESTING_LIMIT: "control nesting exceeds the ProcessIR v1 depth bound",
    PROCESS_IR_SCHEMA_RETRY_COUNT: "retry count is outside the 0-5 bound or is not an integer",
    PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED: (
        "unsupported error scope or error-scope placement"
    ),
    PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED: "the catch body does not reach a terminal",
    PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED: (
        "a process call may not be followed by another node in ProcessIR v1"
    ),
}


def _diagnostic(code: str, path: Tuple[Any, ...], *, message: Optional[str] = None) -> ProcessIRDiagnostic:
    return ProcessIRDiagnostic(
        code=code,
        path=_json_pointer(path),
        message=message or _MESSAGES[code],
        remediation=_REMEDIATION[code],
    )


# The non-list union fields whose loc is followed by a discriminator tag.
# ``idempotency`` (#142) is a non-list tagged union field, so its tag element is
# stripped the same way — a diagnostic must point at ``/idempotency``, not at
# ``/idempotency/key_reference``, which does not exist in the authored JSON.
_UNION_FIELD_NAMES = frozenset({"left", "right", "terminal", "idempotency"})


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
        # #175: a validator that knows WHICH node offended says so through the
        # error context, because a model-level validator's own loc is the model.
        # The segments are our own literals and integers — never authored values
        # — so appending them cannot leak payload into a pointer.
        offending = (error.get("ctx") or {}).get("offending_path") or ()
        if offending:
            path = _loc_to_path(loc + tuple(offending))
        return _diagnostic(code, path, message=message)

    # #142: the retry bound and the scope literal get their own codes, checked
    # BEFORE the generic tails below so a range/type failure on `/retry/count`
    # never falls through to the catch-all cardinality or invalid-value code.
    # ``_pointer_endswith`` works on the AUTHORED pointer, so a discriminator tag
    # in the raw loc cannot hide the match.
    pointer = _json_pointer(path)
    if pointer.endswith("/retry/count"):
        # Covers ge/le violations, non-integer types, AND a `retry` object with no
        # `count` at all (pydantic reports that as `missing` at .../retry/count).
        return _diagnostic(PROCESS_IR_SCHEMA_RETRY_COUNT, path)
    if err_type == "literal_error" and pointer.endswith("/scope"):
        return _diagnostic(PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED, path)
    if err_type == "missing" and (
        pointer.endswith("/catch_body") or pointer.endswith("/catch_body/terminal")
    ):
        return _diagnostic(PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED, path)

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
        # #142: a named construct this version does not author, on an error-scope
        # or connector-call node. Report the capability gate, not "unknown field".
        #
        # Matched against the IMMEDIATE OWNER — the discriminator tag directly
        # preceding the offending key — not against the whole loc. A membership
        # test would also fire for a node NESTED inside a try_catch: a catch-body
        # `message` carrying a stray `backoff` has `try_catch` in its loc as an
        # ancestor, and would then be told to consult the capability manifest
        # about a field that is simply an unknown field on a Message.
        #
        # ``retry`` is an owner too, and NOT an afterthought: the natural way to
        # ask for a backoff is ``retry: {"count": 1, "backoff": 10}``, whose
        # owner is the retry policy object rather than the handler. Recognising
        # only the handler would send the single most likely authoring attempt to
        # the generic unknown-field code, which is precisely the path that most
        # needs to name the capability gate. ``retry`` is unambiguous here — it
        # exists only as a Try/Catch's policy object.
        if (
            isinstance(last, str)
            and last in _GATED_TRY_CATCH_EXTRA_KEYS
            and len(loc) >= 2
            and loc[-2] in ("try_catch", "connector_call", "retry")
        ):
            return _diagnostic(
                PROCESS_IR_CAPABILITY_UNSUPPORTED,
                path,
                message=(
                    "this error-handling construct is capability-gated or unsupported "
                    "in ProcessIR v1"
                ),
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
        # #141: a KNOWN node kind rejected inside a control body is a body-slot
        # capability failure, not an unknown node. Telling a caller that
        # ``process_call`` is an "unknown node kind" when it is a documented kind
        # they used in the wrong slot sends them to fix the wrong thing. A
        # genuinely unknown tag keeps the unknown-node code.
        # #142: ``idempotency`` is a tagged union that is NOT a body slot, so a
        # failure beneath it must be excluded here. Its tag vocabulary
        # (``verified_action``/``key_reference``) is disjoint from the node kinds,
        # but a caller who writes ``{"kind": "message"}`` there hits a tag that IS
        # a node kind while sitting inside a try/catch body loc — and would be
        # told "message is not admitted in this control-body slot", which is both
        # the wrong diagnosis and, since Message IS admitted there, self-
        # contradictory.
        if (
            tag in _NODE_KIND_TAGS
            and "idempotency" not in loc
            and any(part in _BODY_LOC_SEGMENTS for part in loc)
        ):
            # #175: ``process_call`` in a body STEP slot is the pre-#175 authoring
            # of a continuation, not a caller reaching for a kind this slot never
            # admitted. It IS admitted here — in the terminal slot — so the
            # generic body-placement message ("not admitted in this slot") would
            # be actively misleading, and its remediation would send the caller to
            # the placement table to discover the kind is listed after all. Give
            # it the dedicated code, whose remediation says the actual fix: make
            # the call the terminal and drop what followed it.
            #
            # Checked here rather than in a `mode="before"` validator because the
            # discriminated union rejects the tag before any model validator runs,
            # so this translation is the only place that still knows the tag.
            #
            # Scoped to the bodies that actually admit a terminal call — a Branch
            # leg and a Decision TRUE arm. The INNERMOST enclosing body decides,
            # so this reads the LAST body segment rather than asking whether one
            # appears anywhere: a Branch nested under a FALSE arm is still a
            # Branch leg. A Decision FALSE arm and the Try/Catch bodies admit no
            # call in any slot, so they keep the body-placement code, which for
            # them is the true diagnosis.
            innermost = next(
                (part for part in reversed(loc) if part in _BODY_LOC_SEGMENTS), None
            )
            if (
                tag == "process_call"
                and loc[-2:-1] == ("steps",)
                and innermost in ("legs", "true_arm")
            ):
                return _diagnostic(
                    PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED,
                    path,
                    message=PROCESS_CALL_STEP_FORM_MESSAGE,
                )
            return _diagnostic(
                PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY,
                path,
                message="this node kind is not admitted in this control-body slot",
            )
        return _diagnostic(PROCESS_IR_SCHEMA_UNKNOWN_NODE, path)

    if err_type in ("too_short", "too_long"):
        # #141: a Branch's 2-25 leg bound gets its own code. Keyed on the LAST
        # authored token being ``legs`` so only the Branch bound is re-pointed —
        # every other list bound (``steps``, ``source_values``, ...) keeps
        # PROCESS_IR_SCHEMA_INVALID_CARDINALITY exactly as before.
        if last == "legs":
            return _diagnostic(PROCESS_IR_SCHEMA_BRANCH_CARDINALITY, path)
        return _diagnostic(PROCESS_IR_SCHEMA_INVALID_CARDINALITY, path)

    return _diagnostic(PROCESS_IR_SCHEMA_INVALID, path)


def _walk_controls(node: Any, path: Tuple[Any, ...], depth: int, connector_above: bool) -> None:
    """One walk enforcing both whole-document control rules, with exact paths.

    * control depth <= ``PROCESS_IR_V1_MAX_CONTROL_DEPTH`` on any root-to-leaf path;
    * no ``process_call`` sharing a path with a connector.

    Both are PATH properties, so both need the walk; doing them together keeps a
    single traversal and one definition of "on this path".
    """
    kind = getattr(node, "kind", None)
    # ``try_catch`` (#142) is deliberately NOT walked here, and that is a
    # consequence of its unions rather than an oversight: it admits no control
    # node in either body and cannot nest, so it can neither deepen a control
    # chain nor put a ProcessCall on a connector's path. Both invariants this
    # walk enforces are therefore unreachable through it.
    if kind not in ("branch", "decision"):
        return
    depth += 1
    if depth > PROCESS_IR_V1_MAX_CONTROL_DEPTH:
        raise ProcessIRValidationError([
            _diagnostic(
                PROCESS_IR_SEMANTIC_NESTING_LIMIT,
                path,
                message=(
                    "branch/decision nesting exceeds the ProcessIR v1 maximum control "
                    "depth of {0} (a compiler bound, not a Boomi platform limit)".format(
                        PROCESS_IR_V1_MAX_CONTROL_DEPTH
                    )
                ),
            )
        ])

    if kind == "branch":
        bodies = [
            (leg.steps, leg.terminal, path + ("legs", index))
            for index, leg in enumerate(node.legs)
        ]
    else:
        bodies = [
            (node.true_arm.steps, node.true_arm.terminal, path + ("true_arm",)),
            (node.false_arm.steps, node.false_arm.terminal, path + ("false_arm",)),
        ]

    for steps, terminal, body_path in bodies:
        kinds = [step.kind for step in steps]
        # #175: the call moved to the TERMINAL slot, so the ancestor half of the
        # mixing gate has to look there too. Checking only ``steps`` would leave
        # `source -> branch -> leg(terminal=process_call)` accepted — a connector
        # genuinely upstream of a call on the same root-to-leaf path, which is
        # exactly what this gate exists to refuse.
        if connector_above:
            if "process_call" in kinds:
                offending = body_path + ("steps", kinds.index("process_call"))
            elif getattr(terminal, "kind", None) == "process_call":
                offending = body_path + ("terminal",)
            else:
                offending = None
            if offending is not None:
                raise ProcessIRValidationError([
                    _diagnostic(
                        PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY,
                        offending,
                        message=(
                            "a process_call may not share a root-to-leaf path with a "
                            "connector step — a connector runs upstream of this body "
                            "(process_call_connector_mixing is gated)"
                        ),
                    )
                ])
        connector_here = (
            connector_above
            or any(k in _CONNECTOR_KINDS for k in kinds)
            or getattr(terminal, "kind", None) in _CONNECTOR_KINDS
        )
        _walk_controls(terminal, body_path + ("terminal",), depth, connector_here)


def _check_whole_document_rules(ir: "ProcessIRV1") -> None:
    root_kinds = [step.kind for step in ir.body.steps]
    connector_at_root = any(k in _CONNECTOR_KINDS for k in root_kinds)
    for index, step in enumerate(ir.body.steps):
        _walk_controls(step, ("body", "steps", index), 0, connector_at_root)


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
        ir = ProcessIRV1.model_validate(payload)
    except ValidationError as exc:
        diagnostics = [_translate_pydantic_error(err) for err in exc.errors()]
        raise ProcessIRValidationError(diagnostics) from None

    # #141: whole-document rules that a per-model validator cannot state with a
    # useful pointer. A pydantic ``model_validator`` on ``ProcessIRV1`` attaches
    # its error to the MODEL, so a nesting or mixing failure anywhere in the tree
    # reported the document ROOT — true, and useless for finding the offending
    # node. Run here instead, walking with the authored path in hand.
    _check_whole_document_rules(ir)
    return ir


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


# ---------------------------------------------------------------------------
# Sanitized accessors for the #146 authoring projection
# ---------------------------------------------------------------------------
#
# The projection in ``boomi_mcp.authoring.process_ir_projection`` reads these to
# build the served ``process_ir_authoring`` contract. They return plain immutable
# data — never a translator, a discriminator table, or a pydantic internal — so
# the projector cannot reach anything the schema itself does not already
# publish, and so a change here shows up in ``compiler_revision``.


def process_ir_v1_node_kinds() -> Tuple[str, ...]:
    """Every authorable node kind, plus the ``sequence`` root, sorted.

    DERIVED from the root union rather than listed, for the same reason
    ``_NODE_KIND_TAGS`` is: a hand-kept list is a second vocabulary that drifts
    from the one the models actually accept. ``sequence`` is added because it is
    a real authored kind that is not a member of ``ProcessNodeV1`` — it is the
    body, not a step — and a caller still has to author one.
    """
    return tuple(sorted(_NODE_KIND_TAGS | {"sequence"}))


def process_ir_v1_parse_diagnostic_specs() -> Tuple[Mapping[str, str], ...]:
    """(code, message, remediation) for every parse diagnostic, sorted by code.

    The messages and remediations here are STATIC strings selected by code —
    nothing is interpolated from an authored payload — which is what makes them
    safe to publish. A code with no remediation entry is reported with an empty
    string rather than omitted: a caller comparing the served set against the
    codes they actually receive must see the gap.
    """
    return tuple(
        MappingProxyType(
            {
                "code": code,
                "message": _MESSAGES.get(code, ""),
                "remediation": _REMEDIATION.get(code, ""),
            }
        )
        for code in sorted(set(_MESSAGES) | set(_REMEDIATION))
    )
