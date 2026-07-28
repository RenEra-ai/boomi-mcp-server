"""M12.9 (issue #144, epic #134): strict ``SystemTopologySpecV1`` planning models.

The capability-gated, planning-only topology authority ADR-001 §3 reserves for
#144: "it never mutates runtime state and never feeds the process compiler".
Where ``ProcessIRV1`` (#136) owns semantics *within* one process, topology owns
the relationships *between* processes and the platform resources they bind to —
ProcessCall, API-service routing, shared cache/property use, schedule binding,
and deployment-unit intent.

The models ship DARK: nothing at runtime constructs or consumes them, no MCP
tool or action is registered, and no existing schema changes. ADR-001 assigns
the MCP planning surface to #146.

Contract highlights
-------------------

- ``SystemTopologySpecV1(version="1", profile_ref=..., objects=[...], relations=[...])``
  is the root; every authored boundary is ``extra="forbid"``.
- Callers author exactly two things: typed topology objects/relations and opaque
  platform references. No credentials, endpoint configuration, raw component
  XML, CFG edges, ComponentPlan build dependencies, or free-form metadata.
- ``capability``, ``provenance``, ``evidence``, ``action`` and ``apply`` are
  DERIVED planner output (ADR-001 §6 authored-vs-derived). Authoring any of them
  is rejected — a payload cannot assert its own capability state, because the
  whole point of the gate is that evidence, not the caller, decides.
- Diagnostics carry a stable ``TOPOLOGY_*`` code (the shared ``boomi_mcp.errors``
  registry), an RFC 6901 JSON pointer into the authored payload, and static
  remediation text — never authored values, never raw Pydantic internals.
- Serialization is canonical (defaults expanded, keys sorted, compact
  separators) so golden JSON/schema tests are byte-stable.

Why relations carry ROLE-NAMED fields instead of generic endpoints
------------------------------------------------------------------
A generic ``(source, target)`` pair would make ``ProcessCall(A, B)`` and
``DeploymentBinding(unit, env)`` the same shape, so nothing but a convention
would stop a caller — or a later refactor — from putting an environment where a
runtime belongs. Naming the roles (``caller_process``/``callee_process``,
``process``/``runtime``) makes the endpoint matrix a type fact instead of a
documented expectation, which is what lets ``TOPOLOGY_REFERENCE_TYPE_MISMATCH``
be decided structurally.

Evidence bounds encoded here (see docs/architecture/SYSTEM_TOPOLOGY_V1.md)
-------------------------------------------------------------------------
The schema deliberately omits several fields the issue's prose gestures at,
because the live account does not support them:

- **No schedule content.** Every live schedule observed carries an empty
  ``schedules: []`` body, so cron/interval shape has zero evidence. A schedule
  object here is an intent marker; its identity comes from its binding.
- **No schedule-to-environment relation.** A schedule id is base64 of
  ``CPS{atomId}:{processId}`` — it binds a process to a RUNTIME, never to an
  environment. An environment-keyed schedule would be an invented field.
- **No account capability limits.** Nothing was captured, so nothing is modeled.
- **No queue/Event Streams semantics.** Zero queue components exist; both kinds
  are representable as declared intent and permanently blocked in V1.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Literal, Optional, Tuple, Union, get_args

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
)
from pydantic_core import PydanticCustomError
from typing_extensions import Annotated

from ..errors import (
    TOPOLOGY_SCHEMA_DUPLICATE_KEY,
    TOPOLOGY_SCHEMA_INVALID,
    TOPOLOGY_SCHEMA_INVALID_CARDINALITY,
    TOPOLOGY_SCHEMA_UNKNOWN_FIELD,
    TOPOLOGY_SCHEMA_UNKNOWN_OBJECT,
    TOPOLOGY_SCHEMA_UNKNOWN_RELATION,
    TOPOLOGY_SCHEMA_VERSION_UNSUPPORTED,
)

SYSTEM_TOPOLOGY_VERSION = "1"


# ---------------------------------------------------------------------------
# Secret / open-payload boundary (ADR-001 §11)
# ---------------------------------------------------------------------------

# Mirror of the ProcessIR secret list — a COPY, not an import, for the same
# reason process_ir.py copies it from the builder: these two model families are
# independent authorities and neither may become the other's dependency.
# tests/test_system_topology_models.py pins the shared prefix against
# process_ir._FORBIDDEN_SECRET_KEY_SUBSTRINGS so the two cannot drift silently.
_SECRET_KEY_SUBSTRINGS: Tuple[str, ...] = (
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

# #144 additions. These are the topology-specific leak channels the issue names:
# certificates, environment extension values, and raw connection properties.
_TOPOLOGY_SECRET_KEY_SUBSTRINGS: Tuple[str, ...] = (
    "certificate",
    "env_extension",
    "environment_extension",
    "profile_override",
    "connection_propert",  # matches connection_property / connection_properties
)

# Exact-match keys, NOT substrings. A substring rule on "config" would also
# reject a legitimate future "configuration_classification"-style structural
# field, and a substring rule on "xml" would reject "xmlns_role". Exact equality
# says precisely what is banned — an open bag of authored payload — without
# booby-trapping the namespace.
_FORBIDDEN_EXACT_KEYS: Tuple[str, ...] = (
    "config",
    "configuration",
    "metadata",
    "xml",
    "raw_xml",
    "component_xml",
    "extensions",
    # Derived planner output. Authoring these would let a payload assert its own
    # capability verdict, which is the one thing the gate exists to prevent.
    "capability",
    "provenance",
    "evidence",
    "action",
    "apply",
)


def _find_forbidden_key(
    payload: Any, _path: Tuple[Any, ...] = ()
) -> Optional[Tuple[Tuple[Any, ...], str]]:
    """Return ``(json path, reason)`` of the first forbidden key, or None.

    Two rules, checked in one pass:

    * secret-shaped keys — case-insensitive SUBSTRING match, same value-shape
      semantics as ``process_ir._find_secret_shaped_key`` (a match flags a
      non-empty string value or any container; empty strings and bare scalars
      are skipped, so a ``token: ""`` placeholder is not a leak);
    * open-payload keys — case-insensitive EXACT match, flagged regardless of
      value, because the objection to ``config: {}`` is the field's existence,
      not its current contents.
    """
    if isinstance(payload, dict):
        for key, value in payload.items():
            if isinstance(key, str):
                lowered = key.lower()
                if lowered in _FORBIDDEN_EXACT_KEYS:
                    return _path + (key,), TOPOLOGY_SCHEMA_UNKNOWN_FIELD
                if any(
                    sub in lowered
                    for sub in _SECRET_KEY_SUBSTRINGS + _TOPOLOGY_SECRET_KEY_SUBSTRINGS
                ):
                    if isinstance(value, str):
                        if value:
                            return _path + (key,), TOPOLOGY_SCHEMA_UNKNOWN_FIELD
                    elif isinstance(value, (dict, list)):
                        return _path + (key,), TOPOLOGY_SCHEMA_UNKNOWN_FIELD
                    continue
            found = _find_forbidden_key(value, _path + (key,))
            if found is not None:
                return found
    elif isinstance(payload, list):
        for i, item in enumerate(payload):
            found = _find_forbidden_key(item, _path + (i,))
            if found is not None:
                return found
    return None


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------


class SystemTopologyDiagnostic(BaseModel):
    """One deterministic parse/validation diagnostic (ADR-001 §7).

    ``path`` is an RFC 6901 JSON pointer into the AUTHORED payload; ``message``
    and ``remediation`` are static strings — authored values never appear.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    code: str
    path: str
    message: str
    remediation: str


class SystemTopologyValidationError(Exception):
    """Raised by :func:`parse_system_topology_v1` with sorted, secret-free diagnostics."""

    def __init__(self, diagnostics: List[SystemTopologyDiagnostic]) -> None:
        self.diagnostics: Tuple[SystemTopologyDiagnostic, ...] = tuple(
            sorted(diagnostics, key=lambda d: (d.path, d.code))
        )
        summary = "; ".join(
            f"{d.code} at {d.path or '<root>'}" for d in self.diagnostics
        )
        super().__init__(f"SystemTopologySpecV1 validation failed: {summary}")


def _pointer_escape(token: str) -> str:
    return token.replace("~", "~0").replace("/", "~1")


def _json_pointer(parts: Tuple[Any, ...]) -> str:
    return "".join(f"/{_pointer_escape(str(part))}" for part in parts)


# ---------------------------------------------------------------------------
# Base model + opaque references
# ---------------------------------------------------------------------------

# Fields safe to expose in reprs: discriminators and the version tag only.
# Every other field holds an authored reference, and a traceback is a log line.
_REPR_SAFE_FIELDS = frozenset({"kind", "version"})


class _SystemTopologyBase(BaseModel):
    """Shared strict base: unknown fields rejected, authored values repr-suppressed."""

    model_config = ConfigDict(extra="forbid")

    def __repr_args__(self):  # noqa: D105 — pydantic hook
        for key, value in super().__repr_args__():
            if key in _REPR_SAFE_FIELDS:
                yield key, value
            else:
                yield key, "..."


def _validate_opaque_token(value: str) -> str:
    """A non-blank, untrimmed, control-character-free opaque token.

    Control characters are rejected explicitly rather than left to a regex: a
    newline inside a key would break the one-finding-per-line shape every log
    consumer assumes, and a NUL would truncate the value in a C-backed sink.
    """
    if not value or value != value.strip():
        raise PydanticCustomError(
            "topology_reference_invalid_format",
            "value must be a non-blank string without surrounding whitespace",
        )
    if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in value):
        raise PydanticCustomError(
            "topology_reference_invalid_format",
            "value must not contain control characters",
        )
    return value


_REF_TOKEN_PREFIX = "$ref:"


def _validate_component_ref(value: str) -> str:
    """Opaque component reference: exact ``$ref:KEY`` token or literal id.

    The exact rule ``ProcessIRV1`` uses, copied for the same
    no-cross-authority-imports reason as the secret list. A test pins behavioral
    equality against ``process_ir._validate_component_ref`` for both forms.
    """
    _validate_opaque_token(value)
    if value.startswith(_REF_TOKEN_PREFIX):
        key = value[len(_REF_TOKEN_PREFIX) :]
        if not key or any(ch.isspace() for ch in key):
            raise PydanticCustomError(
                "topology_reference_invalid_format",
                "'$ref:' token must carry a non-empty, whitespace-free key",
            )
    return value


TopologyObjectKeyV1 = Annotated[
    str,
    AfterValidator(_validate_opaque_token),
    Field(description="Caller-chosen unique key for a topology object"),
]

TopologyRelationKeyV1 = Annotated[
    str,
    AfterValidator(_validate_opaque_token),
    Field(description="Caller-chosen unique key for a topology relation"),
]

OpaquePlatformRefV1 = Annotated[
    str,
    AfterValidator(_validate_opaque_token),
    Field(description="Opaque platform reference (profile, environment, runtime or resource id)"),
]

TopologyComponentRefV1 = Annotated[
    str,
    AfterValidator(_validate_component_ref),
    Field(description="Opaque component reference: exact '$ref:KEY' token or literal component id"),
]


# ---------------------------------------------------------------------------
# Objects
# ---------------------------------------------------------------------------


class ProcessObjectV1(_SystemTopologyBase):
    """A process participating in the topology.

    ``component_ref`` is either a ``$ref:KEY`` naming a ComponentPlan symbol
    (a process this plan will build) or a literal id (a process that exists).
    That distinction is load-bearing: only the ``$ref`` form can become an
    executable component prerequisite, and only the literal form requires
    parsed component XML to witness a ProcessCall.
    """

    kind: Literal["process"]
    key: TopologyObjectKeyV1
    component_ref: TopologyComponentRefV1


class ApiServiceObjectV1(_SystemTopologyBase):
    """An API Service Component (Boomi ``webservice``) exposing process routes."""

    kind: Literal["api_service"]
    key: TopologyObjectKeyV1
    component_ref: TopologyComponentRefV1


class DocumentCacheObjectV1(_SystemTopologyBase):
    """A shared Document Cache component."""

    kind: Literal["document_cache"]
    key: TopologyObjectKeyV1
    component_ref: TopologyComponentRefV1


class ProcessPropertyObjectV1(_SystemTopologyBase):
    """A shared Process Property component."""

    kind: Literal["process_property"]
    key: TopologyObjectKeyV1
    component_ref: TopologyComponentRefV1


class RuntimeObjectV1(_SystemTopologyBase):
    """A Boomi runtime (atom/molecule/cloud container).

    Present because a schedule binds to a RUNTIME, not an environment — the
    schedule id is base64 of ``CPS{atomId}:{processId}``.
    """

    kind: Literal["runtime"]
    key: TopologyObjectKeyV1
    runtime_ref: OpaquePlatformRefV1


class EnvironmentObjectV1(_SystemTopologyBase):
    """A Boomi environment.

    ``classification`` is optional on purpose. It is a real, readable platform
    field (``TEST``/``PROD``), but a profile may legitimately contain no PROD
    environment at all — one of the two live profiles observed has two TEST
    environments and no PROD. Requiring it would make a valid account
    unmodelable; authoring it opts into an equality check against discovery.
    """

    kind: Literal["environment"]
    key: TopologyObjectKeyV1
    environment_ref: OpaquePlatformRefV1
    classification: Optional[Literal["TEST", "PROD"]] = None


class ScheduleObjectV1(_SystemTopologyBase):
    """Declared intent that a process runs on a schedule.

    Carries NO cron/interval body, no desired active state, and no retry policy:
    every live schedule observed has an empty ``schedules: []`` array, so
    schedule *content* has no evidence to model. Its identity is supplied
    entirely by its :class:`ScheduleBindingRelationV1` — which is why an
    unbound schedule object is a cardinality error.
    """

    kind: Literal["schedule"]
    key: TopologyObjectKeyV1


class DeploymentUnitObjectV1(_SystemTopologyBase):
    """A non-executable grouping of deployment intent.

    Deliberately not a "package": creating a package is a mutation. Live reads
    establish that deployment records exist and can be listed; they establish
    nothing about creating one, so no apply or active-lifecycle capability is
    inferable from them. Like a schedule, its identity comes from its binding
    relation.

    This docstring is PUBLISHED SCHEMA — pydantic emits it verbatim as the
    ``$def`` description — so it states only what the reads actually support.
    """

    kind: Literal["deployment_unit"]
    key: TopologyObjectKeyV1


class ExternalQueueObjectV1(_SystemTopologyBase):
    """An externally-managed queue referenced by, never created by, this plan.

    Permanently ``gated-no-evidence`` in V1: the live account contains zero
    queue components, and the queue MCP tools are runtime troubleshooting
    surfaces, which are not authoring evidence (ADR-001 §12 rejects speculative
    queue mutation outright).
    """

    kind: Literal["external_queue"]
    key: TopologyObjectKeyV1
    resource_ref: OpaquePlatformRefV1


class ExternalEventStreamObjectV1(_SystemTopologyBase):
    """An externally-managed Event Streams topic. Gated exactly like a queue."""

    kind: Literal["external_event_stream"]
    key: TopologyObjectKeyV1
    resource_ref: OpaquePlatformRefV1


TopologyObjectV1 = Annotated[
    Union[
        ProcessObjectV1,
        ApiServiceObjectV1,
        DocumentCacheObjectV1,
        ProcessPropertyObjectV1,
        RuntimeObjectV1,
        EnvironmentObjectV1,
        ScheduleObjectV1,
        DeploymentUnitObjectV1,
        ExternalQueueObjectV1,
        ExternalEventStreamObjectV1,
    ],
    Field(discriminator="kind"),
]


# ---------------------------------------------------------------------------
# Relations
# ---------------------------------------------------------------------------


class ProcessCallRelationV1(_SystemTopologyBase):
    """Process A invokes process B as a sub-process.

    Deliberately carries no call-site, ordering, wait/async flag, or error
    behavior: those are WITHIN-process semantics and stay ProcessIR's authority
    (ADR-001 §3). Duplicating them here would create two answers to "does this
    call wait", which is the duplicate-authority failure §6 exists to remove.
    """

    kind: Literal["process_call"]
    key: TopologyRelationKeyV1
    caller_process: TopologyObjectKeyV1
    callee_process: TopologyObjectKeyV1


class ApiServiceRouteRelationV1(_SystemTopologyBase):
    """An API Service Component routes to a listener process.

    No path, method, or endpoint configuration: those are the ASC's own
    authored content, and carrying them here would both duplicate authority and
    put endpoint detail into a planning contract that promises opaque
    references only.

    There is no standalone listener OBJECT — "listener" is a ROLE a process
    plays by being an ASC route target. Modeling an independently addressable
    listener resource would assert a platform shape that does not exist.
    """

    kind: Literal["api_service_route"]
    key: TopologyRelationKeyV1
    api_service: TopologyObjectKeyV1
    listener_process: TopologyObjectKeyV1


class DocumentCacheUseRelationV1(_SystemTopologyBase):
    """A process reads from or writes to a shared Document Cache."""

    kind: Literal["document_cache_use"]
    key: TopologyRelationKeyV1
    process: TopologyObjectKeyV1
    document_cache: TopologyObjectKeyV1


class ProcessPropertyUseRelationV1(_SystemTopologyBase):
    """A process reads from or writes to a shared Process Property component."""

    kind: Literal["process_property_use"]
    key: TopologyRelationKeyV1
    process: TopologyObjectKeyV1
    process_property: TopologyObjectKeyV1


class ScheduleBindingRelationV1(_SystemTopologyBase):
    """A schedule binds one process to one runtime.

    A typed ternary binding rather than two edges: the platform's own identity
    for a schedule is exactly the ``(atomId, processId)`` pair, so splitting it
    would allow a half-bound schedule to exist, which the platform has no
    representation for.
    """

    kind: Literal["schedule_binding"]
    key: TopologyRelationKeyV1
    schedule: TopologyObjectKeyV1
    process: TopologyObjectKeyV1
    runtime: TopologyObjectKeyV1


class DeploymentBindingRelationV1(_SystemTopologyBase):
    """A deployment unit targets one process at one environment.

    One process, not many: ``orchestrate_deploy`` requires exactly one process
    per deployment, and no live evidence supports atomic multi-process
    deployment. Typed as a ternary so a unit cannot silently accumulate several
    process edges and imply an atomicity the platform does not offer.
    """

    kind: Literal["deployment_binding"]
    key: TopologyRelationKeyV1
    deployment_unit: TopologyObjectKeyV1
    process: TopologyObjectKeyV1
    environment: TopologyObjectKeyV1


class QueueReferenceRelationV1(_SystemTopologyBase):
    """A process references an external queue. Always blocked in V1."""

    kind: Literal["queue_reference"]
    key: TopologyRelationKeyV1
    process: TopologyObjectKeyV1
    external_queue: TopologyObjectKeyV1


class EventStreamReferenceRelationV1(_SystemTopologyBase):
    """A process references an external Event Streams topic. Always blocked in V1."""

    kind: Literal["event_stream_reference"]
    key: TopologyRelationKeyV1
    process: TopologyObjectKeyV1
    external_event_stream: TopologyObjectKeyV1


TopologyRelationV1 = Annotated[
    Union[
        ProcessCallRelationV1,
        ApiServiceRouteRelationV1,
        DocumentCacheUseRelationV1,
        ProcessPropertyUseRelationV1,
        ScheduleBindingRelationV1,
        DeploymentBindingRelationV1,
        QueueReferenceRelationV1,
        EventStreamReferenceRelationV1,
    ],
    Field(discriminator="kind"),
]


# ---------------------------------------------------------------------------
# Root
# ---------------------------------------------------------------------------


class SystemTopologySpecV1(_SystemTopologyBase):
    """The authored topology document root.

    ``profile_ref`` is mandatory and single: topology never crosses a credential
    profile, and making the profile a document-level fact rather than a per-object
    one means a cross-profile reference is unrepresentable rather than merely
    rejected.
    """

    version: Literal["1"]
    profile_ref: OpaquePlatformRefV1
    objects: List[TopologyObjectV1] = Field(..., min_length=1)
    relations: List[TopologyRelationV1] = Field(default_factory=list)


def _kinds_of(union_alias: Any) -> Tuple[str, ...]:
    """The ``kind`` discriminator literals of an ``Annotated[Union[...]]`` alias.

    DERIVED, never hand-listed: the capability registry keys off these sets, and
    a hand-maintained copy would drift the moment a kind is added — the exact
    duplicate-authority failure ADR-001 §6 exists to remove. An import-time
    coverage check pins registry membership against these in both directions.
    """
    members = get_args(get_args(union_alias)[0])
    return tuple(
        get_args(member.model_fields["kind"].annotation)[0] for member in members
    )


TOPOLOGY_OBJECT_KINDS: Tuple[str, ...] = _kinds_of(TopologyObjectV1)
TOPOLOGY_RELATION_KINDS: Tuple[str, ...] = _kinds_of(TopologyRelationV1)

#: Role fields per relation kind, in authored order. DERIVED from the models so
#: the endpoint matrix cannot drift from the schema; the resolver consumes this
#: to know which fields name an object key.
TOPOLOGY_RELATION_ROLES: Dict[str, Tuple[str, ...]] = {
    kind: tuple(
        name
        for name in member.model_fields
        if name not in ("kind", "key")
    )
    for kind, member in zip(
        TOPOLOGY_RELATION_KINDS, get_args(get_args(TopologyRelationV1)[0])
    )
}


# ---------------------------------------------------------------------------
# Diagnostic translation
# ---------------------------------------------------------------------------

_MESSAGES: Dict[str, str] = {
    TOPOLOGY_SCHEMA_UNKNOWN_OBJECT: "unknown or missing topology object kind",
    TOPOLOGY_SCHEMA_UNKNOWN_RELATION: "unknown or missing topology relation kind",
    TOPOLOGY_SCHEMA_UNKNOWN_FIELD: "unknown or prohibited field on a strict topology model",
    TOPOLOGY_SCHEMA_INVALID_CARDINALITY: "collection bound or binding cardinality violated",
    TOPOLOGY_SCHEMA_DUPLICATE_KEY: "a key or semantic relation is declared more than once",
    TOPOLOGY_SCHEMA_VERSION_UNSUPPORTED: "unsupported or missing topology document version",
    TOPOLOGY_SCHEMA_INVALID: "value does not match the strict SystemTopologySpecV1 schema",
}

_REMEDIATION: Dict[str, str] = {
    TOPOLOGY_SCHEMA_UNKNOWN_OBJECT: (
        "Use one of the documented topology object kinds "
        "(see docs/architecture/SYSTEM_TOPOLOGY_V1.md)."
    ),
    TOPOLOGY_SCHEMA_UNKNOWN_RELATION: (
        "Use one of the documented topology relation kinds "
        "(see docs/architecture/SYSTEM_TOPOLOGY_V1.md)."
    ),
    TOPOLOGY_SCHEMA_UNKNOWN_FIELD: (
        "Remove the field — topology models are strict and carry opaque "
        "references only, never secrets, certificates, environment extensions, "
        "raw XML, free-form config, or derived capability verdicts."
    ),
    TOPOLOGY_SCHEMA_INVALID_CARDINALITY: (
        "Satisfy the documented cardinality at the referenced path: declare at "
        "least one object, and bind every schedule and deployment unit exactly once."
    ),
    TOPOLOGY_SCHEMA_DUPLICATE_KEY: (
        "Give every object and relation a unique key, and declare each semantic "
        "relation once."
    ),
    TOPOLOGY_SCHEMA_VERSION_UNSUPPORTED: (
        "Set version to the supported SystemTopology version '1'."
    ),
    TOPOLOGY_SCHEMA_INVALID: (
        "Fix the value type/shape at the referenced path to match the "
        "SystemTopologySpecV1 schema."
    ),
}


def _diagnostic(
    code: str, path: Tuple[Any, ...], *, message: Optional[str] = None
) -> SystemTopologyDiagnostic:
    return SystemTopologyDiagnostic(
        code=code,
        path=_json_pointer(path),
        message=message or _MESSAGES[code],
        remediation=_REMEDIATION[code],
    )


_CUSTOM_ERROR_CODES: Dict[str, str] = {
    "topology_reference_invalid_format": TOPOLOGY_SCHEMA_INVALID,
}

# Pydantic error types that mean "the discriminator did not resolve".
_UNION_TAG_ERRORS = frozenset({"union_tag_invalid", "union_tag_not_found"})


def _loc_to_path(loc: Tuple[Any, ...]) -> Tuple[Any, ...]:
    """Strip the discriminator-tag loc element pydantic inserts after a union index.

    For a tagged union inside a list, pydantic reports
    ``("objects", 0, "process", "component_ref")`` — but ``process`` is the TAG,
    not a key that exists in the authored JSON. Pointing at
    ``/objects/0/process/component_ref`` sends a caller looking for a field that
    was never there, so the tag element is dropped.
    """
    if len(loc) >= 3 and loc[0] in ("objects", "relations") and isinstance(loc[1], int):
        tag_candidate = loc[2]
        known = (
            TOPOLOGY_OBJECT_KINDS if loc[0] == "objects" else TOPOLOGY_RELATION_KINDS
        )
        if isinstance(tag_candidate, str) and tag_candidate in known:
            return loc[:2] + loc[3:]
    return loc


def _translate_pydantic_error(err: Dict[str, Any]) -> SystemTopologyDiagnostic:
    loc = tuple(err.get("loc", ()))
    err_type = str(err.get("type", ""))

    if err_type in _CUSTOM_ERROR_CODES:
        return _diagnostic(_CUSTOM_ERROR_CODES[err_type], _loc_to_path(loc))

    if err_type in _UNION_TAG_ERRORS:
        # The tag itself failed, so there is no tag element to strip; point at
        # the union member position, which IS an authored location.
        code = (
            TOPOLOGY_SCHEMA_UNKNOWN_RELATION
            if loc and loc[0] == "relations"
            else TOPOLOGY_SCHEMA_UNKNOWN_OBJECT
        )
        return _diagnostic(code, loc)

    if err_type == "extra_forbidden":
        return _diagnostic(TOPOLOGY_SCHEMA_UNKNOWN_FIELD, _loc_to_path(loc))

    if err_type in ("too_short", "too_long"):
        return _diagnostic(TOPOLOGY_SCHEMA_INVALID_CARDINALITY, _loc_to_path(loc))

    return _diagnostic(TOPOLOGY_SCHEMA_INVALID, _loc_to_path(loc))


# ---------------------------------------------------------------------------
# Document-level rules
# ---------------------------------------------------------------------------


def _check_document_rules(spec: "SystemTopologySpecV1") -> None:
    """Uniqueness and binding cardinality — rules a per-model validator cannot state.

    Run here rather than as a ``model_validator`` for the reason #141 documents:
    a root-level validator attaches its error to the MODEL, so a duplicate key
    anywhere reports the document root — true, and useless for finding the
    offending declaration. Walking here keeps the authored path in hand.

    All findings accumulate; a caller fixes everything in one pass instead of
    discovering defects one round-trip at a time.
    """
    diagnostics: List[SystemTopologyDiagnostic] = []

    seen_object_keys: Dict[str, int] = {}
    for index, obj in enumerate(spec.objects):
        if obj.key in seen_object_keys:
            # Point at the LATER occurrence: the first one is the definition a
            # caller most likely meant to keep.
            diagnostics.append(
                _diagnostic(TOPOLOGY_SCHEMA_DUPLICATE_KEY, ("objects", index, "key"))
            )
        else:
            seen_object_keys[obj.key] = index

    seen_relation_keys: Dict[str, int] = {}
    seen_semantic: Dict[Tuple[Any, ...], int] = {}
    for index, rel in enumerate(spec.relations):
        if rel.key in seen_relation_keys:
            diagnostics.append(
                _diagnostic(TOPOLOGY_SCHEMA_DUPLICATE_KEY, ("relations", index, "key"))
            )
        else:
            seen_relation_keys[rel.key] = index

        # Semantic identity: the kind plus every role value. Two relations with
        # different keys but identical roles are the same edge declared twice —
        # which would double-count in the runtime graph.
        roles = TOPOLOGY_RELATION_ROLES[rel.kind]
        semantic = (rel.kind,) + tuple(getattr(rel, role) for role in roles)
        if semantic in seen_semantic:
            diagnostics.append(
                _diagnostic(TOPOLOGY_SCHEMA_DUPLICATE_KEY, ("relations", index))
            )
        else:
            seen_semantic[semantic] = index

    # A schedule or deployment unit has no identity of its own — the platform
    # identifies a schedule by (atom, process) and a deployment by
    # (component, environment). An unbound one is therefore not an
    # under-specified object; it is an object that cannot be said to exist.
    bound_schedules = {
        rel.schedule for rel in spec.relations if rel.kind == "schedule_binding"
    }
    bound_units = {
        rel.deployment_unit
        for rel in spec.relations
        if rel.kind == "deployment_binding"
    }
    for index, obj in enumerate(spec.objects):
        if obj.kind == "schedule" and obj.key not in bound_schedules:
            diagnostics.append(
                _diagnostic(TOPOLOGY_SCHEMA_INVALID_CARDINALITY, ("objects", index))
            )
        elif obj.kind == "deployment_unit" and obj.key not in bound_units:
            diagnostics.append(
                _diagnostic(TOPOLOGY_SCHEMA_INVALID_CARDINALITY, ("objects", index))
            )

    if diagnostics:
        raise SystemTopologyValidationError(diagnostics)


def parse_system_topology_v1(payload: Any) -> SystemTopologySpecV1:
    """Parse an authored payload into a validated :class:`SystemTopologySpecV1`.

    Raises :class:`SystemTopologyValidationError` with deterministic, sorted,
    secret-free diagnostics on any failure. Order of gates: payload shape →
    forbidden-key scan → version → strict model validation → document rules.

    The forbidden-key scan runs BEFORE model validation on purpose: a secret
    nested under an unknown field would otherwise be reported by pydantic as
    ``extra_forbidden``, and the diagnostic path would name the field — which is
    fine — but the scan also catches secrets nested inside otherwise-valid
    containers that strict validation would never reach.
    """
    if not isinstance(payload, dict):
        raise SystemTopologyValidationError(
            [
                _diagnostic(
                    TOPOLOGY_SCHEMA_INVALID, (), message="payload must be a JSON object"
                )
            ]
        )

    forbidden = _find_forbidden_key(payload)
    if forbidden is not None:
        path, code = forbidden
        raise SystemTopologyValidationError([_diagnostic(code, path)])

    if payload.get("version") != SYSTEM_TOPOLOGY_VERSION:
        raise SystemTopologyValidationError(
            [_diagnostic(TOPOLOGY_SCHEMA_VERSION_UNSUPPORTED, ("version",))]
        )

    try:
        spec = SystemTopologySpecV1.model_validate(payload)
    except ValidationError as exc:
        diagnostics = [_translate_pydantic_error(err) for err in exc.errors()]
        raise SystemTopologyValidationError(diagnostics) from None

    _check_document_rules(spec)
    return spec


# ---------------------------------------------------------------------------
# Canonical serialization + schema
# ---------------------------------------------------------------------------


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def canonical_system_topology_json(spec: SystemTopologySpecV1) -> str:
    """Canonical JSON: defaults and Nones included, keys sorted, list order kept."""
    return _canonical_json(spec.model_dump(mode="json"))


def system_topology_v1_json_schema() -> dict:
    """The generated JSON Schema for :class:`SystemTopologySpecV1` (closed unions)."""
    return SystemTopologySpecV1.model_json_schema()


def canonical_system_topology_schema_json() -> str:
    return _canonical_json(system_topology_v1_json_schema())
