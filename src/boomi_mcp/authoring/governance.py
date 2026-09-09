"""Governance resolution for the typed per-root envelope (issue #157 / M12.19).

Everything a typed compiling intent (``process_ir`` or ``recipe``) says about a
process root that is NOT its semantics — name, description, folder, runtime
bindings, deliberate operational intent — lives on the per-root envelope
(:mod:`boomi_mcp.models.process_component`) and nowhere else. This module turns
what a caller AUTHORED into what apply will USE, once, server-side, and strictly
before anything is fingerprinted:

* **Default naming.** An envelope may omit ``name`` when it carries
  ``component_prefix``; the resolved name is ``"<prefix> <component_key>"``.
  ``action`` is never derived.
* **Folder fan-out.** One knob per root: the root's ``folder_name`` places every
  supporting component the root OWNS — its dependency/reference closure,
  stopping at another root and skipping reuse references — that has no explicit
  placement of its own. The same ownership rule gives an unnamed supporting
  component its default name from the root's prefix. Two roots disagreeing over
  one unplaced component is refused, never resolved silently.
* **Connection binding contract.** Reuse XOR create: a reuse binding may carry
  only the closed binding/declaration key set; a secured REST connection cannot
  be created inline (it must be reused); connection default headers and
  operation headers may not collide.
* **Recorded-not-wired intent.** The typed watermark declaration (M18 rule
  half) and runtime hints are validated, then served back as
  :class:`RecordedIntentV1` records — never wired into an emitted byte, a
  fingerprint or a mutation.

**Authorities, not copies.** Ownership is the plan's own reference closure
(:func:`iter_plan_component_refs`); "reuse" is
:func:`component_materialization_mode`; the secret scan is the archetype
scanner; the header merger is the archetype assembly's; "secured REST auth" is
the REST builder's recognized modes minus the archetype's create map. None of
those facts is restated here, so none can drift.

**Refusals.** Every rule raises :class:`GovernanceRefusal` carrying the served
code and value-free diagnostics; the workflow translates it into its own error
channel. Diagnostics name paths and keys, never authored values.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from ..errors import (
    GOVERNANCE_CONNECTION_BINDING_CONFLICT,
    GOVERNANCE_HEADER_COLLISION,
    GOVERNANCE_INHERITANCE_AMBIGUOUS,
    GOVERNANCE_NAME_COLLISION,
    GOVERNANCE_RECORDED_INTENT_INVALID,
    GOVERNANCE_WATERMARK_INCONSISTENT,
)
from ..models.authoring_workflow import AuthoringDiagnosticV1
from ..models.governance_intent import RecordedIntentV1
from ..models.integration_models import IntegrationComponentSpec
from ..models.process_component import (
    ProcessAuthoringUnitAuthoredV1,
    ProcessAuthoringUnitV1,
    ProcessComponentEnvelopeAuthoredV1,
    ProcessComponentEnvelopeV1,
)
from .process_materialization import iter_plan_component_refs

#: The builder family's secret refusal, served by every connector builder for a
#: plaintext secret-shaped key. Reused by name — this module adds no second
#: secret vocabulary.
PLAINTEXT_SECRET_REJECTED = "PLAINTEXT_SECRET_REJECTED"

REF_PREFIX = "$ref:"

#: The keys a REUSE binding on a connection may carry, and nothing else. DERIVED
#: from the two authorities that read a reuse binding: ``reference_only`` is
#: what :func:`component_materialization_mode` keys on; ``component_id`` /
#: ``component_name`` are what the legacy plan's reference resolution reads;
#: ``connector_type`` is the DECLARED family the connector-metadata projection
#: (#155) asserts against the resolved component. Anything else on a reuse
#: binding is an inline creation setting for a component that is not being
#: created — the "both present, silent precedence" case the contract refuses.
REUSE_BINDING_KEYS = frozenset(
    {"reference_only", "component_id", "component_name", "connector_type"}
)

_CONNECTION_TYPE = "connector-settings"
_OPERATION_TYPE = "connector-action"


class GovernanceRefusal(Exception):
    """A governance rule refused the intent. ``code`` + value-free diagnostics."""

    def __init__(self, code: str, diagnostics: Tuple[AuthoringDiagnosticV1, ...]) -> None:
        super().__init__(code)
        self.code = code
        self.diagnostics = tuple(diagnostics)


def _diag(code: str, *, message: str, path: str = "", subject_kind: str = "",
          subject_id: str = "", remediation: str = "",
          cause_codes: Tuple[str, ...] = ()) -> AuthoringDiagnosticV1:
    return AuthoringDiagnosticV1(
        code=code,
        severity="error",
        path=path,
        subject_kind=subject_kind,
        subject_id=subject_id,
        message=message,
        remediation=remediation,
        cause_codes=tuple(sorted(set(cause_codes))),
    )


def _refuse(code: str, **kwargs: Any) -> GovernanceRefusal:
    return GovernanceRefusal(code, (_diag(code, **kwargs),))


# ---------------------------------------------------------------------------
# references
# ---------------------------------------------------------------------------


def iter_authored_refs(value: Any) -> List[str]:
    """Every ``$ref:KEY`` token an authored payload actually uses.

    Walks the payload rather than listing the component plan's own keys. Those
    are two different questions, and live QA showed the difference matters: a
    dangling ``$ref`` is the ONE reference a caller needs to see, and it is
    exactly the one a component-key listing omits (issue #146 QA, bug #410).
    Moved here from the workflow module so the ownership closure below and the
    legacy plan echo read one walker.
    """
    found: List[str] = []
    if isinstance(value, str):
        if value.startswith(REF_PREFIX):
            found.append(value)
    elif isinstance(value, Mapping):
        for item in value.values():
            found.extend(iter_authored_refs(item))
    elif isinstance(value, (list, tuple)):
        for item in value:
            found.extend(iter_authored_refs(item))
    return found


def ref_key(reference: Any) -> Optional[str]:
    """The component key a ``$ref:KEY`` names, or ``None`` for anything else."""
    if isinstance(reference, str) and reference.startswith(REF_PREFIX):
        key = reference[len(REF_PREFIX):]
        return key or None
    return None


# ---------------------------------------------------------------------------
# naming
# ---------------------------------------------------------------------------


def derive_default_name(component_prefix: str, component_key: str) -> str:
    """The canonical default display name: the prefix, a space, the stable key.

    The KEY is the suffix, not a role registry: it is the one name a component
    already has that is unique within the plan and stable across re-plans, and
    using it avoids a second hand-maintained role-name table (the legacy
    archetypes' role suffixes stay their producers' responsibility until #159).
    """
    return "{0} {1}".format(component_prefix, component_key)


def resolve_envelope(
    authored: ProcessComponentEnvelopeAuthoredV1,
) -> ProcessComponentEnvelopeV1:
    """The STRICT envelope an authored one resolves to.

    An explicit name is carried unchanged; otherwise the default is derived. The
    authored model already guarantees one of the two exists. Every other carried
    field crosses verbatim — the carried set is derived from the strict model's
    own field list, so a field added to one envelope cannot be dropped by the
    other.
    """
    name = authored.name
    if name is None:
        name = derive_default_name(str(authored.component_prefix), authored.component_key)
    carried = {
        field: getattr(authored, field)
        for field in authored.carried_fields()
        if field != "name"
    }
    return ProcessComponentEnvelopeV1(name=name, **carried)


# ---------------------------------------------------------------------------
# ownership (folder fan-out and inherited default names)
# ---------------------------------------------------------------------------


def _is_reuse_reference(component: IntegrationComponentSpec) -> bool:
    """Is this component a REUSE binding? The binding contract's own question.

    Distinct from :func:`_will_be_created`, deliberately: reuse-XOR-create is a
    rule about the reuse mode specifically, while ownership is a rule about the
    create mode specifically, and the third mode — an update binding — answers
    False to both. One authority, two questions.
    """
    from ..recipes.materialization import component_materialization_mode

    return component_materialization_mode(component) == "reuse_reference"


def _will_be_created(component: IntegrationComponentSpec) -> bool:
    """Will apply CREATE this component? The one authority answers.

    Governance claims a default name and a folder for what this request brings
    into existence, and for nothing else. The predicate used to be the negation
    of "is a reuse reference", which admitted the third mode: an
    ``action="update"`` binding of an EXISTING component. Those were renamed to
    ``"<prefix> <key>"`` and would have been moved — a component the caller
    named nowhere in the request, mutated by inheritance (QA-157-r1-02).

    Asking :func:`component_materialization_mode` for the mode, rather than
    testing one of its three answers, is what makes a fourth mode a compile
    error here instead of a silent claim.
    """
    from ..recipes.materialization import component_materialization_mode

    return component_materialization_mode(component) == "create"


def owned_supporting_keys(
    envelope: ProcessComponentEnvelopeV1,
    process_ir: Any,
    components_by_key: Mapping[str, IntegrationComponentSpec],
    root_keys: Set[str],
) -> Tuple[str, ...]:
    """The supporting components ONE root owns, sorted.

    The closure of the root's own references — its ``depends_on`` plus every
    ``$ref`` the envelope and the IR carry (:func:`iter_plan_component_refs`,
    the single enumeration authority) — followed through each supporting
    component's ``depends_on`` and the ``$ref`` tokens in its config. The walk
    stops at another process root, which owns its own governance, and never
    enters a component apply will not CREATE — a reuse reference or an
    ``action="update"`` binding of an existing component. Both name something
    the account already holds, and inheriting a name or a folder onto either
    would rename or move a component the request never authored
    (QA-157-r1-02).
    """
    seeds: List[str] = list(envelope.depends_on)
    for _path, reference in iter_plan_component_refs(envelope, process_ir):
        key = ref_key(reference)
        if key is not None:
            seeds.append(key)

    owned: List[str] = []
    stack = list(reversed(seeds))
    while stack:
        key = stack.pop()
        if key in owned or key == envelope.component_key or key in root_keys:
            continue
        component = components_by_key.get(key)
        if component is None:
            continue
        if not _will_be_created(component):
            continue
        owned.append(key)
        followers: List[str] = list(component.depends_on or ())
        for reference in iter_authored_refs(component.config or {}):
            follower = ref_key(reference)
            if follower is not None:
                followers.append(follower)
        stack.extend(reversed(followers))
    return tuple(sorted(owned))


def _config_str(component: IntegrationComponentSpec, key: str) -> Optional[str]:
    value = (component.config or {}).get(key)
    if isinstance(value, str) and value.strip():
        return value
    return None


def _declares_its_own_name(component: IntegrationComponentSpec) -> Optional[str]:
    """The name a RAW-XML component declares in its own document, if any.

    A component authored as a document is named by that document, and the
    document is what reaches the wire. Reading only the config left such a
    component looking unnamed: it took a derived preview name that was never
    emitted, and two owning roots with different prefixes refused it as
    ambiguous although its own bytes already named it. Same authority, same
    reader as the placement half.
    """
    document = (component.config or {}).get("xml")
    if not (isinstance(document, str) and document.strip()):
        return None
    from ..categories.components.canonical_process_apply import document_declarations

    return document_declarations(document)["name"]


def _declares_its_own_placement(component: IntegrationComponentSpec) -> Optional[str]:
    """The placement this component already declares, in ANY of its spellings.

    Every spelling is a placement, so every one must stop the fan-out. Testing
    only ``folder_name`` let a component declaring ``folder_id`` inherit the
    root's folder NAME as well — and apply then submitted the id, which wins.
    The resulting row was assembled from two authorities and contradicted
    itself: it named the root's folder as requested, carried a different
    folder's id as resolved, and reported the placement verified (QA-157-r2-02).

    The third spelling is a RAW-XML component's own document: it carries a
    ``folderId`` attribute that reaches the wire unchanged, so a caller who
    authored the document authored the placement. The fan-out claimed the
    root's folder anyway and the create boundary replaced the caller's id —
    silently, with a row that named the folder the caller did not ask for
    (QA-157-r10-01). This repository's own sibling authority already answers
    which wins: the folderless-create lint exempts a raw-XML create because
    the author owns placement. The predicate now agrees with it, and the
    document is ASKED rather than modelled — through the one reader that
    already answers "what placement do these bytes carry".
    """
    declared = _config_str(component, "folder_name") or _config_str(component, "folder_id")
    if declared:
        return declared
    document = (component.config or {}).get("xml")
    if not (isinstance(document, str) and document.strip()):
        return None
    from ..categories.components.canonical_process_apply import applied_placement

    carried = applied_placement(document)
    # ONLY THE SPELLING THAT PLACES COUNTS AS A DECLARATION.
    #
    # `folderName` is what every builder in this repository emits and what
    # every platform readback carries, and this platform IGNORES it on create.
    # Accepting it here as "the author placed this themselves" suppressed the
    # fan-out for a document that places nothing, so the component landed at
    # the account root with no row and no warning — the finding this whole
    # slice opened on, reinstated on the escape hatch this rule was written
    # for. A document declares a placement when it carries a `folderId`.
    return carried["folder_id"]


# ---------------------------------------------------------------------------
# the resolution
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DeferredWatermarkCheck:
    """One watermark whose source-field rule this pass could not decide.

    Governance runs OFFLINE, so a watermark over a reused or literal source
    profile has no field index here. Refusing on that rejected declarations the
    selected profile really satisfies; silently passing would drop the rule.
    The third answer is to record the question with the unit index THIS pass
    assigned it, so the online validation re-asks the same declaration against
    the selected artifact and reports on the same served path. The index is
    carried rather than recomputed: the spec's process order is not the
    authored order, so anyone re-deriving it would name a different unit.
    """

    unit_index: int
    process_key: str
    declaration: Any


@dataclass(frozen=True)
class GovernanceResolution:
    """What normalization consumes: strict units, resolved components, records."""

    units: Tuple[ProcessAuthoringUnitV1, ...]
    components: Tuple[IntegrationComponentSpec, ...]
    recorded_intents: Tuple[RecordedIntentV1, ...]
    #: Watermark source-field rules this offline pass deferred; the online
    #: validation decides exactly these and nothing else.
    deferred_watermarks: Tuple[DeferredWatermarkCheck, ...] = ()


def resolve_governance(
    authored_units: Sequence[ProcessAuthoringUnitAuthoredV1],
    components: Sequence[IntegrationComponentSpec],
    *,
    literal_indexes: Optional[Mapping[str, Any]] = None,
    extra_recorded_intents: Sequence[RecordedIntentV1] = (),
) -> GovernanceResolution:
    """Resolve every authored envelope and derive governance onto its components.

    Order is load-bearing: names resolve first (fan-out needs the prefix and the
    collision check needs every root's final name), then ownership fan-out
    mutates supporting components, then the binding contract runs over the
    RESOLVED components, then watermark rules run over the owned operations,
    then the records are assembled. Every step raises on its first refusal;
    nothing is applied partially.
    """
    components_by_key: Dict[str, IntegrationComponentSpec] = {
        component.key: component for component in components
    }
    root_keys = {unit.envelope.component_key for unit in authored_units}

    # 1. names
    resolved: List[Tuple[int, ProcessAuthoringUnitAuthoredV1, ProcessComponentEnvelopeV1]] = []
    for index, unit in enumerate(authored_units):
        resolved.append((index, unit, resolve_envelope(unit.envelope)))

    # 2. ownership: folders and inherited default names, claimed per root
    folder_claims: Dict[str, Dict[str, int]] = {}
    name_claims: Dict[str, Dict[str, int]] = {}
    owned_by_root: Dict[str, Tuple[str, ...]] = {}
    for index, unit, envelope in resolved:
        owned = owned_supporting_keys(envelope, unit.process_ir, components_by_key, root_keys)
        owned_by_root[envelope.component_key] = owned
        for key in owned:
            component = components_by_key[key]
            if envelope.folder_name and _declares_its_own_placement(component) is None:
                # UNCONDITIONAL, and deliberately so. Asking a builder whether
                # its config vocabulary contains this key, and reading "no" as
                # "this component cannot be placed", was the seventh instance of
                # this slice's recurring class (QA-157-r9-01): placement never
                # travels through a builder's config at all — it is an argument
                # to the create, injected as a folder id at the one raw-create
                # boundary. The five builders that refused the key now accept it
                # as metadata and emit nothing for it, which is where that fact
                # belonged.
                folder_claims.setdefault(key, {}).setdefault(envelope.folder_name, index)
            prefix = unit.envelope.component_prefix
            if (
                prefix
                and not (component.name and component.name.strip())
                and _config_str(component, "component_name") is None
                and _declares_its_own_name(component) is None
            ):
                name_claims.setdefault(key, {}).setdefault(derive_default_name(prefix, key), index)

    for key, claims in sorted(folder_claims.items()):
        if len(claims) > 1:
            raise _refuse(
                GOVERNANCE_INHERITANCE_AMBIGUOUS,
                message=(
                    "A supporting component with no explicit folder is owned by "
                    "two process roots placed in different folders."
                ),
                path="/components/{0}/config/folder_name".format(key),
                subject_kind="component",
                subject_id=key,
                remediation=(
                    "Give the shared component an explicit folder_name, or place "
                    "both roots in one folder."
                ),
            )
    for key, claims in sorted(name_claims.items()):
        if len(claims) > 1:
            raise _refuse(
                GOVERNANCE_INHERITANCE_AMBIGUOUS,
                message=(
                    "A supporting component with no explicit name is owned by "
                    "two process roots carrying different component prefixes."
                ),
                path="/components/{0}/name".format(key),
                subject_kind="component",
                subject_id=key,
                remediation=(
                    "Give the shared component an explicit name, or give both "
                    "roots one component_prefix."
                ),
            )

    updated: Dict[str, IntegrationComponentSpec] = {}
    for key, claims in folder_claims.items():
        (folder,) = claims.keys()
        component = updated.get(key, components_by_key[key])
        config = dict(component.config or {})
        config["folder_name"] = folder
        updated[key] = component.model_copy(update={"config": config})
    for key, claims in name_claims.items():
        (name,) = claims.keys()
        component = updated.get(key, components_by_key[key])
        updated[key] = component.model_copy(update={"name": name})

    resolved_components: List[IntegrationComponentSpec] = [
        updated.get(component.key, component) for component in components
    ]
    components_by_key = {component.key: component for component in resolved_components}

    # 3. create-name uniqueness across roots and governance-named components
    create_names: Dict[str, str] = {}
    for index, unit, envelope in resolved:
        if envelope.action != "create":
            continue
        previous = create_names.get(envelope.name)
        if previous is not None:
            raise _refuse(
                GOVERNANCE_NAME_COLLISION,
                message="Two process roots resolve to the same create name.",
                path="/units/{0}/envelope/name".format(index),
                subject_kind="process",
                subject_id=envelope.component_key,
                remediation=(
                    "Give each root a distinct explicit name, or distinct "
                    "component prefixes."
                ),
            )
        create_names[envelope.name] = envelope.component_key
    for key in sorted(name_claims):
        component = components_by_key[key]
        if not _will_be_created(component):
            continue
        name = str(component.name)
        previous = create_names.get(name)
        if previous is not None:
            raise _refuse(
                GOVERNANCE_NAME_COLLISION,
                message=(
                    "A derived supporting component name collides with another "
                    "resolved create name."
                ),
                path="/components/{0}/name".format(key),
                subject_kind="component",
                subject_id=key,
                remediation="Give the component an explicit, distinct name.",
            )
        create_names[name] = key

    # 4. the connection binding contract over the RESOLVED components
    resolved_components = list(
        apply_connection_binding_contract(resolved_components)
    )
    components_by_key = {component.key: component for component in resolved_components}

    # 5. watermark consistency over the owned operations
    #
    # CONTRIBUTED RECORDS ARE VALIDATED LIKE AUTHORED ONES. A recipe reaches
    # this channel through `extra_recorded_intents`, and validating only the
    # envelope's own declaration let a recipe record a watermark naming a
    # profile or field that does not exist — the identical declaration on an
    # envelope is refused. Same rule, both entry points, before anything is
    # served or persisted.
    records: List[RecordedIntentV1] = []
    deferred_watermarks: List[DeferredWatermarkCheck] = []
    _roots_by_key = {envelope.component_key: (index, envelope) for index, _u, envelope in resolved}
    for contributed in extra_recorded_intents:
        declaration = getattr(contributed, "declaration", None)
        owner = _roots_by_key.get(getattr(contributed, "process_key", ""))
        if (
            declaration is not None
            and getattr(declaration, "declaration_kind", None) == "watermark"
            and owner is not None
        ):
            owner_index, owner_envelope = owner
            if validate_watermark_declaration(
                declaration,
                unit_index=owner_index,
                owned_keys=owned_by_root[owner_envelope.component_key],
                components_by_key=components_by_key,
                literal_indexes=literal_indexes,
            ):
                deferred_watermarks.append(
                    DeferredWatermarkCheck(
                        unit_index=owner_index,
                        process_key=owner_envelope.component_key,
                        declaration=declaration,
                    )
                )
        records.append(contributed)
    for index, unit, envelope in resolved:
        authored = unit.envelope
        if authored.watermark is not None:
            if validate_watermark_declaration(
                authored.watermark,
                unit_index=index,
                owned_keys=owned_by_root[envelope.component_key],
                components_by_key=components_by_key,
                literal_indexes=literal_indexes,
            ):
                deferred_watermarks.append(
                    DeferredWatermarkCheck(
                        unit_index=index,
                        process_key=envelope.component_key,
                        declaration=authored.watermark,
                    )
                )
            records.append(
                RecordedIntentV1(
                    intent_id="watermark",
                    process_key=envelope.component_key,
                    declaration=authored.watermark,
                )
            )
        for position, hint in enumerate(authored.recorded_intents):
            scan_secret_shaped_keys(
                hint.model_dump(mode="json"),
                path="/units/{0}/envelope/recorded_intents/{1}".format(index, position),
            )
            records.append(
                RecordedIntentV1(
                    intent_id="runtime_hint/{0}".format(position),
                    process_key=envelope.component_key,
                    declaration=hint,
                )
            )

    # 6. records: one identity each, deterministic order
    seen: Set[Tuple[str, str]] = set()
    for record in records:
        if record.sort_key in seen:
            raise _refuse(
                GOVERNANCE_RECORDED_INTENT_INVALID,
                message="Two recorded intents share one (process_key, intent_id) identity.",
                subject_kind="process",
                subject_id=record.process_key,
                remediation="Give each recorded intent a distinct intent_id.",
            )
        seen.add(record.sort_key)
    if any(record.process_key not in root_keys for record in records):
        offender = next(r for r in records if r.process_key not in root_keys)
        raise _refuse(
            GOVERNANCE_RECORDED_INTENT_INVALID,
            message="A recorded intent names a process root this request does not author.",
            subject_kind="process",
            subject_id=offender.process_key,
            remediation="Name a root the request authors, or drop the record.",
        )

    units = tuple(
        ProcessAuthoringUnitV1(envelope=envelope, process_ir=unit.process_ir)
        for _index, unit, envelope in resolved
    )
    return GovernanceResolution(
        units=units,
        components=tuple(resolved_components),
        recorded_intents=tuple(sorted(records, key=lambda r: r.sort_key)),
        deferred_watermarks=tuple(deferred_watermarks),
    )


# ---------------------------------------------------------------------------
# the connection binding contract
# ---------------------------------------------------------------------------


def _operations_of(
    connection_key: str, components: Sequence[IntegrationComponentSpec]
) -> List[IntegrationComponentSpec]:
    """Every connector-action that binds to ``connection_key``, in plan order."""
    wanted_ref = REF_PREFIX + connection_key
    found = []
    for component in components:
        if component.type != _OPERATION_TYPE:
            continue
        config = component.config or {}
        if config.get("connection_ref_key") == connection_key or config.get(
            "connection_id"
        ) == wanted_ref:
            found.append(component)
    return found


def _refuse_contradictory_identity(component, config, key: str) -> None:
    """A binding may not name two different components, however it spells reuse.

    Both halves of an identity — the top-level field and the config's copy —
    name ONE component or the contract cannot say which the request means. The
    check lived inside the reuse branch, so a connection declaring
    ``action="create"`` with one id at the top level and another in its config
    passed untouched, while apply under the reuse policy resolved it to the
    first (architect review, item 3).
    """
    top_id = component.component_id.strip() if isinstance(component.component_id, str) else ""
    cfg_id = config.get("component_id")
    cfg_id = cfg_id.strip() if isinstance(cfg_id, str) else ""
    if top_id and cfg_id and top_id != cfg_id:
        raise _refuse(
            GOVERNANCE_CONNECTION_BINDING_CONFLICT,
            message="A connection binding names two disagreeing component ids.",
            path="/components/{0}/config/component_id".format(key),
            subject_kind="component",
            subject_id=key,
            remediation="Name the bound component once.",
        )
    top_name = component.name.strip() if isinstance(component.name, str) else ""
    cfg_name = config.get("component_name")
    cfg_name = cfg_name.strip() if isinstance(cfg_name, str) else ""
    if top_name and cfg_name and top_name != cfg_name:
        raise _refuse(
            GOVERNANCE_CONNECTION_BINDING_CONFLICT,
            message="A connection binding names two disagreeing component names.",
            path="/components/{0}/config/component_name".format(key),
            subject_kind="component",
            subject_id=key,
            remediation="Name the bound component once.",
        )


def apply_connection_binding_contract(
    components: Sequence[IntegrationComponentSpec],
) -> Tuple[IntegrationComponentSpec, ...]:
    """Reuse XOR create, the REST auth gate, and the header collision rule.

    Returns the components with connection default headers MERGED into their
    operations' request headers (the archetype assembly's own merger, strict
    mode) and the consumed ``default_headers`` removed — so a default header on
    the typed surface either reaches the emitted operation or is refused, and
    is never an accepted-but-inert input.
    """
    from ..categories.components.builders.connector_builder import (
        RestClientConnectionBuilder,
        _resolve_rest_connector_type,
    )
    from ..categories.components.builders.connector_builder import (
        BuilderValidationError,
    )
    from ..patterns.archetype_assembly import (
        UNSUPPORTED_REST_AUTH_MODE,
        _REST_CREATE_AUTH_MAP,
        _merge_request_headers,
    )

    updated: Dict[str, IntegrationComponentSpec] = {}
    for component in components:
        if component.type != _CONNECTION_TYPE:
            continue
        key = component.key
        config = dict(component.config or {})
        reuse = _is_reuse_reference(component)

        if reuse:
            extra = sorted(set(config) - REUSE_BINDING_KEYS)
            if extra:
                raise _refuse(
                    GOVERNANCE_CONNECTION_BINDING_CONFLICT,
                    message=(
                        "A reuse binding carries inline creation settings; reuse "
                        "and create are exclusive."
                    ),
                    path="/components/{0}/config/{1}".format(key, extra[0]),
                    subject_kind="component",
                    subject_id=key,
                    remediation=(
                        "Keep only reference_only plus component_id or "
                        "component_name (and the declared connector_type) on a "
                        "reused connection; author creation settings only on a "
                        "connection being created."
                    ),
                )
            _refuse_contradictory_identity(component, config, key)
        else:
            # A CONTRADICTION IS A CONTRADICTION OUTSIDE THE REUSE SPELLING TOO.
            # These checks sat inside the reuse branch, so a connection
            # declaring `action="create"` with one component id at the top
            # level and a different one in its config — which apply resolves to
            # a reuse under the reuse policy — carried two identities and
            # inline settings past the contract entirely.
            _refuse_contradictory_identity(component, config, key)

        if reuse:
            continue

        is_rest = _resolve_rest_connector_type(config.get("connector_type")) is not None
        if is_rest and component.action == "create":
            allowed = {mode.upper() for mode in _REST_CREATE_AUTH_MAP.values()}
            secured = sorted(
                mode for mode in RestClientConnectionBuilder.RECOGNIZED_AUTH_MODES
                if mode.upper() not in allowed
            )
            auth = config.get("auth")
            auth_token = auth.strip().upper() if isinstance(auth, str) else ""
            if auth_token and auth_token not in allowed:
                raise _refuse(
                    UNSUPPORTED_REST_AUTH_MODE,
                    message=(
                        "A secured REST connection cannot be created inline on "
                        "the typed surface; only an unauthenticated connection "
                        "can be created, and a secured one must be reused."
                    ),
                    path="/components/{0}/config/auth".format(key),
                    subject_kind="component",
                    subject_id=key,
                    remediation=(
                        "Reuse an existing secured REST connection "
                        "(config.reference_only=true with component_id or "
                        "component_name). Secured modes: {0}.".format(", ".join(secured))
                    ),
                )

        defaults = config.get("default_headers")
        if isinstance(defaults, Mapping) and defaults:
            for operation in _operations_of(key, components):
                # THE ROUTE HAS TO BE ABLE TO CARRY THEM. An operation authored
                # as a raw document is submitted unchanged, and one bound for
                # reuse is not written at all — so merging headers into their
                # config produced a preview showing headers the wire never
                # receives. The merge is refused rather than performed
                # invisibly, because a caller who authored both a default
                # header and a document has asked for two different things.
                op_config_raw = operation.config or {}
                document = op_config_raw.get("xml")
                if (isinstance(document, str) and document.strip()) or _is_reuse_reference(
                    operation
                ):
                    raise _refuse(
                        GOVERNANCE_CONNECTION_BINDING_CONFLICT,
                        message=(
                            "A connection default header cannot reach an "
                            "operation this request submits as its own document "
                            "or binds for reuse."
                        ),
                        path="/components/{0}/config/default_headers".format(key),
                        subject_kind="component",
                        subject_id=key,
                        remediation=(
                            "Author the header in the operation itself, or drop "
                            "default_headers from the connection."
                        ),
                    )
                op_config = dict(op_config_raw)
                try:
                    merged = _merge_request_headers(
                        dict(defaults),
                        op_config.get("request_headers") or None,
                        default_field="/components/{0}/config/default_headers".format(key),
                        operation_field="/components/{0}/config/request_headers".format(
                            operation.key
                        ),
                        strict=True,
                    )
                except BuilderValidationError as exc:
                    raise _refuse(
                        GOVERNANCE_HEADER_COLLISION,
                        message=(
                            "Connection default headers and operation request "
                            "headers collide, or one of them repeats a header "
                            "name in a different letter case."
                        ),
                        path=str(exc.field or ""),
                        subject_kind="component",
                        subject_id=operation.key,
                        remediation="Set each header in exactly one place.",
                        cause_codes=(str(exc.error_code or ""),),
                    ) from None
                if merged:
                    op_config["request_headers"] = merged
                else:
                    op_config.pop("request_headers", None)
                updated[operation.key] = updated.get(operation.key, operation).model_copy(
                    update={"config": op_config}
                )
            config.pop("default_headers", None)
            updated[key] = updated.get(key, component).model_copy(update={"config": config})

    return tuple(updated.get(component.key, component) for component in components)


# ---------------------------------------------------------------------------
# watermark (M18 rule half)
# ---------------------------------------------------------------------------


def _query_parameter_names(component: IntegrationComponentSpec) -> Set[str]:
    raw = (component.config or {}).get("query_parameters")
    names: Set[str] = set()
    if isinstance(raw, Mapping):
        names.update(str(name) for name in raw)
    elif isinstance(raw, (list, tuple)):
        for entry in raw:
            if isinstance(entry, Mapping) and isinstance(entry.get("name"), str):
                names.add(entry["name"])
    return names


def validate_watermark_declaration(
    declaration: Any,
    *,
    unit_index: int,
    owned_keys: Sequence[str],
    components_by_key: Mapping[str, IntegrationComponentSpec],
    literal_indexes: Optional[Mapping[str, Any]] = None,
) -> bool:
    """The two legacy consistency rules, carried onto the typed declaration.

    (a) the tracked field must be a declared, mappable field of the referenced
        source profile — resolved through the SAME profile-index resolver the
        map validator uses, never a second walker;
    (b) every query parameter the declaration says it will source must exist on
        a REST operation the root owns — the legacy "a watermark-sourced query
        parameter requires a declaration" rule, read from the declaration's
        side.

    Returns True when rule (a) was DEFERRED because the source profile has no
    index in reach — the caller records that question for the online pass.
    Rule (b) is always decided here.
    """
    base = "/units/{0}/envelope/watermark".format(unit_index)
    decided = validate_watermark_source_field(
        declaration,
        unit_index=unit_index,
        components_by_key=components_by_key,
        literal_indexes=literal_indexes,
    )
    # RULE (b) RUNS EITHER WAY. It reads the root's own owned operations and
    # needs no profile index, so letting an undecidable rule (a) return early
    # would have silently retired the query-parameter rule for every watermark
    # over a reused source profile — a gate weakened by a deferral that was
    # never about it (architect review, item 4).
    available: Set[str] = set()
    for key in owned_keys:
        component = components_by_key.get(key)
        if component is not None and component.type == _OPERATION_TYPE:
            available |= _query_parameter_names(component)
    for position, name in enumerate(declaration.query_parameter_refs):
        if name not in available:
            raise _refuse(
                GOVERNANCE_WATERMARK_INCONSISTENT,
                message=(
                    "The watermark names a query parameter that no REST "
                    "operation owned by this root declares."
                ),
                path=base + "/query_parameter_refs/{0}".format(position),
                subject_kind="process",
                remediation=(
                    "Declare the query parameter on an owned REST operation, or "
                    "drop it from the watermark declaration."
                ),
            )
    return not decided


def _watermark_source_is_deferrable(
    source_profile_ref: Any, components_by_key: Mapping[str, IntegrationComponentSpec]
) -> bool:
    """Is an unresolved source-profile index an UNAVAILABLE answer, or a bad reference?

    Deferrable in exactly two shapes: a literal existing-profile id, whose index
    the online pass discovers; and an in-plan profile component this request will
    REUSE rather than write, which is the one case the shared resolver
    deliberately refuses to index from the candidate config. Anything else — a
    `$ref` naming no component, a `$ref` naming a non-profile, or a profile this
    request authors whose own config yields no index — is a reference the online
    pass can never repair, so it stays a refusal.
    """
    if not isinstance(source_profile_ref, str) or not source_profile_ref.strip():
        return False
    if not source_profile_ref.startswith("$ref:"):
        return True
    component = components_by_key.get(source_profile_ref[len("$ref:") :])
    if component is None:
        return False
    if not str(getattr(component, "type", "") or "").startswith("profile."):
        return False
    config = component.config if isinstance(component.config, dict) else {}
    return config.get("reference_only") is True


def validate_watermark_source_field(
    declaration: Any,
    *,
    unit_index: int,
    components_by_key: Mapping[str, IntegrationComponentSpec],
    literal_indexes: Optional[Mapping[str, Any]] = None,
    selected_indexes: Optional[Mapping[str, Any]] = None,
) -> bool:
    """Rule (a) alone: the tracked field is a declared, mappable source leaf.

    Returns True when the rule was DECIDED, False when the referenced source
    profile has no index in reach — an unavailable answer, not a wrong one, and
    the same deferral the required-leaf coverage gate makes for an empty index.
    Split out so the online validation can decide the deferred case with the
    SELECTED artifact without re-running rule (b), whose authority (the root's
    owned operations) it does not carry.

    ``selected_indexes`` is the coverage gate's own parameter, keyed by
    component KEY — the reused artifact's index, which is a different key space
    and a different value shape from ``literal_indexes`` (UUID -> wrapper). The
    resolver takes both separately for exactly that reason; merging one into
    the other resolves nothing.
    """
    from ..categories.components.builders.transform_map_validation import (
        resolve_map_profile_index,
    )

    index = resolve_map_profile_index(
        declaration.source_profile_ref,
        dict(components_by_key),
        literal_indexes,
        dict(selected_indexes) if selected_indexes else None,
    )
    if index is None and not _watermark_source_is_deferrable(
        declaration.source_profile_ref, components_by_key
    ):
        # UNRESOLVABLE IS NOT THE SAME AS UNAVAILABLE. The resolver answers None
        # for three different questions: a reference that names no component, a
        # reference that names a non-profile, and a profile whose index simply is
        # not in reach here. Only the third is a deferral; reading all three as
        # one retired the reference check entirely, so a watermark over
        # `$ref:missing` or over a connector planned and compiled (Stage-2
        # round 5).
        raise _refuse(
            GOVERNANCE_WATERMARK_INCONSISTENT,
            message=(
                "The watermark references a source profile this request does "
                "not author."
            ),
            path="/units/{0}/envelope/watermark/source_profile_ref".format(unit_index),
            subject_kind="process",
            remediation=(
                "Reference an in-plan source profile ($ref:KEY) or an existing "
                "profile component id."
            ),
        )
    if index is None:
        return False
    entry = index.get(declaration.field)
    if entry is None or not entry.get("mappable", True):
        raise _refuse(
            GOVERNANCE_WATERMARK_INCONSISTENT,
            message=(
                "The watermark tracks a field that the referenced source profile "
                "does not declare as a mappable leaf."
            ),
            path="/units/{0}/envelope/watermark/field".format(unit_index),
            subject_kind="process",
            remediation=(
                "Reference an in-plan source profile ($ref:KEY) and name one of "
                "its mappable leaf paths."
            ),
        )
    return True


# ---------------------------------------------------------------------------
# secrets
# ---------------------------------------------------------------------------


def scan_secret_shaped_keys(value: Any, *, path: str) -> None:
    """Refuse a secret-shaped KEY anywhere in ``value`` — the archetype scanner, reused.

    Imports the scanner rather than copying its substring list, so the typed
    surface and the archetype contract cannot disagree on what a secret-shaped
    key is. Value-free: the diagnostic names the path scanned, never the key.
    """
    from ..patterns.archetype_parameters import _scan_for_secret_shaped_keys

    if _scan_for_secret_shaped_keys(_plain(value)):
        raise _refuse(
            PLAINTEXT_SECRET_REJECTED,
            message=(
                "A key whose name matches a secret-shaped substring was found; "
                "the typed surface never carries plaintext secrets."
            ),
            path=path,
            remediation=(
                "Reference connector secrets through the connection's "
                "credential_ref; recorded runtime hints and spec.runtime are "
                "echoed back and must not carry secrets."
            ),
        )


def scan_runtime_secrets(runtime: Any) -> None:
    """The archetype's recursive secret scan, applied to ``spec.runtime``."""
    if runtime:
        scan_secret_shaped_keys(runtime, path="/integration_spec/runtime")


def _plain(value: Any) -> Any:
    """Mappings and sequences as plain dict/list, so the scanner sees every key."""
    if isinstance(value, Mapping):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


def digest_of(value: Any) -> str:
    """Stable sha256 of a canonical JSON rendering — for records, never authority."""
    import json

    return hashlib.sha256(
        json.dumps(_plain(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    ).hexdigest()


__all__ = [
    "GovernanceRefusal",
    "GovernanceResolution",
    "PLAINTEXT_SECRET_REJECTED",
    "REUSE_BINDING_KEYS",
    "apply_connection_binding_contract",
    "derive_default_name",
    "iter_authored_refs",
    "owned_supporting_keys",
    "ref_key",
    "resolve_envelope",
    "resolve_governance",
    "scan_runtime_secrets",
    "scan_secret_shaped_keys",
    "validate_watermark_declaration",
]
