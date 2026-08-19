"""The single public authoring contract manifest (issue #146, M12.11).

**The problem this exists to solve.** The live MCP service reported four
archetypes while the checkout registry had six. Nothing was broken — the
deployment was simply older — but a client had no way to *find that out*, and a
capability catalog you cannot compare is a capability catalog you cannot trust.

So discovery, schema retrieval, planning, compilation, apply and verify all read
their action list, selector list, capability states and revisions from HERE.
``list_capabilities``, ``get_schema_template``, the ``build_integration``
dispatcher and the workflow docs cannot advertise different contracts, because
there is only one to advertise, and a parity test asserts they all agree.

**Revisions are DERIVED, never declared.** ``capability_revision`` is a hash of
the manifest the server can actually serve — built from the live archetype
registry, the live recipe registry snapshot, and the runtime models' own JSON
Schemas. A hand-maintained version string would drift from behavior exactly the
way the four-vs-six catalog did.

**Comparison is honest or absent.** ``compare_capability_revision(None)`` returns
``not_requested`` — never ``match``. A caller who did not supply an expectation
has not verified anything, and reporting parity they never asked for is the same
dishonesty in the other direction. The vocabulary (``not_requested`` / ``match``
/ ``mismatch`` / ``unknown``) is the one ``RecipeRegistrySkewV1`` already ships
on the sibling discovery surface: two words for one concept is the drift this
issue exists to remove.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict, Mapping, NamedTuple, Optional, Tuple

from ..errors import TOPOLOGY_APPLY_NOT_SUPPORTED
from ..models.authoring_workflow import (
    AUTHORING_ACTIONS,
    AUTHORING_CONTRACT_VERSION,
    AUTHORING_INTENT_KINDS,
    authoring_build_provenance_v1_json_schema,
    authoring_compile_result_v1_json_schema,
    authoring_plan_result_v1_json_schema,
    authoring_request_v1_json_schema,
    authoring_revision_binding_v1_json_schema,
)
from .revisions import capability_fingerprint, schema_fingerprint, sha256_fingerprint

class AuthoringSchemaRegistration(NamedTuple):
    """One served selector: version, schema builder, provenance, query builder.

    A ``NamedTuple`` rather than a dataclass ON PURPOSE. It is frozen and gives
    named access, AND it stays index-addressable — so every existing
    ``entry[0]``/``entry[1]``/``entry[2]`` reader keeps working while the named
    form is adopted. A shape change that silently broke an unread consumer would
    be the worst possible way to add a fourth field.

    ``projection_query`` names the local builder that answers a FILTERED
    retrieval, and is ``None`` for every selector that serves a plain schema. It
    is what distinguishes "a schema you fetch" from "a contract you query"
    without adding a second registry to hold the difference.
    """

    version: str
    builder: Any
    provenance: str
    projection_query: Optional[str] = None


#: Selector -> its registration.
#:
#: Provenance is SYMBOLIC — ``runtime_schema_registry``, never a filesystem path.
#: A path in a discovery response is both a leak and a lie: it describes the
#: server's disk, which the caller cannot see and must not depend on.
AUTHORING_SCHEMA_REGISTRY: Mapping[str, AuthoringSchemaRegistration] = MappingProxyType(
    {
        "ProcessIRV1": ("1", "_process_ir_schema", "runtime_schema_registry"),
        "SystemTopologySpecV1": (
            "1",
            "_system_topology_schema",
            "runtime_schema_registry",
        ),
        "AuthoringRequestV1": (
            "2",
            authoring_request_v1_json_schema,
            "runtime_schema_registry",
        ),
        "AuthoringPlanResultV1": (
            "2",
            authoring_plan_result_v1_json_schema,
            "runtime_schema_registry",
        ),
        "AuthoringCompileResultV1": (
            "2",
            authoring_compile_result_v1_json_schema,
            "runtime_schema_registry",
        ),
        "AuthoringRevisionBindingV1": (
            "2",
            authoring_revision_binding_v1_json_schema,
            "runtime_schema_registry",
        ),
        "AuthoringBuildProvenanceV1": (
            "2",
            authoring_build_provenance_v1_json_schema,
            "runtime_schema_registry",
        ),
        "validation_report": (
            "1",
            "_validation_report_schema",
            "semantic_validator",
        ),
        # #146 amendment. The compiler-facing authoring contract rides the SAME
        # registry as every other selector — a second one would be a second place
        # to look, and a second fingerprint to keep in step. It is the only entry
        # that carries a projection query, because it is the only selector whose
        # payload is filtered rather than whole.
        "process_ir_authoring": (
            "1",
            "_process_ir_authoring_schema",
            "runtime_schema_registry",
            "_process_ir_authoring_query",
        ),
    }
)

# Normalize every literal above into the descriptor. Written as plain tuples for
# readability and converted once here, so a reader sees the table rather than
# eleven constructor calls — and so a future entry cannot forget the wrapper.
AUTHORING_SCHEMA_REGISTRY = MappingProxyType(
    {
        selector: AuthoringSchemaRegistration(*entry)
        for selector, entry in AUTHORING_SCHEMA_REGISTRY.items()
    }
)

#: Capability id -> (state, schema version, symbolic evidence provenance).
#:
#: Topology DEPLOYMENT is listed as ``unsupported`` rather than omitted. An
#: absent capability is indistinguishable from one the client forgot to ask
#: about; a present-and-unsupported one carries the refusal code that explains
#: itself (#144 ships a planner and no apply path at all).
AUTHORING_CAPABILITY_REGISTRY: Mapping[str, Tuple[str, str, str]] = MappingProxyType(
    {
        # #153 (§6 review AR1-09): the six authorities whose PUBLIC PROMISE the
        # apply cutover changed bump to "2", atomically with the capability flip
        # — plan §8 lists them by name. The topology and recipe entries stay at
        # "1"; their promises did not move.
        "authoring.process_ir": ("supported", "2", "runtime_schema_registry"),
        "authoring.system_topology.plan": ("supported", "1", "topology_planner"),
        "authoring.system_topology.deploy": (
            "unsupported",
            "1",
            "topology_planner",
        ),
        "authoring.recipe_contributions": ("supported", "1", "recipe_registry"),
        "authoring.integration_spec": ("supported", "2", "archetype_registry"),
        "authoring.compile": ("supported", "2", "canonical_compiler"),
        "authoring.revision_binding": ("supported", "2", "runtime_schema_registry"),
        # #146 amendment. Published so a caller can discover, before authoring
        # anything, that the behavioural contract exists and that direct
        # ProcessIR planning is available without an archetype.
        "authoring.process_ir.contract": (
            "supported",
            "2",
            "runtime_schema_registry",
        ),
        "authoring.process_ir.pre_selection": (
            "supported",
            "2",
            "runtime_schema_registry",
        ),
        # #153 (M12.15): SUPPORTED. The cutover this entry was waiting for has
        # landed — the canonical chain compiles a ProcessIR root, binds real ids
        # during ordered apply, materializes it through the neutral
        # ProcessComponentMaterializer, and records a mutation attestation plus a
        # separate live-readback attestation. Version 2, because the capability's
        # public promise changed rather than its wording.
        "authoring.typed_apply.process_materialization": (
            "supported",
            "2",
            "canonical_compiler",
        ),
    }
)

#: Intent kinds whose compilation produces a ProcessIR root, and which therefore
#: cannot be applied — the runtime refusal keys on the compiled artifact, and
#: this is the same rule stated once for publication.
#:
#: It is a NAMED SET rather than a second conditional. The runtime predicate
#: originally keyed on ``intent_kind == "process_ir"`` and was later corrected to
#: key on the artifact; this matrix kept the old conditional and went on
#: advertising ``recipe.apply: supported`` for a route the server refuses. One
#: rule with two expressions is one rule that drifts.
#: #153 (M12.15): EMPTY. Both process-compiling intents can now be applied, so
#: the set that marked them apply-unsupported is empty rather than deleted — the
#: support matrix below DERIVES from it, and removing the name would move the
#: rule into the matrix as a hand-written literal, which is the drift this named
#: set was introduced to stop.
AUTHORING_PROCESS_COMPILING_INTENTS: Tuple[str, ...] = ()

#: Intent kind x action -> supported / unsupported.
#:
#: Every intent kind supports every READ-ONLY phase. ``apply`` is where they
#: differ, and the difference is real rather than cosmetic: applying means
#: materializing, and no production path materializes a ProcessIR root, so an
#: intent that compiled one cannot be built from its own binding.
#: ``verify`` is build-id scoped, so it is intent-agnostic.
AUTHORING_SUPPORT_MATRIX: Mapping[str, Mapping[str, str]] = MappingProxyType(
    {
        kind: MappingProxyType(
            {
                "plan": "supported",
                "compile": "supported",
                "apply": (
                    "unsupported"
                    if kind in AUTHORING_PROCESS_COMPILING_INTENTS
                    else "supported"
                ),
                "verify": "supported",
            }
        )
        for kind in AUTHORING_INTENT_KINDS
    }
)

#: The refusal a topology deploy request earns, published so a caller can see it
#: before spending a call.
TOPOLOGY_DEPLOY_REASON_CODE = TOPOLOGY_APPLY_NOT_SUPPORTED

#: Why each unsupported capability is unsupported, published alongside it. A bare
#: "unsupported" tells a caller to stop without telling them what to do instead.
_REASON_CODES: Mapping[str, str] = MappingProxyType(
    {
        "authoring.system_topology.deploy": TOPOLOGY_APPLY_NOT_SUPPORTED,
    }
)

_MANIFEST_CACHE: Dict[str, Any] = {}


def _process_ir_schema() -> Dict[str, Any]:
    from ..models.process_ir import process_ir_v1_json_schema

    return process_ir_v1_json_schema()


def _system_topology_schema() -> Dict[str, Any]:
    from ..models.system_topology import system_topology_v1_json_schema

    return system_topology_v1_json_schema()


def _validation_report_schema() -> Dict[str, Any]:
    """The summary this surface actually emits — NOT the compiler's own report.

    It previously served ``ValidationReportV1``, the compiler-internal contract.
    The two share zero property names and the schema is ``additionalProperties:
    false``, so the emitted object was not merely under-described — it was
    invalid against its own published schema (issue #146 QA, bug #406).

    Serving the compiler's model here would also have put a compiler internal on
    an LLM-facing surface, which ADR-001 §6 forbids.
    """
    from ..models.authoring_workflow import ValidationReportSummaryV1

    return ValidationReportSummaryV1.model_json_schema()


def _process_ir_authoring_schema() -> Dict[str, Any]:
    """The page schema. Imported lazily like every other compiler-adjacent one."""
    from ..models.process_ir_authoring import (
        process_ir_authoring_contract_v1_json_schema,
    )

    return process_ir_authoring_contract_v1_json_schema()


def _process_ir_authoring_query(**filters: Any) -> Dict[str, Any]:
    """Answer a FILTERED retrieval of the authoring contract.

    Returns the page as plain JSON data. The serving layer owns the envelope and
    the error translation; this stays a thin adapter so the projection has one
    caller and one shape.
    """
    from .process_ir_projection import query_process_ir_authoring_contract

    return query_process_ir_authoring_contract(**filters).model_dump(mode="json")


_LOCAL_BUILDERS = {
    "_process_ir_schema": _process_ir_schema,
    "_system_topology_schema": _system_topology_schema,
    "_validation_report_schema": _validation_report_schema,
    "_process_ir_authoring_schema": _process_ir_authoring_schema,
    "_process_ir_authoring_query": _process_ir_authoring_query,
}


def projection_query_builder(selector: str):
    """The filtered-retrieval builder for ``selector``, or ``None``.

    ``None`` is the normal answer: only the authoring contract is queryable, and
    every other selector serves its schema whole.
    """
    entry = AUTHORING_SCHEMA_REGISTRY.get(selector)
    if entry is None or not entry.projection_query:
        return None
    return _LOCAL_BUILDERS[entry.projection_query]


def schema_builder(selector: str):
    """The zero-arg callable that produces ``selector``'s JSON Schema.

    Indirection through a name for the three schemas whose modules would
    otherwise be imported at module scope — the compiler's validation contracts
    in particular, which this package must not pay for on import.
    """
    entry = AUTHORING_SCHEMA_REGISTRY.get(selector)
    if entry is None:
        return None
    builder = entry[1]
    if isinstance(builder, str):
        return _LOCAL_BUILDERS[builder]
    return builder


def authoring_schema_selectors() -> Tuple[str, ...]:
    """Every selector this contract serves, sorted."""
    return tuple(sorted(AUTHORING_SCHEMA_REGISTRY))


def schema_version_for(selector: str) -> Optional[str]:
    entry = AUTHORING_SCHEMA_REGISTRY.get(selector)
    return entry[0] if entry else None


def supported_schema_versions(selector: str) -> Tuple[str, ...]:
    """Sorted versions served for ``selector`` — empty when it is unknown here."""
    version = schema_version_for(selector)
    return (version,) if version else ()


def list_archetype_registry() -> Tuple[Dict[str, Any], ...]:
    """The archetypes THIS runtime can actually build, sorted by name.

    Derived from the live package scan, never a literal list: a hard-coded six
    would have reported six on the deployment that could only serve four, which
    is precisely the failure this manifest exists to make visible.

    Uses the package MODULE rather than the string ``"boomi_mcp.patterns"`` —
    the string form breaks under the ``src.boomi_mcp`` namespace.
    """
    from .. import patterns as patterns_pkg
    from ..patterns import PatternKind, PatternRegistry

    registry = PatternRegistry.from_package(patterns_pkg)
    entries = []
    for cls in registry.list_patterns(kind=PatternKind.ARCHETYPE):
        metadata = cls.metadata
        entries.append(
            {
                "name": metadata.name,
                "version": getattr(metadata, "version", "") or "",
                "migrated": _archetype_is_migrated(metadata.name),
            }
        )
    return tuple(sorted(entries, key=lambda entry: entry["name"]))


def _archetype_is_migrated(name: str) -> bool:
    """Whether this archetype runs through the typed recipe path.

    An unmigrated archetype is reported as ``migrated: false``, never omitted —
    hiding it would recreate the four-vs-six blindness inside the manifest.
    """
    try:
        from ..categories.integration_authoring import _ARCHETYPE_ADAPTERS

        return name in _ARCHETYPE_ADAPTERS
    except Exception:  # noqa: BLE001 — advisory metadata only
        return False


def _recipe_registry_block() -> Dict[str, Any]:
    """Registry identity only — entries stay on the recipe surface that owns them."""
    from ..recipes import production_registry

    snapshot = production_registry().snapshot()
    return {
        "schema_version": snapshot.get("schema_version"),
        "registry_revision": snapshot.get("registry_revision"),
        "source_version": snapshot.get("source_version"),
        # ``entry_kinds``, not ``contribution_kinds``. These are registry ENTRY
        # kinds (advisory / executable_recipe / …); the published
        # ``contribution_kinds`` vocabulary is the disjoint four-member
        # RecipeContributionV1 discriminator served by
        # get_schema_template("recipe_contributions"). Publishing one under the
        # other's name meant a client reading this and feeding it to the
        # discriminator was rejected every time.
        "entry_kinds": sorted(
            {str(entry.get("entry_kind", "")) for entry in snapshot.get("entries", ())}
        ),
        "entry_count": len(snapshot.get("entries", ())),
    }


#: Authoring selectors this contract does not OWN but which `get_schema_template`
#: serves, and whose movement must therefore move `schema_revision`.
#:
#: Without them the revision covered only the eight #146 selectors, so a change
#: to `IntegrationSpecV1` — the component/materialization plan every typed result
#: embeds — left every outstanding binding looking current. A revision that does
#: not move when the contract moves is the exact failure this manifest exists to
#: detect, one level in.
_INHERITED_SCHEMA_SELECTORS: Tuple[str, ...] = (
    "IntegrationSpecV1",
    "authoring_workflow",
    # #146 amendment. The cache/property surface publishes the state-visibility
    # model the authoring contract references, so a change to it is a change to
    # the contract a caller bound to — and until now it moved NOTHING.
    #
    # Adding the selector here is necessary but NOT sufficient on its own: the
    # served payload carries none of `_SCHEMA_BODY_KEYS`, so the digest would
    # have been permanently "unavailable" and the revision still would not have
    # moved. The manifest-free short-circuit in `_inherited_schema_digest` is
    # what makes this line load-bearing.
    "cache_property_authoring",
    "recipe_contributions",
    "recipe_registry",
    "workflow_sequences",
)



#: Envelope keys that actually carry a SCHEMA. Different surfaces use different
#: names — ``archetype:<name>`` answers with ``parameter_schema``, the rest with
#: ``json_schema`` — and there is deliberately no fallback to the whole envelope.
#:
#: The fallback is what made this wrong: hashing the envelope pulled in
#: ``examples``, ``limitations`` and ``capability_notes``, so a documentation-only
#: edit to an archetype moved ``schema_revision`` and ``capability_revision`` and
#: failed otherwise-valid typed applies on a revision check, while the accepted
#: parameters had not changed at all.
#: Ordered: the first key present wins. Four names because four surfaces chose
#: four names — and removing the envelope fallback is what revealed the last two.
#: ``recipe_registry`` in particular also carries a live ``snapshot``, so the
#: fallback had been folding REGISTRY STATE into ``schema_revision``, conflating
#: "the schema changed" with "the data changed".
_SCHEMA_BODY_KEYS: Tuple[str, ...] = (
    "json_schema",
    "parameter_schema",
    "expected_registry_schema",
    "record_schema",
)


def _inherited_schema_digest(selector: str) -> str:
    """Digest of the SCHEMA a served selector publishes — never its envelope.

    Raises when the payload carries no recognized schema body, so a surface that
    changes shape shows up as ``unavailable`` in the bundle rather than silently
    fingerprinting prose.
    """
    from ..categories.meta_tools import get_schema_template_action

    if selector == "authoring_workflow":
        # SHORT-CIRCUIT, before any fetch. This selector has no single schema
        # body — it IS the contract — and its served payload embeds the revision,
        # so fetching it here would call back into the manifest currently being
        # built. Reading the manifest-free source AFTER the fetch was not enough:
        # the recursion is in the fetch itself.
        # The WHOLE contract, not a filtered subset. Filtering it through a
        # hard-coded key list made a second catalog that could drift from the
        # first: a new contract member would change the served payload and leave
        # the revision unmoved. `authoring_workflow_contract()` is already
        # contract-only and manifest-free — it carries no revision and no prose —
        # so there is nothing to filter out, and anything added to it is covered
        # automatically.
        contract_body = authoring_workflow_contract()
        if contract_body:
            return sha256_fingerprint(contract_body)
        raise KeyError("authoring_workflow published no contract body")

    if selector == "cache_property_authoring":
        # SHORT-CIRCUIT for the same structural reason as `authoring_workflow`,
        # not the same recursion reason. This selector's payload has no single
        # schema body — it is a vocabulary surface — so the `_SCHEMA_BODY_KEYS`
        # lookup below finds nothing and would report "unavailable" forever.
        # Hashing the manifest-free contract body instead is what makes a
        # state-visibility change actually move `schema_revision`.
        from ..categories.meta_tools import cache_property_authoring_contract

        contract_body = cache_property_authoring_contract()
        if contract_body:
            return sha256_fingerprint(contract_body)
        raise KeyError("cache_property_authoring published no contract body")

    payload = get_schema_template_action(schema_name=selector)
    for key in _SCHEMA_BODY_KEYS:
        body = payload.get(key)
        if body:
            return sha256_fingerprint(body)
    raise KeyError(
        f"{selector!r} published no recognized schema body "
        f"(looked for {list(_SCHEMA_BODY_KEYS)})"
    )



def authoring_workflow_contract() -> Dict[str, Any]:
    """The workflow CONTRACT — phases, actions, intent kinds, terminology, flags.

    Manifest-free ON PURPOSE. ``get_schema_template("authoring_workflow")``
    decorates this with the current revision, and ``_schema_bundle`` fingerprints
    it; if the fingerprint had to go through the served payload, computing the
    manifest would recurse into the manifest.

    One source for both, so the digest cannot describe a contract different from
    the one served.
    """
    return {
        "read_only": True,
        "raw_xml_exposed": False,
        "boomi_mutation": False,
        "phases": [
            {
                "step": 1,
                "call": "list_capabilities()",
                "purpose": "Discover served actions, selectors and revisions.",
                "mutates_boomi": False,
            },
            {
                "step": 2,
                "call": "get_schema_template(schema_name='AuthoringRequestV1')",
                "purpose": "Obtain the exact strict request schema.",
                "mutates_boomi": False,
            },
            {
                "step": 3,
                "call": "plan_integration_design(...)",
                "purpose": (
                    "ADVISORY doctrine, gaps and typed next steps. Prose is "
                    "never compiled or executed."
                ),
                "mutates_boomi": False,
            },
            {
                "step": 4,
                "call": "build_from_archetype(...) or author ProcessIR / recipes",
                "purpose": "Produce typed semantic intent.",
                "mutates_boomi": False,
            },
            {
                "step": 5,
                "call": "build_integration(action='plan', config={'authoring_request': ...})",
                "purpose": (
                    "Semantic validation, resolved references, gaps, decisions, "
                    "and the IntegrationSpecV1 ComponentPlan preview."
                ),
                "mutates_boomi": False,
            },
            {
                "step": 6,
                "call": "build_integration(action='compile', config={'authoring_request': ...})",
                "purpose": (
                    "Canonical compilation: normalized intent, deterministic "
                    "artifact fingerprints, and the compile hash. Returns no "
                    "build_id, because no build exists. A request carrying "
                    "canonical PROCESS UNITS also compiles each one to a "
                    "relocatable materialization plan — account-independent, "
                    "carrying placeholder ids for every component reference — "
                    "and fingerprints it alongside the other artifacts."
                ),
                "mutates_boomi": False,
            },
            {
                "step": 7,
                "call": "build_integration(action='apply', ...)",
                "purpose": (
                    "The FIRST phase permitted to mutate. A typed apply must "
                    "carry expected_capability_revision and expected_compile_hash; "
                    "the server recomputes and compares both before its first write. "
                    "A canonical process unit is applied from the plan compiled "
                    "at step 6 — never a rebuild — with its placeholder ids bound "
                    "LATE, to the component ids published earlier in the same "
                    "apply. Each such write returns two SEPARATE attestations: a "
                    "mutation attestation over the exact bytes submitted, and a "
                    "live read-back attestation over what the platform then "
                    "served."
                ),
                "mutates_boomi": True,
            },
            {
                "step": 8,
                "call": "build_integration(action='verify', config={'build_id': ...})",
                "purpose": (
                    "Component/dependency verification, plus compiler and "
                    "artifact provenance for typed builds."
                ),
                "mutates_boomi": False,
            },
        ],
        "actions": list(AUTHORING_ACTIONS),
        "intent_kinds": list(AUTHORING_INTENT_KINDS),
        "terminology": {
            "pipeline_stages": "The inert PipelineSpec echo (ADR-001 §5).",
            "process_cfg": "The compiler's semantic control-flow graph.",
            "component_dependencies": "ComponentPlan materialization edges.",
            "topology_relations": "SystemTopologySpecV1 relations.",
            # #153 M12.15 (§6 AR3-05). The served workflow contract described a
            # pipeline that no longer matched what the server does: canonical
            # process units, the relocatable plan they compile to, the late
            # binding that turns its placeholders into real ids, and the two
            # separate attestations an apply returns were all absent from it.
            # The names are the served models' own, not hand-typed prose.
            "process_units": (
                "IntegrationSpecV1.processes — canonical ProcessIR roots, each "
                "one ProcessComponentEnvelopeV1 plus one ProcessIRV1."
            ),
            "materialization_plan": (
                "The relocatable, account-independent compile artifact for one "
                "process unit; its fingerprint is what apply executes against."
            ),
            "late_binding": (
                "Placeholder component ids in that plan are replaced with real "
                "ids at apply time, from components written earlier in the same "
                "apply — the plan itself is never string-patched."
            ),
            "mutation_attestation": (
                "ProcessMutationAttestationV1 — what was SUBMITTED: the action, "
                "the resulting component id, a digest of the exact bytes sent, "
                "and the placement actually observed."
            ),
            "readback_attestation": (
                "ProcessLiveReadbackAttestationV1 — what the platform SERVED "
                "back afterwards, recorded separately so an unavailable "
                "read-back reads as unknown rather than as agreement."
            ),
        },
    }


def _schema_bundle() -> Dict[str, str]:
    """Selector -> that schema's own digest.

    Hashing each schema separately (rather than one blob) is what lets a caller
    be told WHICH schema moved, not merely that something did.

    Covers the selectors this contract owns AND the served authoring selectors it
    inherits, plus the live archetype parameter surface — because the revision's
    job is to describe the whole authoring contract a client binds to, not only
    the part #146 introduced.
    """
    bundle = {}
    for selector in authoring_schema_selectors():
        builder = schema_builder(selector)
        try:
            bundle[selector] = sha256_fingerprint(builder())
        except Exception:  # noqa: BLE001 — a schema that cannot build is reported, not fatal
            bundle[selector] = "unavailable"
    for selector in _INHERITED_SCHEMA_SELECTORS:
        try:
            bundle[selector] = _inherited_schema_digest(selector)
        except Exception:  # noqa: BLE001
            bundle[selector] = "unavailable"
    # Each archetype's PARAMETER schema, under its own REAL selector. A changed
    # archetype input is a changed contract even when the archetype list is
    # identical, which a name-and-version list cannot show.
    #
    # Published per archetype rather than as one aggregate: an aggregate told a
    # client that something moved without telling it WHICH archetype moved, and
    # `archetype_parameters` was not a selector `get_schema_template` served at
    # all — so a client traversing the advertised catalog could neither fetch nor
    # verify it. Everything in this bundle is now fetchable under its own name.
    try:
        archetypes = list_archetype_registry()
    except Exception:  # noqa: BLE001
        archetypes = ()
    for entry in archetypes:
        selector = f"archetype:{entry['name']}"
        try:
            bundle[selector] = _inherited_schema_digest(selector)
        except Exception:  # noqa: BLE001
            bundle[selector] = "unavailable"
    return bundle


def build_authoring_contract_manifest() -> Mapping[str, Any]:
    """The whole public authoring contract, deterministically ordered.

    Computed once per process and returned immutable: it is derived from
    registries that do not change at runtime, and rebuilding it per request would
    make an expensive package scan part of every discovery call.
    """
    cached = _MANIFEST_CACHE.get("manifest")
    if cached is not None:
        return cached

    schema_bundle = _schema_bundle()
    # #146 amendment: the VERSION participates, not only the body hash.
    #
    # AUTHORING_WORKFLOW_V1 §11 recorded the gap — `schema_revision` covered
    # selector-to-body hashes, so bumping an owned selector's version while its
    # body happened to be unchanged left every outstanding binding looking
    # current. Folding the owned versions in closes it; the published `schemas`
    # list is unchanged, because this is an input to the hash, not a new field.
    schema_revision = schema_fingerprint(
        {
            "bundle": dict(schema_bundle),
            "owned_versions": {
                selector: AUTHORING_SCHEMA_REGISTRY[selector].version
                for selector in sorted(AUTHORING_SCHEMA_REGISTRY)
            },
        }
    )

    manifest: Dict[str, Any] = {
        "manifest_version": "1",
        "contract_version": AUTHORING_CONTRACT_VERSION,
        "actions": list(AUTHORING_ACTIONS),
        "intent_kinds": list(AUTHORING_INTENT_KINDS),
        # Every selector the revision COVERS, not only the ones this contract
        # owns. Folding a schema into the revision without publishing it left a
        # caller unable to see which schema moved — the revision said "something
        # changed" and the catalog said nothing changed.
        "schemas": [
            {
                "selector": selector,
                # OMITTED for inherited selectors rather than invented. An
                # earlier version published `schema_version: "inherited"`, which
                # combined with this contract's own `<selector>@<version>` syntax
                # to advertise pairs like `IntegrationSpecV1@inherited` that every
                # dispatcher rejects. A version we do not know is a key we do not
                # publish.
                **(
                    {"schema_version": AUTHORING_SCHEMA_REGISTRY[selector][0]}
                    if selector in AUTHORING_SCHEMA_REGISTRY
                    else {}
                ),
                "schema_hash": schema_bundle[selector],
                "provenance": (
                    AUTHORING_SCHEMA_REGISTRY[selector][2]
                    if selector in AUTHORING_SCHEMA_REGISTRY
                    else "runtime_schema_registry"
                ),
                "owned_by_authoring_contract": selector in AUTHORING_SCHEMA_REGISTRY,
            }
            for selector in sorted(schema_bundle)
        ],
        "capabilities": [
            {
                "capability_id": capability_id,
                "state": state,
                "schema_version": schema_version,
                "provenance": provenance,
                **(
                    {"reason_code": _REASON_CODES[capability_id]}
                    if capability_id in _REASON_CODES
                    else {}
                ),
            }
            for capability_id, (
                state,
                schema_version,
                provenance,
            ) in sorted(AUTHORING_CAPABILITY_REGISTRY.items())
        ],
        "support_matrix": {
            kind: dict(sorted(AUTHORING_SUPPORT_MATRIX[kind].items()))
            for kind in sorted(AUTHORING_SUPPORT_MATRIX)
        },
        "schema_revision": schema_revision,
        "compiler_revision": _compiler_revision(),
    }

    # Discovery of the two live registries is best-effort and reported as such.
    # A registry that fails to build must not take the whole capability response
    # down — the same discipline meta_tools already applies to archetype scan.
    try:
        manifest["archetypes"] = [dict(entry) for entry in list_archetype_registry()]
    except Exception:  # noqa: BLE001
        manifest["archetypes"] = []
        manifest["archetype_discovery"] = "unavailable"
    try:
        manifest["recipe_registry"] = _recipe_registry_block()
    except Exception:  # noqa: BLE001
        manifest["recipe_registry"] = {"status": "unavailable"}

    # #146 amendment: the COMPACT index only — counts, facets, mappings, limits.
    # Added BEFORE the fingerprint below, or the index would not be covered by
    # the revision that is supposed to describe it. The detailed entries stay
    # behind `get_schema_template`: `list_capabilities` is the call every client
    # makes first, and a 179-entry catalog inside it would make discovery the
    # most expensive step in the workflow.
    try:
        from .process_ir_projection import build_process_ir_authoring_index

        manifest["process_ir_authoring"] = build_process_ir_authoring_index()
    except Exception:  # noqa: BLE001
        manifest["process_ir_authoring"] = {"status": "unavailable"}

    manifest["capability_revision"] = capability_fingerprint(manifest)
    frozen = MappingProxyType(manifest)
    _MANIFEST_CACHE["manifest"] = frozen
    return frozen


def _compiler_revision() -> str:
    """Fingerprint of the compiler + validator + recipe capability contracts.

    All three are already-published contracts, so this moves when BEHAVIOR moves.
    Deliberately not a source hash or a git SHA: equivalent packaged code must
    produce the same revision, or a rebuilt-but-identical deployment reports
    drift against itself.
    """
    payload: Dict[str, Any] = {}
    try:
        from ..models.process_ir import PROCESS_IR_V1_CAPABILITIES

        payload["process_ir_capabilities"] = dict(PROCESS_IR_V1_CAPABILITIES)
    except Exception:  # noqa: BLE001
        payload["process_ir_capabilities"] = "unavailable"
    try:
        from ..compiler.process_ir.semantic_validation.contracts import (
            VALIDATION_PHASE_ORDER,
        )

        payload["validation_phase_order"] = list(VALIDATION_PHASE_ORDER)
    except Exception:  # noqa: BLE001
        payload["validation_phase_order"] = "unavailable"
    try:
        from ..recipes import production_registry

        payload["recipe_capability_revisions"] = dict(
            production_registry().snapshot().get("capability_revisions", {})
        )
    except Exception:  # noqa: BLE001
        payload["recipe_capability_revisions"] = "unavailable"

    # #146 amendment: every BEHAVIOUR authority the served authoring contract
    # projects. Without these the contract could publish a changed placement
    # rule, a changed replay classification or a changed remediation while
    # `compiler_revision` stood still — and a revision that does not move when
    # behaviour moves is the failure this whole manifest exists to detect.
    #
    # Each in its own try/except, matching the discipline above: one authority
    # that fails to import is reported as unavailable rather than taking the
    # whole capability response down.
    for key, loader in (
        (
            "body_placement_rows",
            lambda: [
                list(row[:2]) + [list(row[2])]
                for row in _import("body_capabilities").body_placement_rows()
            ],
        ),
        (
            "connector_capability_rows",
            lambda: [
                dict(row)
                for row in _import("connector_capabilities").connector_capability_rows()
            ],
        ),
        (
            "retry_rules",
            lambda: [dict(row) for row in _import("error_handling").retry_rule_specs()],
        ),
        (
            "state_visibility",
            lambda: [
                dict(row)
                for row in _import(
                    "semantic_validation.lineage"
                ).state_visibility_rows()
            ],
        ),
        ("process_property_scope", _process_property_scope_payload),
        (
            # The EXECUTION-PROFILE derivation (§6 AR3-07). The served revision
            # is a manifest of the compiler behaviours a caller binds to, and
            # this one was missing: replacing the derivation left the revision
            # unchanged, so a caller could hold a binding across a change in
            # how a process is classified. Read through `execution_profile` —
            # NOT through `contracts` — because the derivation imports the set
            # by value, so a projection of the contracts name would be pinned
            # to a binding the derivation no longer consults, and the
            # non-vacuity witness would pass without covering anything.
            "execution_profile_contract",
            _execution_profile_behaviour_oracle,
        ),
        (
            "compiler_diagnostic_specs",
            lambda: [
                dict(spec)
                for spec in _import("diagnostics").compiler_diagnostic_specs()
            ],
        ),
        (
            "validation_finding_specs",
            lambda: [
                dict(spec)
                for spec in _import("semantic_validation.findings").finding_specs()
            ],
        ),
        ("parse_diagnostic_specs", _parse_diagnostic_specs_payload),
        ("process_ir_authoring_contract", _authoring_projection_payload),
    ):
        try:
            payload[key] = loader()
        except Exception:  # noqa: BLE001
            payload[key] = "unavailable"
    return sha256_fingerprint(payload)


def _import(module: str):
    """Import one compiler submodule lazily, by dotted suffix."""
    import importlib

    return importlib.import_module(f"..compiler.process_ir.{module}", __package__)


def _cfg_semantic_members():
    """The CFG semantic union's own member models.

    `CfgSemanticV1` is `Annotated[Union[...], Field(discriminator=...)]`, so the
    members are read off the annotation rather than re-listed — a re-listing is
    the hand-copy the oracle above exists to stop making.
    """
    import typing

    annotated = _import("contracts").CfgSemanticV1
    union = typing.get_args(annotated)[0]
    return typing.get_args(union)


def _entry_roles(member) -> Tuple[str, ...]:
    """The roles this member may carry AT THE ENTRY POSITION.

    The probe below always installs its node as the CFG entry, so a role the
    compiler forbids there describes a graph production can never hand the
    derivation. Probing it anyway meant a change confined to that unreachable
    shape rotated the served `compiler_revision` and invalidated every caller's
    binding without any supported behaviour having changed (L2 round 45) — the
    first time this oracle's failure mode moved from under-reporting to
    imposing a cost on callers.

    The restriction is read from `ENTRY_ROLE_RESTRICTIONS`, declared beside the
    invariant that enforces it, so the probe and the compiler cannot disagree
    about which shapes exist. A kind that declares no restriction is probed with
    every role it admits.
    """
    declared = _literal_options(member, "role")
    if not declared:
        return ()
    kind = member.model_fields["semantic_kind"].default
    allowed = _import("invariants").ENTRY_ROLE_RESTRICTIONS.get(kind)
    if allowed is None:
        return declared
    return tuple(role for role in declared if role in allowed)


def _connector_member():
    """The CFG union's `connector` member — found by its discriminator, not by
    position in the union nor by importing its class name."""
    for member in _cfg_semantic_members():
        if member.model_fields["semantic_kind"].default == "connector":
            return member
    raise LookupError("the CFG semantic union declares no `connector` member")


def _listener_role(classify, families) -> str:
    """The `connector` role the derivation ACTUALLY classifies as a listener.

    Asked of the rule, not read off the schema (L2 round 45). The previous
    version took `ConnectorSemanticV1`'s first declared `Literal`, which is
    `source` today and correct today — but declaration order carries no meaning,
    and reordering that vocabulary would have returned `target` while the rule
    still tested `source`. The three discriminant rows below would then all have
    stopped at the role check, so removing the operation or family guard would
    have left the served revision unchanged: three inert rows that still look
    like coverage.

    Falls back to the first declared option only if no role yields `listener` at
    all — a state in which the discriminants cannot discriminate anyway, and one
    the accompanying witness fails on.
    """
    options = _literal_options(_connector_member(), "role")
    if families:
        for role in options:
            if classify(role) == _import("execution_profile").LISTENER:
                return role
    return options[0] if options else "source"


def _literal_options(model, field_name: str) -> Tuple[str, ...]:
    """The `Literal` values a model's field admits, or `()` if it has no such field.

    Read off the annotation so a probe uses the member's OWN vocabulary. The
    alternative — one hand-picked value applied to every member — is what put
    `role="source"` on a `connector_call` node, whose schema admits only
    `entry|downstream` (L2 round 44).
    """
    import typing

    field = model.model_fields.get(field_name)
    if field is None:
        return ()
    options = typing.get_args(field.annotation)
    return tuple(str(option) for option in options if isinstance(option, str))


def _execution_profile_behaviour_oracle() -> Dict[str, Any]:
    """The execution-profile derivation, projected as BEHAVIOUR (§6 AR4-01).

    The first attempt projected the derivation's vocabulary — the two profile
    labels and the listener family set. That moves when the family table moves,
    which is what its witness measured, but it is not what a caller binds to: the
    §6 gate replaced `derive_process_execution_profile` with an always-scheduled
    implementation and the served revision did not change by a byte. A caller
    holding that revision would have kept validating across a change in how every
    process is classified.

    So the projection CALLS the rule instead of describing it. The case set is
    derived from the same authority the rule reads — one case per listener
    connector family, so adding or retiring a family still moves the revision —
    plus the fixed discriminants that separate this rule from a constant: no
    entry node, a non-connector entry, a connector acting as a target, a source
    whose family is not a listener family, an operation reference that resolves
    to no symbol, and a family that needs case-folding to match.

    The stand-ins are plain attribute holders rather than the compiler's models
    on purpose: the rule reads its inputs by attribute, the projection must be
    cheap and hermetic (it runs whenever a revision is served), and building
    validated CFGs here would bind the served revision to model construction
    rather than to the classification rule this entry exists to cover.
    """
    from types import SimpleNamespace

    module = _import("execution_profile")
    derive = module.derive_process_execution_profile
    families = sorted(module.LISTENER_CONNECTOR_TYPES)

    def _classify(node, symbols=(), entry_id="entry"):
        cfg = SimpleNamespace(
            entry_node_id=entry_id, nodes=() if node is None else (node,)
        )
        return derive(cfg, SimpleNamespace(symbols=tuple(symbols)))

    def _connector(role, operation_ref):
        return SimpleNamespace(
            node_id="entry",
            semantic=SimpleNamespace(
                semantic_kind="connector", role=role, operation_ref=operation_ref
            ),
        )

    def _symbol(ref, connector_type):
        return SimpleNamespace(ref=ref, connector_type=connector_type)

    # EVERY entry-node kind the CFG schema admits, read from the schema (L2
    # round 43). The first version stood one hand-picked `message` node in for
    # "any non-connector entry", which is the same hand-enumeration defect this
    # oracle was written to fix, one level down: `SemanticCfgV1` already admits
    # `connector_call`, so a derivation that began classifying listener-family
    # connector calls would leave every exercised case — and therefore the served
    # revision — byte-identical. The kinds come from the discriminated union's
    # own members, so a kind added to the schema joins the case set on its own.
    cases = {
        "entry-absent": _classify(None),
    }
    for member in _cfg_semantic_members():
        kind = member.model_fields["semantic_kind"].default
        if kind == "connector":
            continue  # covered in both roles, per family, below
        cases["entry-kind-" + kind] = _classify(
            SimpleNamespace(
                node_id="entry", semantic=SimpleNamespace(semantic_kind=kind)
            )
        )
        # ...and the same kind carrying the fields the connector arm reads, so a
        # rule that started honouring role/operation on ANOTHER kind is visible
        # too. The roles come from THIS member's own declaration (L2 round 44):
        # the previous version stamped `role="source"` on every kind, and
        # `ConnectorCallSemanticV1.role` admits `entry|downstream` — so the one
        # kind most likely to become listener-eligible was probed with a role its
        # schema rejects, and a regression classifying a valid
        # `(connector_call, entry)` node would have left both of its rows
        # scheduled and the revision unmoved. Guessing a field's vocabulary
        # instead of reading it is the same defect as guessing the kind set.
        for role in _entry_roles(member) or (None,):
            semantic = {"semantic_kind": kind, "operation_ref": "$ref:op"}
            suffix = "-listener-shaped"
            if role is not None:
                semantic["role"] = role
                suffix = "-role-" + role
            cases["entry-kind-" + kind + suffix] = _classify(
                SimpleNamespace(node_id="entry", semantic=SimpleNamespace(**semantic)),
                [_symbol("$ref:op", families[0])] if families else [],
            )
    listener_role = _listener_role(
        lambda role: _classify(
            _connector(role, "$ref:op"), [_symbol("$ref:op", families[0])]
        )
        if families
        else None,
        families,
    )
    cases.update({
        # The listener-eligible role, ASKED OF THE RULE rather than read off the
        # schema: these three discriminants are only discriminating if the role
        # they carry is the one the rule can actually say "listener" for.
        "entry-id-names-no-node": _classify(
            _connector(listener_role, "$ref:op"),
            [_symbol("$ref:op", families[0] if families else "http")],
            entry_id="somewhere-else",
        ),
        "operation-unresolved": _classify(
            _connector(listener_role, "$ref:missing"),
            [_symbol("$ref:op", families[0] if families else "http")],
        ),
        # THE ONLY ROW THAT CAN PIN THE scheduled -> listener DIRECTION, and the
        # only reason it can is the decoys (§6 AR5-01). The entry's reference
        # resolves here and the answer is `scheduled`, so a lookup that answers
        # from the WRONG ROW — a `symbols[0]` refactor, a wrong loop variable, or
        # "any listener family anywhere in the table" — flips this row and only
        # this row. Every other row passes at most one symbol, which is why the
        # whole case set was byte-identical under three such mutants.
        #
        # Real tables are never one symbol: `build_symbol_table` projects every
        # component of the spec into one table sorted by `$ref:KEY`, so a
        # listener-family operation used anywhere in a request that COMPILES
        # sits in the same table as the entry's operation, at a caller-chosen
        # index. That is the reachable damage — a correctly-scheduled process
        # stamped with listener `<process>` bytes — and the apply-time re-derive
        # cannot catch it because it calls the same function.
        #
        # The decoy refs match nothing, so this row's VALUE is unchanged and the
        # served revision does NOT rotate: detection is added at zero cost to
        # every outstanding caller binding. Decoys sit on BOTH sides of the
        # referenced symbol so neither "take the first" nor "take the last"
        # passes.
        "non-listener-family": _classify(
            _connector(listener_role, "$ref:op"),
            [
                _symbol("$ref:a-decoy", families[0] if families else "http"),
                _symbol("$ref:op", "database"),
                _symbol("$ref:z-decoy", families[0] if families else "http"),
            ],
        ),
    })
    # The `connector` rows, per family and per role. The ROLES come from
    # `ConnectorSemanticV1`'s own declaration for the same reason the other
    # members' do (L2 round 44's sibling sweep): the pair happened to be spelled
    # correctly here, but a hand-typed literal beside a schema that declares it
    # is the same defect whether or not today's spelling is right. Covering every
    # admitted role is also what makes the role test part of the covered
    # behaviour rather than an untested branch.
    connector_roles = _literal_options(_connector_member(), "role")
    for family in families:
        symbols = [_symbol("$ref:op", family)]
        for role in connector_roles:
            cases["connector-%s-%s" % (role, family)] = _classify(
                _connector(role, "$ref:op"), symbols
            )
            # ...and the same family spelled the way the rule has to normalize
            # it. This twin carries the multi-symbol table for the OTHER
            # direction (listener -> scheduled): a lookup that stops at the
            # first symbol, or refuses a table of more than one, answers
            # `scheduled` here. Its plain sibling above stays single-symbol on
            # purpose, so both arities remain represented in the case set.
            cases["connector-%s-unnormalized-%s" % (role, family)] = _classify(
                _connector(role, "$ref:op"),
                [
                    _symbol("$ref:a-decoy", "database"),
                    _symbol("$ref:op", "  " + family.upper() + "  "),
                ],
            )
    return {
        "profiles": sorted({module.SCHEDULED, module.LISTENER}),
        "cases": dict(sorted(cases.items())),
    }


def _process_property_scope_payload() -> Dict[str, Any]:
    from ..models.cache_property_models import PROCESS_PROPERTY_SCOPE_V1

    return dict(PROCESS_PROPERTY_SCOPE_V1)


def _parse_diagnostic_specs_payload() -> Any:
    from ..models.process_ir import process_ir_v1_parse_diagnostic_specs

    return [dict(spec) for spec in process_ir_v1_parse_diagnostic_specs()]


def _authoring_projection_payload() -> Any:
    from .process_ir_projection import process_ir_authoring_revision_payload

    return process_ir_authoring_revision_payload()


def get_authoring_revisions() -> Dict[str, str]:
    """The three revisions every authoring response carries."""
    manifest = build_authoring_contract_manifest()
    return {
        "contract_version": manifest["contract_version"],
        "schema_revision": manifest["schema_revision"],
        "capability_revision": manifest["capability_revision"],
        "compiler_revision": manifest["compiler_revision"],
    }


def compare_capability_revision(expected: Optional[str]) -> Dict[str, Any]:
    """Compare a caller's expected capability revision against this runtime's.

    ``expected is None`` is ``not_requested`` — NOT ``match``. The caller has
    verified nothing, and a response claiming parity they never asked for is
    exactly the false confidence this issue exists to remove.
    """
    revisions = get_authoring_revisions()
    actual = revisions["capability_revision"]
    if expected is None or not str(expected).strip():
        return {
            "status": "not_requested",
            "actual_capability_revision": actual,
        }
    if str(expected).strip() == actual:
        return {
            "status": "match",
            "actual_capability_revision": actual,
            "expected_capability_revision": str(expected).strip(),
        }
    return {
        "status": "mismatch",
        "actual_capability_revision": actual,
        "expected_capability_revision": str(expected).strip(),
        "remediation": (
            "Re-run list_capabilities and get_schema_template, then re-plan and "
            "recompile before applying."
        ),
    }


def reset_manifest_cache() -> None:
    """Drop the memoized manifest. For tests that perturb a live registry."""
    _MANIFEST_CACHE.clear()


__all__ = [
    "AUTHORING_ACTIONS",
    "AUTHORING_CAPABILITY_REGISTRY",
    "AUTHORING_CONTRACT_VERSION",
    "AUTHORING_SCHEMA_REGISTRY",
    "AUTHORING_SUPPORT_MATRIX",
    "authoring_schema_selectors",
    "build_authoring_contract_manifest",
    "compare_capability_revision",
    "get_authoring_revisions",
    "list_archetype_registry",
    "reset_manifest_cache",
    "schema_builder",
    "schema_version_for",
    "supported_schema_versions",
]
