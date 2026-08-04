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
from typing import Any, Dict, Mapping, Optional, Tuple

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

#: Selector -> (schema version, zero-arg schema builder, symbolic provenance).
#:
#: Provenance is SYMBOLIC — ``runtime_schema_registry``, never a filesystem path.
#: A path in a discovery response is both a leak and a lie: it describes the
#: server's disk, which the caller cannot see and must not depend on.
AUTHORING_SCHEMA_REGISTRY: Mapping[str, Tuple[str, Any, str]] = MappingProxyType(
    {
        "ProcessIRV1": ("1", "_process_ir_schema", "runtime_schema_registry"),
        "SystemTopologySpecV1": (
            "1",
            "_system_topology_schema",
            "runtime_schema_registry",
        ),
        "AuthoringRequestV1": (
            "1",
            authoring_request_v1_json_schema,
            "runtime_schema_registry",
        ),
        "AuthoringPlanResultV1": (
            "1",
            authoring_plan_result_v1_json_schema,
            "runtime_schema_registry",
        ),
        "AuthoringCompileResultV1": (
            "1",
            authoring_compile_result_v1_json_schema,
            "runtime_schema_registry",
        ),
        "AuthoringRevisionBindingV1": (
            "1",
            authoring_revision_binding_v1_json_schema,
            "runtime_schema_registry",
        ),
        "AuthoringBuildProvenanceV1": (
            "1",
            authoring_build_provenance_v1_json_schema,
            "runtime_schema_registry",
        ),
        "validation_report": (
            "1",
            "_validation_report_schema",
            "semantic_validator",
        ),
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
        "authoring.process_ir": ("supported", "1", "runtime_schema_registry"),
        "authoring.system_topology.plan": ("supported", "1", "topology_planner"),
        "authoring.system_topology.deploy": (
            "unsupported",
            "1",
            "topology_planner",
        ),
        "authoring.recipe_contributions": ("supported", "1", "recipe_registry"),
        "authoring.integration_spec": ("supported", "1", "archetype_registry"),
        "authoring.compile": ("supported", "1", "canonical_compiler"),
        "authoring.revision_binding": ("supported", "1", "runtime_schema_registry"),
        # Published as unsupported rather than omitted, for the same reason as
        # topology deploy: a caller must be able to learn BEFORE authoring that a
        # ProcessIR root can be planned and compiled but not built. Nothing on a
        # production path materializes one — the compiler stops at the emission
        # plan, and promoting its emitter to a component writer is an ADR-001 §9
        # byte-parity cutover with its own issue.
        "authoring.typed_apply.process_materialization": (
            "unsupported",
            "1",
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
AUTHORING_PROCESS_COMPILING_INTENTS: Tuple[str, ...] = ("process_ir", "recipe")

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
        "authoring.typed_apply.process_materialization": "PROCESS_KIND_REQUIRED",
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


_LOCAL_BUILDERS = {
    "_process_ir_schema": _process_ir_schema,
    "_system_topology_schema": _system_topology_schema,
    "_validation_report_schema": _validation_report_schema,
}


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

    payload = get_schema_template_action(schema_name=selector)
    for key in _SCHEMA_BODY_KEYS:
        body = payload.get(key)
        if body:
            return sha256_fingerprint(body)
    raise KeyError(
        f"{selector!r} published no recognized schema body "
        f"(looked for {list(_SCHEMA_BODY_KEYS)})"
    )


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
    schema_revision = schema_fingerprint(schema_bundle)

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
    return sha256_fingerprint(payload)


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
