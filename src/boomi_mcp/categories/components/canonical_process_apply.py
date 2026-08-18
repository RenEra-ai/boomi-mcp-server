"""Apply ONE canonical ProcessIR root, and attest what happened (issue #153).

This is the provider the existing apply orchestration calls when an execution
step names a process ROOT rather than a component. It deliberately owns as
little as possible: routing, preflight, dependency ordering, collision handling,
create/update and preservation all stay in ``integration_builder``. What lives
here is the part that did not exist before — late symbol binding, recompilation
against the real account, materialization, and the two attestations.

Kept out of ``integration_builder`` on purpose. That module is ~377 KB of shared
apply code, and this repository's structural-fix rule treats a second defect in
shared apply code as needing its own dedicated batch with its own validation. A
small, separately testable module is the difference between "one branch was
added" and "the shared apply loop was edited".

**Late binding, and why the plan is recompiled rather than patched.** The
materialization plan was compiled with PLACEHOLDER ids (``id-<key>``) so its
fingerprint is relocatable. The real Boomi ids only exist once the dependencies
have been applied, in topological order. Producing the final XML therefore means
recompiling the SAME root against a symbol table carrying the real ids — never
string-substituting placeholders in already-emitted XML. Lowering embeds resolved
ids in several node kinds; a textual patch would have to re-implement that
knowledge, and would drift from it.
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

_REF_PREFIX = "$ref:"


class CanonicalProcessApplyError(Exception):
    """A named failure applying one canonical root. Carries the error code."""

    def __init__(self, message: str, *, error_code: str, component_key: str) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.component_key = component_key


def _resolve_ref(ref: str, id_registry: Mapping[str, str], component_key: str) -> str:
    """A ``$ref:KEY`` token -> the real component id the registry published."""
    key = ref[len(_REF_PREFIX):] if ref.startswith(_REF_PREFIX) else ref
    resolved = id_registry.get(key)
    if not resolved:
        raise CanonicalProcessApplyError(
            "process {0!r} depends on {1!r}, which has no applied component id"
            .format(component_key, key),
            error_code="PROCESS_MATERIALIZATION_SYMBOL_BINDING_INVALID",
            component_key=component_key,
        )
    return resolved


def _ref_key(ref: str) -> str:
    """The logical key inside a ``$ref:KEY`` token (or the token itself)."""
    return ref[len(_REF_PREFIX):] if ref.startswith(_REF_PREFIX) else ref


def bind_symbols_to_applied_ids(
    symbols, id_registry: Mapping[str, str], component_key: str, *, required_keys=()
):
    """The plan's placeholder symbol table, rebound to REAL applied ids.

    **Which symbols MUST resolve is derived, not assumed.** The first draft
    demanded an applied id for EVERY symbol in the table, and that made the
    capability unreachable in two distinct shapes (QA-153-r2-04): the table
    deliberately carries every canonical root — including the one being created,
    which by definition has no id yet — and, in a multi-root spec, the sibling
    roots that ordered apply has not reached. Both failed 100% of the time, on
    the first call, for reasons that had nothing to do with the reference being
    bound.

    The authority is the root's DECLARED dependency set. A ``$ref`` the IR uses
    but ``depends_on`` does not declare is already refused at compile
    (``INTEGRATION_DEPENDENCY_REQUIRED``), so ``depends_on`` is a superset of
    what this root can reference — which makes it, and not "every symbol in the
    table", the correct universe. A declared dependency with no applied id is
    still a hard failure: topological order guarantees it was applied first, so
    its absence is a real ordering defect.

    Everything else keeps its placeholder. That is safe because it is CHECKED
    rather than trusted: :func:`materialize_canonical_process_xml` refuses any
    emitted artifact in which a placeholder actually survived, so a reference
    this rule wrongly judged unreachable fails closed at the artifact instead of
    shipping ``id-db_conn`` into a component that looks applied.
    """
    from ...compiler.process_ir.contracts import SymbolTableV1

    required = set(required_keys)
    rebound = []
    for symbol in symbols.symbols:
        key = _ref_key(symbol.ref)
        if key in id_registry:
            component_id = id_registry[key]
        elif key in required:
            # Declared, ordered before this root, and still unapplied.
            raise CanonicalProcessApplyError(
                "process {0!r} depends on {1!r}, which has no applied component id"
                .format(component_key, key),
                error_code="PROCESS_MATERIALIZATION_SYMBOL_BINDING_INVALID",
                component_key=component_key,
            )
        else:
            component_id = symbol.component_id
        rebound.append(symbol.model_copy(update={"component_id": component_id}))
    return SymbolTableV1(
        symbols=tuple(rebound), idempotency_contracts=symbols.idempotency_contracts
    )


def _surviving_placeholders(xml: str, symbols) -> Tuple[str, ...]:
    """Placeholder ids and ``$ref`` tokens that reached the emitted artifact.

    The fail-closed half of the binding rule above. Scanned for the CONCRETE
    placeholder each unbound symbol would have contributed
    (``placeholder_component_id``) rather than by a generic pattern, so the check
    names the symbol that leaked instead of merely reporting that something did.
    """
    from ...recipes.materialization import placeholder_component_id

    found = {
        placeholder
        for symbol in symbols.symbols
        for placeholder in (placeholder_component_id(symbol.ref),)
        if placeholder in xml
    }
    found.update(
        symbol.ref for symbol in symbols.symbols if symbol.ref and symbol.ref in xml
    )
    return tuple(sorted(found))


def resolve_extension_connections(
    envelope, id_registry: Mapping[str, str]
) -> Tuple[Dict[str, Any], ...]:
    """The envelope's typed extension bindings, with ``$ref`` tokens resolved.

    Returns the normalized dict shape the override renderer consumes. The
    renderer is shared with the legacy path, so the shape is its contract, not a
    new one.
    """
    resolved = []
    for connection in envelope.process_extensions.connections:
        entry: Dict[str, Any] = {
            "connection_id": _resolve_ref(
                connection.connection_id, id_registry, envelope.component_key
            ),
        }
        # Every field, in order — the renderer emits them verbatim, so a reorder
        # here would move emitted bytes.
        entry["fields"] = [
            {
                key: value
                for key, value in (
                    ("id", field.id),
                    ("label", field.label),
                    ("xpath", field.xpath),
                )
                if value is not None
            }
            for field in connection.fields
        ]
        if connection.connector_type:
            entry["connector_type"] = connection.connector_type
        resolved.append(entry)
    return tuple(resolved)


def materialize_canonical_process_xml(
    *,
    plan,
    id_registry: Mapping[str, str],
    symbols,
) -> str:
    """Recompile the plan's root against REAL ids and produce deployable XML.

    Verifies the plan against its own fingerprint FIRST. A plan whose recorded
    fingerprint does not match its material is not the plan that was compiled,
    and materializing it would attest a build nobody certified.
    """
    from ...authoring.process_materialization import process_plan_fingerprint
    from ...compiler.process_ir.emitter_registry import emit_process
    from ...compiler.process_ir.execution_profile import (
        derive_process_execution_profile,
    )
    from ...compiler.process_ir.pipeline import compile_process_ir_v1
    from .process_component_materializer import ProcessComponentMaterializer

    envelope = plan.envelope
    key = envelope.component_key

    recomputed, _material = process_plan_fingerprint(plan)
    if recomputed != plan.plan_fingerprint:
        raise CanonicalProcessApplyError(
            "the materialization plan for {0!r} does not match its recorded "
            "fingerprint".format(key),
            error_code="PROCESS_MATERIALIZATION_FINGERPRINT_MISMATCH",
            component_key=key,
        )

    bound = bind_symbols_to_applied_ids(
        symbols, id_registry, key, required_keys=envelope.depends_on
    )
    cfg, emission_plan = compile_process_ir_v1(plan.process_ir, bound)

    # The profile is RE-DERIVED and compared, never re-decided. Recompiling
    # against real ids must not change what kind of process this is; if it does,
    # the plan and the artifact disagree and the mismatch is the finding.
    profile = derive_process_execution_profile(cfg, bound)
    if profile != plan.execution_profile:
        raise CanonicalProcessApplyError(
            "recompiling {0!r} against applied ids changed its execution profile "
            "({1!r} -> {2!r})".format(key, plan.execution_profile, profile),
            error_code="PROCESS_MATERIALIZATION_EXECUTION_PROFILE_INVALID",
            component_key=key,
        )

    # The BOUND table, so emitted shapes carry real ids rather than placeholders.
    artifact = emit_process(emission_plan, bound)
    xml = ProcessComponentMaterializer().materialize(
        artifact.shape_xml_parts,
        name=envelope.name,
        execution_profile=plan.execution_profile,
        description=envelope.description,
        folder_name=envelope.folder_name,
        extension_connections=resolve_extension_connections(envelope, id_registry),
    )

    # The binding rule above resolves the DECLARED dependencies and leaves every
    # other symbol on its placeholder. This is the check that makes that rule
    # safe rather than optimistic: if a placeholder or a raw `$ref` token
    # actually reached the artifact, the rule was wrong about what this root
    # references, and the honest outcome is a refusal — not a component whose
    # `connectionId` is the string `id-db_conn`, which Boomi stores happily and
    # which nothing downstream would flag.
    leaked = _surviving_placeholders(xml, bound)
    if leaked:
        raise CanonicalProcessApplyError(
            "the materialized XML for {0!r} still carries unbound reference(s): "
            "{1}".format(key, ", ".join(leaked)),
            error_code="PROCESS_MATERIALIZATION_SYMBOL_BINDING_INVALID",
            component_key=key,
        )
    return xml


def build_mutation_attestation(
    *,
    plan,
    action: str,
    target_component_id: Optional[str],
    result_component_id: Optional[str],
    submitted_xml: str,
    account_scope_hash: str,
    resolved_folder_id: Optional[str] = None,
):
    """The apply-time mutation attestation for one root.

    ``submitted_xml`` is digested HERE, from the exact bytes that were sent —
    for an update that is the MERGED result of read-merge-write, not the desired
    XML, because the merged bytes are what the platform received.

    A create that reports success without a component id fails closed: an
    attestation naming no result describes a mutation nobody can verify, and
    recording it would be worse than refusing.
    """
    from ...authoring.revisions import sha256_fingerprint
    from ...models.authoring_workflow import (
        ProcessMutationAttestationV1,
        ResolvedProcessPlacementV1,
    )

    key = plan.envelope.component_key
    if not result_component_id:
        raise CanonicalProcessApplyError(
            "the {0} of process {1!r} reported success without a component id, so "
            "the mutation cannot be attested".format(action, key),
            error_code="PROCESS_MATERIALIZATION_RESULT_ID_MISSING",
            component_key=key,
        )

    return ProcessMutationAttestationV1(
        component_key=key,
        plan_fingerprint=plan.plan_fingerprint,
        account_scope_hash=account_scope_hash,
        action=action,
        # A create never names a target; an update's target IS its result.
        target_component_id=target_component_id if action == "update" else None,
        result_component_id=result_component_id,
        resolved_placement=ResolvedProcessPlacementV1(
            folder_name=plan.envelope.folder_name,
            folder_id=resolved_folder_id or plan.resolved_folder_id,
        ),
        submitted_xml_digest=sha256_fingerprint({"component_xml": submitted_xml}),
    )


def build_readback_attestation(*, component_key: str, component_id: str, digest):
    """The post-apply live readback, recorded SEPARATELY from the mutation.

    ``digest=None`` when the readback failed. The mutation stands regardless —
    that is the whole reason the two are separate records.
    """
    from ...models.authoring_workflow import ProcessLiveReadbackAttestationV1

    return ProcessLiveReadbackAttestationV1(
        component_key=component_key, component_id=component_id, digest=digest
    )


__all__ = [
    "CanonicalProcessApplyError",
    "bind_symbols_to_applied_ids",
    "build_mutation_attestation",
    "build_readback_attestation",
    "materialize_canonical_process_xml",
    "resolve_extension_connections",
]
