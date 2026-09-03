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

import hashlib
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
    # As in ``placeholder_backed_symbols``: the carried fields are derived, not
    # listed, so a field added to the table cannot be lost by rebuilding it here.
    return SymbolTableV1.rebinding(symbols, rebound)


def _surviving_placeholders(xml: str, symbols) -> Tuple[str, ...]:
    """Placeholder ids and ``$ref`` tokens that reached the emitted artifact.

    The fail-closed half of the binding rule above.

    **Matched as a WHOLE attribute value, never as a substring** (Codex round 1).
    The first version searched the serialized document for ``id-<key>`` anywhere,
    so a process whose component key is ``db`` and whose ``message`` text
    mentions ``id-db`` was refused — after its dependencies had already been
    created — even though every structural reference had rebound correctly. A
    structural reference IS the entire value of an attribute
    (``connectionId="id-db"``); authored prose containing the same token is not.
    Exact attribute-value equality separates the two without enumerating which
    attributes carry references, which would be a hand-model of the emitter.

    Only symbols left UNBOUND can leak: one that took a real id no longer has a
    placeholder in the table to find.
    """
    import xml.etree.ElementTree as ET

    from ...recipes.materialization import placeholder_component_id

    suspect = {}
    for symbol in symbols.symbols:
        # `component_id` still equal to the placeholder means this symbol was
        # left unbound by the declared-dependency rule.
        placeholder = placeholder_component_id(symbol.ref)
        if symbol.component_id == placeholder:
            suspect[placeholder] = placeholder
        if symbol.ref:
            suspect[symbol.ref] = symbol.ref

    if not suspect:
        return ()

    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        # Unparseable emitted XML is its own failure and is caught downstream;
        # this guard refuses to guess from raw text rather than fall back to the
        # substring scan it exists to replace.
        return ()

    found = set()
    for element in root.iter():
        for value in element.attrib.values():
            if value in suspect:
                found.add(value)
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
    name_override: Optional[str] = None,
    snapshot=None,
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

    # The required set is the plan's RECORDED slot inventory (§6 AR1-03) — what
    # this plan actually binds, derived at compile — not the whole `depends_on`
    # declaration. A slot key with no applied id is a hard ordering failure;
    # everything else keeps its placeholder and the artifact guard still refuses
    # any that survives into emitted bytes.
    bound = bind_symbols_to_applied_ids(
        symbols,
        id_registry,
        key,
        required_keys={
            _ref_key(slot.ref) for slot in plan.unresolved_symbol_slots
        },
    )
    # #180. The recompile is a COMPILE ENTRY, and it was the one entry the
    # effect channel never reached: a root whose declaration turned a blocking
    # finding into a warning planned clean, compiled clean, and then failed
    # here — so the whole channel stopped at compile and apply refused it.
    #
    # The context comes off the PLAN, which recorded the very object its own
    # compile used. Reading it here rather than accepting it as a parameter is
    # the point: there is no argument a caller or a future call site can forget
    # to pass. `None` keeps the strict default rather than overriding it.
    # PROJECTED ONLY WITH A SNAPSHOT, because this one function serves two very
    # different callers. The DRY emit rehearses with `dry-run-` prefixed ids and
    # supplies no snapshot: projecting there would compare a record's real id
    # against a rehearsal id and refuse every valid retry. The WET apply supplies
    # the resolution's snapshot, which carries the observed identities AND the
    # account — without it the account comparison has nothing to compare and a
    # foreign-account record could corroborate.
    #
    # So the snapshot is the switch, not a flag a caller can set independently:
    # enforcement is possible exactly where the data to enforce safely exists.
    if snapshot is not None:
        from ...compiler.process_ir.connector_resolution import project_grants_for_root

        bound = project_grants_for_root(
            plan.process_ir,
            bound,
            # DIRECT. This is a load-bearing argument, and a defaulting accessor
            # here would substitute a literal root for a missing one — the same
            # shape that disabled the gate elsewhere in this slice.
            process_root_ref=plan.envelope.component_key,
            snapshot=snapshot,
        )
    cfg, emission_plan = (
        compile_process_ir_v1(
            plan.process_ir, bound, capabilities=plan.effect_capabilities
        )
        if plan.effect_capabilities is not None
        else compile_process_ir_v1(plan.process_ir, bound)
    )

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
        # `name_override` is the clone overlay (§6 AR1-01): a clone-generated
        # concrete name is EXCLUDED from covered plan material by design, so it
        # enters here — at emission — and in the mutation attestation, never in
        # the plan or its fingerprint.
        name=name_override or envelope.name,
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
    applied_folder_name: Optional[str] = None,
    submitted_xml_digest: Optional[str] = None,
    observed_placement: Optional[str] = None,
    observed_folder_id: Optional[str] = None,
    applied_folder_id: Optional[str] = None,
    replay_evidence_bindings=(),
    replay_account_scope_hash: Optional[str] = None,
):
    """The apply-time mutation attestation for one root.

    ``submitted_xml_digest`` is raw SHA-256 over the exact UTF-8 bytes that were
    sent (§6 review AR1-05b — the plan mandates the bytes AND the encoding; the
    repo's object-wrapping ``sha256_fingerprint`` convention changes the hashed
    material). For an update those bytes are the MERGED result of
    read-merge-write, digested immediately before the push and passed in; for a
    create the caller digests the final XML immediately before the raw create
    call. The fallback here digests the same bytes for any caller that did not.

    A create that reports success without a component id fails closed: an
    attestation naming no result describes a mutation nobody can verify, and
    recording it would be worse than refusing.
    """
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

    from ...models.authoring_workflow import (
        ConnectorReplayEvidenceBindingAttestationV1,
    )

    # SORTED, so two applies of the same plan attest byte-identically. An
    # accounting record whose ordering depends on how a dict happened to iterate
    # is not a record anyone can compare, and comparison is the only thing a
    # mutation-accounting record is for.
    def _field(source, name):
        # The boundary hands over resolved MAPPINGS carrying the concrete evidence;
        # older callers hand over grant objects. Both are read the same way rather
        # than converted at the call sites, because a conversion in three places is
        # three chances to convert differently.
        if isinstance(source, Mapping):
            return source.get(name)
        return getattr(source, name, None)

    _attested = []
    for g in (replay_evidence_bindings or ()):
        if not _field(g, "record_digest"):
            continue
        _scope = _field(g, "account_scope_hash")
        if (
            _scope
            and replay_account_scope_hash
            and _scope != replay_account_scope_hash
        ):
            # THE NESTED SCOPE MUST NAME THE PARENT'S ACCOUNT. The plan words
            # this as "nested account scope equals the parent attestation scope",
            # and taken literally that is unsatisfiable: the parent's
            # `account_scope_hash` is `sha256_fingerprint({scope, keyed_on,
            # account_id})` from the authoring layer, while a binding's is the
            # registry's `sha256(account_id)` — two different digests of the same
            # account, by two different owners, which can never compare equal.
            # Measured, not assumed. So the invariant is enforced against the
            # REGISTRY-side scope for this apply, which is the same value the
            # boundary recheck compared against; the two sides of the comparison
            # therefore come from one authority instead of two.
            #
            # This is the last gate before the record becomes durable, and it is
            # not redundant with the recheck: a root whose recheck did not run
            # falls back to raw grants, and that path reaches here unchecked.
            # THE CODE NAMES WHAT HAPPENED. The first version reused the
            # result-id code, which says a component id is missing — nothing here
            # is missing, and a served code that describes a different failure is
            # worse than a generic one because a machine reader acts on it. This
            # is a reconciliation failure found after the write, over a retained
            # result, which is exactly what the registered code means.
            #
            # Reaching it at all means the pre-submission recheck did not run:
            # whenever a root carries grants and the apply is not a dry run, that
            # recheck runs and compares this same scope, so in production this is
            # defence in depth rather than the enforcement point. It is kept
            # because this is the last gate before the record becomes durable, and
            # a durable record claiming evidence from another account is the one
            # thing it must never say.
            raise CanonicalProcessApplyError(
                "an evidence binding for process {0!r} names account scope {1!r}, "
                "which is not the scope this apply is authorised in; the "
                "attestation would claim evidence from another account".format(
                    key, _scope
                ),
                error_code="CONNECTOR_REPLAY_POST_SUBMISSION_RECONCILIATION_DRIFT",
                component_key=key,
            )
        _attested.append(
            ConnectorReplayEvidenceBindingAttestationV1(
                contract_ref=_field(g, "contract_ref"),
                operation_ref=_field(g, "operation_ref"),
                call_source_path=_field(g, "call_source_path"),
                process_root_ref=_field(g, "process_root_ref"),
                connection_ref=_field(g, "connection_ref"),
                record_digest=_field(g, "record_digest"),
                account_scope_hash=_scope,
                operation_component_id=_field(g, "operation_component_id"),
                operation_version=_field(g, "operation_version"),
                connection_component_id=_field(g, "connection_component_id"),
                connection_version=_field(g, "connection_version"),
                operation_config_digest=_field(g, "operation_config_digest"),
                connection_config_digest=_field(g, "connection_config_digest"),
                route_coverage_kind=_field(g, "route_coverage_kind"),
                route_digests=tuple(_field(g, "route_digests") or ()),
                capture_digest=_field(g, "capture_digest"),
                route_capture_digest=_field(g, "route_capture_digest"),
            )
        )

    # THE KEY THE CONTRACT NAMES, both to order and to deduplicate. It sorted on
    # three fields and deduplicated on none, so two applies could attest the same
    # binding twice — and a duplicate in an accounting record is a second claim
    # that a second grant was consumed, which is a different fact from the one
    # that happened. `dict` keeps the FIRST of each key and preserves order, so
    # the sort below decides the record and the dedup cannot reorder it.
    def _key(b):
        return (
            b.process_root_ref or "",
            b.call_source_path,
            b.contract_ref,
            b.operation_ref,
            b.connection_ref or "",
        )

    bindings = tuple(
        {_key(b): b for b in sorted(_attested, key=_key)}.values()
    )

    return ProcessMutationAttestationV1(
        component_key=key,
        plan_fingerprint=plan.plan_fingerprint,
        account_scope_hash=account_scope_hash,
        replay_evidence_bindings=bindings,
        action=action,
        # A create never names a target; an update's target IS its result.
        target_component_id=target_component_id if action == "update" else None,
        result_component_id=result_component_id,
        # The placement ACTUALLY OBSERVED, never the one requested (Codex
        # round 1; hardened by QA-153-r12-01, which measured the platform
        # IGNORING `folderName` on create for every builder and spelling — so
        # even the submitted bytes over-claim). For a create the caller passes
        # the READBACK's observation and the resolved id only when it matches;
        # for an update the merged submitted bytes carry the preserved live
        # placement. `None` means unknown/unplaced, never a guess.
        resolved_placement=ResolvedProcessPlacementV1(
            folder_name=(
                observed_placement if action == "create" else applied_folder_name
            ),
            # A create's folder id is the honoured resolution or, failing that,
            # the READBACK's own folderId (Codex round 17: an identity the
            # readback reported is knowledge — discarding it made the actual
            # placement indistinguishable from the requested same-named
            # folder). The requested resolution is never attested unhonoured.
            folder_id=(
                (resolved_folder_id or observed_folder_id)
                if action == "create"
                # An UPDATE's id comes from the same authority as its name: the
                # merged bytes the platform received (§6 AR2-04). The previous
                # expression fell back to the plan's REQUESTED resolution, which
                # for an update describes an intent, not what was applied — so
                # it is replaced, not augmented. `None` stays honest for a root
                # whose merged bytes carry no folder id.
                else applied_folder_id
            ),
        ),
        submitted_xml_digest=(
            submitted_xml_digest
            or "sha256:"
            + hashlib.sha256(submitted_xml.encode("utf-8")).hexdigest()
        ),
    )


def applied_placement(submitted_xml: str) -> Dict[str, Optional[str]]:
    """BOTH placement facts on the bytes that were actually SENT.

    Read from the submitted document rather than from the envelope, because for
    an update those bytes are the read-merge-write RESULT: preservation does not
    own the root's folder attributes, so what the platform received is the LIVE
    folder, not the requested one.

    Name and id come from ONE read (§6 AR2-04). Extracting only the name meant
    an update attested a folder by name with ``folder_id: null`` while the very
    bytes it was reading carried the id — a placement recorded as less than what
    the platform received. Two separate readers could also disagree; one cannot.
    """
    import xml.etree.ElementTree as ET

    try:
        root = ET.fromstring(submitted_xml)
    except ET.ParseError:
        return {"folder_name": None, "folder_id": None}
    return {
        "folder_name": root.attrib.get("folderName") or None,
        "folder_id": root.attrib.get("folderId") or None,
    }


def applied_folder_name(submitted_xml: str) -> Optional[str]:
    """The ``folderName`` half of :func:`applied_placement`."""
    return applied_placement(submitted_xml)["folder_name"]


def applied_component_name(component_xml: str) -> Optional[str]:
    """The ``name`` on the component the platform actually created.

    Read from the LIVE readback, because the platform does not always honour the
    requested one: Boomi treats a soft-deleted predecessor's name as taken and
    appends a counter, so authoring ``X`` against a deleted ``X`` silently
    produces ``"X 2"``.
    """
    import xml.etree.ElementTree as ET

    try:
        root = ET.fromstring(component_xml)
    except ET.ParseError:
        return None
    return root.attrib.get("name") or None


def observed_folder_identity(component_xml: str) -> Optional[Dict[str, Any]]:
    """WHERE the platform actually placed the component, as an identity.

    QA-153-r12-01: the platform ignores ``folderName`` on create, so an
    attestation built from the request or the submitted bytes promises a
    placement that never happened — only the readback reports where the
    component IS.

    Returns ``None`` when the XML is unreadable (location UNKNOWN — the caller
    must not claim anything, Codex round 16 F2), else a dict:

    - ``full_path`` / ``leaf`` — from ``folderFullPath`` (``folderName`` as a
      fallback for platforms that echo it);
    - ``folder_id`` — the readback's own ``folderId``, when present, which is
      the STRONGEST comparison basis;
    - ``is_root`` — a single-segment full path is the ACCOUNT itself, not a
      folder (Codex round 16 F1: reducing it to a leaf attested the account
      name as a placement, and a requested folder that happened to equal the
      account name would have false-positively confirmed).
    """
    import xml.etree.ElementTree as ET

    try:
        root = ET.fromstring(component_xml)
    except ET.ParseError:
        return None
    full_path = root.attrib.get("folderFullPath") or ""
    folder_id = root.attrib.get("folderId") or None
    if full_path:
        segments = [part for part in full_path.rstrip("/").split("/") if part]
        if len(segments) > 1:
            return {"full_path": full_path, "leaf": segments[-1],
                    "folder_id": folder_id, "is_root": False}
        # A single-segment full path is the ACCOUNT itself, and that holds
        # even when the readback also names a folder id: Boomi's root IS a
        # folder with an id of its own — live-captured in
        # tests/fixtures/live_xml/m11/processproperty_minimal.xml, whose
        # rooted component carries the account-name path, the account-name
        # folderName AND a folderId (Codex round 19, correcting round 18's
        # tie-break). The id is retained as knowledge but the classification
        # is root, and callers never propagate a root's id as a placement.
        return {"full_path": full_path,
                "leaf": segments[-1] if segments else None,
                "folder_id": folder_id, "is_root": True}
    name = root.attrib.get("folderName")
    if name:
        return {"full_path": name, "leaf": name, "folder_id": folder_id,
                "is_root": False}
    if folder_id:
        # An id with no path metadata is a FOLDER whose name the readback did
        # not report, never the root (Codex round 18: classifying it as root
        # while the id propagated attested one create as simultaneously in a
        # folder and at the root).
        return {"full_path": None, "leaf": None, "folder_id": folder_id,
                "is_root": False}
    # Parsed, and NO folder evidence at all: the platform reported nothing,
    # which on this surface means the account root.
    return {"full_path": None, "leaf": None, "folder_id": None,
            "is_root": True}


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
    "applied_component_name",
    "applied_folder_name",
    "applied_placement",
    "build_readback_attestation",
    "materialize_canonical_process_xml",
    "observed_folder_identity",
    "resolve_extension_connections",
]
