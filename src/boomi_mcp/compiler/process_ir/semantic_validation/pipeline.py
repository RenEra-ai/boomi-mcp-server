"""``validate_process_ir`` — the one unified entry point (#143). DARK in slice 6.

Callable and fully tested here, but invoked from no compiler stage, adapter,
builder, plan or apply path. Slice 8 makes it a mutation gate; slice 9 makes it
the compiler's gate.

Phase order
-----------
Phases run in ``VALIDATION_PHASE_ORDER``. The order matters twice over:

* **Report order.** Findings sort by phase RANK, so the earliest failure in the
  pipeline reads first regardless of alphabet.
* **Dependency suppression.** A reference that did not resolve makes every
  finding that needed that component meaningless. Rather than fail fast — which
  would hide four unrelated defects behind one bad ref — the reference phase
  records what it could not resolve, and later phases are skipped ONLY for the
  parts that depended on it.

Expected invalidity is returned, never raised
---------------------------------------------
A payload being wrong is the normal case this function exists to describe, so it
comes back as a report. Only a COMPILER defect raises, and it raises as itself:
``ProcessIRCompileError`` passes through untouched so the caller's existing
handling still sees it. That is why the function has no ``try/except`` around the
collectors — swallowing a compile error to "make the report complete" would
convert a compiler bug into a user-facing validation finding.
"""

from __future__ import annotations

import hashlib
from typing import List, Optional, Tuple

from ....errors import PROCESS_IR_CAPABILITY_EFFECT_CONTRACT_INVALID
from ....models.process_ir import ProcessIRV1
from ..contracts import SymbolTableV1
from .contracts import (
    DEFAULT_VALIDATION_CAPABILITIES,
    ProcessIRValidationCapabilitiesV1,
    ValidationDiagnosticV1,
    ValidationReportV1,
    build_validation_report,
)
from .context import PreparedProcessValidationV1, prepare_validation_context
from .effects import collect_effect_findings
from .flow import collect_flow_findings
from .lineage import collect_lineage_findings
from .findings import finding
from .references import collect_reference_findings

_CAPABILITY_PHASE = "capability"


def validate_process_ir(
    ir: ProcessIRV1,
    symbol_table: SymbolTableV1,
    capabilities: ProcessIRValidationCapabilitiesV1 = DEFAULT_VALIDATION_CAPABILITIES,
) -> ValidationReportV1:
    """Validate a ProcessIR document and return everything wrong with it.

    Pure: no network, no filesystem, no clock, no mutation. The same
    ``(ir, symbol_table, capabilities)`` triple always produces the same report.

    Raises only on a COMPILER defect (``ProcessIRCompileError``), never on an
    invalid payload.
    """
    prepared = prepare_validation_context(ir, symbol_table)
    return _validate_prepared(prepared, capabilities)


def validate_lowered_process_ir(
    ir: ProcessIRV1,
    cfg,
    symbol_table: SymbolTableV1,
    capabilities: ProcessIRValidationCapabilitiesV1 = DEFAULT_VALIDATION_CAPABILITIES,
) -> ValidationReportV1:
    """Validate against an ALREADY-lowered CFG, without lowering a second time.

    The compiler has just built the CFG it is about to emit from; re-lowering
    here would burn the work twice AND — worse — create two graphs that could in
    principle disagree. The gate must judge exactly the graph that will be
    emitted, not a fresh one that merely ought to be identical.

    The IR snapshot guarantee is weaker on this path by construction: the caller
    supplies the CFG, so the correspondence between ``ir`` and ``cfg`` is the
    caller's to uphold. That is why this is used only from inside the compiler,
    which lowered the pair itself one call earlier.
    """
    from .context import PreparedProcessValidationV1 as _Prepared
    from .context import _edge_index

    prepared = _Prepared(
        ir=ir,
        cfg=cfg,
        symbols=symbol_table,
        node_by_id={node.node_id: node for node in cfg.nodes},
        outgoing=_edge_index(cfg.edges, "source_node_id"),
        incoming=_edge_index(cfg.edges, "target_node_id"),
        symbol_by_ref={symbol.ref: symbol for symbol in symbol_table.symbols},
    )
    return _validate_prepared(prepared, capabilities)


def _validate_prepared(
    prepared: PreparedProcessValidationV1,
    capabilities: ProcessIRValidationCapabilitiesV1,
) -> ValidationReportV1:
    """Run every phase over an already-prepared context.

    Private on purpose: a caller that could supply its own prepared context
    could supply one whose CFG does not correspond to its IR, and every phase
    trusts that correspondence.
    """
    findings: List[ValidationDiagnosticV1] = []

    findings.extend(_collect_capability_findings(prepared, capabilities))

    reference_findings, reference_facts = collect_reference_findings(prepared)
    findings.extend(reference_findings)

    # Flow, lineage and effects each depend on references only for the nodes
    # whose refs failed. They are run in full and their findings filtered,
    # rather than skipped wholesale — a map that did not resolve must not
    # suppress an unreachable node three steps away.
    findings.extend(collect_flow_findings(prepared))
    findings.extend(collect_lineage_findings(prepared, capabilities))
    findings.extend(collect_effect_findings(prepared, capabilities))

    return build_validation_report(
        _suppress_dependents(findings, prepared, reference_facts)
    )


def _collect_capability_findings(
    prepared: PreparedProcessValidationV1,
    capabilities: ProcessIRValidationCapabilitiesV1,
) -> Tuple[ValidationDiagnosticV1, ...]:
    """A supplied contract that binds to NOTHING in this IR.

    The `capability` phase existed in the order with no collector behind it, and
    `PROCESS_IR_CAPABILITY_EFFECT_CONTRACT_INVALID` was registered, given a
    message and a remediation, and emitted by nobody. A contract naming a map,
    script digest or subprocess the document does not contain is a CALLER error:
    silently ignoring it meant a caller could believe a map was vouched for
    while the node stayed opaque, and the only symptom was a
    `…LINEAGE_EFFECT_UNKNOWN` warning pointing at the node rather than at the
    contract that failed to match it.

    Binding keys only — no contract CONTENT reaches a diagnostic, so the
    evidence stays inside the closed vocabulary the redaction boundary allows.
    """
    map_refs: set = set()
    process_refs: set = set()
    script_keys: set = set()
    for node in prepared.cfg.nodes:
        semantic = node.semantic
        kind = semantic.semantic_kind
        if kind == "map":
            map_refs.add(semantic.map_ref)
        elif kind == "process_call":
            process_refs.add(semantic.process_ref)
        elif kind == "data_process":
            for step in getattr(semantic, "steps", ()) or ():
                if getattr(step, "operation", None) != "custom_scripting":
                    continue
                digest = hashlib.sha256(step.script.encode("utf-8")).hexdigest()
                script_keys.add((step.language, digest))

    findings: List[ValidationDiagnosticV1] = []

    def _unbound(container: str, index: int, effect_kind: str) -> None:
        findings.append(
            finding(
                PROCESS_IR_CAPABILITY_EFFECT_CONTRACT_INVALID,
                "error",
                _CAPABILITY_PHASE,
                "/capabilities/{0}/{1}".format(container, index),
                evidence=(("effect_kind", effect_kind),),
            )
        )

    for index, item in enumerate(capabilities.map_effects):
        if item.map_ref not in map_refs:
            _unbound("map_effects", index, "map")
    for index, item in enumerate(capabilities.script_effects):
        if (item.language, item.source_sha256) not in script_keys:
            _unbound("script_effects", index, "script")
    for index, item in enumerate(capabilities.subprocess_summaries):
        if item.process_ref not in process_refs:
            _unbound("subprocess_summaries", index, "subprocess")

    return tuple(findings)


def _suppress_dependents(
    findings: List[ValidationDiagnosticV1],
    prepared: PreparedProcessValidationV1,
    reference_facts,
) -> Tuple[ValidationDiagnosticV1, ...]:
    """Drop findings that only exist because a reference failed to resolve.

    Deliberately narrow, and matched EXACTLY. An unresolved cache reference
    makes "this cache has no writer" meaningless — the cache identity itself is
    unknown — so that one lineage finding is dropped for that one node. The
    unresolved-reference finding already tells the author what to fix; reporting
    both would send them chasing a phantom lineage defect.

    The join goes through the offending NODE, not through a name: findings carry
    no cache name (redaction), so the node id the collector recorded is the only
    honest key. Suppressing on "some reference somewhere failed" would be
    over-suppression, which turns a report into a lie by omission.
    """
    if not reference_facts.unresolved:
        return tuple(findings)

    from ....errors import PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING

    kept: List[ValidationDiagnosticV1] = []
    for item in findings:
        if (
            item.code == PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING
            and _node_cache_ref_unresolved(item, prepared, reference_facts)
        ):
            continue
        kept.append(item)
    return tuple(kept)


def _node_cache_ref_unresolved(item, prepared, reference_facts) -> bool:
    """Whether THIS finding's own node holds a cache ref that did not resolve."""
    if item.internal_node_id is None:
        return False
    node = prepared.node(item.internal_node_id)
    if node is None:
        return False
    cache_ref = getattr(node.semantic, "cache_ref", None)
    return cache_ref is not None and cache_ref in reference_facts.unresolved


__all__ = ["validate_lowered_process_ir", "validate_process_ir"]
