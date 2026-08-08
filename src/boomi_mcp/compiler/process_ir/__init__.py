"""Internal ProcessIR compiler: CFG + emission-plan lowering (issue #137, M12.2).

**Charter (rewritten by #146).** This package was DARK through #138. It is not
any more: ``boomi_mcp.authoring.workflow`` compiles authored documents through it
on the production ``build_integration(action="plan"|"compile")`` path. What has
NOT changed — and is the invariant that matters — is that no compiler TYPE
reaches an MCP surface or an LLM-facing JSON Schema (issue #137's acceptance
criterion, ADR-001 §6). ``SemanticCfgV1``, ``EmissionPlanV1``, the emitter
inputs, and the capability registries stay internal, and nothing here is added
to ``__all__`` for a serving surface to pick up.

#146 opens exactly one seam, named in the charters of the three modules it
touches (``body_capabilities``, ``connector_capabilities``, ``error_handling``):
``boomi_mcp.authoring.process_ir_projection`` may READ those registries to derive
a sanitized, read-only authoring contract. That projection is OUTPUT ONLY — no
caller input re-enters the compiler as capability context — and it publishes
semantic facts under a distinct public vocabulary, never these modules' own
identifiers. The legacy ``flow_sequence`` path is unchanged and stays
authoritative for materialization.

**Recorded deviation (#146 amendment).** The implementation plan said "do not
re-export internal capability or CFG types". This package's ``__all__`` still
re-exports ``SemanticCfgV1``, ``EmissionPlanV1`` and their neighbours — they
predate this work, the test-only emitter registry and the parity suites import
them through it, and withdrawing them is a refactor with its own blast radius
rather than part of serving an authoring contract. It is NOT an MCP leak: those
names are in ``FORBIDDEN_NAMES`` and a repository-wide guard fails the build if
any of them reaches a served payload or a tool description. Recorded here so the
gap between the plan and the code is visible rather than discovered.

Pipeline::

    authored payload --parse--> ProcessIRV1 --lower--> SemanticCfgV1
                                                  --lower--> EmissionPlanV1

The split is the point. ``SemanticCfgV1`` carries control-flow MEANING only —
no shape ids, layout, or XML. ``EmissionPlanV1`` owns everything generated:
synthetic shapes, ``shapeN`` identities, geometry, dragpoints, and resolved
symbols. A caller can therefore never author reachability, wiring, or a
synthetic node, which is exactly what issue #137 set out to make impossible.

See ``docs/architecture/PROCESS_IR_COMPILER_V1.md``.
"""

from typing import List

from .contracts import (
    BRANCH_MAX_LEGS,
    BRANCH_MIN_LEGS,
    CfgEdgeKindV1,
    CfgEdgeV1,
    CfgExitRoleV1,
    CfgNodeV1,
    CfgSemanticV1,
    ComponentSymbolV1,
    IdempotencyContractSymbolV1,
    EmissionLayoutV1,
    EmissionNodeV1,
    EmissionPlanV1,
    EmissionTransitionV1,
    EmitterInputV1,
    SemanticCfgV1,
    SymbolTableV1,
    canonical_cfg_json,
    canonical_emission_plan_json,
)
from .diagnostics import (
    CompilerDiagnostic,
    CompilerPhase,
    ProcessIRCompileError,
    node_identity_for,
)
from .invariants import check_cfg_invariants, check_emission_plan_invariants
from .lowering import lower_cfg_to_emission_plan, lower_process_ir_to_cfg
from .pipeline import compile_process_ir_v1, parse_and_compile_process_ir_v1

__all__: List[str] = [
    "BRANCH_MAX_LEGS",
    "BRANCH_MIN_LEGS",
    "CfgEdgeKindV1",
    "CfgEdgeV1",
    "CfgExitRoleV1",
    "CfgNodeV1",
    "CfgSemanticV1",
    "CompilerDiagnostic",
    "CompilerPhase",
    "ComponentSymbolV1",
    "IdempotencyContractSymbolV1",
    "EmissionLayoutV1",
    "EmissionNodeV1",
    "EmissionPlanV1",
    "EmissionTransitionV1",
    "EmitterInputV1",
    "ProcessIRCompileError",
    "SemanticCfgV1",
    "SymbolTableV1",
    "canonical_cfg_json",
    "canonical_emission_plan_json",
    "check_cfg_invariants",
    "check_emission_plan_invariants",
    "compile_process_ir_v1",
    "lower_cfg_to_emission_plan",
    "lower_process_ir_to_cfg",
    "node_identity_for",
    "parse_and_compile_process_ir_v1",
]
