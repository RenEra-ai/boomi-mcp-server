"""Internal ProcessIR compiler: CFG + emission-plan lowering (issue #137, M12.2).

DARK. No MCP tool, production builder, or JSON Schema constructs or consumes
these types. Since #138 (M12.3) a TEST-ONLY consumer exists — the typed
process-emitter registry in ``emitter_registry`` turns an ``EmissionPlanV1`` into
process XML for parity tests — but it is imported directly (never via this
package's ``__all__``), is invoked by no MCP tool or production builder, and adds
no eager compiler/builder import coupling. The legacy ``flow_sequence`` path is
unchanged and stays authoritative until #139 (production adapters) reaches parity.

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
