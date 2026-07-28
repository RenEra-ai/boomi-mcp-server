"""The dark SystemTopology planner (M12.9 / issue #144).

A capability-gated, planning-only authority over the relationships BETWEEN
processes and the platform resources they bind to (ADR-001 §3). It compiles
nothing, emits no XML, registers no MCP tool, and has no apply path — #146 owns
the eventual planning surface.

Sibling to, never layered with, ``boomi_mcp.compiler.process_ir``: neither
imports the other, and their graphs are deliberately disjoint. A ProcessCall
relation is not a CFG edge, and a ComponentPlan build dependency is neither.

The narrow surface below is the whole public API. Deliberately NOT exported:
the prepared context and its indexes (a caller-supplied one would defeat
revalidation), the XML projection helpers (they exist to keep raw XML from
escaping, so re-exporting them would reopen the channel), the capability
registry internals, and the discovery port — which is a read-only Protocol with
no shipped adapter.
"""

from __future__ import annotations

from typing import List

from .context import TopologyResolutionContextV1
from .contracts import (
    SystemTopologyPlanV1,
    TopologyCapabilityReportV1,
    TopologyValidationReportV1,
    canonical_topology_plan_json,
    canonical_topology_report_json,
)
from .invariants import TopologyPlanningInvariantError
from .pipeline import plan_system_topology, validate_system_topology

__all__: List[str] = [
    "SystemTopologyPlanV1",
    "TopologyCapabilityReportV1",
    "TopologyPlanningInvariantError",
    "TopologyResolutionContextV1",
    "TopologyValidationReportV1",
    "canonical_topology_plan_json",
    "canonical_topology_report_json",
    "plan_system_topology",
    "validate_system_topology",
]
