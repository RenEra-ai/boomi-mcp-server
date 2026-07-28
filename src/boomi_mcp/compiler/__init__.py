"""Compiler-internal packages (M12, issues #137 and #144).

Two independent internal packages live here:

* ``process_ir`` — the ProcessIR compiler: CFG, lowering, emission plan. It
  turns one process's semantics into buildable XML.
* ``system_topology`` — the dark SystemTopology PLANNER (#144). It resolves
  relationships BETWEEN processes and platform resources and emits a plan. It
  compiles nothing, emits no XML, and never mutates runtime state (ADR-001 §3).

They are siblings, not layers: neither imports the other, and their graphs are
deliberately disjoint (a ProcessCall relation is not a CFG edge, and a
ComponentPlan build dependency is neither).

Everything under ``boomi_mcp.compiler`` is INTERNAL to the authoring compiler.
Nothing here is re-exported through ``boomi_mcp`` or ``boomi_mcp.models``, and
none of it may appear in an LLM-facing JSON Schema or MCP tool surface (issue
#137 acceptance criterion; ADR-001 §6 authored-vs-derived).

This module deliberately performs NO imports, so ``import boomi_mcp`` never
pays for the compiler.
"""
