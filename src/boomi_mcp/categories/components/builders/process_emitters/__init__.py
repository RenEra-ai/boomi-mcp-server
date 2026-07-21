"""Shared process-shape byte serializers (issue #138 M12.3).

This package owns the SINGLE copy of every process-shape XML template plus the
deterministic layout/dragpoint primitives. Two callers feed it:

  * ``legacy`` — thin adapters preserving the historical ``_emit_*`` dict
    signatures, so ``process_flow_builder`` keeps its orchestration but no longer
    owns any XML template (it imports the emitters from here).
  * ``boomi_mcp.compiler.process_ir.emitter_registry`` — the typed ProcessIR
    emitter registry (#138), which builds the same neutral render value objects
    from an ``EmissionPlanV1`` and its resolved symbols.

Both paths call the SAME ``rendering`` functions, so byte output is generated in
exactly one place. ``rendering`` is pure serialization: it holds no legacy
config, does no cross-shape orchestration, and knows nothing about ProcessIR
semantics or ``IntegrationSpecV1``/``PipelineSpec``. The module deliberately
imports nothing from ``process_flow_builder`` (that would close an import cycle).

Nothing here is a public authoring surface; these names stay module-private.
"""
