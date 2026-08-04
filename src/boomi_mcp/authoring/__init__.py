"""MCP-facing authoring orchestration (issue #146, M12.11).

This package is the seam between the MCP tool layer and the canonical compiler.
It may import ``compiler.*``, ``recipes``, ``patterns`` and ``models``; the
``categories`` modules import IT.

**Why it is not under ``boomi_mcp.compiler``.** That package's ``__init__``
states two invariants this code would break: nothing under it may appear on an
MCP tool surface or in an LLM-facing JSON Schema (issue #137's acceptance
criterion, ADR-001 §6), and it deliberately performs NO imports so ``import
boomi_mcp`` never pays for the compiler. Orchestration that exists precisely to
be reachable from ``build_integration``, and that must import the compiler to do
its job, belongs on the other side of that line.

Kept import-light for the same reason: ``patterns`` and ``recipes`` are imported
at CALL time, not module scope, because archetype discovery reaches
``categories.components.builders`` and a module-scope import cycles.

Nothing under ``boomi_mcp.compiler`` is re-exported through this package.

**#146 amendment — the sanitized projection.** ``process_ir_projection`` READS
three named compiler registries (``body_capabilities``,
``connector_capabilities``, ``error_handling``) to derive the read-only
``process_ir_authoring`` contract. That does not weaken the rule above: what
crosses is derived DATA under a public vocabulary, never a compiler type, and
the projection is output only — nothing a caller sends can re-enter the compiler
through it. Every compiler import inside that module is function-local, so
``import boomi_mcp`` and ``import server`` still pay nothing for the compiler.
"""

from .revisions import (
    account_scope_fingerprint,
    artifact_fingerprint,
    canonical_json_bytes,
    capability_fingerprint,
    compile_fingerprint,
    plan_fingerprint,
    schema_fingerprint,
    semantic_fingerprint,
    sha256_fingerprint,
)

__all__ = [
    "account_scope_fingerprint",
    "artifact_fingerprint",
    "canonical_json_bytes",
    "capability_fingerprint",
    "compile_fingerprint",
    "plan_fingerprint",
    "schema_fingerprint",
    "semantic_fingerprint",
    "sha256_fingerprint",
]
