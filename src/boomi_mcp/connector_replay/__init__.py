"""Connector-replay evidence registry.

A packaged, self-contained record of what is KNOWN about the replay safety of
connector actions, derived only from executed captures. Nothing here infers a
capability from documentation or from the shape of a component: a row exists
because an execution produced it.

The package is deliberately importable on its own. Nothing in it imports
``boomi_mcp.compiler`` or ``boomi_mcp.categories`` at module load, so the registry
can be loaded by tooling — and from inside a built image — without the authoring
stack. Submodules are imported lazily by callers rather than re-exported here,
which is what keeps that property true as the package grows.
"""

from __future__ import annotations

__all__ = ["ids"]
