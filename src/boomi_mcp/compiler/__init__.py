"""Compiler-internal packages (M12, issue #137).

Everything under ``boomi_mcp.compiler`` is INTERNAL to the authoring compiler.
Nothing here is re-exported through ``boomi_mcp`` or ``boomi_mcp.models``, and
none of it may appear in an LLM-facing JSON Schema or MCP tool surface (issue
#137 acceptance criterion; ADR-001 §6 authored-vs-derived).

This module deliberately performs NO imports, so ``import boomi_mcp`` never
pays for the compiler.
"""
