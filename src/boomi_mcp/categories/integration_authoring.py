"""Integration authoring MCP action layer (V3 archetype tools — Issue #18).

Exposes the V3 archetype framework without calling Boomi or emitting XML.
All responses are JSON-serializable and include ``_success``.
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import ValidationError

from .components.builders.connector_builder import BuilderValidationError
from ..patterns import (
    PatternError,
    PatternKind,
    PatternRegistry,
    PatternRegistryError,
    pattern_validation_error,
)


def list_integration_archetypes_action(
    query: str | None = None,
    tags: list[str] | str | None = None,
) -> dict[str, Any]:
    try:
        normalized_tags = _normalize_tags(tags)
    except (TypeError, ValueError) as exc:
        return PatternError(
            error_code="INVALID_INPUT",
            error=f"Invalid tags argument: {exc}",
            suggestion="Provide tags as a list of strings, a comma-separated string, or a JSON array string.",
            retryable=False,
        ).to_dict()

    if query is not None and not isinstance(query, str):
        return PatternError(
            error_code="INVALID_INPUT",
            error=f"query must be a string or None; got {type(query).__name__}",
            suggestion="Provide query as a substring to match (or omit to list all archetypes).",
            retryable=False,
        ).to_dict()

    try:
        registry = PatternRegistry.from_package("boomi_mcp.patterns")
        patterns = registry.list_patterns(
            kind=PatternKind.ARCHETYPE,
            query=query,
            tags=normalized_tags,
        )
    except PatternRegistryError as exc:
        return exc.to_pattern_error().to_dict()

    return {
        "_success": True,
        "count": len(patterns),
        "archetypes": [p.metadata.model_dump(mode="json") for p in patterns],
        "query": query,
        "tags": normalized_tags,
        "raw_xml_exposed": False,
    }


def get_integration_archetype_action(name: str) -> dict[str, Any]:
    try:
        registry = PatternRegistry.from_package("boomi_mcp.patterns")
        cls = registry.get(name, kind=PatternKind.ARCHETYPE)
    except PatternRegistryError as exc:
        return exc.to_pattern_error().to_dict()

    return {
        "_success": True,
        "archetype": cls.describe(),
        "raw_xml_exposed": False,
        "next_tool": "build_from_archetype",
    }


def build_from_archetype_action(
    name: str,
    parameters: dict[str, Any] | str | None = None,
) -> dict[str, Any]:
    try:
        params_dict = _normalize_parameters(parameters)
    except (ValueError, TypeError) as exc:
        return PatternError(
            error_code="PARAM_VALIDATION_FAILED",
            error=str(exc),
            suggestion="Provide parameters as a JSON object (dict) or JSON-encoded string.",
            retryable=False,
        ).to_dict()

    try:
        registry = PatternRegistry.from_package("boomi_mcp.patterns")
        cls = registry.get(name, kind=PatternKind.ARCHETYPE)
    except PatternRegistryError as exc:
        return exc.to_pattern_error().to_dict()

    try:
        params_obj = cls.validate_parameters(params_dict)
    except ValidationError as exc:
        return pattern_validation_error(
            exc,
            suggestion="Inspect field_errors[] for per-field problems.",
        ).to_dict()

    try:
        spec = cls.emit_spec(params_obj)
    except BuilderValidationError as exc:
        # A primitive/builder rejected the assembly (e.g. UNSUPPORTED_REST_AUTH_MODE,
        # UNSUPPORTED_TRANSFORM_ROUTE, SCRIPT_MAPPING_REF_REQUIRED). These errors
        # are already structured and secret-safe — they name the offending field
        # and never echo caller values — so surface them verbatim instead of the
        # opaque ARCHETYPE_BUILD_FAILED envelope.
        context: dict[str, Any] = {"archetype": name}
        if exc.field:
            context["field"] = exc.field
        if exc.details:
            context["details"] = exc.details
        return PatternError(
            error_code=exc.error_code or "ARCHETYPE_BUILD_VALIDATION_FAILED",
            error=str(exc),
            suggestion=exc.hint
            or f"Adjust the {name} archetype parameters to satisfy the builder.",
            retryable=False,
            context=context,
        ).to_dict()
    except Exception as exc:  # noqa: BLE001 — last-line defense; do not leak parameters
        return PatternError(
            error_code="ARCHETYPE_BUILD_FAILED",
            error=f"emit_spec() failed for archetype {name!r}: {exc}",
            suggestion=f"Inspect the {name} archetype implementation.",
            retryable=False,
            context={"archetype": name, "exception_type": type(exc).__name__},
        ).to_dict()

    return {
        "_success": True,
        "archetype": cls.metadata.name,
        "archetype_version": cls.metadata.version,
        "integration_spec": spec.model_dump(mode="json"),
        "raw_xml_exposed": False,
        "boomi_mutation": False,
        "next_steps": (
            "Pass integration_spec to build_integration(action='plan', config=...) "
            "to preview steps before applying."
        ),
    }


# ---- private input normalizers ------------------------------------------


def _normalize_tags(tags: list[str] | str | None) -> list[str] | None:
    if tags is None:
        return None
    if isinstance(tags, str):
        stripped = tags.strip()
        if not stripped:
            return []
        if stripped.startswith("["):
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, list):
                return [str(t).strip() for t in parsed if str(t).strip()]
        return [t.strip() for t in stripped.split(",") if t.strip()]
    return [str(t) for t in tags]


def _normalize_parameters(parameters: dict[str, Any] | str | None) -> dict[str, Any]:
    if parameters is None:
        return {}
    if isinstance(parameters, dict):
        return parameters
    if isinstance(parameters, str):
        try:
            parsed = json.loads(parameters)
        except json.JSONDecodeError as exc:
            raise ValueError(f"parameters must be valid JSON: {exc}") from exc
        if not isinstance(parsed, dict):
            raise ValueError(
                "parameters JSON must be an object, not " + type(parsed).__name__
            )
        return parsed
    raise TypeError(
        f"parameters must be dict, JSON string, or None; got {type(parameters).__name__}"
    )
