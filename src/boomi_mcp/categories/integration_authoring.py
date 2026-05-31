"""Integration authoring MCP action layer (V3 archetype tools — Issue #18).

Exposes the V3 archetype framework without calling Boomi or emitting XML.
All responses are JSON-serializable and include ``_success``.
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import ValidationError

from .components.builders.connector_builder import BuilderValidationError
from .components.builders.profile_inference import (
    PROFILE_INFERENCE_INPUT_TOO_LARGE,
    PROFILE_INFERENCE_INVALID_INPUT,
    _resolve_limits,
    infer_profile_from_db_metadata,
    infer_profile_from_sample_json,
    infer_profile_from_sample_xml,
    infer_profile_from_xsd,
)
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


# ---- Issue #47: read-only profile inference action -----------------------

# Every response from this action carries these flags so the advertised
# read-only / no-Boomi-mutation / no-raw-XML contract holds on success AND error.
_INFERENCE_FLAGS = {"read_only": True, "boomi_mutation": False, "raw_xml_exposed": False}

_INFERENCE_DISPATCH = {
    "profile_from_db_metadata": infer_profile_from_db_metadata,
    "profile_from_sample_json": infer_profile_from_sample_json,
    "profile_from_xsd": infer_profile_from_xsd,
    "profile_from_sample_xml": infer_profile_from_sample_xml,
}
_SUPPORTED_SOURCE_TYPES = list(_INFERENCE_DISPATCH)


def _inference_error_envelope(
    code: str,
    message: str,
    *,
    field: str | None = None,
    hint: str | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    env: dict[str, Any] = {
        "_success": False,
        **_INFERENCE_FLAGS,
        "code": code,
        "error": message,
    }
    if field is not None:
        env["field"] = field
    if hint is not None:
        env["hint"] = hint
    if details is not None:
        env["details"] = details
    if code == PROFILE_INFERENCE_INPUT_TOO_LARGE:
        # Oversize is reported as an error envelope that also carries the
        # truncation metadata + ready_for_builder=False (never partial output).
        env["truncated"] = True
        env["truncation"] = details
        env["ready_for_builder"] = False
    return env


def _normalize_inference_options(options: dict[str, Any] | str | None) -> dict[str, Any]:
    if options is None:
        return {}
    if isinstance(options, dict):
        return options
    if isinstance(options, str):
        try:
            parsed = json.loads(options)
        except json.JSONDecodeError as exc:
            raise ValueError(f"options must be valid JSON: {exc}") from exc
        if not isinstance(parsed, dict):
            raise ValueError(
                "options JSON must be an object, not " + type(parsed).__name__
            )
        return parsed
    raise TypeError(
        f"options must be dict, JSON string, or None; got {type(options).__name__}"
    )


def infer_profile_fields_action(
    source_type: str,
    artifact: Any,
    options: dict[str, Any] | str | None = None,
) -> dict[str, Any]:
    """Read-only profile-field inference (issue #47).

    Turns a caller-supplied DB metadata summary / sample JSON / XSD / sample XML
    into an issue-#43 builder-ready profile-field contract WITHOUT calling Boomi,
    constructing an SDK client, or reading credentials. Every response carries
    ``read_only=True``, ``boomi_mutation=False``, ``raw_xml_exposed=False``.
    """
    try:
        opts = _normalize_inference_options(options)
    except (ValueError, TypeError) as exc:
        return _inference_error_envelope(
            PROFILE_INFERENCE_INVALID_INPUT,
            str(exc),
            field="options",
            hint="Provide options as a JSON object (dict) or JSON-encoded string.",
        )

    fn = _INFERENCE_DISPATCH.get(source_type)
    if fn is None:
        return _inference_error_envelope(
            PROFILE_INFERENCE_INVALID_INPUT,
            f"unknown source_type {source_type!r}",
            field="source_type",
            hint="Use one of the supported inference source types.",
            details={"supported_source_types": _SUPPORTED_SOURCE_TYPES},
        )

    # Guard oversized string artifacts BEFORE parsing — never echo content.
    if isinstance(artifact, str):
        limits = _resolve_limits(opts)
        if len(artifact) > limits["max_input_chars"]:
            return _inference_error_envelope(
                PROFILE_INFERENCE_INPUT_TOO_LARGE,
                f"artifact length {len(artifact)} exceeds max_input_chars "
                f"{limits['max_input_chars']}",
                field="artifact",
                hint="Reduce the artifact or raise max_input_chars (up to the hard cap).",
                details={
                    "kind": "input_chars",
                    "limit": limits["max_input_chars"],
                    "observed": len(artifact),
                },
            )

    try:
        result = fn(artifact, options=opts)
    except BuilderValidationError as exc:
        return _inference_error_envelope(
            exc.error_code or PROFILE_INFERENCE_INVALID_INPUT,
            str(exc),
            field=exc.field,
            hint=exc.hint,
            details=exc.details,
        )
    except Exception as exc:  # noqa: BLE001 — last-line defense; never leak the artifact
        return _inference_error_envelope(
            PROFILE_INFERENCE_INVALID_INPUT,
            f"inference failed: {type(exc).__name__}",
            field="artifact",
        )

    return {"_success": True, **_INFERENCE_FLAGS, **result}
