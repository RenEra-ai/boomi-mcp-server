"""Integration authoring MCP action layer: recipe discovery and profile inference.

#159 S3: ``list_integration_archetypes`` and ``get_integration_archetype`` are
RECIPE-ONLY discovery tools backed by ``production_registry()``. Nothing here
builds or composes a process: the archetype build and composition dispatchers
retired with their public tools, and a recipe is authored through
``build_integration(config={'authoring_request': ...})``.

Neither tool calls Boomi or emits XML. All responses are JSON-serializable and
include ``_success``.
"""

from __future__ import annotations

import json
from typing import Any

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
from ..errors import INVALID_INPUT
from ..recipes import RecipeError, production_registry, recipe_error_envelope
from ..recipes.contracts import ExpectedRecipeRegistryV1


#: The one registry entry kind discovery serves. Advisory doctrine pointers and
#: constraint-only entries are registered but are not something a caller
#: invokes as an integration, so listing them here would advertise a row that
#: ``build_integration`` cannot run as one.
_DISCOVERABLE_ENTRY_KIND = "executable_recipe"

#: Where a discovered recipe is authored. Discovery never builds anything.
_NEXT_TOOL = "build_integration"

_DISCOVERY_FLAGS = {"read_only": True, "boomi_mutation": False, "raw_xml_exposed": False}


def _error(error: str, suggestion: str) -> dict[str, Any]:
    return {
        "_success": False,
        "error_code": INVALID_INPUT,
        "error": error,
        "suggestion": suggestion,
        "retryable": False,
        **_DISCOVERY_FLAGS,
    }


def _normalize_expected_registry(
    expected: dict[str, Any] | str | None,
) -> ExpectedRecipeRegistryV1 | None:
    if expected is None:
        return None
    if isinstance(expected, str):
        try:
            expected = json.loads(expected)
        except json.JSONDecodeError as exc:
            raise ValueError(f"must be valid JSON: {exc}") from exc
    if not isinstance(expected, dict):
        raise TypeError(
            "must be a JSON object, not " + type(expected).__name__
        )
    return ExpectedRecipeRegistryV1.model_validate(expected)


def recipe_descriptor_projection(registry: Any, descriptor: Any) -> dict[str, Any]:
    """The ONE projection of a registered recipe that both discovery surfaces serve.

    ``get_integration_archetype`` and ``get_schema_template('recipe:<id>@<v>')``
    publish exactly this: the descriptor's public payload, the input schema of
    the registration's own input model, and — only for a registration that
    declares the closed component-material channel — that channel's schema.
    Derived from the registration every time, never hand-copied, so the two
    tools cannot drift apart.
    """
    projection: dict[str, Any] = {"descriptor": descriptor.public_payload()}
    if descriptor.input_schema_id is not None:
        projection["input_schema"] = registry.input_model_for(
            descriptor
        ).model_json_schema()
        preparation = registry.component_preparation_for(descriptor)
        if preparation is not None:
            projection["component_material_schema"] = preparation[
                0
            ].model_json_schema()
    return projection


def _discoverable(descriptor: Any) -> bool:
    return descriptor.entry_kind == _DISCOVERABLE_ENTRY_KIND and bool(
        descriptor.is_default
    )


def list_integration_archetypes_action(
    query: str | None = None,
    expected_recipe_registry: dict[str, Any] | str | None = None,
) -> dict[str, Any]:
    """Every default executable recipe, optionally filtered by recipe id.

    ``query`` is a case-insensitive substring over the canonical ``recipe_id``.
    ``expected_recipe_registry`` is COMPARED with the live registry and
    reported as ``recipe_registry_skew``; it never selects what runs.
    """
    try:
        expected = _normalize_expected_registry(expected_recipe_registry)
    except (TypeError, ValueError) as exc:
        return _error(
            f"Invalid expected_recipe_registry argument: {exc}",
            "Provide expected_recipe_registry as a JSON object with "
            "schema_version='1' and an entries[] list of "
            "{recipe_id, recipe_version, implementation_sha256?}.",
        )

    if query is not None and not isinstance(query, str):
        return _error(
            f"query must be a string or None; got {type(query).__name__}",
            "Provide query as a substring of a recipe_id (or omit it to list "
            "every recipe).",
        )

    try:
        registry = production_registry()
    except RecipeError as exc:
        return recipe_error_envelope(exc)
    needle = query.strip().lower() if query else ""
    recipes = [
        descriptor.public_payload()
        for descriptor in registry.descriptors()
        if _discoverable(descriptor)
        and (not needle or needle in descriptor.recipe_id.lower())
    ]
    recipes.sort(key=lambda payload: payload["recipe_id"])

    return {
        "_success": True,
        "count": len(recipes),
        "recipes": recipes,
        "query": query,
        **_DISCOVERY_FLAGS,
        "next_tool": "get_integration_archetype",
        "recipe_registry": registry.snapshot(),
        "recipe_registry_skew": registry.compare(expected).model_dump(mode="json"),
    }


def get_integration_archetype_action(
    name: str, recipe_version: str | None = None
) -> dict[str, Any]:
    """One discoverable recipe by its published ``recipe_id`` and optional exact pin.

    An omitted ``recipe_version`` selects the id's default. A pin is EXACT: an
    empty, unknown or retired version refuses with the registry's own lookup
    diagnostic and never falls forward or backward.
    """
    if not isinstance(name, str) or not name.strip():
        return _error(
            "name must be a non-empty recipe_id",
            "Pass a recipe_id returned by list_integration_archetypes().",
        )
    if recipe_version is not None and not isinstance(recipe_version, str):
        return _error(
            "recipe_version must be a string or None; got "
            f"{type(recipe_version).__name__}",
            "Pass an exact recipe_version from list_integration_archetypes(), "
            "or omit it to select the default.",
        )

    try:
        registry = production_registry()
    except RecipeError as exc:
        return recipe_error_envelope(exc)
    try:
        descriptor = registry.resolve(name, recipe_version)
    except RecipeError as exc:
        return recipe_error_envelope(
            exc, registry_revision=registry.registry_revision
        )

    if descriptor.entry_kind != _DISCOVERABLE_ENTRY_KIND:
        return _error(
            f"{descriptor.recipe_id!r} is registered as "
            f"{descriptor.entry_kind!r}, not as an executable recipe",
            "Pass a recipe_id returned by list_integration_archetypes(); "
            "get_schema_template(schema_name='recipe:<id>') describes every "
            "registered entry kind.",
        )

    return {
        "_success": True,
        "recipe_id": descriptor.recipe_id,
        "recipe_version": descriptor.recipe_version,
        "schema_name": f"recipe:{descriptor.recipe_id}@{descriptor.recipe_version}",
        **recipe_descriptor_projection(registry, descriptor),
        **_DISCOVERY_FLAGS,
        "next_tool": _NEXT_TOOL,
        "next_steps": (
            "build_integration(action='plan', config={'authoring_request': "
            "{'contract_version': '2', 'intent': {'intent_kind': 'recipe', "
            "'integration_name': ..., 'invocations': [{'recipe_id': "
            f"'{descriptor.recipe_id}', 'recipe_version': "
            f"'{descriptor.recipe_version}', 'invocation_id': ..., "
            "'raw_input': {...matching input_schema}"
            + (
                ", 'component_material': {...matching component_material_schema}"
                if registry.component_preparation_for(descriptor) is not None
                else ""
            )
            + "}]}}})"
        ),
    }


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
        # ``code`` is the legacy key; ``error_code`` is the taxonomy-standard
        # branchable key (#10). Both carry the same value.
        "code": code,
        "error_code": code,
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
