"""Tests for the recipe discovery action layer (Issues #18, #19; recipe-only since #159 S3).

#159 S3 retired the archetype build and composition dispatchers. The two
discovery actions are backed by ``production_registry()`` alone: list returns
the default executable recipe descriptors, get returns one by its published
``recipe_id`` and optional exact pin, and authoring goes through
``build_integration(config.authoring_request)``. Node ids of the retired
build/tags tests are kept (the manifest pins them); their bodies now witness the
replacement route or the retirement itself.
"""

import inspect
import json
from unittest.mock import MagicMock, patch

import pytest

import boomi_mcp.categories.integration_authoring as authoring_module
from boomi_mcp.categories.integration_authoring import (
    get_integration_archetype_action,
    list_integration_archetypes_action,
)
from boomi_mcp.categories import integration_builder
from boomi_mcp.categories.integration_builder import build_integration_action
from boomi_mcp.categories.meta_tools import get_schema_template_action
from boomi_mcp.recipes import RecipeError, production_registry
from boomi_mcp.recipes.errors import recipe_diagnostic


_STUB = "boomi.archetype.stub_minimal"
_FANOUT = "boomi.compose.db_rest_fanout"

#: The six executable registrations S3 serves (frozen registry contract).
_SERVED = {
    ("boomi.archetype.api_to_database_sync", "1.0.0"),
    ("boomi.archetype.database_to_api_sync", "1.0.0"),
    ("boomi.archetype.http_listener_to_db", "1.0.0"),
    ("boomi.archetype.http_listener_to_rest", "1.0.0"),
    ("boomi.archetype.stub_minimal", "1.0.0"),
    ("boomi.compose.db_rest_fanout", "2.0.0"),
}


def _stub_request(raw_input=None):
    """A recipe authoring request built ONLY from what discovery served."""
    got = get_integration_archetype_action(_STUB)
    assert got["_success"] is True, got
    example = got["descriptor"]["examples"][0]
    return {
        "contract_version": "2",
        "intent": {
            "intent_kind": "recipe",
            "integration_name": "demo",
            "invocations": [
                {
                    "recipe_id": got["recipe_id"],
                    "recipe_version": got["recipe_version"],
                    "invocation_id": "stub",
                    "raw_input": (
                        dict(example["raw_input"]) if raw_input is None else raw_input
                    ),
                }
            ],
        },
    }


def _build(action, request):
    """build_integration offline: the account answers every listing empty, so
    collision resolution plans a create without touching a transport."""
    with patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []):
        return build_integration_action(
            MagicMock(), "test-profile", action, {"authoring_request": request}
        )


def _registry_failure(monkeypatch):
    def _boom():
        raise RecipeError(
            (recipe_diagnostic("RECIPE_NOT_FOUND", phase="lookup"),)
        )

    monkeypatch.setattr(authoring_module, "production_registry", _boom)


# ---------------------------------------------------------------------------
# list_integration_archetypes_action
# ---------------------------------------------------------------------------


def test_list_returns_stub_minimal_integration():
    result = list_integration_archetypes_action()
    assert result["_success"] is True
    assert result["raw_xml_exposed"] is False
    assert result["boomi_mutation"] is False
    assert result["count"] == len(result["recipes"]) == len(_SERVED)
    pins = {(r["recipe_id"], r["recipe_version"]) for r in result["recipes"]}
    assert pins == _SERVED
    assert _STUB in {r["recipe_id"] for r in result["recipes"]}
    assert result["query"] is None
    # No archetype-registry vocabulary survives on the recipe-only surface.
    assert "archetypes" not in result and "tags" not in result


def test_list_query_filter_hits_and_misses():
    hit = list_integration_archetypes_action(query="STUB")
    assert hit["_success"] is True
    assert [r["recipe_id"] for r in hit["recipes"]] == [_STUB]

    miss = list_integration_archetypes_action(query="nonexistent-recipe-xyzzy")
    assert miss["_success"] is True
    assert miss["count"] == 0
    assert miss["recipes"] == []


def test_list_tags_filter_native_list():
    """#159 S3: the legacy tags filter retired — the recipe registry has no tag
    authority. The action no longer takes it (tombstone candidate)."""
    assert "tags" not in inspect.signature(list_integration_archetypes_action).parameters
    with pytest.raises(TypeError):
        list_integration_archetypes_action(tags=["safe"])


def test_list_tags_filter_comma_string():
    """#159 S3: tags retired (tombstone candidate). A comma string is not
    reinterpreted as a query either."""
    with pytest.raises(TypeError):
        list_integration_archetypes_action(tags="safe,test")


def test_list_tags_filter_json_string():
    """#159 S3: tags retired (tombstone candidate)."""
    with pytest.raises(TypeError):
        list_integration_archetypes_action(tags='["safe"]')


# ---------------------------------------------------------------------------
# get_integration_archetype_action
# ---------------------------------------------------------------------------


def test_get_returns_metadata_and_parameter_schema():
    result = get_integration_archetype_action(_STUB)
    assert result["_success"] is True
    assert result["raw_xml_exposed"] is False
    assert result["next_tool"] == "build_integration"
    assert result["recipe_id"] == _STUB
    assert result["recipe_version"] == "1.0.0"
    assert result["schema_name"] == f"recipe:{_STUB}@1.0.0"

    descriptor = result["descriptor"]
    assert descriptor["recipe_id"] == _STUB
    assert descriptor["entry_kind"] == "executable_recipe"
    assert descriptor["is_default"] is True

    schema = result["input_schema"]
    assert "process_key" in schema["properties"]
    # The stub declares no component-material channel, so none is served.
    assert "component_material_schema" not in result


def test_get_missing_archetype_returns_failure():
    result = get_integration_archetype_action("does-not-exist")
    assert result["_success"] is False
    assert result["error_code"] == "RECIPE_NOT_FOUND"
    # A legacy archetype NAME is not a recipe id.
    legacy = get_integration_archetype_action("stub_minimal_integration")
    assert legacy["_success"] is False
    assert legacy["error_code"] == "RECIPE_NOT_FOUND"


# ---------------------------------------------------------------------------
# Authoring a discovered recipe (build_from_archetype_action retired, #159 S3)
# ---------------------------------------------------------------------------


def test_build_valid_returns_stub_spec():
    """list -> get -> build_integration(plan): the replacement for the retired
    build_from_archetype route, driven only by what discovery served."""
    assert not hasattr(authoring_module, "build_from_archetype_action")
    plan = _build("plan", _stub_request())
    assert plan["_success"] is True, plan
    assert plan.get("mutation_performed") in (None, False)


def test_build_invalid_parameters_returns_field_error():
    plan = _build("plan", _stub_request(raw_input={"version": "1"}))
    assert plan["_success"] is False
    # The recipe input gate refuses it; the plan reports the blocked compile
    # with the recipe's own code among its diagnostics.
    assert plan["error_code"] == "AUTHORING_COMPILE_BLOCKED"
    assert "RECIPE_INPUT_INVALID" in json.dumps(plan, default=str)


def test_build_parameters_as_json_string():
    """The one JSON-string-tolerant discovery argument: the expected registry."""
    snapshot = production_registry().snapshot()
    result = list_integration_archetypes_action(
        expected_recipe_registry=json.dumps(
            {
                "schema_version": "1",
                "registry_revision": snapshot["registry_revision"],
                "entries": [],
            }
        )
    )
    assert result["_success"] is True
    assert result["recipe_registry_skew"]["status"] != "not_requested"


def test_build_missing_archetype_returns_failure():
    """A RETIRED pin never resolves and never falls forward."""
    for recipe_id, version in (
        (_FANOUT, "1.0.0"),
        ("boomi.archetype.api_to_database_sync", "0.1.0"),
    ):
        result = get_integration_archetype_action(recipe_id, recipe_version=version)
        assert result["_success"] is False, (recipe_id, version)
        assert result["error_code"] == "RECIPE_VERSION_UNAVAILABLE"
        assert version not in result["recipe_diagnostics"][0]["available_versions"]
    for retired in ("boomi.archetype.api_to_api_sync", "boomi.adapter.compose_archetypes"):
        result = get_integration_archetype_action(retired)
        assert result["_success"] is False, retired
        assert result["error_code"] == "RECIPE_NOT_FOUND"


def test_build_bad_parameter_shape_returns_validation_error():
    result = list_integration_archetypes_action(expected_recipe_registry="not-json")
    assert result["_success"] is False
    assert result["error_code"] == "INVALID_INPUT"
    assert "not-json" not in result["error"]


# ---------------------------------------------------------------------------
# Cross-cutting: outputs are JSON-serializable and never contain XML
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "callable_factory",
    [
        lambda: list_integration_archetypes_action(),
        lambda: get_integration_archetype_action(_STUB),
        lambda: get_integration_archetype_action(_FANOUT, recipe_version="2.0.0"),
    ],
)
def test_outputs_have_no_xml_or_primitive_markers(callable_factory):
    result = callable_factory()
    payload = json.dumps(result)
    for marker in ("<?xml", "<process", "<component", "<connector", "<operation"):
        assert marker not in payload, f"Unexpected XML marker {marker!r} in {payload[:200]}..."


# ===========================================================================
# Issue #19 — hardening (recipe-only since #159 S3)
# ===========================================================================


def test_list_excludes_primitives(monkeypatch):
    """Only DEFAULT EXECUTABLE recipes are listed: the advisory and
    constraint-only registrations are registered but are not integrations."""
    result = list_integration_archetypes_action()
    kinds = {r["entry_kind"] for r in result["recipes"]}
    assert kinds == {"executable_recipe"}
    assert all(r["is_default"] is True for r in result["recipes"])
    registered = {d.entry_kind for d in production_registry().descriptors()}
    # Non-vacuous: the registry really carries the kinds the list drops.
    assert {"advisory", "constraint_only"} <= registered


def test_list_returns_structured_error_when_discovery_fails(monkeypatch):
    _registry_failure(monkeypatch)
    result = list_integration_archetypes_action()
    assert result["_success"] is False
    assert result["error_code"] == "RECIPE_NOT_FOUND"
    assert result["boomi_mutation"] is False


def test_list_invalid_tags_returns_structured_error():
    """#159 S3: tags retired (tombstone candidate); the remaining object
    argument still refuses a non-object with a structured INVALID_INPUT."""
    result = list_integration_archetypes_action(expected_recipe_registry=42)
    assert result["_success"] is False
    assert result["error_code"] == "INVALID_INPUT"
    assert "expected_recipe_registry" in result["error"]


def test_list_invalid_query_returns_structured_error():
    result = list_integration_archetypes_action(query=123)
    assert result["_success"] is False
    assert result["error_code"] == "INVALID_INPUT"
    assert "query" in result["error"].lower()


def test_get_schema_is_machine_readable_and_strict():
    for row in list_integration_archetypes_action()["recipes"]:
        result = get_integration_archetype_action(
            row["recipe_id"], recipe_version=row["recipe_version"]
        )
        assert result["_success"] is True, row
        schema = result["input_schema"]
        assert schema.get("type") == "object"
        assert schema.get("additionalProperties") is False, row["recipe_id"]
        material = result.get("component_material_schema")
        if material is not None:
            assert material.get("additionalProperties") is False, row["recipe_id"]

        payload = json.dumps(result)
        for marker in (
            "<?xml",
            "<process",
            "<component",
            "<connector",
            "<operation",
            "boomi.models",
            "BoomiClient",
        ):
            assert marker not in payload, f"Unexpected leakage marker {marker!r}"


def test_get_returns_structured_error_when_discovery_fails(monkeypatch):
    _registry_failure(monkeypatch)
    result = get_integration_archetype_action(_STUB)
    assert result["_success"] is False
    assert result["error_code"] == "RECIPE_NOT_FOUND"


def test_build_uses_validate_parameters_entry_point(monkeypatch):
    """ONE projection: get serves exactly what get_schema_template serves for
    the same exact pin — descriptor, input schema and material schema."""
    for recipe_id, version in sorted(_SERVED):
        got = get_integration_archetype_action(recipe_id, recipe_version=version)
        served = get_schema_template_action(schema_name=f"recipe:{recipe_id}@{version}")
        assert served["_success"] is True, recipe_id
        for key in ("descriptor", "input_schema", "component_material_schema"):
            assert got.get(key) == served.get(key), (recipe_id, key)
        assert got["schema_name"] == served["schema_name"]


def test_build_validation_error_does_not_echo_input_values():
    secret = "sk_live_super_secret_token_DEADBEEF"
    plan = _build(
        "plan",
        _stub_request(
            raw_input={
                "version": "1",
                "process_key": "main_process",
                "rogue_field": secret,
            }
        ),
    )
    assert plan["_success"] is False
    assert secret not in json.dumps(plan, default=str), (
        "a refused recipe input must not be echoed"
    )


def test_build_success_spec_is_accepted_by_build_integration_plan():
    """list -> get -> plan -> compile, for the one recipe whose served example
    is complete without component material."""
    listed = list_integration_archetypes_action(query="stub")
    assert [r["recipe_id"] for r in listed["recipes"]] == [_STUB]
    request = _stub_request()
    for action in ("plan", "compile"):
        result = _build(action, request)
        assert result["_success"] is True, (action, result)
        json.dumps(result, default=str)


def test_get_action_returns_enriched_describe_payload():
    """Every listed row carries the descriptor enrichment a client authors from."""
    for row in list_integration_archetypes_action()["recipes"]:
        result = get_integration_archetype_action(row["recipe_id"])
        assert result["_success"] is True
        descriptor = result["descriptor"]
        for key in ("capability_notes", "limitations", "examples", "not_for"):
            assert key in descriptor, (row["recipe_id"], key)
        assert descriptor["limitations"], row["recipe_id"]
        assert descriptor["examples"], row["recipe_id"]
        for example in descriptor["examples"]:
            assert example["is_template"] is False
        json.dumps(result)


def test_every_listed_recipe_is_fetchable_by_its_exact_pin():
    """QA-159-S2-r1-02: every row list returns resolves through get by its EXACT
    pin to the same descriptor, and an omitted pin selects that same default."""
    rows = list_integration_archetypes_action()["recipes"]
    assert {(r["recipe_id"], r["recipe_version"]) for r in rows} == _SERVED
    for row in rows:
        pinned = get_integration_archetype_action(
            row["recipe_id"], recipe_version=row["recipe_version"]
        )
        assert pinned["_success"] is True, row
        assert pinned["descriptor"] == row
        unpinned = get_integration_archetype_action(row["recipe_id"])
        assert unpinned["recipe_version"] == row["recipe_version"]
        assert unpinned["descriptor"] == row
