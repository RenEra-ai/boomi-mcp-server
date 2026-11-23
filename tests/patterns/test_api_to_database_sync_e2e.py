"""Issue #74 (M5.8): api_to_database_sync preset over sync_pipeline — end-to-end.

Proves the preset's contract and the full local chain through the *public*
action entry points, without live Boomi:

    build_from_archetype_action  ->  build_integration_action(plan / apply)

Acceptance criteria covered:
* Preset maps to ``sync_pipeline`` stages (main process carries
  ``process_kind="sync_pipeline"`` with an intact fetch -> map -> write graph and
  no pairwise source/target/transform process config).
* Database write behavior routes through the confirmed #32 component builders
  (a profile.db write profile + a database Send connector-action), and the
  transform map binds that write profile (Fields/Conditions) as its target.
* Unconfirmed write-profile variants (e.g. ``upsert``) remain blocked
  (UNSUPPORTED_DB_STATEMENT_TYPE).
* No raw XML or canned payload templates are exposed.

Payloads use sentinel placeholders only (``<<...>>``), never canned payloads,
mappings, raw XML, credentials, or live account IDs.
"""

from __future__ import annotations

import copy
import json
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from boomi_mcp.categories.integration_builder import build_integration_action
from boomi_mcp.categories.meta_tools import get_schema_template_action


def build_from_archetype_action(*args, **kwargs):
    """The retired ``build_from_archetype`` tool's action (#159 S3 deletes it).

    Resolved at CALL time, so this module still collects once it is gone and only
    the tests that call it are affected.
    """
    from boomi_mcp.categories import integration_authoring

    return getattr(integration_authoring, "build_from_archetype_action")(*args, **kwargs)


def get_integration_archetype_action(*args, **kwargs):
    """The discovery action #159 S3 reshapes to recipe-only (lane D).

    Resolved at CALL time, so this module still collects once it is gone and only
    the tests that call it are affected.
    """
    from boomi_mcp.categories import integration_authoring

    return getattr(integration_authoring, "get_integration_archetype_action")(*args, **kwargs)


def list_integration_archetypes_action(*args, **kwargs):
    """The discovery action #159 S3 reshapes to recipe-only (lane D).

    Resolved at CALL time, so this module still collects once it is gone and only
    the tests that call it are affected.
    """
    from boomi_mcp.categories import integration_authoring

    return getattr(integration_authoring, "list_integration_archetypes_action")(*args, **kwargs)


_ARCHETYPE = "api_to_database_sync"
_PAGINATE_TARGET = "boomi_mcp.categories.integration_builder.paginate_metadata"
_EXECUTE_TARGET = "boomi_mcp.categories.integration_builder._execute_component"
_PROFILE = "qa-profile"  # never used to reach Boomi; calls are fully mocked

_EXPECTED_KEYS = {
    "source_response_profile",
    "source_rest_source_connection",
    "source_rest_source_operation",
    "target_db_connection",
    "target_db_write_profile",
    "target_db_write_operation",
    "transform_transform_map",
    "main_process",
}
_STAGE_DEPENDS = [
    "source_rest_source_connection",
    "source_rest_source_operation",
    "transform_transform_map",
    "target_db_connection",
    "target_db_write_operation",
]


# ---------------------------------------------------------------------------
# Payload builders (synthetic, sentinel-only)
# ---------------------------------------------------------------------------


def _minimal() -> Dict[str, Any]:
    """Smallest executable payload: create REST source (no auth) + reuse DB
    target connection + a dynamic-insert write profile + a single direct
    transform into a write-profile column."""
    return {
        "naming": {"integration_name": "demo-api-db-sync", "component_prefix": "DEMO"},
        "source": {
            "binding": {
                "mode": "create",
                "settings": {
                    "base_url": "https://source.example.com",
                    "auth_mode": "none",
                },
            },
            "fetch_request": {"path": "/v1/<<source resource>>"},
            "response_profile": {
                "format": "json",
                "root": {
                    "name": "Root",
                    "kind": "object",
                    "children": [
                        {"name": "source_a", "kind": "simple", "data_type": "character"}
                    ],
                },
            },
        },
        "target": {
            "connection": {"mode": "reuse", "component_id": "<<existing db conn id>>"},
            "write_profile": {
                "statement_type": "dynamicinsert",
                "table_name": "<<target table>>",
                "fields": [
                    {"name": "col_a", "data_type": "character", "mandatory": True}
                ],
            },
        },
        "transform": {
            "operations": [
                {
                    "operation_type": "direct",
                    "source_path": "Root/source_a",
                    "target_path": "Fields/col_a",
                }
            ]
        },
    }


def _dynamic_update() -> Dict[str, Any]:
    """Reuse-mode DB target with a dynamic update (Fields + WHERE Conditions) and
    a map_function."""
    return {
        "naming": {
            "integration_name": "demo-api-db-upsert",
            "component_prefix": "DEMO-UPD",
            "folder_path": "Integrations/API/DB",
        },
        "source": {
            "binding": {"mode": "reuse", "component_id": "<<existing source conn id>>"},
            "fetch_request": {
                "path": "/v1/<<source resource>>",
                "query_parameters": {"limit": "100"},
            },
            "response_profile": {
                "format": "json",
                "root": {
                    "name": "Root",
                    "kind": "object",
                    "children": [
                        {"name": "id", "kind": "simple", "data_type": "character"},
                        {"name": "updated_at", "kind": "simple", "data_type": "datetime"},
                    ],
                },
            },
        },
        "target": {
            "connection": {"mode": "reuse", "component_name": "<<existing db conn>>"},
            "write_profile": {
                "statement_type": "dynamicupdate",
                "table_name": "<<target table>>",
                "fields": [{"name": "modified", "data_type": "datetime"}],
                "conditions": [{"name": "key_id", "data_type": "character"}],
            },
        },
        "transform": {
            "operations": [
                {
                    "operation_type": "direct",
                    "source_path": "Root/id",
                    "target_path": "Conditions/key_id",
                },
                {
                    "operation_type": "map_function",
                    "function_type": "date_format",
                    "inputs": ["Root/updated_at"],
                    "target_path": "Fields/modified",
                    "parameters": {
                        "input_format": "<<source datetime format>>",
                        "output_format": "<<target datetime format>>",
                    },
                },
            ]
        },
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build(params: Dict[str, Any]) -> Dict[str, Any]:
    return build_from_archetype_action(_ARCHETYPE, params)


def _spec(params: Dict[str, Any]) -> Dict[str, Any]:
    result = _build(params)
    assert result["_success"] is True, result
    return result["integration_spec"]


def _by_key(spec: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {c["key"]: c for c in spec["components"]}


def _plan(spec: Dict[str, Any], mock_pag, conflict_policy: str = "reuse") -> Dict[str, Any]:
    mock_pag.return_value = []
    return build_integration_action(
        MagicMock(),
        _PROFILE,
        "plan",
        {"integration_spec": spec, "conflict_policy": conflict_policy},
    )


def _has_ref_token(value: Any) -> bool:
    if isinstance(value, str):
        return value.startswith("$ref:")
    if isinstance(value, dict):
        return any(_has_ref_token(v) for v in value.values())
    if isinstance(value, list):
        return any(_has_ref_token(v) for v in value)
    return False


# ===========================================================================
# Catalog / schema discovery
# ===========================================================================


class TestCatalogAndSchema:
    # #159 S3: discovery is recipe-only (step-4 decision 1). The surviving
    # API->DB executable is ``boomi.archetype.api_to_database_sync`` 1.0.0.
    _RECIPE_ID = "boomi.archetype.api_to_database_sync"

    def test_list_includes_api_to_database_sync(self):
        result = list_integration_archetypes_action(query="database")
        assert result["_success"] is True
        listed = {r["recipe_id"]: r["recipe_version"] for r in result["recipes"]}
        assert listed.get(self._RECIPE_ID) == "1.0.0"
        assert result["raw_xml_exposed"] is False

    def test_get_returns_strict_schema_without_xml(self):
        result = get_integration_archetype_action(self._RECIPE_ID)
        assert result["_success"] is True
        assert result["raw_xml_exposed"] is False
        assert (result["recipe_id"], result["recipe_version"]) == (self._RECIPE_ID, "1.0.0")
        descriptor = result["descriptor"]
        assert descriptor["capability_notes"], "must publish capability_notes"
        assert descriptor["limitations"], "must publish limitations"
        assert descriptor["examples"], "must publish at least one example"
        # Strict contract: the input AND the material channel forbid extras.
        for schema in (result["input_schema"], result["component_material_schema"]):
            assert schema.get("additionalProperties") is False
            assert schema["properties"]
        assert "<?xml" not in json.dumps(result)

    def test_schema_template_resolves(self):
        # #159 S3: the ``archetype:<name>`` selector retired; the preset's schema
        # is served through its exact recipe pin.
        retired = get_schema_template_action(schema_name=f"archetype:{_ARCHETYPE}")
        assert retired["_success"] is False
        assert retired["error_code"] == "SCHEMA_NAME_UNSUPPORTED"
        result = get_schema_template_action(schema_name="recipe:boomi.archetype.api_to_database_sync@1.0.0")
        assert result["_success"] is True, result
        assert "<?xml" not in json.dumps(result)


# ===========================================================================
# Emitted spec maps to sync_pipeline stages
# ===========================================================================


# ===========================================================================
# Parameter validation (negative)
# ===========================================================================


# ===========================================================================
# Full local chain: plan + apply
# ===========================================================================


# ===========================================================================
# #159 S2: API->DB 1.0.0 — typed direct unit through the component-material channel
# ===========================================================================
#
# Oracles: the two frozen #157 cases' historical inputs (``input.legacy.json``)
# and the legacy archetype at HEAD (read-only, called as an oracle only). The
# canonical inputs are DERIVED from the legacy inputs by
# :func:`_canonical_request_v1` (a mechanical re-spelling) and the committed
# ``input.canonical.json`` files must equal that derivation byte-for-value.

import re as _re
import sys as _sys
from pathlib import Path as _Path

_TESTS_DIR = _Path(__file__).resolve().parents[1]
if str(_TESTS_DIR) not in _sys.path:
    _sys.path.insert(0, str(_TESTS_DIR))

from boomi_mcp.recipes import component_material as _component_material  # noqa: E402
from boomi_mcp.recipes.builtins import catalog as _catalog  # noqa: E402
from boomi_mcp.recipes.builtins.sync import (  # noqa: E402
    ApiToDatabaseSyncInputV1,
    ApiToDatabaseSyncMaterialV1,
    emit_api_to_database_sync_v1,
    prepare_api_to_database_sync_v1,
)
from boomi_mcp.recipes.contracts import RecipeRegistrationV1  # noqa: E402
from boomi_mcp.recipes.errors import RecipeError  # noqa: E402
from boomi_mcp.recipes.registry import build_test_registry  # noqa: E402
from pydantic import ValidationError as _ValidationError  # noqa: E402

_V1 = "1.0.0"
_CASES_DIR = _TESTS_DIR / "fixtures" / "governance" / "issue_157" / "cases" / "api_to_database_sync"
_V1_CASES = ("create", "source_reuse")


def _v1_registration() -> RecipeRegistrationV1:
    """The registration handed to the integrator (the catalog is not lane-owned)."""
    return RecipeRegistrationV1(
        recipe_id=_catalog.RECIPE_API_TO_DATABASE_SYNC,
        recipe_version=_V1,
        entry_kind="executable_recipe",
        is_default=False,
        input_model=ApiToDatabaseSyncInputV1,
        executor=emit_api_to_database_sync_v1,
        output_types=("constraint_requirement", "process_unit"),
        capability_requirements=(
            _catalog._SOURCE_EMITTER,
            _catalog._TARGET_EMITTER,
            _catalog._MAP_EMITTER,
        ),
        conflict_policy=_catalog._SYNC_CONFLICT_POLICY,
        component_material_model=ApiToDatabaseSyncMaterialV1,
        component_preparer=prepare_api_to_database_sync_v1,
        implementation_dependency_modules=(_component_material.__name__,),
    )


def _v1_registry():
    """Production registrations plus 1.0.0 (unless the catalog already has it)."""
    registered = {
        (reg.recipe_id, reg.recipe_version) for reg in _catalog.PRODUCTION_REGISTRATIONS
    }
    extra = () if (_catalog.RECIPE_API_TO_DATABASE_SYNC, _V1) in registered else (_v1_registration(),)
    return build_test_registry(_catalog.PRODUCTION_REGISTRATIONS + extra)


def _respelled_path(path: str) -> str:
    """The ONE value re-spelling: a ``<<placeholder>>`` path segment becomes its
    words joined by ``-``. The shared request-target grammar refuses ``<``,
    ``>`` and spaces; the path is in no frozen row."""
    return _re.sub(
        r"<<([^<>]*)>>",
        lambda match: _re.sub(r"[^A-Za-z0-9]+", "-", match.group(1)).strip("-"),
        path,
    )


def _legacy_selector(binding: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "mode": "reuse",
        **{key: binding[key] for key in ("component_id", "component_name") if binding.get(key)},
    }


def _v1_split(parameters: Dict[str, Any]):
    """``(raw_input, component_material)`` re-spelled from LEGACY parameters."""
    naming = {"component_prefix": parameters["naming"]["component_prefix"]}
    if parameters["naming"].get("folder_path"):
        naming["folder_name"] = parameters["naming"]["folder_path"]
    source_binding = parameters["source"]["binding"]
    target_connection = parameters["target"]["connection"]
    target: Dict[str, Any] = {"binding_mode": target_connection["mode"]}
    if target_connection["mode"] == "create":
        target["auth_mode"] = target_connection["auth_mode"]
    raw_input = {
        "version": "1",
        "process_key": "main_process",
        "naming": naming,
        "source": {"binding_mode": source_binding["mode"]},
        "target": target,
    }
    if source_binding["mode"] == "create":
        source_connection = {"mode": "create", "base_url": source_binding["settings"]["base_url"]}
    else:
        source_connection = _legacy_selector(source_binding)
    fetch = parameters["source"]["fetch_request"]
    if target_connection["mode"] == "create":
        db_connection = {"mode": "create", "driver": target_connection["driver_id"]}
        for key in ("host", "port", "database", "username", "credential_ref"):
            if target_connection.get(key) is not None:
                db_connection[key] = target_connection[key]
    else:
        db_connection = _legacy_selector(target_connection)
    operations = []
    for operation in parameters["transform"]["operations"]:
        if operation["operation_type"] == "direct":
            converted = {
                "operation_type": "direct",
                "source_field": operation["source_path"],
                "target_path": operation["target_path"],
            }
        else:
            converted = {k: v for k, v in operation.items() if k != "parameters"}
            if operation.get("parameters"):
                converted["parameters"] = [
                    {"name": name, "value": value} for name, value in operation["parameters"].items()
                ]
        if operation.get("documentation_hint") is not None:
            converted["documentation_hint"] = operation["documentation_hint"]
        operations.append(converted)
    target_material: Dict[str, Any] = {
        "connection": db_connection,
        "write_profile": copy.deepcopy(parameters["target"]["write_profile"]),
    }
    if parameters["target"].get("send_operation"):
        target_material["operation"] = copy.deepcopy(parameters["target"]["send_operation"])
    material = {
        "version": "1",
        "source": {
            "connection": source_connection,
            "request": {
                "path": _respelled_path(fetch["path"]),
                "query_parameters": [
                    {"name": name, "value_source": "literal", "literal_value": value}
                    for name, value in (fetch.get("query_parameters") or {}).items()
                ],
            },
            "response_profile": copy.deepcopy(parameters["source"]["response_profile"]),
        },
        "target": target_material,
        "transform": {"operations": operations},
    }
    return raw_input, material


def _canonical_request_v1(case: str) -> Dict[str, Any]:
    """The case's canonical ``AuthoringRequestV1``: one pinned 1.0.0 invocation."""
    legacy = json.loads((_CASES_DIR / case / "input.legacy.json").read_text())
    raw_input, material = _v1_split(legacy["input"]["parameters"])
    return {
        "contract_version": "2",
        "intent": {
            "integration_name": f"issue-157 replay api_to_database_sync/{case}",
            "intent_kind": "recipe",
            "invocations": [
                {
                    "recipe_id": _catalog.RECIPE_API_TO_DATABASE_SYNC,
                    "recipe_version": _V1,
                    "invocation_id": "api_to_database_sync",
                    "raw_input": raw_input,
                    "component_material": material,
                }
            ],
        },
    }


def _legacy_parameters(case: str) -> Dict[str, Any]:
    return json.loads((_CASES_DIR / case / "input.legacy.json").read_text())["input"]["parameters"]


def _legacy_spec(case: str):
    """The LEGACY archetype's spec for the case's frozen parameters — the oracle
    the 1.0.0 parity tests compare against.

    ``emit_spec`` ends by re-running its components through the 0.1.0 preset, a
    verification whose result it discards (its own comment: "the emitted spec
    below is byte-unchanged"). #159 S3 retired that registration, so that one
    check is stubbed; every byte of the spec is still the isolated legacy
    archetype's (kept until #160).
    """
    from boomi_mcp.patterns.archetypes import api_to_database_sync as legacy_module

    with patch.object(legacy_module, "run_sync_preset_recipe", lambda **_kwargs: None):
        return legacy_module.ApiToDatabaseSyncArchetype.emit_spec(
            legacy_module.ApiToDatabaseSyncParameters.model_validate(_legacy_parameters(case))
        )


def _v1_prepared(raw_input, material, base_catalog=None):
    return prepare_api_to_database_sync_v1(
        ApiToDatabaseSyncInputV1.model_validate(raw_input),
        ApiToDatabaseSyncMaterialV1.model_validate(material),
        base_catalog=base_catalog or {},
    )


def canary_api_to_database_sync_v1():
    """Seed for ``_CANARY_SEEDS`` in ``tests/test_recipe_component_material.py``
    (handed to the integrator): ``(raw_input, material)`` reaching every
    secret-classified shared field — a REST create ``base_url``, the fetch
    ``path``, a LITERAL query parameter's name and value, and a DB create
    target's ``host``, ``database`` and ``credential_ref``."""
    raw_input, material = _v1_split(_legacy_parameters("create"))
    raw_input["target"] = {"binding_mode": "create", "auth_mode": "username_password"}
    material["source"]["request"]["query_parameters"] = [
        {"name": "limit", "value_source": "literal", "literal_value": "50"}
    ]
    material["target"]["connection"] = {
        "mode": "create",
        "driver": "microsoft_jdbc",
        "host": "db.example.invalid",
        "database": "AppDB",
        "username": "svc_sync",
        "credential_ref": "credential://secrets/db/svc_sync",
    }
    return raw_input, material


class TestRecipeV1:
    @pytest.mark.parametrize("case", _V1_CASES)
    def test_v1_canonical_input_is_the_derivation_of_the_legacy_input(self, case):
        committed = json.loads((_CASES_DIR / case / "input.canonical.json").read_text())
        assert committed == _canonical_request_v1(case)
        provenance = json.loads((_CASES_DIR / case / "provenance.json").read_text())
        assert provenance["case_id"] == f"api_to_database_sync/{case}"
        assert provenance["executed_at_replay"] is False

    @pytest.mark.parametrize("case", _V1_CASES)
    def test_v1_components_match_the_legacy_route(self, case):
        """Component-set parity: keys, order, types, names, configs and
        dependencies equal the legacy archetype's — except the fetch path,
        re-spelled by the derivation's one documented rule on BOTH sides."""
        spec = _legacy_spec(case)
        expected = [component.model_dump(mode="json") for component in spec.components[:-1]]
        for component in expected:
            if component["key"] == "source_rest_source_operation":
                component["config"]["path"] = _respelled_path(component["config"]["path"])
        invocation = _canonical_request_v1(case)["intent"]["invocations"][0]
        prepared = _v1_prepared(invocation["raw_input"], invocation["component_material"])
        assert [c.model_dump(mode="json") for c in prepared.supporting_components] == expected
        assert prepared.selected_base_keys == ()
        assert len(prepared.rest_request_targets) == 1

    @pytest.mark.parametrize("case", _V1_CASES)
    def test_v1_root_is_the_legacy_sync_pipeline_root(self, case):
        """Process parity: the unit's root equals the legacy adapter's root once
        its adapter pointers are resolved against the legacy process config —
        the same IR the canonical compiler emits for both — and the envelope
        carries the legacy name, folder and dependency set."""
        from boomi_mcp.compiler.process_ir.legacy_adapters.sync_pipeline import (
            adapt_sync_pipeline_config,
        )

        process = _legacy_spec(case).components[-1]
        adapted = adapt_sync_pipeline_config(process.config)
        prefix = "$ref:legacy.adapter:"

        def resolved(node):
            if isinstance(node, dict):
                return {key: resolved(value) for key, value in node.items()}
            if isinstance(node, list):
                return [resolved(value) for value in node]
            if isinstance(node, str) and node.startswith(prefix):
                cursor = process.config
                for part in node[len(prefix):].strip("/").split("/"):
                    cursor = cursor[int(part)] if isinstance(cursor, list) else cursor[part]
                return cursor
            return node

        raw_input = _canonical_request_v1(case)["intent"]["invocations"][0]["raw_input"]
        unit = emit_api_to_database_sync_v1(ApiToDatabaseSyncInputV1.model_validate(raw_input))[0].unit
        assert unit.process_ir.model_dump(mode="json") == resolved(
            adapted.process_ir.model_dump(mode="json")
        )
        assert unit.envelope.component_key == process.key
        assert unit.envelope.name == process.name
        assert unit.envelope.folder_name == process.config.get("folder_name")
        assert set(unit.envelope.depends_on) == set(process.depends_on)

    def test_v1_registers_beside_the_default_0_1_0_pin(self):
        # #159 S3: 0.1.0 is retired and 1.0.0 is the default and only version —
        # the node id keeps its S2 spelling; the body pins the cutover's state.
        from boomi_mcp.errors import RECIPE_VERSION_UNAVAILABLE

        registry = _v1_registry()
        pinned = registry.resolve(_catalog.RECIPE_API_TO_DATABASE_SYNC, _V1)
        default = registry.resolve(_catalog.RECIPE_API_TO_DATABASE_SYNC, None)
        assert pinned.recipe_version == _V1
        assert default.recipe_version == _V1
        with pytest.raises(RecipeError) as caught:
            registry.resolve(_catalog.RECIPE_API_TO_DATABASE_SYNC, "0.1.0")
        assert caught.value.diagnostics[0].code == RECIPE_VERSION_UNAVAILABLE
        model, preparer = registry.component_preparation_for(pinned)
        assert model is ApiToDatabaseSyncMaterialV1
        assert preparer is prepare_api_to_database_sync_v1


def _refused(raw_input, material, base_catalog=None) -> RecipeError:
    with pytest.raises(RecipeError) as caught:
        _v1_prepared(raw_input, material, base_catalog)
    return caught.value


def _mutated(mutate):
    raw_input, material = _v1_split(_legacy_parameters("create"))
    mutate(raw_input, material)
    return raw_input, material


def _set_source_path(raw, mat):
    mat["source"]["request"]["path"] = "/v1/items/{item_id}"


def _unknown_source_leaf(raw, mat):
    mat["transform"]["operations"][0]["source_field"] = "Root/not_declared"


def _unknown_target_column(raw, mat):
    mat["transform"]["operations"][1]["target_path"] = "Fields/not_declared"


def _required_column_unmapped(raw, mat):
    del mat["transform"]["operations"][0]


def _column_bound_twice(raw, mat):
    mat["transform"]["operations"][1]["target_path"] = "Fields/id"


def _upsert_statement(raw, mat):
    mat["target"]["write_profile"]["statement_type"] = "upsert"


def _watermark_query_parameter(raw, mat):
    mat["source"]["request"]["query_parameters"] = [
        {"name": "updated_since", "value_source": "watermark"}
    ]


def _unpublished_literal_query_name(raw, mat):
    mat["source"]["request"]["query_parameters"] = [
        {"name": "api_key", "value_source": "literal", "literal_value": "sekret-value"}
    ]


def _map_script_reference(raw, mat):
    mat["transform"]["operations"].append(
        {
            "operation_type": "map_script",
            "script_component_key": "shared_script",
            "inputs": ["Root/id"],
            "outputs": ["Fields/name"],
        }
    )
    del mat["transform"]["operations"][1]


def _map_script_port_collision_across(raw, mat):
    """``Root/id`` in and ``Fields/id`` out both derive the script variable ``id``."""
    mat["transform"]["operations"].append(
        {
            "operation_type": "map_script",
            "script_component_key": "shared_script",
            "inputs": ["Root/id"],
            "outputs": ["Fields/id"],
        }
    )
    del mat["transform"]["operations"][0]


def _map_script_port_collision_inputs(raw, mat):
    """Two distinct source paths deriving the same input variable ``id``."""
    mat["source"]["response_profile"]["root"]["children"].append(
        {"name": "sub", "kind": "object", "children": [
            {"name": "id", "kind": "simple", "data_type": "character"}]}
    )
    mat["transform"]["operations"].append(
        {
            "operation_type": "map_script",
            "script_component_key": "shared_script",
            "inputs": ["Root/id", "Root/sub/id"],
            "outputs": ["Fields/name"],
        }
    )
    del mat["transform"]["operations"][1]


def _material_carries_selected_connection(raw, mat):
    raw["target"]["connection_key"] = "shared_db"


_V1_REFUSALS = {
    "dynamic_path_token": (_set_source_path, "/component_material/source/request/path"),
    "unknown_source_leaf": (_unknown_source_leaf, "/component_material/transform/operations/0"),
    "unknown_target_column": (_unknown_target_column, "/component_material/transform/operations/1"),
    "required_column_unmapped": (_required_column_unmapped, "/component_material/transform"),
    "column_bound_twice": (_column_bound_twice, "/component_material/transform/operations/1"),
    "unsupported_statement_type": (_upsert_statement, "/component_material/target/write_profile"),
    "watermark_query_parameter": (
        _watermark_query_parameter,
        "/component_material/source/request/query_parameters/0",
    ),
    "unpublished_literal_query_name": (
        _unpublished_literal_query_name,
        "/component_material/source/request/query_parameters/0/name",
    ),
    # #159 S2 integration: a map script is ACCEPTED by reference to a base
    # script.mapping component; with no such base the reference is refused.
    "map_script_reference": (
        _map_script_reference,
        "/component_material/transform/operations/1/script_component_key",
    ),
    # The script's inputs and outputs share ONE variable namespace (the legacy
    # contract's rule), refused before any base lookup or assembly.
    "map_script_port_collision_across": (
        _map_script_port_collision_across,
        "/component_material/transform/operations/1",
    ),
    "map_script_port_collision_inputs": (
        _map_script_port_collision_inputs,
        "/component_material/transform/operations/1",
    ),
    "selected_key_with_material_connection": (
        _material_carries_selected_connection,
        "/component_material/target/connection",
    ),
}


class TestRecipeV1Refusals:
    @pytest.mark.parametrize("name", sorted(_V1_REFUSALS))
    def test_v1_refusal_is_value_free_and_precedes_assembly(self, name, monkeypatch):
        from boomi_mcp.models.integration_models import IntegrationComponentSpec

        mutate, pointer = _V1_REFUSALS[name]
        raw_input, material = _mutated(mutate)
        built = []
        real_init = IntegrationComponentSpec.__init__

        def spying_init(self, *args, **kwargs):
            built.append(kwargs.get("key"))
            real_init(self, *args, **kwargs)

        monkeypatch.setattr(IntegrationComponentSpec, "__init__", spying_init)
        # Control: the unmutated material DOES assemble (the spy sees it).
        _v1_prepared(*_mutated(lambda raw, mat: None))
        assert built
        del built[:]
        error = _refused(raw_input, material)
        assert built == []
        (diagnostic,) = error.diagnostics
        assert diagnostic.path == pointer
        rendered = json.dumps([d.model_dump(mode="json") for d in error.diagnostics])
        for planted in ("sekret-value", "not_declared", "item_id", "upsert"):
            assert planted not in rendered

    def test_v1_map_script_binds_a_base_script_by_its_leaf_ports(self):
        """A map-script reference to a base ``script.mapping`` component is
        assembled: the JSON leaf PATHS bind script ports named by the legacy
        rule (each path's final segment), the script is selected once, and the
        map depends on it."""
        from boomi_mcp.models.integration_models import IntegrationComponentSpec

        script = IntegrationComponentSpec(
            key="shared_script",
            type="script.mapping",
            name="Shared Script",
            config={
                "component_type": "script.mapping",
                "component_name": "Shared Script",
                "language": "groovy2",
                "script_body": "name = id",
                "inputs": [{"name": "id", "data_type": "character"}],
                "outputs": [{"name": "name"}],
            },
        )
        raw_input, material = _mutated(_map_script_reference)
        prepared = _v1_prepared(raw_input, material, base_catalog={"shared_script": script})
        the_map = next(c for c in prepared.supporting_components if c.type == "transform.map")
        (mapping,) = the_map.config["script_mappings"]
        assert mapping["script_component_id"] == "$ref:shared_script"
        assert [port["input_name"] for port in mapping["inputs"]] == ["id"]
        assert [port["source_path"] for port in mapping["inputs"]] == ["Root/id"]
        assert [port["output_name"] for port in mapping["outputs"]] == ["name"]
        assert [port["target_path"] for port in mapping["outputs"]] == ["Fields/name"]
        assert "shared_script" in the_map.depends_on
        assert prepared.selected_base_keys.count("shared_script") == 1

    def test_v1_create_target_requires_its_auth_mode(self):
        raw_input, _material = _v1_split(_legacy_parameters("create"))
        raw_input["target"] = {"binding_mode": "create"}
        with pytest.raises(_ValidationError):
            ApiToDatabaseSyncInputV1.model_validate(raw_input)

    def test_v1_legacy_only_fetch_options_are_not_accepted(self):
        _raw_input, material = _v1_split(_legacy_parameters("create"))
        for option, value in (
            ("request_headers", {"Accept": "application/json"}),
            ("follow_redirects", "STRICT"),
            ("track_response", True),
        ):
            bad = copy.deepcopy(material)
            bad["source"]["request"][option] = value
            with pytest.raises(_ValidationError):
                ApiToDatabaseSyncMaterialV1.model_validate(bad)

    def test_v1_base_key_bindings_emit_no_connection_and_bind_by_key(self):
        """Both sides selected by base key: no connection is generated, the
        fetch and Send operations bind to the base keys, and both bases are
        selected exactly once."""
        from boomi_mcp.models.integration_models import IntegrationComponentSpec

        bases = {
            "shared_rest": IntegrationComponentSpec(
                key="shared_rest",
                type="connector-settings",
                name="Shared REST",
                component_id="rest-id",
                config={"reference_only": True, "connector_type": "rest", "component_id": "rest-id"},
            ),
            "shared_db": IntegrationComponentSpec(
                key="shared_db",
                type="connector-settings",
                name="Shared DB",
                component_id="db-id",
                config={"reference_only": True, "connector_type": "database", "component_id": "db-id"},
            ),
        }
        raw_input, material = _v1_split(_legacy_parameters("create"))
        raw_input["source"] = {"binding_mode": "reuse", "connection_key": "shared_rest"}
        raw_input["target"] = {"binding_mode": "reuse", "connection_key": "shared_db"}
        material["source"].pop("connection")
        material["target"].pop("connection")
        prepared = _v1_prepared(raw_input, material, bases)
        by_key = {c.key: c for c in prepared.supporting_components}
        assert "source_rest_source_connection" not in by_key
        assert "target_db_connection" not in by_key
        assert by_key["source_rest_source_operation"].config["connection_ref_key"] == "shared_rest"
        assert by_key["target_db_write_operation"].config["connection_ref_key"] == "shared_db"
        assert prepared.selected_base_keys == ("shared_rest", "shared_db")
        assert prepared.rest_request_targets[0][0] == "shared_rest"
        unit = emit_api_to_database_sync_v1(ApiToDatabaseSyncInputV1.model_validate(raw_input))[0].unit
        assert {"shared_rest", "shared_db"} <= set(unit.envelope.depends_on)

    def test_v1_canary_seed_assembles_and_breaches_are_refused(self):
        raw_input, material = canary_api_to_database_sync_v1()
        assert _v1_prepared(raw_input, material).supporting_components
        breached = copy.deepcopy(material)
        breached["target"]["connection"]["credential_ref"] = "plain-password"
        error = _refused(raw_input, breached)
        assert error.diagnostics[0].path == "/component_material/target/connection/credential_ref"


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
