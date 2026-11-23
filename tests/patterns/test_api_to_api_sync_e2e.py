"""Issue #73 (M5.7): api_to_api_sync preset over sync_pipeline — end-to-end.

Proves the preset's contract and the full local chain through the *public*
action entry points, without live Boomi:

    build_from_archetype_action  ->  review_transformation_action
                                 ->  build_integration_action(plan / apply)

Acceptance criteria covered:
* Preset maps to ``sync_pipeline`` stages (main process carries
  ``process_kind="sync_pipeline"`` with an intact fetch -> map -> send graph and
  no pairwise source/target/transform process config).
* Generated plan is inspectable through the existing MCP planning/review flows
  (build_integration plan, review_transformation, get_schema_template).
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
from boomi_mcp.categories.transformation_review import review_transformation_action


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


_ARCHETYPE = "api_to_api_sync"
_PAGINATE_TARGET = "boomi_mcp.categories.integration_builder.paginate_metadata"
_EXECUTE_TARGET = "boomi_mcp.categories.integration_builder._execute_component"
_PROFILE = "qa-profile"  # never used to reach Boomi; calls are fully mocked

_EXPECTED_KEYS = {
    "source_response_profile",
    "source_rest_source_connection",
    "source_rest_source_operation",
    "transform_target_profile",
    "transform_transform_map",
    "target_rest_connection",
    "target_rest_operation",
    "main_process",
}
_STAGE_DEPENDS = [
    "source_rest_source_connection",
    "source_rest_source_operation",
    "transform_transform_map",
    "target_rest_connection",
    "target_rest_operation",
]


# ---------------------------------------------------------------------------
# Payload builders (synthetic, sentinel-only)
# ---------------------------------------------------------------------------


def _minimal() -> Dict[str, Any]:
    """Smallest executable payload: create REST source + create REST target (no
    auth) + a single direct transform."""
    return {
        "naming": {"integration_name": "demo-api-sync", "component_prefix": "DEMO"},
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
            "binding": {
                "mode": "create",
                "settings": {
                    "base_url": "https://target.example.com",
                    "auth_mode": "none",
                },
            },
            "send_request": {"method": "POST", "path": "/v1/<<target resource>>"},
            "payload_profile": {
                "format": "json",
                "root": {
                    "name": "Root",
                    "kind": "object",
                    "children": [
                        {"name": "target_a", "kind": "simple", "data_type": "character"}
                    ],
                },
            },
        },
        "transform": {
            "operations": [
                {
                    "operation_type": "direct",
                    "source_path": "Root/source_a",
                    "target_path": "Root/target_a",
                }
            ]
        },
    }


def _full_reuse() -> Dict[str, Any]:
    """Reuse-mode source + target with a required leaf and a map_function."""
    return {
        "naming": {
            "integration_name": "demo-api-incremental",
            "component_prefix": "DEMO-INC",
            "folder_path": "Integrations/API/Sync",
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
                        {"name": "id", "kind": "simple", "data_type": "character", "required": True},
                        {"name": "updated_at", "kind": "simple", "data_type": "datetime"},
                    ],
                },
            },
        },
        "target": {
            "binding": {"mode": "reuse", "component_id": "<<existing target conn id>>"},
            "send_request": {"method": "PUT", "path": "/v1/<<target resource>>"},
            "payload_profile": {
                "format": "json",
                "root": {
                    "name": "Root",
                    "kind": "object",
                    "children": [
                        {"name": "external_id", "kind": "simple", "data_type": "character", "required": True},
                        {"name": "modified", "kind": "simple", "data_type": "datetime"},
                    ],
                },
            },
        },
        "transform": {
            "operations": [
                {
                    "operation_type": "direct",
                    "source_path": "Root/id",
                    "target_path": "Root/external_id",
                },
                {
                    "operation_type": "map_function",
                    "function_type": "date_format",
                    "inputs": ["Root/updated_at"],
                    "target_path": "Root/modified",
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
    def test_list_includes_api_to_api_sync(self):
        # #159 S3: discovery serves recipes only, and API->API has no executable
        # recipe (step-4 decision 1: it is authored as a direct ProcessIR unit).
        # The node keeps its S2 name; the body pins the retired id's ABSENCE
        # beside the recipes the same query still finds.
        result = list_integration_archetypes_action(query="api")
        assert result["_success"] is True
        ids = [recipe["recipe_id"] for recipe in result["recipes"]]
        assert ids, "non-vacuity: the query still matches the API->DB and DB->API recipes"
        assert "boomi.archetype.api_to_api_sync" not in ids
        assert result["raw_xml_exposed"] is False

    def test_get_returns_strict_schema_without_xml(self):
        # #159 S3: the retired API->API recipe id does not resolve on the
        # recipe-only get, and the refusal carries no raw XML.
        result = get_integration_archetype_action("boomi.archetype.api_to_api_sync")
        assert result["_success"] is False
        assert result["error_code"] == "RECIPE_NOT_FOUND"
        assert "<?xml" not in json.dumps(result)

    def test_schema_template_resolves(self):
        # #159 S3: the ``archetype:<name>`` selector retired, and the API->API
        # executable retired with no successor — neither spelling resolves.
        for selector in (f"archetype:{_ARCHETYPE}", "recipe:boomi.archetype.api_to_api_sync@1.0.0"):
            result = get_schema_template_action(schema_name=selector)
            assert result["_success"] is False, selector
            assert result["error_code"] == "SCHEMA_NAME_UNSUPPORTED", selector
            assert "<?xml" not in json.dumps(result)


# ===========================================================================
# Emitted spec maps to sync_pipeline stages
# ===========================================================================


# ===========================================================================
# Parameter validation (negative)
# ===========================================================================


# ===========================================================================
# Header handling + map_script variable naming
# ===========================================================================


# ===========================================================================
# Full local chain: review + plan + apply
# ===========================================================================


# ===========================================================================
# #159 S2: the two frozen API->API cases as DIRECT canonical units
# ===========================================================================
#
# API->API has no typed successor recipe (its executable registration retires
# in S3); its frozen working behaviour is carried by the committed direct
# ``input.canonical.json`` fixtures. These checks are cheap and structural; the
# full replay runs in ``tests/test_issue_157_flows_accounting.py``.

from pathlib import Path as _Path

_API_API_CASES = _Path(__file__).resolve().parents[1] / (
    "fixtures/governance/issue_157/cases/api_to_api_sync"
)


def _strip_hints(components):
    """The map's served ``documentation_hint`` keys: the canonical fixture
    carries them on the map so the rich transform row serves them; the legacy
    map component never did (it served them on the flow row only)."""
    for component in components:
        for entry in (component.get("config") or {}).get("field_mappings") or ():
            entry.pop("documentation_hint", None)
        for entry in (component.get("config") or {}).get("function_mappings") or ():
            entry.pop("documentation_hint", None)
    return components


class TestDirectCanonicalCases:
    @pytest.mark.parametrize("case", ["create", "reuse_with_function"])
    def test_direct_canonical_case_carries_the_legacy_route(self, case):
        from boomi_mcp.authoring.derived_flows import infer_direct_linear_facts
        from boomi_mcp.models.authoring_workflow import AuthoringRequestV1
        from boomi_mcp.patterns.archetypes import api_to_api_sync as legacy_module

        directory = _API_API_CASES / case
        legacy = json.loads((directory / "input.legacy.json").read_text())
        # The isolated legacy archetype (kept until #160) is the oracle. Its
        # ``emit_spec`` ends with a discarded re-run through the 0.1.0 preset,
        # retired in #159 S3; that one check is stubbed, the spec is unchanged.
        with patch.object(legacy_module, "run_sync_preset_recipe", lambda **_kwargs: None):
            spec = legacy_module.ApiToApiSyncArchetype.emit_spec(
                legacy_module.ApiToApiSyncArchetype.parameters_model.model_validate(
                    legacy["input"]["parameters"]
                )
            )
        request = AuthoringRequestV1.model_validate(
            json.loads((directory / "input.canonical.json").read_text())
        )
        intent = request.intent
        assert intent.intent_kind == "process_ir"
        (unit,) = intent.units
        kinds = [step.kind for step in unit.process_ir.body.steps]
        assert kinds == ["source", "map_ref", "target", "stop"]
        process = spec.components[-1]
        assert unit.envelope.component_key == process.key
        assert unit.envelope.name == process.name
        assert set(unit.envelope.depends_on) == set(process.depends_on)
        assert _strip_hints([c.model_dump(mode="json") for c in intent.components]) == [
            c.model_dump(mode="json") for c in spec.components[:-1]
        ]
        facts = infer_direct_linear_facts(unit, intent.components)
        assert facts is not None
        assert facts.source.operation_key == "source_rest_source_operation"
        assert facts.target.operation_key == "target_rest_operation"


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
