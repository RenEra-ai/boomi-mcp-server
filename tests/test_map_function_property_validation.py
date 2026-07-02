"""Plan-level validation for native property map functions.

Covers the defined_process_property_* component-reference rule enforced by
``validate_transform_map`` (MAP_FUNCTION_COMPONENT_REF_REQUIRED) and the
``_mappings_from_map_config`` tolerance for no-output setters that omit
target_path.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List, Optional

from boomi_mcp.categories.components.builders.transform_map_validation import (
    validate_transform_map,
)
from boomi_mcp.categories.transformation_review import _mappings_from_map_config


def _comp(comp_type: str, *, name: str = "C", config: Optional[Dict[str, Any]] = None):
    return SimpleNamespace(type=comp_type, name=name, config=config or {})


def _json_profile_config(name: str) -> Dict[str, Any]:
    return {
        "component_type": "profile.json",
        "profile_type": "json.generated",
        "component_name": name,
        "root": {
            "name": "Root",
            "kind": "object",
            "children": [
                {
                    "name": "list",
                    "kind": "array",
                    "children": [
                        {"name": "status", "kind": "simple", "data_type": "character"},
                    ],
                },
            ],
        },
    }


_DEFINED_PARAMS = {
    "process_property_component_id": "$ref:pp",
    "process_property_component_name": "New Process Property",
    "process_property_key": "0e89ebf1-cd46-46df-904e-94c7e7ade31e",
    "process_property_name": "Example Property",
}


def _function_map_config(params: Dict[str, Any], **overrides: Any) -> Dict[str, Any]:
    cfg: Dict[str, Any] = {
        "component_type": "transform.map",
        "map_type": "function",
        "component_name": "Property Map",
        "source_profile_id": "aaaaaaaa-1111-1111-1111-111111111111",
        "source_profile_type": "profile.json",
        "target_profile_id": "bbbbbbbb-2222-2222-2222-222222222222",
        "target_profile_type": "profile.json",
        "function_mappings": [
            {
                "function_type": "defined_process_property_get",
                "inputs": [],
                "target_path": "Root/list[]/status",
                "parameters": params,
            },
        ],
    }
    cfg.update(overrides)
    return cfg


def test_defined_property_missing_depends_on_rejected():
    cfg = _function_map_config(dict(_DEFINED_PARAMS))
    err = validate_transform_map(cfg, ["src", "tgt"], {"pp": _comp("processproperty")})
    assert err is not None
    assert err.error_code == "MAP_FUNCTION_COMPONENT_REF_REQUIRED"
    assert err.field == "depends_on"


def test_defined_property_literal_id_rejected():
    params = dict(_DEFINED_PARAMS)
    params["process_property_component_id"] = "cccccccc-3333-3333-3333-333333333333"
    cfg = _function_map_config(params)
    err = validate_transform_map(cfg, ["pp"], {"pp": _comp("processproperty")})
    assert err is not None
    assert err.error_code == "MAP_FUNCTION_COMPONENT_REF_REQUIRED"
    assert err.field.endswith("process_property_component_id")


def test_defined_property_ref_to_non_processproperty_rejected():
    cfg = _function_map_config(dict(_DEFINED_PARAMS))
    # $ref:pp resolves to a profile component, not a processproperty.
    err = validate_transform_map(
        cfg, ["pp"], {"pp": _comp("profile.json", config=_json_profile_config("P"))}
    )
    assert err is not None
    assert err.error_code == "MAP_FUNCTION_COMPONENT_REF_REQUIRED"
    assert err.details.get("target_component_type") == "profile.json"


def test_defined_property_clean_spec_passes_component_ref_check():
    cfg = _function_map_config(
        dict(_DEFINED_PARAMS),
        source_profile_id="$ref:src",
        target_profile_id="$ref:tgt",
    )
    components = {
        "src": _comp("profile.json", config=_json_profile_config("Src")),
        "tgt": _comp("profile.json", config=_json_profile_config("Tgt")),
        "pp": _comp("processproperty"),
    }
    err = validate_transform_map(cfg, ["src", "tgt", "pp"], components)
    assert err is None


def test_mappings_from_map_config_tolerates_missing_target_path():
    # A no-output property setter contributes a record with empty target_paths.
    records = _mappings_from_map_config(
        {
            "function_mappings": [
                {
                    "function_type": "document_property_set",
                    "inputs": ["rows/row[]/name"],
                    "parameters": {"document_property_name": "DDP_FOO"},
                },
            ],
        }
    )
    assert len(records) == 1
    assert records[0]["target_paths"] == []
    assert records[0]["source_paths"] == ["rows/row[]/name"]


def test_defined_property_green_path_with_builder_backed_component():
    """Issue #131 M11.7: the MAP_FUNCTION_COMPONENT_REF_REQUIRED green path is
    now reachable end-to-end — the referenced processproperty is a REAL
    builder-backed config (previously unsatisfiable without hand-authored raw
    XML), and the explicit property key declared in the component is the same
    UUID the map function passes as process_property_key."""
    from boomi_mcp.categories.components.builders.process_property_builder import (
        ProcessPropertyBuilder,
    )

    pp_config = {
        "component_type": "processproperty",
        "component_name": "New Process Property",
        "properties": [
            {
                "key": _DEFINED_PARAMS["process_property_key"],
                "name": _DEFINED_PARAMS["process_property_name"],
                "type": "string",
            }
        ],
    }
    # (a) The component config itself validates through the new builder.
    assert ProcessPropertyBuilder.validate_config(pp_config) is None
    # (b) The referencing function map passes the component-ref check with
    #     the builder-backed component in scope.
    cfg = _function_map_config(
        dict(_DEFINED_PARAMS),
        source_profile_id="$ref:src",
        target_profile_id="$ref:tgt",
    )
    components = {
        "src": _comp("profile.json", config=_json_profile_config("Src")),
        "tgt": _comp("profile.json", config=_json_profile_config("Tgt")),
        "pp": _comp("processproperty", config=pp_config),
    }
    err = validate_transform_map(cfg, ["src", "tgt", "pp"], components)
    assert err is None
    # (c) The key coupling holds: the key the map references is declared in
    #     the component config verbatim.
    declared_keys = {p["key"] for p in pp_config["properties"]}
    assert _DEFINED_PARAMS["process_property_key"] in declared_keys
