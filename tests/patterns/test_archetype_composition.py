"""Issue #14 (M8): compose_archetypes — archetype composition end-to-end.

Proves the composition contract and the full local chain through the public
action entry points, without live Boomi:

    compose_archetypes_action -> build_integration_action(plan)
                              -> ProcessFlowBuilder.build (Branch XML shape)

Acceptance criteria covered:
* Composition emits ONE coherent IntegrationSpecV1 (component_count matches,
  one main process on the flow_sequence map_ref + Branch surface).
* Invalid contract links fail with COMPOSITION_* codes BEFORE any spec exists
  (and therefore before any Boomi mutation).
* Composed output still plans clean through the normal build_integration path
  and the golden example spec round-trips byte-for-byte.

Payloads use sentinel placeholders only (``<<...>>``), never canned payloads,
raw XML, credentials, or live account IDs.
"""

from __future__ import annotations

import copy
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from boomi_mcp.categories.components.builders import ProcessFlowBuilder
from boomi_mcp.categories.integration_authoring import (
    compose_archetypes_action,
    list_integration_archetypes_action,
)
from boomi_mcp.categories.integration_builder import _build_plan
from boomi_mcp.errors import (
    COMPOSITION_COMPONENT_KEY_COLLISION,
    COMPOSITION_CONTRACT_MISMATCH,
    COMPOSITION_UNSUPPORTED_TOPOLOGY,
    ERROR_TAXONOMY,
)

_PAGINATE_TARGET = "boomi_mcp.categories.integration_builder.paginate_metadata"

_EXAMPLE_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "examples"
    / "m8"
    / "composed_db_to_api_fanout.integration.json"
)


def _payload_profile(*leaf_names: str) -> Dict[str, Any]:
    return {
        "format": "json",
        "root": {
            "name": "Root",
            "kind": "object",
            "children": [
                {"name": name, "kind": "simple", "data_type": "character"}
                for name in leaf_names
            ],
        },
    }


def _parts() -> List[Dict[str, Any]]:
    return [
        {
            "key": "db",
            "kind": "db_source",
            "parameters": {
                "binding": {
                    "mode": "create",
                    "settings": {
                        "driver": "microsoft_jdbc",
                        "auth_mode": "username_password",
                        "host": "db.example.invalid",
                        "database": "AppDB",
                        "username": "<<db user>>",
                        "credential_ref": "<<opaque credential reference>>",
                    },
                },
                "read_operation": {
                    "sql": "<<user-authored DB read statement>>",
                    "result_schema": {
                        "fields": [
                            {"name": "source_a", "data_type": "character"},
                        ],
                    },
                },
            },
        },
        {
            "key": "shape",
            "kind": "transform",
            "parameters": {
                "operations": [
                    {
                        "operation_type": "direct",
                        "source_field": "source_a",
                        "target_path": "Root/target_a",
                    },
                ],
            },
        },
        {
            "key": "orders",
            "kind": "rest_target",
            "label": "Orders",
            "parameters": {
                "binding": {
                    "mode": "create",
                    "settings": {
                        "base_url": "https://orders.example.invalid",
                        "auth_mode": "none",
                    },
                },
                "send_request": {"method": "POST", "path": "/v1/orders"},
                "payload_profile": _payload_profile("target_a"),
            },
        },
        {
            "key": "billing",
            "kind": "rest_target",
            "label": "Billing",
            "parameters": {
                "binding": {
                    "mode": "create",
                    "settings": {
                        "base_url": "https://billing.example.invalid",
                        "auth_mode": "none",
                    },
                },
                "send_request": {"method": "POST", "path": "/v1/billing"},
                "payload_profile": _payload_profile("target_a"),
            },
        },
    ]


def _options() -> Dict[str, Any]:
    return {
        "naming": {
            "integration_name": "demo-composed-fanout",
            "component_prefix": "DEMO",
        },
    }


def _compose(parts=None, options=None) -> Dict[str, Any]:
    return compose_archetypes_action(
        parts=parts if parts is not None else _parts(),
        options=options if options is not None else _options(),
    )


# ---------------------------------------------------------------------------
# Success path — one coherent IntegrationSpecV1
# ---------------------------------------------------------------------------


def test_compose_success_emits_one_coherent_spec():
    result = _compose()
    assert result["_success"] is True
    assert result["raw_xml_exposed"] is False
    assert result["boomi_mutation"] is False
    assert "plan" in result["next_steps"]
    assert result["composition"] == {
        "topology": "db_source->transform->rest_fanout",
        "handoff": "document_stream",
        "fanout_targets": 2,
    }

    spec = result["integration_spec"]
    keys = [c["key"] for c in spec["components"]]
    assert keys == [
        "source_db_connection",
        "source_db_read_profile",
        "source_db_get_operation",
        "transform_target_profile",
        "transform_transform_map",
        "target_rest_connection",
        "target_rest_operation",
        "target_billing_rest_connection",
        "target_billing_rest_operation",
        "main_process",
    ]
    assert spec["validation_rules"]["component_count"] == len(spec["components"])
    composition = spec["validation_rules"]["composition"]
    assert composition["first_target_part"] == "orders"
    assert [p["kind"] for p in composition["parts"]] == [
        "db_source",
        "transform",
        "rest_target",
        "rest_target",
    ]


def test_compose_success_process_uses_flow_sequence_branch():
    spec = _compose()["integration_spec"]
    process = spec["components"][-1]
    assert process["key"] == "main_process"
    config = process["config"]
    assert config["transform"] == {"mode": "passthrough"}
    assert "reliability" not in config
    seq = config["flow_sequence"]
    assert [step["kind"] for step in seq] == ["map_ref", "branch"]
    assert seq[0]["map_ref"] == "$ref:transform_transform_map"
    legs = seq[1]["legs"]
    assert [leg["target"]["operation_id"] for leg in legs] == [
        "$ref:target_rest_operation",
        "$ref:target_billing_rest_operation",
    ]
    for extra_key in (
        "target_billing_rest_connection",
        "target_billing_rest_operation",
    ):
        assert extra_key in process["depends_on"]


def test_compose_fanout_components_bind_shared_profile_and_labels():
    spec = _compose()["integration_spec"]
    by_key = {c["key"]: c for c in spec["components"]}
    op = by_key["target_billing_rest_operation"]
    assert op["config"]["request_profile_id"] == "$ref:transform_target_profile"
    assert op["name"] == "DEMO Billing REST Send"
    conn = by_key["target_billing_rest_connection"]
    assert conn["name"] == "DEMO Billing REST Connection"
    # The first target's label drives the base pair's display names too.
    assert by_key["target_rest_connection"]["name"] == "DEMO Orders REST Connection"
    assert by_key["target_rest_operation"]["name"] == "DEMO Orders REST Send"


def test_first_target_without_label_gets_humanized_key_names():
    parts = _parts()
    del parts[2]["label"]  # first target: derived label falls back to the key
    spec = _compose(parts=parts)["integration_spec"]
    by_key = {c["key"]: c for c in spec["components"]}
    assert by_key["target_rest_connection"]["name"] == "DEMO Orders REST Connection"
    assert by_key["target_rest_operation"]["name"] == "DEMO Orders REST Send"


def test_explicit_component_name_overrides_beat_derived_labels():
    options = _options()
    options["naming"]["component_names"] = {
        "rest_connection": "Custom First Connection",
    }
    spec = _compose(options=options)["integration_spec"]
    by_key = {c["key"]: c for c in spec["components"]}
    assert by_key["target_rest_connection"]["name"] == "Custom First Connection"
    # The non-overridden role still gets the derived-label default.
    assert by_key["target_rest_operation"]["name"] == "DEMO Orders REST Send"


def test_invalid_component_name_override_still_fails_validation():
    """A malformed role-keyed override (non-string) must reach NamingConfig
    validation and fail — the derived-label default must never overwrite a
    present caller value (compose behaves like the standalone archetype)."""
    options = _options()
    options["naming"]["component_names"] = {"rest_connection": 123}
    result = _compose(options=options)
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"


@pytest.mark.parametrize("malformed", ["bad", 0, [], ["a"]])
def test_non_dict_component_names_still_fails_validation(malformed):
    """A present non-dict component_names must flow verbatim to NamingConfig
    and fail PARAM_VALIDATION_FAILED — never be silently replaced by derived
    defaults (0, []) or crash the pre-validation copy ('bad')."""
    options = _options()
    options["naming"]["component_names"] = malformed
    result = _compose(options=options)
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"


def test_emitted_key_component_name_overrides_beat_derived_labels():
    """The prefixed emitted key ('target_rest_connection') is a documented
    override fallback (_component_names) — the derived-label default must not
    shadow it by populating the higher-precedence role key."""
    options = _options()
    options["naming"]["component_names"] = {
        "target_rest_connection": "Emitted-Key Connection Name",
        "target_rest_operation": "Emitted-Key Operation Name",
    }
    spec = _compose(options=options)["integration_spec"]
    by_key = {c["key"]: c for c in spec["components"]}
    assert by_key["target_rest_connection"]["name"] == "Emitted-Key Connection Name"
    assert by_key["target_rest_operation"]["name"] == "Emitted-Key Operation Name"


def test_fanout_create_default_headers_recorded_as_deferred_intent():
    parts = _parts()
    parts[3]["parameters"]["binding"]["settings"]["default_headers"] = {
        "<<header name>>": "<<header value>>",
    }
    result = _compose(parts=parts)
    assert result["_success"] is True
    legs = result["integration_spec"]["validation_rules"]["composition"]["fanout_legs"]
    deferred = legs[0]["deferred"]["default_headers"]
    assert deferred["count"] == 1
    # Counts only — the caller-authored header keys/values are never echoed.
    assert "<<header name>>" not in json.dumps(deferred)


def test_compose_registry_and_archetype_list_unchanged():
    listed = list_integration_archetypes_action()
    assert listed["_success"] is True
    names = {a["name"] for a in listed["archetypes"]}
    assert "compose_archetypes" not in names
    assert "database_to_api_sync" in names


# ---------------------------------------------------------------------------
# Composed output flows through normal orchestration (plan + XML shape)
# ---------------------------------------------------------------------------


def test_composed_spec_plans_clean_and_emits_branch_xml():
    spec = _compose()["integration_spec"]
    with patch(_PAGINATE_TARGET, return_value=[]):
        plan = _build_plan(
            MagicMock(),
            {"integration_spec": spec, "conflict_policy": "reuse"},
        )
    assert plan.get("_success", True) is not False
    for step in plan["steps"]:
        assert "validation_error" not in step, (step["key"], step.get("validation_error"))
        assert step["planned_action"] in ("create", "reuse")

    process = spec["components"][-1]
    xml = ProcessFlowBuilder.build(process["config"], name=process["name"])
    shapes = [s.get("shapetype") for s in ET.fromstring(xml).iter("shape")]
    assert shapes.count("branch") == 1
    # DB Get source + one REST send per leg.
    assert shapes.count("connectoraction") == 3
    # Each Branch leg terminates in its own Stop.
    assert shapes.count("stop") == 2
    assert shapes.count("map") == 1


def test_golden_example_round_trips():
    payload = json.loads(_EXAMPLE_PATH.read_text(encoding="utf-8"))
    assert payload["example_not_template"] is True
    assert payload["is_template"] is False
    assert payload["template_status"] == "example_only_not_reusable_template"
    request = payload["compose_request"]
    result = compose_archetypes_action(
        parts=request["parts"], options=request["options"]
    )
    assert result["_success"] is True
    assert result["integration_spec"] == payload["integration_spec"]


def test_golden_example_plans_clean():
    payload = json.loads(_EXAMPLE_PATH.read_text(encoding="utf-8"))
    with patch(_PAGINATE_TARGET, return_value=[]):
        plan = _build_plan(
            MagicMock(),
            {
                "integration_spec": payload["integration_spec"],
                "conflict_policy": "reuse",
            },
        )
    assert plan.get("_success", True) is not False
    for step in plan["steps"]:
        assert "validation_error" not in step, (step["key"], step.get("validation_error"))


# ---------------------------------------------------------------------------
# Contract mismatches fail with COMPOSITION_CONTRACT_MISMATCH (no spec emitted)
# ---------------------------------------------------------------------------


def test_transform_referencing_undeclared_source_field_is_contract_mismatch():
    parts = _parts()
    parts[1]["parameters"]["operations"][0]["source_field"] = "not_declared"
    result = _compose(parts=parts)
    assert result["_success"] is False
    assert result["error_code"] == COMPOSITION_CONTRACT_MISMATCH
    assert "integration_spec" not in result


def test_divergent_fanout_payload_profile_is_contract_mismatch():
    parts = _parts()
    parts[3]["parameters"]["payload_profile"] = _payload_profile("different_leaf")
    result = _compose(parts=parts)
    assert result["_success"] is False
    assert result["error_code"] == COMPOSITION_CONTRACT_MISMATCH
    assert "integration_spec" not in result


def test_unmapped_required_leaf_is_contract_mismatch():
    parts = _parts()
    profile = _payload_profile("target_a")
    profile["root"]["children"].append(
        {
            "name": "target_required",
            "kind": "simple",
            "data_type": "character",
            "required": True,
        }
    )
    parts[2]["parameters"]["payload_profile"] = copy.deepcopy(profile)
    parts[3]["parameters"]["payload_profile"] = copy.deepcopy(profile)
    result = _compose(parts=parts)
    assert result["_success"] is False
    assert result["error_code"] == COMPOSITION_CONTRACT_MISMATCH


# ---------------------------------------------------------------------------
# Unsupported topologies
# ---------------------------------------------------------------------------


def test_single_rest_target_is_unsupported_topology():
    parts = _parts()[:3]
    result = _compose(parts=parts)
    assert result["_success"] is False
    assert result["error_code"] == COMPOSITION_UNSUPPORTED_TOPOLOGY
    assert "build_from_archetype" in result["suggestion"]


def test_more_than_25_rest_targets_is_unsupported_topology():
    parts = _parts()[:2]
    template = _parts()[2]
    for i in range(26):
        part = copy.deepcopy(template)
        part["key"] = f"t{i}"
        part["label"] = f"Leg {chr(ord('A') + i)}"
        parts.append(part)
    result = _compose(parts=parts)
    assert result["_success"] is False
    assert result["error_code"] == COMPOSITION_UNSUPPORTED_TOPOLOGY


def test_missing_transform_part_is_unsupported_topology():
    parts = [p for p in _parts() if p["kind"] != "transform"]
    result = _compose(parts=parts)
    assert result["_success"] is False
    assert result["error_code"] == COMPOSITION_UNSUPPORTED_TOPOLOGY


def test_document_cache_handoff_is_gated_unsupported_topology():
    options = _options()
    options["links"] = [
        {"from_part": "db", "to_part": "shape"},
        {"from_part": "shape", "to_part": "orders"},
        {
            "from_part": "shape",
            "to_part": "billing",
            "handoff": {"mode": "document_cache"},
        },
    ]
    result = _compose(options=options)
    assert result["_success"] is False
    assert result["error_code"] == COMPOSITION_UNSUPPORTED_TOPOLOGY
    assert "cache_property_authoring" in result["suggestion"]


def test_explicit_star_links_accepted_and_wrong_links_rejected():
    options = _options()
    options["links"] = [
        {"from_part": "db", "to_part": "shape"},
        {"from_part": "shape", "to_part": "orders"},
        {"from_part": "shape", "to_part": "billing"},
    ]
    assert _compose(options=options)["_success"] is True

    options["links"] = [
        {"from_part": "db", "to_part": "orders"},
        {"from_part": "shape", "to_part": "orders"},
        {"from_part": "shape", "to_part": "billing"},
    ]
    result = _compose(options=options)
    assert result["_success"] is False
    assert result["error_code"] == COMPOSITION_UNSUPPORTED_TOPOLOGY


def test_path_replacements_on_any_target_is_unsupported_topology():
    parts = _parts()
    parts[2]["parameters"]["send_request"] = {
        "method": "POST",
        "path": "/v1/orders/{orderId}",
        "path_replacements": [{"name": "orderId", "target_path": "Root/target_a"}],
    }
    result = _compose(parts=parts)
    assert result["_success"] is False
    assert result["error_code"] == COMPOSITION_UNSUPPORTED_TOPOLOGY


def test_watermark_query_parameter_on_fanout_target_is_unsupported_topology():
    parts = _parts()
    parts[3]["parameters"]["send_request"] = {
        "method": "POST",
        "path": "/v1/billing",
        "query_parameters": [{"name": "since", "value_source": "watermark"}],
    }
    options = _options()
    options["execution"] = {
        "trigger": {"mode": "manual"},
        "watermark": {"field": "source_a", "kind": "timestamp", "persistence": "dpp"},
    }
    result = _compose(parts=parts, options=options)
    assert result["_success"] is False
    assert result["error_code"] == COMPOSITION_UNSUPPORTED_TOPOLOGY


# ---------------------------------------------------------------------------
# Key / name collisions
# ---------------------------------------------------------------------------


def test_duplicate_part_keys_collide():
    parts = _parts()
    parts[3]["key"] = parts[2]["key"]
    result = _compose(parts=parts)
    assert result["_success"] is False
    assert result["error_code"] == COMPOSITION_COMPONENT_KEY_COLLISION


def test_colliding_slug_prefixes_collide():
    parts = _parts()
    extra = copy.deepcopy(parts[3])
    extra["key"] = "billing "  # strips, then slugs identically to 'billing'
    extra["label"] = "Billing Two"
    parts.append(extra)
    result = _compose(parts=parts)
    assert result["_success"] is False
    assert result["error_code"] == COMPOSITION_COMPONENT_KEY_COLLISION


def test_duplicate_labels_collide():
    parts = _parts()
    parts[3]["label"] = parts[2]["label"]
    result = _compose(parts=parts)
    assert result["_success"] is False
    assert result["error_code"] == COMPOSITION_COMPONENT_KEY_COLLISION


# ---------------------------------------------------------------------------
# Parameter validation + envelope hygiene
# ---------------------------------------------------------------------------


def test_malformed_part_parameters_return_field_errors():
    parts = _parts()
    del parts[0]["parameters"]["read_operation"]
    result = _compose(parts=parts)
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"
    assert result["field_errors"]


def test_unknown_part_kind_rejected():
    parts = _parts()
    parts[0]["kind"] = "queue_source"
    result = _compose(parts=parts)
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"


def test_reliability_options_are_rejected_not_dropped():
    options = _options()
    options["reliability"] = {"retry": {"max_attempts": 3}}
    result = _compose(options=options)
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"


def test_parts_accepts_json_string_and_rejects_non_array():
    result = compose_archetypes_action(
        parts=json.dumps(_parts()), options=json.dumps(_options())
    )
    assert result["_success"] is True

    result = compose_archetypes_action(parts='{"not": "an array"}')
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED"


def test_secured_create_auth_on_fanout_target_rejected():
    parts = _parts()
    parts[3]["parameters"]["binding"]["settings"]["auth_mode"] = "basic"
    result = _compose(parts=parts)
    assert result["_success"] is False
    # RestCreateSettings gates the auth vocabulary at the contract boundary.
    assert result["error_code"] in (
        "PARAM_VALIDATION_FAILED",
        "UNSUPPORTED_REST_AUTH_MODE",
    )


def test_action_never_touches_boomi_or_credentials():
    with (
        patch("boomi_mcp.categories.integration_builder.paginate_metadata") as m_paginate,
    ):
        result = _compose()
    assert result["_success"] is True
    m_paginate.assert_not_called()


def test_composition_error_codes_are_in_taxonomy():
    for code in (
        COMPOSITION_CONTRACT_MISMATCH,
        COMPOSITION_UNSUPPORTED_TOPOLOGY,
        COMPOSITION_COMPONENT_KEY_COLLISION,
    ):
        assert code in ERROR_TAXONOMY
        assert ERROR_TAXONOMY[code].category == "authoring"
        assert ERROR_TAXONOMY[code].retryable is False
        assert ERROR_TAXONOMY[code].owner == "#14"
