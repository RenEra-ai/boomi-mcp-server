"""Handler + golden tests for the read-only transformation review surface (issue #46).

Exercises review_transformation_action directly with hand-built specs (no Boomi,
no SDK). Covers both input sources — the database_to_api_sync contract flow and
executable generated-profile + transform.map components — plus the safety
contract, the validation findings, mapping diff, synthetic skeletons, and the
expected-vs-actual comparison.
"""

import copy
import sys
from pathlib import Path

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.boomi_mcp.categories.transformation_review import (  # noqa: E402
    EXTRA_FIELD,
    MISSING_FIELD,
    TRANSFORM_REVIEW_DUPLICATE_TARGET,
    TRANSFORM_REVIEW_FIELD_NOT_FOUND,
    TRANSFORM_REVIEW_FIELD_NOT_MAPPABLE,
    TRANSFORM_REVIEW_INVALID_INPUT,
    TRANSFORM_REVIEW_NO_TRANSFORM_FOUND,
    TRANSFORM_REVIEW_PROFILE_INDEX_UNAVAILABLE,
    TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED,
    TRANSFORM_REVIEW_SCRIPT_REF_MISSING,
    TRANSFORM_REVIEW_UNSUPPORTED_ROUTE,
    TRANSFORM_REVIEW_XML_UNSUPPORTED,
    TYPE_MISMATCH,
    VALUE_MISMATCH,
    review_transformation_action,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _source_index():
    return {
        "customer_id": {
            "path": "customer_id", "name": "customer_id", "data_type": "number",
            "mappable": True, "profile_component_type": "profile.db",
            "source": "db_read_fields",
        },
        "name": {
            "path": "name", "name": "name", "data_type": "character",
            "mappable": True, "profile_component_type": "profile.db",
            "source": "db_read_fields",
        },
    }


def _target_index():
    return {
        "Root": {
            "path": "Root", "name": "Root", "kind": "object", "data_type": None,
            "required": True, "mappable": False,
            "profile_component_type": "profile.json", "source": "json_schema",
        },
        "Root/cust_id": {
            "path": "Root/cust_id", "name": "cust_id", "kind": "simple",
            "data_type": "number", "required": True, "mappable": True,
            "profile_component_type": "profile.json", "source": "json_schema",
        },
        "Root/cust_name": {
            "path": "Root/cust_name", "name": "cust_name", "kind": "simple",
            "data_type": "character", "required": False, "mappable": True,
            "profile_component_type": "profile.json", "source": "json_schema",
        },
        "Root/cust_name_upper": {
            "path": "Root/cust_name_upper", "name": "cust_name_upper", "kind": "simple",
            "data_type": "character", "required": False, "mappable": True,
            "profile_component_type": "profile.json", "source": "json_schema",
        },
        "Root/note": {
            "path": "Root/note", "name": "note", "kind": "simple",
            "data_type": "character", "required": False, "mappable": True,
            "profile_component_type": "profile.json", "source": "json_schema",
        },
    }


def _contract_spec(operations=None):
    if operations is None:
        operations = [
            {"operation_type": "direct", "source_field": "customer_id", "target_path": "Root/cust_id"},
            {"operation_type": "direct", "source_field": "name", "target_path": "Root/cust_name"},
            {"operation_type": "map_function", "function_type": "uppercase",
             "inputs": ["name"], "input_count": 1, "target_path": "Root/cust_name_upper"},
            {"operation_type": "map_script", "script_slot": "enrich", "language": "groovy2",
             "inputs": ["name"], "input_count": 1, "outputs": ["Root/note"],
             "output_count": 1, "script_body_present": True,
             "script_component_ref": "$ref:scr"},
        ]
    return {
        "version": "1.0", "name": "demo", "flows": [
            {"key": "transform", "operation": "transform", "executable": False,
             "source_profile_generation": {"field_index_by_path": _source_index()},
             "target_profile_generation": {"field_index_by_path": _target_index()},
             "operations": operations}
        ],
    }


def _executable_spec():
    return {
        "version": "1.0", "name": "demo", "components": [
            {"key": "db", "type": "profile.db", "action": "create", "name": "DB Profile",
             "config": {"output_fields": [
                 {"name": "customer_id", "data_type": "number", "mandatory": True},
                 {"name": "name", "data_type": "character"}]}},
            {"key": "json", "type": "profile.json", "action": "create", "name": "JSON Profile",
             "config": {"profile_type": "json.generated", "format": "json", "root": {
                 "name": "Root", "kind": "object", "required": True, "children": [
                     {"name": "cust_id", "kind": "simple", "data_type": "number", "required": True},
                     {"name": "cust_name", "kind": "simple", "data_type": "character"}]}}},
            {"key": "map", "type": "transform.map", "action": "create",
             "name": "Customer Map", "depends_on": ["db", "json"], "config": {
                 "map_type": "direct", "source_profile_id": "$ref:db",
                 "target_profile_id": "$ref:json", "source_profile_type": "profile.db",
                 "target_profile_type": "profile.json", "field_mappings": [
                     {"source_path": "customer_id", "target_path": "Root/cust_id"},
                     {"source_path": "name", "target_path": "Root/cust_name"}]}},
        ],
    }


# ---------------------------------------------------------------------------
# Safety contract
# ---------------------------------------------------------------------------


def _assert_safety(result):
    assert result["read_only"] is True
    assert result["boomi_mutation"] is False
    assert result["raw_xml_exposed"] is False


def test_every_response_carries_safety_flags():
    for action, cfg in [
        ("list_fields", {"integration_spec": _contract_spec()}),
        ("validate_unmapped", {"integration_spec": _contract_spec()}),
        ("mapping_diff", {"integration_spec": _contract_spec()}),
        ("generate_test_payload", {"integration_spec": _contract_spec()}),
        ("compare_expected_actual", {"expected_payload": {}, "actual_payload": {}}),
        ("list_fields", {}),  # error path
    ]:
        _assert_safety(review_transformation_action(action, cfg))


# ---------------------------------------------------------------------------
# list_fields
# ---------------------------------------------------------------------------


def test_list_fields_from_contract_output():
    r = review_transformation_action("list_fields", {"integration_spec": _contract_spec()})
    assert r["_success"] is True
    assert r["source_kind"] == "contract_flow"
    assert {f["path"] for f in r["source_fields"]} == {"customer_id", "name"}
    assert "Root/cust_id" in {f["path"] for f in r["target_fields"]}
    assert r["field_count"] == len(r["source_fields"]) + len(r["target_fields"])
    assert r["required_target_count"] == 1  # only Root/cust_id is required + mappable


def test_list_fields_from_executable_components():
    r = review_transformation_action("list_fields", {"integration_spec": _executable_spec()})
    assert r["_success"] is True
    assert r["source_kind"] == "executable_components"
    assert {f["path"] for f in r["source_fields"]} == {"customer_id", "name"}
    target_paths = {f["path"] for f in r["target_fields"]}
    assert {"Root", "Root/cust_id", "Root/cust_name"} <= target_paths
    # provenance is annotated for executable-component indexes
    assert all(f["source"] == "executable_component" for f in r["target_fields"])


# ---------------------------------------------------------------------------
# validate_unmapped
# ---------------------------------------------------------------------------


def test_validate_unmapped_full_coverage_is_valid():
    r = review_transformation_action("validate_unmapped", {"integration_spec": _contract_spec()})
    assert r["_success"] is True
    assert r["valid"] is True
    assert r["issue_count"] == 0
    assert set(r["mapped_target_paths"]) == {
        "Root/cust_id", "Root/cust_name", "Root/cust_name_upper", "Root/note"
    }
    assert r["unmapped_required_target_paths"] == []


def _codes(result):
    return {i["code"] for i in result["issues"]}


def test_validate_unmapped_unknown_source_field():
    spec = _contract_spec([
        {"operation_type": "direct", "source_field": "nope", "target_path": "Root/cust_id"},
    ])
    r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
    assert r["valid"] is False
    assert TRANSFORM_REVIEW_FIELD_NOT_FOUND in _codes(r)


def test_validate_unmapped_unknown_target_path():
    spec = _contract_spec([
        {"operation_type": "direct", "source_field": "customer_id", "target_path": "Root/missing"},
    ])
    r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
    assert r["valid"] is False
    assert any(
        i["code"] == TRANSFORM_REVIEW_FIELD_NOT_FOUND and i["details"].get("side") == "target"
        for i in r["issues"]
    )


def test_validate_unmapped_non_mappable_structural_target():
    spec = _contract_spec([
        {"operation_type": "direct", "source_field": "customer_id", "target_path": "Root"},
    ])
    r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
    assert r["valid"] is False
    assert TRANSFORM_REVIEW_FIELD_NOT_MAPPABLE in _codes(r)


def test_validate_unmapped_duplicate_target_binding():
    spec = _contract_spec([
        {"operation_type": "direct", "source_field": "customer_id", "target_path": "Root/cust_id"},
        {"operation_type": "map_function", "function_type": "uppercase",
         "inputs": ["name"], "target_path": "Root/cust_id"},
    ])
    r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
    assert r["valid"] is False
    assert TRANSFORM_REVIEW_DUPLICATE_TARGET in _codes(r)


def test_validate_unmapped_required_leaf_unmapped():
    # Map nothing — Root/cust_id is required and stays unmapped.
    spec = _contract_spec([
        {"operation_type": "direct", "source_field": "name", "target_path": "Root/cust_name"},
    ])
    r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
    assert r["valid"] is False
    assert TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED in _codes(r)
    assert "Root/cust_id" in r["unmapped_required_target_paths"]


def test_validate_unmapped_unsupported_route():
    spec = _contract_spec([
        {"operation_type": "xslt", "source_field": "name", "target_path": "Root/note"},
    ])
    r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
    assert r["valid"] is False
    assert TRANSFORM_REVIEW_UNSUPPORTED_ROUTE in _codes(r)


def test_validate_unmapped_bad_function_arity_uses_registry_code():
    # 'uppercase' expects exactly one mapped input; supplying two trips the
    # map_function_registry validator, whose code is surfaced verbatim.
    spec = _contract_spec([
        {"operation_type": "map_function", "function_type": "uppercase",
         "inputs": ["name", "customer_id"], "target_path": "Root/cust_name_upper"},
    ])
    r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
    assert r["valid"] is False
    assert "MAP_FUNCTION_INPUT_COUNT_MISMATCH" in _codes(r)


def test_validate_unmapped_unknown_function_type():
    spec = _contract_spec([
        {"operation_type": "map_function", "function_type": "bogus_fn",
         "inputs": ["name"], "target_path": "Root/cust_name_upper"},
    ])
    r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
    assert r["valid"] is False
    assert TRANSFORM_REVIEW_UNSUPPORTED_ROUTE in _codes(r)


def test_validate_unmapped_does_not_leak_script_body():
    spec = _contract_spec([
        {"operation_type": "map_script", "inputs": ["name"], "outputs": ["Root/note"],
         "script_body": "SECRET GROOVY", "script_body_present": True,
         "script_component_ref": "$ref:scr"},
    ])
    r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
    import json as _json
    assert "SECRET GROOVY" not in _json.dumps(r)


def test_validate_unmapped_inline_script_body_without_ref_is_valid():
    """A contract-flow map_script with an inline script_body and NO
    script_component_ref is a valid archetype contract (the body materializes
    downstream) — review must NOT flag it. Codex r6 P1 false-rejection."""
    spec = _contract_spec([
        {"operation_type": "map_script", "inputs": ["name"], "outputs": ["Root/note"],
         "script_body": "out = in.toUpperCase()", "script_body_present": True},
        # cover the remaining required target so coverage stays clean
        {"operation_type": "direct", "source_field": "customer_id", "target_path": "Root/cust_id"},
    ])
    r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
    assert r["valid"] is True
    assert TRANSFORM_REVIEW_SCRIPT_REF_MISSING not in _codes(r)


def test_validate_unmapped_map_script_without_ref_or_body_flagged():
    """A map_script with NEITHER a ref NOR an inline body is incomplete."""
    spec = _contract_spec([
        {"operation_type": "map_script", "inputs": ["name"], "outputs": ["Root/note"]},
    ])
    r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
    assert r["valid"] is False
    assert TRANSFORM_REVIEW_SCRIPT_REF_MISSING in _codes(r)


def test_validate_unmapped_script_body_present_flag_without_real_body_flagged():
    """script_body_present=true with NO actual (non-blank) script_body and no
    ref must still be flagged — presence is derived from a runnable body, not
    the summary flag (a flag-only map_script can't materialize). Codex r6b."""
    for body in (None, "", "   "):
        op = {"operation_type": "map_script", "inputs": ["name"],
              "outputs": ["Root/note"], "script_body_present": True}
        if body is not None:
            op["script_body"] = body
        spec = _contract_spec([op])
        r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
        assert r["valid"] is False, f"body={body!r} should be flagged"
        assert TRANSFORM_REVIEW_SCRIPT_REF_MISSING in _codes(r)


# ---------------------------------------------------------------------------
# mapping_diff
# ---------------------------------------------------------------------------


def test_mapping_diff_without_previous_spec():
    r = review_transformation_action("mapping_diff", {"integration_spec": _contract_spec()})
    assert r["_success"] is True
    assert r["comparison_available"] is False
    assert len(r["current_mappings"]) == 4
    assert r["added"] == [] and r["removed"] == [] and r["changed"] == []


def test_mapping_diff_added_removed_changed_unchanged():
    previous = _contract_spec([
        {"operation_type": "direct", "source_field": "customer_id", "target_path": "Root/cust_id"},
        {"operation_type": "direct", "source_field": "name", "target_path": "Root/cust_name"},
        {"operation_type": "map_function", "function_type": "uppercase",
         "inputs": ["name"], "target_path": "Root/cust_name_upper"},
    ])
    current = _contract_spec([
        # unchanged
        {"operation_type": "direct", "source_field": "customer_id", "target_path": "Root/cust_id"},
        # changed: same target, different source
        {"operation_type": "direct", "source_field": "customer_id", "target_path": "Root/cust_name"},
        # added
        {"operation_type": "map_script", "inputs": ["name"], "outputs": ["Root/note"],
         "script_component_ref": "$ref:scr"},
        # (uppercase -> cust_name_upper removed)
    ])
    r = review_transformation_action(
        "mapping_diff", {"integration_spec": current, "previous_spec": previous}
    )
    assert r["comparison_available"] is True
    added_targets = {tuple(c["target_paths"]) for c in r["added"]}
    removed_targets = {tuple(c["target_paths"]) for c in r["removed"]}
    changed_targets = {tuple(c["current"]["target_paths"]) for c in r["changed"]}
    unchanged_targets = {tuple(c["target_paths"]) for c in r["unchanged"]}
    assert ("Root/note",) in added_targets
    assert ("Root/cust_name_upper",) in removed_targets
    assert ("Root/cust_name",) in changed_targets
    assert ("Root/cust_id",) in unchanged_targets


# ---------------------------------------------------------------------------
# generate_test_payload
# ---------------------------------------------------------------------------


def test_generate_test_payload_deterministic_skeletons():
    r = review_transformation_action("generate_test_payload", {"integration_spec": _contract_spec()})
    assert r["_success"] is True
    assert r["source_payload_skeleton"] == {"customer_id": 123, "name": "sample_text"}
    assert r["target_payload_skeleton"] == {
        "Root": {
            "cust_id": 123,
            "cust_name": "sample_text",
            "cust_name_upper": "sample_text",
            "note": "sample_text",
        }
    }
    assert isinstance(r["notes"], list) and r["notes"]


def test_generate_test_payload_handles_arrays():
    target_index = {
        "Root": {"path": "Root", "name": "Root", "kind": "object", "data_type": None,
                 "required": True, "mappable": False},
        "Root/items[]": {"path": "Root/items[]", "name": "items", "kind": "array",
                         "data_type": None, "required": False, "mappable": False},
        "Root/items[]/id": {"path": "Root/items[]/id", "name": "id", "kind": "simple",
                            "data_type": "number", "required": True, "mappable": True},
    }
    spec = {"version": "1.0", "name": "demo", "flows": [
        {"key": "transform", "operation": "transform",
         "source_profile_generation": {"field_index_by_path": _source_index()},
         "target_profile_generation": {"field_index_by_path": target_index},
         "operations": []}]}
    r = review_transformation_action("generate_test_payload", {"integration_spec": spec})
    assert r["target_payload_skeleton"] == {"Root": {"items": [{"id": 123}]}}


# ---------------------------------------------------------------------------
# compare_expected_actual
# ---------------------------------------------------------------------------


def test_compare_expected_actual_difference_codes():
    r = review_transformation_action("compare_expected_actual", {
        "expected_payload": {"a": 1, "b": "x", "nested": {"deep": 5}},
        "actual_payload": {"a": 2, "b": 7, "extra": True},
    })
    assert r["_success"] is True
    assert r["match"] is False
    by_path = {d["path"]: d["code"] for d in r["differences"]}
    assert by_path["a"] == VALUE_MISMATCH
    assert by_path["b"] == TYPE_MISMATCH
    assert by_path["extra"] == EXTRA_FIELD
    # whole subtree missing → report the missing parent, not its leaves
    assert by_path["nested"] == MISSING_FIELD


def test_compare_expected_actual_reports_nested_leaf_diff():
    r = review_transformation_action("compare_expected_actual", {
        "expected_payload": {"nested": {"deep": 5}},
        "actual_payload": {"nested": {}},
    })
    by_path = {d["path"]: d["code"] for d in r["differences"]}
    assert by_path["nested/deep"] == MISSING_FIELD


def test_compare_expected_actual_allow_extra_and_ignored_paths():
    r = review_transformation_action("compare_expected_actual", {
        "expected_payload": {"a": 1},
        "actual_payload": {"a": 1, "b": 2},
        "allow_extra": True,
    })
    assert r["match"] is True

    r2 = review_transformation_action("compare_expected_actual", {
        "expected_payload": {"a": 1, "b": 2},
        "actual_payload": {"a": 9, "b": 2},
        "ignored_paths": ["a"],
    })
    assert r2["match"] is True


def test_compare_expected_actual_lenient_types():
    r = review_transformation_action("compare_expected_actual", {
        "expected_payload": {"a": 1},
        "actual_payload": {"a": "1"},
        "strict_types": False,
    })
    assert r["match"] is True

    r_strict = review_transformation_action("compare_expected_actual", {
        "expected_payload": {"a": 1},
        "actual_payload": {"a": "1"},
    })
    assert r_strict["match"] is False


def test_compare_expected_actual_requires_both_payloads():
    r = review_transformation_action("compare_expected_actual", {"expected_payload": {}})
    assert r["_success"] is False
    assert r["code"] == "TRANSFORM_REVIEW_COMPARE_FAILED"


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


def test_raw_xml_profile_is_unsupported():
    spec = _executable_spec()
    spec["components"][1]["config"] = {"xml": "<JSONProfile/>"}
    r = review_transformation_action("list_fields", {"integration_spec": spec})
    assert r["_success"] is False
    assert r["code"] == TRANSFORM_REVIEW_XML_UNSUPPORTED


def test_literal_uuid_profile_index_unavailable():
    spec = _executable_spec()
    spec["components"][2]["config"]["source_profile_id"] = "11111111-2222-3333-4444-555555555555"
    r = review_transformation_action("list_fields", {"integration_spec": spec})
    assert r["_success"] is False
    assert r["code"] == TRANSFORM_REVIEW_PROFILE_INDEX_UNAVAILABLE


def test_missing_integration_spec_is_invalid_input():
    r = review_transformation_action("list_fields", {})
    assert r["_success"] is False
    assert r["code"] == TRANSFORM_REVIEW_INVALID_INPUT


def test_no_transform_found():
    spec = {"version": "1.0", "name": "demo", "flows": [], "components": []}
    r = review_transformation_action("list_fields", {"integration_spec": spec})
    assert r["_success"] is False
    assert r["code"] == TRANSFORM_REVIEW_NO_TRANSFORM_FOUND


def test_unknown_action():
    r = review_transformation_action("frobnicate", {})
    assert r["_success"] is False
    assert r["code"] == TRANSFORM_REVIEW_INVALID_INPUT
    assert "list_fields" in r["valid_actions"]


def test_malformed_contract_flow_does_not_raise():
    """Source A with truthy non-Mapping generation/index blocks must degrade
    gracefully (never raise an unhandled exception). Bug #132."""
    # field_index_by_path is a string
    spec1 = {"flows": [{"operation": "transform",
                        "source_profile_generation": {"field_index_by_path": "garbage"},
                        "target_profile_generation": {"field_index_by_path": _target_index()},
                        "operations": []}]}
    r1 = review_transformation_action("list_fields", {"integration_spec": spec1})
    assert r1["_success"] is True
    assert r1["source_fields"] == []
    _assert_safety(r1)

    # *_profile_generation itself is a non-Mapping; operations is a non-list
    spec2 = {"flows": [{"operation": "transform",
                        "source_profile_generation": "nope",
                        "target_profile_generation": ["also", "nope"],
                        "operations": "not-a-list"}]}
    r2 = review_transformation_action("validate_unmapped", {"integration_spec": spec2})
    assert r2["_success"] is True
    _assert_safety(r2)


def test_executable_happy_path_is_valid():
    """A complete executable direct map (named, $ref profiles, full field
    mappings) passes validate_unmapped via the canonical builder."""
    r = review_transformation_action("validate_unmapped", {"integration_spec": _executable_spec()})
    assert r["_success"] is True
    assert r["valid"] is True
    assert r["issue_count"] == 0


def test_executable_route_class_mismatch_flagged():
    """A transform.map with map_type='direct' but a function_mappings entry is
    a route-class violation that build_integration rejects — review surfaces the
    builder's canonical code, not a hand-rolled one. Codex r3 (delegation)."""
    spec = _executable_spec()
    spec["components"][2]["config"]["function_mappings"] = [
        {"function_type": "uppercase", "inputs": ["name"], "target_path": "Root/cust_name"}
    ]
    r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
    assert r["valid"] is False
    assert "UNSUPPORTED_TRANSFORM_ROUTE" in _codes(r)


def test_executable_unknown_map_type_flagged():
    spec = _executable_spec()
    spec["components"][2]["config"]["map_type"] = "xslt_transform"
    r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
    assert r["valid"] is False
    assert "UNSUPPORTED_TRANSFORM_ROUTE" in _codes(r)


def test_executable_missing_required_primary_list_flagged():
    """map_type='function' with only field_mappings (no function_mappings) is
    rejected by the canonical MapFunctionBuilder — review must flag it instead
    of giving a false pass. Codex r2 finding #1 / r3 delegation."""
    spec = _executable_spec()
    spec["components"][2]["config"]["map_type"] = "function"
    # field_mappings present, function_mappings absent
    r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
    assert r["valid"] is False
    assert "PROFILE_FIELD_VALIDATION_FAILED" in _codes(r)


def test_executable_empty_direct_field_mappings_flagged():
    spec = _executable_spec()
    spec["components"][2]["config"]["field_mappings"] = []
    r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
    assert r["valid"] is False
    assert "PROFILE_FIELD_VALIDATION_FAILED" in _codes(r)


def test_executable_script_ref_must_resolve_to_in_spec_component():
    """A script map whose $ref points at a non-script/transform.function (or
    missing) component is rejected — delegated cross-ref check. Codex r3 #3."""
    spec = _executable_spec()
    spec["components"][2]["config"] = {
        "map_type": "script", "source_profile_id": "$ref:db",
        "target_profile_id": "$ref:json", "source_profile_type": "profile.db",
        "target_profile_type": "profile.json",
        "script_mappings": [{
            "script_component_id": "$ref:json",  # resolves to a profile, not a script
            "inputs": [{"source_path": "name", "input_name": "n"}],
            "outputs": [{"output_name": "o", "target_path": "Root/cust_name"}],
        }],
    }
    spec["components"][2]["depends_on"] = ["db", "json"]
    r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
    assert r["valid"] is False
    assert "SCRIPT_MAPPING_REF_REQUIRED" in _codes(r)


def test_multiple_transform_maps_all_reviewed():
    """Issue #46 delegation: a 2nd transform.map must not be silently dropped —
    a problem in any map fails validate_unmapped. Codex r3 #1."""
    spec = _executable_spec()
    # Add a 2nd map referencing the same profiles but with an unknown map_type.
    spec["components"].append({
        "key": "map2", "type": "transform.map", "action": "create", "name": "Map 2",
        "depends_on": ["db", "json"], "config": {
            "map_type": "xslt_transform", "source_profile_id": "$ref:db",
            "target_profile_id": "$ref:json", "source_profile_type": "profile.db",
            "target_profile_type": "profile.json",
        },
    })
    r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
    assert r["valid"] is False
    assert "UNSUPPORTED_TRANSFORM_ROUTE" in _codes(r)


def test_mapping_diff_redacts_secret_function_parameters():
    """mapping_diff must not echo secret-shaped map-function parameter values
    from the spec (no-credential contract): nested, list, AND case/substring/
    separator variants. Codex r4 (recursive) + r5 (variant matching)."""
    spec = _contract_spec([
        {"operation_type": "map_function", "function_type": "uppercase",
         "inputs": ["name"], "target_path": "Root/cust_name_upper",
         "parameters": {
             "api_key": "S1", "delimiter": ",",
             "nested": {"token": "S2"},                  # recursive
             "creds": [{"password": "S3", "ok": "v"}],   # list of dicts
             "API_KEY": "S4",                            # uppercase
             "db_password": "S5",                        # substring
             "AUTH-TOKEN": "S6",                         # separator + substring
             "x-api-key": "S7",                          # hyphen variant
         }},
    ])
    r = review_transformation_action("mapping_diff", {"integration_spec": spec})
    import json as _json
    blob = _json.dumps(r)
    for secret in ("S1", "S2", "S3", "S4", "S5", "S6", "S7"):
        assert secret not in blob, f"{secret} leaked"
    params = r["current_mappings"][0]["parameters"]
    assert params["api_key"] == "[REDACTED]"
    assert params["nested"]["token"] == "[REDACTED]"
    assert params["creds"][0]["password"] == "[REDACTED]"
    assert params["API_KEY"] == "[REDACTED]"
    assert params["db_password"] == "[REDACTED]"
    assert params["AUTH-TOKEN"] == "[REDACTED]"
    assert params["x-api-key"] == "[REDACTED]"
    assert params["delimiter"] == ","          # non-secret preserved
    assert params["creds"][0]["ok"] == "v"      # non-secret sibling in list preserved


def test_mapping_diff_does_not_mutate_input_spec():
    """Redaction must deep-copy — the caller's spec parameters stay intact."""
    spec = _contract_spec([
        {"operation_type": "map_function", "function_type": "uppercase",
         "inputs": ["name"], "target_path": "Root/cust_name_upper",
         "parameters": {"api_key": "SECRET"}},
    ])
    review_transformation_action("mapping_diff", {"integration_spec": spec})
    op = spec["flows"][0]["operations"][0]
    assert op["parameters"]["api_key"] == "SECRET"  # original untouched


def test_non_string_map_type_does_not_raise():
    """A non-string map_type must not crash on .lower(); it becomes an
    unsupported-route issue. Codex r4 finding #2."""
    spec = _executable_spec()
    spec["components"][2]["config"]["map_type"] = 42
    r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
    assert r["_success"] is True
    assert r["valid"] is False
    assert "UNSUPPORTED_TRANSFORM_ROUTE" in _codes(r)


def test_non_list_depends_on_does_not_raise():
    """A non-list depends_on must not crash set() in validate_transform_map.
    Codex r4 finding #3."""
    spec = _executable_spec()
    spec["components"][2]["depends_on"] = 5
    r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
    assert r["_success"] is True
    # depends_on coerced to [] → the $ref profiles are now "not in depends_on"
    assert r["valid"] is False
    assert "MAP_PROFILE_REF_REQUIRED" in _codes(r)


def test_non_dict_transform_map_config_does_not_raise():
    """A transform.map whose config is a non-object must degrade to a structured
    error, not raise. Codex r2 finding #2."""
    spec = _executable_spec()
    spec["components"][2]["config"] = "not-an-object"
    r = review_transformation_action("list_fields", {"integration_spec": spec})
    assert r["_success"] is False
    assert r["code"] == TRANSFORM_REVIEW_PROFILE_INDEX_UNAVAILABLE
    _assert_safety(r)


def test_non_dict_profile_config_does_not_raise():
    # JSON profile (components[1]) runs validate_config, so a non-dict config
    # coerces to {} and surfaces a structured unavailable-index error rather
    # than raising. Codex r2 finding #2.
    spec = _executable_spec()
    spec["components"][1]["config"] = "garbage"
    r = review_transformation_action("list_fields", {"integration_spec": spec})
    assert r["_success"] is False
    assert r["code"] == TRANSFORM_REVIEW_PROFILE_INDEX_UNAVAILABLE
    _assert_safety(r)


def test_generate_test_payload_includes_xml_element_leaves():
    """XML profiles use kind='element' (mappable) for scalar leaves; the
    skeleton must include them, keyed off mappable, not kind=='simple'.
    Codex r2 finding #3."""
    xml_index = {
        "Root": {"path": "Root", "name": "Root", "kind": "element", "data_type": None,
                 "required": True, "mappable": False},
        "Root/id": {"path": "Root/id", "name": "id", "kind": "element",
                    "data_type": "number", "required": True, "mappable": True},
        "Root/label": {"path": "Root/label", "name": "label", "kind": "element",
                       "data_type": "character", "required": False, "mappable": True},
    }
    spec = {"version": "1.0", "name": "demo", "flows": [
        {"key": "transform", "operation": "transform",
         "source_profile_generation": {"field_index_by_path": _source_index()},
         "target_profile_generation": {"field_index_by_path": xml_index},
         "operations": []}]}
    r = review_transformation_action("generate_test_payload", {"integration_spec": spec})
    assert r["target_payload_skeleton"] == {"Root": {"id": 123, "label": "sample_text"}}


def test_truthy_non_list_map_lists_do_not_raise():
    """field_mappings/function_mappings/script_mappings (and script inputs/
    outputs) that are truthy non-lists must not raise during normalization.
    Codex r3 finding #2 (crash sub-case)."""
    for list_key, mt in [("field_mappings", "direct"), ("function_mappings", "function"), ("script_mappings", "script")]:
        spec = _executable_spec()
        cfg = spec["components"][2]["config"]
        cfg["map_type"] = mt
        cfg.pop("field_mappings", None)
        cfg[list_key] = True  # truthy non-list
        r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
        assert r["_success"] is True  # structured, no unhandled exception

    # script entry with non-list inputs/outputs
    spec = _executable_spec()
    cfg = spec["components"][2]["config"]
    cfg["map_type"] = "script"
    cfg.pop("field_mappings", None)
    cfg["script_mappings"] = [{"script_component_id": "$ref:scr", "inputs": True, "outputs": True}]
    r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
    assert r["_success"] is True


def test_non_iterable_components_does_not_raise():
    """Truthy non-list components must not crash with a raw TypeError. Codex r1 finding #3."""
    spec = {"version": "1.0", "name": "demo", "components": 5}
    r = review_transformation_action("list_fields", {"integration_spec": spec})
    assert r["_success"] is False
    assert r["code"] == TRANSFORM_REVIEW_NO_TRANSFORM_FOUND
    _assert_safety(r)


def test_non_iterable_flows_does_not_raise():
    spec = {"version": "1.0", "name": "demo", "flows": 7, "components": "junk"}
    r = review_transformation_action("validate_unmapped", {"integration_spec": spec})
    assert r["_success"] is False
    assert r["code"] == TRANSFORM_REVIEW_NO_TRANSFORM_FOUND
    _assert_safety(r)


def test_mapping_diff_malformed_previous_spec_does_not_raise():
    bad_prev = {"flows": [{"operation": "transform",
                          "source_profile_generation": {"field_index_by_path": 12345},
                          "operations": None}]}
    r = review_transformation_action(
        "mapping_diff",
        {"integration_spec": _contract_spec(), "previous_spec": bad_prev},
    )
    assert r["_success"] is True
    assert r["comparison_available"] is True


def test_invalid_json_string_config():
    r = review_transformation_action("list_fields", "{not valid json")
    assert r["_success"] is False
    assert r["code"] == TRANSFORM_REVIEW_INVALID_INPUT


# ---------------------------------------------------------------------------
# Golden: normalized mapping summaries
# ---------------------------------------------------------------------------


def test_golden_normalized_mapping_summaries():
    r = review_transformation_action("mapping_diff", {"integration_spec": _contract_spec()})
    by_target = {tuple(c["target_paths"]): c for c in r["current_mappings"]}

    direct = by_target[("Root/cust_id",)]
    assert direct["route"] == "direct"
    assert direct["source_paths"] == ["customer_id"]
    assert direct["function_type"] is None
    assert direct["script_ref"] is None

    func = by_target[("Root/cust_name_upper",)]
    assert func["route"] == "map_function"
    assert func["function_type"] == "uppercase"
    assert func["source_paths"] == ["name"]

    script = by_target[("Root/note",)]
    assert script["route"] == "map_script"
    assert script["script_ref"] == "$ref:scr"
    # This op is ref-based with no inline script_body text, so presence is
    # False — derived from a real body, not the summary flag (Codex r6b).
    assert script["script_body_present"] is False
    assert "script_body" not in script
