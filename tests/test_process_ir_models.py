"""Issue #136 (M12.1): unit/schema/golden tests for the strict ProcessIRV1 models.

Covers every node kind and nested variant, strictness (extras, strict
ints/bools), the PROCESS_IR_* diagnostic families with pinned JSON pointers,
reference syntax, ordering/terminal rules, secret handling, repr suppression,
deterministic canonical serialization, and the committed schema/JSON goldens.
"""

import json
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

import boomi_mcp.models as models
from boomi_mcp.errors import (
    PROCESS_IR_CAPABILITY_UNSUPPORTED,
    PROCESS_IR_REFERENCE_INVALID_FORMAT,
    PROCESS_IR_SCHEMA_INVALID,
    PROCESS_IR_SCHEMA_INVALID_CARDINALITY,
    PROCESS_IR_SCHEMA_UNKNOWN_FIELD,
    PROCESS_IR_SCHEMA_UNKNOWN_NODE,
    PROCESS_IR_SCHEMA_VERSION_UNSUPPORTED,
)
from boomi_mcp.models import (
    PROCESS_IR_V1_CAPABILITIES,
    ProcessIRV1,
    ProcessIRValidationError,
    canonical_process_ir_json,
    canonical_process_ir_schema_json,
    parse_process_ir_v1,
    process_ir_v1_json_schema,
)
from boomi_mcp.models import process_ir as process_ir_module

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "process_ir"


# ---------------------------------------------------------------------------
# Payload builders (sentinel values only)
# ---------------------------------------------------------------------------


def source(**over):
    return {"kind": "source", "connection_ref": "$ref:db_conn", "operation_ref": "$ref:db_op", **over}


def target(**over):
    return {"kind": "target", "connection_ref": "$ref:rest_conn", "operation_ref": "$ref:rest_op", **over}


def message(text="sentinel-text", **over):
    return {"kind": "message", "text": text, **over}


def doc(*steps):
    return {"version": "1", "body": {"kind": "sequence", "steps": list(steps)}}


def linear_doc(*mid):
    return doc(source(), *mid, target(), {"kind": "stop"})


LINEAR_NODES = [
    {"kind": "flow_control", "for_each_count": 5, "label": "b"},
    message(),
    {"kind": "map_ref", "map_ref": "$ref:map"},
    {
        "kind": "data_process",
        "steps": [
            {"operation": "custom_scripting", "script": "return 1"},
            {
                "operation": "split_documents",
                "profile_type": "json",
                "profile_ref": "$ref:profile",
                "link_element_key": "k",
                "link_element_name": "n",
            },
            {
                "operation": "combine_documents",
                "profile_type": "xml",
                "profile_ref": "$ref:profile2",
                "link_element_key": "k2",
                "link_element_name": "n2",
                "combine_into_link_element_key": "parent",
            },
        ],
    },
    {"kind": "cache_put", "cache_ref": "$ref:cache"},
    {"kind": "cache_get", "cache_ref": "$ref:cache", "external_writer": True},
    {"kind": "document_cache_retrieve", "cache_ref": "$ref:cache"},
    {"kind": "cache_remove", "cache_ref": "$ref:cache"},
    {
        "kind": "set_ddp",
        "name": "DDP_X",
        "source_values": [
            {"value_type": "static", "value": ""},
            {"value_type": "current"},
            {
                "value_type": "profile",
                "element_id": "el",
                "element_name": "eln",
                "profile_ref": "$ref:profile",
                "profile_type": "profile.json",
            },
            {"value_type": "ddp", "property_name": "P", "default_value": "d"},
            {"value_type": "dpp", "property_name": "Q"},
        ],
    },
    {"kind": "set_dpp", "name": "DPP_Y", "source_values": [{"value_type": "static", "value": "v"}], "persist": True},
]


def decision(**over):
    node = {
        "kind": "decision",
        "comparison": "equals",
        "left": {"value_type": "track", "property_id": "dynamicdocument.DDP_S"},
        "right": {"value_type": "static", "static_value": ""},
        "true_arm": {"steps": [message()], "terminal": target()},
        "false_arm": {"steps": [message()], "terminal": {"kind": "stop"}},
    }
    node.update(over)
    return node


def branch(**over):
    node = {
        "kind": "branch",
        "legs": [
            {"steps": [message()], "terminal": target()},
            {"steps": [message()], "terminal": {"kind": "cache_put", "cache_ref": "$ref:cache"}},
        ],
    }
    node.update(over)
    return node


def exception(**over):
    return {"kind": "exception", "message_template": "boom {1}", **over}


def codes_of(err: ProcessIRValidationError):
    return [(d.code, d.path) for d in err.diagnostics]


def parse_error(payload) -> ProcessIRValidationError:
    with pytest.raises(ProcessIRValidationError) as exc_info:
        parse_process_ir_v1(payload)
    return exc_info.value


# ---------------------------------------------------------------------------
# Construction round-trips for every node kind and nested variant
# ---------------------------------------------------------------------------


def test_linear_full_vocabulary_parses_and_roundtrips():
    ir = parse_process_ir_v1(linear_doc(*LINEAR_NODES))
    dumped = ir.model_dump(mode="json")
    assert ProcessIRV1.model_validate(dumped) == ir


def test_control_vocabulary_parses_and_roundtrips():
    ir = parse_process_ir_v1(
        doc(
            source(),
            message(),
            decision(
                true_arm={"steps": [], "terminal": branch()},
                false_arm={"steps": [message()], "terminal": exception(title="T")},
            ),
        )
    )
    assert ProcessIRV1.model_validate(ir.model_dump(mode="json")) == ir


def test_wrapper_vocabulary_parses_and_roundtrips():
    ir = parse_process_ir_v1(
        doc(
            {"kind": "process_call", "process_ref": "$ref:child"},
            {"kind": "process_call", "process_ref": "lit-id", "wait": False, "abort_on_error": True, "label": "L"},
            {"kind": "return_documents", "label": "out"},
        )
    )
    calls = [s for s in ir.body.steps if s.kind == "process_call"]
    assert calls[0].wait is True and calls[0].abort_on_error is False
    assert calls[1].wait is False and calls[1].abort_on_error is True


def test_linear_return_documents_is_a_standalone_terminal():
    # Legacy parity: with return_documents enabled the builder emits ONLY the
    # returndocuments terminal after the sequence — the configured target is
    # dead and is NOT represented in IR (_target_terminal_entries).
    ir = parse_process_ir_v1(doc(source(), message(), {"kind": "return_documents", "label": "out"}))
    assert ir.body.steps[-1].kind == "return_documents"


def test_target_followed_by_return_documents_rejected():
    err = parse_error(doc(source(), message(), target(), {"kind": "return_documents"}))
    assert err.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID_CARDINALITY


def test_defaults_expand_to_current_parity_values():
    ir = parse_process_ir_v1(linear_doc({"kind": "document_cache_retrieve", "cache_ref": "$ref:c"}))
    node = ir.body.steps[1]
    assert node.empty_cache_behavior == "stopprocess"
    assert node.load_all_documents is True
    ex = parse_process_ir_v1(doc(source(), exception())).body.steps[-1]
    assert ex.stop_single_document is False
    assert ex.parameter_source == "caught_error"
    assert ex.title is None


# ---------------------------------------------------------------------------
# Strict types + strict extras
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "node",
    [
        {"kind": "flow_control", "for_each_count": "5"},
        {"kind": "flow_control", "for_each_count": True},
        {"kind": "flow_control", "for_each_count": 0},
        {"kind": "cache_get", "cache_ref": "x", "external_writer": "true"},
        {"kind": "set_dpp", "name": "N", "source_values": [{"value_type": "current"}], "persist": "yes"},
        {"kind": "process_call", "process_ref": "x", "wait": 1},
    ],
)
def test_strict_scalar_coercions_rejected(node):
    err = parse_error(linear_doc(node))
    assert err.diagnostics, codes_of(err)


ALL_MODEL_CLASSES = [
    models.SourceEndpointV1,
    models.TargetEndpointV1,
    models.FlowControlNodeV1,
    models.MessageNodeV1,
    models.MapRefNodeV1,
    models.DataProcessNodeV1,
    models.CachePutNodeV1,
    models.DocumentCacheRetrieveNodeV1,
    models.CacheGetNodeV1,
    models.CacheRemoveNodeV1,
    models.SetDdpNodeV1,
    models.SetDppNodeV1,
    models.ProcessCallNodeV1,
    models.BranchNodeV1,
    models.BranchLegV1,
    models.DecisionNodeV1,
    models.DecisionTrueArmV1,
    models.DecisionFalseArmV1,
    models.ExceptionNodeV1,
    models.StopNodeV1,
    models.ReturnDocumentsNodeV1,
    models.SequenceNodeV1,
    models.ProcessIRV1,
    models.CustomScriptingOpV1,
    models.SplitDocumentsOpV1,
    models.CombineDocumentsOpV1,
    models.StaticPropertySourceV1,
    models.CurrentPropertySourceV1,
    models.ProfilePropertySourceV1,
    models.DdpPropertySourceV1,
    models.DppPropertySourceV1,
    models.TrackOperandV1,
    models.StaticOperandV1,
    models.ProcessIRDiagnostic,
]


@pytest.mark.parametrize("model_cls", ALL_MODEL_CLASSES, ids=lambda c: c.__name__)
def test_every_authored_model_forbids_extras(model_cls):
    assert model_cls.model_config.get("extra") == "forbid"


def test_unknown_field_diagnostic_pins_pointer():
    err = parse_error(linear_doc({**message(), "bogus_key": 1}))
    assert codes_of(err) == [(PROCESS_IR_SCHEMA_UNKNOWN_FIELD, "/body/steps/1/bogus_key")]


def test_unknown_field_on_nested_arm_pins_pointer():
    bad_decision = decision(
        true_arm={"steps": [], "terminal": target(), "sneaky": True}
    )
    err = parse_error(doc(source(), bad_decision))
    assert (PROCESS_IR_SCHEMA_UNKNOWN_FIELD, "/body/steps/1/true_arm/sneaky") in codes_of(err)


# ---------------------------------------------------------------------------
# Unknown discriminators and capability gates
# ---------------------------------------------------------------------------


def test_unknown_kind_is_unknown_node():
    err = parse_error(doc({"kind": "teleport"}))
    assert codes_of(err) == [(PROCESS_IR_SCHEMA_UNKNOWN_NODE, "/body/steps/0")]


def test_unknown_property_source_value_type_is_unknown_node():
    node = {"kind": "set_ddp", "name": "N", "source_values": [{"value_type": "wat"}]}
    err = parse_error(linear_doc(node))
    assert codes_of(err) == [(PROCESS_IR_SCHEMA_UNKNOWN_NODE, "/body/steps/1/source_values/0")]


def test_unknown_dataprocess_operation_is_unknown_node():
    node = {"kind": "data_process", "steps": [{"operation": "zip"}]}
    err = parse_error(linear_doc(node))
    assert codes_of(err) == [(PROCESS_IR_SCHEMA_UNKNOWN_NODE, "/body/steps/1/steps/0")]


def test_definedparameter_source_is_capability_gated():
    node = {
        "kind": "set_ddp",
        "name": "N",
        "source_values": [{"value_type": "definedparameter", "component_id": "c", "property_key": "k"}],
    }
    err = parse_error(linear_doc(node))
    assert codes_of(err) == [
        (PROCESS_IR_CAPABILITY_UNSUPPORTED, "/body/steps/1/source_values/0")
    ]


@pytest.mark.parametrize("gated_key", ["doc_cache_index", "cache_key_values", "load_all_documents"])
def test_keyed_cache_keys_on_cache_get_are_capability_gated(gated_key):
    node = {"kind": "cache_get", "cache_ref": "$ref:c", gated_key: 1}
    err = parse_error(linear_doc(node))
    assert codes_of(err) == [
        (PROCESS_IR_CAPABILITY_UNSUPPORTED, f"/body/steps/1/{gated_key}")
    ]


def test_gated_key_on_other_node_stays_unknown_field():
    err = parse_error(linear_doc({**message(), "doc_cache_index": 1}))
    assert codes_of(err) == [(PROCESS_IR_SCHEMA_UNKNOWN_FIELD, "/body/steps/1/doc_cache_index")]


def test_keyed_cache_literal_false_is_capability_gated():
    # load_all_documents/remove_all_documents accept only True; False is a
    # keyed/indexed cache request — the NAMED gate, not a generic mismatch.
    retrieve = {"kind": "document_cache_retrieve", "cache_ref": "$ref:c", "load_all_documents": False}
    err = parse_error(linear_doc(retrieve))
    assert codes_of(err) == [
        (PROCESS_IR_CAPABILITY_UNSUPPORTED, "/body/steps/1/load_all_documents")
    ]
    remove = {"kind": "cache_remove", "cache_ref": "$ref:c", "remove_all_documents": False}
    err = parse_error(linear_doc(remove))
    assert codes_of(err) == [
        (PROCESS_IR_CAPABILITY_UNSUPPORTED, "/body/steps/1/remove_all_documents")
    ]


@pytest.mark.parametrize("truthy_int", [1, 1.0])
def test_strict_true_fields_reject_int_coercion(truthy_int):
    retrieve = {"kind": "document_cache_retrieve", "cache_ref": "$ref:c", "load_all_documents": truthy_int}
    err = parse_error(linear_doc(retrieve))
    assert err.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID
    op = {"operation": "custom_scripting", "script": "s", "use_cache": truthy_int}
    err = parse_error(linear_doc({"kind": "data_process", "steps": [op]}))
    assert err.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID


def test_use_cache_false_rejected():
    op = {"operation": "custom_scripting", "script": "s", "use_cache": False}
    err = parse_error(linear_doc({"kind": "data_process", "steps": [op]}))
    assert err.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID


# ---------------------------------------------------------------------------
# Version + payload-shape gates
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("payload", [{"body": {}}, {"version": "2", "body": {}}, {"version": 1, "body": {}}])
def test_version_gate(payload):
    err = parse_error(payload)
    assert codes_of(err) == [(PROCESS_IR_SCHEMA_VERSION_UNSUPPORTED, "/version")]


@pytest.mark.parametrize("payload", [None, [], "x", 7])
def test_non_mapping_payload_is_schema_invalid(payload):
    err = parse_error(payload)
    assert codes_of(err) == [(PROCESS_IR_SCHEMA_INVALID, "")]


# ---------------------------------------------------------------------------
# Cardinality
# ---------------------------------------------------------------------------


def test_empty_root_steps_is_cardinality():
    err = parse_error({"version": "1", "body": {"kind": "sequence", "steps": []}})
    assert codes_of(err) == [(PROCESS_IR_SCHEMA_INVALID_CARDINALITY, "/body/steps")]


@pytest.mark.parametrize("leg_count", [1, 26])
def test_branch_leg_bounds(leg_count):
    legs = [{"steps": [], "terminal": target()} for _ in range(leg_count)]
    err = parse_error(doc(source(), branch(legs=legs)))
    assert (PROCESS_IR_SCHEMA_INVALID_CARDINALITY, "/body/steps/1/legs") in codes_of(err)


def test_branch_25_legs_accepted():
    legs = [{"steps": [], "terminal": target()} for _ in range(25)]
    ir = parse_process_ir_v1(doc(source(), branch(legs=legs)))
    assert len(ir.body.steps[-1].legs) == 25


def test_empty_source_values_is_cardinality():
    node = {"kind": "set_ddp", "name": "N", "source_values": []}
    err = parse_error(linear_doc(node))
    assert codes_of(err) == [
        (PROCESS_IR_SCHEMA_INVALID_CARDINALITY, "/body/steps/1/source_values")
    ]


def test_empty_data_process_steps_is_cardinality():
    err = parse_error(linear_doc({"kind": "data_process", "steps": []}))
    assert codes_of(err) == [(PROCESS_IR_SCHEMA_INVALID_CARDINALITY, "/body/steps/1/steps")]


def test_false_arm_bare_stop_is_cardinality():
    bad = decision(false_arm={"steps": [], "terminal": {"kind": "stop"}})
    err = parse_error(doc(source(), bad))
    assert err.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID_CARDINALITY


# ---------------------------------------------------------------------------
# Ordering / terminal rules
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "steps,expect_code",
    [
        # source not first
        ([message(), source(), target(), {"kind": "stop"}], PROCESS_IR_SCHEMA_INVALID_CARDINALITY),
        # connector flow missing source
        ([message(), target(), {"kind": "stop"}], PROCESS_IR_SCHEMA_INVALID_CARDINALITY),
        # target not followed by stop/return_documents
        ([source(), target(), message()], PROCESS_IR_SCHEMA_INVALID_CARDINALITY),
        # bare trailing target
        ([source(), message(), target()], PROCESS_IR_SCHEMA_INVALID_CARDINALITY),
        # control mid-sequence
        ([source(), decision(), message(), target(), {"kind": "stop"}], PROCESS_IR_SCHEMA_INVALID_CARDINALITY),
        # stop without preceding target
        ([source(), message(), {"kind": "stop"}], PROCESS_IR_SCHEMA_INVALID_CARDINALITY),
        # cache_put feeding the target
        (
            [source(), {"kind": "cache_put", "cache_ref": "$ref:c"}, target(), {"kind": "stop"}],
            PROCESS_IR_SCHEMA_INVALID_CARDINALITY,
        ),
        # cache_put followed by a non-read
        (
            [source(), {"kind": "cache_put", "cache_ref": "$ref:c"}, message(), target(), {"kind": "stop"}],
            PROCESS_IR_SCHEMA_INVALID_CARDINALITY,
        ),
        # process_call mixed with a connector node
        ([{"kind": "process_call", "process_ref": "x"}, message(), {"kind": "stop"}], PROCESS_IR_CAPABILITY_UNSUPPORTED),
        # process_call sequence without terminal
        ([{"kind": "process_call", "process_ref": "x"}], PROCESS_IR_SCHEMA_INVALID_CARDINALITY),
    ],
)
def test_sequence_ordering_rules(steps, expect_code):
    err = parse_error(doc(*steps))
    assert err.diagnostics[0].code == expect_code, codes_of(err)


def test_branch_leg_trailing_cache_put_directed_to_terminal():
    bad_branch = branch(
        legs=[
            {"steps": [{"kind": "cache_put", "cache_ref": "$ref:c"}], "terminal": target()},
            {"steps": [], "terminal": target()},
        ]
    )
    err = parse_error(doc(source(), bad_branch))
    assert err.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID_CARDINALITY


def test_true_arm_trailing_cache_put_rejected():
    bad = decision(true_arm={"steps": [{"kind": "cache_put", "cache_ref": "$ref:c"}], "terminal": target()})
    err = parse_error(doc(source(), bad))
    assert err.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID_CARDINALITY


def test_false_arm_trailing_cache_put_allowed_only_before_stop():
    ok = decision(false_arm={"steps": [{"kind": "cache_put", "cache_ref": "$ref:c"}], "terminal": {"kind": "stop"}})
    parse_process_ir_v1(doc(source(), ok))
    bad = decision(
        false_arm={"steps": [{"kind": "cache_put", "cache_ref": "$ref:c"}], "terminal": exception()}
    )
    err = parse_error(doc(source(), bad))
    assert err.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID_CARDINALITY


def test_nested_decision_is_impossible_by_schema():
    bad = decision(true_arm={"steps": [], "terminal": decision()})
    err = parse_error(doc(source(), bad))
    assert err.diagnostics[0].code == PROCESS_IR_SCHEMA_UNKNOWN_NODE


def test_process_call_not_allowed_in_branch_leg():
    bad = branch(legs=[
        {"steps": [{"kind": "process_call", "process_ref": "x"}], "terminal": target()},
        {"steps": [], "terminal": target()},
    ])
    err = parse_error(doc(source(), bad))
    assert err.diagnostics[0].code == PROCESS_IR_SCHEMA_UNKNOWN_NODE


# ---------------------------------------------------------------------------
# Reference + property-name syntax
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ref", ["", " id ", "$ref:", "$ref: x", "$ref:a b", "$ref:x "])
def test_reference_syntax_rejected(ref):
    err = parse_error(linear_doc({"kind": "map_ref", "map_ref": ref}))
    assert err.diagnostics[0].code == PROCESS_IR_REFERENCE_INVALID_FORMAT
    assert err.diagnostics[0].path == "/body/steps/1/map_ref"


@pytest.mark.parametrize("ref", ["$ref:key", "literal-component-id", "00000000-0000-0000-0000-000000000001"])
def test_reference_syntax_accepted(ref):
    parse_process_ir_v1(linear_doc({"kind": "map_ref", "map_ref": ref}))


@pytest.mark.parametrize(
    "name", ["dynamicdocument.X", "process.X", "document.dynamic.userdefined.X", "HAS SPACE", "  "]
)
def test_property_name_rules(name):
    node = {"kind": "set_ddp", "name": name, "source_values": [{"value_type": "current"}]}
    err = parse_error(linear_doc(node))
    assert err.diagnostics[0].code in (
        PROCESS_IR_SCHEMA_INVALID_CARDINALITY,
        PROCESS_IR_CAPABILITY_UNSUPPORTED,
    )


@pytest.mark.parametrize(
    "op_field", ["link_element_key", "link_element_name", "combine_into_link_element_key"]
)
def test_whitespace_only_dataprocess_identifiers_rejected(op_field):
    op = {
        "operation": "combine_documents",
        "profile_type": "json",
        "profile_ref": "$ref:p",
        "link_element_key": "k",
        "link_element_name": "n",
        op_field: "   ",
    }
    err = parse_error(linear_doc({"kind": "data_process", "steps": [op]}))
    assert err.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID_CARDINALITY


@pytest.mark.parametrize("field", ["element_id", "element_name", "profile_type"])
def test_whitespace_only_profile_source_identifiers_rejected(field):
    src = {
        "value_type": "profile",
        "element_id": "el",
        "element_name": "eln",
        "profile_ref": "$ref:p",
        "profile_type": "profile.json",
        field: " ",
    }
    node = {"kind": "set_ddp", "name": "N", "source_values": [src]}
    err = parse_error(linear_doc(node))
    assert err.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID_CARDINALITY


def test_whitespace_only_property_source_name_rejected():
    node = {"kind": "set_dpp", "name": "N", "source_values": [{"value_type": "ddp", "property_name": "  "}]}
    err = parse_error(linear_doc(node))
    assert err.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID_CARDINALITY


def test_exception_placeholder_required_when_binding():
    err = parse_error(doc(source(), exception(message_template="no placeholder")))
    assert err.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID_CARDINALITY
    parse_process_ir_v1(doc(source(), exception(message_template="static", parameter_source="none")))


# ---------------------------------------------------------------------------
# Secrets (ADR-001 §11)
# ---------------------------------------------------------------------------


def test_secret_scan_tuple_matches_builder():
    from boomi_mcp.categories.components.builders.process_flow_builder import ProcessFlowBuilder

    assert process_ir_module._FORBIDDEN_SECRET_KEY_SUBSTRINGS == ProcessFlowBuilder.FORBIDDEN_SECRET_FIELDS


def test_secret_shaped_key_rejected_without_echo():
    sentinel = "hunter2-sentinel-value"
    payload = doc(source(), {**message(), "api_key": sentinel}, target(), {"kind": "stop"})
    err = parse_error(payload)
    assert codes_of(err) == [(PROCESS_IR_CAPABILITY_UNSUPPORTED, "/body/steps/1/api_key")]
    rendered = str(err) + repr(err.diagnostics) + json.dumps([d.model_dump() for d in err.diagnostics])
    assert sentinel not in rendered


def test_secret_container_value_rejected_and_empty_scalar_skipped():
    err = parse_error({"version": "1", "authorization": {"nested": "x"}, "body": {}})
    assert err.diagnostics[0].code == PROCESS_IR_CAPABILITY_UNSUPPORTED
    # An empty-string secret-shaped value is skipped (same semantics as the
    # builder scanner) — validation proceeds to the schema gate instead.
    err2 = parse_error({"version": "1", "password": "", "body": {}})
    assert err2.diagnostics[0].code != PROCESS_IR_CAPABILITY_UNSUPPORTED


def test_repr_and_str_hide_authored_values():
    sentinel = "SENTINEL_SCRIPT_BODY"
    ir = parse_process_ir_v1(
        linear_doc({"kind": "data_process", "steps": [{"operation": "custom_scripting", "script": sentinel}]})
    )
    assert sentinel not in repr(ir)
    assert sentinel not in str(ir)
    assert sentinel not in repr(ir.body.steps[1])


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_diagnostics_sorted_and_deterministic():
    payload = doc(
        {"kind": "teleport"},
        {**message(), "bogus": 1},
    )
    err1, err2 = parse_error(payload), parse_error(payload)
    assert codes_of(err1) == codes_of(err2)
    assert codes_of(err1) == sorted(codes_of(err1), key=lambda t: (t[1], t[0]))


def test_canonical_json_deterministic_across_runs():
    payload = linear_doc(*LINEAR_NODES)
    first = canonical_process_ir_json(parse_process_ir_v1(payload))
    second = canonical_process_ir_json(parse_process_ir_v1(json.loads(json.dumps(payload))))
    assert first == second


def test_canonical_schema_deterministic_across_runs():
    assert canonical_process_ir_schema_json() == canonical_process_ir_schema_json()


# ---------------------------------------------------------------------------
# Generated JSON Schema shape
# ---------------------------------------------------------------------------


def _walk_schema_objects(schema):
    if isinstance(schema, dict):
        yield schema
        for value in schema.values():
            yield from _walk_schema_objects(value)
    elif isinstance(schema, list):
        for item in schema:
            yield from _walk_schema_objects(item)


def test_every_discriminator_is_schema_required():
    # Schema/runtime agreement: a node without its discriminator must fail the
    # SCHEMA too, not only runtime parsing (impl-review r1 high finding).
    schema = process_ir_v1_json_schema()
    for name, definition in schema["$defs"].items():
        props = definition.get("properties", {})
        required = set(definition.get("required", []))
        for disc in ("kind", "value_type", "operation"):
            if disc in props:
                assert disc in required, f"{name}.{disc} must be schema-required"
    assert "version" in set(schema.get("required", []))


def test_kindless_node_fails_the_json_schema():
    jsonschema = pytest.importorskip("jsonschema")
    schema = process_ir_v1_json_schema()
    items = schema["$defs"]["SequenceNodeV1"]["properties"]["steps"]["items"]
    node_schema = {"$defs": schema["$defs"], **items}
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate({"text": "x"}, node_schema)


def test_strict_true_fields_keep_const_true_in_schema():
    schema = process_ir_v1_json_schema()
    defs = schema["$defs"]
    assert defs["DocumentCacheRetrieveNodeV1"]["properties"]["load_all_documents"]["const"] is True
    assert defs["CacheRemoveNodeV1"]["properties"]["remove_all_documents"]["const"] is True
    assert defs["CustomScriptingOpV1"]["properties"]["use_cache"]["const"] is True


def test_schema_closed_discriminated_union():
    schema = process_ir_v1_json_schema()
    body_defs = schema["$defs"]
    seq = body_defs["SequenceNodeV1"]
    steps_items = seq["properties"]["steps"]["items"]
    assert "discriminator" in steps_items and "oneOf" in steps_items
    mapping = steps_items["discriminator"]["mapping"]
    assert set(mapping) == {
        "source", "target", "flow_control", "message", "map_ref", "data_process",
        "cache_put", "document_cache_retrieve", "cache_get", "cache_remove",
        "set_ddp", "set_dpp", "process_call", "branch", "decision", "exception",
        "stop", "return_documents",
    }


def test_every_schema_object_rejects_extras():
    schema = process_ir_v1_json_schema()
    for obj in _walk_schema_objects(schema):
        if obj.get("type") == "object" and "properties" in obj:
            assert obj.get("additionalProperties") is False, obj.get("title", obj)


def test_schema_carries_no_layout_cfg_or_open_config_vocabulary():
    text = canonical_process_ir_schema_json().lower()
    for forbidden in ("dragpoint", "coordinate", "shape_id", "shapetype", "layout", "tosha"):
        assert forbidden not in text
    schema = process_ir_v1_json_schema()
    for obj in _walk_schema_objects(schema):
        props = obj.get("properties")
        if isinstance(props, dict):
            assert "config" not in props
            assert "edges" not in props


# ---------------------------------------------------------------------------
# Golden pins
# ---------------------------------------------------------------------------


def golden_documents():
    """The committed full-vocabulary canonical documents (see fixtures/process_ir)."""
    linear_flow = parse_process_ir_v1(linear_doc(*LINEAR_NODES))
    control_flow = parse_process_ir_v1(
        doc(
            source(),
            message("route me"),
            decision(
                label="router",
                true_arm={
                    "steps": [message("t")],
                    "terminal": branch(
                        label="fan",
                        legs=[
                            {"steps": [{"kind": "map_ref", "map_ref": "$ref:map"}], "terminal": target()},
                            {"steps": [message("stage")], "terminal": {"kind": "cache_put", "cache_ref": "$ref:cache"}},
                        ],
                    ),
                },
                false_arm={"steps": [message("f")], "terminal": exception(title="Sentinel")},
            ),
        )
    )
    wrapper_flow = parse_process_ir_v1(
        doc(
            {"kind": "process_call", "process_ref": "$ref:child"},
            {"kind": "process_call", "process_ref": "00000000-0000-0000-0000-000000000001", "wait": False, "abort_on_error": True, "label": "second"},
            {"kind": "return_documents", "label": "out"},
        )
    )
    return {"linear_flow": linear_flow, "control_flow": control_flow, "wrapper_flow": wrapper_flow}


def canonical_golden_payload() -> str:
    docs = {name: json.loads(canonical_process_ir_json(ir)) for name, ir in golden_documents().items()}
    return json.dumps(docs, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def test_canonical_json_golden_pin():
    committed = (FIXTURES / "process_ir_v1.json").read_text()
    assert canonical_golden_payload() == committed
    assert canonical_golden_payload() == committed  # twice per run — deterministic


def test_canonical_schema_golden_pin():
    committed = (FIXTURES / "process_ir_v1.schema.json").read_text()
    assert canonical_process_ir_schema_json() == committed
    assert canonical_process_ir_schema_json() == committed


# ---------------------------------------------------------------------------
# Exports + manifest
# ---------------------------------------------------------------------------


def test_package_exports_pinned():
    for name in (
        "ProcessIRV1", "SequenceNodeV1", "ProcessNodeV1", "LinearNodeV1", "ComponentRefV1",
        "PropertySourceV1", "DataProcessOperationV1", "DecisionOperandV1",
        "ProcessIRDiagnostic", "ProcessIRValidationError", "parse_process_ir_v1",
        "canonical_process_ir_json", "canonical_process_ir_schema_json",
        "process_ir_v1_json_schema", "PROCESS_IR_V1_CAPABILITIES", "PROCESS_IR_VERSION",
    ):
        assert name in models.__all__, name
    assert "_process_ir_compat" not in models.__all__


def test_private_codec_not_imported_by_package_import():
    # Fresh interpreter: importing the package must NOT pull the private codec
    # in (order-independent — in-process sys.modules is polluted by the codec's
    # own test module when the full suite runs).
    import subprocess

    code = (
        "import sys, boomi_mcp.models; "
        "assert 'boomi_mcp.models._process_ir_compat' not in sys.modules"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        env={"PYTHONPATH": _src, "PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_capability_manifest_immutable_and_complete():
    with pytest.raises(TypeError):
        PROCESS_IR_V1_CAPABILITIES["joins"] = "supported"  # type: ignore[index]
    assert PROCESS_IR_V1_CAPABILITIES["caller_authored_cfg_edges"] == "unsupported"
    assert PROCESS_IR_V1_CAPABILITIES["secret_values"] == "unsupported"
    assert PROCESS_IR_V1_CAPABILITIES["keyed_cache"] == "gated"
    assert PROCESS_IR_V1_CAPABILITIES["mixed_connector_execution"] == "gated"
    assert set(PROCESS_IR_V1_CAPABILITIES.values()) <= {"gated", "unsupported"}
