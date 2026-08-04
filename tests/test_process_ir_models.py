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
    PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY,
    PROCESS_IR_CAPABILITY_UNSUPPORTED,
    PROCESS_IR_REFERENCE_INVALID_FORMAT,
    PROCESS_IR_SCHEMA_BRANCH_CARDINALITY,
    PROCESS_IR_SCHEMA_INVALID,
    PROCESS_IR_SCHEMA_INVALID_CARDINALITY,
    PROCESS_IR_SCHEMA_UNKNOWN_FIELD,
    PROCESS_IR_SCHEMA_UNKNOWN_NODE,
    PROCESS_IR_SCHEMA_VERSION_UNSUPPORTED,
    PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED,
    PROCESS_IR_SEMANTIC_NESTING_LIMIT,
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
    """#141: the 2-25 Branch bound gets its OWN code.

    It is the platform's documented bound on Branch paths, not an arbitrary list
    length, so it is worth distinguishing from every other cardinality failure —
    ``test_non_branch_list_bounds_keep_the_generic_cardinality_code`` pins that
    nothing else moved with it.
    """
    legs = [{"steps": [], "terminal": target()} for _ in range(leg_count)]
    err = parse_error(doc(source(), branch(legs=legs)))
    assert (PROCESS_IR_SCHEMA_BRANCH_CARDINALITY, "/body/steps/1/legs") in codes_of(err)


def test_non_branch_list_bounds_keep_the_generic_cardinality_code():
    """The #141 re-point is keyed on a trailing ``legs`` token ONLY."""
    err = parse_error({"version": "1", "body": {"kind": "sequence", "steps": []}})
    assert codes_of(err) == [(PROCESS_IR_SCHEMA_INVALID_CARDINALITY, "/body/steps")]
    err = parse_error(linear_doc({"kind": "data_process", "steps": []}))
    assert codes_of(err) == [(PROCESS_IR_SCHEMA_INVALID_CARDINALITY, "/body/steps/1/steps")]


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


def test_false_arm_bare_stop_is_accepted():
    """#141 REMOVED the "reject path is never a bare Stop" rule.

    It was legacy BUILDER parity, never a platform rule: a real production
    Decision routes its false outcome straight to a Stop with zero intervening
    steps (`.codex/plans/issue-141-live-captures.md` §2.1). The legacy
    ``flow_sequence`` surface still rejects it — see
    ``test_legacy_flow_sequence_still_rejects_a_bare_false_stop``.
    """
    ok = decision(false_arm={"steps": [], "terminal": {"kind": "stop"}})
    ir = parse_process_ir_v1(doc(source(), ok))
    assert ir.body.steps[-1].false_arm.terminal.kind == "stop"
    assert ir.body.steps[-1].false_arm.steps == []


def test_branch_leg_and_true_arm_still_reject_a_bare_stop():
    """The bare-Stop relaxation is FALSE-ARM ONLY.

    A Branch leg or Decision TRUE arm that only stops does no work, and the
    capture records the empty-leg question as explicitly UNPROVEN (§2.4), so V1
    stays fail-closed there.
    """
    bad_leg = branch(
        legs=[
            {"steps": [], "terminal": {"kind": "stop"}},
            {"steps": [], "terminal": target()},
        ]
    )
    err = parse_error(doc(source(), bad_leg))
    assert err.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID_CARDINALITY

    bad_arm = decision(true_arm={"steps": [], "terminal": {"kind": "stop"}})
    err = parse_error(doc(source(), bad_arm))
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
        # control mid-sequence — #141 gives continuation its own capability code:
        # the caller has not miscounted a list, they have asked for a feature
        # ProcessIR v1 deliberately does not emit.
        (
            [source(), decision(), message(), target(), {"kind": "stop"}],
            PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED,
        ),
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


def test_nested_decision_is_supported(): # #141
    """Decision-in-Decision is the single most common real shape in the reference
    account (`.codex/plans/issue-141-live-captures.md` §2.1) and #136 could not
    express it at all. It is admitted on BOTH outcomes, bounded by the depth cap."""
    ok = decision(true_arm={"steps": [message()], "terminal": decision()})
    ir = parse_process_ir_v1(doc(source(), ok))
    assert ir.body.steps[-1].true_arm.terminal.kind == "decision"

    ok_false = decision(false_arm={"steps": [], "terminal": decision()})
    ir = parse_process_ir_v1(doc(source(), ok_false))
    assert ir.body.steps[-1].false_arm.terminal.kind == "decision"


def test_nested_decision_in_a_branch_leg_is_supported():  # #141
    """Branch-leg -> Decision is live-attested (capture §2.1)."""
    ok = branch(
        legs=[
            {"steps": [message()], "terminal": decision()},
            {"steps": [], "terminal": target()},
        ]
    )
    ir = parse_process_ir_v1(doc(source(), ok))
    assert ir.body.steps[-1].legs[0].terminal.kind == "decision"


def test_nested_branch_in_a_branch_leg_stays_gated():  # #141
    """Branch-leg -> Branch appears NOWHERE in the capture. Unproven stays closed."""
    bad = branch(
        legs=[
            {"steps": [message()], "terminal": branch(
                legs=[{"steps": [], "terminal": target()}, {"steps": [], "terminal": target()}]
            )},
            {"steps": [], "terminal": target()},
        ]
    )
    err = parse_error(doc(source(), bad))
    # #141 r1 (architect review): a KNOWN kind rejected in a control body is a
    # body-slot capability failure, not an unknown node. Calling `branch` an
    # "unknown node kind" would send the caller to fix a documented kind.
    assert err.diagnostics[0].code == PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY


def test_process_call_allowed_in_a_branch_leg_only_in_path_mode():  # #141
    """ProcessCall inside a Branch leg is live-attested (capture §2.2), but
    ``process_call_connector_mixing`` stays gated PER PATH: a leg that uses
    ProcessCall may contain nothing else and must end in a stop."""
    ok = {
        "kind": "branch",
        "legs": [
            {"steps": [{"kind": "process_call", "process_ref": "x"}], "terminal": {"kind": "stop"}},
            {"steps": [message()], "terminal": {"kind": "stop"}},
        ],
    }
    ir = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [ok]}})
    assert ir.body.steps[0].legs[0].steps[0].kind == "process_call"

    mixed = {
        "kind": "branch",
        "legs": [
            {
                "steps": [{"kind": "process_call", "process_ref": "x"}, message()],
                "terminal": {"kind": "stop"},
            },
            {"steps": [message()], "terminal": {"kind": "stop"}},
        ],
    }
    err = parse_error({"version": "1", "body": {"kind": "sequence", "steps": [mixed]}})
    assert err.diagnostics[0].code == PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY

    not_stop = {
        "kind": "branch",
        "legs": [
            {"steps": [{"kind": "process_call", "process_ref": "x"}], "terminal": target()},
            {"steps": [message()], "terminal": {"kind": "stop"}},
        ],
    }
    err = parse_error({"version": "1", "body": {"kind": "sequence", "steps": [not_stop]}})
    assert err.diagnostics[0].code == PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY


def test_process_call_stays_out_of_the_decision_false_arm():  # #141
    """The capture attests ProcessCall on TRUE outcomes only."""
    bad = decision(
        false_arm={"steps": [{"kind": "process_call", "process_ref": "x"}], "terminal": {"kind": "stop"}}
    )
    err = parse_error(doc(source(), bad))
    assert err.diagnostics[0].code == PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY
    # ...and it names the exact authored slot, not the document root.
    assert err.diagnostics[0].path == "/body/steps/1/false_arm/steps/0"


def test_an_unknown_discriminator_is_still_an_unknown_node():
    """The body-slot translation must not swallow a genuinely unknown tag."""
    bad = branch(legs=[
        {"steps": [{"kind": "no_such_kind"}], "terminal": target()},
        {"steps": [], "terminal": target()},
    ])
    err = parse_error(doc(source(), bad))
    assert err.diagnostics[0].code == PROCESS_IR_SCHEMA_UNKNOWN_NODE


# ---------------------------------------------------------------------------
# connector_call sequences (issue #140)
# ---------------------------------------------------------------------------


def call(operation_ref="$ref:op", **extra):
    step = {"kind": "connector_call", "operation_ref": operation_ref}
    step.update(extra)
    return step


def test_connector_call_sequence_parses_with_multiple_calls_and_a_map():
    ir = parse_process_ir_v1(
        doc(
            call("$ref:op_a", action="GET", label="read"),
            {"kind": "map_ref", "map_ref": "$ref:m"},
            call("$ref:op_b"),
            call("$ref:op_c", action="Send"),
            {"kind": "stop"},
        )
    )
    assert [step.kind for step in ir.body.steps] == [
        "connector_call", "map_ref", "connector_call", "connector_call", "stop"
    ]


def test_connector_call_action_is_optional_and_defaults_to_none():
    ir = parse_process_ir_v1(doc(call(), {"kind": "stop"}))
    assert ir.body.steps[0].action is None
    assert ir.body.steps[0].label is None


def test_connector_call_return_documents_terminal_is_allowed():
    parse_process_ir_v1(doc(call(), {"kind": "return_documents"}))


@pytest.mark.parametrize(
    "steps,expect_code",
    [
        # a map has no producer before it
        ([{"kind": "map_ref", "map_ref": "$ref:m"}, call(), {"kind": "stop"}],
         PROCESS_IR_SCHEMA_INVALID_CARDINALITY),
        # a trailing map has no consumer after it, so its target profile is
        # unverifiable — the whole reason maps must be bracketed
        ([call(), {"kind": "map_ref", "map_ref": "$ref:m"}, {"kind": "stop"}],
         PROCESS_IR_SCHEMA_INVALID_CARDINALITY),
        # two maps in a row: the first one's target has no call to check against
        ([call(), {"kind": "map_ref", "map_ref": "$ref:m"},
          {"kind": "map_ref", "map_ref": "$ref:m2"}, call(), {"kind": "stop"}],
         PROCESS_IR_SCHEMA_INVALID_CARDINALITY),
        # no terminal at all
        ([call(), call()], PROCESS_IR_SCHEMA_INVALID_CARDINALITY),
        # a target/stop terminal pair belongs to the legacy dialect
        ([call(), target(), {"kind": "stop"}], PROCESS_IR_CAPABILITY_UNSUPPORTED),
        ([source(), call(), {"kind": "stop"}], PROCESS_IR_CAPABILITY_UNSUPPORTED),
        # process_call mixing stays gated (that is what mixed_connector_execution names)
        ([call(), {"kind": "process_call", "process_ref": "p"}, {"kind": "stop"}],
         PROCESS_IR_CAPABILITY_UNSUPPORTED),
        # every other linear kind stays out of a connector_call sequence for now
        ([call(), message(), call(), {"kind": "stop"}], PROCESS_IR_CAPABILITY_UNSUPPORTED),
        ([call(), {"kind": "cache_put", "cache_ref": "$ref:c"}, call(), {"kind": "stop"}],
         PROCESS_IR_CAPABILITY_UNSUPPORTED),
    ],
)
def test_connector_call_sequence_rules(steps, expect_code):
    err = parse_error(doc(*steps))
    assert err.diagnostics[0].code == expect_code, codes_of(err)


@pytest.mark.parametrize("control", ["branch", "decision"])
def test_a_connector_call_sequence_may_terminate_in_a_control(control):
    """#141 widened this terminal set.

    The legacy connector flow has ALWAYS admitted a terminal control, and without
    the same allowance a connector-call flow could not fan out at all — the
    issue's divergent-fixture criterion (sibling legs running different connector
    families off ONE shared entry call) would be unbuildable, since the only
    alternative is duplicating the entry call into every leg, which is a
    different process.
    """
    node = (
        branch(legs=[
            {"steps": [message()], "terminal": {"kind": "stop"}},
            {"steps": [message()], "terminal": {"kind": "stop"}},
        ])
        if control == "branch"
        else decision(
            true_arm={"steps": [message()], "terminal": {"kind": "stop"}},
            false_arm={"steps": [], "terminal": {"kind": "stop"}},
        )
    )
    ir = parse_process_ir_v1(doc(call(), node))
    assert ir.body.steps[-1].kind == control


def test_a_connector_call_sequence_still_rejects_an_exception_terminal():
    """``exception`` is a legacy terminal control with no #141 construct behind
    it, so the widening deliberately stops short of it."""
    err = parse_error(doc(call(), exception()))
    assert err.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID_CARDINALITY


def test_a_map_may_not_directly_precede_a_control_terminal():
    """#140's bracketing guarantee survives the widening.

    A map immediately before the control has no downstream call in the ROOT body
    to check its target profile against — the exact continuity hole map
    bracketing exists to close — so it stays rejected.
    """
    err = parse_error(
        doc(
            call(),
            {"kind": "map_ref", "map_ref": "$ref:m"},
            branch(legs=[
                {"steps": [message()], "terminal": {"kind": "stop"}},
                {"steps": [message()], "terminal": {"kind": "stop"}},
            ]),
        )
    )
    assert err.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID_CARDINALITY


def test_connector_call_is_authorable_in_control_bodies():  # #141
    """``connector_call_in_control_body`` is SUPPORTED as of #141: a call may sit
    in a Branch leg and in either Decision arm. The ROOT sequence rules are
    unchanged — a connector_call root still may not mix with source/target."""
    ok = branch(legs=[
        {"steps": [call()], "terminal": target()},
        {"steps": [], "terminal": target()},
    ])
    ir = parse_process_ir_v1(doc(source(), ok))
    assert ir.body.steps[-1].legs[0].steps[0].kind == "connector_call"

    ok_true = decision(true_arm={"steps": [call()], "terminal": target()})
    ir = parse_process_ir_v1(doc(source(), ok_true))
    assert ir.body.steps[-1].true_arm.steps[0].kind == "connector_call"

    ok_false = decision(false_arm={"steps": [call()], "terminal": {"kind": "stop"}})
    ir = parse_process_ir_v1(doc(source(), ok_false))
    assert ir.body.steps[-1].false_arm.steps[0].kind == "connector_call"


@pytest.mark.parametrize(
    "field",
    ["connection_ref", "connector_type", "action_type", "config", "profile_ref"],
)
def test_connector_call_rejects_derived_and_free_form_fields(field):
    """A ConnectorCall authors ONE reference and an optional assertion. Anything
    that would duplicate compiler-derived authority (a connection, a family, an
    action type, a config bag) is an unknown field."""
    err = parse_error(doc(call(**{field: "x"}), {"kind": "stop"}))
    assert err.diagnostics[0].code == PROCESS_IR_SCHEMA_UNKNOWN_FIELD
    assert err.diagnostics[0].path == "/body/steps/0/{0}".format(field)


@pytest.mark.parametrize("secret_field", ["password", "auth_token", "client_secret"])
def test_connector_call_secret_shaped_fields_are_capability_gated_not_unknown(secret_field):
    """A secret-shaped key must hit the pre-parse secret scan (which names the
    path but never the value), not the generic unknown-field path."""
    err = parse_error(doc(call(**{secret_field: "s3cret"}), {"kind": "stop"}))
    assert err.diagnostics[0].code == PROCESS_IR_CAPABILITY_UNSUPPORTED
    assert "s3cret" not in str(err)


@pytest.mark.parametrize("blank", ["", "   ", "\t"])
def test_connector_call_blank_action_is_rejected(blank):
    err = parse_error(doc(call(action=blank), {"kind": "stop"}))
    assert err.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID_CARDINALITY
    assert err.diagnostics[0].path == "/body/steps/0/action"


def test_connector_call_operation_ref_obeys_the_reference_grammar():
    err = parse_error(doc(call("$ref: spaced"), {"kind": "stop"}))
    assert err.diagnostics[0].code == PROCESS_IR_REFERENCE_INVALID_FORMAT


def test_legacy_sequences_are_untouched_by_the_connector_call_branch():
    """Guard the guard: the #140 branch is only entered when a connector_call is
    present, so every legacy shape must still parse exactly as before."""
    parse_process_ir_v1(doc(source(), {"kind": "map_ref", "map_ref": "$ref:m"}, target(), {"kind": "stop"}))
    parse_process_ir_v1(doc(source(), {"kind": "return_documents"}))
    parse_process_ir_v1(doc({"kind": "process_call", "process_ref": "$ref:p"}, {"kind": "stop"}))


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
        "source", "target", "connector_call", "flow_control", "message", "map_ref",
        "data_process", "cache_put", "document_cache_retrieve", "cache_get",
        "cache_remove", "set_ddp", "set_dpp", "process_call", "branch", "decision",
        "try_catch",  # #142
        "exception", "stop", "return_documents",
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
    # #140 shipped ConnectorCall AND many calls per linear path — both of the
    # things ADR-001 §8 lists under its ownership. The name
    # ``mixed_connector_execution`` used to be overloaded (ADR-001 §8 read it as
    # "multiple connector calls per path"; PROCESS_IR_V1 §3's sequence rules read
    # it as process_call x connector mixing), so #140 SPLIT it rather than pick
    # one meaning and silently redefine the flag.
    assert PROCESS_IR_V1_CAPABILITIES["generalized_connector_call"] == "supported"
    assert PROCESS_IR_V1_CAPABILITIES["mixed_connector_execution"] == "supported"
    # ...and the two constructs #140 does NOT ship each have their own name now.
    assert PROCESS_IR_V1_CAPABILITIES["process_call_connector_mixing"] == "gated"
    # #141 M12.6 flipped exactly two flags. ``process_call_connector_mixing``
    # deliberately stays GATED even though ProcessCall is now authorable in a
    # control body: PATH MODE means no root-to-leaf path ever carries both a
    # ProcessCall and connector execution, and sibling legs are independent paths
    # rather than a mix.
    assert PROCESS_IR_V1_CAPABILITIES["connector_call_in_control_body"] == "supported"
    assert PROCESS_IR_V1_CAPABILITIES["rich_branch_decision_bodies"] == "supported"
    # Continuation is the one #141 names and does NOT ship — terminal fan-out only.
    assert PROCESS_IR_V1_CAPABILITIES["continuation_after_branch_or_decision"] == "gated"
    assert PROCESS_IR_V1_CAPABILITIES["joins"] == "gated"
    assert PROCESS_IR_V1_CAPABILITIES["loops"] == "gated"
    # #142 M12.7 flipped scoped_try_catch and added two more supported rows.
    assert PROCESS_IR_V1_CAPABILITIES["scoped_try_catch"] == "supported"
    assert PROCESS_IR_V1_CAPABILITIES["bounded_retry"] == "supported"
    assert PROCESS_IR_V1_CAPABILITIES["typed_idempotency_evidence"] == "supported"
    # #142's three UNSUPPORTED rows mean "never", not "not yet" — the same sense
    # as caller_authored_cfg_edges. Pinning them here is what stops a later slice
    # from quietly downgrading an impossibility into a promise of future work:
    # error-type lists have no wire representation at all, the retry wait
    # schedule is platform-owned with no authorable field, and no queue component
    # exists to model (.codex/plans/issue-142-live-captures.md §G2/§G1/§G5).
    assert PROCESS_IR_V1_CAPABILITIES["catch_error_type_lists"] == "unsupported"
    assert PROCESS_IR_V1_CAPABILITIES["retry_backoff_authoring"] == "unsupported"
    assert PROCESS_IR_V1_CAPABILITIES["queue_topology"] == "unsupported"
    # ...while these four are genuinely "not yet", each blocked on a different
    # missing thing (see the manifest comments).
    assert PROCESS_IR_V1_CAPABILITIES["catch_failure_trigger_selection"] == "gated"
    assert PROCESS_IR_V1_CAPABILITIES["verified_write_replay_safety"] == "gated"
    assert PROCESS_IR_V1_CAPABILITIES["listener_error_scope"] == "gated"
    assert PROCESS_IR_V1_CAPABILITIES["nested_try_catch"] == "gated"
    assert set(PROCESS_IR_V1_CAPABILITIES.values()) <= {
        "supported",
        "gated",
        "unsupported",
    }
    assert sorted(
        name
        for name, state in PROCESS_IR_V1_CAPABILITIES.items()
        if state == "supported"
    ) == [
        "bounded_retry",
        "connector_call_in_control_body",
        "generalized_connector_call",
        "mixed_connector_execution",
        "rich_branch_decision_bodies",
        "scoped_try_catch",
        "typed_idempotency_evidence",
    ]


# ---------------------------------------------------------------------------
# #146 amendment: the served schema is published contract text for an LLM
# ---------------------------------------------------------------------------


def test_every_process_ir_def_has_a_non_empty_description():
    """A ``$def`` with no description teaches a caller nothing.

    20 of the 39 shipped undescribed at 845bda1 — including the three control
    nodes whose behaviour a caller most needs (Branch, Decision, Flow Control).
    The schema is SERVED, so an undescribed node is a node the caller has to
    guess at or discover by failing a compile.
    """
    defs = process_ir_v1_json_schema()["$defs"]
    assert len(defs) == 39
    undescribed = sorted(name for name, body in defs.items() if not body.get("description"))
    assert undescribed == []


def test_no_served_description_cites_an_unserved_repository_artifact():
    """A remediation a caller cannot fetch is a remediation that does not exist.

    The capture ledgers under ``.codex/plans/`` and the pages under
    ``docs/architecture/`` reach no MCP tool, and neither does the
    capability manifest by its Python name. Evidence pointers belong in
    comments — which are not served — and callers are pointed at
    ``process_ir_authoring`` entry ids instead.
    """
    blob = json.dumps(process_ir_v1_json_schema())
    for artifact in (".codex/", "docs/architecture", "PROCESS_IR_V1_CAPABILITIES"):
        assert artifact not in blob, artifact


def test_branch_description_states_ordered_sequential_leg_execution():
    """The word the schema never said.

    "sequential" and "parallel" both occurred ZERO times in the served schema at
    845bda1, so nothing told a caller whether Branch legs race. They do not —
    and the ordering is load-bearing, because execution-scoped state written in
    an earlier leg is visible in a later one.
    """
    branch = process_ir_v1_json_schema()["$defs"]["BranchNodeV1"]["description"]
    # Collapse the docstring's own line wrapping: a phrase that happens to
    # straddle a newline is still the phrase the caller reads.
    lowered = " ".join(branch.lower().split())
    assert "sequential" in lowered
    assert "order" in lowered
    assert "never parallel" in lowered
    # The depth bound is OURS, not the platform's — a caller told otherwise
    # would go looking for a Boomi setting that does not exist.
    assert "compiler bound" in lowered
    assert "not a boomi platform limit" in lowered


def test_flow_control_description_states_batching_without_configurable_parallelism():
    """Documents-per-batch is the only authorable mode.

    Deliberately NOT the stronger claim "threading is off": emission fixes the
    mode with zero parallel chunks, which proves non-configurability, not that
    no threading exists anywhere. Splitting and combining are separate explicit
    ``data_process`` operations and are named as such.
    """
    flow = process_ir_v1_json_schema()["$defs"]["FlowControlNodeV1"]["description"]
    lowered = " ".join(flow.lower().split())
    assert "batch" in lowered
    assert "for_each_count" in lowered
    assert "no caller-configurable parallel" in lowered
    assert "split_documents" in lowered and "combine_documents" in lowered
    assert "threading" not in lowered
