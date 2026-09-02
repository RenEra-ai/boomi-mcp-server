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
    PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED,
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
    # #175: a process-call root is the EXACT SINGLETON — the call ends its own
    # path, so the chain-plus-terminal form this test used before is now a
    # continuation request (see test_sequence_ordering_rules). Both field
    # variants are still covered, one document each, so the defaults and the
    # non-defaults keep their round-trip coverage.
    defaults = parse_process_ir_v1(doc({"kind": "process_call", "process_ref": "$ref:child"}))
    call = defaults.body.steps[0]
    assert call.wait is True and call.abort_on_error is False and call.label is None
    assert ProcessIRV1.model_validate(defaults.model_dump(mode="json")) == defaults

    explicit = parse_process_ir_v1(
        doc(
            {
                "kind": "process_call",
                "process_ref": "lit-id",
                "wait": False,
                "abort_on_error": True,
                "label": "L",
            }
        )
    )
    call = explicit.body.steps[0]
    assert call.wait is False and call.abort_on_error is True and call.label == "L"
    assert ProcessIRV1.model_validate(explicit.model_dump(mode="json")) == explicit


def test_linear_return_documents_is_a_standalone_terminal():
    # Legacy parity: with return_documents enabled the builder emits ONLY the
    # returndocuments terminal after the sequence — the configured target is
    # dead and is NOT represented in IR (_target_terminal_entries).
    ir = parse_process_ir_v1(doc(source(), message(), {"kind": "return_documents", "label": "out"}))
    assert ir.body.steps[-1].kind == "return_documents"


def test_target_followed_by_return_documents_is_a_terminal_suffix():
    """#154 item 3. The legacy builder goldens ``return_documents_terminal``
    with a routed target ahead of it, so ``target + return_documents`` is a
    terminal PAIR exactly as ``target + stop`` is."""
    ir = parse_process_ir_v1(doc(source(), message(), target(), {"kind": "return_documents"}))
    assert [step.kind for step in ir.body.steps] == [
        "source", "message", "target", "return_documents",
    ]
    # the bare form keeps working too
    assert parse_process_ir_v1(doc(source(), target(), {"kind": "return_documents"}))


@pytest.mark.parametrize(
    "steps",
    [
        # target is a TERMINAL suffix, not a freely movable linear step: it may
        # only sit immediately before its stop/return_documents.
        (source(), target(), message(), {"kind": "return_documents"}),
        (source(), target(), target(), {"kind": "return_documents"}),
        (source(), message(), {"kind": "return_documents"}, target()),
        (source(), target()),
    ],
)
def test_target_keeps_its_positional_meaning(steps):
    err = parse_error(doc(*steps))
    assert err.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID_CARDINALITY, codes_of(err)


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
        # #175 F5. process_call genuinely mixed with a CONNECTOR keeps the mixing
        # code and its precedence: that pairing stays gated on its own terms even
        # once return-path binding lands, so it must not be reported as a
        # return-path problem. (Before #175 this row used `message()`, which is
        # not a connector at all — the old validator raised the mixing code for
        # any non-process_call neighbour, so the case never tested what its label
        # claimed.)
        (
            [{"kind": "process_call", "process_ref": "x"}, target(), {"kind": "stop"}],
            PROCESS_IR_CAPABILITY_UNSUPPORTED,
        ),
        # #175 F4/F2. A non-connector neighbour is a CONTINUATION request, and now
        # reports as one: the call ends its path, so neither the message nor the
        # trailing stop can follow it.
        (
            [{"kind": "process_call", "process_ref": "x"}, message(), {"kind": "stop"}],
            PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED,
        ),
        (
            [{"kind": "process_call", "process_ref": "x"}, {"kind": "stop"}],
            PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED,
        ),
        # #175 F3. Same for a return_documents terminal: the wrapper "call a child,
        # then return documents" shape needs the child to hand control back, which
        # is exactly the gated binding.
        (
            [{"kind": "process_call", "process_ref": "x"}, {"kind": "return_documents"}],
            PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED,
        ),
        # #175 F4. A CHAIN of calls: every call but the last would need its child's
        # return paths to reach the next one.
        (
            [
                {"kind": "process_call", "process_ref": "x"},
                {"kind": "process_call", "process_ref": "y"},
                {"kind": "stop"},
            ],
            PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED,
        ),
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


def test_process_call_is_a_branch_leg_terminal_not_a_step():  # #141, amended by #175
    """ProcessCall inside a Branch leg is live-attested (capture §2.2) — as the END
    of the leg, which is where #175 moved it.

    #141 read the capture as "a leg of process_call STEPS ending in a stop". The
    capture attests the control edge landing ON a call; the trailing stop was an
    inference, and it is the shape the platform declines to draw. So the call is
    the leg's TERMINAL, with an empty step prefix, and the old step form is now a
    continuation request.
    """
    ok = {
        "kind": "branch",
        "legs": [
            {"steps": [], "terminal": {"kind": "process_call", "process_ref": "x"}},
            {"steps": [message()], "terminal": {"kind": "stop"}},
        ],
    }
    ir = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [ok]}})
    assert ir.body.steps[0].legs[0].terminal.kind == "process_call"
    assert ir.body.steps[0].legs[0].steps == []

    # F6 — the pre-#175 form: a call as a STEP, wired onward to a stop.
    as_step = {
        "kind": "branch",
        "legs": [
            {"steps": [{"kind": "process_call", "process_ref": "x"}], "terminal": {"kind": "stop"}},
            {"steps": [message()], "terminal": {"kind": "stop"}},
        ],
    }
    err = parse_error({"version": "1", "body": {"kind": "sequence", "steps": [as_step]}})
    assert (
        err.diagnostics[0].code
        == PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED
    ), codes_of(err)

    # F8 — a terminal call with a non-connector prefix. The prefix is unattested:
    # the capture shows the control edge landing directly on the call.
    with_prefix = {
        "kind": "branch",
        "legs": [
            {"steps": [message()], "terminal": {"kind": "process_call", "process_ref": "x"}},
            {"steps": [message()], "terminal": {"kind": "stop"}},
        ],
    }
    err = parse_error({"version": "1", "body": {"kind": "sequence", "steps": [with_prefix]}})
    assert (
        err.diagnostics[0].code
        == PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED
    ), codes_of(err)


def test_a_connector_prefix_before_a_terminal_process_call_keeps_the_mixing_code():  # #175 F11
    """Moving the call to the terminal slot made ``steps=[connector_call],
    terminal=process_call`` REPRESENTABLE for the first time — a shape the #141
    step-only mixing check could never have seen.

    It must keep reporting as MIXING, not as a return-path problem: the pairing
    stays gated on its own terms even once return-path binding lands, so telling
    the caller to "remove what follows the call" would send them the wrong way.
    """
    mixed = {
        "kind": "branch",
        "legs": [
            {
                "steps": [{"kind": "connector_call", "operation_ref": "$ref:op"}],
                "terminal": {"kind": "process_call", "process_ref": "x"},
            },
            {"steps": [message()], "terminal": {"kind": "stop"}},
        ],
    }
    err = parse_error({"version": "1", "body": {"kind": "sequence", "steps": [mixed]}})
    assert err.diagnostics[0].code == PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY, codes_of(err)


def test_a_connector_above_a_terminal_process_call_is_still_mixing():  # #175 F12
    """The ANCESTOR half of the mixing gate has to look at the terminal too.

    ``source -> branch -> leg(terminal=process_call)`` puts a connector genuinely
    upstream of a call on the same root-to-leaf path. Before #175 the whole-document
    walk only inspected each body's ``steps``, so moving the call to the terminal
    would have slipped straight past it — widening the slot would have opened the
    exact hole #141 gated.
    """
    bad = {
        "kind": "branch",
        "legs": [
            {"steps": [], "terminal": {"kind": "process_call", "process_ref": "x"}},
            {"steps": [message()], "terminal": target()},
        ],
    }
    err = parse_error(doc(source(), bad))
    assert err.diagnostics[0].code == PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY, codes_of(err)
    assert err.diagnostics[0].path.endswith("/terminal"), err.diagnostics[0].path


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
        # #154 item 5 admits the LINEAR vocabulary between calls, but the
        # sequence must still end ON a call: steps after the last call would run
        # on documents no further call consumes.
        ([call(), call(), message(), {"kind": "stop"}], PROCESS_IR_CAPABILITY_UNSUPPORTED),
        ([{"kind": "set_dpp", "name": "p",
           "source_values": [{"value_type": "static", "value": "v"}]},
          {"kind": "stop"}],
         PROCESS_IR_SCHEMA_INVALID_CARDINALITY),
        # cache_put is admitted into the vocabulary, so its ADJACENCY rule has to
        # reach this branch too — it did not before #154.
        ([call(), {"kind": "cache_put", "cache_ref": "$ref:c"}, call(), {"kind": "stop"}],
         PROCESS_IR_SCHEMA_INVALID_CARDINALITY),
        # a map still needs a call on BOTH sides; with a linear prefix admitted,
        # the predecessor half is no longer implied by "step 0 is a call".
        ([{"kind": "set_dpp", "name": "p",
           "source_values": [{"value_type": "static", "value": "v"}]},
          {"kind": "map_ref", "map_ref": "$ref:m"}, call(), {"kind": "stop"}],
         PROCESS_IR_SCHEMA_INVALID_CARDINALITY),
    ],
)
def test_connector_call_sequence_rules(steps, expect_code):
    err = parse_error(doc(*steps))
    assert err.diagnostics[0].code == expect_code, codes_of(err)


@pytest.mark.parametrize(
    "steps",
    [
        # the issue's own probe shape
        (call(), {"kind": "set_ddp", "name": "d",
                  "source_values": [{"value_type": "static", "value": "v"}]},
         call(), {"kind": "stop"}),
        # a linear PREFIX: property preparation before the entry read
        ({"kind": "set_dpp", "name": "p",
          "source_values": [{"value_type": "static", "value": "v"}]},
         call(), call(), {"kind": "stop"}),
        (call(), message(), call(), {"kind": "stop"}),
        # cache_put admitted, with its adjacency rule satisfied
        (call(), {"kind": "cache_put", "cache_ref": "$ref:c"},
         {"kind": "cache_get", "cache_ref": "$ref:c"}, call(), {"kind": "stop"}),
    ],
)
def test_linear_steps_are_admitted_before_and_between_connector_calls(steps):
    """#154 item 5. The legacy builder emits Set Properties around its reads;
    a generalized call sequence could not express that at all before this."""
    ir = parse_process_ir_v1(doc(*steps))
    assert ir.body.steps[-1].kind == "stop"


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
    # #175: the process-call root is now the exact singleton — the trailing stop
    # this line used to carry is the continuation the platform never honoured.
    parse_process_ir_v1(doc({"kind": "process_call", "process_ref": "$ref:p"}))


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
    # #175: the process-call root is the exact singleton. The pre-#175 document
    # chained two calls into a return_documents terminal — a form the platform
    # never honoured, because each call's outbound edge would need its child's
    # return paths. Every FIELD variant it covered is kept: the literal component
    # id, and non-default wait/abort_on_error/label all ride on the one call.
    wrapper_flow = parse_process_ir_v1(
        doc(
            {
                "kind": "process_call",
                "process_ref": "00000000-0000-0000-0000-000000000001",
                "wait": False,
                "abort_on_error": True,
                "label": "second",
            }
        )
    )
    # ...and the `return_documents` terminal keeps its own canonical document
    # rather than losing coverage along with the chain that used to carry it.
    # It is the CHILD-side capability — how a called process hands documents
    # back — which is exactly the half #175 leaves intact.
    return_documents_flow = parse_process_ir_v1(
        doc(source(), message("collect"), {"kind": "return_documents", "label": "out"})
    )
    return {
        "linear_flow": linear_flow,
        "control_flow": control_flow,
        "wrapper_flow": wrapper_flow,
        "return_documents_flow": return_documents_flow,
    }


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
    # ...while these three are genuinely "not yet", each blocked on a different
    # missing thing (see the manifest comments).
    assert PROCESS_IR_V1_CAPABILITIES["catch_failure_trigger_selection"] == "gated"
    # #155 slice F. Flipped and withdrawn twice before this stuck: the authored
    # shape that reaches the evidence is reference-only reuse, and two retention
    # defects had to be fixed before it compiled.
    assert PROCESS_IR_V1_CAPABILITIES["verified_write_replay_safety"] == "supported"
    assert PROCESS_IR_V1_CAPABILITIES["listener_error_scope"] == "gated"
    assert PROCESS_IR_V1_CAPABILITIES["nested_try_catch"] == "gated"
    # #175. The pair is the whole point: the non-returning form is what V1 emits,
    # and the returning form is gated rather than absent, so a caller reading the
    # manifest can tell "this repo will not do it" from "this repo cannot do it
    # yet". Binding a returning child's return paths needs that child's compiled
    # shapes, which is a cross-component late binding (#176).
    assert PROCESS_IR_V1_CAPABILITIES["terminal_process_call"] == "supported"
    assert PROCESS_IR_V1_CAPABILITIES["process_call_return_path_binding"] == "gated"
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
        "dynamic_path",
        "generalized_connector_call",
        "mixed_connector_execution",
        "rich_branch_decision_bodies",
        "scoped_try_catch",
        "source_replay_policy",
        "terminal_process_call",
        "typed_idempotency_evidence",
        "verified_write_replay_safety",
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
    assert len(defs) == 40
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


@pytest.mark.parametrize(
    "steps,expected_pointer,label",
    [
        ([{"kind": "process_call", "process_ref": "a"},
          {"kind": "process_call", "process_ref": "b"}],
         "/body/steps/1", "an all-call chain with NO trailing terminal"),
        ([{"kind": "process_call", "process_ref": "a"},
          {"kind": "process_call", "process_ref": "b"},
          {"kind": "process_call", "process_ref": "c"}],
         "/body/steps/1", "three calls, no terminal"),
        ([{"kind": "process_call", "process_ref": "a"},
          {"kind": "process_call", "process_ref": "b"},
          {"kind": "stop"}],
         "/body/steps/1", "a call chain WITH a trailing stop"),
        ([message(), {"kind": "process_call", "process_ref": "a"}],
         "/body/steps/0", "a step BEFORE the call"),
    ],
)
def test_every_illegal_process_call_root_returns_a_typed_diagnostic(
    steps, expected_pointer, label
):
    """#175 Stage-2: an unsupported root must never escape as an exception.

    The offending index was first written as "the first step that is not a
    process_call". An ALL-CALL chain has no such step, so `next(...)` raised
    `StopIteration` straight out of the model validator — and
    `parse_process_ir_v1` catches only `ValidationError`, so caller input escaped
    as an unhandled exception instead of the typed refusal. No test covered a
    call chain without a trailing terminal, which is why the suite stayed green.

    The selection is written against the first call's INDEX instead, which is
    total: the legal singleton returns earlier, so at least two steps remain and
    an index other than the first call always exists.

    The pointer matters too, not just the absence of a crash: `[pc, pc, stop]`
    previously pointed at the trailing stop, but removing the stop would not fix
    the document — the second CALL is the first thing that may not follow a call.
    """
    err = parse_error({"version": "1", "body": {"kind": "sequence", "steps": steps}})
    assert (
        err.diagnostics[0].code
        == PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED
    ), (label, codes_of(err))
    assert err.diagnostics[0].path == expected_pointer, (label, codes_of(err))


# ---------------------------------------------------------------------------
# Issue #155 — the served reference list is DERIVED from these models
# ---------------------------------------------------------------------------
#
# The authoring contract used to carry a hand-written table of which references
# each node kind requires. It went stale exactly the way a second copy of a fact
# does: #155 added `path_binding.request_profile_ref` to two node kinds and the
# table said nothing about it, so the served answer was silently incomplete
# about the very field that slice shipped. The table is gone; these tests pin
# the walker that replaced it.


def _ref_paths():
    from boomi_mcp.models.process_ir import process_ir_v1_node_reference_paths

    return process_ir_v1_node_reference_paths()


def test_the_derived_reference_table_covers_exactly_the_node_vocabulary():
    """Totality against the authority, not against a list written beside it."""
    from boomi_mcp.models.process_ir import process_ir_v1_node_kinds

    assert set(_ref_paths()) == set(process_ir_v1_node_kinds())


@pytest.mark.parametrize("kind", ["connector_call", "target"])
def test_the_connector_path_binding_profile_is_served(kind):
    """The regression that motivated deriving the list at all.

    Both node kinds that accept a path binding can carry a profile reference
    inside it, and neither must: a binding whose segment needs no profile is
    valid. A caller reading the contract has to be told the field exists.
    """
    assert ("path_binding.request_profile_ref", False) in _ref_paths()[kind]


def test_a_directly_held_reference_keeps_its_bare_name_and_required_flag():
    """A top-level required ref is served exactly as it always was."""
    paths = _ref_paths()
    assert ("operation_ref", True) in paths["connector_call"]
    assert ("connection_ref", True) in paths["target"]
    assert ("map_ref", True) in paths["map_ref"]


def test_a_reference_reached_through_a_list_is_served_once_and_not_required():
    """`steps.profile_ref` — indices elided, and never promised."""
    assert _ref_paths()["data_process"] == (("steps.profile_ref", False),)


@pytest.mark.parametrize(
    "annotation, expected_required",
    [
        ("required", True),
        ("optional", False),
    ],
)
def test_adding_a_reference_to_a_model_grows_the_served_list(
    annotation, expected_required, monkeypatch
):
    """NON-VACUITY: the walker reads the models, it does not recite a constant.

    A field planted on a node kind must appear, with the `required` flag its
    annotation implies. Without this the whole table could be a frozen literal
    and every other test here would still pass.
    """
    from typing import Optional

    from pydantic.fields import FieldInfo

    from boomi_mcp.models.process_ir import ComponentRefV1, StopNodeV1

    assert _ref_paths()["stop"] == ()

    planted = FieldInfo.from_annotation(
        ComponentRefV1 if annotation == "required" else Optional[ComponentRefV1]
    )
    fields = dict(StopNodeV1.model_fields)
    fields["planted_ref"] = planted
    monkeypatch.setattr(StopNodeV1, "model_fields", fields)

    assert _ref_paths()["stop"] == (("planted_ref", expected_required),)


def test_the_walk_stops_at_nested_nodes_and_that_rule_is_load_bearing():
    """The stop rule is what keeps a Branch's entry about the BRANCH.

    A Branch leg holds steps, and those steps are node kinds with their own
    contract entries. Attributing their references upward would tell a caller
    that authoring a Branch requires an operation reference, a process
    reference, a map reference — every reference in the IR, reachable through
    two legs of arbitrary content.

    The control is a deliberately naive walk written here, independent of the
    implementation: it recurses through everything and is asserted to find the
    leak the real walker excludes. Without it "branch has no references" would
    be indistinguishable from a walker that found nothing at all.
    """
    from typing import Union, get_args, get_origin

    from pydantic import BaseModel

    from boomi_mcp.models.process_ir import BranchNodeV1, _is_component_ref_field

    def naive(model, prefix, seen):
        for name, field_info in model.model_fields.items():
            if _is_component_ref_field(field_info):
                yield "{0}{1}".format(prefix, name)
                continue
            pending, reachable = [field_info.annotation], []
            while pending:
                current = pending.pop()
                if isinstance(current, type) and issubclass(current, BaseModel):
                    reachable.append(current)
                    continue
                pending.extend(a for a in get_args(current) if a is not type(None))
            for child in reachable:
                if child in seen:
                    continue
                yield from naive(child, "{0}{1}.".format(prefix, name), seen + (child,))

    leaked = set(naive(BranchNodeV1, "", (BranchNodeV1,)))
    assert len(leaked) >= 20, sorted(leaked)
    assert any(path.endswith("operation_ref") for path in leaked)
    assert _ref_paths()["branch"] == ()


def test_the_served_contract_agrees_with_the_derived_table():
    """Bidirectional pin: the projection may not carry its own answer.

    The frozen contract snapshot would catch a change in the served bytes, but
    not a projection that quietly stopped consulting the models and started
    hand-listing again — the bytes could still match on the day of the edit.
    """
    from boomi_mcp.authoring.process_ir_projection import (
        build_process_ir_authoring_entries,
        collect_projection_sources,
    )

    derived = _ref_paths()
    served = {
        entry.subject: tuple((ref.field, ref.required) for ref in entry.required_references)
        for entry in build_process_ir_authoring_entries(collect_projection_sources())
        if entry.entry_type == "node"
    }
    assert served == derived


def test_the_property_write_walk_derives_its_models_from_the_shared_validator():
    """The set of property-writing kinds is READ, not listed (#155).

    Both writer kinds must be found, and the connector path binding must not be:
    it names a property another step wrote rather than writing one, and it is
    not a node. Asserting the exact set is what makes this a derivation rather
    than a coincidence — a predicate that matched everything would pass a test
    that only checked the two writers were present.
    """
    from pydantic import BaseModel

    from boomi_mcp.models import process_ir as m
    from boomi_mcp.models.process_ir import _property_name_field

    found = {}
    for attr in dir(m):
        obj = getattr(m, attr)
        if isinstance(obj, type) and issubclass(obj, BaseModel):
            field = _property_name_field(obj)
            if field is not None:
                found[obj.__name__] = (field, "kind" in obj.model_fields)

    assert found == {
        "ConnectorPathBindingV1": ("property_name", False),
        "SetDdpNodeV1": ("name", True),
        "SetDppNodeV1": ("name", True),
    }, found


def test_a_new_property_writing_kind_is_picked_up_with_no_edit():
    """NON-VACUITY for the derivation: membership is earned, not granted.

    A model that enforces the bare-name rule is walked because it enforces it.
    This is the property a hand-written class list does NOT have, and the reason
    the list was not written: it stays correct only until the next kind lands.
    """
    from pydantic import model_validator

    from boomi_mcp.models import process_ir as m
    from boomi_mcp.models.process_ir import _ProcessIRBase, iter_property_writes

    class _NewWriterNodeV1(_ProcessIRBase):
        kind: str = "invented_writer"
        name: str = "invented.bad"

        @model_validator(mode="after")
        def _name_rules(self):
            # Reached through the module, the way every real writer reaches it.
            m._validate_bare_property_name(self.name)
            return self

    node = _NewWriterNodeV1.model_construct(kind="invented_writer", name="invented.bad")
    assert list(iter_property_writes(node)) == [("/name", "invented.bad")]


def test_a_collection_of_references_is_recognised_and_walked():
    """QA-155-r6-01: the collection branch was unreachable, and said otherwise.

    `iter_component_refs` has always carried a branch for a field holding
    several references, under a comment promising a future
    `Tuple[ComponentRefV1, ...]` would need no edit. The predicate answered
    False for exactly that annotation, so the branch was dead and the promise
    was false — the same defect as the optional one, one container level over.

    No model field has this shape today, so the witness plants one. Without it
    the fix is unobservable: every real field is bare or optional, and the suite
    would stay green with the branch dead again.
    """
    from typing import Optional, Tuple

    from pydantic.fields import FieldInfo

    from boomi_mcp.models.process_ir import (
        ComponentRefV1,
        StopNodeV1,
        _is_component_ref_field,
        iter_component_refs,
    )

    for annotation in (Tuple[ComponentRefV1, ...], Optional[Tuple[ComponentRefV1, ...]]):
        assert _is_component_ref_field(FieldInfo.from_annotation(annotation)), annotation

    node = StopNodeV1.model_construct(kind="stop")
    object.__setattr__(node, "many_refs", ("$ref:A", "$ref:B"))
    fields = dict(StopNodeV1.model_fields)
    fields["many_refs"] = FieldInfo.from_annotation(Tuple[ComponentRefV1, ...])
    try:
        StopNodeV1.model_fields = fields
        assert list(iter_component_refs(node)) == [
            ("/many_refs/0", "$ref:A"),
            ("/many_refs/1", "$ref:B"),
        ]
    finally:
        StopNodeV1.model_fields = {k: v for k, v in fields.items() if k != "many_refs"}


#: Every annotation shape the reference predicate is asked about, with a runtime
#: value of that shape. The shapes are named for what QA measured about them: the
#: first version of the container fix admitted `Set`/`FrozenSet` the walk never
#: iterated, and rejected `Sequence`/`Iterable` that pydantic coerces to a list
#: the walk would have iterated. Both directions are pinned here.
_REF_SHAPES = [
    ("bare", "ComponentRefV1", "$ref:A"),
    ("optional", "Optional[ComponentRefV1]", "$ref:A"),
    ("tuple", "Tuple[ComponentRefV1, ...]", ("$ref:A", "$ref:B")),
    ("list", "List[ComponentRefV1]", ["$ref:A"]),
    ("sequence", "Sequence[ComponentRefV1]", ["$ref:A"]),
    ("deque", "Deque[ComponentRefV1]", ["$ref:A"]),
    ("optional_tuple", "Optional[Tuple[ComponentRefV1, ...]]", ("$ref:A",)),
    ("optional_sequence", "Optional[Sequence[ComponentRefV1]]", ["$ref:A"]),
]


@pytest.mark.parametrize("label, annotation_src, value", _REF_SHAPES)
def test_every_admitted_reference_shape_is_also_walkable(label, annotation_src, value):
    """QA-155-r7-01: the predicate and the walk must answer ONE question.

    The predicate reads ANNOTATIONS and the walk reads VALUES, so they cannot
    share code — and the first container fix therefore wrote the rule twice, as
    an origin list on one side and an isinstance tuple on the other. They
    disagreed immediately and in both directions, the worse of which advertises
    a reference in the served contract that the walk then never resolves.

    This test is the authority the two spellings answer to: for every shape the
    predicate admits, a real value of that shape must resolve through the walk.
    A future divergence fails here instead of in a QA round.
    """
    import typing

    from pydantic.fields import FieldInfo

    from boomi_mcp.models.process_ir import (
        ComponentRefV1,
        StopNodeV1,
        _is_component_ref_field,
        iter_component_refs,
    )

    namespace = dict(vars(typing))
    namespace["ComponentRefV1"] = ComponentRefV1
    annotation = eval(annotation_src, namespace)  # noqa: S307 - fixed table above

    assert _is_component_ref_field(FieldInfo.from_annotation(annotation)), annotation_src

    node = StopNodeV1.model_construct(kind="stop")
    object.__setattr__(node, "planted", value)
    fields = dict(StopNodeV1.model_fields)
    fields["planted"] = FieldInfo.from_annotation(annotation)
    try:
        StopNodeV1.model_fields = fields
        found = {ref for _path, ref in iter_component_refs(node)}
    finally:
        StopNodeV1.model_fields = {k: v for k, v in fields.items() if k != "planted"}

    expected = {value} if isinstance(value, str) else set(value)
    assert found == expected, (label, found, expected)


@pytest.mark.parametrize(
    "annotation_src",
    [
        "Tuple[Tuple[ComponentRefV1, ...], ...]",
        "Dict[str, ComponentRefV1]",
        "Mapping[str, ComponentRefV1]",
        # Pydantic validates this one to a LAZY iterator, so the walk would
        # consume it — see the re-iterability test below for why that is worse
        # than not recognising it at all.
        "Iterable[ComponentRefV1]",
        # Unordered: the walk's index IS the identity every consumer receives,
        # and these produce a different one per process.
        "Set[ComponentRefV1]",
        "FrozenSet[ComponentRefV1]",
    ],
)
def test_a_shape_the_walk_cannot_resolve_is_not_admitted(annotation_src):
    """The other half of the biconditional, and the direction that misleads.

    A nested collection is flattened one level by the walk and a mapping
    iterates its KEYS, so neither resolves to references. Admitting either
    would publish a reference the walk cannot find — silence would be better.
    """
    import typing

    from pydantic.fields import FieldInfo

    from boomi_mcp.models.process_ir import ComponentRefV1, _is_component_ref_field

    namespace = dict(vars(typing))
    namespace["ComponentRefV1"] = ComponentRefV1
    annotation = eval(namespace and annotation_src, namespace)  # noqa: S307
    assert not _is_component_ref_field(FieldInfo.from_annotation(annotation)), annotation_src


def test_the_widening_did_not_admit_a_shape_that_is_not_a_reference():
    """The other direction — a predicate that says yes to everything is worse.

    A heterogeneous tuple and a wide union are both refused: in the first the
    claim would be true of some positions and false of others, and in the second
    which arm a value took is not a schema fact. Both were reachable regressions
    of this widening, so both are pinned.
    """
    from typing import List, Optional, Tuple, Union

    from pydantic.fields import FieldInfo

    from boomi_mcp.models.process_ir import ComponentRefV1, _is_component_ref_field

    for annotation in (
        str,
        Optional[str],
        List[str],
        Tuple[ComponentRefV1, str],
        Union[ComponentRefV1, int],
    ):
        assert not _is_component_ref_field(FieldInfo.from_annotation(annotation)), annotation


@pytest.mark.parametrize("label, annotation_src, value", _REF_SHAPES)
def test_an_admitted_reference_field_survives_being_read(label, annotation_src, value):
    """QA-155-r8-01: the walk must not CONSUME the document it reads.

    This is why the container floor is `Collection` and not `Iterable`. Pydantic
    validates an `Iterable[...]` field to a lazy iterator, so the first of the
    four consumers of this walk would drain it and the other three would see an
    empty field — and the read would MUTATE the document, with nothing to
    detect it, because an exhausted iterator is still iterable.

    A collection is re-iterable by contract, so the property is asserted the
    only way it can be: read the same field four times and require the same
    answer. Reading once would pass on the broken tree.
    """
    import typing

    from pydantic.fields import FieldInfo

    from boomi_mcp.models.process_ir import (
        ComponentRefV1,
        StopNodeV1,
        iter_component_refs,
    )

    namespace = dict(vars(typing))
    namespace["ComponentRefV1"] = ComponentRefV1
    annotation = eval(annotation_src, namespace)  # noqa: S307 - fixed table above

    node = StopNodeV1.model_construct(kind="stop")
    object.__setattr__(node, "planted", value)
    fields = dict(StopNodeV1.model_fields)
    fields["planted"] = FieldInfo.from_annotation(annotation)
    try:
        StopNodeV1.model_fields = fields
        reads = [{ref for _p, ref in iter_component_refs(node)} for _ in range(4)]
    finally:
        StopNodeV1.model_fields = {k: v for k, v in fields.items() if k != "planted"}

    expected = {value} if isinstance(value, str) else set(value)
    assert reads == [expected] * 4, (label, reads)


def test_a_one_shot_value_is_refused_by_the_value_side_too():
    """The value-domain half of the same floor.

    The annotation side cannot catch everything — a field typed as a collection
    could still be handed a generator by `model_construct`, which performs no
    validation. Both spellings therefore carry the floor, and this pins the one
    that has no annotation to consult.
    """
    from boomi_mcp.models.process_ir import _is_walkable_collection_value

    for admitted in (["a"], ("a",)):
        assert _is_walkable_collection_value(admitted), admitted
    # A set is refused HERE too, not only in the annotation: the walk's index is
    # the identity its consumers receive, and set iteration order moves between
    # processes. Measured across four hash seeds: four different path-to-
    # reference maps, while the list control was identical in all four.
    for refused in ({"a"}, frozenset({"a"}), (x for x in "ab"), iter(["a"]),
                    "abc", b"ab", {"a": 1}):
        assert not _is_walkable_collection_value(refused), refused


#: Every annotation shape the reference predicate is asked about, with a runtime
#: value of that shape. The shapes are named for what QA measured about them: the
#: first version of the container fix admitted `Set`/`FrozenSet` the walk never
#: iterated, and rejected `Sequence`/`Iterable` that pydantic coerces to a list
#: the walk would have iterated. Both directions are pinned here.
_REF_SHAPES = [
    ("bare", "ComponentRefV1", "$ref:A"),
    ("optional", "Optional[ComponentRefV1]", "$ref:A"),
    ("tuple", "Tuple[ComponentRefV1, ...]", ("$ref:A", "$ref:B")),
    ("list", "List[ComponentRefV1]", ["$ref:A"]),
    ("sequence", "Sequence[ComponentRefV1]", ["$ref:A"]),
    ("deque", "Deque[ComponentRefV1]", ["$ref:A"]),
    ("optional_tuple", "Optional[Tuple[ComponentRefV1, ...]]", ("$ref:A",)),
    ("optional_sequence", "Optional[Sequence[ComponentRefV1]]", ["$ref:A"]),
]


@pytest.mark.parametrize("label, annotation_src, value", _REF_SHAPES)
def test_every_admitted_reference_shape_is_also_walkable(label, annotation_src, value):
    """QA-155-r7-01: the predicate and the walk must answer ONE question.

    The predicate reads ANNOTATIONS and the walk reads VALUES, so they cannot
    share code — and the first container fix therefore wrote the rule twice, as
    an origin list on one side and an isinstance tuple on the other. They
    disagreed immediately and in both directions, the worse of which advertises
    a reference in the served contract that the walk then never resolves.

    This test is the authority the two spellings answer to: for every shape the
    predicate admits, a real value of that shape must resolve through the walk.
    A future divergence fails here instead of in a QA round.
    """
    import typing

    from pydantic.fields import FieldInfo

    from boomi_mcp.models.process_ir import (
        ComponentRefV1,
        StopNodeV1,
        _is_component_ref_field,
        iter_component_refs,
    )

    namespace = dict(vars(typing))
    namespace["ComponentRefV1"] = ComponentRefV1
    annotation = eval(annotation_src, namespace)  # noqa: S307 - fixed table above

    assert _is_component_ref_field(FieldInfo.from_annotation(annotation)), annotation_src

    node = StopNodeV1.model_construct(kind="stop")
    object.__setattr__(node, "planted", value)
    fields = dict(StopNodeV1.model_fields)
    fields["planted"] = FieldInfo.from_annotation(annotation)
    try:
        StopNodeV1.model_fields = fields
        found = {ref for _path, ref in iter_component_refs(node)}
    finally:
        StopNodeV1.model_fields = {k: v for k, v in fields.items() if k != "planted"}

    expected = {value} if isinstance(value, str) else set(value)
    assert found == expected, (label, found, expected)


@pytest.mark.parametrize(
    "annotation_src",
    [
        "Tuple[Tuple[ComponentRefV1, ...], ...]",
        "Dict[str, ComponentRefV1]",
        "Mapping[str, ComponentRefV1]",
        # Pydantic validates this one to a LAZY iterator, so the walk would
        # consume it — see the re-iterability test below for why that is worse
        # than not recognising it at all.
        "Iterable[ComponentRefV1]",
        # Unordered: the walk's index IS the identity every consumer receives,
        # and these produce a different one per process.
        "Set[ComponentRefV1]",
        "FrozenSet[ComponentRefV1]",
    ],
)
def test_a_shape_the_walk_cannot_resolve_is_not_admitted(annotation_src):
    """The other half of the biconditional, and the direction that misleads.

    A nested collection is flattened one level by the walk and a mapping
    iterates its KEYS, so neither resolves to references. Admitting either
    would publish a reference the walk cannot find — silence would be better.
    """
    import typing

    from pydantic.fields import FieldInfo

    from boomi_mcp.models.process_ir import ComponentRefV1, _is_component_ref_field

    namespace = dict(vars(typing))
    namespace["ComponentRefV1"] = ComponentRefV1
    annotation = eval(namespace and annotation_src, namespace)  # noqa: S307
    assert not _is_component_ref_field(FieldInfo.from_annotation(annotation)), annotation_src


def test_the_widening_did_not_admit_a_shape_that_is_not_a_reference():
    """The other direction — a predicate that says yes to everything is worse.

    A heterogeneous tuple and a wide union are both refused: in the first the
    claim would be true of some positions and false of others, and in the second
    which arm a value took is not a schema fact. Both were reachable regressions
    of this widening, so both are pinned.
    """
    from typing import List, Optional, Tuple, Union

    from pydantic.fields import FieldInfo

    from boomi_mcp.models.process_ir import ComponentRefV1, _is_component_ref_field

    for annotation in (
        str,
        Optional[str],
        List[str],
        Tuple[ComponentRefV1, str],
        Union[ComponentRefV1, int],
    ):
        assert not _is_component_ref_field(FieldInfo.from_annotation(annotation)), annotation


@pytest.mark.parametrize("label, annotation_src, value", _REF_SHAPES)
def test_an_admitted_reference_field_survives_being_read(label, annotation_src, value):
    """QA-155-r8-01: the walk must not CONSUME the document it reads.

    This is why the container floor is `Collection` and not `Iterable`. Pydantic
    validates an `Iterable[...]` field to a lazy iterator, so the first of the
    four consumers of this walk would drain it and the other three would see an
    empty field — and the read would MUTATE the document, with nothing to
    detect it, because an exhausted iterator is still iterable.

    A collection is re-iterable by contract, so the property is asserted the
    only way it can be: read the same field four times and require the same
    answer. Reading once would pass on the broken tree.
    """
    import typing

    from pydantic.fields import FieldInfo

    from boomi_mcp.models.process_ir import (
        ComponentRefV1,
        StopNodeV1,
        iter_component_refs,
    )

    namespace = dict(vars(typing))
    namespace["ComponentRefV1"] = ComponentRefV1
    annotation = eval(annotation_src, namespace)  # noqa: S307 - fixed table above

    node = StopNodeV1.model_construct(kind="stop")
    object.__setattr__(node, "planted", value)
    fields = dict(StopNodeV1.model_fields)
    fields["planted"] = FieldInfo.from_annotation(annotation)
    try:
        StopNodeV1.model_fields = fields
        reads = [{ref for _p, ref in iter_component_refs(node)} for _ in range(4)]
    finally:
        StopNodeV1.model_fields = {k: v for k, v in fields.items() if k != "planted"}

    expected = {value} if isinstance(value, str) else set(value)
    assert reads == [expected] * 4, (label, reads)


def test_a_one_shot_value_is_refused_by_the_value_side_too():
    """The value-domain half of the same floor.

    The annotation side cannot catch everything — a field typed as a collection
    could still be handed a generator by `model_construct`, which performs no
    validation. Both spellings therefore carry the floor, and this pins the one
    that has no annotation to consult.
    """
    from boomi_mcp.models.process_ir import _is_walkable_collection_value

    for admitted in (["a"], ("a",)):
        assert _is_walkable_collection_value(admitted), admitted
    # A set is refused HERE too, not only in the annotation: the walk's index is
    # the identity its consumers receive, and set iteration order moves between
    # processes. Measured across four hash seeds: four different path-to-
    # reference maps, while the list control was identical in all four.
    for refused in ({"a"}, frozenset({"a"}), (x for x in "ab"), iter(["a"]),
                    "abc", b"ab", {"a": 1}):
        assert not _is_walkable_collection_value(refused), refused


def test_a_named_tuple_that_is_not_all_references_is_refused():
    """The over-fire control for the shape above.

    Homogeneity is required here for the same reason it is for a tuple: the
    walk yields per element with no way to tell a reference position from a
    plain one. A named tuple mixing the two, and one holding none, are both
    refused.
    """
    from typing import NamedTuple

    from pydantic.fields import FieldInfo

    from boomi_mcp.models.process_ir import ComponentRefV1, _is_component_ref_field

    class _Mixed(NamedTuple):
        first: ComponentRefV1
        second: str

    class _Plain(NamedTuple):
        a: str
        b: int

    assert not _is_component_ref_field(FieldInfo.from_annotation(_Mixed))
    assert not _is_component_ref_field(FieldInfo.from_annotation(_Plain))
    # ...and an ordinary class with annotations is not a container at all.
    class _NotATuple:
        __annotations__ = {"ref": ComponentRefV1}

    assert not _is_component_ref_field(FieldInfo.from_annotation(_NotATuple))


def _is_ref(field_info) -> bool:
    from boomi_mcp.models.process_ir import _is_component_ref_field

    return _is_component_ref_field(field_info)


def test_no_model_declares_a_reference_in_an_unsupported_shape():
    """The guard that CLOSES the space, run over every real model.

    Four rounds of QA findings on this predicate all had one shape: it was
    correct about the annotations it had been shown and wrong about the next
    one, because "a container of X" has no finite set of spellings. This test
    is why the fifth version does not need to be right about all of them — any
    shape outside the three admitted forms cannot exist in the first place.
    """
    from pydantic import BaseModel

    from boomi_mcp.models import process_ir as ir_models
    from boomi_mcp.models.process_ir import (
        ProcessIRV1,
        assert_component_refs_are_declared_in_supported_shapes,
        models_reachable_by_the_reference_walk,
    )

    reachable = models_reachable_by_the_reference_walk(ProcessIRV1)

    # Non-vacuity: the guard passes trivially over an empty universe, so the
    # universe is asserted to contain the models that actually carry references.
    assert len(reachable) > 30, len(reachable)
    carriers = [m for m in reachable if any(_is_ref(f) for f in m.model_fields.values())]
    assert len(carriers) >= 8, [m.__name__ for m in carriers]

    assert_component_refs_are_declared_in_supported_shapes(reachable)

    # ...and the derived universe must not be SMALLER than the module's own
    # models where it matters: every model in this module that carries a
    # reference must be reachable from the root. A model the walk cannot reach
    # contributes to no served answer; one it can reach and the guard misses is
    # exactly the universe failure this derivation replaced a snapshot to avoid.
    in_module = [
        obj
        for obj in vars(ir_models).values()
        if isinstance(obj, type)
        and issubclass(obj, BaseModel)
        and obj.__module__ == ir_models.__name__
        and any(_is_ref(f) for f in obj.model_fields.values())
    ]
    unreachable = [m.__name__ for m in in_module if m not in reachable]
    assert not unreachable, unreachable


@pytest.mark.parametrize(
    "annotation_src",
    [
        "Set[ComponentRefV1]",
        "FrozenSet[ComponentRefV1]",
        "Iterable[ComponentRefV1]",
        "Dict[str, ComponentRefV1]",
        "Tuple[Tuple[ComponentRefV1, ...], ...]",
        "Tuple[ComponentRefV1, str]",
    ],
)
def test_the_guard_refuses_each_unsupported_shape(annotation_src):
    """NON-VACUITY: a guard that passes because it finds nothing is not a guard.

    Every shape here was measured to mis-serve — unordered iteration gives a
    different path per process, a lazy iterable is consumed by the first of the
    walk's consumers, a mapping iterates keys, a nested collection is flattened
    one level, a heterogeneous tuple has no per-position answer.
    """
    import typing

    from pydantic.fields import FieldInfo

    from boomi_mcp.models.process_ir import (
        ComponentRefV1,
        StopNodeV1,
        assert_component_refs_are_declared_in_supported_shapes,
    )

    namespace = dict(vars(typing))
    namespace["ComponentRefV1"] = ComponentRefV1
    annotation = eval(annotation_src, namespace)  # noqa: S307 - fixed table above

    class _Planted(StopNodeV1):
        pass

    _Planted.model_fields = dict(
        StopNodeV1.model_fields, planted=FieldInfo.from_annotation(annotation)
    )
    with pytest.raises(TypeError, match="component references must be declared"):
        assert_component_refs_are_declared_in_supported_shapes([_Planted])


def test_the_guard_refuses_the_named_tuple_shapes_that_emit_WRONG_references():
    """The direction that is worse than invisibility, and how it was found.

    A previous version of this predicate read a `NamedTuple`'s `__annotations__`
    as its member list. They agree only for a directly-declared one: a subclass
    reports an EMPTY mapping (its references become invisible), and a subclass
    that adds a non-member annotation reports that instead — so the walk yielded
    every position as a reference, handing a plain string to the dependency
    preflight and the relocatability gate as a component id. A tuple subclass
    carrying annotations did the same while being no named tuple at all.

    Both are now refused a shape and caught by the guard, which is the only
    outcome that does not depend on classifying them correctly.
    """
    from typing import NamedTuple

    from pydantic.fields import FieldInfo

    from boomi_mcp.models.process_ir import (
        ComponentRefV1,
        StopNodeV1,
        assert_component_refs_are_declared_in_supported_shapes,
    )

    class _NTRefs(NamedTuple):
        a: ComponentRefV1
        b: ComponentRefV1

    class _ChildBare(_NTRefs):
        pass

    class _ChildRefAnn(_NTRefs):
        extra: ComponentRefV1

    class _TupleSubclass(tuple):
        __annotations__ = {"a": ComponentRefV1}

    for annotation in (_NTRefs, _ChildBare, _ChildRefAnn, _TupleSubclass):
        class _Planted(StopNodeV1):
            pass

        _Planted.model_fields = dict(
            StopNodeV1.model_fields, planted=FieldInfo.from_annotation(annotation)
        )
        with pytest.raises(TypeError, match="component references must be declared"):
            assert_component_refs_are_declared_in_supported_shapes([_Planted])


def test_the_shape_guard_does_not_depend_on_resolving_annotations():
    """QA-155-r10-01: the closure's one blind axis, removed rather than patched.

    The guard used to decide whether a non-generic container carried a reference
    by reading that class's member annotations. This module compiles under
    postponed annotation evaluation, so a foreign class's `__annotations__` hold
    strings and forward references — and the scanner, which resolves neither,
    saw nothing. QA proved it end to end: the same named tuple of references
    passed the guard when declared with postponed evaluation and was refused
    without it, while the field really carried two references and the walk
    yielded none.

    The fix does not teach the scanner to resolve. It stops asking: a
    non-generic container is refused as a field annotation whatever its members
    are. This test forces the exact condition — members as strings, and as
    forward references — and requires refusal from the shape alone, asserting
    that the scanner still cannot see them so the refusal cannot be coming from
    there.
    """
    from typing import ForwardRef, NamedTuple

    from pydantic.fields import FieldInfo

    from boomi_mcp.models.process_ir import (
        ComponentRefV1,
        StopNodeV1,
        _annotation_mentions_component_ref,
        assert_component_refs_are_declared_in_supported_shapes,
    )

    class _Resolved(NamedTuple):
        a: ComponentRefV1
        b: ComponentRefV1

    class _Strings(NamedTuple):
        a: ComponentRefV1
        b: ComponentRefV1

    _Strings.__annotations__ = {"a": "ComponentRefV1", "b": "ComponentRefV1"}

    class _Forward(NamedTuple):
        a: ComponentRefV1
        b: ComponentRefV1

    _Forward.__annotations__ = {"a": ForwardRef("ComponentRefV1")}

    class _TupleSubclass(tuple):
        __annotations__ = {"a": "ComponentRefV1"}

    for annotation in (_Resolved, _Strings, _Forward, _TupleSubclass):
        # The scanner is blind to every one of these...
        assert not _annotation_mentions_component_ref(annotation), annotation
        # ...and every one is refused anyway, which is the whole point.
        class _Planted(StopNodeV1):
            pass

        _Planted.model_fields = dict(
            StopNodeV1.model_fields, planted=FieldInfo.from_annotation(annotation)
        )
        with pytest.raises(TypeError, match="non-generic container"):
            assert_component_refs_are_declared_in_supported_shapes([_Planted])


@pytest.mark.parametrize("annotation_src", ["str", "int", "bool", "StopNodeV1"])
def test_the_shape_guard_does_not_refuse_an_ordinary_field(annotation_src):
    """The over-fire control. A rule that refuses containers must not refuse
    a string — which IS a sequence — a scalar, or a nested model."""
    from pydantic.fields import FieldInfo

    from boomi_mcp.models.process_ir import (
        StopNodeV1,
        assert_component_refs_are_declared_in_supported_shapes,
    )

    annotation = {"str": str, "int": int, "bool": bool, "StopNodeV1": StopNodeV1}[annotation_src]

    class _Planted(StopNodeV1):
        pass

    _Planted.model_fields = dict(
        StopNodeV1.model_fields, planted=FieldInfo.from_annotation(annotation)
    )
    assert_component_refs_are_declared_in_supported_shapes([_Planted])


def test_pydantic_resolves_field_annotations_under_postponed_evaluation():
    """The assumption the SCANNER rests on, pinned rather than assumed.

    The shape rule above needs no resolution, but the mention scanner still
    reads generic arguments — and it is only safe to do so because pydantic
    resolves a model's field annotations when it builds the model, even in a
    module using postponed evaluation. If that ever stopped being true, generic
    shapes would go blind exactly as the non-generic ones did, and this test is
    what would say so.
    """
    from boomi_mcp.models.process_ir import (
        ConnectorPathBindingV1,
        _annotation_mentions_component_ref,
    )

    # This model lives in THIS module, which uses postponed evaluation.
    annotation = ConnectorPathBindingV1.model_fields["request_profile_ref"].annotation
    assert "ForwardRef" not in repr(annotation), annotation
    assert _annotation_mentions_component_ref(annotation)


def test_an_arbitrary_class_cannot_carry_a_reference_past_the_models():
    """The third axis, closed by the base model rather than by this guard.

    A non-container class holding a reference would be walked by nothing and
    caught by no shape rule — but it cannot be declared at all: the ProcessIR
    base forbids arbitrary types, so pydantic refuses the field outright. Pinned
    because the guard's completeness argument depends on it.
    """
    from boomi_mcp.models.process_ir import ComponentRefV1, _ProcessIRBase

    assert _ProcessIRBase.model_config.get("arbitrary_types_allowed", False) is False

    class _Holder:
        ref: ComponentRefV1

    with pytest.raises(Exception) as excinfo:
        class _Model(_ProcessIRBase):
            held: _Holder

    assert "arbitrary_types_allowed" in str(excinfo.value)


@pytest.mark.parametrize("label", ["type_variable", "forward_reference", "any"])
def test_the_guard_refuses_an_annotation_it_cannot_reason_about(label):
    """QA-155-r11-01: the two counterexamples to the trichotomy, plus `Any`.

    I claimed a field annotation here could only be a bare class, a generic
    alias or an annotated type, each closed by its own mechanism. QA refuted it
    with two stored annotation kinds that are none of the three and were passed
    silently: an unbound type variable, whose real type is chosen at a use site
    the guard never sees, and a whole annotation still held as a forward
    reference, which is what a model stores while it is incomplete because its
    referent did not exist yet. `Any` is added on the same reasoning — it is a
    class, so it slipped through as kind one while asserting nothing at all.

    None occurs in the models today. They are refused so that the guard's other
    rules are total over what remains, rather than silently inapplicable.
    """
    from typing import Any, ForwardRef, TypeVar

    from pydantic.fields import FieldInfo

    from boomi_mcp.models.process_ir import (
        StopNodeV1,
        assert_component_refs_are_declared_in_supported_shapes,
    )

    annotation = {
        "type_variable": TypeVar("T"),
        "forward_reference": ForwardRef("Optional[List[Later]]"),
        "any": Any,
    }[label]

    class _Planted(StopNodeV1):
        pass

    planted = FieldInfo.from_annotation(str)
    planted.annotation = annotation
    _Planted.model_fields = dict(StopNodeV1.model_fields, planted=planted)

    with pytest.raises(TypeError, match="component references must be declared"):
        assert_component_refs_are_declared_in_supported_shapes([_Planted])


def test_the_guard_universe_is_derived_from_reachability_not_a_snapshot():
    """Both counterexamples were UNIVERSE failures, not logic failures.

    A module snapshot is wrong in two independent ways: it sweeps in foreign
    models that merely happen to be imported — the framework's own base class
    among them, which is permanently incomplete and would now be refused — and
    it misses any model reached from somewhere else. Reachability from the root
    is the property that matters, since a model the walk cannot reach cannot
    contribute a reference to a served answer.
    """
    from pydantic import BaseModel

    from boomi_mcp.models.process_ir import (
        ProcessIRV1,
        StopNodeV1,
        models_reachable_by_the_reference_walk,
    )

    reachable = models_reachable_by_the_reference_walk(ProcessIRV1)

    assert ProcessIRV1 in reachable
    assert StopNodeV1 in reachable
    # The framework's base is NOT swept in by reachability, though a snapshot of
    # the module's contents does contain it.
    assert BaseModel not in reachable
    # Reachability is transitive, not one level: a node's nested body models are
    # included, or the guard would check only the root's own fields.
    assert len(reachable) > 30, len(reachable)


def test_no_model_in_the_guard_universe_subclasses_another():
    """QA-155-r12-01: makes the last residue structurally impossible.

    Reachability walks ANNOTATIONS while the walk itself reads a value's runtime
    `model_fields`. Those part company for exactly one shape: an instance of a
    subclass held in a field declared as its parent. Such a model is walked but
    never enumerated, so a reference it declares in a refused shape is skipped
    in silence rather than refused loudly.

    It cannot be reached the way callers construct — validating the equivalent
    document is rejected, because the base forbids extra fields, and only
    in-process Python construction preserves the subclass. So this is not a
    defect at the tool boundary; QA said as much and did not gate on it.

    It is asserted anyway because the assertion is derivable from the universe
    the guard already computes, and it converts "no model happens to subclass
    another" from a fact that is true today into one that cannot quietly stop
    being true.
    """
    from boomi_mcp.models.process_ir import (
        ProcessIRV1,
        models_reachable_by_the_reference_walk,
    )

    reachable = models_reachable_by_the_reference_walk(ProcessIRV1)
    offenders = [
        "{0} subclasses {1}".format(child.__name__, parent.__name__)
        for child in reachable
        for parent in reachable
        if child is not parent and issubclass(child, parent)
    ]
    assert not offenders, offenders
