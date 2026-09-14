"""Issue #136 (M12.1): legacy ``flow_sequence`` <-> ProcessIRV1 codec parity.

Proves the losslessness acceptance criterion over the frozen vocabulary:
``canonical(legacy->IR) == canonical(legacy->IR->legacy->IR)`` for every
fixture case, that reconstructed configs still pass the UNCHANGED legacy
builders' ``validate_config``, that connector metadata never enters the IR,
and that alias/default normalization is canonical.

The codec under test is the PRIVATE dark module ``_process_ir_compat`` —
imported here only; runtime code never touches it (#139 owns the production
adapter).
"""

import copy
import json
import sys
from pathlib import Path

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.categories.components.builders.process_flow_builder import (
    BuilderValidationError,
    ProcessFlowBuilder,
    WrapperSubprocessBuilder,
)
from boomi_mcp.errors import (
    PROCESS_IR_CAPABILITY_UNSUPPORTED,
    PROCESS_IR_SCHEMA_INVALID,
)
from boomi_mcp.models._process_ir_compat import (
    ConnectorBindingV1,
    ConnectorResolutionContextV1,
    ir_to_legacy_flow_sequence,
    legacy_flow_sequence_to_ir,
)
from boomi_mcp.models.process_ir import (
    ProcessIRValidationError,
    canonical_process_ir_json,
    parse_process_ir_v1,
)

FIXTURE_PATH = (
    Path(__file__).resolve().parent / "fixtures" / "process_ir" / "flow_sequence_compat_cases.json"
)
_RAW = json.loads(FIXTURE_PATH.read_text())
_SHARED = _RAW["shared"]


def _resolve_placeholders(value):
    """Expand '@source'/'@target'/'@target_b' placeholders from the shared block."""
    if isinstance(value, str) and value.startswith("@"):
        return copy.deepcopy(_SHARED[value[1:]])
    if isinstance(value, dict):
        return {k: _resolve_placeholders(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_placeholders(v) for v in value]
    return value


CASES = {name: _resolve_placeholders(case) for name, case in _RAW["cases"].items()}


def build_context(*, with_fallback: bool) -> ConnectorResolutionContextV1:
    return ConnectorResolutionContextV1(
        operation_bindings={
            ref: ConnectorBindingV1(**binding) for ref, binding in _SHARED["bindings"].items()
        },
        fallback_target=copy.deepcopy(_SHARED["target"]) if with_fallback else None,
    )


def roundtrip(config, *, with_fallback: bool):
    ir1 = legacy_flow_sequence_to_ir(config)
    legacy2 = ir_to_legacy_flow_sequence(ir1, build_context(with_fallback=with_fallback))
    ir2 = legacy_flow_sequence_to_ir(legacy2)
    return ir1, legacy2, ir2


# ---------------------------------------------------------------------------
# Round-trip equivalence + builder acceptance for every fixture case
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(CASES), ids=sorted(CASES))
def test_roundtrip_is_canonically_lossless(name):
    case = CASES[name]
    ir1, _, ir2 = roundtrip(case["config"], with_fallback=case.get("needs_fallback_target", False))
    assert canonical_process_ir_json(ir1) == canonical_process_ir_json(ir2)


@pytest.mark.parametrize("name", sorted(CASES), ids=sorted(CASES))
def test_reconstructed_config_passes_unchanged_builder_validation(name):
    case = CASES[name]
    _, legacy2, _ = roundtrip(case["config"], with_fallback=case.get("needs_fallback_target", False))
    depends_on = _SHARED["depends_on"]
    if legacy2.get("process_kind") == "wrapper_subprocess":
        err = WrapperSubprocessBuilder.validate_config(legacy2, depends_on=depends_on)
    else:
        err = ProcessFlowBuilder.validate_config(legacy2, depends_on=depends_on)
    assert err is None, f"{name}: {err}"


@pytest.mark.parametrize("name", sorted(CASES), ids=sorted(CASES))
def test_serialized_ir_carries_no_connector_metadata(name):
    case = CASES[name]
    ir = legacy_flow_sequence_to_ir(case["config"])
    canonical = canonical_process_ir_json(ir)
    for connector_token in ('"database"', '"rest_client"', '"Get"', '"POST"', '"PUT"'):
        assert connector_token not in canonical
    assert "connector_type" not in canonical
    assert "action_type" not in canonical


# ---------------------------------------------------------------------------
# #184 amendment 3: the withdrawn LINEAR cache chains, kept as refusal witnesses
# ---------------------------------------------------------------------------

#: The three compat cases whose pre-amendment content chained a cache write (and,
#: in two of them, a removal) linearly. Each case NAME stays in the fixture with
#: legal ordered-leg content; the withdrawn chain is preserved here, pinned to the
#: pointer that refuses it — the treatment the fixture already gives the withdrawn
#: multi-call wrapper. Value: (the pre-amendment flow_sequence, index of the step
#: the builder blames, i.e. the first cache write's successor).
_WITHDRAWN_LINEAR_CACHE_CHAINS = {
    "cache_load_retrieve_remove": (
        [
            {"kind": "doccacheload", "document_cache_id": "$ref:cache"},
            {"kind": "doccacheretrieve", "document_cache_id": "$ref:cache",
             "empty_cache_behavior": "stopprocess", "load_all_documents": True},
            {"kind": "doccacheremove", "document_cache_id": "$ref:cache",
             "remove_all_documents": True},
        ],
        1,
    ),
    "cache_put_get": (
        [
            {"kind": "cache_put", "document_cache_id": "$ref:cache"},
            {"kind": "cache_get", "document_cache_id": "$ref:cache",
             "empty_cache_behavior": "stopprocess"},
        ],
        1,
    ),
    "full_vocabulary_linear": (
        [
            {"kind": "flow_control", "for_each_count": 10, "label": "batch"},
            {"kind": "message", "message_text": "sentinel-message"},
            {"kind": "map_ref", "map_ref": "$ref:map"},
            {"kind": "dataprocess", "steps": [
                {"operation": "custom_scripting", "script": "return 1"},
                {"operation": "split_documents", "profile_type": "json",
                 "profile_id": "$ref:profile_json", "link_element_key": "lek",
                 "link_element_name": "len"},
                {"operation": "combine_documents", "profile_type": "xml",
                 "profile_id": "$ref:profile_xml", "link_element_key": "lek2",
                 "link_element_name": "len2", "combine_into_link_element_key": "parent"},
            ]},
            {"kind": "set_ddp", "name": "DDP_SENTINEL", "source_values": [
                {"value_type": "static", "value": "sv"},
                {"value_type": "current"},
                {"value_type": "profile", "element_id": "el", "element_name": "eln",
                 "profile_id": "$ref:profile_props", "profile_type": "profile.json"},
                {"value_type": "ddp", "property_name": "OTHER_DDP", "default_value": "dv"},
            ]},
            {"kind": "set_dpp", "name": "DPP_SENTINEL",
             "source_values": [{"value_type": "dpp", "property_name": "OTHER_DPP"}],
             "persist": True},
            {"kind": "doccacheload", "document_cache_id": "$ref:cache"},
            {"kind": "doccacheretrieve", "document_cache_id": "$ref:cache"},
            {"kind": "doccacheremove", "document_cache_id": "$ref:cache"},
            {"kind": "cache_put", "document_cache_id": "$ref:cache", "label": "stage-write"},
            {"kind": "cache_get", "document_cache_id": "$ref:cache", "external_writer": True},
        ],
        7,
    ),
}


@pytest.mark.parametrize(
    "name", sorted(_WITHDRAWN_LINEAR_CACHE_CHAINS), ids=sorted(_WITHDRAWN_LINEAR_CACHE_CHAINS)
)
def test_withdrawn_linear_cache_chain_is_refused_at_the_successor(name):
    """Add to Cache and Remove from Cache hand on no documents (measured): a step
    wired after one never runs while the run still reads COMPLETE — a cache read
    included, since nothing arrives to trigger it.

    So each withdrawn chain is refused on all three surfaces a compat case feeds:
    the builder's validator blames the first write's SUCCESSOR at its ``.kind``,
    the build path refuses the same way before emitting anything, and the codec
    cannot represent the chain either — the IR refuses the write's authored
    successor at the write's own ``cache_ref`` (the source occupies
    ``/body/steps/0``, so ``flow_sequence[i]`` is ``/body/steps/i+1``).
    """
    steps, successor = _WITHDRAWN_LINEAR_CACHE_CHAINS[name]
    # the blamed step really is the successor of a cache WRITE
    assert steps[successor - 1]["kind"] in ("doccacheload", "cache_put")
    config = _linear_config(*copy.deepcopy(steps))

    err = ProcessFlowBuilder.validate_config(
        copy.deepcopy(config), depends_on=_SHARED["depends_on"]
    )
    assert err is not None
    assert (err.error_code, err.field) == (
        "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
        f"flow_sequence[{successor}].kind",
    )

    with pytest.raises(BuilderValidationError) as build_exc:
        ProcessFlowBuilder.build(copy.deepcopy(config), name="P", folder_name="F")
    assert (build_exc.value.error_code, build_exc.value.field) == (
        "PROCESS_FLOW_SEQUENCE_CONFIG_INVALID",
        f"flow_sequence[{successor}].kind",
    )

    with pytest.raises(ProcessIRValidationError) as exc_info:
        legacy_flow_sequence_to_ir(copy.deepcopy(config))
    assert [(d.code, d.path) for d in exc_info.value.diagnostics] == [
        ("PROCESS_IR_SCHEMA_INVALID_CARDINALITY", f"/body/steps/{successor}/cache_ref")
    ]


def test_a_removal_has_no_legal_flow_sequence_placement():
    """Why ``cache_load_retrieve_remove`` could keep its load and retrieve in the
    ordered-leg form but not its removal: Remove from Cache hands on no documents,
    and the flow_sequence surface authors no terminal removal. A leg ENDING in a
    doccacheremove is refused whether or not it carries a target (the target is a
    successor that never runs; without one the removal itself is blamed), so the
    removal survives only in ProcessIR, as a branch-leg terminal (golden 000082).

    Ledger C18: because no position admits it, ``doccacheremove`` was withdrawn
    from the flow_sequence vocabulary and is refused BY NAME at the step itself,
    one identity whether or not a target follows."""
    remove = {"kind": "doccacheremove", "document_cache_id": "$ref:cache",
              "remove_all_documents": True}
    staging = {"steps": [{"kind": "doccacheload", "document_cache_id": "$ref:cache"}]}
    for leg, field in (
        ({"steps": [remove], "target": copy.deepcopy(_SHARED["target_b"])},
         "flow_sequence[0].legs[1].steps[0].kind"),
        ({"steps": [remove]}, "flow_sequence[0].legs[1].steps[0].kind"),
    ):
        config = _linear_config({"kind": "branch", "legs": [copy.deepcopy(staging), leg]})
        err = ProcessFlowBuilder.validate_config(config, depends_on=_SHARED["depends_on"])
        assert err is not None, field
        assert (err.error_code, err.field) == ("PROCESS_FLOW_SEQUENCE_CONFIG_INVALID", field)


# ---------------------------------------------------------------------------
# Alias + default normalization
# ---------------------------------------------------------------------------


def _linear_config(*steps):
    return {
        "process_kind": "database_to_api_sync",
        "source": copy.deepcopy(_SHARED["source"]),
        "target": copy.deepcopy(_SHARED["target"]),
        "flow_sequence": list(steps),
    }


def _staged_write_then_read(write_kind):
    """A cache write staged in an earlier leg and read in a later one.

    #184 amendment 3: Add to Cache hands on no documents (measured), so a read
    wired straight after the write never runs and the builder refuses that linear
    pair at the read's ``.kind``. The legal form stages the write as the terminal
    of a target-less branch leg and reads in the later leg — legs run in authored
    order and execution cache state accumulates across them.
    """
    return _linear_config(
        {
            "kind": "branch",
            "legs": [
                {"steps": [{"kind": write_kind, "document_cache_id": "$ref:cache"}]},
                {
                    "steps": [{"kind": "doccacheretrieve", "document_cache_id": "$ref:cache"}],
                    "target": copy.deepcopy(_SHARED["target_b"]),
                },
            ],
        }
    )


def test_a_terminal_cache_remove_has_no_reverse_trip():
    """#184 amendment 3: the IR→legacy codec refuses a Branch-leg terminal with no legacy form.

    The legacy dialect's only target-less leg ends in a cache_put staging write;
    ``legacy_flow_sequence_to_ir`` refuses any other. A terminal ``cache_remove`` was
    serialized anyway, as a target-less leg ending in ``doccacheremove``. The builder
    refuses that leg by name and the reverse codec cannot read it back. It is now refused
    at the reverse trip itself, following the #175 precedent. The control: the same leg
    ending in its ``cache_put`` round-trips equal.
    """
    config = _staged_write_then_read("cache_put")
    ir1, _legacy2, ir2 = roundtrip(copy.deepcopy(config), with_fallback=True)
    assert canonical_process_ir_json(ir1) == canonical_process_ir_json(ir2)

    payload = json.loads(canonical_process_ir_json(ir1))
    leg = payload["body"]["steps"][-1]["legs"][0]
    assert leg["terminal"]["kind"] == "cache_put", leg
    leg["terminal"]["kind"] = "cache_remove"
    removal = parse_process_ir_v1(payload)  # a legal IR document: a Branch leg may end on a removal
    with pytest.raises(ProcessIRValidationError) as excinfo:
        ir_to_legacy_flow_sequence(removal, build_context(with_fallback=True))
    assert [(d.code, d.path) for d in excinfo.value.diagnostics] == [(PROCESS_IR_SCHEMA_INVALID, "")]
    assert "cache_remove" in excinfo.value.diagnostics[0].message


def test_doccacheload_alias_normalizes_to_cache_put():
    legacy_spelling = _staged_write_then_read("doccacheload")
    authored_spelling = _staged_write_then_read("cache_put")
    # Both spellings are builder-legal in this form, so the alias is compared on
    # configs the runtime actually accepts.
    for config in (legacy_spelling, authored_spelling):
        assert ProcessFlowBuilder.validate_config(
            copy.deepcopy(config), depends_on=_SHARED["depends_on"]
        ) is None
    assert canonical_process_ir_json(
        legacy_flow_sequence_to_ir(legacy_spelling)
    ) == canonical_process_ir_json(legacy_flow_sequence_to_ir(authored_spelling))
    # ...and the legacy spelling really lands on the staging leg's cache_put terminal.
    branch = legacy_flow_sequence_to_ir(legacy_spelling).body.steps[-1]
    assert branch.legs[0].terminal.kind == "cache_put"


def test_dataprocess_alias_and_defaults_normalize():
    explicit = _linear_config(
        {
            "kind": "dataprocess",
            "steps": [
                {"operation": "custom_scripting", "script": "s", "language": "groovy2", "use_cache": True}
            ],
        }
    )
    defaulted = _linear_config(
        {"kind": "dataprocess", "steps": [{"operation": "custom_scripting", "script": "s"}]}
    )
    assert canonical_process_ir_json(
        legacy_flow_sequence_to_ir(explicit)
    ) == canonical_process_ir_json(legacy_flow_sequence_to_ir(defaulted))


def test_retrieve_defaults_explicit_vs_omitted_identical():
    explicit = _linear_config(
        {
            "kind": "doccacheretrieve",
            "document_cache_id": "$ref:cache",
            "empty_cache_behavior": "stopprocess",
            "load_all_documents": True,
        }
    )
    omitted = _linear_config({"kind": "doccacheretrieve", "document_cache_id": "$ref:cache"})
    assert canonical_process_ir_json(
        legacy_flow_sequence_to_ir(explicit)
    ) == canonical_process_ir_json(legacy_flow_sequence_to_ir(omitted))


def test_process_call_defaults_explicit_vs_omitted_identical():
    explicit = {
        "process_kind": "wrapper_subprocess",
        "process_calls": [{"subprocess_ref": "$ref:child", "wait": True, "abort_on_error": False}],
    }
    omitted = {
        "process_kind": "wrapper_subprocess",
        "process_calls": [{"subprocess_ref": "$ref:child"}],
    }
    assert canonical_process_ir_json(
        legacy_flow_sequence_to_ir(explicit)
    ) == canonical_process_ir_json(legacy_flow_sequence_to_ir(omitted))


def test_exception_defaults_explicit_vs_omitted_identical():
    explicit = _linear_config(
        {"kind": "message", "message_text": "m"},
        {
            "kind": "exception",
            "message_template": "x {1}",
            "stop_single_document": False,
            "parameter_source": "caught_error",
        },
    )
    omitted = _linear_config(
        {"kind": "message", "message_text": "m"},
        {"kind": "exception", "message_template": "x {1}"},
    )
    assert canonical_process_ir_json(
        legacy_flow_sequence_to_ir(explicit)
    ) == canonical_process_ir_json(legacy_flow_sequence_to_ir(omitted))


def test_list_order_preserved_through_roundtrip():
    config = CASES["full_vocabulary_linear"]["config"]
    ir1, legacy2, _ = roundtrip(config, with_fallback=False)
    ddp_sources_ir = next(
        s for s in ir1.body.steps if s.kind == "set_ddp"
    ).source_values
    assert [s.value_type for s in ddp_sources_ir] == ["static", "current", "profile", "ddp"]
    ddp_sources_legacy = next(
        s for s in legacy2["flow_sequence"] if s["kind"] == "set_ddp"
    )["source_values"]
    assert [s["value_type"] for s in ddp_sources_legacy] == ["static", "current", "profile", "ddp"]


def test_branch_leg_count_bounds_roundtrip():
    legs = [
        {"steps": [], "target": copy.deepcopy(_SHARED["target_b"])} for _ in range(25)
    ]
    config = _linear_config({"kind": "message", "message_text": "m"}, {"kind": "branch", "legs": legs})
    ir1, _, ir2 = roundtrip(config, with_fallback=True)
    assert canonical_process_ir_json(ir1) == canonical_process_ir_json(ir2)
    assert len(ir1.body.steps[-1].legs) == 25


# ---------------------------------------------------------------------------
# Symbol-table contract
# ---------------------------------------------------------------------------


def test_missing_operation_binding_is_typed_error():
    ir = legacy_flow_sequence_to_ir(CASES["one_step_sequence"]["config"])
    empty_context = ConnectorResolutionContextV1()
    with pytest.raises(ProcessIRValidationError) as exc_info:
        ir_to_legacy_flow_sequence(ir, empty_context)
    assert exc_info.value.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID


def test_missing_fallback_target_is_typed_error():
    ir = legacy_flow_sequence_to_ir(CASES["exception_terminal"]["config"])
    context = build_context(with_fallback=False)
    with pytest.raises(ProcessIRValidationError) as exc_info:
        ir_to_legacy_flow_sequence(ir, context)
    assert exc_info.value.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID


def test_context_is_read_only():
    context = build_context(with_fallback=True)
    with pytest.raises(Exception):
        context.fallback_target = None  # frozen


# ---------------------------------------------------------------------------
# Frozen-scope rejections (the codec never grows into the #139 adapter)
# ---------------------------------------------------------------------------


def test_unknown_root_key_rejected():
    config = _linear_config({"kind": "message", "message_text": "m"})
    config["surprise_root"] = {"x": 1}
    with pytest.raises(ProcessIRValidationError) as exc_info:
        legacy_flow_sequence_to_ir(config)
    assert exc_info.value.diagnostics[0].code == PROCESS_IR_CAPABILITY_UNSUPPORTED


def test_try_catch_reliability_rejected():
    config = _linear_config({"kind": "message", "message_text": "m"})
    config["reliability"] = {"retry_count": 2, "dlq": {"mode": "document_cache_ref", "document_cache_ref": "$ref:cache"}}
    with pytest.raises(ProcessIRValidationError) as exc_info:
        legacy_flow_sequence_to_ir(config)
    assert exc_info.value.diagnostics[0].code == PROCESS_IR_CAPABILITY_UNSUPPORTED


def test_noop_reliability_treated_as_absent():
    config = _linear_config({"kind": "message", "message_text": "m"})
    config["reliability"] = {"retry_count": 0, "dlq": {"mode": "disabled"}}
    baseline = _linear_config({"kind": "message", "message_text": "m"})
    assert canonical_process_ir_json(
        legacy_flow_sequence_to_ir(config)
    ) == canonical_process_ir_json(legacy_flow_sequence_to_ir(baseline))


def test_dynamic_path_rejected():
    config = _linear_config({"kind": "message", "message_text": "m"})
    config["source"]["dynamic_path"] = {"mode": "static"}
    with pytest.raises(ProcessIRValidationError) as exc_info:
        legacy_flow_sequence_to_ir(config)
    assert exc_info.value.diagnostics[0].code == PROCESS_IR_CAPABILITY_UNSUPPORTED


def test_wrapper_process_extensions_rejected():
    config = {
        "process_kind": "wrapper_subprocess",
        "process_calls": [{"subprocess_ref": "$ref:child"}],
        "process_extensions": {"connections": []},
    }
    with pytest.raises(ProcessIRValidationError) as exc_info:
        legacy_flow_sequence_to_ir(config)
    assert exc_info.value.diagnostics[0].code == PROCESS_IR_CAPABILITY_UNSUPPORTED


def test_gated_keyed_cache_keys_rejected():
    config = _linear_config(
        {"kind": "cache_put", "document_cache_id": "$ref:cache"},
        {"kind": "cache_get", "document_cache_id": "$ref:cache", "doc_cache_index": 1},
    )
    with pytest.raises(ProcessIRValidationError) as exc_info:
        legacy_flow_sequence_to_ir(config)
    assert exc_info.value.diagnostics[0].code == PROCESS_IR_CAPABILITY_UNSUPPORTED


def test_unknown_legacy_step_key_rejected_not_dropped():
    config = _linear_config({"kind": "message", "message_text": "m", "surprise": 1})
    with pytest.raises(ProcessIRValidationError) as exc_info:
        legacy_flow_sequence_to_ir(config)
    assert exc_info.value.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID


def test_unsupported_process_kind_rejected():
    with pytest.raises(ProcessIRValidationError) as exc_info:
        legacy_flow_sequence_to_ir({"process_kind": "sync_pipeline", "pipeline": {}})
    assert exc_info.value.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID


@pytest.mark.parametrize(
    "flow_sequence",
    [
        [{"kind": "teleport"}],
        [{"message_text": "kindless"}],
        [
            {
                "kind": "branch",
                "legs": [
                    {"steps": [{"kind": "teleport"}], "target": None},
                    {"steps": [], "target": None},
                ],
            }
        ],
        [
            {
                "kind": "decision",
                "comparison": "equals",
                "left": {"value_type": "static", "static_value": ""},
                "right": {"value_type": "static", "static_value": ""},
                "true_steps": [{"kind": "teleport"}],
                "false_steps": [{"kind": "message", "message_text": "f"}],
            }
        ],
    ],
    ids=["root-unknown", "root-kindless", "branch-leg-unknown", "decision-arm-unknown"],
)
def test_unknown_legacy_step_kind_is_typed_error_not_keyerror(flow_sequence):
    config = _linear_config(*flow_sequence)
    for leg_holder in flow_sequence:
        if leg_holder.get("kind") == "branch":
            for leg in leg_holder["legs"]:
                leg["target"] = copy.deepcopy(_SHARED["target_b"])
    with pytest.raises(ProcessIRValidationError) as exc_info:
        legacy_flow_sequence_to_ir(config)
    assert exc_info.value.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID


def test_endpoint_labels_preserved_both_directions():
    # shared source/target carry labels; a leg target label rides through too.
    config = _linear_config({"kind": "message", "message_text": "m"})
    ir = legacy_flow_sequence_to_ir(config)
    assert ir.body.steps[0].label == "DB Read"
    assert ir.body.steps[-2].label == "REST Send"
    legacy2 = ir_to_legacy_flow_sequence(ir, build_context(with_fallback=False))
    assert legacy2["source"]["label"] == "DB Read"
    assert legacy2["target"]["label"] == "REST Send"


def test_dataprocess_operation_extra_key_rejected_not_dropped():
    config = _linear_config(
        {
            "kind": "dataprocess",
            "steps": [{"operation": "custom_scripting", "script": "s", "surprise": 1}],
        }
    )
    with pytest.raises(ProcessIRValidationError) as exc_info:
        legacy_flow_sequence_to_ir(config)
    assert exc_info.value.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID


def test_property_source_extra_key_rejected_not_dropped():
    config = _linear_config(
        {
            "kind": "set_ddp",
            "name": "N",
            "source_values": [{"value_type": "current", "surprise": 1}],
        }
    )
    with pytest.raises(ProcessIRValidationError) as exc_info:
        legacy_flow_sequence_to_ir(config)
    assert exc_info.value.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID


def test_decision_operand_extra_key_rejected_not_dropped():
    config = _linear_config(
        {
            "kind": "decision",
            "comparison": "equals",
            "left": {"value_type": "static", "static_value": "", "surprise": 1},
            "right": {"value_type": "static", "static_value": ""},
            "true_steps": [],
            "false_steps": [{"kind": "message", "message_text": "f"}],
        }
    )
    with pytest.raises(ProcessIRValidationError) as exc_info:
        legacy_flow_sequence_to_ir(config)
    assert exc_info.value.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID


def test_return_documents_ir_has_no_dead_target():
    ir = legacy_flow_sequence_to_ir(CASES["linear_return_documents"]["config"])
    kinds = [s.kind for s in ir.body.steps]
    assert kinds == ["source", "message", "return_documents"]
    assert "target" not in kinds


# ---------------------------------------------------------------------------
# Impl-review round 2: malformed blocks are typed rejections, never silently
# normalized/dropped; legacy field semantics are exact
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "rd",
    ["yes", {"label": "x"}, {"enabled": "true"}, {"enabled": True, "bogus": 1}, {"enabled": True, "label": 5}],
    ids=["non-dict", "missing-enabled", "string-enabled", "extra-key", "non-string-label"],
)
def test_malformed_return_documents_rejected_not_disabled(rd):
    config = _linear_config({"kind": "message", "message_text": "m"})
    config["return_documents"] = rd
    with pytest.raises(ProcessIRValidationError) as exc_info:
        legacy_flow_sequence_to_ir(config)
    assert exc_info.value.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID


def test_malformed_return_documents_rejected_even_with_control_terminal():
    config = _linear_config(
        {"kind": "message", "message_text": "m"},
        {"kind": "exception", "message_template": "x {1}"},
    )
    config["return_documents"] = "yes"
    with pytest.raises(ProcessIRValidationError) as exc_info:
        legacy_flow_sequence_to_ir(config)
    assert exc_info.value.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID


@pytest.mark.parametrize(
    "reliability",
    ["yes", {"retry_count": "2"}, {"dlq": "x"}, {"retry_count": -1}, {"retry_count": 6, "dlq": {"mode": "disabled"}}],
    ids=["non-dict", "string-retry", "non-dict-dlq", "negative-retry", "over-range-retry"],
)
def test_malformed_reliability_is_typed_error_not_crash(reliability):
    config = _linear_config({"kind": "message", "message_text": "m"})
    config["reliability"] = reliability
    with pytest.raises(ProcessIRValidationError) as exc_info:
        legacy_flow_sequence_to_ir(config)
    assert exc_info.value.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID


@pytest.mark.parametrize("bad_steps", [False, 0, "", {}], ids=["False", "0", "empty-str", "dict"])
def test_falsy_non_list_steps_rejected(bad_steps):
    branch_cfg = _linear_config(
        {"kind": "message", "message_text": "m"},
        {
            "kind": "branch",
            "legs": [
                {"steps": bad_steps, "target": copy.deepcopy(_SHARED["target_b"])},
                {"steps": [], "target": copy.deepcopy(_SHARED["target_b"])},
            ],
        },
    )
    with pytest.raises(ProcessIRValidationError):
        legacy_flow_sequence_to_ir(branch_cfg)
    decision_cfg = _linear_config(
        {
            "kind": "decision",
            "comparison": "equals",
            "left": {"value_type": "static", "static_value": ""},
            "right": {"value_type": "static", "static_value": ""},
            "true_steps": bad_steps,
            "false_steps": [{"kind": "message", "message_text": "f"}],
        }
    )
    with pytest.raises(ProcessIRValidationError):
        legacy_flow_sequence_to_ir(decision_cfg)


@pytest.mark.parametrize(
    "call",
    [
        {"subprocess_ref": "literal-id"},
        {"process_id": "$ref:child"},
        {"subprocess_ref": "$ref:"},
        {"subprocess_ref": "$ref:child", "wait": None},
        {"subprocess_ref": "$ref:child", "abort_on_error": "false"},
    ],
    ids=["literal-in-sref", "ref-in-pid", "empty-ref-key", "null-wait", "string-abort"],
)
def test_process_call_field_semantics_enforced(call):
    config = {"process_kind": "wrapper_subprocess", "process_calls": [call]}
    with pytest.raises(ProcessIRValidationError) as exc_info:
        legacy_flow_sequence_to_ir(config)
    assert exc_info.value.diagnostics[0].code == PROCESS_IR_SCHEMA_INVALID


def test_builder_accepted_normalizations_ride_through():
    # external_writer=null is treated as absent by the builder; a padded
    # comparison is builder-accepted and emitted stripped — both normalize.
    #
    # #184 amendment 3: the cache_get used to follow a linear cache_put, which is
    # refused now (Add to Cache hands on no documents, so nothing arrives to
    # trigger the read). The put/get scaffolding is in the legal ordered-leg form —
    # a staging leg ending at the put, a later leg that reads — carried as the
    # decision's true-arm terminal so the padded decision stays the root's last step.
    config = _linear_config(
        {"kind": "decision", "comparison": " equals ",
         "left": {"value_type": "static", "static_value": ""},
         "right": {"value_type": "static", "static_value": ""},
         "true_steps": [{"kind": "branch", "legs": [
             {"steps": [{"kind": "cache_put", "document_cache_id": "$ref:cache"}]},
             {"steps": [{"kind": "cache_get", "document_cache_id": "$ref:cache", "external_writer": None}],
              "target": copy.deepcopy(_SHARED["target_b"])},
         ]}],
         "false_steps": [{"kind": "message", "message_text": "f"}]},
    )
    ir = legacy_flow_sequence_to_ir(config)
    decision = ir.body.steps[-1]
    cache_get = next(
        s for leg in decision.true_arm.terminal.legs for s in leg.steps if s.kind == "cache_get"
    )
    assert cache_get.external_writer is False
    assert decision.comparison == "equals"


def test_context_deep_immutability_and_fallback_key_hygiene():
    context = build_context(with_fallback=True)
    with pytest.raises(TypeError):
        context.operation_bindings["new"] = ConnectorBindingV1(connector_type="x", action_type="y")
    with pytest.raises(Exception):
        ConnectorResolutionContextV1(fallback_target={"connector_type": "rest_client", "shape_id": "shape1"})
    # Post-construction mutation must not bypass the key hygiene (QA #163).
    with pytest.raises(TypeError):
        context.fallback_target["shape_id"] = "shape-INJECTED"
    # Default-constructed contexts are equally read-only (review r2).
    default_context = ConnectorResolutionContextV1()
    with pytest.raises(TypeError):
        default_context.operation_bindings["x"] = ConnectorBindingV1(
            connector_type="x", action_type="y"
        )
    # Freezing must not break standard pydantic operations (review r2b):
    # serialization and deep copies work, and the deep copy stays read-only.
    dumped = context.model_dump(mode="json")
    assert dumped["fallback_target"]["connector_type"] == "rest_client"
    assert context.model_dump_json()
    deep = context.model_copy(deep=True)
    with pytest.raises(TypeError):
        deep.operation_bindings["x"] = ConnectorBindingV1(connector_type="x", action_type="y")
    with pytest.raises(TypeError):
        deep.fallback_target["shape_id"] = "shape-INJECTED"
    # Inherited in-place mutation paths are blocked too (review r2c): |= must
    # raise WITHOUT mutating, and a re-called __init__ must raise.
    before = dict(context.fallback_target)
    with pytest.raises(TypeError):
        context.fallback_target |= {"shape_id": "shape-INJECTED"}
    assert dict(context.fallback_target) == before
    with pytest.raises(TypeError):
        context.fallback_target.__init__({"shape_id": "shape-INJECTED"})
    assert dict(context.fallback_target) == before
    # Not a dict subclass, so UNBOUND dict mutators cannot apply either
    # (review r2d): dict.__setitem__ / dict.update reject the foreign type.
    with pytest.raises(TypeError):
        dict.__setitem__(context.fallback_target, "shape_id", "shape-INJECTED")
    with pytest.raises(TypeError):
        dict.update(context.fallback_target, {"shape_id": "shape-INJECTED"})
    assert dict(context.fallback_target) == before
    # The backing store is a read-only proxy too (review r2e) — reaching for
    # the private attribute cannot mutate it.
    with pytest.raises(TypeError):
        context.fallback_target._data["shape_id"] = "shape-INJECTED"
    assert dict(context.fallback_target) == before
    # Nor can the slot be deleted to defeat the one-shot __init__ guard and
    # re-populate with unvalidated keys (review r3).
    with pytest.raises(TypeError):
        del context.fallback_target._data
    with pytest.raises(TypeError):
        context.fallback_target.__init__({"shape_id": "shape-INJECTED"})
    assert dict(context.fallback_target) == before
