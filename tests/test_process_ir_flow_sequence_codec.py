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
# Alias + default normalization
# ---------------------------------------------------------------------------


def _linear_config(*steps):
    return {
        "process_kind": "database_to_api_sync",
        "source": copy.deepcopy(_SHARED["source"]),
        "target": copy.deepcopy(_SHARED["target"]),
        "flow_sequence": list(steps),
    }


def test_doccacheload_alias_normalizes_to_cache_put():
    legacy_spelling = _linear_config(
        {"kind": "doccacheload", "document_cache_id": "$ref:cache"},
        {"kind": "doccacheretrieve", "document_cache_id": "$ref:cache"},
    )
    authored_spelling = _linear_config(
        {"kind": "cache_put", "document_cache_id": "$ref:cache"},
        {"kind": "doccacheretrieve", "document_cache_id": "$ref:cache"},
    )
    assert canonical_process_ir_json(
        legacy_flow_sequence_to_ir(legacy_spelling)
    ) == canonical_process_ir_json(legacy_flow_sequence_to_ir(authored_spelling))


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
