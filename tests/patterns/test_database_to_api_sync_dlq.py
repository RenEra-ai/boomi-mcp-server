"""Issue #87 — archetype DLQ contract aligned to verified builder modes.

Covers: caller-facing DlqTarget validation (document_cache_ref /
error_subprocess_ref / guidance_only), the wiring of verified DLQ modes into the
emitted process.reliability (retry_count==0), the $ref-binding depends_on edge,
operational-intent recording, the end-to-end plan-time ref type check, and an
archetype-path golden that reaches the verified catcherrors + DLQ catch path.

Pure-unit against boomi_mcp (PYTHONPATH=src) — no live Boomi.
"""

from __future__ import annotations

import copy
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from unittest.mock import MagicMock, patch

_src = str(Path(__file__).resolve().parent.parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
_tests = str(Path(__file__).resolve().parent.parent)
if _tests not in sys.path:
    sys.path.insert(0, _tests)

from boomi_mcp.categories.integration_authoring import build_from_archetype_action
from boomi_mcp.categories.integration_builder import _build_plan

# The golden-case definitions live in the corpus (#165); this module CONSUMES
# them. The aliases keep every existing call site and assertion unchanged.
import _wave_gate_golden_corpus as _corpus

NS = {"bns": "http://api.platform.boomi.com/"}
_PAGINATE = "boomi_mcp.categories.integration_builder.paginate_metadata"

_CACHE_ID = _corpus.ARCH_CACHE_ID
_HANDLER_ID = "77777777-7777-7777-7777-777777777777"

_GOLDEN = (
    Path(__file__).resolve().parent.parent
    / "fixtures"
    / "golden_xml"
    / "try_catch_dlq_document_cache_archetype.xml"
)

# #184 amendment 3: golden-000060 (`try_catch_notify_dlq_document_cache_archetype.xml`)
# is retired. Each connector's catch leg ran notify -> doccacheload -> Stop, and Add
# to Cache hands on no documents, so neither Stop ever ran. The replacement
# golden-000088 was frozen independently of this builder, as a byte transform of the
# retired golden (tests/fixtures/process_ir/issue184/PROVENANCE.md). Never
# regenerate it here.
_NOTIFY_GOLDEN = (
    Path(__file__).resolve().parent.parent
    / "fixtures"
    / "golden_xml"
    / "issue184_cache_notify_archetype_terminal.xml"
)

_CATCH_NOTIFY = _corpus.ARCH_CATCH_NOTIFY

_params = _corpus.arch_params
_result = _corpus.arch_result
_emit = _corpus.arch_emit
_main_process = _corpus.arch_main_process


def _operational_intent(spec: dict) -> dict:
    return spec["validation_rules"]["operational_intent"]


# ---------------------------------------------------------------------------
# Wired emission — the verified DLQ modes reach process.reliability
# ---------------------------------------------------------------------------


def test_document_cache_ref_emits_wired_reliability():
    spec = _emit({"enabled": True, "target": {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID}})
    # Issue #99 G1: the wired sync path opts into the connector-scoped Try/Catch.
    assert _main_process(spec)["config"]["reliability"] == {
        "retry_count": 0,
        "try_catch_scope": "connector",
        "dlq": {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID},
    }


def test_error_subprocess_ref_emits_wired_reliability():
    spec = _emit({"enabled": True, "target": {"mode": "error_subprocess_ref", "process_id": _HANDLER_ID}})
    assert _main_process(spec)["config"]["reliability"] == {
        "retry_count": 0,
        "try_catch_scope": "connector",
        "dlq": {"mode": "error_subprocess_ref", "process_id": _HANDLER_ID},
    }


def test_ref_token_binding_added_to_depends_on():
    spec = _emit({"enabled": True, "target": {"mode": "document_cache_ref", "document_cache_id": "$ref:my_cache"}})
    main = _main_process(spec)
    assert "my_cache" in main["depends_on"]
    assert main["config"]["reliability"]["dlq"]["document_cache_id"] == "$ref:my_cache"


def test_literal_binding_not_added_to_depends_on():
    spec = _emit({"enabled": True, "target": {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID}})
    # A literal id needs no $ref dependency edge.
    assert _CACHE_ID not in _main_process(spec)["depends_on"]


def test_guidance_only_does_not_wire():
    spec = _emit({"enabled": True, "target": {"mode": "guidance_only", "kind": "queue", "address": "<<addr>>"}})
    assert _main_process(spec)["config"]["reliability"] == {"retry_count": 0, "dlq": {"mode": "disabled"}}


def test_disabled_unchanged():
    spec = _emit({"enabled": False})
    assert _main_process(spec)["config"]["reliability"] == {"retry_count": 0, "dlq": {"mode": "disabled"}}


def test_retry_gt1_with_wired_dlq_emits_retry_count():
    # Issue #88: caller retry max_attempts=5 with a wired DLQ emits
    # process retry_count = max_attempts - 1 = 4 (platform-timed).
    spec = _emit(
        {"enabled": True, "target": {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID}},
        retry={"max_attempts": 5, "backoff": "platform"},
    )
    rel = _main_process(spec)["config"]["reliability"]
    assert rel["retry_count"] == 4
    assert rel["dlq"] == {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID}


# ---------------------------------------------------------------------------
# Contract validation — no silent acceptance of unimplementable input
# ---------------------------------------------------------------------------


def _expect_validation_error(dlq: dict):
    result = build_from_archetype_action("database_to_api_sync", _params(dlq))
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED", result
    return result


def test_document_cache_ref_requires_id():
    _expect_validation_error({"enabled": True, "target": {"mode": "document_cache_ref"}})


def test_error_subprocess_ref_requires_process_id():
    _expect_validation_error({"enabled": True, "target": {"mode": "error_subprocess_ref"}})


def test_cross_mode_fields_rejected():
    _expect_validation_error(
        {"enabled": True, "target": {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID, "process_id": _HANDLER_ID}}
    )


def test_guidance_only_requires_kind_and_address():
    _expect_validation_error({"enabled": True, "target": {"mode": "guidance_only", "kind": "queue"}})


def test_legacy_kind_address_without_mode_rejected():
    # The old folder|topic|queue + address shape (no mode) is no longer silently
    # accepted — mode is required.
    _expect_validation_error({"enabled": True, "target": {"kind": "queue", "address": "<<addr>>"}})


def test_unsupported_mode_rejected():
    _expect_validation_error({"enabled": True, "target": {"mode": "folder", "address": "<<addr>>"}})


# ---------------------------------------------------------------------------
# RetryPolicy contract reconciliation (#88): bounds, mapping, no over-promise
# ---------------------------------------------------------------------------

_WIRED_DLQ = {"enabled": True, "target": {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID}}


def test_max_attempts_6_maps_to_retry_count_5():
    # Boomi platform maximum: 6 attempts = 5 retries.
    spec = _emit(_WIRED_DLQ, retry={"max_attempts": 6, "backoff": "platform"})
    assert _main_process(spec)["config"]["reliability"]["retry_count"] == 5


def _expect_retry_validation_error(retry: dict, dlq: dict | None = None):
    result = build_from_archetype_action(
        "database_to_api_sync", _params(dlq, retry)
    )
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED", result


def test_max_attempts_7_rejected():
    _expect_retry_validation_error({"max_attempts": 7}, _WIRED_DLQ)


def test_legacy_backoff_exponential_rejected():
    _expect_retry_validation_error({"max_attempts": 1, "backoff": "exponential"})


def test_removed_initial_interval_seconds_rejected():
    # extra="forbid" rejects the dropped field rather than silently ignoring it.
    _expect_retry_validation_error({"max_attempts": 1, "initial_interval_seconds": 2})


# ---------------------------------------------------------------------------
# Operational intent
# ---------------------------------------------------------------------------


def test_operational_intent_emitted_status_for_wired_modes():
    spec = _emit({"enabled": True, "target": {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID}})
    dq = _operational_intent(spec)["reliability"]["dlq_requested"]
    assert dq["status"] == "emitted"
    assert dq["builder_mode"] == "document_cache_ref"
    assert dq["binding"] == _CACHE_ID
    assert "deferred_to" not in dq
    # The dlq intent block mirrors the emitted process dlq.
    assert _operational_intent(spec)["reliability"]["dlq"] == {
        "mode": "document_cache_ref",
        "document_cache_id": _CACHE_ID,
    }


def test_operational_intent_guidance_only_status_and_no_freeform_echo():
    spec = _emit(
        {
            "enabled": True,
            "target": {
                "mode": "guidance_only",
                "kind": "queue",
                "address": "<<secret addr>>",
                "reason": "<<secret reason addr>>",
            },
        }
    )
    dq = _operational_intent(spec)["reliability"]["dlq_requested"]
    assert dq["status"] == "guidance_only"
    assert dq["kind"] == "queue"
    assert dq["address_present"] is True
    assert dq["reason_present"] is True
    # Neither free-form caller value (address OR reason) may be echoed anywhere
    # in the spec — both are leak vectors.
    blob = json.dumps(spec)
    assert "<<secret addr>>" not in blob
    assert "<<secret reason addr>>" not in blob


def test_operational_intent_retry_no_longer_deferred():
    # Issue #88: with a wired DLQ, retry is emitted (not deferred). The intent
    # records the emitted process_retry_count and carries no deferred_to.
    spec = _emit(
        {"enabled": True, "target": {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID}},
        retry={"max_attempts": 5},
    )
    retry = _operational_intent(spec)["reliability"]["retry"]
    assert "deferred_to" not in retry
    assert retry["requested_max_attempts"] == 5
    assert retry["process_retry_count"] == 4


# ---------------------------------------------------------------------------
# Cross-issue interaction with #86 build_from_archetype design_doctrine_hints:
# a WIRED DLQ must NOT trigger the "DLQ kept disabled" downgrade hint.
# ---------------------------------------------------------------------------


def test_no_dlq_downgrade_hint_for_wired_mode():
    result = _result({"enabled": True, "target": {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID}})
    hints = result.get("design_doctrine_hints", [])
    assert not any("DLQ" in h for h in hints), hints


def test_dlq_downgrade_hint_present_for_guidance_only():
    result = _result({"enabled": True, "target": {"mode": "guidance_only", "kind": "queue", "address": "<<addr>>"}})
    hints = result.get("design_doctrine_hints", [])
    assert any("error_routing_and_dlq" in h for h in hints), hints


# ---------------------------------------------------------------------------
# End-to-end plan-time ref type check (round-trip with an in-spec component)
# ---------------------------------------------------------------------------


def _append_component(spec: dict, key: str, ctype: str) -> dict:
    spec = copy.deepcopy(spec)
    config = {"name": key.replace("_", " ").title()}
    if ctype == "process":
        # Process components require a typed process_kind under the
        # process_kind-required contract; a minimal wrapper_subprocess (one
        # out-of-spec literal process_id call) plans clean as its own step and
        # keeps _effective_component_type -> "process" for the DLQ ref-type check.
        config["process_kind"] = "wrapper_subprocess"
        config["process_calls"] = [
            {"process_id": "99999999-9999-9999-9999-999999999999"}
        ]
    spec["components"].append(
        {
            "key": key,
            "type": ctype,
            "action": "update",
            "component_id": _HANDLER_ID,
            "name": key.replace("_", " ").title(),
            "config": config,
            "depends_on": [],
        }
    )
    return spec


@patch(_PAGINATE, return_value=[])
def test_error_subprocess_ref_round_trips_with_in_spec_subprocess(_mock_pag):
    spec = _emit({"enabled": True, "target": {"mode": "error_subprocess_ref", "process_id": "$ref:dlq_handler"}})
    spec = _append_component(spec, "dlq_handler", "process")
    plan = _build_plan(MagicMock(), {"integration_spec": spec})
    assert plan["_success"] is True, plan
    # The referenced in-spec subprocess must itself plan clean — no step may
    # carry a validation error (it must satisfy the process_kind-required gate).
    assert all(s.get("validation_error") is None for s in plan["steps"]), [
        (s["key"], s.get("validation_error")) for s in plan["steps"]
        if s.get("validation_error") is not None
    ]
    step = next(s for s in plan["steps"] if s["key"] == _main_process(spec)["key"])
    assert step.get("validation_error") is None


@patch(_PAGINATE, return_value=[])
def test_document_cache_ref_round_trips_with_in_spec_documentcache(_mock_pag):
    spec = _emit({"enabled": True, "target": {"mode": "document_cache_ref", "document_cache_id": "$ref:dlq_cache"}})
    spec = _append_component(spec, "dlq_cache", "documentcache")
    plan = _build_plan(MagicMock(), {"integration_spec": spec})
    assert plan["_success"] is True, plan
    step = next(s for s in plan["steps"] if s["key"] == _main_process(spec)["key"])
    assert step.get("validation_error") is None


@patch(_PAGINATE, return_value=[])
def test_wrong_type_dlq_ref_flagged_by_plan_check(_mock_pag):
    spec = _emit({"enabled": True, "target": {"mode": "document_cache_ref", "document_cache_id": "$ref:dlq_cache"}})
    # Wrong type: a connector-settings component where a documentcache is required.
    spec = _append_component(spec, "dlq_cache", "connector-settings")
    plan = _build_plan(MagicMock(), {"integration_spec": spec})
    step = next(s for s in plan["steps"] if s["key"] == _main_process(spec)["key"])
    assert step["validation_error"]["error_code"] == "PROCESS_REF_TYPE_MISMATCH"


# ---------------------------------------------------------------------------
# Archetype-path golden — reaches the verified catcherrors + DLQ catch path
# ---------------------------------------------------------------------------


_build_archetype_process_xml = _corpus.arch_build_archetype_process_xml


def test_archetype_document_cache_ref_matches_golden():
    spec = _emit({"enabled": True, "target": {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID}})
    emitted = _build_archetype_process_xml(spec)
    assert emitted == _GOLDEN.read_text()


def test_archetype_dlq_shape_sequence_is_trycatch_with_map_and_dlq():
    spec = _emit({"enabled": True, "target": {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID}})
    root = ET.fromstring(_build_archetype_process_xml(spec))
    shapes = root.find("bns:object/process", NS).find("shapes").findall("shape")
    # Issue #99 G1: connector scope emits a Try/Catch per connector — the source
    # connector (DB Get) in its own catcherrors and the target connector (REST)
    # in its own catcherrors, separated by the source so the target retry does
    # not re-run the DB read. Each connector gets its own DLQ catch leg.
    assert [s.attrib["shapetype"] for s in shapes] == [
        "start", "catcherrors", "connectoraction", "map", "catcherrors",
        "connectoraction", "stop", "doccacheload", "doccacheload",
    ]
    # Both catch legs route to the same DLQ document cache.
    dlq_legs = [s for s in shapes if s.attrib["shapetype"] == "doccacheload"]
    assert len(dlq_legs) == 2
    for leg in dlq_legs:
        assert leg.find("configuration/doccacheload").attrib["docCache"] == _CACHE_ID
    # The source Try/Catch carries retry 0; the target carries the configured
    # retry (here 0 — the default max_attempts=1).
    catcherrors = [s for s in shapes if s.attrib["shapetype"] == "catcherrors"]
    assert len(catcherrors) == 2
    assert catcherrors[0].find("configuration/catcherrors").attrib["retryCount"] == "0"


# ---------------------------------------------------------------------------
# Issue #89 — catch_notify reaches process.reliability and the emitted XML
# ---------------------------------------------------------------------------

_WIRED_DC = _corpus.ARCH_WIRED_DC
_WIRED_SUB = {"enabled": True, "target": {"mode": "error_subprocess_ref", "process_id": _HANDLER_ID}}


def test_catch_notify_reaches_process_reliability_document_cache():
    spec = _emit(_WIRED_DC, catch_notify=_CATCH_NOTIFY)
    assert _main_process(spec)["config"]["reliability"] == {
        "retry_count": 0,
        "try_catch_scope": "connector",
        "dlq": {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID},
        "catch_notify": {"level": "ERROR", "message_template": _CATCH_NOTIFY["message_template"]},
    }


def test_catch_notify_reaches_process_reliability_error_subprocess():
    spec = _emit(_WIRED_SUB, catch_notify=_CATCH_NOTIFY)
    rel = _main_process(spec)["config"]["reliability"]
    assert rel["dlq"] == {"mode": "error_subprocess_ref", "process_id": _HANDLER_ID}
    assert rel["catch_notify"] == {"level": "ERROR", "message_template": _CATCH_NOTIFY["message_template"]}


def test_catch_notify_level_normalized_to_uppercase():
    spec = _emit(_WIRED_DC, catch_notify={"level": "warning", "message_template": _CATCH_NOTIFY["message_template"]})
    assert _main_process(spec)["config"]["reliability"]["catch_notify"]["level"] == "WARNING"


def _expect_notify_validation_error(catch_notify: dict, dlq: dict | None = _WIRED_DC):
    result = build_from_archetype_action(
        "database_to_api_sync", _params(dlq, None, catch_notify)
    )
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED", result


def test_catch_notify_without_wired_dlq_rejected():
    _expect_notify_validation_error(_CATCH_NOTIFY, dlq={"enabled": False})


def test_catch_notify_with_guidance_only_rejected():
    _expect_notify_validation_error(
        _CATCH_NOTIFY,
        dlq={"enabled": True, "target": {"mode": "guidance_only", "kind": "queue", "address": "<<addr>>"}},
    )


def test_catch_notify_missing_caught_error_token_rejected():
    _expect_notify_validation_error({"level": "ERROR", "message_template": "static text, no token"})


def test_catch_notify_unsupported_level_rejected():
    _expect_notify_validation_error({"level": "SEVERE", "message_template": _CATCH_NOTIFY["message_template"]})


def test_catch_notify_extra_channel_key_rejected():
    _expect_notify_validation_error(
        {"level": "ERROR", "message_template": _CATCH_NOTIFY["message_template"], "email_to": "ops@example.com"}
    )


def test_catch_notify_operational_intent_records_no_echo():
    spec = _emit(_WIRED_DC, catch_notify=_CATCH_NOTIFY)
    notify_intent = _operational_intent(spec)["reliability"]["catch_notify"]
    assert notify_intent == {
        "requested": True,
        "status": "emitted",
        "level": "ERROR",
        "message_template_present": True,
        "references_caught_error_property": True,
    }
    # The message body is never echoed into the intent metadata.
    assert _CATCH_NOTIFY["message_template"] not in str(notify_intent)


def test_archetype_notify_matches_golden():
    spec = _emit(_WIRED_DC, catch_notify=_CATCH_NOTIFY)
    emitted = _build_archetype_process_xml(spec, name="Archetype Notify DLQ Golden")
    assert emitted == _NOTIFY_GOLDEN.read_text()


def test_archetype_notify_shape_sequence():
    spec = _emit(_WIRED_DC, catch_notify=_CATCH_NOTIFY)
    root = ET.fromstring(_build_archetype_process_xml(spec, name="Archetype Notify DLQ Golden"))
    shapes = root.find("bns:object/process", NS).find("shapes").findall("shape")
    # Issue #99 G1: connector scope — each connector's catch leg is the full
    # notify -> dlq route sequence (one per connector). #184 amendment 3: no
    # catch-row Stop follows either DLQ cache load. Add to Cache hands on no
    # documents, so a Stop wired after it never ran (cap184-cache-put-successor).
    assert [s.attrib["shapetype"] for s in shapes] == [
        "start", "catcherrors", "connectoraction", "map", "catcherrors",
        "connectoraction", "stop",
        "notify", "doccacheload",
        "notify", "doccacheload",
    ]
    loads = [s for s in shapes if s.attrib["shapetype"] == "doccacheload"]
    assert len(loads) == 2
    for load in loads:
        assert list(load.find("dragpoints")) == []


# ===========================================================================
# #159 S2 lane L1 — reliability of the typed preset
# ``boomi.archetype.database_to_api_sync`` 1.0.0: connector-scoped Try/Catch,
# the dead-letter cache, catch Notify, and the architect retry decision.
# ===========================================================================

import test_database_to_api_sync_contract as _l1  # noqa: E402  (sibling test module)
import pytest as _pytest  # noqa: E402


def _l1_inputs(case_id="wm_off_dlq_on_create"):
    intent = copy.deepcopy(_l1.l1_case(case_id).input_canonical["intent"])
    invocation = intent["invocations"][0]
    return intent, invocation["raw_input"], invocation["component_material"]


def _l1_root(raw):
    from boomi_mcp.recipes.builtins import database_to_api_sync as preset

    unit = preset.emit_database_to_api_sync(preset.DatabaseToApiSyncInputV1.model_validate(raw))[0].unit
    return unit.process_ir.model_dump(mode="json", exclude_none=True)


def test_l1_root_shapes_follow_the_dead_letter_cache():
    """Without a DLQ: the plain linear chain (the legacy route emits no
    Try/Catch). With one: golden-000086's connector-scoped double guard, retry
    count ZERO in both, no idempotency block, each catch an optional Notify
    then Add to Cache into the DLQ cache."""
    _intent, raw, _material = _l1_inputs("wm_off_dlq_off_create")
    assert [s["kind"] for s in _l1_root(raw)["body"]["steps"]] == ["source", "map_ref", "target", "stop"]

    for case_id in ("wm_off_dlq_on_create", "wm_on_dlq_on_reuse"):
        _intent, raw, _material = _l1_inputs(case_id)
        steps = _l1_root(raw)["body"]["steps"]
        assert [s["kind"] for s in steps] == ["try_catch", "map_ref", "try_catch"], case_id
        read_guard, _map, send_guard = steps
        notify = raw["reliability"].get("catch_notify")
        for guard in (read_guard, send_guard):
            assert guard["scope"] == "connector" and guard["retry"]["count"] == 0
            expected_steps = (
                [{"kind": "notify", "level": notify["level"], "message_template": notify["message_template"]}]
                if notify else []
            )
            assert [
                {k: v for k, v in step.items() if k in ("kind", "level", "message_template")}
                for step in guard["catch_body"]["steps"]
            ] == expected_steps, case_id
            assert guard["catch_body"]["terminal"] == {
                "kind": "cache_put", "cache_ref": "$ref:dlq_cache", "label": "Route caught errors to DLQ cache"}
            (call,) = guard["try_body"]["steps"]
            assert call["kind"] == "connector_call" and "idempotency" not in call
        assert read_guard["try_body"]["terminal"] == {"kind": "continue"}
        assert send_guard["try_body"]["terminal"] == {"kind": "stop"}
        assert read_guard["try_body"]["steps"][0]["operation_ref"] == "$ref:source_db_get_operation"
        assert send_guard["try_body"]["steps"][0]["operation_ref"] == "$ref:target_rest_operation"


def test_l1_catch_notify_refusals():
    """Value-free input refusals: no caught-error token, an unsupported level,
    an extra channel key, and a Notify without a dead-letter cache."""
    from pydantic import ValidationError

    from boomi_mcp.recipes.builtins import database_to_api_sync as preset

    for case_id, catch_notify in (
        ("wm_off_dlq_on_create", {"level": "ERROR", "message_template": "static text, no token"}),
        ("wm_off_dlq_on_create", {"level": "SEVERE", "message_template": _CATCH_NOTIFY["message_template"]}),
        ("wm_off_dlq_on_create", {"level": "ERROR", "message_template": _CATCH_NOTIFY["message_template"],
                                  "email_to": "ops@example.invalid"}),
        ("wm_off_dlq_off_create", dict(_CATCH_NOTIFY)),
    ):
        _intent, raw, _material = _l1_inputs(case_id)
        raw["reliability"]["catch_notify"] = catch_notify
        with _pytest.raises(ValidationError) as caught:
            preset.DatabaseToApiSyncInputV1.model_validate(raw)
        assert "ops@example.invalid" not in str(caught.value.errors(include_input=False))


def test_l1_catch_notify_served_schema_states_the_validator_rules():
    """QA-159-S2-r1-01: the served ``catch_notify`` schema enumerates exactly the
    levels the ProcessIR Notify authority accepts and carries the caught-error
    token rule; the refusal for a violation is the value-free code at the
    field's pointer."""
    from pydantic import ValidationError

    from boomi_mcp.models.process_ir import NotifyNodeV1
    from boomi_mcp.recipes.builtins import database_to_api_sync as preset

    schema = preset.DatabaseToApiSyncInputV1.model_json_schema()
    notify = schema["$defs"]["DbToApiCatchNotifyV1"]["properties"]
    accepted = []
    for level in ("INFO", "WARNING", "ERROR", "SEVERE", "DEBUG", "FATAL"):
        try:
            NotifyNodeV1(kind="notify", level=level, message_template="x meta.base.catcherrorsmessage")
        except ValidationError:
            continue
        accepted.append(level)
    assert accepted and set(notify["level"]["enum"]) == set(accepted)
    assert "meta.base.catcherrorsmessage" in notify["message_template"]["description"]
    # QA-159-S2-r2-01: the served text names the cause a caller actually receives at the
    # authoring boundary, never the model-internal error type, which is not served.
    assert "RECIPE_INPUT_INVALID" in notify["message_template"]["description"]
    assert "db_to_api_catch_notify_invalid" not in notify["message_template"]["description"]

    for case_id, catch_notify in (
        ("wm_off_dlq_on_create", {"level": "SEVERE", "message_template": _CATCH_NOTIFY["message_template"]}),
        ("wm_off_dlq_on_create", {"level": "ERROR", "message_template": "static text, no token"}),
    ):
        _intent, raw, _material = _l1_inputs(case_id)
        raw["reliability"]["catch_notify"] = catch_notify
        with _pytest.raises(ValidationError) as caught:
            preset.DatabaseToApiSyncInputV1.model_validate(raw)
        (error,) = caught.value.errors(include_input=False)
        assert error["type"] == "db_to_api_catch_notify_invalid"
        assert error["loc"][:3] == ("reliability", "catch_notify")
        assert "SEVERE" not in str(error) and "static text" not in str(error)


# --- retry evidence (architect decision) ------------------------------------


def test_l1_retry_above_one_is_refused_not_reduced():
    """The frozen ``wm_on_dlq_on_reuse`` legacy input asks for 5 attempts. The
    preset refuses 2..6 — in the preparer's FIRST step and in the executor —
    with ``DB_TO_API_RETRY_UNAVAILABLE``, before anything is assembled."""
    from boomi_mcp.recipes.builtins import database_to_api_sync as preset
    from boomi_mcp.recipes.errors import RecipeError

    for attempts in (2, 5, 6):
        _intent, raw, material = _l1_inputs("wm_on_dlq_on_reuse")
        raw["reliability"]["retry"]["max_attempts"] = attempts
        inp = preset.DatabaseToApiSyncInputV1.model_validate(raw)
        for call in (
            # An EMPTY base catalog: a step past the retry rule would refuse the DLQ base.
            lambda: preset.prepare_database_to_api_sync(
                inp, preset.DatabaseToApiSyncMaterialV1.model_validate(material), base_catalog={}),
            lambda: preset.emit_database_to_api_sync(inp),
        ):
            with _pytest.raises(RecipeError) as caught:
                call()
            (diagnostic,) = caught.value.diagnostics
            assert diagnostic.code == "RECIPE_CONSTRAINT_FAILED"
            assert diagnostic.cause_codes == ("DB_TO_API_RETRY_UNAVAILABLE",)
            assert diagnostic.path == "/raw_input/reliability/retry/max_attempts"
            # #159 S2: the cause's own static remediation names the follow-up.
            assert "issue #199" in diagnostic.remediation
    # Control: another single cause, and the same cause beside a second one,
    # keep the code's generic remediation.
    from boomi_mcp.recipes.errors import recipe_diagnostic

    generic = recipe_diagnostic("RECIPE_CONSTRAINT_FAILED", phase="input", cause_codes=("OTHER",))
    paired = recipe_diagnostic(
        "RECIPE_CONSTRAINT_FAILED", phase="input",
        cause_codes=("DB_TO_API_RETRY_UNAVAILABLE", "OTHER"),
    )
    assert "#199" not in generic.remediation and generic.remediation == paired.remediation


def test_l1_retry_five_is_refused_through_the_public_planner():
    from boomi_mcp.authoring.workflow import AuthoringWorkflowError

    request = copy.deepcopy(_l1.l1_case("wm_on_dlq_on_reuse").input_canonical)
    request["intent"]["invocations"][0]["raw_input"]["reliability"]["retry"]["max_attempts"] = 5
    with _pytest.raises(AuthoringWorkflowError) as caught:
        _l1.l1_plan(request, client=None)
    assert "DB_TO_API_RETRY_UNAVAILABLE" in str(caught.value.diagnostics)


def test_l1_retry_remediation_reaches_the_mcp_result():
    """#159 S2: through the MCP-level dispatch (build_integration_action plan),
    a DB->API 1.0.0 invocation asking for 5 attempts serves the cause's own
    #199 remediation; a refusal of another cause keeps the channel's fixed text."""
    from unittest.mock import MagicMock, patch

    from boomi_mcp.categories import integration_builder

    def served(mutate):
        request = copy.deepcopy(_l1.l1_case("wm_off_dlq_off_create").input_canonical)
        mutate(request["intent"]["invocations"][0])
        with patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []):
            envelope = integration_builder.build_integration_action(
                MagicMock(), "issue-157-replay", "plan", {"authoring_request": request}
            )
        assert envelope["_success"] is False and envelope["mutation_performed"] is False
        (diagnostic,) = envelope["authoring_diagnostics"]
        return diagnostic

    def five_attempts(invocation):
        invocation["raw_input"]["reliability"]["retry"]["max_attempts"] = 5

    def key_collision(invocation):
        invocation["raw_input"]["process_key"] = "source_db_connection"

    retry = served(five_attempts)
    assert retry["cause_codes"] == ["DB_TO_API_RETRY_UNAVAILABLE", "RECIPE_CONSTRAINT_FAILED"]
    assert retry["path"] == "/intent/invocations/0/raw_input/reliability/retry/max_attempts"
    assert "issue #199" in retry["remediation"] and "max_attempts to 1" in retry["remediation"]
    other = served(key_collision)
    assert "DB_TO_API_RETRY_UNAVAILABLE" not in other["cause_codes"]
    assert other["remediation"].startswith("Fix the recipe input or invocation and re-plan.")


def test_l1_retry_input_is_strict_and_carries_no_idempotency_field():
    from pydantic import ValidationError

    from boomi_mcp.recipes.builtins import database_to_api_sync as preset

    for retry in ({"max_attempts": 0}, {"max_attempts": 7}, {"max_attempts": "1"}, {"max_attempts": True},
                  {"max_attempts": 1.0},
                  {"max_attempts": 1, "idempotency": {"kind": "key_reference", "contract_ref": "$ref:x"}}):
        _intent, raw, _material = _l1_inputs("wm_off_dlq_on_create")
        raw["reliability"]["retry"] = retry
        with _pytest.raises(ValidationError):
            preset.DatabaseToApiSyncInputV1.model_validate(raw)


# --- the dead-letter cache ------------------------------------------------------


def test_l1_dlq_cache_must_be_an_existing_reference_only_document_cache():
    intent, raw, material = _l1_inputs("wm_off_dlq_on_create")
    (cache,) = intent["base_components"]
    missing = _l1._l1_refusal(raw, material, ())
    assert missing == (("COMPOSITION_CONTRACT_MISMATCH",), "/raw_input/reliability/dlq/cache_key")
    wrong_type = copy.deepcopy(cache)
    wrong_type["type"] = "connector-settings"
    assert _l1._l1_refusal(raw, material, (wrong_type,)) == missing
    written = copy.deepcopy(cache)
    written["config"].pop("reference_only")
    assert _l1._l1_refusal(raw, material, (written,)) == missing
    prepared = _l1._l1_prepare(raw, material, (cache,))
    assert "dlq_cache" in prepared.selected_base_keys
    assert prepared.projection_facts.reliability.dlq_component_key == "dlq_cache"


def _l1_dlq_plan_causes(xml):
    request = _l1.l1_case("wm_off_dlq_on_create").input_canonical
    cache_id = request["intent"]["base_components"][0]["config"]["component_id"]
    result, _internals = _l1.l1_plan(request, answers=_l1.l1_cache_answers(cache_id, xml))
    return [(d.cause_codes, d.path) for d in result.errors]


def test_l1_dlq_cache_stored_profile_decides_the_plan():
    """The workflow reads the cache's STORED XML; the compiler's profile rule
    admits an unprofiled cache and refuses one that is profiled (it cannot
    admit both the DB and the JSON documents the two catch legs route) or that
    cannot be read."""
    assert _l1_dlq_plan_causes(_l1.l1_unprofiled_cache_xml()) == []
    for xml in (_l1.l1_profiled_cache_xml(), None):
        causes = _l1_dlq_plan_causes(xml)
        assert causes and all(c == ("PROCESS_IR_SEMANTIC_PROFILE_MISMATCH",) for c, _p in causes), causes
        assert {p.rsplit("/", 1)[-1] for _c, p in causes} == {"cache_ref"}


# --- process XML parity with the legacy route ---------------------------------


def _l1_canonical_process_xml(dlq=None, catch_notify=None, name="Archetype DLQ Golden"):
    """The preset's materialized process for ``arch_params`` (the archetype
    goldens' own input), through the public plan -> compile -> late binding ->
    canonical materialization, with the goldens' frozen ids."""
    from boomi_mcp.authoring.contract import get_authoring_revisions
    from boomi_mcp.authoring.process_materialization import build_materialization_plan
    from boomi_mcp.categories.components.canonical_process_apply import (
        materialize_canonical_process_xml,
    )
    from boomi_mcp.categories.integration_builder import _materializer_revision
    from boomi_mcp.compiler.process_ir.emitter_registry import emitter_revision

    legacy = {"parameters": _params(copy.deepcopy(dlq) if dlq else None, None, catch_notify)}
    if dlq:
        legacy["parameters"]["reliability"]["dlq"]["target"]["document_cache_id"] = _corpus.ARCH_CACHE_ID
    request = _l1.l1_canonical_request("arch_golden", legacy, "Map source to JSON payload")
    raw = request["intent"]["invocations"][0]["raw_input"]
    raw["naming"].update({"process_name": name, "folder_name": "Golden/Fixtures"})
    result, internals = _l1.l1_plan(request, client=None)
    assert not result.errors, [(d.cause_codes, d.path) for d in result.errors]
    unit = internals.normalized.integration_spec.processes[0]
    plan = build_materialization_plan(
        envelope=unit.envelope,
        process_ir=unit.process_ir,
        symbols=internals.symbols,
        conflict_policy="reuse",
        compiler_revision=get_authoring_revisions()["compiler_revision"],
        emitter_revision=emitter_revision(),
        materializer_revision=_materializer_revision(),
    )
    ids = {
        "source_db_connection": _corpus.ARCH_DB_CONN_ID,
        "source_db_get_operation": _corpus.ARCH_DB_OP_ID,
        "transform_transform_map": _corpus.ARCH_MAP_ID,
        "target_rest_connection": _corpus.ARCH_REST_CONN_ID,
        "target_rest_operation": _corpus.ARCH_REST_OP_ID,
        "dlq_cache": _corpus.ARCH_CACHE_ID,
    }
    return materialize_canonical_process_xml(plan=plan, id_registry=ids, symbols=internals.symbols)


def test_l1_process_xml_equals_the_archetype_goldens_and_the_legacy_builder():
    """The frozen archetype goldens (DLQ: ``try_catch_dlq_document_cache_archetype.xml``;
    notify DLQ: golden-000088 ``issue184_cache_notify_archetype_terminal.xml``,
    frozen independently of both builders) and, without a DLQ, the legacy
    builder's own rendering of the same input."""
    assert _l1_canonical_process_xml(dlq=_WIRED_DC) == _GOLDEN.read_text()
    assert _l1_canonical_process_xml(
        dlq=_WIRED_DC, catch_notify=_CATCH_NOTIFY, name="Archetype Notify DLQ Golden"
    ) == _NOTIFY_GOLDEN.read_text()
    assert _l1_canonical_process_xml(name="Archetype DLQ Golden") == _build_archetype_process_xml(_emit())


_L1_XML_IDS = {
    "source_db_connection": _corpus.ARCH_DB_CONN_ID,
    "source_db_get_operation": _corpus.ARCH_DB_OP_ID,
    "transform_transform_map": _corpus.ARCH_MAP_ID,
    "target_rest_connection": _corpus.ARCH_REST_CONN_ID,
    "target_rest_operation": _corpus.ARCH_REST_OP_ID,
    "dlq_cache": _corpus.ARCH_CACHE_ID,
}


def test_l1_each_frozen_case_materializes_the_legacy_builder_process():
    """Per frozen case: the preset's canonical process XML equals the legacy
    builder's rendering of the legacy archetype's process for the SAME case,
    under the same late-bound ids. The one exact difference is the measured
    retry restriction: ``wm_on_dlq_on_reuse``'s legacy send guard retries 4
    times; the preset refuses that input, so its fixture carries one attempt."""
    from boomi_mcp.authoring.contract import get_authoring_revisions
    from boomi_mcp.authoring.process_materialization import build_materialization_plan
    from boomi_mcp.categories.components.builders import ProcessFlowBuilder
    from boomi_mcp.categories.components.canonical_process_apply import (
        materialize_canonical_process_xml,
    )
    from boomi_mcp.categories.integration_builder import (
        _materializer_revision,
        _resolve_dependency_tokens,
    )
    from boomi_mcp.compiler.process_ir.emitter_registry import emitter_revision

    for case_id in _l1.L1_CASE_IDS:
        process = next(c for c in _l1._l1_legacy_spec(case_id)["components"] if c["type"] == "process")
        config = copy.deepcopy(process["config"])
        dlq = config.get("reliability", {}).get("dlq", {})
        if dlq.get("document_cache_id"):
            dlq["document_cache_id"] = _L1_XML_IDS["dlq_cache"]
        legacy = ProcessFlowBuilder.build(
            _resolve_dependency_tokens(config, _L1_XML_IDS),
            name=process["name"], folder_name=config.get("folder_name"),
        )
        if case_id == "wm_on_dlq_on_reuse":
            assert legacy.count('retryCount="4"') == 1 and legacy.count("(retry 4)") == 1
            legacy = legacy.replace('retryCount="4"', 'retryCount="0"').replace("(retry 4)", "(no retry)")
        result, internals = _l1.l1_plan(_l1.l1_case(case_id).input_canonical, client=None)
        assert not result.errors, (case_id, [(d.cause_codes, d.path) for d in result.errors])
        unit = internals.normalized.integration_spec.processes[0]
        plan = build_materialization_plan(
            envelope=unit.envelope, process_ir=unit.process_ir, symbols=internals.symbols,
            conflict_policy="reuse",
            compiler_revision=get_authoring_revisions()["compiler_revision"],
            emitter_revision=emitter_revision(), materializer_revision=_materializer_revision(),
        )
        emitted = materialize_canonical_process_xml(
            plan=plan, id_registry=dict(_L1_XML_IDS), symbols=internals.symbols
        )
        assert emitted == legacy, case_id
