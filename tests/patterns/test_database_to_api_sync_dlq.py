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

import pytest

_src = str(Path(__file__).resolve().parent.parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.categories.integration_authoring import build_from_archetype_action
from boomi_mcp.categories.integration_builder import (
    _build_plan,
    _resolve_dependency_tokens,
)
from boomi_mcp.categories.components.builders import ProcessFlowBuilder

NS = {"bns": "http://api.platform.boomi.com/"}
_PAGINATE = "boomi_mcp.categories.integration_builder.paginate_metadata"

# Fixed ids for the archetype-path golden (mirror the builder golden's ids so
# the only structural delta is the archetype's map shape).
_DB_CONN_ID = "11111111-1111-1111-1111-111111111111"
_DB_OP_ID = "22222222-2222-2222-2222-222222222222"
_REST_CONN_ID = "33333333-3333-3333-3333-333333333333"
_REST_OP_ID = "44444444-4444-4444-4444-444444444444"
_CACHE_ID = "55555555-5555-5555-5555-555555555555"
_MAP_ID = "66666666-6666-6666-6666-666666666666"
_HANDLER_ID = "77777777-7777-7777-7777-777777777777"

_GOLDEN = (
    Path(__file__).resolve().parent.parent
    / "fixtures"
    / "golden_xml"
    / "try_catch_dlq_document_cache_archetype.xml"
)

_NOTIFY_GOLDEN = (
    Path(__file__).resolve().parent.parent
    / "fixtures"
    / "golden_xml"
    / "try_catch_notify_dlq_document_cache_archetype.xml"
)

# Issue #89: placeholder catch_notify (references the caught-error property).
_CATCH_NOTIFY = {
    "level": "ERROR",
    "message_template": "Sync failed; caught error: meta.base.catcherrorsmessage",
}


def _params(
    dlq: dict | None = None,
    retry: dict | None = None,
    catch_notify: dict | None = None,
) -> dict:
    """Smallest executable create/create payload; DLQ/retry/notify overridable."""
    reliability: dict = {
        "retry": retry or {"max_attempts": 1},
        "dlq": dlq if dlq is not None else {"enabled": False},
        "error_classifier": {},
    }
    if catch_notify is not None:
        reliability["catch_notify"] = catch_notify
    return {
        "naming": {"integration_name": "demo-sync", "component_prefix": "DEMO"},
        "source": {
            "binding": {
                "mode": "create",
                "settings": {
                    "driver": "microsoft_jdbc",
                    "auth_mode": "username_password",
                    "host": "db.internal",
                    "database": "AppDB",
                    "username": "svc_sync",
                    "credential_ref": "secrets/db/svc_sync",
                },
            },
            "read_operation": {
                "sql": "<<user-authored DB read statement>>",
                "result_schema": {"fields": [{"name": "source_a", "data_type": "character"}]},
            },
        },
        "target": {
            "binding": {"mode": "create", "settings": {"base_url": "https://api.example.com", "auth_mode": "none"}},
            "send_request": {"method": "POST", "path": "/v1/items"},
            "payload_profile": {
                "format": "json",
                "root": {
                    "name": "Root",
                    "kind": "object",
                    "children": [{"name": "target_a", "kind": "simple", "data_type": "character"}],
                },
            },
        },
        "transform": {
            "operations": [
                {"operation_type": "direct", "source_field": "source_a", "target_path": "Root/target_a"}
            ]
        },
        "execution": {"trigger": {"mode": "manual"}},
        "reliability": reliability,
    }


def _result(
    dlq: dict | None = None, retry: dict | None = None, catch_notify: dict | None = None
) -> dict:
    result = build_from_archetype_action(
        "database_to_api_sync", _params(dlq, retry, catch_notify)
    )
    assert result["_success"] is True, result
    return result


def _emit(
    dlq: dict | None = None, retry: dict | None = None, catch_notify: dict | None = None
) -> dict:
    return _result(dlq, retry, catch_notify)["integration_spec"]


def _main_process(spec: dict) -> dict:
    return next(c for c in spec["components"] if c["type"] == "process")


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


def _build_archetype_process_xml(spec: dict, name: str = "Archetype DLQ Golden") -> str:
    cfg = _main_process(spec)["config"]

    def _rk(token: str) -> str:
        return token[len("$ref:"):]

    registry = {
        _rk(cfg["source"]["connection_id"]): _DB_CONN_ID,
        _rk(cfg["source"]["operation_id"]): _DB_OP_ID,
        _rk(cfg["transform"]["map_ref"]): _MAP_ID,
        _rk(cfg["target"]["connection_id"]): _REST_CONN_ID,
        _rk(cfg["target"]["operation_id"]): _REST_OP_ID,
    }
    resolved = _resolve_dependency_tokens(cfg, registry)
    return ProcessFlowBuilder.build(resolved, name=name, folder_name="Golden/Fixtures")


def test_archetype_document_cache_ref_matches_golden():
    spec = _emit({"enabled": True, "target": {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID}})
    emitted = _build_archetype_process_xml(spec)
    assert ET.canonicalize(emitted) == ET.canonicalize(_GOLDEN.read_text())


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

_WIRED_DC = {"enabled": True, "target": {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID}}
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
    assert ET.canonicalize(emitted) == ET.canonicalize(_NOTIFY_GOLDEN.read_text())


def test_archetype_notify_shape_sequence():
    spec = _emit(_WIRED_DC, catch_notify=_CATCH_NOTIFY)
    root = ET.fromstring(_build_archetype_process_xml(spec, name="Archetype Notify DLQ Golden"))
    shapes = root.find("bns:object/process", NS).find("shapes").findall("shape")
    # Issue #99 G1: connector scope — each connector's catch leg is the full
    # notify -> dlq route -> catch stop sequence (one per connector).
    assert [s.attrib["shapetype"] for s in shapes] == [
        "start", "catcherrors", "connectoraction", "map", "catcherrors",
        "connectoraction", "stop",
        "notify", "doccacheload", "stop",
        "notify", "doccacheload", "stop",
    ]
