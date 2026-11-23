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

from boomi_mcp.categories.integration_builder import _build_plan

# The golden-case definitions live in the corpus (#165); this module CONSUMES
# them. The aliases keep every existing call site and assertion unchanged.
import _wave_gate_golden_corpus as _corpus


def build_from_archetype_action(*args, **kwargs):
    """The retired ``build_from_archetype`` tool's action (#159 S3 deletes it).

    Resolved at CALL time, so this module still collects once it is gone and only
    the tests that call it are affected.
    """
    from boomi_mcp.categories import integration_authoring

    return getattr(integration_authoring, "build_from_archetype_action")(*args, **kwargs)


NS = {"bns": "http://api.platform.boomi.com/"}
_PAGINATE = "boomi_mcp.categories.integration_builder.paginate_metadata"

_CACHE_ID = _corpus.ARCH_CACHE_ID
_HANDLER_ID = "77777777-7777-7777-7777-777777777777"

# #159 S3: golden-000056 (``try_catch_dlq_document_cache_archetype.xml``) and
# golden-000088 (``issue184_cache_notify_archetype_terminal.xml``) retire with the
# archetype route that rendered them, and their files are removed. The DLQ byte
# behaviour is re-homed onto the L1 preset (``boomi.archetype.database_to_api_sync``
# 1.0.0), whose materialized XML equalled both files byte for byte (S2-D7): the
# frozen files' SHA-256 digests, taken from the committed bytes before removal,
# are the oracle. Never regenerate them from either route.
_GOLDEN_SHA256 = "3a872dc87065c625ea044d4b6334300d1a880079f26bfba1e5679d3bcda7a5c4"

# #184 amendment 3: golden-000060 (`try_catch_notify_dlq_document_cache_archetype.xml`)
# is retired. Each connector's catch leg ran notify -> doccacheload -> Stop, and Add
# to Cache hands on no documents, so neither Stop ever ran. The replacement
# golden-000088 was frozen independently of this builder, as a byte transform of the
# retired golden (tests/fixtures/process_ir/issue184/PROVENANCE.md). Never
# regenerate it here.
_NOTIFY_GOLDEN_SHA256 = "4845f0319ac55e93cdaa03b91a72d135d914594d262786b1ba2b40c52a133cab"


def _sha256(text: str) -> str:
    import hashlib

    return hashlib.sha256(text.encode("utf-8")).hexdigest()

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


# ---------------------------------------------------------------------------
# Contract validation — no silent acceptance of unimplementable input
# ---------------------------------------------------------------------------


def _expect_validation_error(dlq: dict):
    result = build_from_archetype_action("database_to_api_sync", _params(dlq))
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED", result
    return result


# ---------------------------------------------------------------------------
# RetryPolicy contract reconciliation (#88): bounds, mapping, no over-promise
# ---------------------------------------------------------------------------

_WIRED_DLQ = {"enabled": True, "target": {"mode": "document_cache_ref", "document_cache_id": _CACHE_ID}}


def _expect_retry_validation_error(retry: dict, dlq: dict | None = None):
    result = build_from_archetype_action(
        "database_to_api_sync", _params(dlq, retry)
    )
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED", result


# ---------------------------------------------------------------------------
# Operational intent
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Cross-issue interaction with #86 build_from_archetype design_doctrine_hints:
# a WIRED DLQ must NOT trigger the "DLQ kept disabled" downgrade hint.
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Archetype-path golden — reaches the verified catcherrors + DLQ catch path
# ---------------------------------------------------------------------------


_build_archetype_process_xml = _corpus.arch_build_archetype_process_xml


def test_archetype_document_cache_ref_matches_golden():
    """#159 S3: re-homed onto the L1 preset — its materialized DLQ process is the
    retired golden-000056's frozen bytes (digest oracle above)."""
    assert _sha256(_l1_canonical_process_xml(dlq=_WIRED_DC)) == _GOLDEN_SHA256


# ---------------------------------------------------------------------------
# Issue #89 — catch_notify reaches process.reliability and the emitted XML
# ---------------------------------------------------------------------------

_WIRED_DC = _corpus.ARCH_WIRED_DC
_WIRED_SUB = {"enabled": True, "target": {"mode": "error_subprocess_ref", "process_id": _HANDLER_ID}}


def _expect_notify_validation_error(catch_notify: dict, dlq: dict | None = _WIRED_DC):
    result = build_from_archetype_action(
        "database_to_api_sync", _params(dlq, None, catch_notify)
    )
    assert result["_success"] is False
    assert result["error_code"] == "PARAM_VALIDATION_FAILED", result


def test_archetype_notify_matches_golden():
    """#159 S3: re-homed onto the L1 preset — its materialized notify DLQ process
    is the retired golden-000088's frozen bytes (digest oracle above)."""
    assert _sha256(_l1_canonical_process_xml(
        dlq=_WIRED_DC, catch_notify=_CATCH_NOTIFY, name="Archetype Notify DLQ Golden"
    )) == _NOTIFY_GOLDEN_SHA256


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
    """The frozen archetype goldens, by the digests recorded from their committed
    bytes before #159 S3 removed them (DLQ: golden-000056
    ``try_catch_dlq_document_cache_archetype.xml``; notify DLQ: golden-000088
    ``issue184_cache_notify_archetype_terminal.xml``, frozen independently of
    both builders).

    #159 S3: the third arm — the no-DLQ case against the legacy archetype route's
    own rendering — retired with that route. It had no recorded digest, and one
    minted now would come from the code under test; the no-DLQ legacy-builder
    parity stays covered per frozen case by
    ``test_l1_each_frozen_case_materializes_the_legacy_builder_process``."""
    assert _sha256(_l1_canonical_process_xml(dlq=_WIRED_DC)) == _GOLDEN_SHA256
    assert _sha256(_l1_canonical_process_xml(
        dlq=_WIRED_DC, catch_notify=_CATCH_NOTIFY, name="Archetype Notify DLQ Golden"
    )) == _NOTIFY_GOLDEN_SHA256


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
