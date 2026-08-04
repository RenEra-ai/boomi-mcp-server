"""#146 (M12.11): typed apply, build provenance, and drift-aware verify.

Drives the real ``build_integration_action`` dispatcher with the network
boundary faked — the offline tool-layer recipe this repo already uses — so the
routing, preflight ordering and build-record shape are exercised for real rather
than asserted about in isolation.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _m12_11_support import appliable_request, process_ir_request  # noqa: E402
from boomi_mcp.authoring.workflow import compile_authoring_request_v1  # noqa: E402
from boomi_mcp.categories import integration_builder  # noqa: E402
from boomi_mcp.categories.integration_builder import (  # noqa: E402
    _BUILD_REGISTRY,
    build_integration_action,
)
from boomi_mcp.errors import (  # noqa: E402
    AUTHORING_APPLY_VALIDATION_REQUIRED,
    AUTHORING_LIVE_DEPLOYMENT_DRIFT,
    INVALID_INPUT,
)

_PAGINATE = "boomi_mcp.categories.integration_builder.paginate_metadata"
_EXECUTE = "boomi_mcp.categories.integration_builder._execute_component"
_GET_XML = "boomi_mcp.categories.integration_builder.component_get_xml"

_PROFILE = "qa_profile"

#: A syntactically real process XML, so the graph verifier has something to parse.
_LIVE_XML = (
    '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
    'type="connector-settings"><bns:object><connection/></bns:object>'
    "</bns:Component>"
)


@pytest.fixture(autouse=True)
def _clean_registry():
    _BUILD_REGISTRY.clear()
    yield
    _BUILD_REGISTRY.clear()


@pytest.fixture(autouse=True)
def _no_live_metadata_queries(monkeypatch):
    """Cut the metadata query at the network boundary for EVERY test here.

    Plan and compile resolve references read-only, which reaches
    ``paginate_metadata``. Left unpatched against a ``MagicMock`` client, that
    helper's ``while result.query_token`` loop never terminates — a mock's
    attribute is always truthy — so the test hangs rather than fails. Patching it
    once, for the whole module, keeps every test on the offline boundary this
    repo's QA recipe already uses.
    """
    monkeypatch.setattr(integration_builder, "paginate_metadata", lambda *a, **k: [])


#: A typed apply can only materialize a plan the existing builders can emit. A
#: direct ProcessIR intent is plan/compile-only by design (see
#: test_a_direct_process_ir_intent_cannot_be_applied), so the apply fixtures below
#: use an integration_spec intent — otherwise every apply assertion here would be
#: unreachable behind the materialization refusal.
def _bound_payload(profile=_PROFILE):
    # A CLIENT is passed, matching what the tool layer always does. The
    # component-plan lint's result is part of the plan evidence and therefore of
    # the hash, so a binding compiled without a client cannot satisfy an apply
    # made with one — see
    # test_a_binding_compiled_without_the_lint_cannot_satisfy_an_apply_with_it.
    result, _ = compile_authoring_request_v1(
        appliable_request(), boomi_client=MagicMock(), profile=profile
    )
    binding = result.revision_binding
    payload = appliable_request().model_dump(mode="json")
    payload["expected_capability_revision"] = binding.capability_revision
    payload["expected_compile_hash"] = binding.compile_hash
    return payload


def _apply(payload, xml=_LIVE_XML):
    with patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _GET_XML
    ) as get_xml:
        paginate.return_value = []
        execute.side_effect = lambda *a, **k: {
            "_success": True,
            "component_id": "cid-" + str(len(execute.mock_calls)),
        }
        get_xml.return_value = {"type": "connector-settings", "xml": xml}
        return build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            config={"authoring_request": payload, "dry_run": False},
        )


# ---------------------------------------------------------------------------
# routing
# ---------------------------------------------------------------------------


def test_compile_requires_a_typed_request():
    result = build_integration_action(MagicMock(), _PROFILE, "compile", config={})
    assert result["_success"] is False
    assert result["error_code"] == INVALID_INPUT
    assert "AuthoringRequestV1" in result["hint"]
    assert result["mutation_performed"] is False


def test_compile_routes_through_the_dispatcher():
    result = build_integration_action(
        MagicMock(),
        _PROFILE,
        "compile",
        config={"authoring_request": process_ir_request().model_dump(mode="json")},
    )
    assert result["_success"] is True
    assert result["action"] == "compile"
    assert result["mutation_performed"] is False
    assert result["profile"] == _PROFILE
    assert "build_id" not in result
    assert result["authoring_result"]["artifact_fingerprints"]


def test_a_typed_plan_routes_to_the_typed_path():
    with patch(_PAGINATE) as paginate:
        paginate.return_value = []
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            "plan",
            config={"authoring_request": process_ir_request().model_dump(mode="json")},
        )
    assert result["_success"] is True
    assert result["mutation_performed"] is False
    assert result["authoring_result"]["phase"] == "plan"


@pytest.mark.parametrize("action", ["plan", "compile", "apply"])
def test_a_config_carrying_both_roots_is_rejected(action):
    """Never resolved by precedence — a precedence rule silently ignores one of
    the two payloads, and the caller cannot tell which."""
    result = build_integration_action(
        MagicMock(),
        _PROFILE,
        action,
        config={
            "authoring_request": process_ir_request().model_dump(mode="json"),
            "integration_spec": {"name": "x", "components": []},
            "dry_run": False,
        },
    )
    assert result["_success"] is False
    assert result["error_code"] == INVALID_INPUT
    assert "mutually exclusive" in result["error"]


def test_an_unknown_action_lists_all_four():
    result = build_integration_action(MagicMock(), _PROFILE, "nope", config={})
    assert result["_success"] is False
    for action in ("plan", "compile", "apply", "verify"):
        assert action in result["hint"]


# ---------------------------------------------------------------------------
# the apply gate
# ---------------------------------------------------------------------------


def test_an_unbound_typed_apply_mutates_nothing():
    with patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute:
        paginate.return_value = []
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            config={
                "authoring_request": appliable_request().model_dump(mode="json"),
                "dry_run": False,
            },
        )
        assert execute.call_count == 0
    assert result["_success"] is False
    assert result["error_code"] == AUTHORING_APPLY_VALIDATION_REQUIRED
    assert result["mutation_performed"] is False


def test_the_preflight_runs_before_the_first_materializer_call():
    """Ordering, recorded — not inferred from the fact that nothing happened."""
    order = []

    import boomi_mcp.authoring.workflow as workflow

    real_preflight = workflow.preflight_typed_apply_v1

    def _recording_preflight(*args, **kwargs):
        order.append("preflight")
        return real_preflight(*args, **kwargs)

    payload = _bound_payload()
    with patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute, patch(
        _GET_XML
    ) as get_xml, patch.object(
        workflow, "preflight_typed_apply_v1", _recording_preflight
    ):
        paginate.return_value = []
        get_xml.return_value = {"type": "connector-settings", "xml": _LIVE_XML}

        def _execute(*args, **kwargs):
            order.append("execute")
            return {"_success": True, "component_id": "cid-1"}

        execute.side_effect = _execute
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            config={"authoring_request": payload, "dry_run": False},
        )

    assert result["_success"] is True
    assert order[0] == "preflight"
    assert "execute" in order


# ---------------------------------------------------------------------------
# provenance + verify
# ---------------------------------------------------------------------------


def test_a_typed_apply_records_provenance_and_keeps_the_legacy_shape():
    result = _apply(_bound_payload())
    assert result["_success"] is True
    build_id = result["build_id"]
    for key in ("build_id", "message", "execution_order", "results"):
        assert key in result, key

    record = _BUILD_REGISTRY[build_id]
    # The five original keys survive untouched...
    for key in ("created_at", "profile", "spec", "results", "execution_order"):
        assert key in record, key
    # ...and provenance rides in ONE optional extra key.
    assert set(record) == {
        "created_at",
        "profile",
        "spec",
        "results",
        "execution_order",
        "authoring",
    }
    assert record["authoring"]["live_component_fingerprints"]


def test_a_legacy_build_record_keeps_exactly_its_five_keys():
    with patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute:
        paginate.return_value = []
        execute.return_value = {"_success": True, "component_id": "cid-legacy"}
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            config={
                "dry_run": False,
                "integration_spec": {
                    "name": "Legacy",
                    "components": [
                        {
                            "key": "c1",
                            "type": "connector-settings",
                            "name": "Legacy Conn",
                            "config": {"connector_type": "http"},
                        }
                    ],
                },
            },
        )
    assert result["_success"] is True
    record = _BUILD_REGISTRY[result["build_id"]]
    assert set(record) == {
        "created_at",
        "profile",
        "spec",
        "results",
        "execution_order",
    }
    assert "authoring" not in record
    assert "mutation_performed" not in result


def test_verify_of_a_legacy_build_has_no_authoring_provenance_key():
    """ABSENT, not null — so no existing verify assertion changes."""
    with patch(_PAGINATE) as paginate, patch(_EXECUTE) as execute:
        paginate.return_value = []
        execute.return_value = {"_success": True, "component_id": "cid-legacy"}
        applied = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            config={
                "dry_run": False,
                "integration_spec": {
                    "name": "Legacy",
                    "components": [
                        {
                            "key": "c1",
                            "type": "connector-settings",
                            "name": "Legacy Conn",
                            "config": {"connector_type": "http"},
                        }
                    ],
                },
            },
        )
    with patch(_GET_XML) as get_xml:
        get_xml.return_value = {"type": "connector-settings", "xml": "<x/>"}
        verified = build_integration_action(
            MagicMock(), _PROFILE, "verify", config={"build_id": applied["build_id"]}
        )
    for key in (
        "_success",
        "build_id",
        "verified_components",
        "failed_components",
        "dependency_issues",
        "verification",
        "profile",
    ):
        assert key in verified, key
    assert "authoring_provenance" not in verified


def test_verify_of_an_unchanged_typed_build_reports_match():
    applied = _apply(_bound_payload())
    with patch(_GET_XML) as get_xml:
        get_xml.return_value = {"type": "connector-settings", "xml": _LIVE_XML}
        verified = build_integration_action(
            MagicMock(), _PROFILE, "verify", config={"build_id": applied["build_id"]}
        )
    provenance = verified["authoring_provenance"]
    assert provenance["live_comparison"]["status"] == "match"
    assert provenance["live_comparison"]["revision_skew"] == "match"
    # This build's plan has no process, so it has no artifact fingerprints — the
    # live comparison rests on the apply-time component fingerprints instead.
    assert provenance["revision_binding"]["compile_hash"]
    assert provenance["live_comparison"]["drifted_components"] == []


def test_verify_detects_an_out_of_band_component_edit():
    applied = _apply(_bound_payload())
    tampered = _LIVE_XML.replace("<connection/>", "<connection edited='yes'/>")
    with patch(_GET_XML) as get_xml:
        get_xml.return_value = {"type": "connector-settings", "xml": tampered}
        verified = build_integration_action(
            MagicMock(), _PROFILE, "verify", config={"build_id": applied["build_id"]}
        )
    comparison = verified["authoring_provenance"]["live_comparison"]
    assert comparison["status"] == "drift"
    assert comparison["drifted_components"]
    assert verified["_success"] is False
    codes = {d["code"] for d in comparison["diagnostics"]}
    assert codes == {AUTHORING_LIVE_DEPLOYMENT_DRIFT}


def test_verify_never_returns_the_fetched_xml():
    """It is hashed, and the bytes stay inside the server."""
    applied = _apply(_bound_payload())
    with patch(_GET_XML) as get_xml:
        get_xml.return_value = {"type": "connector-settings", "xml": _LIVE_XML}
        verified = build_integration_action(
            MagicMock(), _PROFILE, "verify", config={"build_id": applied["build_id"]}
        )
    blob = repr(verified)
    assert "<bns:Component" not in blob
    assert "<shapes/>" not in blob


def test_revision_skew_is_reported_separately_from_component_drift(monkeypatch):
    """An upgraded server and an edited component need different remedies, so
    they must not collapse into one status."""
    from boomi_mcp.authoring import workflow

    applied = _apply(_bound_payload())
    record = _BUILD_REGISTRY[applied["build_id"]]
    # Simulate a server whose capability revision moved since the build.
    record["authoring"]["revision_binding"]["capability_revision"] = (
        "sha256:" + "0" * 64
    )
    comparison = workflow.compare_live_build_provenance(
        record["authoring"],
        {
            key: value["digest"]
            for key, value in record["authoring"]["live_component_fingerprints"].items()
        },
    )
    assert comparison.revision_skew == "mismatch"
    assert comparison.status == "match"  # the components themselves are unchanged


def test_a_direct_process_ir_intent_cannot_be_applied():
    """Plan and compile, yes. Apply, no — and said out loud.

    Process materialization emits XML from the component plan, so applying a
    ProcessIR intent would create an artifact the compile hash does not describe.
    A binding that certifies something that was never built is worse than a
    refusal, so this is refused — with the gap named at PLAN time, before the
    caller spends a compile.
    """
    from boomi_mcp.authoring.workflow import (
        MATERIALIZATION_CAPABILITY,
        compile_authoring_request_v1 as _compile,
    )

    compiled, _ = _compile(process_ir_request(), profile=_PROFILE)
    gaps = [g for g in compiled.capability_gaps if g.capability_id == MATERIALIZATION_CAPABILITY]
    assert gaps and gaps[0].state == "unsupported"

    payload = process_ir_request().model_dump(mode="json")
    payload["expected_capability_revision"] = (
        compiled.revision_binding.capability_revision
    )
    payload["expected_compile_hash"] = compiled.revision_binding.compile_hash

    with patch(_EXECUTE) as execute:
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            config={"authoring_request": payload, "dry_run": False},
        )
        assert execute.call_count == 0
    assert result["_success"] is False
    assert result["error_code"] == AUTHORING_APPLY_VALIDATION_REQUIRED
    assert result["mutation_performed"] is False


def test_the_support_matrix_matches_what_apply_actually_does():
    """The published matrix and the runtime refusal are the same statement."""
    from boomi_mcp.authoring.contract import AUTHORING_SUPPORT_MATRIX

    assert AUTHORING_SUPPORT_MATRIX["process_ir"]["apply"] == "unsupported"
    assert AUTHORING_SUPPORT_MATRIX["process_ir"]["plan"] == "supported"
    assert AUTHORING_SUPPORT_MATRIX["process_ir"]["compile"] == "supported"
    assert AUTHORING_SUPPORT_MATRIX["integration_spec"]["apply"] == "supported"


# ---------------------------------------------------------------------------
# Regressions for the defects live QA found (issue #146 QA, bugs #401-#411)
# ---------------------------------------------------------------------------


def test_a_non_dict_authoring_request_is_rejected_not_silently_ignored():
    """Bug #404. Keying the typed path on dict-ness meant a mis-serialised
    request ran the LEGACY planner and returned `_success: true` for an empty
    spec — a successful-looking answer to a question nobody asked."""
    for malformed in ("not-a-dict", [1, 2], 5, True):
        for action in ("plan", "compile", "apply"):
            result = build_integration_action(
                MagicMock(),
                _PROFILE,
                action,
                config={"authoring_request": malformed, "dry_run": False},
            )
            assert result["_success"] is False, (action, malformed)
            assert result["error_code"] == INVALID_INPUT, (action, malformed)
            assert "authoring_result" not in result


def test_an_explicit_null_authoring_request_still_means_absent():
    """The one non-dict value that fairly reads as "no typed request"."""
    with patch(_PAGINATE) as paginate:
        paginate.return_value = []
        result = build_integration_action(
            MagicMock(), _PROFILE, "plan", config={"authoring_request": None}
        )
    assert result["_success"] is True
    assert "authoring_result" not in result


@pytest.mark.parametrize(
    "legacy_root,value",
    [
        ("name", "_TEST_legacy_name"),
        ("mode", "redesign"),
        ("conflict_policy", "fail"),
        ("integration_spec", {"name": "x", "components": []}),
        ("source_description", "some prose"),
        ("components", []),
    ],
)
def test_every_influential_legacy_root_is_mutually_exclusive(legacy_root, value):
    """Bug #405. `name`, `mode` and `conflict_policy` each drive the legacy plan
    on their own, yet were silently discarded beside a typed request.
    `conflict_policy` is the consequential one — a caller's "fail" was replaced
    by the typed default "reuse"."""
    result = build_integration_action(
        MagicMock(),
        _PROFILE,
        "plan",
        config={
            "authoring_request": appliable_request().model_dump(mode="json"),
            legacy_root: value,
        },
    )
    assert result["_success"] is False, legacy_root
    assert result["error_code"] == INVALID_INPUT, legacy_root
    assert "mutually exclusive" in result["error"]


def test_conflict_policy_is_inside_the_compile_hash():
    """Bug #403. Identical hashes for reuse/clone/fail let a caller compile
    under `fail` and apply under `clone` with that binding — and it created an
    extra live component."""
    hashes = {}
    for policy in ("reuse", "clone", "fail"):
        payload = appliable_request().model_dump(mode="json")
        payload["intent"]["conflict_policy"] = policy
        result = build_integration_action(
            MagicMock(), _PROFILE, "compile", config={"authoring_request": payload}
        )
        assert result["_success"] is True, policy
        hashes[policy] = result["authoring_result"]["revision_binding"]["compile_hash"]
    assert len(set(hashes.values())) == 3, hashes


def test_swapping_conflict_policy_after_compile_is_refused():
    payload = appliable_request().model_dump(mode="json")
    payload["intent"]["conflict_policy"] = "fail"
    compiled = build_integration_action(
        MagicMock(), _PROFILE, "compile", config={"authoring_request": payload}
    )
    binding = compiled["authoring_result"]["revision_binding"]

    swapped = appliable_request().model_dump(mode="json")
    swapped["intent"]["conflict_policy"] = "clone"
    swapped["expected_capability_revision"] = binding["capability_revision"]
    swapped["expected_compile_hash"] = binding["compile_hash"]

    with patch(_EXECUTE) as execute:
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            config={"authoring_request": swapped, "dry_run": False},
        )
        assert execute.call_count == 0
    assert result["_success"] is False
    assert result["error_code"] == "AUTHORING_PLAN_STALE"


def test_a_drift_failing_verify_carries_a_top_level_error_code():
    """Bug #411. The code lived only inside the nested comparison, so a caller
    keying on `error_code` saw nothing."""
    applied = _apply(_bound_payload())
    tampered = _LIVE_XML.replace("<connection/>", "<connection edited='yes'/>")
    with patch(_GET_XML) as get_xml:
        get_xml.return_value = {"type": "connector-settings", "xml": tampered}
        verified = build_integration_action(
            MagicMock(), _PROFILE, "verify", config={"build_id": applied["build_id"]}
        )
    assert verified["_success"] is False
    assert verified["error_code"] == AUTHORING_LIVE_DEPLOYMENT_DRIFT
    assert verified["error"]


def test_the_verify_provenance_validates_against_its_published_schema():
    """Bug #407. `live_comparison` is the reason to call verify, and the
    published schema is additionalProperties:false — so omitting it made the
    schema reject the payload the surface emits."""
    import jsonschema

    from boomi_mcp.categories.meta_tools import _get_authoring_schema_by_name

    applied = _apply(_bound_payload())
    with patch(_GET_XML) as get_xml:
        get_xml.return_value = {"type": "connector-settings", "xml": _LIVE_XML}
        verified = build_integration_action(
            MagicMock(), _PROFILE, "verify", config={"build_id": applied["build_id"]}
        )
    schema = _get_authoring_schema_by_name("AuthoringBuildProvenanceV1")["json_schema"]
    jsonschema.validate(applied["authoring_provenance"], schema)
    jsonschema.validate(verified["authoring_provenance"], schema)


def test_a_binding_compiled_without_the_lint_cannot_satisfy_an_apply_with_it():
    """The component-plan lint is EVIDENCE, so it is part of the plan hash.

    A compile that could not run the lint saw less than an apply that can, and
    the binding says so rather than papering over the difference. The tool layer
    always supplies a client, so both sides agree in production; this pins the
    library-level behaviour so it is a decision rather than a surprise.
    """
    unlinted, _ = compile_authoring_request_v1(appliable_request(), profile=_PROFILE)
    linted, _ = compile_authoring_request_v1(
        appliable_request(), boomi_client=MagicMock(), profile=_PROFILE
    )
    assert (
        unlinted.revision_binding.compile_hash
        != linted.revision_binding.compile_hash
    )

    payload = appliable_request().model_dump(mode="json")
    payload["expected_capability_revision"] = (
        unlinted.revision_binding.capability_revision
    )
    payload["expected_compile_hash"] = unlinted.revision_binding.compile_hash
    with patch(_EXECUTE) as execute:
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            config={"authoring_request": payload, "dry_run": False},
        )
        assert execute.call_count == 0
    assert result["_success"] is False
    assert result["error_code"] == "AUTHORING_PLAN_STALE"


# ---------------------------------------------------------------------------
# Regressions for the Codex review findings (round 1)
# ---------------------------------------------------------------------------


def test_the_bound_conflict_policy_survives_into_materialization():
    """P1. `conflict_policy` is a legacy root, so the typed-apply strip removed
    it and `_build_plan` defaulted to "reuse" — a compile that bound "fail" or
    "clone" would mutate under "reuse". The hash promised one policy and the
    write performed another."""
    seen = {}

    real_build_plan = integration_builder._build_plan

    def _recording(client, config):
        seen["conflict_policy"] = config.get("conflict_policy")
        return real_build_plan(client, config)

    for policy in ("reuse", "clone", "fail"):
        payload = appliable_request().model_dump(mode="json")
        payload["intent"]["conflict_policy"] = policy
        compiled = build_integration_action(
            MagicMock(), _PROFILE, "compile", config={"authoring_request": payload}
        )
        binding = compiled["authoring_result"]["revision_binding"]
        payload["expected_capability_revision"] = binding["capability_revision"]
        payload["expected_compile_hash"] = binding["compile_hash"]

        seen.clear()
        with patch(_EXECUTE) as execute, patch(_GET_XML) as get_xml, patch.object(
            integration_builder, "_build_plan", _recording
        ):
            execute.side_effect = lambda *a, **k: {
                "_success": True,
                "component_id": "cid-1",
            }
            get_xml.return_value = {"type": "connector-settings", "xml": _LIVE_XML}
            result = build_integration_action(
                MagicMock(),
                _PROFILE,
                "apply",
                config={"authoring_request": payload, "dry_run": False},
            )
        assert result["_success"] is True, policy
        assert seen["conflict_policy"] == policy, policy


def test_a_schema_invalid_typed_request_never_echoes_its_input():
    """P1. Pydantic's default error text interpolates `input_value`, so letting
    a ValidationError reach the generic handler echoed the rejected payload —
    including a credential sitting in a recipe intent's `raw_input`."""
    secret = "PW_REVIEW_LEAK_CHECK"
    payload = {
        "contract_version": "1",
        "intent": {
            "intent_kind": "recipe",
            # `integration_name` deliberately omitted -> ValidationError
            "invocations": [
                {
                    "recipe_id": "r",
                    "invocation_id": "i",
                    "raw_input": {"password": secret},
                }
            ],
        },
    }
    for action in ("plan", "compile", "apply"):
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            action,
            config={"authoring_request": payload, "dry_run": False},
        )
        assert result["_success"] is False, action
        assert result["error_code"] == INVALID_INPUT, action
        assert secret not in repr(result), f"{action} echoed the rejected input"
        assert result["validation_errors"], action


def test_an_incomplete_apply_baseline_verifies_as_unknown_not_match():
    """P2. A component whose post-apply read failed was omitted entirely, so
    verify compared only what succeeded and reported `match` over an incomplete
    baseline."""
    payload = _bound_payload()
    with patch(_EXECUTE) as execute, patch(_GET_XML) as get_xml:
        execute.side_effect = lambda *a, **k: {
            "_success": True,
            "component_id": "cid-1",
        }
        get_xml.side_effect = RuntimeError("read-back failed")
        applied = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            config={"authoring_request": payload, "dry_run": False},
        )
    assert applied["_success"] is True
    recorded = _BUILD_REGISTRY[applied["build_id"]]["authoring"][
        "live_component_fingerprints"
    ]
    # The component IS recorded, with no digest — not silently dropped.
    assert recorded
    assert all(entry["digest"] is None for entry in recorded.values())

    with patch(_GET_XML) as get_xml:
        get_xml.return_value = {"type": "connector-settings", "xml": _LIVE_XML}
        verified = build_integration_action(
            MagicMock(), _PROFILE, "verify", config={"build_id": applied["build_id"]}
        )
    comparison = verified["authoring_provenance"]["live_comparison"]
    assert comparison["status"] == "unknown"
    assert comparison["unverifiable_components"]


# ---------------------------------------------------------------------------
# Regressions for QA round 4 (bugs #415-#417)
# ---------------------------------------------------------------------------


def test_a_typed_apply_refused_by_its_bound_policy_answers_in_the_typed_envelope():
    """Bug #415. A refusal by the bound conflict_policy came back in the LEGACY
    shape with no `error_code` and no `mutation_performed` — so an agent
    branching on the fields this contract tells it to read saw None for both.
    This path was unreachable from the typed root until the policy fix landed."""
    payload = appliable_request().model_dump(mode="json")
    payload["intent"]["conflict_policy"] = "fail"
    compiled = build_integration_action(
        MagicMock(), _PROFILE, "compile", config={"authoring_request": payload}
    )
    binding = compiled["authoring_result"]["revision_binding"]
    payload["expected_capability_revision"] = binding["capability_revision"]
    payload["expected_compile_hash"] = binding["compile_hash"]

    # A pre-existing component makes conflict_policy="fail" refuse.
    existing = [
        {
            "component_id": "existing-1",
            "name": "M12.11 Applied Conn",
            "type": "connector-settings",
            "version": "1",
        }
    ]
    with patch(_EXECUTE) as execute, patch.object(
        integration_builder, "paginate_metadata", lambda *a, **k: existing
    ):
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            config={"authoring_request": payload, "dry_run": False},
        )
        assert execute.call_count == 0
    assert result["_success"] is False
    assert result["action"] == "apply"
    assert result["mutation_performed"] is False
    assert result["error_code"]


def test_a_compile_that_cannot_be_applied_does_not_report_itself_valid():
    """Bug #416. The lint already resolved the collision, yet compile reported
    `is_valid: true` with no warnings and issued a binding whose apply then
    failed every time."""
    payload = appliable_request().model_dump(mode="json")
    payload["intent"]["conflict_policy"] = "fail"
    existing = [
        {
            "component_id": "existing-1",
            "name": "M12.11 Applied Conn",
            "type": "connector-settings",
            "version": "1",
        }
    ]
    with patch.object(
        integration_builder, "paginate_metadata", lambda *a, **k: existing
    ):
        result = build_integration_action(
            MagicMock(), _PROFILE, "compile", config={"authoring_request": payload}
        )
    assert result["_success"] is False
    assert result["error_code"] == "AUTHORING_COMPILE_BLOCKED"
    causes = {c for d in result["authoring_diagnostics"] for c in d["cause_codes"]}
    assert any(cause.startswith("error_") for cause in causes)


def test_a_process_ir_compile_is_not_blocked_by_a_lint_it_can_never_apply():
    """The other half of #416's scope: a ProcessIR intent is plan/compile-only,
    so its component plan exists to resolve `$ref` symbols rather than be built.
    Blocking compile there would make the capability this issue adds unusable."""
    result = build_integration_action(
        MagicMock(),
        _PROFILE,
        "compile",
        config={"authoring_request": process_ir_request().model_dump(mode="json")},
    )
    assert result["_success"] is True
    assert result["authoring_result"]["artifact_fingerprints"]


# ---------------------------------------------------------------------------
# Regressions for QA round 5 (bugs #418-#419)
# ---------------------------------------------------------------------------


def test_a_typed_dry_run_does_not_claim_it_mutated():
    """Bug #418. `mutation_performed` was `bool(_success)`, so a dry run — which
    succeeds having written nothing — claimed it mutated."""
    payload = _bound_payload()
    with patch(_EXECUTE) as execute:
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            config={"authoring_request": payload, "dry_run": True},
        )
        assert execute.call_count == 0
    assert result["_success"] is True
    assert result["dry_run"] is True
    assert result["mutation_performed"] is False


def test_a_partial_failure_that_wrote_something_says_so():
    """Bug #418, the direction that matters most. A failure AFTER a real create
    reported `mutation_performed: false`, so an agent had no reason to clean up."""
    from boomi_mcp.models.integration_models import (
        IntegrationComponentSpec,
        IntegrationSpecV1,
    )
    from boomi_mcp.models.authoring_workflow import (
        AuthoringRequestV1,
        IntegrationSpecAuthoringIntentV1,
    )

    two = AuthoringRequestV1(
        intent=IntegrationSpecAuthoringIntentV1(
            integration_spec=IntegrationSpecV1(
                name="M12.11 Partial",
                components=[
                    IntegrationComponentSpec(
                        key="a", type="connector-settings", name="M12.11 A",
                        config={"connector_type": "rest", "component_name": "M12.11 A",
                                "base_url": "https://api.example.com", "auth": "NONE"},
                    ),
                    IntegrationComponentSpec(
                        key="b", type="connector-settings", name="M12.11 B",
                        config={"connector_type": "rest", "component_name": "M12.11 B",
                                "base_url": "https://api.example.com", "auth": "NONE"},
                    ),
                ],
            )
        )
    )
    compiled, _ = compile_authoring_request_v1(
        two, boomi_client=MagicMock(), profile=_PROFILE
    )
    payload = two.model_dump(mode="json")
    payload["expected_capability_revision"] = (
        compiled.revision_binding.capability_revision
    )
    payload["expected_compile_hash"] = compiled.revision_binding.compile_hash

    calls = {"n": 0}

    def _first_ok_then_fail(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return {"_success": True, "component_id": "created-1"}
        return {"_success": False, "error": "builder blew up"}

    with patch(_EXECUTE) as execute, patch(_GET_XML) as get_xml:
        execute.side_effect = _first_ok_then_fail
        get_xml.return_value = {"type": "connector-settings", "xml": _LIVE_XML}
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            config={"authoring_request": payload, "dry_run": False},
        )
    assert result["_success"] is False
    assert result["mutation_performed"] is True, (
        "a failure after a real create must not report that nothing was written"
    )


def test_a_post_mutation_failure_is_not_labelled_as_needing_revalidation():
    """Bug #419. AUTHORING_APPLY_VALIDATION_REQUIRED means "nothing was mutated;
    re-plan and retry". Attaching it after a real create turned its remediation
    into "retry over live state" — which under clone duplicated the component on
    every retry. Once mutation begins the legacy failure stands (ADR-001 §7)."""
    from boomi_mcp.categories.integration_builder import _decorate_typed_apply

    post_mutation = {
        "_success": False,
        "error": "Failed at step 'b'",
        "partial_results": {"a": {"status": "created", "component_id": "created-1", "result": {"_success": True}}},
    }
    _decorate_typed_apply(post_mutation, {})
    assert post_mutation["mutation_performed"] is True
    assert "error_code" not in post_mutation

    pre_mutation = {"_success": False, "error": "refused before any write"}
    _decorate_typed_apply(pre_mutation, {})
    assert pre_mutation["mutation_performed"] is False
    assert pre_mutation["error_code"] == AUTHORING_APPLY_VALIDATION_REQUIRED


def test_a_reused_step_is_not_evidence_of_a_write():
    """Bug #420. A `reused` step carries the id of a component it merely BOUND,
    so "has a component_id" reported a mutation that never happened — and,
    because the error-code gate keys off the same value, simultaneously lost the
    AUTHORING_APPLY_VALIDATION_REQUIRED that marks the failure retry-safe."""
    from boomi_mcp.categories.integration_builder import (
        _components_were_written,
        _decorate_typed_apply,
    )

    reused_only = {
        "_success": False,
        "error": "later step failed",
        "partial_results": {"a": {"status": "reused", "component_id": "pre-existing"}},
    }
    assert _components_were_written(reused_only) is False
    _decorate_typed_apply(reused_only, {})
    assert reused_only["mutation_performed"] is False
    assert reused_only["error_code"] == AUTHORING_APPLY_VALIDATION_REQUIRED

    for writing_status in ("created", "updated"):
        wrote = {
            "_success": False,
            "partial_results": {"a": {"status": writing_status, "component_id": "c1", "result": {"_success": True}}},
        }
        assert _components_were_written(wrote) is True, writing_status
        _decorate_typed_apply(wrote, {})
        assert wrote["mutation_performed"] is True
        assert "error_code" not in wrote


def test_an_unknown_step_status_fails_toward_reporting_a_write():
    """The two error directions are not symmetric.

    Over-reporting costs a retry-safety hint; under-reporting tells an agent
    nothing needs cleanup when something does, and then invites a retry that
    duplicates under `clone`. An unseen status must fail toward the recoverable
    mistake.
    """
    from boomi_mcp.categories.integration_builder import _components_were_written

    assert _components_were_written(
        {"partial_results": {"a": {"status": "some_future_status", "component_id": "c"}}}
    ) is True


def test_an_attempted_write_with_no_returned_id_counts_as_a_possible_mutation():
    """Codex review (round 2), P1. The builder records the FAILING step too,
    with a writing status and `component_id: None` — the ambiguous case where
    the create reached Boomi but the response was lost or failed to parse.
    Demanding an id skipped exactly those and certified that nothing was
    written when something may well have been."""
    from boomi_mcp.categories.integration_builder import (
        _components_were_written,
        _decorate_typed_apply,
    )

    ambiguous = {
        "_success": False,
        "error": "Failed at step 'a'",
        "partial_results": {
            "a": {"status": "created", "component_id": None, "result": {"_success": False}}
        },
    }
    assert _components_were_written(ambiguous) is True
    _decorate_typed_apply(ambiguous, {})
    assert ambiguous["mutation_performed"] is True
    # ...and crucially NOT the retry-oriented code, which would advise an action
    # that duplicates under conflict_policy="clone".
    assert "error_code" not in ambiguous


def test_a_reused_step_is_still_the_only_confirmed_non_write():
    """The conservative widening must not swallow the one case we KNOW wrote
    nothing — otherwise every failure would claim a mutation."""
    from boomi_mcp.categories.integration_builder import _components_were_written

    assert _components_were_written(
        {"partial_results": {"a": {"status": "reused", "component_id": "pre-existing"}}}
    ) is False
    # A plan/dry-run envelope carries no results at all.
    assert _components_were_written({"_success": True, "dry_run": True, "steps": []}) is False


def test_the_response_says_how_SURE_it_is_that_something_was_written():
    """QA #421. One boolean was answering two different questions — "must the
    caller reconcile?" and "is this failure retry-safe?" — which diverge exactly
    where it matters. The certainty is now published."""
    from boomi_mcp.categories.integration_builder import (
        _decorate_typed_apply,
        _mutation_status,
    )

    observed = {
        "partial_results": {
            "a": {"status": "created", "component_id": "c1", "result": {"_success": True}}
        }
    }
    assert _mutation_status(observed) == "performed"

    ambiguous = {"partial_results": {"a": {"status": "created", "component_id": None}}}
    assert _mutation_status(ambiguous) == "possible"

    nothing = {"partial_results": {"a": {"status": "reused", "component_id": "pre"}}}
    assert _mutation_status(nothing) == "none"

    assert _mutation_status({"_success": True, "dry_run": True}) == "none"

    # An observed write anywhere outranks an ambiguous one.
    mixed = {
        "partial_results": {
            "a": {"status": "created", "component_id": None},
            "b": {"status": "created", "component_id": "c2", "result": {"_success": True}},
        }
    }
    assert _mutation_status(mixed) == "performed"


def test_only_a_provably_untouched_apply_is_marked_retry_safe():
    """`possible` withholds the retry code just as `performed` does: retrying an
    unconfirmed write duplicates under conflict_policy="clone"."""
    from boomi_mcp.categories.integration_builder import _decorate_typed_apply

    for status_value, expect_code in (
        ("created", False),   # observed or ambiguous -> not retry-safe
        ("reused", True),     # confirmed non-write   -> retry-safe
    ):
        envelope = {
            "_success": False,
            "partial_results": {"a": {"status": status_value, "component_id": None
                                      if status_value == "created" else "pre"}},
        }
        _decorate_typed_apply(envelope, {})
        assert ("error_code" in envelope) is expect_code, status_value

    # And the boolean stays conservative for the ambiguous case.
    ambiguous = {
        "_success": False,
        "partial_results": {"a": {"status": "created", "component_id": None}},
    }
    _decorate_typed_apply(ambiguous, {})
    assert ambiguous["mutation_performed"] is True
    assert ambiguous["mutation_status"] == "possible"


def test_a_target_id_on_a_FAILED_update_is_not_proof_of_a_write():
    """Codex review (round 3), P2. A pre-write component GET that times out
    returns `{_success: False, retryable: True, component_id: <target>}`, and
    `_extract_component_id` reads that field first — so a failed, explicitly
    retryable update was reported as an OBSERVED write, suppressing a retry the
    envelope itself marks safe."""
    from boomi_mcp.categories.integration_builder import _mutation_status

    failed_update = {
        "_success": False,
        "partial_results": {
            "a": {
                "status": "updated",
                "component_id": "target-1",
                "result": {
                    "_success": False,
                    "error_code": "COMPONENT_GET_DEADLINE_EXCEEDED",
                    "component_id": "target-1",
                    "retryable": True,
                },
            }
        },
    }
    assert _mutation_status(failed_update) == "possible"

    succeeded = {
        "results": {
            "a": {
                "status": "updated",
                "component_id": "target-1",
                "result": {"_success": True},
            }
        }
    }
    assert _mutation_status(succeeded) == "performed"


def test_a_malformed_typed_apply_still_publishes_its_mutation_status():
    """Codex review (round 3), P2. The documented envelope promises the field is
    ALWAYS present; this refusal skipped the decorator entirely, so a client
    told to branch on it could not classify a deterministic INVALID_INPUT."""
    for malformed in ("not-a-dict", [1, 2], 7, True):
        result = build_integration_action(
            MagicMock(),
            _PROFILE,
            "apply",
            config={"authoring_request": malformed, "dry_run": False},
        )
        assert result["_success"] is False, malformed
        assert result["mutation_status"] == "none", malformed
        assert result["mutation_performed"] is False, malformed
        assert result["error_code"] == INVALID_INPUT, malformed
