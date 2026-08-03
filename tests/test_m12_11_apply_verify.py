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
