"""#146 (M12.11): the read-only semantic plan performs ZERO remote mutation.

"Plan performs zero remote mutation" is an acceptance criterion, and the only
honest way to test a negative is to make the forbidden call explode. Every test
here installs :class:`MutationSpy` over the builder's write helpers, so a plan
that reached one fails loudly rather than passing quietly.
"""

import sys
from pathlib import Path

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _m12_11_support import (  # noqa: E402
    UNRESOLVABLE_IR_DOC,
    MutationSpy,
    components,
    integration_spec_request,
    process_ir_request,
    supporting_components,
)
from boomi_mcp.authoring.workflow import (  # noqa: E402
    AuthoringWorkflowError,
    plan_authoring_request_v1,
)
from boomi_mcp.errors import (  # noqa: E402
    AUTHORING_CAPABILITY_REVISION_MISMATCH,
    AUTHORING_PLAN_STALE,
    AUTHORING_REQUIRED_DECISION_MISSING,
)
from boomi_mcp.models.authoring_workflow import (  # noqa: E402
    AuthoringRequestV1,
    DecisionResolutionV1,
    RecipeAuthoringIntentV1,
    RecipeInvocationRequestV1,
)


@pytest.fixture
def spy(monkeypatch):
    return MutationSpy().install(monkeypatch)


def _plan(request):
    result, _internals = plan_authoring_request_v1(
        request, profile="qa_profile", account_id="qa_account"
    )
    return result


def test_a_process_ir_plan_validates_and_previews_without_mutating(spy):
    """#153 moved the ROOT out of `components`, and the preview WITHHOLDS it.

    Two separate facts, both asserted here because each is easy to break while
    the other still holds:

    * the supporting components are previewed exactly as before, and the process
      is no longer among them — it is not a component any more;
    * the served preview carries NO process roots at all. That is deliberate:
      `build_integration_spec_preview` withholds them so the served envelope
      cannot replay authored ProcessIR (ADR-001 §11). The root is not lost — it
      is still resolved as an in-plan reference below, and compile reports it in
      `process_cfg` and the artifact fingerprints.
    """
    result = _plan(process_ir_request())
    assert result.mutation_performed is False
    assert result.validation_report.is_valid is True
    assert result.errors == ()
    preview = result.integration_spec_preview
    assert preview.name == "M12.11 Integration"

    assert {c.key for c in preview.components} == {
        c.key for c in supporting_components()
    }
    assert "proc" not in {c.key for c in preview.components}
    # The served projection withholds every authored root.
    assert preview.processes == ()
    # ...but the root is still a DECLARED participant, so a reference to it
    # resolves. Without this the assertion above would be satisfied by a
    # regression that simply dropped the root from the request entirely.
    assert {r.ref for r in result.resolved_references} >= {"$ref:proc"}
    assert spy.calls == []


def test_an_integration_spec_plan_reports_its_capability_gap_honestly(spy):
    """A ProcessIR root is not derivable from every legacy process_kind.

    The honest answer is a named gap plus a working preview — not a silent
    success that implies the process was semantically validated when it was not.
    """
    result = _plan(integration_spec_request())
    assert result.mutation_performed is False
    gaps = [gap.capability_id for gap in result.capability_gaps]
    assert gaps == ["authoring.integration_spec_intent.wrapper_subprocess"]
    assert result.capability_gaps[0].state == "unsupported"
    assert result.capability_gaps[0].reason_code == "PROCESS_IR_ROOT_NOT_DERIVABLE"
    assert spy.calls == []


def test_a_topology_adjunct_is_validated_but_never_deployed(spy):
    from boomi_mcp.models.system_topology import parse_system_topology_v1

    topology = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "qa_profile",
            "objects": [
                {"kind": "process", "key": "p1", "component_ref": "$ref:proc"}
            ],
            "relations": [],
        }
    )
    request = process_ir_request()
    request = AuthoringRequestV1(intent=request.intent, topology_spec=topology)
    result = _plan(request)
    assert result.mutation_performed is False
    # Relations are reported under their OWN name, never as a generic "flow".
    assert result.topology_relations == ()
    assert spy.calls == []


def test_the_component_plan_preview_is_an_integration_spec_v1(spy):
    """The issue asks for IntegrationSpecV1 EXPLICITLY as the component plan."""
    from boomi_mcp.models.integration_models import IntegrationSpecV1

    result = _plan(process_ir_request())
    assert isinstance(result.integration_spec_preview, IntegrationSpecV1)


def test_semantic_and_plan_hashes_are_stable_across_key_insertion_order(spy):
    """A hash that depends on dict ordering is not a fingerprint."""
    first = _plan(process_ir_request())

    reordered_doc = {
        "body": {
            "steps": [
                {
                    "operation_ref": "$ref:db_op",
                    "kind": "source",
                    "connection_ref": "$ref:db_conn",
                },
                {"text": "hello", "kind": "message"},
                {
                    "operation_ref": "$ref:api_op",
                    "connection_ref": "$ref:api_conn",
                    "kind": "target",
                },
                {"kind": "stop"},
            ],
            "kind": "sequence",
        },
        "version": "1",
    }
    second = _plan(process_ir_request(reordered_doc))

    assert (
        first.revision_binding.semantic_hash == second.revision_binding.semantic_hash
    )
    assert first.revision_binding.plan_hash == second.revision_binding.plan_hash


def test_an_unresolvable_reference_passes_PLAN_and_is_caught_at_COMPILE(spy):
    """Where the gate actually is — measured, not assumed.

    Reference resolution is NOT one of the semantic-validation phases:
    ``validate_process_ir`` returns a clean report for a ``$ref`` no component
    declares. The unresolvable reference is caught when the emission plan is
    built, i.e. at COMPILE.

    That is still safe, and this test exists to pin why: a typed apply runs
    through compile (``preflight_typed_apply_v1`` calls
    ``compile_authoring_request_v1``), so an unresolvable reference cannot reach
    a mutation. What it must NOT do is make plan claim more than it checked —
    hence the explicit assertion that plan reports itself valid here rather than
    a wishful assertion that it catches everything.
    """
    from boomi_mcp.authoring.workflow import compile_authoring_request_v1
    from boomi_mcp.errors import AUTHORING_COMPILE_BLOCKED

    result = _plan(process_ir_request(UNRESOLVABLE_IR_DOC))
    assert result.mutation_performed is False
    assert result.validation_report.is_valid is True
    assert result.errors == ()

    with pytest.raises(AuthoringWorkflowError) as excinfo:
        compile_authoring_request_v1(
            process_ir_request(UNRESOLVABLE_IR_DOC),
            profile="qa_profile",
            account_id="qa_account",
        )
    assert excinfo.value.code == AUTHORING_COMPILE_BLOCKED
    # The compiler's own code travels verbatim as a value-free causative.
    causes = {c for d in excinfo.value.diagnostics for c in d.cause_codes}
    assert causes, "the causative canonical codes must travel"
    assert spy.calls == []


def test_diagnostics_are_ordered_deterministically(spy):
    from boomi_mcp.models.authoring_workflow import (
        AuthoringDiagnosticV1,
        sort_authoring_diagnostics,
    )

    shuffled = (
        AuthoringDiagnosticV1(code="B_CODE", severity="warning", path="/z"),
        AuthoringDiagnosticV1(code="A_CODE", severity="error", path="/b"),
        AuthoringDiagnosticV1(code="A_CODE", severity="error", path="/a"),
        AuthoringDiagnosticV1(code="C_CODE", severity="advisory", path="/a"),
    )
    ordered = sort_authoring_diagnostics(shuffled)
    # Errors first, then by code, then by path — deterministic and total.
    assert [(d.severity, d.code, d.path) for d in ordered] == [
        ("error", "A_CODE", "/a"),
        ("error", "A_CODE", "/b"),
        ("warning", "B_CODE", "/z"),
        ("advisory", "C_CODE", "/a"),
    ]
    assert sort_authoring_diagnostics(tuple(reversed(shuffled))) == ordered


def test_answering_a_decision_this_plan_never_raised_is_refused(spy):
    """A resolution for an unknown decision means the caller is working from a
    DIFFERENT plan — exactly the staleness the binding exists to catch."""
    request = AuthoringRequestV1(
        intent=process_ir_request().intent,
        decisions=(DecisionResolutionV1(decision_id="ghost", option_id="a"),),
    )
    result = _plan(request)
    codes = {diagnostic.code for diagnostic in result.errors}
    assert AUTHORING_REQUIRED_DECISION_MISSING in codes
    assert spy.calls == []


def test_a_capability_revision_mismatch_blocks_the_plan(spy):
    request = AuthoringRequestV1(
        intent=process_ir_request().intent,
        expected_capability_revision="sha256:" + "0" * 64,
    )
    with pytest.raises(AuthoringWorkflowError) as excinfo:
        _plan(request)
    assert excinfo.value.code == AUTHORING_CAPABILITY_REVISION_MISMATCH
    assert spy.calls == []


def test_a_stale_expected_plan_hash_is_refused(spy):
    request = AuthoringRequestV1(
        intent=process_ir_request().intent,
        expected_plan_hash="sha256:" + "1" * 64,
    )
    with pytest.raises(AuthoringWorkflowError) as excinfo:
        _plan(request)
    assert excinfo.value.code == AUTHORING_PLAN_STALE
    assert spy.calls == []


def test_the_recipe_intent_reaches_the_engine_and_still_mutates_nothing(spy):
    """A recipe that cannot resolve is a recipe-layer refusal, carried
    value-free — never a mutation, and never re-diagnosed by this layer."""
    request = AuthoringRequestV1(
        intent=RecipeAuthoringIntentV1(
            integration_name="M12.11 Recipe",
            invocations=(
                RecipeInvocationRequestV1(
                    recipe_id="definitely_not_a_registered_recipe",
                    invocation_id="i1",
                    raw_input={},
                ),
            ),
        )
    )
    with pytest.raises(AuthoringWorkflowError):
        _plan(request)
    assert spy.calls == []


def test_every_result_collection_is_present_even_when_empty(spy):
    """A field that is absent when empty forces every caller to write the same
    defensive branch, and they will not all write it correctly."""
    result = _plan(process_ir_request())
    for name in (
        "errors",
        "warnings",
        "capability_gaps",
        "required_decisions",
        "resolved_references",
        "component_dependencies",
        "topology_relations",
        "pipeline_stages",
        "process_cfg",
    ):
        assert isinstance(getattr(result, name), tuple), name
    dumped = result.model_dump(mode="json")
    for name in ("errors", "warnings", "capability_gaps", "required_decisions"):
        assert name in dumped and isinstance(dumped[name], list)


def test_the_read_only_phase_never_imports_a_write_helper():
    """Structural, not behavioural: the orchestration module must not even NAME
    a create/update/execute/deploy helper."""
    import inspect

    from boomi_mcp.authoring import workflow

    source = inspect.getsource(workflow)
    for forbidden in (
        "create_component",
        "update_component",
        "create_connector",
        "update_connector",
        "create_trading_partner",
        "update_trading_partner",
        "execute_process",
        "manage_deployment",
        "orchestrate_deploy",
    ):
        assert forbidden not in source, forbidden


# ---------------------------------------------------------------------------
# Regressions for the defects live QA found (issue #146 QA, bugs #401-#411)
# ---------------------------------------------------------------------------


def test_the_typed_plan_reuses_the_legacy_redaction(monkeypatch, spy):
    """Bug #401 (High). The same spec returned "[REDACTED]" down the legacy root
    and the plaintext password down the typed one, because the typed path echoed
    the caller's spec instead of the legacy planner's redacted one."""
    from unittest.mock import MagicMock

    import boomi_mcp.categories.integration_builder as builder
    from boomi_mcp.models.authoring_workflow import (
        AuthoringRequestV1,
        IntegrationSpecAuthoringIntentV1,
    )
    from boomi_mcp.models.integration_models import (
        IntegrationComponentSpec,
        IntegrationSpecV1,
    )

    monkeypatch.setattr(builder, "paginate_metadata", lambda *a, **k: [])
    secret = "PW_146_PLAINTEXT"
    request = AuthoringRequestV1(
        intent=IntegrationSpecAuthoringIntentV1(
            integration_spec=IntegrationSpecV1(
                name="M12.11 Redaction",
                components=[
                    IntegrationComponentSpec(
                        key="db",
                        type="connector-settings",
                        name="M12.11 DB",
                        config={
                            "connector_type": "database",
                            "component_name": "M12.11 DB",
                            "password": secret,
                        },
                    )
                ],
            )
        )
    )
    result, _ = plan_authoring_request_v1(
        request, boomi_client=MagicMock(), profile="qa", account_id="acct"
    )
    blob = result.model_dump_json()
    assert secret not in blob, "the typed plan echoed a plaintext credential"


def test_the_typed_plan_does_not_claim_zero_warnings_when_nothing_looked(spy):
    """Bug #402 (High). With no client the component-plan lint cannot run, so
    the honest answer is "unknown", not an affirmative empty list."""
    result = _plan(process_ir_request())
    assert result.warnings, "a plan that ran no lint must not report zero warnings"
    assert any(
        "did not run" in diagnostic.message for diagnostic in result.warnings
    )


def test_a_dangling_ref_appears_in_resolved_references(spy):
    """Bug #410. The field listed declared component keys, so the one reference
    an agent needs to see — the dangling one — was exactly the one omitted."""
    result = _plan(process_ir_request(UNRESOLVABLE_IR_DOC))
    refs = {r.ref: r.resolved for r in result.resolved_references}
    assert "$ref:nonexistent_op" in refs
    assert refs["$ref:nonexistent_op"] is False


def test_the_compile_block_remediation_is_not_circular(spy):
    """Bug #409. It said "re-plan", and plan reports the input valid."""
    from boomi_mcp.authoring.workflow import compile_authoring_request_v1

    with pytest.raises(AuthoringWorkflowError) as excinfo:
        compile_authoring_request_v1(
            process_ir_request(UNRESOLVABLE_IR_DOC), profile="qa", account_id="acct"
        )
    remediations = " ".join(d.remediation for d in excinfo.value.diagnostics).lower()
    # It must point at the COMPONENT the step references...
    assert "component" in remediations
    # ...and say outright that re-planning will not surface this, rather than
    # sending the caller back to a phase that reports the input valid.
    assert "will not surface" in remediations


def test_the_validation_summary_counts_this_results_diagnostics(spy):
    """Bug #412. `warnings: [5 items]` beside `warning_count: 0` is the same
    "positively asserts absence" defect one field over."""
    result = _plan(process_ir_request())
    assert result.validation_report.warning_count + (
        result.validation_report.advisory_count
    ) == len(result.warnings)
    assert result.validation_report.error_count == len(result.errors)
    assert result.validation_report.is_valid == (not result.errors)


def test_the_validation_summary_tracks_a_blocking_plan(spy):
    """Guard the guard: a summary hard-coded to zero would pass the pin above
    on a clean plan."""
    request = AuthoringRequestV1(
        intent=process_ir_request().intent,
        decisions=(DecisionResolutionV1(decision_id="ghost", option_id="a"),),
    )
    result = _plan(request)
    assert result.errors
    assert result.validation_report.error_count == len(result.errors)
    assert result.validation_report.is_valid is False


# ---------------------------------------------------------------------------
# Regressions for the Codex review findings (round 1)
# ---------------------------------------------------------------------------


def test_pipeline_stages_carry_the_authored_stage_keys(spy):
    """P2. StageSpec has `key`, no `name` — read behind a getattr default, every
    authored pipeline summarized as a list of empty strings."""
    from boomi_mcp.authoring.workflow import build_pipeline_stages
    from boomi_mcp.models.integration_models import IntegrationSpecV1
    from boomi_mcp.models.pipeline_models import PipelineSpec, StageSpec

    spec = IntegrationSpecV1(
        name="pipeline",
        components=[],
        pipeline=PipelineSpec(
            stages=[StageSpec(key="fetch", kind="fetch"), StageSpec(key="send", kind="send")]
        ),
    )
    assert build_pipeline_stages(spec) == ("fetch", "send")


def test_topology_relations_summarize_their_real_fields(spy):
    """P2. No relation variant defines relation_kind/source_key/target_key, so
    every relation summarized as empty strings behind getattr defaults."""
    from boomi_mcp.authoring.workflow import build_topology_relations
    from boomi_mcp.models.system_topology import parse_system_topology_v1

    topology = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "qa",
            "objects": [
                {"kind": "process", "key": "p1", "component_ref": "$ref:a"},
                {"kind": "process", "key": "p2", "component_ref": "$ref:b"},
            ],
            "relations": [
                {
                    "kind": "process_call",
                    "key": "r1",
                    "caller_process": "p1",
                    "callee_process": "p2",
                }
            ],
        }
    )
    summaries = build_topology_relations(topology)
    assert len(summaries) == 1
    summary = summaries[0]
    assert summary.relation_kind == "process_call"
    assert summary.relation_key == "r1"
    assert [(p.role, p.ref) for p in summary.participants] == [
        ("callee_process", "p2"),
        ("caller_process", "p1"),
    ]


def test_a_three_role_relation_is_not_forced_into_a_pair(spy):
    """`deployment_binding` binds THREE objects; a source/target shape loses one."""
    from boomi_mcp.authoring.workflow import build_topology_relations
    from boomi_mcp.models.system_topology import parse_system_topology_v1

    topology = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "qa",
            "objects": [
                {"kind": "process", "key": "p", "component_ref": "$ref:a"},
                {"kind": "deployment_unit", "key": "u"},
                {
                    "kind": "environment",
                    "key": "e",
                    "environment_ref": "env-1",
                    "classification": "TEST",
                },
            ],
            "relations": [
                {
                    "kind": "deployment_binding",
                    "key": "r",
                    "deployment_unit": "u",
                    "process": "p",
                    "environment": "e",
                }
            ],
        }
    )
    participants = build_topology_relations(topology)[0].participants
    assert len(participants) == 3
    assert {p.role for p in participants} == {
        "deployment_unit",
        "process",
        "environment",
    }


def test_an_in_plan_reference_is_resolved_not_dangling(spy):
    """P2. A `$ref` to a component THIS plan declares is resolved by the symbol
    table whether or not it exists yet. Marking it unresolved made a valid
    forward reference indistinguishable from a dangling one."""
    result = _plan(process_ir_request())
    by_ref = {r.ref: r for r in result.resolved_references}
    assert by_ref["$ref:db_conn"].resolved is True
    assert by_ref["$ref:proc"].resolved is True


def test_a_dangling_reference_is_still_distinguishable(spy):
    """Guard the guard: marking everything resolved would satisfy the pin above."""
    result = _plan(process_ir_request(UNRESOLVABLE_IR_DOC))
    by_ref = {r.ref: r for r in result.resolved_references}
    assert by_ref["$ref:nonexistent_op"].resolved is False
    assert by_ref["$ref:db_conn"].resolved is True


def test_a_failed_legacy_lint_blocks_instead_of_echoing_the_raw_request(spy):
    """P1. A failed `_build_plan` returns no `integration_spec`; treating it as
    success left the preview as the RAW request — and that path includes
    PLAINTEXT_SECRET_REJECTED, so the response would have echoed the very value
    the legacy planner refused, while reporting success."""
    from unittest.mock import MagicMock

    import boomi_mcp.authoring.workflow as workflow
    from boomi_mcp.errors import AUTHORING_COMPILE_BLOCKED

    def _failing_plan(*args, **kwargs):
        return {"_success": False, "error_code": "PLAINTEXT_SECRET_REJECTED"}

    import boomi_mcp.categories.integration_builder as builder

    original = builder._build_plan
    builder._build_plan = _failing_plan
    try:
        with pytest.raises(workflow.AuthoringWorkflowError) as excinfo:
            plan_authoring_request_v1(
                process_ir_request(),
                boomi_client=MagicMock(),
                profile="qa",
                account_id="acct",
            )
    finally:
        builder._build_plan = original
    assert excinfo.value.code == AUTHORING_COMPILE_BLOCKED
    causes = {c for d in excinfo.value.diagnostics for c in d.cause_codes}
    assert "PLAINTEXT_SECRET_REJECTED" in causes


def test_the_served_plan_never_echoes_an_authored_process_ir_value(spy):
    """#153 secrets regression guard: the spec echo must not replay authored IR.

    Moving process roots INTO ``IntegrationSpecV1`` (issue #153 item 4) put the
    caller's ProcessIR inside a SERVED field for the first time — both
    ``integration_spec_preview`` and the legacy ``integration_spec`` envelope
    echo the spec. A ``set_property`` value or a scripting step is caller content
    that can carry a credential, so replaying it through a logged, cached,
    LLM-visible response weakens the secrets posture even though the caller sent
    it. ADR-001 §11: results carry hashes, opaque references and value-free
    diagnostics, never authored payload.

    The watermark is planted in the AUTHORED root and swept for across the whole
    serialized result, which is the only way to catch it wherever it surfaces.
    """
    import json

    watermark = "M12_15_PLAN_ECHO_WATERMARK"
    doc = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                {
                    "kind": "source",
                    "connection_ref": "$ref:db_conn",
                    "operation_ref": "$ref:db_op",
                },
                {"kind": "message", "text": watermark},
                {
                    "kind": "target",
                    "connection_ref": "$ref:api_conn",
                    "operation_ref": "$ref:api_op",
                },
                {"kind": "stop"},
            ],
        },
    }
    result = _plan(process_ir_request(doc))
    served = result.model_dump_json()

    # POSITIVE CONTROL first: the sweep can see the watermark when it IS there.
    # Without this the assertion below passes just as happily against a probe
    # that looks at the wrong object.
    assert watermark in json.dumps({"authored": doc})
    assert watermark not in served, "authored ProcessIR leaked into the served plan"
    assert result.integration_spec_preview.processes == ()


def test_the_legacy_component_plan_echo_cannot_restore_withheld_roots(spy, monkeypatch):
    """QA-153-r1-02. The FIRST withholding fix was silently undone downstream.

    ``build_integration_spec_preview`` withholds the authored roots, and fifty
    lines later the legacy component-plan lint's echo REPLACED the preview with
    one rebuilt from the normalized spec — which since #153 carries
    ``processes[]``. Live QA measured planted canaries in every served plan and
    compile response while the builder itself was provably withholding them.

    **Why the original guard missed it, and why this test forces the echo.**
    ``_legacy_plan_echo`` returns ``None`` when there is no ``boomi_client``, so
    in a unit test the overwriting line never executes and a canary sweep cannot
    discriminate — measured: with the fix reverted, an unforced probe still
    reported no leak. Forcing the echo is what makes this a control rather than
    a coincidence. Verified in both directions: with the fix reverted this test
    FAILS, with it in place it passes.
    """
    import copy
    import json

    import boomi_mcp.authoring.workflow as workflow

    watermark = "M12_15_ECHO_RESTORE_WATERMARK"
    doc = copy.deepcopy(UNRESOLVABLE_IR_DOC) if False else None

    def _echo(normalized, request, boomi_client):
        # Exactly what a live client produces: the NORMALIZED spec, roots and all.
        return {
            "_success": True,
            "integration_spec": normalized.integration_spec.model_dump(mode="json"),
            "steps": [],
        }

    monkeypatch.setattr(workflow, "_legacy_plan_echo", _echo)

    from _m12_11_support import VALID_IR_DOC

    authored = copy.deepcopy(VALID_IR_DOC)
    authored["body"]["steps"].insert(1, {"kind": "message", "text": watermark})

    result = _plan(process_ir_request(authored))
    served = result.model_dump_json()

    # Positive control: the sweep can see the watermark when it IS present.
    assert watermark in json.dumps({"authored": authored})
    assert watermark not in served, "the legacy echo restored the withheld roots"
    assert result.integration_spec_preview.processes == ()
