"""#146 (M12.11): the read-only canonical compile.

Compile re-runs the whole plan path and then lowers every authored process
through the ONE canonical chain. It creates nothing, so it returns no build id —
and it must never be reachable without the #143 semantic gate having run.
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
    integration_spec_request,
    process_ir_request,
    walk_keys,
    walk_strings,
)
from boomi_mcp.authoring.workflow import (  # noqa: E402
    AuthoringWorkflowError,
    compile_authoring_request_v1,
)
from boomi_mcp.errors import AUTHORING_COMPILE_BLOCKED  # noqa: E402


@pytest.fixture
def spy(monkeypatch):
    return MutationSpy().install(monkeypatch)


def _compile(request):
    result, _internals = compile_authoring_request_v1(
        request, profile="qa_profile", account_id="qa_account"
    )
    return result


def test_a_process_ir_compile_produces_artifact_fingerprints(spy):
    result = _compile(process_ir_request())
    assert result.mutation_performed is False
    kinds = {a.artifact_kind for a in result.artifact_fingerprints}
    assert kinds == {"process_ir_emission_plan", "process_ir_normalized"}
    for artifact in result.artifact_fingerprints:
        assert artifact.component_key == "proc"
        assert artifact.digest.startswith("sha256:")
        assert len(artifact.digest) == len("sha256:") + 64
        assert artifact.byte_length > 0
    assert spy.calls == []


def test_compile_returns_no_build_id_at_all(spy):
    """Not "build_id is None" — the field does not exist.

    An always-null build id is an invitation to look for one, and compile
    creates nothing that could have an id.
    """
    result = _compile(process_ir_request())
    assert "build_id" not in result.model_dump(mode="json")
    assert not hasattr(result, "build_id")


def test_compile_is_deterministic(spy):
    first = _compile(process_ir_request())
    second = _compile(process_ir_request())
    assert (
        first.revision_binding.compile_hash == second.revision_binding.compile_hash
    )
    assert [a.digest for a in first.artifact_fingerprints] == [
        a.digest for a in second.artifact_fingerprints
    ]
    assert first.normalized_intent_digest == second.normalized_intent_digest


def test_the_canonical_compiler_is_actually_invoked(spy, monkeypatch):
    """Guard against a compile that fingerprints something it did not compile."""
    import boomi_mcp.compiler.process_ir.pipeline as pipeline

    seen = []
    real = pipeline.compile_process_ir_v1

    def _recording(ir, symbols, **kwargs):
        seen.append(kwargs)
        return real(ir, symbols, **kwargs)

    monkeypatch.setattr(pipeline, "compile_process_ir_v1", _recording)
    _compile(process_ir_request())
    assert len(seen) == 1


def test_the_143_gate_cannot_be_bypassed(spy):
    """A payload the canonical chain rejects is BLOCKED, with no artifact bundle."""
    with pytest.raises(AuthoringWorkflowError) as excinfo:
        _compile(process_ir_request(UNRESOLVABLE_IR_DOC))
    error = excinfo.value
    assert error.code == AUTHORING_COMPILE_BLOCKED
    assert error.diagnostics
    assert all(d.severity == "error" for d in error.diagnostics)
    causes = {c for d in error.diagnostics for c in d.cause_codes}
    assert causes, "the canonical codes must travel as causatives"
    assert spy.calls == []


def test_no_validation_policy_exemption_is_reachable(spy, monkeypatch):
    """A legacy dialect's exemptions must be unreachable from the typed path."""
    import boomi_mcp.compiler.process_ir.pipeline as pipeline

    captured = {}
    real = pipeline.compile_process_ir_v1

    def _recording(ir, symbols, **kwargs):
        captured.update(kwargs)
        return real(ir, symbols, **kwargs)

    monkeypatch.setattr(pipeline, "compile_process_ir_v1", _recording)
    _compile(process_ir_request())
    assert captured.get("validation_policy") is None


def test_no_raw_xml_or_artifact_body_appears_in_the_response(spy):
    """Bytes stay internal: only the digest and the length are published."""
    result = _compile(process_ir_request()).model_dump(mode="json")
    for path, value in walk_strings(result):
        assert "<bns:" not in value, path
        assert "<Process" not in value, path
        assert "shapetype" not in value.lower(), path
    for path, key in walk_keys(result):
        assert key not in ("xml", "emission_plan", "cfg", "artifact_body"), (
            f"{path}.{key}"
        )


def test_the_integration_spec_intent_compiles_with_its_gap_reported(spy):
    """No ProcessIR root means no process artifact — said out loud, not implied."""
    result = _compile(integration_spec_request())
    assert result.mutation_performed is False
    assert result.artifact_fingerprints == ()
    assert [g.capability_id for g in result.capability_gaps] == [
        "authoring.integration_spec_intent.wrapper_subprocess"
    ]


def test_artifact_fingerprints_are_sorted(spy):
    result = _compile(process_ir_request())
    keys = [a.sort_key for a in result.artifact_fingerprints]
    assert keys == sorted(keys)


def test_the_compile_hash_moves_when_the_intent_moves(spy):
    """Guard the guard: a constant hash would satisfy the determinism pin."""
    baseline = _compile(process_ir_request())
    changed_doc = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                {
                    "kind": "source",
                    "connection_ref": "$ref:db_conn",
                    "operation_ref": "$ref:db_op",
                },
                {"kind": "message", "text": "a DIFFERENT message"},
                {
                    "kind": "target",
                    "connection_ref": "$ref:api_conn",
                    "operation_ref": "$ref:api_op",
                },
                {"kind": "stop"},
            ],
        },
    }
    changed = _compile(process_ir_request(changed_doc))
    assert (
        changed.revision_binding.compile_hash
        != baseline.revision_binding.compile_hash
    )
    assert changed.normalized_intent_digest != baseline.normalized_intent_digest


def test_the_process_cfg_summary_is_shape_only(spy):
    """The CFG is a compiler internal; only counts and terminal kinds surface."""
    result = _compile(process_ir_request())
    assert len(result.process_cfg) == 1
    summary = result.process_cfg[0]
    assert summary.component_key == "proc"
    assert summary.node_count > 0
    assert summary.edge_count > 0
    assert summary.terminal_kinds == ("stop",)
    dumped = summary.model_dump(mode="json")
    assert set(dumped) == {
        "component_key",
        "node_count",
        "edge_count",
        "terminal_kinds",
    }
