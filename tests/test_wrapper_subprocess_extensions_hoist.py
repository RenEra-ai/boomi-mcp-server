"""Tests for issue #99 G3 — hoisting a called child's connection env-extension
declarations onto the wrapper parent.

#92 writes <ConnectionOverride> override points into the DECLARING process's
processOverrides, but the #91 capstone proved they do NOT surface through a #90
wrapper Process Call deployment (only when the declaring process is deployed
directly). G3 copies the child's process_extensions.connections onto the wrapper
at plan time (_synthesize_wrapper_subprocess_extensions) so the wrapper-deployed
package surfaces them, and type-checks the wrapper's connection refs
(_check_wrapper_subprocess_ref_types).
"""

from __future__ import annotations

import sys
from pathlib import Path

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.boomi_mcp.models.integration_models import (
    IntegrationComponentSpec,
    IntegrationSpecV1,
)
import pytest


def _retired(*_args, **_kwargs):
    # #159 S3 (step-4 decision 3): wrapper synthesis and the legacy wrapper ref-type
    # check left the serving module. A test that reaches this measures only the
    # retired auto-hoist mechanism and is a tombstone candidate for the integrator;
    # the typed process-call witness at the end of this module is its replacement.
    pytest.fail("retired at #159 S3: wrapper synthesis / legacy wrapper ref-type check")


_synthesize_wrapper_subprocess_edges = _retired
_synthesize_wrapper_subprocess_extensions = _retired
_check_wrapper_subprocess_ref_types = _retired


def _field(fid, label, xpath):
    return {"id": fid, "label": label, "xpath": xpath}


_HOST = _field("host", "Host", "DatabaseConnectionSettings/@host")
_PORT = _field("port", "Port", "DatabaseConnectionSettings/@port")
_PASSWORD = _field("password", "Password", "DatabaseConnectionSettings/@password")


def _child(pe_connections, key="main_logic", conn_key="db_conn"):
    return IntegrationComponentSpec(
        key=key, type="process", action="create", name="Main",
        depends_on=[conn_key],
        config={
            "process_kind": "database_to_api_sync",
            "source": {}, "target": {}, "transform": {"mode": "passthrough"},
            "process_extensions": {"connections": pe_connections},
        },
    )


def _db_conn(key="db_conn"):
    return IntegrationComponentSpec(
        key=key, type="connector-settings", action="create", name="DB",
        config={"connector_type": "database"},
    )


def _wrapper(child_key="main_logic", process_extensions=None, key="wrap"):
    cfg = {
        "process_kind": "wrapper_subprocess",
        "process_calls": [{"subprocess_ref": f"$ref:{child_key}", "wait": True, "abort_on_error": False}],
    }
    if process_extensions is not None:
        cfg["process_extensions"] = process_extensions
    return IntegrationComponentSpec(
        key=key, type="process", action="create", name="Wrapper", depends_on=[], config=cfg,
    )


def _synth(spec):
    _synthesize_wrapper_subprocess_edges(spec)
    _synthesize_wrapper_subprocess_extensions(spec)
    return next(c for c in spec.components if c.key == "wrap")


def _spec(*components):
    return IntegrationSpecV1(name="t", integration_name="t", components=list(components))


# ---------------------------------------------------------------------------
# Hoisting
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Dedup / precedence (wrapper-declared wins)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Ref-type check
# ---------------------------------------------------------------------------

def _by_key(spec):
    return {c.key: c for c in spec.components}


# ---------------------------------------------------------------------------
# Codex review fixes — hand-authored wrapper process_extensions robustness
# ---------------------------------------------------------------------------

def _wrapper_pe_cfg(connection_id):
    return {
        "process_kind": "wrapper_subprocess",
        "process_calls": [{"process_id": "EXISTING-CHILD-ID"}],
        "process_extensions": {"connections": [{"connection_id": connection_id,
                                                "fields": [_HOST]}]},
    }


def test_unreachable_process_extensions_ref_fails_cleanly():
    # A wrapper process_extensions $ref to a connection NOT in depends_on must
    # fail with a clean reachability error, not leak an unresolved $ref at apply.
    from src.boomi_mcp.categories.components.builders import WrapperSubprocessBuilder
    err = WrapperSubprocessBuilder.validate_config(_wrapper_pe_cfg("$ref:nonexistent_conn"), depends_on=[])
    assert err is not None
    assert err.error_code == "MISSING_PROCESS_DEPENDENCY"


def test_padded_process_extensions_ref_rejected():
    # A whitespace-padded ref is NOT substituted by _resolve_dependency_tokens
    # (which requires '$ref:' at byte 0) — it would leak unresolved, so reject it.
    from src.boomi_mcp.categories.components.builders import WrapperSubprocessBuilder
    err = WrapperSubprocessBuilder.validate_config(_wrapper_pe_cfg(" $ref:db_conn "), depends_on=["db_conn"])
    assert err is not None
    assert err.error_code == "MISSING_PROCESS_DEPENDENCY"


def test_empty_key_process_extensions_ref_rejected():
    # '$ref:' with an empty key resolves to nothing — reject rather than leak.
    from src.boomi_mcp.categories.components.builders import WrapperSubprocessBuilder
    err = WrapperSubprocessBuilder.validate_config(_wrapper_pe_cfg("$ref:"), depends_on=[])
    assert err is not None
    assert err.error_code == "MISSING_PROCESS_DEPENDENCY"


def test_reachable_process_extensions_ref_passes_validate_config():
    # When the connection $ref IS in depends_on (the synthesis-added edge), the
    # reachability check passes.
    from src.boomi_mcp.categories.components.builders import WrapperSubprocessBuilder
    assert WrapperSubprocessBuilder.validate_config(_wrapper_pe_cfg("$ref:db_conn"), depends_on=["db_conn"]) is None


def test_literal_process_extensions_ref_passes_validate_config():
    # A literal (non-$ref) connection id needs no depends_on edge.
    from src.boomi_mcp.categories.components.builders import WrapperSubprocessBuilder
    assert WrapperSubprocessBuilder.validate_config(_wrapper_pe_cfg("literal-conn-id-1234"), depends_on=[]) is None


# ---------------------------------------------------------------------------
# #159 S3: the same outcome on a TYPED unit, with nothing synthesized
# ---------------------------------------------------------------------------


def test_a_typed_process_call_wrapper_declares_its_child_edge_and_extensions_explicitly():
    """Wrapper synthesis leaves planning at the #159 cutover (decision 4): a migrated
    wrapper is a typed ProcessIR unit whose lone ``process_call`` names the child,
    whose envelope DECLARES the child edge and the connection dependency, and whose
    typed ``process_extensions`` carry the override the hoist used to copy from the
    child. Plan and compile keep all three exactly as authored (nothing is inferred
    or hoisted), order the child before the wrapper, and fingerprint both roots."""
    from unittest.mock import MagicMock, patch

    from boomi_mcp.authoring.workflow import compile_authoring_request_v1
    from boomi_mcp.models.process_component import (
        ProcessAuthoringUnitAuthoredV1,
        ProcessComponentEnvelopeAuthoredV1,
    )
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    import test_issue_157_governance as gov

    conn, op = gov._conn(key="conn_b"), gov._op(key="op_b", conn="conn_b")
    child = gov._unit(key="child", conn="conn_b", op="op_b", component_prefix="C")
    extensions = {"connections": [{"connection_id": "$ref:conn_b", "connector_type": "rest",
                                   "fields": [{"id": "url", "label": "Base URL"}]}]}
    wrapper = ProcessAuthoringUnitAuthoredV1(
        envelope=ProcessComponentEnvelopeAuthoredV1(
            component_key="wrapper", action="create", name="Wrapper",
            depends_on=("child", "conn_b"), process_extensions=extensions,
        ),
        process_ir=parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
            {"kind": "process_call", "process_ref": "$ref:child", "wait": True, "abort_on_error": True}]}}),
    )
    request = gov._request([wrapper, child], [conn, op])
    with patch(gov._PAGINATE, lambda *a, **k: []):
        compiled, internals = compile_authoring_request_v1(request, boomi_client=MagicMock(), profile="p")
    roots = {u.envelope.component_key: u.envelope for u in internals.normalized.integration_spec.processes}
    assert roots["wrapper"].depends_on == ("child", "conn_b")
    connections = roots["wrapper"].process_extensions.connections
    assert [(c.connection_id, [f.id for f in c.fields]) for c in connections] == [("$ref:conn_b", ["url"])]
    # The child keeps no extension it did not author (nothing was hoisted either way).
    assert roots["child"].process_extensions.connections == ()
    kinds = {(f.component_key, f.artifact_kind) for f in compiled.artifact_fingerprints}
    assert {("wrapper", "process_component_materialization_plan"),
            ("child", "process_component_materialization_plan")} <= kinds
