"""The FIRST mutation gate (issue #143, M12.8) — slice 8.

Slices 1-7 were dark. This is the slice where behavior changes, so the tests are
about the two properties that make it safe rather than about the rules
themselves (those are covered by the per-collector suites):

``legacy precedence still wins``
    The gate runs LAST. A payload that fails an existing check must still report
    the existing code — otherwise a shipped error code silently changes meaning
    for every caller keying on it.

``a fatal report reaches no mutation``
    The issue's acceptance criterion is explicit: "No component build/apply API
    is called when validation has a fatal error." Proven per error FAMILY, not
    once, because a single spy over a single family proves only that one path.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.categories import integration_builder
from boomi_mcp.categories.integration_builder import _process_ir_semantic_error
from boomi_mcp.compiler.process_ir.semantic_validation.legacy_bridge import (
    blocking_codes,
    validate_legacy_process_config,
)
from boomi_mcp.errors import (
    PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING,
    PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE,
)


def _flow_sequence_config(steps):
    return {
        "process_kind": "database_to_api_sync",
        "flow_sequence": list(steps),
    }


# ---------------------------------------------------------------------------
# projection: fail OPEN
# ---------------------------------------------------------------------------


def test_an_unmigrated_dialect_is_not_gated():
    """Projection is not this gate's job. A dialect the adapters do not cover
    must pass through untouched rather than be blamed for a compiler-side gap."""
    assert validate_legacy_process_config("database_to_api_sync", {}) is None


def test_an_unknown_process_kind_is_not_gated():
    assert validate_legacy_process_config("", {}) is None
    assert validate_legacy_process_config("not_a_kind", {}) is None


def test_a_config_the_adapter_cannot_project_is_not_gated():
    """A malformed config is the legacy validators' business — they ran first.
    Manufacturing a semantic error out of a projection failure would blame the
    author for the wrong thing."""
    broken = _flow_sequence_config([{"kind": "not_a_real_step"}])
    assert validate_legacy_process_config("database_to_api_sync", broken) is None


def test_the_helper_returns_none_rather_than_raising_on_garbage():
    assert _process_ir_semantic_error("", {}) is None
    assert _process_ir_semantic_error("sync_pipeline", {"pipeline": "nonsense"}) is None


# ---------------------------------------------------------------------------
# the sync_pipeline adapter is entered through its RAW-config wrapper
# ---------------------------------------------------------------------------


def test_sync_pipeline_is_entered_through_the_raw_config_wrapper():
    """``adapt_sync_pipeline`` consumes an already-lowered core; only
    ``adapt_sync_pipeline_config`` accepts the raw dialect config. Binding the
    inner function would make every sync_pipeline projection fail silently, and
    a gate that silently never runs is worse than no gate."""
    from boomi_mcp.compiler.process_ir.semantic_validation import legacy_bridge

    adapter, policy = legacy_bridge._adapter_for_config("sync_pipeline", {})
    assert adapter is not None
    assert adapter.__name__ == "adapt_sync_pipeline_config"
    assert policy == "sync_pipeline"


def test_each_dialect_maps_to_its_own_policy():
    from boomi_mcp.compiler.process_ir.semantic_validation import legacy_bridge

    _a, wrapper = legacy_bridge._adapter_for_config("wrapper_subprocess", {})
    assert wrapper == "wrapper_subprocess"
    _b, flow = legacy_bridge._adapter_for_config(
        "database_to_api_sync", {"flow_sequence": [{"kind": "stop"}]}
    )
    assert flow == "flow_sequence"


# ---------------------------------------------------------------------------
# blocking behavior
# ---------------------------------------------------------------------------


def test_blocking_codes_is_empty_for_an_absent_report():
    assert blocking_codes(None) == ()


def test_only_errors_block_not_warnings():
    """A report of warnings alone must not produce a builder error, or every
    opaque map in the repo becomes unbuildable."""
    from boomi_mcp.compiler.process_ir.semantic_validation import (
        ValidationDiagnosticV1,
        build_validation_report,
    )

    warning_only = build_validation_report(
        [
            ValidationDiagnosticV1(
                code=PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE,
                severity="warning",
                phase="lineage",
                path="/a",
                node_identity="/a",
                message="m",
                remediation="r",
            )
        ]
    )
    assert blocking_codes(warning_only) == ()
    assert warning_only.is_valid is True


# ---------------------------------------------------------------------------
# precedence: the gate runs LAST
# ---------------------------------------------------------------------------


def test_the_gate_is_invoked_only_after_every_legacy_check_passed():
    """Read off the source rather than simulated: the call sits under an
    ``if process_flow_err is None`` guard, which is what makes legacy codes win.
    A refactor that hoisted it would silently change every existing error code.
    """
    source = Path(integration_builder.__file__).read_text()
    marker = "process_flow_err = _process_ir_semantic_error(process_kind, raw_config)"
    assert marker in source
    before = source.split(marker)[0]
    tail = before[-600:]
    assert "process_flow_err is None" in tail
    assert 'planned_action in ("create", "create_clone", "update")' in tail


def test_the_gate_is_restricted_to_authoring_actions():
    """A reuse / reference step emits no process XML, so validating it would
    newly reject payloads that plan clean today."""
    source = Path(integration_builder.__file__).read_text()
    marker = "process_flow_err = _process_ir_semantic_error(process_kind, raw_config)"
    guard = source.split(marker)[0][-600:]
    assert 'comp.type == "process"' in guard


# ---------------------------------------------------------------------------
# the builder error it produces
# ---------------------------------------------------------------------------


def test_a_blocking_finding_becomes_a_builder_error_carrying_the_stable_code():
    """Built directly from a report so the assertion is about the TRANSLATION,
    not about which payload happens to trip a rule today."""
    from boomi_mcp.compiler.process_ir.semantic_validation import (
        ValidationDiagnosticV1,
        build_validation_report,
    )
    from boomi_mcp.compiler.process_ir.semantic_validation import legacy_bridge

    report = build_validation_report(
        [
            ValidationDiagnosticV1(
                code=PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING,
                severity="error",
                phase="lineage",
                path="/body/steps/2",
                node_identity="/body/steps/2",
                message="m",
                remediation="add an upstream cache write",
            )
        ]
    )
    original = legacy_bridge.validate_legacy_process_config

    def _stub(kind, config, capabilities=None):
        return report

    integration_builder_mod = sys.modules[integration_builder.__name__]
    legacy_bridge.validate_legacy_process_config = _stub
    try:
        err = _process_ir_semantic_error("sync_pipeline", {})
    finally:
        legacy_bridge.validate_legacy_process_config = original

    assert err is not None
    assert err.error_code == PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING
    assert "/body/steps/2" in str(err)
    assert err.details["codes"] == [PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING]


def test_the_builder_error_carries_no_authored_value():
    """#143's redaction boundary reaches all the way to the builder error, which
    is what actually gets returned to a caller and logged."""
    from boomi_mcp.compiler.process_ir.semantic_validation import (
        ValidationDiagnosticV1,
        build_validation_report,
    )
    from boomi_mcp.compiler.process_ir.semantic_validation import legacy_bridge

    report = build_validation_report(
        [
            ValidationDiagnosticV1(
                code=PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING,
                severity="error",
                phase="lineage",
                path="/body/steps/2",
                node_identity="/body/steps/2",
                message="m",
                remediation="r",
            )
        ]
    )
    original = legacy_bridge.validate_legacy_process_config
    legacy_bridge.validate_legacy_process_config = lambda *a, **k: report
    try:
        err = _process_ir_semantic_error("sync_pipeline", {"secret": "SENTINEL_VALUE"})
    finally:
        legacy_bridge.validate_legacy_process_config = original

    blob = "{0}{1}".format(err, err.details)
    assert "SENTINEL_VALUE" not in blob


# ---------------------------------------------------------------------------
# no mutation on a fatal report — per family
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "code",
    [
        "PROCESS_IR_REFERENCE_COMPONENT_NOT_FOUND",
        "PROCESS_IR_REFERENCE_COMPONENT_TYPE_MISMATCH",
        "PROCESS_IR_CAPABILITY_EFFECT_CONTRACT_INVALID",
        "PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE",
        "PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING",
        "PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID",
        "PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID",
        "PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE",
        "PROCESS_IR_SEMANTIC_RETRY_EFFECT_UNSAFE",
        "PROCESS_IR_SEMANTIC_UNREACHABLE",
        "PROCESS_IR_SEMANTIC_MISSING_TERMINAL",
    ],
)
def test_every_fatal_family_produces_a_blocking_builder_error(code):
    """One spy over one family would prove only that family. The acceptance
    criterion is per-family, so the parametrization is the test."""
    from boomi_mcp.compiler.process_ir.semantic_validation import (
        ValidationDiagnosticV1,
        build_validation_report,
    )
    from boomi_mcp.compiler.process_ir.semantic_validation import legacy_bridge

    report = build_validation_report(
        [
            ValidationDiagnosticV1(
                code=code,
                severity="error",
                phase="lineage",
                path="/body/steps/0",
                node_identity="/body/steps/0",
                message="m",
                remediation="r",
            )
        ]
    )
    original = legacy_bridge.validate_legacy_process_config
    legacy_bridge.validate_legacy_process_config = lambda *a, **k: report
    try:
        err = _process_ir_semantic_error("sync_pipeline", {})
    finally:
        legacy_bridge.validate_legacy_process_config = original

    # A non-None BuilderValidationError from the preflight is precisely what
    # _build_plan's fail-fast set keys on to skip every _execute_component.
    assert err is not None
    assert err.error_code == code


# ---------------------------------------------------------------------------
# the chain from "fatal finding" to "no mutation", pinned rather than assumed
# ---------------------------------------------------------------------------


def test_a_preflight_error_is_routed_to_the_blocking_planned_action():
    """The link a comment would otherwise be asserting: a non-None preflight
    error sets ``planned_action = "error_process_validation"``."""
    source = Path(integration_builder.__file__).read_text()
    marker = "process_flow_err = _process_component_preflight("
    after = source.split(marker)[1][:900]
    assert "if process_flow_err is not None:" in after
    assert 'planned_action = "error_process_validation"' in after


def test_the_blocking_action_is_in_apply_plans_fail_fast_set():
    """And that action is refused BEFORE any component executes.

    Without this, "validation blocks mutation" would rest on an unverified
    assumption about a routing constant three thousand lines away.
    """
    source = Path(integration_builder.__file__).read_text()
    marker = "unresolvable_steps = ["
    fail_fast = source.split(marker)[1][:1200]
    assert '"error_process_validation"' in fail_fast
    # and the set is acted on before execution
    tail = source.split(marker)[1][:2000]
    assert "if unresolvable_steps:" in tail
