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
from unittest import mock

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


# ---------------------------------------------------------------------------
# the DERIVED create_clone action reaches the gate (QA Bug #190)
#
# The docs claimed this arm was unreachable, reasoning from the AUTHORED action:
# `IntegrationComponentSpec.action` is Literal["create","update"], so nobody can
# supply "create_clone". But the guard tests the DERIVED planned_action, and
# _build_plan sets it to "create_clone" itself when action="create" meets a name
# collision under conflict_policy="clone".
#
# That made it a behavioural question, not a wording one: if the clone arm could
# reach a mutation ungated, the acceptance criterion "no build/apply API is
# called when validation has a fatal error" would be false on that path. It
# does not — the gate runs — and this pins it so a future edit to the guard
# tuple cannot silently drop the arm.
# ---------------------------------------------------------------------------


def test_the_derived_create_clone_action_reaches_the_semantic_gate():
    seen = []
    original = integration_builder._process_component_preflight
    original_resolve = integration_builder._resolve_existing_components

    def _spy(comp, raw_config, process_kind, planned_action, *a, **k):
        seen.append((comp.type, comp.action, planned_action))
        return original(comp, raw_config, process_kind, planned_action, *a, **k)

    integration_builder._process_component_preflight = _spy
    # two same-named components force conflict_policy="clone" to derive the arm
    integration_builder._resolve_existing_components = lambda client, comp: [
        {"component_id": "dup-1", "name": comp.name},
        {"component_id": "dup-2", "name": comp.name},
    ]
    try:
        integration_builder._build_plan(
            None,
            {
                "conflict_policy": "clone",
                "integration_spec": {
                    "name": "clone-probe",
                    "mode": "lift_shift",
                    "components": [
                        {
                            "key": "p",
                            "type": "process",
                            "name": "ClashingName",
                            "action": "create",
                            "config": {
                                "process_kind": "database_to_api_sync",
                                "flow_sequence": [{"kind": "stop"}],
                            },
                        }
                    ],
                },
            },
        )
    except Exception:  # noqa: BLE001 — the plan's outcome is not what is asserted
        pass
    finally:
        integration_builder._process_component_preflight = original
        integration_builder._resolve_existing_components = original_resolve

    assert seen, "the preflight was never reached"
    assert "create_clone" in {planned for _t, _a, planned in seen}


def test_the_guard_tuple_still_covers_every_authoring_action():
    """Including the derived one. Dropping `create_clone` here would silently
    ungate every clone-policy collision."""
    source = Path(integration_builder.__file__).read_text()
    marker = "process_flow_err = _process_ir_semantic_error(process_kind, raw_config)"
    guard = source.split(marker)[0][-600:]
    assert 'planned_action in ("create", "create_clone", "update")' in guard


# ---------------------------------------------------------------------------
# §6 architect review: the preflight bridge swallowed EVERY exception and
# returned None, which the gate reads as "nothing blocks" — so an internal
# failure of the validator became a silent approval of an UNVALIDATED payload.
#
# Fail OPEN on projection (above) and fail CLOSED on a defect are different
# directions on purpose; only the second was wrong.
# ---------------------------------------------------------------------------

#: A config the flow_sequence adapter can actually PROJECT. `_flow_sequence_config`
#: alone cannot: the adapter needs the endpoints too, and without them projection
#: fails and the bridge returns None long before the validator is reached.
_PROJECTABLE = {
    "process_kind": "database_to_api_sync",
    "source": {
        "connector_type": "database",
        "connection_id": "11111111-1111-1111-1111-111111111111",
        "operation_id": "22222222-2222-2222-2222-222222222222",
        "action_type": "Get",
    },
    "transform": {"mode": "passthrough"},
    "target": {
        "connector_type": "rest",
        "connection_id": "33333333-3333-3333-3333-333333333333",
        "operation_id": "44444444-4444-4444-4444-444444444444",
        "action_type": "POST",
    },
    "flow_sequence": [
        {
            "kind": "set_dpp",
            "name": "OUT",
            "source_values": [{"value_type": "static", "value": "v"}],
        }
    ],
}


def _boom(*_args, **_kwargs):
    raise RuntimeError("simulated validator defect")


def test_a_validator_defect_raises_instead_of_reporting_no_findings():
    """`validate_process_ir` documents that it raises only on a COMPILER
    defect. Swallowing that and returning None told the caller the payload was
    clean."""
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
    from boomi_mcp.compiler.process_ir.semantic_validation import legacy_bridge as LB

    config = dict(_PROJECTABLE)
    with mock.patch.object(LB, "validate_process_ir", _boom):
        with pytest.raises(ProcessIRCompileError) as excinfo:
            validate_legacy_process_config("database_to_api_sync", config)
    assert excinfo.value.diagnostics[0].code == "PROCESS_IR_COMPILE_INTERNAL"


def test_the_builder_turns_that_defect_into_a_blocking_error():
    """It still blocks — but under the COMPILER's own code, reused rather than
    minted, and with a message that says whose bug it is."""
    from boomi_mcp.compiler.process_ir.semantic_validation import legacy_bridge as LB

    config = dict(_PROJECTABLE)
    with mock.patch.object(LB, "validate_process_ir", _boom):
        error = _process_ir_semantic_error("database_to_api_sync", config)
    assert error is not None
    assert error.error_code == "PROCESS_IR_COMPILE_INTERNAL"


def test_the_same_payload_is_clean_when_the_validator_is_healthy():
    """The discriminator: the block above comes from the defect, not the
    payload."""
    config = dict(_PROJECTABLE)
    assert _process_ir_semantic_error("database_to_api_sync", config) is None


def test_a_projection_failure_still_fails_OPEN():
    """The other direction must be untouched: an adapter that cannot project
    the config still returns None rather than raising."""
    broken = _flow_sequence_config([{"kind": "not_a_real_step"}])
    assert validate_legacy_process_config("database_to_api_sync", broken) is None
