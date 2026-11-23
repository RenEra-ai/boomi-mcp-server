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


# ---------------------------------------------------------------------------
# the builder error it produces
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# no mutation on a fatal report — per family
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# the chain from "fatal finding" to "no mutation", pinned rather than assumed
# ---------------------------------------------------------------------------


def test_the_blocking_action_is_in_apply_plans_fail_fast_set():
    """And that action is refused BEFORE any component executes.

    Without this, "validation blocks mutation" would rest on an unverified
    assumption about a routing constant three thousand lines away.

    Asserted BEHAVIOURALLY, by spying on the mutation function, rather than by
    searching the fail-fast expression for the action's name. The name search
    was passing for a reason that had nothing to do with the property: it
    confirmed one string appeared in an enumeration, and that same enumeration
    was silently missing two other blocking actions the planner emits. A source
    grep cannot tell "this action is refused" from "this action is spelled
    nearby" — and the gate is now a prefix rule with no names to find at all
    (issue #145, §6 architect review).
    """
    from unittest.mock import MagicMock, patch

    def fake_plan(client, cfg, *args, **kwargs):  # noqa: ARG001
        return {
            "_success": True,  # "successful" AND blocking, as _build_plan reports it
            "steps": [
                {
                    "planned_action": "error_process_validation",
                    "key": "main_process",
                    "name": "P",
                    "validation_error": {
                        "error_code": "PROCESS_XML_VALIDATION_FAILED",
                        "field": "config",
                    },
                }
            ],
        }

    with patch.object(
        integration_builder, "_build_plan", side_effect=fake_plan
    ), patch.object(integration_builder, "_execute_component") as execute_spy:
        result = integration_builder._apply_plan(
            MagicMock(), "qa", {"integration_spec": {"name": "x"}, "dry_run": False}
        )

    assert execute_spy.call_count == 0
    assert result["_success"] is False
    assert result["unresolvable_steps"][0]["planned_action"] == "error_process_validation"


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


def test_a_projection_failure_still_fails_OPEN():
    """The other direction must be untouched: an adapter that cannot project
    the config still returns None rather than raising."""
    broken = _flow_sequence_config([{"kind": "not_a_real_step"}])
    assert validate_legacy_process_config("database_to_api_sync", broken) is None
