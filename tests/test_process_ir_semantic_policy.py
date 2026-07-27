"""Registry-owned legacy exemptions (issue #143, M12.8) — slice 7. Still DARK.

The single most important test in this file is the NEGATIVE one: an exemption a
caller can request is not an exemption, it is a bypass. The issue forbids
"relaxing rules via free-form 'trust me' flags", so several independent routes a
caller might take are each closed and asserted.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.compiler.process_ir.semantic_validation import (
    ProcessIRValidationCapabilitiesV1,
    ValidationDiagnosticV1,
    build_validation_report,
    validate_process_ir,
)
from boomi_mcp.compiler.process_ir.semantic_validation.validation_policy import (
    LegacyValidationPolicyV1,
    apply_policy,
    lookup_policy,
    registered_adapters,
)
from boomi_mcp.errors import (
    ERROR_TAXONOMY,
    LEGACY_ADAPTER_EXEMPTION_DECISION_PROPERTY_READ,
    LEGACY_ADAPTER_EXEMPTION_OPAQUE_STATE_WRITER,
    LEGACY_ADAPTER_EXEMPTION_STANDALONE_CACHE_READ,
    LEGACY_ADAPTER_EXEMPTION_SUBPROCESS_SUMMARY,
    PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING,
    PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE,
)

_ALL_EXEMPTIONS = (
    LEGACY_ADAPTER_EXEMPTION_OPAQUE_STATE_WRITER,
    LEGACY_ADAPTER_EXEMPTION_DECISION_PROPERTY_READ,
    LEGACY_ADAPTER_EXEMPTION_STANDALONE_CACHE_READ,
    LEGACY_ADAPTER_EXEMPTION_SUBPROCESS_SUMMARY,
)


def _error(code, path="/body/steps/0"):
    return ValidationDiagnosticV1(
        code=code,
        severity="error",
        phase="lineage",
        path=path,
        node_identity=path,
        message="m",
        remediation="r",
    )


# ---------------------------------------------------------------------------
# the registry
# ---------------------------------------------------------------------------


def test_every_registered_exemption_code_exists_in_the_taxonomy():
    for adapter in registered_adapters():
        for code in lookup_policy(adapter).exemptions:
            assert code in ERROR_TAXONOMY, code
            assert ERROR_TAXONOMY[code].owner == "#143", code


def test_every_declared_exemption_code_is_actually_used_by_some_policy():
    """A code no policy references is unreachable — this repo has learned that
    an untested code is a code that does not work."""
    used = set()
    for adapter in registered_adapters():
        used |= lookup_policy(adapter).exemptions
    assert used == set(_ALL_EXEMPTIONS)


def test_an_unknown_adapter_gets_no_policy_which_means_strict():
    """Fail closed: a typo must tighten, never loosen."""
    assert lookup_policy("not_an_adapter") is None
    assert lookup_policy("") is None


def test_a_policy_cannot_be_built_with_an_unknown_exemption_code():
    with pytest.raises(ValueError):
        LegacyValidationPolicyV1("x", ("LEGACY_ADAPTER_EXEMPTION_MADE_UP",))


def test_policies_are_registered_for_the_real_legacy_adapters():
    assert set(registered_adapters()) == {
        "flow_sequence",
        "wrapper_subprocess",
        "sync_pipeline",
    }


# ---------------------------------------------------------------------------
# application: reclassify, never delete
# ---------------------------------------------------------------------------


def test_an_exempted_error_becomes_an_advisory_rather_than_disappearing():
    """An exemption that deleted the finding would make the migration ledger
    unfalsifiable — nobody could see what the strict rule would have said."""
    report = build_validation_report(
        [_error(PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE)]
    )
    assert report.is_valid is False

    exempted = apply_policy(report, lookup_policy("flow_sequence"))
    assert exempted.errors == ()
    assert exempted.is_valid is True
    assert [f.code for f in exempted.advisories] == [
        LEGACY_ADAPTER_EXEMPTION_OPAQUE_STATE_WRITER
    ]


def test_the_advisory_records_which_canonical_code_it_replaced():
    report = build_validation_report(
        [_error(PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE)]
    )
    advisory = apply_policy(report, lookup_policy("flow_sequence")).advisories[0]
    evidence = {e.key: e.value for e in advisory.evidence}
    assert evidence["related_code"] == (
        PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE
    )


def test_an_error_the_policy_does_not_cover_still_blocks():
    """The exemption set is exact. A policy that swallowed everything would be
    a bypass wearing a registry's clothes."""
    report = build_validation_report([_error("PROCESS_IR_SEMANTIC_UNREACHABLE")])
    exempted = apply_policy(report, lookup_policy("flow_sequence"))
    assert exempted.is_valid is False
    assert [f.code for f in exempted.errors] == ["PROCESS_IR_SEMANTIC_UNREACHABLE"]


def test_a_policy_covering_a_different_code_does_not_apply():
    """``wrapper_subprocess`` exempts only the subprocess-summary case."""
    report = build_validation_report(
        [_error(PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING)]
    )
    exempted = apply_policy(report, lookup_policy("wrapper_subprocess"))
    assert exempted.is_valid is False


def test_no_policy_means_the_report_is_returned_untouched():
    report = build_validation_report(
        [_error(PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE)]
    )
    assert apply_policy(report, None) is report


def test_warnings_are_not_reclassified():
    """They never blocked, so exempting one would only hide it."""
    warning = ValidationDiagnosticV1(
        code=PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE,
        severity="warning",
        phase="lineage",
        path="/a",
        node_identity="/a",
        message="m",
        remediation="r",
    )
    report = build_validation_report([warning])
    exempted = apply_policy(report, lookup_policy("flow_sequence"))
    assert len(exempted.warnings) == 1
    assert exempted.advisories == ()


# ---------------------------------------------------------------------------
# THE negative test: a caller cannot select a policy
# ---------------------------------------------------------------------------


def test_validate_process_ir_takes_no_policy_argument():
    """Strict is not a mode the public entry point can be talked out of."""
    import inspect

    params = set(inspect.signature(validate_process_ir).parameters)
    assert params == {"ir", "symbol_table", "capabilities"}


def test_the_capability_contract_has_no_policy_or_exemption_field():
    fields = set(ProcessIRValidationCapabilitiesV1.model_fields)
    for forbidden in ("policy", "exemption", "exemptions", "adapter", "trusted"):
        assert forbidden not in fields


def test_capabilities_reject_an_unknown_policy_field_outright():
    with pytest.raises(Exception):
        ProcessIRValidationCapabilitiesV1(policy="flow_sequence")


def test_the_ir_model_carries_no_policy_field():
    """The last route a payload could take: authoring the exemption itself."""
    from boomi_mcp.models.process_ir import ProcessIRV1

    fields = set(ProcessIRV1.model_fields)
    for forbidden in ("policy", "validation_policy", "exemptions", "trusted"):
        assert forbidden not in fields


def test_lookup_is_keyed_on_adapter_identity_not_on_a_payload_token():
    """Every registered key is an ADAPTER name this repo actually ships, not a
    string a caller could invent and have honoured."""
    for adapter in registered_adapters():
        assert lookup_policy(adapter).adapter == adapter


# ---------------------------------------------------------------------------
# §6 architect review: "registry-owned" and "immutable by construction" were
# both only comments — the objects and the mapping were freely mutable.
# ---------------------------------------------------------------------------


def test_a_registered_policy_cannot_be_mutated():
    """`__slots__` bounds WHICH attributes exist, not whether they can be
    reassigned. Policies are shared module-level singletons reached through
    `lookup_policy`, so one assignment repointed the registered policy for every
    later caller in the process — the exact loosening this registry prevents."""
    from boomi_mcp.compiler.process_ir.semantic_validation.validation_policy import (
        lookup_policy,
    )

    policy = lookup_policy("flow_sequence")
    with pytest.raises(AttributeError):
        policy.adapter = "hacked"
    with pytest.raises(AttributeError):
        del policy.adapter
    assert lookup_policy("flow_sequence").adapter == "flow_sequence"


def test_the_policy_registry_itself_cannot_be_extended_at_runtime():
    """An injected adapter policy would grant exemptions no review ever saw."""
    from boomi_mcp.compiler.process_ir.semantic_validation import validation_policy

    with pytest.raises(TypeError):
        validation_policy._POLICY_REGISTRY["injected"] = None
    assert "injected" not in validation_policy.registered_adapters()


def test_the_shipped_policies_still_work_after_being_frozen():
    """The discriminator: freezing must not break construction or lookup."""
    from boomi_mcp.compiler.process_ir.semantic_validation.validation_policy import (
        lookup_policy,
        registered_adapters,
    )

    assert set(registered_adapters()) == {
        "flow_sequence", "wrapper_subprocess", "sync_pipeline",
    }
    assert len(lookup_policy("flow_sequence").exemptions) == 3
    assert lookup_policy("nonexistent") is None
