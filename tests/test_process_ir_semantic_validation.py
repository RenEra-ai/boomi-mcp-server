"""Semantic-validation contracts (issue #143, M12.8) — slice 1.

Slice 1 is deliberately DARK: it adds a frozen contract layer that nothing
imports. These tests therefore pin the *contract surface* only — severity and
phase vocabularies, frozen/strict behavior, the closed safe-evidence rule,
deterministic report ordering, deduplication, and repr redaction.

Two properties are load-bearing enough to be tested from several angles:

``evidence is closed``
    A diagnostic must be unable to carry an authored value. The rule is enforced
    on BOTH halves of an evidence pair — a key outside the closed allowlist is
    rejected, and a string value that does not look like a lowercase structural
    token or an uppercase diagnostic code is rejected. That combination is what
    blocks component ids, ``$ref`` tokens, labels, property names, script text,
    and exception messages.

``ordering is total``
    A report that sorts by only *some* of its key would be stable within one
    process and unstable across runs. Every component of the sort key is
    exercised in isolation here.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from boomi_mcp.compiler.process_ir.semantic_validation import (
    VALIDATION_PHASE_ORDER,
    VALIDATION_SEVERITY_ORDER,
    ValidationDiagnosticV1,
    ValidationEvidenceV1,
    ValidationReportV1,
    build_validation_report,
    canonical_report_json,
)


def _diag(
    code: str = "PROCESS_IR_SEMANTIC_UNREACHABLE",
    *,
    severity: str = "error",
    phase: str = "reachability",
    path: str = "/body/steps/0",
    evidence=(),
) -> ValidationDiagnosticV1:
    return ValidationDiagnosticV1(
        code=code,
        severity=severity,
        phase=phase,
        path=path,
        node_identity=path,
        message="static message",
        remediation="static remediation",
        evidence=tuple(evidence),
    )


# --------------------------------------------------------------------------
# vocabularies
# --------------------------------------------------------------------------


def test_severity_vocabulary_is_exactly_three_ranked_values():
    assert VALIDATION_SEVERITY_ORDER == ("error", "warning", "advisory")


def test_phase_vocabulary_matches_the_documented_eleven_phases():
    assert VALIDATION_PHASE_ORDER == (
        "model",
        "capability",
        "reference",
        "terminal",
        "reachability",
        "profile",
        "cardinality",
        "lineage",
        "side_effect",
        "retry",
        "compatibility",
    )


def test_an_unknown_severity_is_rejected():
    with pytest.raises(ValidationError):
        _diag(severity="fatal")


def test_an_unknown_phase_is_rejected():
    with pytest.raises(ValidationError):
        _diag(phase="emission_planning")


# --------------------------------------------------------------------------
# frozen + strict
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "model, kwargs",
    [
        (ValidationEvidenceV1, {"key": "leg_ordinal", "value": 2}),
        (ValidationReportV1, {}),
    ],
)
def test_contracts_are_frozen(model, kwargs):
    instance = model(**kwargs)
    with pytest.raises(ValidationError):
        setattr(instance, next(iter(model.model_fields)), None)


def test_diagnostic_is_frozen():
    with pytest.raises(ValidationError):
        _diag().code = "OTHER"


def test_contracts_reject_unknown_fields():
    with pytest.raises(ValidationError):
        ValidationEvidenceV1(key="leg_ordinal", value=1, extra="nope")


# --------------------------------------------------------------------------
# closed safe evidence — the redaction boundary
# --------------------------------------------------------------------------


def test_evidence_key_must_come_from_the_closed_allowlist():
    with pytest.raises(ValidationError):
        ValidationEvidenceV1(key="connection_id", value=1)


@pytest.mark.parametrize(
    "value",
    [
        "b7f3c8d2-1234-4a5b-9c0d-1e2f3a4b5c6d",  # component id (dashes)
        "$ref:ORDER_DB",  # authored reference token
        "Customer Order Map",  # human label (spaces + caps)
        "line 1\nline 2",  # script / exception text
        "sk-live-abcdef123456",  # credential-shaped
        "SELECT * FROM orders",  # operand / query text
    ],
)
def test_evidence_rejects_value_shapes_that_could_carry_authored_text(value):
    with pytest.raises(ValidationError):
        ValidationEvidenceV1(key="effect_kind", value=value)


def test_the_key_allowlist_not_the_value_shape_is_what_blocks_bare_names():
    """Honest statement of where the redaction guarantee actually comes from.

    A property name like ``dpp_customer_email`` is lexically indistinguishable
    from a structural token, so no value-shape rule can reject it. The real
    control is that evidence keys are a CLOSED, code-chosen set and none of them
    is defined to carry a name. This test pins that invariant: adding a
    name-bearing key later would break it loudly instead of silently widening
    the redaction boundary.
    """
    for key in ValidationEvidenceV1.allowed_keys():
        assert not key.endswith(("_name", "_id", "_ref", "_label", "_text"))
    # and the lexically-safe-looking property name is only expressible under a
    # key that does not exist
    with pytest.raises(ValidationError):
        ValidationEvidenceV1(key="property_name", value="dpp_customer_email")


def test_evidence_accepts_booleans_and_counts():
    assert ValidationEvidenceV1(key="retry_count", value=3).value == 3
    assert ValidationEvidenceV1(key="external_writer", value=True).value is True


def test_evidence_accepts_a_lowercase_structural_token():
    assert ValidationEvidenceV1(key="effect_kind", value="cache_write").value == (
        "cache_write"
    )


def test_evidence_accepts_an_uppercase_diagnostic_code():
    got = ValidationEvidenceV1(
        key="suppressed_by", value="PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND"
    )
    assert got.value == "PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND"


def test_evidence_accepts_only_bounded_lowercase_tokens():
    """A lowercase token is bounded, so a long authored name cannot ride in."""
    with pytest.raises(ValidationError):
        ValidationEvidenceV1(key="effect_kind", value="a" * 65)


def test_evidence_rejects_a_mixed_case_token():
    """Mixed case is neither a structural token nor a code — reject, don't guess."""
    with pytest.raises(ValidationError):
        ValidationEvidenceV1(key="effect_kind", value="cacheWrite")


# --------------------------------------------------------------------------
# repr redaction
# --------------------------------------------------------------------------


def test_repr_renders_structural_fields_and_suppresses_the_rest():
    text = repr(_diag(evidence=[ValidationEvidenceV1(key="leg_ordinal", value=7)]))
    # structural discriminators survive
    assert "PROCESS_IR_SEMANTIC_UNREACHABLE" in text
    assert "reachability" in text
    # everything else is suppressed
    assert "static message" not in text
    assert "static remediation" not in text


def test_repr_of_a_report_never_renders_its_diagnostics():
    report = build_validation_report([_diag()])
    assert "static message" not in repr(report)


# --------------------------------------------------------------------------
# report assembly: bucketing, ordering, dedup
# --------------------------------------------------------------------------


def test_diagnostics_are_bucketed_by_severity():
    report = build_validation_report(
        [
            _diag(severity="error"),
            _diag(severity="warning", code="PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN"),
            _diag(
                severity="advisory",
                code="LEGACY_ADAPTER_EXEMPTION_OPAQUE_STATE_WRITER",
                phase="compatibility",
            ),
        ]
    )
    assert len(report.errors) == 1
    assert len(report.warnings) == 1
    assert len(report.advisories) == 1


def test_is_valid_is_false_exactly_when_there_are_errors():
    assert build_validation_report([]).is_valid is True
    assert build_validation_report([_diag(severity="warning")]).is_valid is True
    assert build_validation_report([_diag(severity="advisory")]).is_valid is True
    assert build_validation_report([_diag(severity="error")]).is_valid is False


def test_ordering_is_by_phase_rank_first_not_alphabetical():
    """``terminal`` (rank 3) must precede ``cardinality`` (rank 6).

    That pair is the discriminating one, and the codes are chosen so BOTH
    plausible wrong orderings fail: sorting by phase NAME puts ``cardinality``
    first, and sorting by CODE also puts ``A_CARDINALITY`` first. Only phase
    RANK yields the asserted order. Distinct codes are required because dedup
    keys on ``(code, path, evidence)`` — reusing one code at one path would
    collapse the pair into a single finding and prove nothing about order.
    """
    report = build_validation_report(
        [
            _diag(phase="cardinality", path="/a", code="A_CARDINALITY"),
            _diag(phase="terminal", path="/a", code="B_TERMINAL"),
        ]
    )
    assert [d.phase for d in report.errors] == ["terminal", "cardinality"]


def test_ordering_falls_through_to_path_then_code():
    report = build_validation_report(
        [
            _diag(phase="lineage", path="/b", code="AAA_SECOND"),
            _diag(phase="lineage", path="/a", code="ZZZ_FIRST"),
            _diag(phase="lineage", path="/b", code="AAA_FIRST"),
        ]
    )
    assert [(d.path, d.code) for d in report.errors] == [
        ("/a", "ZZZ_FIRST"),
        ("/b", "AAA_FIRST"),
        ("/b", "AAA_SECOND"),
    ]


def test_identical_findings_are_deduplicated():
    report = build_validation_report([_diag(), _diag(), _diag()])
    assert len(report.errors) == 1


def test_dedup_keys_on_evidence_so_two_distinct_findings_both_survive():
    first = _diag(evidence=[ValidationEvidenceV1(key="leg_ordinal", value=0)])
    second = _diag(evidence=[ValidationEvidenceV1(key="leg_ordinal", value=1)])
    report = build_validation_report([first, second])
    assert len(report.errors) == 2


def test_dedup_does_not_merge_across_severity_buckets():
    report = build_validation_report(
        [_diag(severity="error"), _diag(severity="warning")]
    )
    assert len(report.errors) == 1
    assert len(report.warnings) == 1


def test_report_ordering_is_independent_of_input_order():
    diags = [
        _diag(phase="retry", path="/z"),
        _diag(phase="model", path="/a"),
        _diag(phase="lineage", path="/m"),
    ]
    forward = build_validation_report(diags)
    backward = build_validation_report(list(reversed(diags)))
    assert canonical_report_json(forward) == canonical_report_json(backward)


# --------------------------------------------------------------------------
# canonical serialization
# --------------------------------------------------------------------------


def test_canonical_json_is_stable_and_ascii():
    payload = canonical_report_json(build_validation_report([_diag()]))
    assert payload == canonical_report_json(build_validation_report([_diag()]))
    assert payload.isascii()


def test_canonical_json_has_no_whitespace_padding():
    payload = canonical_report_json(build_validation_report([_diag()]))
    assert ", " not in payload
    assert '": ' not in payload


def test_an_empty_report_is_valid_and_serializes():
    report = build_validation_report([])
    assert report.is_valid is True
    assert canonical_report_json(report)


# --------------------------------------------------------------------------
# darkness: slice 1 must be wired to nothing
# --------------------------------------------------------------------------


def test_the_package_exports_only_its_public_surface():
    from boomi_mcp.compiler.process_ir import semantic_validation

    assert set(semantic_validation.__all__) == {
        "DEFAULT_VALIDATION_CAPABILITIES",
        "MapEffectContractV1",
        "ExternalWriterContractV1",
        "ProcessIRValidationCapabilitiesV1",
        "STATE_SCOPES",
        "ScriptEffectContractV1",
        "StateEffectV1",
        "SubprocessSummaryV1",
        "VALIDATION_PHASE_ORDER",
        "VALIDATION_SEVERITY_ORDER",
        "ValidationDiagnosticV1",
        "ValidationEvidenceV1",
        "ValidationPhaseV1",
        "ValidationReportV1",
        "ValidationSeverityV1",
        "build_validation_report",
        "canonical_report_json",
        "validate_process_ir",
    }
    # collectors, prepared contexts and policy hooks stay PRIVATE
    for private in ("context", "flow", "lineage", "effects", "references"):
        assert private not in semantic_validation.__all__


# ---------------------------------------------------------------------------
# Model-validator parity — added after QA Bug #183
#
# The §7.1 migration matrix cites these seven tests as the regression evidence
# discharging "each existing validator is accounted for ... with regression
# evidence". Six rows concede in their own "Sound?/Complete?" cell that a
# dedicated negative was missing and name a NEW test as the answer; that "New "
# prefix is a promise about this issue's own deliverable. The tests were cited
# and never written. These are them.
#
# Each asserts the validator REJECTS its blank/invalid case — the parity claim
# is that #143 left these model validators untouched (`port-unchanged`), so the
# behavior they pin is the pre-existing one.
# ---------------------------------------------------------------------------

from boomi_mcp.models.process_ir import (  # noqa: E402
    ProcessIRValidationError,
    parse_process_ir_v1,
)

_PARITY_SOURCE = {"kind": "source", "connection_ref": "$ref:c", "operation_ref": "$ref:o"}
_PARITY_TARGET = {"kind": "target", "connection_ref": "$ref:tc", "operation_ref": "$ref:to"}


def _parity_doc(steps):
    return {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [_PARITY_SOURCE] + list(steps) + [_PARITY_TARGET, {"kind": "stop"}],
        },
    }


def _rejects(doc, expected_code="PROCESS_IR_SCHEMA_INVALID_CARDINALITY"):
    """Assert the doc is rejected FOR THE CITED REASON, not merely rejected.

    Added after QA Bug #184. The first version of these tests asserted only
    ``.diagnostics`` was non-empty, which is satisfied by ANY defect — and one
    fixture turned out to be invalid for three independent reasons, so it kept
    passing with the validator it cited deleted. A test that survives the
    deletion of the thing it tests is not evidence.

    Asserting the exact code list converts that whole class of defect from
    silent to loud: an incidentally-invalid fixture now fails on arrival.
    """
    with pytest.raises(ProcessIRValidationError) as excinfo:
        parse_process_ir_v1(doc)
    assert [d.code for d in excinfo.value.diagnostics] == [expected_code]
    return excinfo.value


def test_model_validator_parity_for_blank_dpp_source():
    """``DppPropertySourceV1._non_blank`` — a whitespace-only property name."""
    doc = _parity_doc(
        [
            {
                "kind": "set_dpp",
                "name": "OUT",
                "source_values": [{"value_type": "dpp", "property_name": "   "}],
            }
        ]
    )
    _rejects(doc)


def test_model_validator_parity_for_blank_script():
    """``CustomScriptingOpV1._script_non_blank`` — a whitespace-only script."""
    doc = _parity_doc(
        [
            {
                "kind": "data_process",
                "steps": [
                    {
                        "operation": "custom_scripting",
                        "script": "   ",
                        "language": "groovy2",
                        "use_cache": True,
                    }
                ],
            }
        ]
    )
    _rejects(doc)


def test_model_validator_parity_for_blank_split_identifier():
    """``SplitDocumentsOpV1._non_blank`` — blank link element key/name."""
    doc = _parity_doc(
        [
            {
                "kind": "data_process",
                "steps": [
                    {
                        "operation": "split_documents",
                        "profile_type": "json",
                        "profile_ref": "$ref:p",
                        "link_element_key": "  ",
                        "link_element_name": "n",
                    }
                ],
            }
        ]
    )
    _rejects(doc)


def test_model_validator_parity_for_blank_track_id():
    """``TrackOperandV1._property_id_non_blank`` — blank decision operand id."""
    doc = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                _PARITY_SOURCE,
                {
                    "kind": "decision",
                    "comparison": "equals",
                    "left": {"value_type": "track", "property_id": "   "},
                    "right": {"value_type": "static", "static_value": "x"},
                    "true_arm": {
                        "steps": [{"kind": "message", "text": "t"}],
                        "terminal": {"kind": "stop"},
                    },
                    "false_arm": {
                        "steps": [{"kind": "message", "text": "f"}],
                        "terminal": {"kind": "stop"},
                    },
                },
            ],
        },
    }
    _rejects(doc)


def test_model_validator_parity_for_set_dpp_name():
    """``SetDppNodeV1._name_rules`` — a name carrying whitespace.

    The rule is not merely non-blank: a property name must be whitespace-free,
    because the wire form is a prefix concatenation and an embedded space would
    silently produce a different property.
    """
    doc = _parity_doc(
        [
            {
                "kind": "set_dpp",
                "name": "HAS SPACE",
                "source_values": [{"value_type": "static", "value": "v"}],
            }
        ]
    )
    _rejects(doc)


def test_model_validator_parity_for_try_trailing_cache_put():
    """``TryCatchTryBodyV1._try_body_rules`` — a try body ending on a cache_put.

    A ``cache_put`` must be followed by a stream-replacing read, so it may not
    be the last thing in the protected body.

    The fixture is deliberately valid in every OTHER respect, which took two
    corrections to get right (QA Bug #184). ``retry_count`` is not an input
    field at all — it is a derived read-only property, so authoring it raised
    ``PROCESS_IR_SCHEMA_UNKNOWN_FIELD`` and masked the rule under test. And a
    process-scoped try body must BEGIN with the connector call that produces the
    flow's documents, so omitting it raised
    ``PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED`` — a second, independent
    mask waiting behind the first. With both removed the cited rule is the only
    defect left, and the test fails if that validator is neutered.
    """
    doc = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                {
                    "kind": "try_catch",
                    "scope": "process",
                    "try_body": {
                        "steps": [
                            {"kind": "connector_call", "operation_ref": "$ref:GETOP"},
                            {"kind": "cache_put", "cache_ref": "$ref:c"},
                        ],
                        "terminal": {"kind": "stop"},
                    },
                    "catch_body": {
                        "steps": [{"kind": "message", "text": "c"}],
                        "terminal": {"kind": "stop"},
                    },
                }
            ],
        },
    }
    _rejects(doc)


def test_unexpected_phase_failure_is_redacted():
    """``pipeline._guarded`` — an unexpected stage crash never leaks its text.

    The exception's message and type are deliberately discarded: an internal
    message can echo authored values, and diagnostics are logged.
    """
    from boomi_mcp.compiler.process_ir import pipeline as compiler_pipeline
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError

    def _boom(*_args):
        raise RuntimeError("SENTINEL_AUTHORED_VALUE_IN_EXCEPTION")

    with pytest.raises(ProcessIRCompileError) as excinfo:
        compiler_pipeline._guarded("semantic_lowering", _boom, None)

    error = excinfo.value
    assert [d.code for d in error.diagnostics] == ["PROCESS_IR_COMPILE_INTERNAL"]
    blob = "{0}{1}{2}".format(error, repr(error), [d.message for d in error.diagnostics])
    assert "SENTINEL_AUTHORED_VALUE_IN_EXCEPTION" not in blob
    assert "RuntimeError" not in blob


# ---------------------------------------------------------------------------
# the compile-family constraint is ENFORCED, not merely asserted in prose
# (added after QA Bug #185)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "code",
    [
        "PROCESS_IR_COMPILE_INTERNAL",
        "PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID",
        "PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID",
    ],
)
def test_a_report_cannot_carry_a_compile_family_code(code):
    """ADR-001 §7 gives ``PROCESS_IR_COMPILE_*`` to the compiler.

    The docs claimed a report "cannot carry" one; nothing enforced it, so the
    claim was true only by convention. It is now a field validator: a report
    that could carry a compiler-blame code would tell a caller to fix correct
    input.
    """
    with pytest.raises(ValidationError):
        _diag(code=code)


def test_the_constraint_is_on_the_prefix_not_an_allowlist():
    """A SEMANTIC code that merely mentions compilation is still fine — the rule
    is about the family prefix, not about the word."""
    assert _diag(code="PROCESS_IR_SEMANTIC_COMPILED_THING").code == (
        "PROCESS_IR_SEMANTIC_COMPILED_THING"
    )


def test_the_finding_builder_also_refuses_a_compile_family_code():
    """The constraint has to hold on the path collectors actually use."""
    from boomi_mcp.compiler.process_ir.semantic_validation.findings import finding

    with pytest.raises(ValidationError):
        finding("PROCESS_IR_COMPILE_INTERNAL", "error", "lineage", "/a")
