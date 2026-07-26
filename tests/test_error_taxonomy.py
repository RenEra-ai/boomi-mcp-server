"""Contract tests for the shared error taxonomy (Issue #10).

The taxonomy module is the canonical home for stable error codes; modules that
shipped constants before it existed re-export them, so this file pins the
identity between boomi_mcp.errors and every consuming site.
"""

import sys
from pathlib import Path

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp import errors as taxonomy
from boomi_mcp.categories import meta_tools
from boomi_mcp.categories.deployment import deployment_utils
from boomi_mcp.patterns import errors as pattern_errors  # noqa: F401 — import-safety check
from boomi_mcp.errors import (
    ERROR_TAXONOMY,
    RESERVED_ERROR_CODE_PREFIXES,
    ErrorCodeSpec,
)


# ---------------------------------------------------------------------------
# Re-export identity — shipped constants resolve to the taxonomy's objects
# ---------------------------------------------------------------------------


def test_deployment_constants_are_taxonomy_reexports():
    assert (
        deployment_utils.ENVIRONMENT_ACCOUNT_ATOM_ATTACHMENT_UNSUPPORTED
        is taxonomy.ENVIRONMENT_ACCOUNT_ATOM_ATTACHMENT_UNSUPPORTED
    )
    assert (
        deployment_utils.DEPRECATED_ATOM_ATTACHMENT_ACTION
        is taxonomy.DEPRECATED_ATOM_ATTACHMENT_ACTION
    )


def test_meta_tools_raw_write_constant_is_taxonomy_reexport():
    assert (
        meta_tools.RAW_WRITE_CONFIRMATION_REQUIRED
        is taxonomy.RAW_WRITE_CONFIRMATION_REQUIRED
    )


def test_constants_equal_their_own_names():
    """Every taxonomy constant's value is its own name — codes stay greppable."""
    for code, spec in ERROR_TAXONOMY.items():
        assert getattr(taxonomy, code) == code
        assert spec.code == code


# ---------------------------------------------------------------------------
# Catalog shape
# ---------------------------------------------------------------------------


def test_every_taxonomy_entry_is_a_spec_with_required_fields():
    for code, spec in ERROR_TAXONOMY.items():
        assert isinstance(spec, ErrorCodeSpec)
        assert spec.category
        assert spec.summary
        assert spec.owner.startswith("#")
        assert spec.retryable is False


def test_expected_codes_present():
    expected = {
        # shipped before the taxonomy existed
        "ENVIRONMENT_ACCOUNT_ATOM_ATTACHMENT_UNSUPPORTED",
        "DEPRECATED_ATOM_ATTACHMENT_ACTION",
        "RAW_WRITE_CONFIRMATION_REQUIRED",
        # pattern/authoring
        "INVALID_INPUT",
        "PARAM_VALIDATION_FAILED",
        "PATTERN_DISCOVERY_FAILED",
        "PATTERN_NOT_FOUND",
        "DUPLICATE_PATTERN_NAME",
        "INVALID_PATTERN_KIND",
        "PATTERN_CONTRACT_INVALID",
        "ARCHETYPE_BUILD_VALIDATION_FAILED",
        "ARCHETYPE_BUILD_FAILED",
        # schema discovery (#10)
        "SCHEMA_SELECTOR_REQUIRED",
        "SCHEMA_NAME_UNSUPPORTED",
        "SCHEMA_LOOKUP_FAILED",
        "WORKFLOW_SEQUENCE_NOT_FOUND",
        # ProcessIRV1 model/codec boundary (#136, ADR-001 §7)
        "PROCESS_IR_SCHEMA_UNKNOWN_NODE",
        "PROCESS_IR_SCHEMA_UNKNOWN_FIELD",
        "PROCESS_IR_SCHEMA_INVALID_CARDINALITY",
        "PROCESS_IR_SCHEMA_VERSION_UNSUPPORTED",
        "PROCESS_IR_SCHEMA_INVALID",
        "PROCESS_IR_REFERENCE_INVALID_FORMAT",
        "PROCESS_IR_CAPABILITY_UNSUPPORTED",
        # ProcessIR compiler CFG/lowering (#137, ADR-001 §7)
        "PROCESS_IR_SEMANTIC_UNREACHABLE",
        "PROCESS_IR_SEMANTIC_MISSING_TERMINAL",
        "PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW",
        "PROCESS_IR_COMPILE_INTERNAL",
        "PROCESS_IR_COMPILE_NONDETERMINISTIC",
        "PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID",
        # ProcessIR process-emitter registry (#138, M12.3)
        "PROCESS_IR_COMPILE_EMITTER_MISSING",
        "PROCESS_IR_COMPILE_EMITTER_INPUT_INVALID",
        "PROCESS_IR_COMPILE_SYMBOL_UNRESOLVED",
        "PROCESS_IR_COMPILE_XML_INVALID",
        "PROCESS_IR_COMPILE_VERIFIER_FAILED",
    }
    assert expected <= set(ERROR_TAXONOMY)


_ISSUE_137_CODES = (
    "PROCESS_IR_SEMANTIC_UNREACHABLE",
    "PROCESS_IR_SEMANTIC_MISSING_TERMINAL",
    "PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW",
    "PROCESS_IR_COMPILE_INTERNAL",
    "PROCESS_IR_COMPILE_NONDETERMINISTIC",
    "PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID",
)

_ISSUE_136_CODES = (
    "PROCESS_IR_SCHEMA_UNKNOWN_NODE",
    "PROCESS_IR_SCHEMA_UNKNOWN_FIELD",
    "PROCESS_IR_SCHEMA_INVALID_CARDINALITY",
    "PROCESS_IR_SCHEMA_VERSION_UNSUPPORTED",
    "PROCESS_IR_SCHEMA_INVALID",
    "PROCESS_IR_REFERENCE_INVALID_FORMAT",
    "PROCESS_IR_CAPABILITY_UNSUPPORTED",
)


_ISSUE_138_CODES = (
    "PROCESS_IR_COMPILE_EMITTER_MISSING",
    "PROCESS_IR_COMPILE_EMITTER_INPUT_INVALID",
    "PROCESS_IR_COMPILE_SYMBOL_UNRESOLVED",
    "PROCESS_IR_COMPILE_XML_INVALID",
    "PROCESS_IR_COMPILE_VERIFIER_FAILED",
)


def test_issue_137_codes_owned_and_categorized():
    for code in _ISSUE_137_CODES:
        spec = ERROR_TAXONOMY[code]
        assert spec.owner == "#137", code
        assert spec.category == "process_ir", code
        assert spec.retryable is False, code


def test_issue_138_codes_owned_and_categorized():
    for code in _ISSUE_138_CODES:
        spec = ERROR_TAXONOMY[code]
        assert spec.owner == "#138", code
        assert spec.category == "process_ir", code
        assert spec.retryable is False, code


_ISSUE_139_CODES = (
    "LEGACY_ADAPTER_UNSUPPORTED_KIND",
    "LEGACY_ADAPTER_PIPELINE_DRAFT_ONLY",
    "LEGACY_ADAPTER_AUTHORITY_CONFLICT",
    "LEGACY_ADAPTER_SEMANTIC_LOSS",
    "LEGACY_ADAPTER_OUTPUT_PARITY_FAILED",
)


def test_issue_139_codes_present():
    assert set(_ISSUE_139_CODES) <= set(ERROR_TAXONOMY)


def test_issue_139_codes_owned_and_categorized():
    for code in _ISSUE_139_CODES:
        spec = ERROR_TAXONOMY[code]
        assert spec.owner == "#139", code
        assert spec.category == "process_ir", code
        assert spec.retryable is False, code


def test_issue_139_codes_do_not_overwrite_prior_process_ir_codes():
    # The taxonomy is a last-wins dict comprehension; pin that #139's additions
    # did not silently re-register a #136/#137/#138 code.
    for code in _ISSUE_136_CODES:
        assert ERROR_TAXONOMY[code].owner == "#136", code
    for code in _ISSUE_137_CODES:
        assert ERROR_TAXONOMY[code].owner == "#137", code
    for code in _ISSUE_138_CODES:
        assert ERROR_TAXONOMY[code].owner == "#138", code


def test_process_ir_compile_internal_not_re_registered_by_138():
    # #138 REUSES the existing PROCESS_IR_COMPILE_INTERNAL rather than adding a
    # duplicate taxonomy key — it stays #137's.
    assert ERROR_TAXONOMY["PROCESS_IR_COMPILE_INTERNAL"].owner == "#137"


def test_issue_136_codes_still_owned_by_136():
    """Guards the silent-overwrite hazard in ``ERROR_TAXONOMY``.

    The taxonomy is a dict comprehension keyed on ``spec.code``, so a duplicate
    ``ErrorCodeSpec`` for an existing code would replace the earlier entry —
    last-wins, no error, no warning. Nothing else in this file would notice:
    ``test_expected_codes_present`` uses a subset check, and the other tests
    iterate the already-collapsed dict. #137 references
    ``PROCESS_IR_CAPABILITY_UNSUPPORTED`` (for its listener guard) rather than
    re-registering it; this pins that it stayed #136's.
    """
    for code in _ISSUE_136_CODES:
        assert ERROR_TAXONOMY[code].owner == "#136", code


def test_superseded_advisory_code_absent():
    """#79 shipped an ENFORCED gate; the advisory RAW_API_TYPED_TOOL_AVAILABLE
    code it superseded must not exist anywhere in the taxonomy."""
    assert not hasattr(taxonomy, "RAW_API_TYPED_TOOL_AVAILABLE")
    assert "RAW_API_TYPED_TOOL_AVAILABLE" not in ERROR_TAXONOMY


# ---------------------------------------------------------------------------
# Reserved namespaces (#78 / M9.2)
# ---------------------------------------------------------------------------


def test_reserved_prefixes_declared_for_gotcha_codes():
    assert RESERVED_ERROR_CODE_PREFIXES == ("GOTCHA_",)


def test_no_taxonomy_code_squats_on_reserved_prefix():
    for code in ERROR_TAXONOMY:
        for prefix in RESERVED_ERROR_CODE_PREFIXES:
            assert not code.startswith(prefix), (
                f"{code} squats on reserved prefix {prefix} (owned by #78/M9.2)"
            )


# ---------------------------------------------------------------------------
# Issue #143 (M12.8) — unified semantic validation
#
# The tuples for #140-#142 are added here alongside #143's because #143 adds
# codes to the SAME four families they own. Until now the no-overwrite guard
# stopped at #139, so a #143 typo that re-registered, say,
# PROCESS_IR_SEMANTIC_PROFILE_MISMATCH would have flipped its owner silently —
# ERROR_TAXONOMY is a last-wins dict comprehension keyed on spec.code.
# ---------------------------------------------------------------------------

_ISSUE_140_CODES = (
    "PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND",
    "PROCESS_IR_REFERENCE_CONNECTION_NOT_FOUND",
    "PROCESS_IR_REFERENCE_CONNECTION_MISMATCH",
    "PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED",
    "PROCESS_IR_SEMANTIC_PROFILE_MISMATCH",
    "PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH",
    "PROCESS_IR_COMPILE_CONNECTOR_BINDING_INVALID",
)

_ISSUE_141_CODES = (
    "PROCESS_IR_SCHEMA_BRANCH_CARDINALITY",
    "PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED",
    "PROCESS_IR_SEMANTIC_JOIN_UNSUPPORTED",
    "PROCESS_IR_SEMANTIC_NESTING_LIMIT",
    "PROCESS_IR_SEMANTIC_UNTERMINATED_PATH",
    "PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY",
    "PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID",
)

_ISSUE_142_CODES = (
    "PROCESS_IR_SCHEMA_RETRY_COUNT",
    "PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED",
    "PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION",
    "PROCESS_IR_SEMANTIC_RETRY_NON_IDEMPOTENT_WRITE",
    "PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING",
    "PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED",
    "PROCESS_IR_COMPILE_ERROR_REGION_INVALID",
)

_ISSUE_143_CODES = (
    # PROCESS_IR_REFERENCE_* — generic component roles only. The specialized
    # #140 operation/connection codes keep their own conditions.
    "PROCESS_IR_REFERENCE_COMPONENT_NOT_FOUND",
    "PROCESS_IR_REFERENCE_COMPONENT_TYPE_MISMATCH",
    # PROCESS_IR_CAPABILITY_*
    "PROCESS_IR_CAPABILITY_EFFECT_CONTRACT_INVALID",
    # PROCESS_IR_SEMANTIC_* — lineage
    "PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE",
    "PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID",
    "PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID",
    "PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING",
    "PROCESS_IR_SEMANTIC_LINEAGE_AMBIGUOUS_LAST_WRITE",
    "PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN",
    "PROCESS_IR_SEMANTIC_LINEAGE_EXTERNAL_WRITER_ASSUMED",
    # PROCESS_IR_SEMANTIC_* — side effect / retry
    "PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE",
    "PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNKNOWN",
    "PROCESS_IR_SEMANTIC_RETRY_EFFECT_UNSAFE",
    # LEGACY_ADAPTER_EXEMPTION_* — registry-owned advisory policies
    "LEGACY_ADAPTER_EXEMPTION_OPAQUE_STATE_WRITER",
    "LEGACY_ADAPTER_EXEMPTION_DECISION_PROPERTY_READ",
    "LEGACY_ADAPTER_EXEMPTION_STANDALONE_CACHE_READ",
    "LEGACY_ADAPTER_EXEMPTION_SUBPROCESS_SUMMARY",
)


def test_issue_143_codes_present():
    assert set(_ISSUE_143_CODES) <= set(ERROR_TAXONOMY)


def test_issue_143_adds_exactly_seventeen_codes():
    """Pins the count the #143 error-code ledger declares (44 existing + 17)."""
    assert len(_ISSUE_143_CODES) == 17
    owned = {c for c, s in ERROR_TAXONOMY.items() if s.owner == "#143"}
    assert owned == set(_ISSUE_143_CODES)


def test_issue_143_codes_owned_and_categorized():
    for code in _ISSUE_143_CODES:
        spec = ERROR_TAXONOMY[code]
        assert spec.owner == "#143", code
        assert spec.category == "process_ir", code
        assert spec.retryable is False, code


def test_issue_143_introduces_no_compile_family_code():
    """ADR-001 §7: #143 is NOT an introducer for PROCESS_IR_COMPILE_*.

    That family blames the compiler; #143's report blames the authored payload.
    A compile-family code minted here would also be unreachable from
    ValidationReportV1, which deliberately cannot carry one.
    """
    for code in _ISSUE_143_CODES:
        assert not code.startswith("PROCESS_IR_COMPILE_"), code


def test_issue_143_codes_sit_only_in_its_four_declared_families():
    """ADR-001 §7 lists #143 as an introducer for exactly these four."""
    allowed = (
        "PROCESS_IR_REFERENCE_",
        "PROCESS_IR_CAPABILITY_",
        "PROCESS_IR_SEMANTIC_",
        "LEGACY_ADAPTER_EXEMPTION_",
    )
    for code in _ISSUE_143_CODES:
        assert code.startswith(allowed), code


def test_issue_143_does_not_overwrite_any_prior_process_ir_code():
    """The silent-overwrite guard, now covering every prior ProcessIR owner."""
    for codes, owner in (
        (_ISSUE_136_CODES, "#136"),
        (_ISSUE_137_CODES, "#137"),
        (_ISSUE_138_CODES, "#138"),
        (_ISSUE_139_CODES, "#139"),
        (_ISSUE_140_CODES, "#140"),
        (_ISSUE_141_CODES, "#141"),
        (_ISSUE_142_CODES, "#142"),
    ):
        for code in codes:
            assert ERROR_TAXONOMY[code].owner == owner, code


def test_prior_process_ir_code_count_is_unchanged_at_forty_four():
    """A #143 addition must ADD keys, never collapse an existing one.

    Counting by owner catches the failure mode a per-code assertion misses: a
    duplicate spec removes one key from the dict entirely, so the totals move
    even when every code the tuples happen to name still resolves.
    """
    prior = {
        code
        for code, spec in ERROR_TAXONOMY.items()
        if spec.owner in {"#136", "#137", "#138", "#139", "#140", "#141", "#142"}
        and (code.startswith("PROCESS_IR_") or code.startswith("LEGACY_ADAPTER_"))
    }
    assert len(prior) == 44
