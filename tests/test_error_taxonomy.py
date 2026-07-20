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


def test_issue_137_codes_owned_and_categorized():
    for code in _ISSUE_137_CODES:
        spec = ERROR_TAXONOMY[code]
        assert spec.owner == "#137", code
        assert spec.category == "process_ir", code
        assert spec.retryable is False, code


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
