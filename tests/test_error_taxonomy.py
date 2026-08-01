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


# ---------------------------------------------------------------------------
# Issue #144 (M12.9) — the TOPOLOGY_* family
# ---------------------------------------------------------------------------

_ISSUE_144_CODES = (
    # TOPOLOGY_SCHEMA_* — shape of the authored document itself
    "TOPOLOGY_SCHEMA_UNKNOWN_OBJECT",
    "TOPOLOGY_SCHEMA_UNKNOWN_RELATION",
    "TOPOLOGY_SCHEMA_UNKNOWN_FIELD",
    "TOPOLOGY_SCHEMA_INVALID_CARDINALITY",
    "TOPOLOGY_SCHEMA_DUPLICATE_KEY",
    "TOPOLOGY_SCHEMA_VERSION_UNSUPPORTED",
    "TOPOLOGY_SCHEMA_INVALID",
    # TOPOLOGY_REFERENCE_* — does an authored reference resolve, and to what
    "TOPOLOGY_REFERENCE_NOT_FOUND",
    "TOPOLOGY_REFERENCE_TYPE_MISMATCH",
    # lifecycle / capability / graph
    "TOPOLOGY_RELATION_UNSUPPORTED",
    "TOPOLOGY_CAPABILITY_GATED",
    "TOPOLOGY_ENVIRONMENT_MISMATCH",
    "TOPOLOGY_DEPENDENCY_CYCLE",
    # the standing refusal
    "TOPOLOGY_APPLY_NOT_SUPPORTED",
)


def test_issue_144_codes_present():
    assert set(_ISSUE_144_CODES) <= set(ERROR_TAXONOMY)


def test_issue_144_constants_are_taxonomy_reexports():
    """Each code is reachable as a module constant equal to its own name."""
    for code in _ISSUE_144_CODES:
        assert getattr(taxonomy, code) == code, code


def test_issue_144_adds_exactly_fourteen_codes():
    """Pins the census the #144 plan declares: fourteen TOPOLOGY_* codes.

    Counting by owner is the shrink-detector: a duplicate spec removes one key
    from the last-wins dict entirely, so this total moves even when every code
    the tuple names still resolves.
    """
    assert len(_ISSUE_144_CODES) == 14
    owned = {c for c, s in ERROR_TAXONOMY.items() if s.owner == "#144"}
    assert owned == set(_ISSUE_144_CODES)


def test_issue_144_codes_owned_and_categorized():
    for code in _ISSUE_144_CODES:
        spec = ERROR_TAXONOMY[code]
        assert spec.owner == "#144", code
        assert spec.category == "topology", code
        assert spec.retryable is False, code
        assert spec.summary, code


def test_issue_144_owns_the_whole_topology_family():
    """ADR-001 §7 makes #144 the SOLE introducer of TOPOLOGY_*.

    Asserted as a biconditional, which is stronger than the per-issue subset
    check the ProcessIR families use: those are extended by several issues, so
    only "mine are mine" is checkable there. Here "every TOPOLOGY_ code is
    mine" is also true, and pinning it is what stops a later issue from
    quietly appending to a family whose semantics #144 defined.
    """
    for code in _ISSUE_144_CODES:
        assert code.startswith("TOPOLOGY_"), code
    for code, spec in ERROR_TAXONOMY.items():
        if code.startswith("TOPOLOGY_"):
            assert spec.owner == "#144", code


def test_issue_144_introduces_no_process_ir_or_legacy_code():
    """#144 is not an introducer for any ProcessIR family.

    The topology planner and the ProcessIR compiler are separate authorities
    (ADR-001 §3): a topology finding must never blame process semantics, and a
    compile diagnostic must never blame topology.
    """
    for code in _ISSUE_144_CODES:
        assert not code.startswith("PROCESS_IR_"), code
        assert not code.startswith("LEGACY_ADAPTER_"), code


def test_issue_144_does_not_overwrite_any_prior_code():
    """The silent-overwrite guard, now covering #143 as well."""
    for codes, owner in (
        (_ISSUE_136_CODES, "#136"),
        (_ISSUE_137_CODES, "#137"),
        (_ISSUE_138_CODES, "#138"),
        (_ISSUE_139_CODES, "#139"),
        (_ISSUE_140_CODES, "#140"),
        (_ISSUE_141_CODES, "#141"),
        (_ISSUE_142_CODES, "#142"),
        (_ISSUE_143_CODES, "#143"),
    ):
        for code in codes:
            assert ERROR_TAXONOMY[code].owner == owner, code


def test_prior_m12_code_count_is_unchanged_at_sixty_one():
    """44 pre-#143 ProcessIR codes + #143's 17 = 61, unmoved by #144.

    The sibling test above pins the pre-#143 total at 44 and deliberately
    excludes #143; this one carries the same guard forward one owner so a #144
    collision with a #143 code cannot hide.
    """
    prior = {
        code
        for code, spec in ERROR_TAXONOMY.items()
        if spec.owner
        in {"#136", "#137", "#138", "#139", "#140", "#141", "#142", "#143"}
        and (code.startswith("PROCESS_IR_") or code.startswith("LEGACY_ADAPTER_"))
    }
    assert len(prior) == 61


# ---------------------------------------------------------------------------
# Issue #145 (M12.10) — the RECIPE_* family
# ---------------------------------------------------------------------------

_ISSUE_145_CODES = (
    # lookup / version / capability — is this recipe runnable at all
    "RECIPE_NOT_FOUND",
    "RECIPE_VERSION_UNAVAILABLE",
    "RECIPE_CAPABILITY_GATED",
    # input / output — is what went in and what came out strictly typed
    "RECIPE_INPUT_INVALID",
    "RECIPE_CONTRIBUTION_INVALID",
    # composition — do the closed operations resolve and agree
    "RECIPE_PATCH_TARGET_NOT_FOUND",
    "RECIPE_PATCH_CONFLICT",
    # validation / determinism
    "RECIPE_CONSTRAINT_FAILED",
    "RECIPE_OUTPUT_NONDETERMINISTIC",
    # the request ENVELOPE, as distinct from the recipe input's contents
    "RECIPE_REQUEST_INVALID",
)


def test_issue_145_codes_present():
    assert set(_ISSUE_145_CODES) <= set(ERROR_TAXONOMY)


def test_issue_145_constants_are_taxonomy_reexports():
    """Each code is reachable as a module constant equal to its own name."""
    for code in _ISSUE_145_CODES:
        assert getattr(taxonomy, code) == code, code


def test_issue_145_adds_exactly_ten_codes():
    """Pins the census: the issue's nine RECIPE_* codes plus one.

    ``RECIPE_REQUEST_INVALID`` was added after live QA showed a malformed request
    ENVELOPE (two invocations sharing an id) reusing ``RECIPE_INPUT_INVALID``,
    whose remediation tells the caller their input carried credentials, SQL and
    raw XML. A wrong remediation is worse than a coarse one, and the envelope is
    genuinely a different failure from the input's contents.

    Counting by owner is the shrink-detector: a duplicate spec removes one key
    from the last-wins dict entirely, so this total moves even when every code
    the tuple names still resolves.
    """
    assert len(_ISSUE_145_CODES) == 10
    owned = {c for c, s in ERROR_TAXONOMY.items() if s.owner == "#145"}
    assert owned == set(_ISSUE_145_CODES)


def test_issue_145_codes_owned_and_categorized():
    for code in _ISSUE_145_CODES:
        spec = ERROR_TAXONOMY[code]
        assert spec.owner == "#145", code
        assert spec.category == "recipe", code
        assert spec.retryable is False, code
        assert spec.summary, code


def test_issue_145_owns_the_whole_recipe_family():
    """#145 is the SOLE introducer of RECIPE_*, asserted as a biconditional.

    Same discipline as #144's TOPOLOGY_* guard: "mine are mine" AND "every
    RECIPE_ code is mine", so a later issue cannot quietly append to a family
    whose semantics #145 defined.
    """
    for code in _ISSUE_145_CODES:
        assert code.startswith("RECIPE_"), code
    for code, spec in ERROR_TAXONOMY.items():
        if code.startswith("RECIPE_"):
            assert spec.owner == "#145", code


def test_issue_145_introduces_no_foreign_family_code():
    """The recipe layer blames the RECIPE layer, never a canonical authority.

    A canonical rejection is carried as ``RECIPE_CONSTRAINT_FAILED`` with
    value-free ``cause_codes``; #145 never introduces a ``PROCESS_IR_*``,
    ``TOPOLOGY_*``, ``LEGACY_ADAPTER_*`` or ``COMPOSITION_*`` code of its own.
    """
    for code in _ISSUE_145_CODES:
        assert not code.startswith("PROCESS_IR_"), code
        assert not code.startswith("TOPOLOGY_"), code
        assert not code.startswith("LEGACY_ADAPTER_"), code
        assert not code.startswith("COMPOSITION_"), code


def test_issue_145_does_not_overwrite_any_prior_code():
    """The silent-overwrite guard, now covering #144 as well."""
    for codes, owner in (
        (_ISSUE_136_CODES, "#136"),
        (_ISSUE_137_CODES, "#137"),
        (_ISSUE_138_CODES, "#138"),
        (_ISSUE_139_CODES, "#139"),
        (_ISSUE_140_CODES, "#140"),
        (_ISSUE_141_CODES, "#141"),
        (_ISSUE_142_CODES, "#142"),
        (_ISSUE_143_CODES, "#143"),
        (_ISSUE_144_CODES, "#144"),
    ):
        for code in codes:
            assert ERROR_TAXONOMY[code].owner == owner, code


def test_issue_145_leaves_the_topology_family_at_fourteen():
    """#145 adds no TOPOLOGY_* code and collides with none.

    Carries #144's own census forward one owner, exactly as
    ``test_prior_m12_code_count_is_unchanged_at_sixty_one`` carries #143's.
    """
    owned = {c for c, s in ERROR_TAXONOMY.items() if s.owner == "#144"}
    assert len(owned) == 14


def test_a_recipe_diagnostic_rejects_a_code_outside_the_closed_family():
    """``RecipeDiagnosticV1.code`` is a closed ``RecipeErrorCode``, not ``str``.

    The model is PUBLIC and directly constructible, so before this the closed
    family rested entirely on ``recipe_diagnostic`` being the only constructor —
    which it never was. Nothing observed the type: reverting it to ``str`` left
    the whole suite green, because every test builds diagnostics through the
    helper, and the helper only ever passes a listed constant (issue #145, live
    QA).

    The realistic mistake is a CANONICAL code, not a nonsense string: the recipe
    layer's whole taxonomy rule is that a ``PROCESS_IR_*`` / ``TOPOLOGY_*`` code
    rides along as a value-free ``cause_code`` and never becomes the diagnostic's
    own code.
    """
    import pytest
    from pydantic import ValidationError

    from boomi_mcp.recipes.errors import RECIPE_ERROR_CODES, RecipeDiagnosticV1

    for bad in ("PROCESS_IR_TERMINAL_INVALID", "TOPOLOGY_SCHEMA_UNKNOWN_FIELD", "nope"):
        with pytest.raises(ValidationError):
            RecipeDiagnosticV1(
                code=bad, phase="composition", message="m", remediation="r"
            )

    # Positive control: every member of the declared family IS accepted, so the
    # test cannot pass by the model rejecting everything.
    for good in RECIPE_ERROR_CODES:
        assert (
            RecipeDiagnosticV1(
                code=good, phase="composition", message="m", remediation="r"
            ).code
            == good
        )
