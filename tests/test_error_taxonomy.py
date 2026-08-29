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
        assert isinstance(spec.retryable, bool)


def test_exactly_the_intended_codes_advertise_a_retry():
    """`retryable` is a declared field, so a True is legitimate — but rare.

    This assertion used to live in the shape test as `spec.retryable is False`
    for EVERY code, which was true only because no retryable code existed yet:
    it pinned an accident of the catalog's contents as though it were a rule,
    and the first genuinely retryable condition would have had to either lie in
    the catalog or quietly weaken the guard. Naming the set keeps the teeth —
    a new retryable code is a decision someone has to make here — without
    forcing the catalog to misdescribe itself.
    """
    retryable = {code for code, spec in ERROR_TAXONOMY.items() if spec.retryable}
    assert retryable == {
        # An authority the authoring contract derives from failed. The selector
        # is real and the request was valid, so retrying can genuinely succeed —
        # unlike every other code here, which describes a caller mistake or a
        # permanent platform limit.
        "AUTHORING_SCHEMA_SOURCE_UNAVAILABLE",
        # The live XML could not be READ, so the merge never started and nothing
        # was written. That is the one preservation failure where a retry is
        # genuinely safe, and the served envelope already advertises
        # `retryable: True` on it — registering the family (#153, from Codex
        # review round 2) is what brought the declaration into the catalog.
        # Its sibling PUSH_FAILED is deliberately NOT here: the merged document
        # was submitted, so a write may already have landed.
        "UPDATE_PRESERVATION_FETCH_FAILED",
        # The apply's post-write FINALIZATION failed on a run whose every root
        # was reused (#153, Codex round 24). The retry is safe on the same
        # evidence the envelope serves: the steps' own statuses prove no write
        # was attempted, which is why this code is reachable only on the
        # no-write escape. The mid-write escape deliberately carries no code —
        # there the outcome is unknown, and advertising a retry would be the
        # exact defect this code was registered to remove.
        "PROCESS_MATERIALIZATION_FINALIZATION_FAILED",
    }, sorted(retryable)


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


# ---------------------------------------------------------------------------
# M12.15 / issue #153 — canonical process component materialization
# ---------------------------------------------------------------------------

#: The three prefixes #153 opens. Kept as data so the biconditional below reads
#: from ONE authority instead of repeating the prefix list per assertion.
_ISSUE_153_PREFIXES = (
    "PROCESS_COMPONENT_",
    "INTEGRATION_DEPENDENCY_",
    "PROCESS_MATERIALIZATION_",
)

#: ``INTEGRATION_COMPONENT_KEY_DUPLICATE`` belongs to the dependency family by
#: subject but not by prefix — it blames the shared key namespace, which is the
#: same authority the depends_on codes blame. Named explicitly rather than
#: widening the prefix tuple to ``INTEGRATION_``, which would sweep in unrelated
#: future codes.
_ISSUE_153_EXTRA_CODES = frozenset({"INTEGRATION_COMPONENT_KEY_DUPLICATE"})


def test_issue_153_owns_its_three_families_as_a_biconditional():
    """#153 is the SOLE introducer of its three families.

    The same biconditional #144 carries for ``TOPOLOGY_*``: every code #153 owns
    sits under one of its prefixes (or is the named key-namespace code), AND
    every taxonomy key under those prefixes is owned by #153. The forward half
    alone would let a later issue append to a family whose semantics #153
    defined; the reverse half alone would let #153 scatter codes into families
    it does not own.
    """
    owned = {code for code, spec in ERROR_TAXONOMY.items() if spec.owner == "#153"}
    assert owned, "no #153 codes registered — this check would be vacuous"

    for code in owned:
        assert code.startswith(_ISSUE_153_PREFIXES) or code in _ISSUE_153_EXTRA_CODES, code

    for code, spec in ERROR_TAXONOMY.items():
        if code.startswith(_ISSUE_153_PREFIXES) or code in _ISSUE_153_EXTRA_CODES:
            assert spec.owner == "#153", code


def test_issue_153_does_not_extend_a_family_it_does_not_own():
    """#153 introduces no ``TOPOLOGY_*``, ``AUTHORING_*`` or ``PROCESS_IR_*`` code.

    ``TOPOLOGY_*`` is closed to #144 by ADR-001 §7; ``AUTHORING_*`` blames the
    MCP authoring surface rather than the authored process component; the
    ProcessIR families belong to the compiler. #153 reaches callers THROUGH the
    authoring surface, which is exactly why this is worth pinning — proximity is
    not ownership.
    """
    owned = {code for code, spec in ERROR_TAXONOMY.items() if spec.owner == "#153"}
    for code in owned:
        assert not code.startswith(("TOPOLOGY_", "AUTHORING_", "PROCESS_IR_")), code


def test_issue_153_codes_carry_distinct_registered_categories():
    """Each family declares its own category, and every #153 spec is complete.

    A duplicated ``code=`` in the ``ERROR_TAXONOMY`` comprehension silently
    overwrites the earlier entry and shrinks the dict, so the count is asserted
    against the source tuple rather than trusted.
    """
    owned = {code: spec for code, spec in ERROR_TAXONOMY.items() if spec.owner == "#153"}
    categories = {spec.category for spec in owned.values()}
    assert categories == {
        "process_component",
        "integration_dependency",
        "process_materialization",
    }, sorted(categories)
    for code, spec in owned.items():
        assert spec.summary.strip(), code
    # #153's codes are non-retryable with ONE registered exception: a post-write
    # finalization failure on a run that mutated nothing (Codex round 24).
    # Asserted as the exact retryable SET rather than by relaxing the per-code
    # rule, so a second retryable code cannot slip in under it — the family's
    # default is still "do not retry", and the exception has to be named.
    assert {code for code, spec in owned.items() if spec.retryable} == {
        "PROCESS_MATERIALIZATION_FINALIZATION_FAILED"
    }, sorted(code for code, spec in owned.items() if spec.retryable)


# ---------------------------------------------------------------------------
# Issue #155 — the codes this slice OWNS, pinned non-vacuously
# ---------------------------------------------------------------------------


import pytest  # noqa: E402 — this module had no parametrised test before

ISSUE_155_CODES = (
    "PROCESS_IR_CAPABILITY_DYNAMIC_PATH_UNSUPPORTED",
    "PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED",
    "PROCESS_IR_SEMANTIC_DYNAMIC_PATH_NO_DYNAMIC_SEGMENT",
    "PROCESS_IR_SEMANTIC_DYNAMIC_PATH_PROFILE_BINDING_MISMATCH",
    # slice C — the operation stores a blank path and the step binds none
    "PROCESS_IR_SEMANTIC_DYNAMIC_PATH_REQUIRED",
    "PROCESS_IR_SEMANTIC_RETRY_SOURCE_POLICY_REQUIRES_RETRY",
    "PROCESS_IR_SEMANTIC_RETRY_SOURCE_POLICY_SCOPE_INVALID",
    # slice B — the connector-replay evidence registry
    "CONNECTOR_REPLAY_CONFIGURATION_DIGEST_REFUSED",
    "CONNECTOR_REPLAY_REGISTRY_INVALID",
    "CONNECTOR_REPLAY_ROUTE_DIGEST_REFUSED",
    # slice C — the trusted connector-resolution snapshot. Raised in the AUTHORING
    # layer, never under compiler/process_ir: connector identity is an account fact.
    "CONNECTOR_REPLAY_IDENTITY_MISMATCH",
    "CONNECTOR_REPLAY_IDENTITY_UNAVAILABLE",
)


def test_issue_155_owns_exactly_these_codes_and_they_are_fully_specified():
    """A presence-and-ownership pin, which the taxonomy suite otherwise lacks.

    Everything else here checks the SHAPE of whatever rows happen to exist, so a
    slice could delete one of its codes, or hand it to another owner, and the
    suite would stay green. The set is asserted by equality rather than by
    containment so a code silently added under this owner also fails — an owner
    is a claim about who is accountable for the text, not a label.
    """
    from boomi_mcp.errors import ERROR_TAXONOMY

    owned = {code for code, spec in ERROR_TAXONOMY.items() if spec.owner == "#155"}
    assert owned == set(ISSUE_155_CODES), {
        "missing": sorted(set(ISSUE_155_CODES) - owned),
        "unexpected": sorted(owned - set(ISSUE_155_CODES)),
    }

    for code in ISSUE_155_CODES:
        spec = ERROR_TAXONOMY[code]
        assert spec.summary and spec.summary.strip(), code
        assert spec.category and spec.category.strip(), code
        # Served text: a summary citing a document a caller cannot fetch sends
        # them nowhere, and this repo's sweep already found one such class.
        assert "docs/" not in spec.summary, code


def _issue_155_witnesses():
    """One document per #155 code that the compiler ACTUALLY refuses with it.

    A grep for the constant is not a reachability proof, and the first version of
    this test was exactly that: it counted any mention outside the taxonomy as a
    raiser. The codes also appear in the message and remediation tables, so the
    construction site could be deleted and the test stayed green — verified by
    neutering it, which is how the defect was confirmed rather than argued.

    A witness cannot be fooled that way: if nothing constructs the code, no
    document produces it.
    """
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from test_process_ir_semantic_lineage import (  # noqa: E402
        _BOUND, _DPPSEG, _STATIC, _dynpath_symbols,
    )
    from boomi_mcp.compiler.process_ir.contracts import (  # noqa: E402
        ComponentSymbolV1, SymbolTableV1,
    )

    base = _dynpath_symbols()
    soap_symbols = SymbolTableV1(
        symbols=list(base.symbols) + [
            ComponentSymbolV1(ref="$ref:SCONN", component_id="SC",
                              component_type="connector-settings",
                              connector_type="wssoapclientsdk"),
            ComponentSymbolV1(ref="$ref:SOP", component_id="SO",
                              component_type="connector-action",
                              connector_type="wssoapclientsdk", action_type="EXECUTE",
                              connection_ref="$ref:SCONN"),
        ],
        idempotency_contracts=base.idempotency_contracts,
    )
    writer = {"kind": "set_ddp", "name": "P", "source_values": [_STATIC, _DPPSEG]}
    static_only = {"kind": "set_ddp", "name": "P", "source_values": [_STATIC]}
    profile_src = {"value_type": "profile", "profile_ref": "$ref:PROF",
                   "profile_type": "profile.json", "element_id": "3",
                   "element_name": "clientId (Root/Object/clientId)"}
    stop = {"kind": "stop"}

    return {
        "PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED": ([_BOUND, stop], None),
        "PROCESS_IR_SEMANTIC_DYNAMIC_PATH_NO_DYNAMIC_SEGMENT": ([static_only, _BOUND, stop], None),
        "PROCESS_IR_SEMANTIC_DYNAMIC_PATH_PROFILE_BINDING_MISMATCH": (
            [{"kind": "set_ddp", "name": "P", "source_values": [_STATIC, profile_src]},
             _BOUND, stop], None),
        "PROCESS_IR_CAPABILITY_DYNAMIC_PATH_UNSUPPORTED": (
            [writer,
             {"kind": "connector_call", "operation_ref": "$ref:SOP",
              "path_binding": {"property_name": "P"}}, stop],
            soap_symbols),
    }


@pytest.mark.parametrize("code", sorted(_issue_155_witnesses() if False else [
    "PROCESS_IR_CAPABILITY_DYNAMIC_PATH_UNSUPPORTED",
    "PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED",
    "PROCESS_IR_SEMANTIC_DYNAMIC_PATH_NO_DYNAMIC_SEGMENT",
    "PROCESS_IR_SEMANTIC_DYNAMIC_PATH_PROFILE_BINDING_MISMATCH",
]))
def test_each_dynamic_path_code_is_produced_by_a_real_document(code):
    """Reachability proved by EXERCISING each code, not by finding its name."""
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
    from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    steps, symbols = _issue_155_witnesses()[code]
    if symbols is None:
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from test_process_ir_semantic_lineage import _dynpath_symbols
        symbols = _dynpath_symbols()

    doc = {"version": "1", "body": {"kind": "sequence", "steps": steps}}
    with pytest.raises(ProcessIRCompileError) as excinfo:
        compile_process_ir_v1(parse_process_ir_v1(doc), symbols)
    assert code in [d.code for d in excinfo.value.diagnostics], [
        d.code for d in excinfo.value.diagnostics]


@pytest.mark.parametrize("code", [
    "PROCESS_IR_SEMANTIC_RETRY_SOURCE_POLICY_REQUIRES_RETRY",
    "PROCESS_IR_SEMANTIC_RETRY_SOURCE_POLICY_SCOPE_INVALID",
])
def test_each_replay_policy_code_is_produced_by_a_real_document(code):
    """The other two #155 codes, exercised through the error-handling builders."""
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from test_process_ir_error_handling import (  # noqa: E402
        _compile_error, _connector_scope, _process_scope,
    )

    allow = {"source_replay_policy": "allow_duplicates"}
    doc = (_process_scope(retry={"count": 0, **allow})
           if code.endswith("REQUIRES_RETRY")
           else _connector_scope(retry={"count": 2, **allow}))
    assert _compile_error(doc).code == code
