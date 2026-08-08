"""#146 amendment: the authoring contract is pinned to its runtime authorities.

Every test here is TWO-WAY. A one-way test ("every registry row has an entry")
catches a missing projection but not an invented one, and an invented entry — a
served claim with no code behind it — is the exact failure this contract exists
to prevent. So each axis asserts set EQUALITY between the runtime source and the
projected entries.

The drift negative controls at the bottom are the other half. They perturb ONE
source, rebuild, and assert the parity check now fails. Without them a parity
test can pass because both sides are derived from the same accidental value; with
them, changing a capability state or a placement row without updating the
contract is a CI failure rather than a silent divergence.

Perturbation goes through an injected ``ProjectionSourcesV1``, never a
monkeypatch of a production mapping. A mutated module-level registry leaks into
every later test in the process, and a suite that passes for that reason is worse
than one that fails.
"""

import json
import sys
from pathlib import Path

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.authoring.process_ir_projection import (  # noqa: E402
    ProcessIRAuthoringQueryError,
    build_process_ir_authoring_entries,
    build_process_ir_authoring_index,
    collect_projection_sources,
    process_ir_authoring_revision_payload,
    query_process_ir_authoring_contract,
    state_mappings,
    validate_process_ir_authoring_projection,
)
from boomi_mcp.compiler.process_ir.body_capabilities import (  # noqa: E402
    BODY_CAPABILITIES_V1,
    PUBLIC_BODY_CONTEXTS,
    body_placement_rows,
)
from boomi_mcp.compiler.process_ir.connector_capabilities import (  # noqa: E402
    CONNECTOR_CALL_CAPABILITIES_V1,
    PUBLIC_CAPABILITY_FIELDS,
    ConnectorCapabilityV1,
    connector_capability_rows,
)
from boomi_mcp.compiler.process_ir.diagnostics import (  # noqa: E402
    compiler_diagnostic_specs,
)
from boomi_mcp.compiler.process_ir.error_handling import retry_rule_specs  # noqa: E402
from boomi_mcp.compiler.process_ir.semantic_validation.findings import (  # noqa: E402
    finding_specs,
)
from boomi_mcp.compiler.process_ir.semantic_validation.lineage import (  # noqa: E402
    STATE_VISIBILITY_V1,
    state_visibility_rows,
)
from boomi_mcp.errors import ERROR_TAXONOMY  # noqa: E402
from boomi_mcp.kb.design_doctrine import DESIGN_DOCTRINE_ENTRIES  # noqa: E402
from boomi_mcp.models.cache_property_models import (  # noqa: E402
    PROCESS_PROPERTY_SCOPE_V1,
)
from boomi_mcp.models.process_ir import (  # noqa: E402
    PROCESS_IR_V1_CAPABILITIES,
    process_ir_v1_node_kinds,
)
from boomi_mcp.models.recipe_contributions import (  # noqa: E402
    RECIPE_CONTRIBUTION_KINDS,
)


def entries():
    return build_process_ir_authoring_entries()


def ids_of(entry_type):
    return {e.subject for e in entries() if e.entry_type == entry_type}


# ---------------------------------------------------------------------------
# Two-way coverage, one axis per runtime authority
# ---------------------------------------------------------------------------


def test_node_entries_equal_the_authored_node_kinds():
    assert ids_of("node") == set(process_ir_v1_node_kinds())


def test_capability_entries_equal_the_capability_manifest():
    assert ids_of("capability") == set(PROCESS_IR_V1_CAPABILITIES)


def test_capability_states_are_reproduced_verbatim():
    """The authority's OWN word, not a paraphrase of it.

    ``source_state`` must be byte-identical to the manifest value. The canonical
    state may differ (it is a mapping), but the moment the source state stops
    matching, a caller comparing the two surfaces sees two different answers to
    one question.
    """
    projected = {
        e.subject: e.source_state for e in entries() if e.entry_type == "capability"
    }
    assert projected == dict(PROCESS_IR_V1_CAPABILITIES)


def test_gated_and_unsupported_never_collapse():
    """The distinction that survives every mapping.

    'gated' means not yet; 'unsupported' means never. Collapsing them would tell
    a caller to abandon a design that is merely pending — or to wait for one that
    is not coming.
    """
    for entry in entries():
        if entry.entry_type != "capability":
            continue
        assert entry.canonical_state == entry.source_state, entry.contract_entry_id


def test_placement_entries_equal_the_body_registry_rows():
    expected = {
        f"{PUBLIC_BODY_CONTEXTS[context]}.{slot}"
        for context, slot in BODY_CAPABILITIES_V1
    }
    assert ids_of("placement") == expected


def test_placement_admitted_kinds_equal_the_registry_in_both_directions():
    by_id = {
        e.subject: set(e.node_kinds) for e in entries() if e.entry_type == "placement"
    }
    for (context, slot), kinds in BODY_CAPABILITIES_V1.items():
        key = f"{PUBLIC_BODY_CONTEXTS[context]}.{slot}"
        assert by_id[key] == set(kinds), key


def test_the_public_body_context_map_is_total_and_injective():
    """A rename that merged two contexts would silently lose a whole slot."""
    contexts = {context for context, _ in BODY_CAPABILITIES_V1}
    assert contexts <= set(PUBLIC_BODY_CONTEXTS)
    public = [PUBLIC_BODY_CONTEXTS[c] for c in contexts]
    assert len(set(public)) == len(public)


def test_connector_entries_equal_the_connector_registry():
    expected = {
        f"{spec.family} {spec.action}" for spec in CONNECTOR_CALL_CAPABILITIES_V1.values()
    }
    assert ids_of("connector_action") == expected


def test_the_public_capability_field_map_is_total_and_injective():
    """Every field is projected, and no two collapse into one.

    A partial map would drop a fact silently; a non-injective one would publish
    two different facts under a single name.
    """
    model_fields = set(ConnectorCapabilityV1.model_fields)
    assert model_fields == set(PUBLIC_CAPABILITY_FIELDS)
    public = list(PUBLIC_CAPABILITY_FIELDS.values())
    assert len(set(public)) == len(public)


def test_connector_rows_carry_every_field_with_verbatim_values():
    rows = {(row["family"], row["action"]): row for row in connector_capability_rows()}
    assert len(rows) == len(CONNECTOR_CALL_CAPABILITIES_V1)
    for spec in CONNECTOR_CALL_CAPABILITIES_V1.values():
        row = rows[(spec.family, spec.action)]
        dumped = spec.model_dump(mode="json")
        for internal, public in PUBLIC_CAPABILITY_FIELDS.items():
            assert row[public] == dumped[internal], (spec.family, spec.action, internal)


def test_retry_rules_reproduce_the_shipped_fail_closed_rule():
    """Stated as SHIPPED, including the part callers get wrong.

    Refusal over ``non_idempotent``/``unverified`` is absolute — no evidence
    lifts it. Publishing ``evidence_can_authorise`` explicitly is what stops
    "attach evidence and it will work" reading as true.
    """
    rules = {row["replay_classification"]: row for row in retry_rule_specs()}
    assert rules["non_idempotent"]["retry_permitted"] is False
    assert rules["unverified"]["retry_permitted"] is False
    assert rules["non_idempotent"]["evidence_can_authorise"] is False
    assert rules["unverified"]["evidence_can_authorise"] is False
    assert rules["idempotent_write"]["required_evidence"] == "verified_action"
    assert rules["conditionally_idempotent"]["required_evidence"] == "key_reference"
    assert rules["read_only"]["required_evidence"] == ""


def test_diagnostic_entries_equal_the_union_of_the_three_spec_tables():
    from boomi_mcp.models.process_ir import process_ir_v1_parse_diagnostic_specs

    expected = set()
    for specs in (
        process_ir_v1_parse_diagnostic_specs(),
        finding_specs(),
        compiler_diagnostic_specs(),
    ):
        expected |= {spec["code"] for spec in specs}
    assert ids_of("diagnostic") == expected


def test_every_projected_diagnostic_code_is_a_registered_error():
    """A code with no taxonomy entry is a code nobody owns."""
    for entry in entries():
        for code in entry.diagnostic_codes:
            assert code in ERROR_TAXONOMY, code


def test_state_visibility_entries_equal_the_four_scopes():
    expected = set(STATE_VISIBILITY_V1) | {PROCESS_PROPERTY_SCOPE_V1["state_scope"]}
    assert ids_of("state_visibility") == expected


def test_process_property_is_not_claimed_by_the_lineage_model():
    """The false-equality guard.

    ``lineage`` models execution state. A process property is a component with
    deploy-time defaults — pinning it against the lineage traversal would assert
    an ownership neither side enforces, and the test would pass by agreeing with
    a claim that is not true.
    """
    assert "processproperty" not in STATE_VISIBILITY_V1
    assert PROCESS_PROPERTY_SCOPE_V1["state_scope"] == "processproperty"
    assert PROCESS_PROPERTY_SCOPE_V1["scope"] == "component"


def test_state_visibility_descriptor_is_load_bearing_not_a_second_copy():
    """The descriptor DRIVES the traversal, so the two cannot disagree.

    Flip the lifetime of DDP in a copy of the model and the compartment choice
    must follow. If ``_State`` had kept its hard-coded ``key[0] == DDP`` test,
    this would still pass with a stale descriptor — which is the drift the
    contract exists to prevent, one level down.
    """
    from boomi_mcp.compiler.process_ir.semantic_validation import lineage

    assert lineage._DOCUMENT_LIFETIME_SCOPES == frozenset(
        scope
        for scope, row in STATE_VISIBILITY_V1.items()
        if row["lifetime"] == "document"
    )
    state = lineage._State()
    assert state.with_write(("ddp", "X")).document
    assert not state.with_write(("ddp", "X")).execution
    assert state.with_write(("dpp", "Y")).execution
    assert not state.with_write(("dpp", "Y")).document


def test_doctrine_entries_equal_the_doctrine_registry():
    assert ids_of("doctrine") == set(DESIGN_DOCTRINE_ENTRIES)


def test_doctrine_states_map_without_erasing_the_advice_distinction():
    """``guidance_only`` is advice, not a withdrawn feature.

    It maps to ``unsupported`` for uniformity, but ``applicable=False`` says the
    row was never a capability at all — so a caller is not told a feature was
    removed when none was ever offered.
    """
    for entry in entries():
        if entry.entry_type != "doctrine":
            continue
        source = DESIGN_DOCTRINE_ENTRIES[entry.subject]["capability_status"]
        assert entry.source_state == source
        assert entry.applicable is (source not in ("guidance_only", "na"))


def test_recipe_contribution_entries_equal_the_closed_kind_set():
    assert ids_of("recipe_contribution") == set(RECIPE_CONTRIBUTION_KINDS)


def test_recipe_entries_equal_the_production_registry():
    sources = collect_projection_sources()
    assert ids_of("recipe") == {row["recipe_id"] for row in sources.recipe_entries}


# ---------------------------------------------------------------------------
# Structural invariants that make a citation trustworthy
# ---------------------------------------------------------------------------


def test_projection_self_check_is_clean():
    assert validate_process_ir_authoring_projection() == ()


def test_every_entry_names_at_least_one_source_with_a_derivation_mode():
    for entry in entries():
        assert entry.sources, entry.contract_entry_id
        for source in entry.sources:
            assert source.projection in ("generated", "parity_pinned")
            assert source.source_id.startswith("runtime.")


def test_every_related_entry_id_resolves():
    known = {e.contract_entry_id for e in entries()}
    for entry in entries():
        for related in entry.related_entry_ids:
            assert related in known, f"{entry.contract_entry_id} -> {related}"


def test_every_schema_ref_resolves_against_the_served_process_ir_schema():
    from boomi_mcp.models.process_ir import process_ir_v1_json_schema

    defs = process_ir_v1_json_schema()["$defs"]
    for entry in entries():
        for ref in entry.schema_refs:
            assert ref.startswith("#/$defs/")
            assert ref[len("#/$defs/") :] in defs, ref


def test_merge_is_an_alias_of_joins_not_a_second_row():
    """One construct, one state.

    A separate ``capability.merge`` row would be a second place to update, and
    the second place is the one that goes stale.
    """
    assert "capability.merge" not in {e.contract_entry_id for e in entries()}
    joins = next(e for e in entries() if e.contract_entry_id == "capability.joins")
    assert "merge" in joins.display_aliases


def test_the_id_token_substitution_applies_exactly_where_it_is_needed():
    """The map exists for one collision, and must not quietly grow.

    Lowercasing a diagnostic code is the id rule; the substitution is the
    documented exception. A second silent entry in the map would mean an id that
    no longer follows from its code.
    """
    from boomi_mcp.authoring.process_ir_projection import _PUBLIC_CODE_TOKENS

    assert _PUBLIC_CODE_TOKENS == {"emitter_input": "emission_input"}
    ids = {e.contract_entry_id for e in entries() if e.entry_type == "diagnostic"}
    assert "diagnostic.process_ir_compile_emission_input_invalid" in ids
    subjects = {e.subject for e in entries() if e.entry_type == "diagnostic"}
    # The CODE itself is public and reaches callers verbatim — only the id is
    # respelled, so the subject must still carry the exact code.
    assert "PROCESS_IR_COMPILE_EMITTER_INPUT_INVALID" in subjects


def test_the_projection_is_deterministic():
    first = json.dumps(process_ir_authoring_revision_payload(), sort_keys=True)
    second = json.dumps(process_ir_authoring_revision_payload(), sort_keys=True)
    assert first == second


def test_no_served_entry_cites_an_unserved_repository_artifact():
    blob = json.dumps(process_ir_authoring_revision_payload())
    for artifact in (".codex/", "docs/architecture", "PROCESS_IR_V1_CAPABILITIES"):
        assert artifact not in blob, artifact


def test_no_compiler_internal_name_reaches_the_projection():
    from test_process_ir_compiler_surface import FORBIDDEN_NAMES

    blob = json.dumps(
        [
            process_ir_authoring_revision_payload(),
            build_process_ir_authoring_index(),
        ],
        default=str,
    )
    leaked = sorted({name for name in FORBIDDEN_NAMES if name in blob})
    assert leaked == []


# ---------------------------------------------------------------------------
# Drift negative controls
# ---------------------------------------------------------------------------
#
# Each one perturbs a single source and asserts the contract CHANGES. A parity
# suite with no negative control can pass because both sides read the same
# accidental value; these prove the pin is real.


def _perturbed(**overrides):
    return collect_projection_sources()._replace(**overrides)


def test_drift_control_flipping_a_capability_state_changes_the_contract():
    rows = dict(PROCESS_IR_V1_CAPABILITIES)
    assert rows["joins"] == "gated"
    rows["joins"] = "supported"
    drifted = build_process_ir_authoring_entries(_perturbed(capability_rows=rows))
    entry = next(e for e in drifted if e.contract_entry_id == "capability.joins")
    assert entry.source_state == "supported"
    # ...and the shipped contract still says the true thing.
    live = next(e for e in entries() if e.contract_entry_id == "capability.joins")
    assert live.source_state == "gated"


def test_drift_control_removing_a_placement_row_changes_the_contract():
    rows = tuple(
        row
        for row in body_placement_rows()
        if not (row[0] == "branch_path" and row[1] == "terminal")
    )
    drifted = build_process_ir_authoring_entries(_perturbed(placement_rows=rows))
    ids = {e.contract_entry_id for e in drifted}
    assert "placement.branch_path.terminal" not in ids
    assert "placement.branch_path.terminal" in {e.contract_entry_id for e in entries()}


def test_drift_control_changing_a_replay_classification_changes_the_contract():
    rows = []
    for row in connector_capability_rows():
        row = dict(row)
        if row["family"] == "database" and row["action"] == "Send":
            row["replay_classification"] = "idempotent_write"
        rows.append(row)
    drifted = build_process_ir_authoring_entries(_perturbed(connector_rows=tuple(rows)))
    entry = next(
        e for e in drifted if e.contract_entry_id == "connector_action.database.send"
    )
    assert any("idempotent_write" in fact for fact in entry.ordering_facts)
    assert any("May sit inside a retried region." == fact for fact in entry.ordering_facts)
    live = next(
        e for e in entries() if e.contract_entry_id == "connector_action.database.send"
    )
    assert any("May NOT sit inside" in fact for fact in live.ordering_facts)


def test_drift_control_changing_a_validator_remediation_changes_the_contract():
    code = "PROCESS_IR_SEMANTIC_LINEAGE_BRANCH_ORDER_INVALID"
    specs = tuple(
        dict(spec, remediation="CHANGED") if spec["code"] == code else spec
        for spec in finding_specs()
    )
    drifted = build_process_ir_authoring_entries(_perturbed(finding_specs=specs))
    entry = next(e for e in drifted if e.subject == code)
    # Substring, not membership: each fact is prefixed with the PHASE that
    # emits it, so a code with two disagreeing producers publishes both.
    assert any("CHANGED" in fact for fact in entry.ordering_facts)
    assert any(fact.startswith("[semantic validation]") for fact in entry.ordering_facts)
    live = next(e for e in entries() if e.subject == code)
    assert not any("CHANGED" in fact for fact in live.ordering_facts)


def test_drift_control_changing_a_state_visibility_fact_changes_the_contract():
    rows = tuple(
        dict(row, visible_across_sibling_paths=True)
        if row["state_scope"] == "ddp"
        else row
        for row in state_visibility_rows()
    )
    drifted = build_process_ir_authoring_entries(_perturbed(state_visibility=rows))
    entry = next(e for e in drifted if e.contract_entry_id == "state_visibility.ddp")
    assert any("sibling paths: yes" in fact for fact in entry.ordering_facts)
    live = next(e for e in entries() if e.contract_entry_id == "state_visibility.ddp")
    assert any("sibling paths: no" in fact for fact in live.ordering_facts)


def test_drift_control_a_new_node_kind_with_no_facts_fails_loudly():
    """Coverage cannot be lost silently.

    A node kind added to the models with no authoring facts must BREAK the
    build, not be skipped. A skip would leave the two-way test passing while a
    caller who reaches for that kind learns nothing.
    """
    kinds = tuple(sorted(set(process_ir_v1_node_kinds()) | {"brand_new_kind"}))
    with pytest.raises(KeyError):
        build_process_ir_authoring_entries(_perturbed(node_kinds=kinds))


# ---------------------------------------------------------------------------
# Retrieval contract
# ---------------------------------------------------------------------------


def test_bare_retrieval_returns_facets_and_no_entries():
    page = query_process_ir_authoring_contract()
    assert page.returned_entry_count == 0
    assert page.matched_entry_count == 0
    assert page.catalog_entry_count == len(entries())
    assert page.facets.node_kinds
    assert page.state_mappings


def test_an_unknown_exact_id_is_a_successful_empty_result():
    """Dangling citations must be OBSERVABLE, not indistinguishable from a typo.

    The clean-room harness resolves citations by exact id; if an unknown id
    raised, a dangling citation would look like a malformed request instead of a
    missing entry.
    """
    page = query_process_ir_authoring_contract(authoring_entry_id="node.does_not_exist")
    assert page.returned_entry_count == 0
    assert page.matched_entry_count == 0


def test_an_unknown_enumerated_filter_reports_the_facet():
    with pytest.raises(ProcessIRAuthoringQueryError) as exc:
        query_process_ir_authoring_contract(category="nonsense")
    assert exc.value.field == "category"
    assert exc.value.allowed


def test_a_cursor_without_a_semantic_filter_is_rejected():
    with pytest.raises(ProcessIRAuthoringQueryError):
        query_process_ir_authoring_contract(after_entry_id="node.branch")


@pytest.mark.parametrize("limit", [0, -1, 51, 1000])
def test_limit_bounds_are_enforced(limit):
    with pytest.raises(ProcessIRAuthoringQueryError):
        query_process_ir_authoring_contract(category="capability", limit=limit)


def test_filters_and_together():
    page = query_process_ir_authoring_contract(
        category="control", node_kind="branch", limit=50
    )
    assert page.matched_entry_count >= 1
    for entry in page.entries:
        assert entry.category == "control"
        assert "branch" in entry.node_kinds


def test_paging_a_filter_reaches_every_matching_entry_exactly_once():
    seen = []
    cursor = None
    for _ in range(50):
        page = query_process_ir_authoring_contract(
            category="diagnostic", after_entry_id=cursor, limit=5
        )
        seen.extend(entry.contract_entry_id for entry in page.entries)
        if not page.truncated:
            break
        cursor = page.next_after_entry_id
        assert cursor is not None
    expected = sorted(e.contract_entry_id for e in entries() if e.category == "diagnostic")
    assert seen == expected
    assert len(set(seen)) == len(seen)


def test_entries_are_returned_in_stable_id_order():
    # ``category`` is the entry's DOMAIN (control, connector, state, ...), not
    # its entry_type — a node entry is categorised by what it does, so there is
    # deliberately no "node" category to ask for.
    page = query_process_ir_authoring_contract(category="capability", limit=50)
    ids = [entry.contract_entry_id for entry in page.entries]
    assert ids == sorted(ids)


def test_the_index_publishes_counts_but_never_the_entries():
    index = build_process_ir_authoring_index()
    assert index["entry_count"] == len(entries())
    assert sum(index["entry_counts_by_type"].values()) == len(entries())
    assert "entries" not in index
    assert index["retrieval"]["bare_retrieval_returns_entries"] is False


def test_state_mappings_cover_every_projected_source_state():
    """No entry may carry a state the published mapping does not explain."""
    published = {
        (mapping.source_vocabulary, mapping.source_state) for mapping in state_mappings()
    }
    vocabularies = {vocab for vocab, _ in published}
    for entry in entries():
        if entry.source_state is None:
            continue
        assert any(
            (vocab, entry.source_state) in published for vocab in vocabularies
        ), entry.contract_entry_id


# ---------------------------------------------------------------------------
# §6 review: frozen evidence, and content pins for handwritten prose
# ---------------------------------------------------------------------------

_CONTRACT_FIXTURES = (
    Path(__file__).resolve().parent / "fixtures" / "authoring_contract"
)


def test_the_whole_contract_is_frozen_in_a_committed_snapshot():
    """What makes ``parity_pinned`` TRUE for prose.

    ``_NODE_FACTS`` and ``_SEMANTIC_RULES`` are handwritten — deliberately, for
    facts with no runtime source to generate them from — and every entry they
    produce was labelled ``parity_pinned``. That label promises a CI test
    asserting the content against its source in both directions, and for the
    prose there was none: the two-way tests compared IDs and the presence of a
    source label, never a summary, an ordering fact, a document-semantics row or
    a reference requirement. A sentence could be rewritten to say the opposite
    and nothing would fail.

    A committed byte snapshot is the pin that actually holds for prose: it
    cannot verify the claim is TRUE, but it makes every change to a served
    behavioural statement land in a diff a reviewer must approve, which is the
    reviewable property the label was asserting.
    """
    committed = (_CONTRACT_FIXTURES / "process_ir_authoring_v1.contract.json").read_text()
    rendered = json.dumps(
        process_ir_authoring_revision_payload(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    assert rendered == committed, (
        "the served authoring contract changed. If the change is intended, "
        "regenerate tests/fixtures/authoring_contract/"
        "process_ir_authoring_v1.contract.json and review the diff — every line "
        "of it is text an LLM caller will read."
    )
    # ...and it is deterministic across rebuilds, not merely stable in one read.
    assert rendered == json.dumps(
        process_ir_authoring_revision_payload(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def test_the_page_schema_is_frozen_in_a_committed_snapshot():
    from boomi_mcp.models.process_ir_authoring import (
        process_ir_authoring_contract_v1_json_schema,
    )

    committed = (_CONTRACT_FIXTURES / "process_ir_authoring_v1.schema.json").read_text()
    rendered = json.dumps(
        process_ir_authoring_contract_v1_json_schema(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    assert rendered == committed


def test_the_handwritten_facts_that_ARE_derivable_are_pinned_to_their_source():
    """Not every handwritten fact is unpinnable — several have a real authority.

    Where one exists, the snapshot is not enough: the prose must agree with the
    code, not merely stay unchanged.
    """
    from boomi_mcp.models.process_ir import PROCESS_IR_V1_MAX_CONTROL_DEPTH

    by_id = {entry.contract_entry_id: entry for entry in entries()}

    # The depth bound is a real constant, and the contract states it as OURS.
    branch_entry = by_id["node.branch"]
    branch = " ".join(
        (branch_entry.summary + " " + " ".join(branch_entry.ordering_facts)).lower().split()
    )
    assert "two control levels" in branch
    assert PROCESS_IR_V1_MAX_CONTROL_DEPTH == 2, (
        "the served Branch prose says 'two control levels' — update both"
    )
    assert "not a boomi platform limit" in branch

    # "A nested Branch is not a legal Branch-path terminal" is derivable from
    # the allowlist: it holds precisely because `branch` is absent from the row.
    terminal = by_id["placement.branch_path.terminal"]
    assert "branch" not in terminal.node_kinds
    assert "nested branch is not a legal branch-path terminal" in branch

    # Flow control's single mode is a model constraint.
    flow_entry = by_id["node.flow_control"]
    flow = " ".join(
        (flow_entry.summary + " " + " ".join(flow_entry.ordering_facts)).lower().split()
    )
    assert "no caller-configurable parallel" in flow
    assert by_id["capability.flow_control_parallel_chunks"].source_state == "unsupported"

    # The retry rule's wording must match the shipped fail-closed rule.
    rule = " ".join(by_id["semantic_rule.retry.replay_safety"].summary.lower().split())
    rules = {row["replay_classification"]: row for row in retry_rule_specs()}
    for classification in ("non_idempotent", "unverified"):
        assert classification in rule
        assert rules[classification]["retry_permitted"] is False
    assert "verified_action" in rule
    assert rules["idempotent_write"]["required_evidence"] == "verified_action"
    assert "key_reference" in rule
    assert rules["conditionally_idempotent"]["required_evidence"] == "key_reference"


def test_the_doctrine_projection_carries_the_entry_s_own_metadata():
    """Two fields of six moved no revision at all.

    Publishing only name+status meant a pattern's verification status, its
    provenance or a cross-reference could change and the projection would not
    see it — so no revision moved, and a caller bound to one kept believing
    stale doctrine was current.
    """
    projected = {e.subject: e for e in entries() if e.entry_type == "doctrine"}
    assert projected

    checked_verification = checked_refs = 0
    for name, row in DESIGN_DOCTRINE_ENTRIES.items():
        entry = projected[name]
        assert entry.category == (row.get("category") or "doctrine"), name
        facts = " ".join(entry.ordering_facts)
        if row.get("verification_status"):
            assert row["verification_status"] in facts, name
            checked_verification += 1
        if row.get("cross_refs"):
            for ref in row["cross_refs"]:
                assert ref in facts, (name, ref)
            checked_refs += 1
    assert checked_verification and checked_refs


def test_a_doctrine_cross_reference_change_moves_the_contract():
    """The drift this narrowing hid."""
    sources = collect_projection_sources()
    rows = []
    for row in sources.doctrine_rows:
        row = dict(row)
        if row["name"] == sorted(DESIGN_DOCTRINE_ENTRIES)[0]:
            row["cross_refs"] = ["a_pattern_that_did_not_exist_before"]
        rows.append(row)
    drifted = build_process_ir_authoring_entries(sources._replace(doctrine_rows=tuple(rows)))
    blob = json.dumps([e.model_dump(mode="json") for e in drifted])
    assert "a_pattern_that_did_not_exist_before" in blob
    live = json.dumps([e.model_dump(mode="json") for e in entries()])
    assert "a_pattern_that_did_not_exist_before" not in live
