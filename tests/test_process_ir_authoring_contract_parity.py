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
    AUTHORING_LAYERS,
    AUTHORING_LAYER_SEMANTIC_VALIDATOR,
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

    # ...and the OTHER claims, which the lifetime check alone did not establish.
    # `lifetime` is the only descriptor field the traversal reads, so the rest
    # were asserted nowhere: the descriptor could have said DDP crosses sibling
    # paths, or that convergence is a union, and nothing would have failed.
    #
    # visible_across_sibling_paths — a document-scoped write does NOT reach a
    # sibling path, because each path gets its own copy of the stream.
    ddp_written = state.with_write(("ddp", "X")).entering_branch_leg()
    assert not ddp_written.establishes(("ddp", "X")) or STATE_VISIBILITY_V1["ddp"][
        "survives_branch_path_entry"
    ], "the descriptor and the traversal disagree about branch-path entry"

    # convergence — the meet is INTERSECTION, not union. State established on
    # one incoming path only must not survive the merge.
    left = lineage._State().with_write(("dpp", "ONLY_LEFT"))
    right = lineage._State().with_write(("dpp", "ONLY_RIGHT"))
    merged = left.merged_with(right)
    assert not merged.establishes(("dpp", "ONLY_LEFT"))
    assert not merged.establishes(("dpp", "ONLY_RIGHT"))
    for scope in STATE_VISIBILITY_V1:
        assert STATE_VISIBILITY_V1[scope]["convergence"] == "intersection", scope

    # read_before_write — the descriptor says rejected, and an unestablished
    # read really is unestablished.
    assert not lineage._State().establishes(("dpp", "NEVER_WRITTEN"))
    assert STATE_VISIBILITY_V1["dpp"]["read_before_write"] == "rejected"
    assert (
        STATE_VISIBILITY_V1["cache"]["read_before_write"]
        == "rejected_unless_external_writer_declared"
    )


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


def test_a_blank_diagnostic_field_from_any_producer_is_refused():
    """#177 QA-177-r2-01. The blank-field guard had NO test at all.

    Five surgical mutants against it all SURVIVED the affected node set — delete the guard,
    drop the `remediation` half, accept `""` and `"   "` by testing `is None`, or
    interpolate the whole authored row into the error — because nothing asserted it raises.
    The three PRODUCTION accessors already refuse a blank registry, so the guard's entire
    reachable domain is INJECTED snapshots, which is exactly the shape a "remove unused
    branch" cleanup deletes with nothing failing.

    Both halves of the promise are pinned here: it refuses every blank form from every
    producer, and the error names the CODE and nothing else.
    """
    import pytest as _pytest

    # The code is DERIVED from each table rather than typed, so a registry that gains or
    # loses rows cannot leave this test pinning a code its producer no longer serves.
    live = collect_projection_sources()
    producers = {
        field: sorted(spec["code"] for spec in getattr(live, field))[0]
        for field in ("compiler_specs", "parse_specs", "finding_specs")
    }
    # NOT required to be distinct. `_diagnostic_entries` explicitly supports one code
    # emitted by several producers, and the live registries already contain such codes, so
    # demanding three different sample codes would fail this test on a valid registry
    # change that has nothing to do with blank fields. Each source is perturbed
    # independently, which needs no uniqueness at all.
    assert len(producers) == 3, producers
    canary = "CANARY-AUTHORED-CONTENT-DO-NOT-SERVE"
    for source_field, code in producers.items():
        for blank in ("", "   ", None):
            for field in ("message", "remediation"):
                rows = tuple(
                    dict(spec, **{field: blank, "remediation": spec["remediation"] + canary})
                    if spec["code"] == code and field == "message"
                    else (dict(spec, **{field: blank}) if spec["code"] == code else dict(spec))
                    for spec in getattr(collect_projection_sources(), source_field)
                )
                with _pytest.raises(ValueError) as caught:
                    build_process_ir_authoring_entries(_perturbed(**{source_field: rows}))
                message = str(caught.value)
                assert code in message, (source_field, field, blank, message)
                # The error carries the CODE and no authored content.
                assert canary not in message, message


def test_the_blank_field_guard_names_no_authored_content():
    """A non-string row must still fail CLOSED, and with the honest named error.

    `.strip()` on a non-string raised `AttributeError` — fail-closed and correctly served as
    `unavailable`, but it tells the caller nothing about which code is at fault.
    """
    import pytest as _pytest

    code = "PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID"
    rows = tuple(
        dict(spec, message=object()) if spec["code"] == code else dict(spec)
        for spec in collect_projection_sources().compiler_specs
    )
    with _pytest.raises(ValueError) as caught:
        build_process_ir_authoring_entries(_perturbed(compiler_specs=rows))
    assert code in str(caught.value)


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
    # Derived, not re-typed: this assertion silently rotted once already when
    # the served label changed and the test kept pinning the old spelling.
    assert AUTHORING_LAYER_SEMANTIC_VALIDATOR in AUTHORING_LAYERS
    assert any(
        fact.startswith(f"[{AUTHORING_LAYER_SEMANTIC_VALIDATOR}]")
        for fact in entry.ordering_facts
    )
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
        # `category` is the served FILTER FACET and stays "doctrine" — the
        # pattern's own taxonomy is metadata, published as a fact.
        assert entry.category == "doctrine", name
        facts = " ".join(entry.ordering_facts)
        if row.get("category"):
            assert "Doctrine category: {0}.".format(row["category"]) in facts, name
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


def test_every_producer_s_message_AND_remediation_is_findable_in_its_entry():
    """A caller matches on the text they RECEIVED, not on the one we kept.

    Seven codes have producers whose wording differs. Attributing only the
    remediations fixed half of it: the compiler's MESSAGE was still dropped
    from an entry that names the compiler as a generated source, so a caller
    who received it could not find the rule.
    """
    from boomi_mcp.models.process_ir import process_ir_v1_parse_diagnostic_specs

    by_code = {}
    for specs in (
        process_ir_v1_parse_diagnostic_specs(),
        finding_specs(),
        compiler_diagnostic_specs(),
    ):
        for spec in specs:
            by_code.setdefault(spec["code"], []).append(spec)

    projected = {e.subject: e for e in entries() if e.entry_type == "diagnostic"}
    # #177: BOTH fields are required for every producer. The `if spec.get(field)`
    # skip this replaces meant an empty field was not a failure but an exemption,
    # so the seven codes that carried a remediation and no message satisfied this
    # pin while serving half a diagnostic.
    blank = sorted(
        (code, field)
        for code, specs in by_code.items()
        for spec in specs
        for field in ("message", "remediation")
        if not (spec.get(field) or "").strip()
    )
    assert blank == [], blank

    multi = set()
    covered = 0
    for code, specs in by_code.items():
        entry = projected[code]
        published = entry.summary + " " + " ".join(entry.ordering_facts)
        for spec in specs:
            for field in ("message", "remediation"):
                assert spec[field] in published, (code, field, spec[field][:60])
        covered += 1
        if len({(s["message"], s["remediation"]) for s in specs}) > 1:
            multi.add(code)

    # NON-VACUITY, replacing the hand-typed `multi >= 5` floor — a number that had
    # to be bumped whenever a producer's wording changed (the trap #165 closed).
    # Two properties, both derived, neither a count to maintain: the loop visited
    # EVERY merged code, and at least one code really is served with different text
    # by different producers — which is the only case in which "each producer's own
    # text is findable" says more than "the entry has some text".
    assert by_code, "no diagnostic specs at all — the loop would be vacuous"
    assert covered == len(by_code), (covered, len(by_code))
    assert multi, "no code differs across its producers — the pin would be vacuous"


def test_an_unmapped_cache_state_fails_loudly_instead_of_becoming_unsupported():
    """"unsupported" means NEVER. Defaulting to it invents a verdict.

    It would also let a served term cite a source state absent from the
    published `state_mappings`, so a caller could not look it up either.
    """
    from boomi_mcp.categories import meta_tools

    original = dict(meta_tools._CACHE_PROPERTY_AUTHORING_TERMS)
    patched = {k: dict(v) for k, v in original.items()}
    first = sorted(patched)[0]
    patched[first]["capability_status"] = "a_state_nobody_mapped"
    meta_tools._CACHE_PROPERTY_AUTHORING_TERMS = patched
    try:
        with pytest.raises(KeyError):
            meta_tools._canonical_term_states()
    finally:
        meta_tools._CACHE_PROPERTY_AUTHORING_TERMS = original

    # ...and every state the surface really emits IS mapped.
    for row in meta_tools._canonical_term_states():
        assert row["canonical_state"] in ("supported", "gated", "unsupported")
        assert any(
            m.source_vocabulary == "cache_property_authoring"
            and m.source_state == row["source_state"]
            for m in state_mappings()
        ), row


def test_every_authoring_layer_label_is_actually_served():
    """The constant tuple and the served labels agree in BOTH directions.

    Naming the labels once only helps if the name stays tied to reality: a
    constant nothing serves is as stale as a re-typed string.
    """
    # A code authored by more than one layer is labelled `[compiler/parser]`,
    # so the label is a SET of layers, not one — split it rather than treating
    # the joined form as a fourth layer.
    served = {
        layer
        for entry in entries()
        for fact in entry.ordering_facts
        if fact.startswith("[") and "]" in fact
        for layer in fact[1 : fact.index("]")].split("/")
    }
    assert set(AUTHORING_LAYERS) == served, (
        sorted(set(AUTHORING_LAYERS) ^ served),
    )


def test_every_published_page_rule_participates_in_the_revision():
    """#498. Moving the legend out of the entries moved it out of coverage.

    The byte snapshot and both revisions are computed from
    `process_ir_authoring_revision_payload()`. When the label legend lived
    inside each entry's summary it was covered by all three; published on the
    page envelope instead, it was covered by NONE — its text could be rewritten
    to assert the opposite and nothing moved.

    Page fields split cleanly in two: those describing the QUERY vary per call
    and must stay out, and those publishing a RULE are served behaviour and must
    be in. This asserts the split is total, so a new envelope field forces the
    decision instead of defaulting to uncovered.
    """
    from boomi_mcp.models.process_ir_authoring import (
        ProcessIRAuthoringContractPageV1,
    )

    from test_process_ir_authoring_contract import _QUERY_DEPENDENT_PAGE_FIELDS

    query_dependent = set(_QUERY_DEPENDENT_PAGE_FIELDS)
    # Two rules carry no key of their own because they are FUNCTIONS of the
    # entries the payload already carries whole: `facets` enumerates the
    # entries' filterable values and `catalog_entry_count` counts them. A key
    # for either would be a second copy of a fact already covered — and the
    # coverage is real, not asserted: the control below changes an entry and
    # both move with it.
    derived_from_entries = {"facets", "catalog_entry_count"}
    payload = process_ir_authoring_revision_payload()
    fields = set(ProcessIRAuthoringContractPageV1.model_fields)
    rules = fields - query_dependent - derived_from_entries

    assert query_dependent <= fields, sorted(query_dependent - fields)
    missing = sorted(rule for rule in rules if rule not in payload)
    assert missing == [], missing

    # ...and the derivation is DEMONSTRATED, not asserted: both are computed
    # from the payload's own entry list, so a change to any entry necessarily
    # moves them. A weaker version of this control compared a query to itself
    # and proved nothing.
    served = query_process_ir_authoring_contract(category="capability")
    covered = payload["entries"]
    assert served.catalog_entry_count == len(covered)

    facets = served.facets.model_dump(mode="json")
    assert facets["categories"] == sorted({row["category"] for row in covered})
    assert facets["entry_types"] == sorted({row["entry_type"] for row in covered})
    assert facets["node_kinds"] == sorted(
        {kind for row in covered for kind in row["node_kinds"]}
    )
    assert facets["capability_ids"] == sorted(
        {row["capability_id"] for row in covered if row["capability_id"]}
    )


def test_rewriting_the_label_legend_moves_the_revision():
    """The negative control for the pin above: the value, not just the key."""
    import json

    from boomi_mcp.authoring.process_ir_projection import DIAGNOSTIC_LABEL_LEGEND

    payload = process_ir_authoring_revision_payload()
    assert payload["diagnostic_label_legend"] == DIAGNOSTIC_LABEL_LEGEND

    inverted = dict(payload, diagnostic_label_legend="A compile never returns "
                    "parser-authored wording.")
    assert json.dumps(inverted, sort_keys=True) != json.dumps(payload, sort_keys=True)

    # ...and the committed snapshot carries it, so a rewrite lands in a diff.
    committed = json.loads(
        (_CONTRACT_FIXTURES / "process_ir_authoring_v1.contract.json").read_text()
    )
    assert committed["diagnostic_label_legend"] == DIAGNOSTIC_LABEL_LEGEND


#: §8's own table syntax, pinned. The parser below refuses the document unless
#: every one of these is present EXACTLY as written — which is the bidirectional
#: half: rewording the authority's table must FAIL the guard, never quietly
#: reduce it to a comparison against nothing. This repo has shipped a guard that
#: enumerated nothing and therefore passed everything five separate times.
_CAPABILITY_HEADING = "## 8. Capability states"
_CAPABILITY_NEXT_HEADING = "## 9. Ownership boundaries (#137\u2013#143)"
_CAPABILITY_TABLE_HEADER = "| Capability | State | Owner |"
_CAPABILITY_TABLE_DELIMITER = "|---|---|---|"

#: The document renders a state with markdown emphasis and spells the permanent
#: form long-hand; the runtime manifest carries neither. Normalisation is EXACT
#: and total: anything not a key here is a parse failure, not a skipped row, so a
#: new state cannot enter the doc without a deliberate change here.
_CAPABILITY_DOC_STATES = {
    "supported": "supported",
    "gated": "gated",
    "unsupported": "unsupported",
    "unsupported (permanent)": "unsupported",
}


def _assert_not_fenced(doc, offset, marker):
    """Refuse the marker AT `offset` if it sits inside a fenced code block.

    A comment is not the only way to stop Markdown rendering an authority: wrapping §8's
    heading and table in ``` leaves every raw line intact — and the parser returned all 27
    keys — while the rendered document contains neither a heading nor a table. Counted by
    fence parity, the same way the comment check counts delimiters.
    """
    # Markdown fence rules, not "starts with three backticks": a fence opens with three or
    # more `` ` `` or `~`, and CLOSES only on the same character with at least the opening
    # run length. Counting backtick-prefixed lines missed `~~~` entirely, and treated a
    # short ``` line inside a longer ```` fence as a close.
    open_char = None
    open_len = 0
    for line in doc[:offset].splitlines():
        stripped = line.lstrip(" ")
        indent = len(line) - len(stripped)
        # Python-Markdown treats a marker indented FOUR or more spaces as code, not a fence.
        if indent >= 4 or not stripped or stripped[0] not in "`~":
            continue
        char = stripped[0]
        run = len(stripped) - len(stripped.lstrip(char))
        if run < 3:
            continue
        remainder = stripped[run:].strip()
        if open_char is None:
            open_char, open_len = char, run
        elif char == open_char and run >= open_len and not remainder:
            # A CLOSING fence carries nothing after its run; a line with trailing text is an
            # info string, which opens rather than closes. Treating it as a close let §8 sit
            # inside rendered code with parity reporting balanced.
            open_char, open_len = None, 0
    if open_char is not None:
        raise AssertionError(
            "{0!r} is inside a fenced code block — the authority is not rendered".format(marker)
        )


def _assert_not_commented_out(doc, offset, marker):
    """Refuse the marker AT `offset` if it sits inside an HTML comment.

    Counted by delimiter parity: more `<!--` than `-->` before it means an open comment, so
    it renders as nothing at all.

    The offset is required rather than searched. Splitting on the first textual occurrence
    checked whatever came first in the document — a fenced example, say — so the REAL §8
    table could be commented out while an unrelated earlier line was inspected and the guard
    stayed green. The caller has already located the authority; this checks that one.
    """
    head = doc[:offset]
    if head.count("<!--") > head.count("-->"):
        raise AssertionError(
            "{0!r} is inside an HTML comment — the authority is not rendered".format(marker)
        )


def _parse_capability_states(doc):
    """§8's table as a `{key: runtime_state}` mapping, or a hard failure.

    #177 invariant 3. The predecessor of this parser compared KEYS only and
    skipped any line it could not match, so a state that drifted from the
    manifest was invisible and a restructured table degraded to an empty set
    guarded by a hand-typed `>= 25` floor. Both halves are gone: states are
    compared, and every structural expectation below raises rather than yields
    fewer rows.
    """
    import re

    # EXACT LINE, not a substring. `doc.count(heading)` was the first shape and it is
    # satisfied by `## 8. Capability states (normative)` — a reworded authority that left
    # the guard green, which is precisely what acceptance criterion 1 forbids.
    lines = doc.splitlines()
    # RAW line, not `.strip()`. Indent a heading by four spaces and Python-Markdown renders
    # it as a CODE BLOCK — §8 stops being a section at all — while a stripped comparison
    # still matches and this parser happily returns the same table. That is a structurally
    # removed authority leaving the guard green, which is the exact failure acceptance
    # criterion 1 names.
    # The heading must be LIVE Markdown, not commented out. Wrapping §8 in `<!-- -->`
    # removes the authority from the rendered document entirely while every raw-line check
    # still matched, so a structurally removed §8 left the guard green — the same failure
    # the raw-line fix was made to close, one level up.
    heads = [i for i, line in enumerate(lines) if line == _CAPABILITY_HEADING]
    if len(heads) != 1:
        raise AssertionError(
            "expected exactly one line equal to {0!r}, found {1}".format(
                _CAPABILITY_HEADING, len(heads)
            )
        )
    # The heading must be LIVE Markdown, checked at the position actually located.
    heading_offset = sum(len(line) + 1 for line in lines[: heads[0]])
    _assert_not_commented_out(doc, heading_offset, _CAPABILITY_HEADING)
    _assert_not_fenced(doc, heading_offset, _CAPABILITY_HEADING)
    after = "\n".join(lines[heads[0] + 1 :])
    section_offset = heading_offset + len(_CAPABILITY_HEADING) + 1

    # The section ENDS at the next level-2 heading, and that heading is pinned:
    # if §9 is renamed or §8 is moved, this fails instead of silently swallowing
    # the rest of the document into the section.
    boundary = re.search(r"^## .*$", after, re.M)
    if boundary is None or boundary.group(0) != _CAPABILITY_NEXT_HEADING:
        raise AssertionError(
            "expected the next level-2 heading to be {0!r}, found {1!r}".format(
                _CAPABILITY_NEXT_HEADING,
                None if boundary is None else boundary.group(0),
            )
        )
    section = after[: boundary.start()]

    if section.count(_CAPABILITY_TABLE_HEADER) != 1:
        raise AssertionError(
            "expected exactly one {0!r} row, found {1}".format(
                _CAPABILITY_TABLE_HEADER, section.count(_CAPABILITY_TABLE_HEADER)
            )
        )

    lines = section.splitlines()
    header_at = [i for i, line in enumerate(lines) if line == _CAPABILITY_TABLE_HEADER]
    if len(header_at) != 1:
        raise AssertionError(
            "expected exactly one {0!r} row, found {1}".format(
                _CAPABILITY_TABLE_HEADER, len(header_at)
            )
        )
    start = header_at[0]
    # The TABLE must be live Markdown too, not only the heading — and checked at ITS own
    # located position. Leaving §8's heading visible and wrapping just the table block in
    # `<!-- -->` removed the capability table from the rendered document while the heading
    # check passed and the raw-line parser returned all 27 rows.
    _assert_not_commented_out(
        doc,
        section_offset + sum(len(line) + 1 for line in lines[:start]),
        _CAPABILITY_TABLE_HEADER,
    )
    _assert_not_fenced(
        doc,
        section_offset + sum(len(line) + 1 for line in lines[:start]),
        _CAPABILITY_TABLE_HEADER,
    )
    if start + 1 >= len(lines) or lines[start + 1] != _CAPABILITY_TABLE_DELIMITER:
        raise AssertionError(
            "expected {0!r} immediately after the header, found {1!r}".format(
                _CAPABILITY_TABLE_DELIMITER,
                lines[start + 1] if start + 1 < len(lines) else None,
            )
        )
    # The table runs to the first BLANK line, and every non-blank line in it is a row —
    # the way Python-Markdown reads one. Selecting rows by `startswith("|")` was the first
    # shape, and Markdown does not require the outer pipes: a legal row written
    # ``key` | gated | #999` was invisible to it, so a doc-only capability could be added
    # while the 27/27 equality stayed green. Selecting by POSITION cannot miss a row.
    end = start + 2
    while end < len(lines) and lines[end].strip():
        end += 1
    rows = [line for line in lines[start + 2 : end]]
    if not rows:
        raise AssertionError("§8's table has a header but no rows")

    # A table row anywhere ELSE in the section is a structural surprise; a parser that
    # ignored it would be reading a different table than a human sees.
    stray = [
        line
        for index, line in enumerate(lines)
        if not (start <= index < end) and line.count("|") >= 2 and line.strip()
    ]
    if stray:
        raise AssertionError("§8 has table rows outside its one block: {0!r}".format(stray))

    parsed = {}
    for line in rows:
        body = line.strip()
        # Outer pipes are OPTIONAL in Markdown; normalise before splitting so a row is
        # read the same way whether or not the author wrote them.
        if body.startswith("|"):
            body = body[1:]
        if body.endswith("|"):
            body = body[:-1]
        cells = [""] + body.split("|") + [""]
        if len(cells) != 5:
            raise AssertionError("malformed §8 row: {0!r}".format(line))
        # Whitespace-tolerant: Markdown does not require a space after the pipe, and a row
        # written without outer pipes has none. Being strict here reported a doc-only
        # capability as a MALFORMED row, which sends the reader to fix the wrong thing —
        # the accurate diagnosis is the key-set equality below.
        key_match = re.match(r"^\s*`([a-z0-9_]+)`", cells[1])
        if key_match is None:
            raise AssertionError(
                "§8 row does not open with a backticked snake_case key: {0!r}".format(line)
            )
        key = key_match.group(1)
        if key in parsed:
            raise AssertionError("§8 lists {0!r} twice".format(key))
        if not cells[3].strip():
            raise AssertionError("§8 row {0!r} has a blank Owner".format(key))

        state = cells[2].strip()
        # Exactly ONE balanced outer emphasis span is removed. Not a general
        # markdown strip: `**gated` (unbalanced) must fail, because a half-written
        # cell is a defect in the authority, not something to normalise away.
        if state.startswith("**") and state.endswith("**") and len(state) > 4:
            state = state[2:-2].strip()
        if state not in _CAPABILITY_DOC_STATES:
            raise AssertionError(
                "§8 row {0!r} carries an unknown state {1!r}".format(key, cells[2])
            )
        parsed[key] = _CAPABILITY_DOC_STATES[state]

    return parsed


def test_the_published_capability_table_matches_the_registry_exactly():
    """#146 F4, extended by #177 to compare STATE as well as key.

    `PROCESS_IR_V1.md` §8 calls itself "the immutable
    `PROCESS_IR_V1_CAPABILITIES` manifest" and every slice (#140/#141/#142/#146)
    hand-appended to it; #146 added two rows and forgot. Nothing recorded the
    table as partial and no test compared it to the registry, so the drift was
    silent — while a two-way parity test for the PROJECTION had existed all
    along. This extends the same discipline to the document.

    #177: comparing keys left the STATE unpinned, so a row could advertise
    `supported` for a capability the manifest still gates — which is DC-175-D's
    mechanism exactly, a served description of a capability the enforcement does
    not grant. The mapping is now compared whole.
    """
    doc = (
        Path(__file__).resolve().parents[1]
        / "docs"
        / "architecture"
        / "PROCESS_IR_V1.md"
    ).read_text()

    from boomi_mcp.models.process_ir import PROCESS_IR_V1_CAPABILITIES

    published = _parse_capability_states(doc)

    assert published == dict(PROCESS_IR_V1_CAPABILITIES), {
        "in the doc only": sorted(set(published) - set(PROCESS_IR_V1_CAPABILITIES)),
        "in the registry only": sorted(set(PROCESS_IR_V1_CAPABILITIES) - set(published)),
        "state disagrees": sorted(
            (key, published[key], PROCESS_IR_V1_CAPABILITIES[key])
            for key in set(published) & set(PROCESS_IR_V1_CAPABILITIES)
            if published[key] != PROCESS_IR_V1_CAPABILITIES[key]
        ),
    }
    # NON-VACUITY is now STRUCTURAL, not a floor. The old test guarded an empty
    # parse with `len(published) >= 25`, a hand-typed count that had to be edited
    # on every capability addition — the trap #165 closed elsewhere. The parser
    # above raises on an empty or restructured table, so an empty mapping can
    # never reach the equality; the two controls below prove both directions.


def test_the_capability_table_guard_sees_a_state_that_drifts():
    """The real DC-175-D shape: a doc row advertising a state the runtime gates.

    Mutated IN MEMORY from the real document, so the control cannot go stale
    against a rewritten table — and mutating the state ALONE proves the state
    half is load-bearing, since the key set is untouched and the predecessor
    key-only guard passes this exact input.
    """
    doc = (
        Path(__file__).resolve().parents[1]
        / "docs"
        / "architecture"
        / "PROCESS_IR_V1.md"
    ).read_text()

    real = _parse_capability_states(doc)
    gated = sorted(key for key, state in real.items() if state == "gated")
    assert gated, "no gated row to mutate — the control would be vacuous"
    victim = gated[0]

    # Rewritten CELL-WISE, the same way the parser reads a row, so the mutation
    # cannot drift from what the parser considers a row.
    lines = doc.splitlines()
    count = 0
    for index, line in enumerate(lines):
        if not line.startswith("| `" + victim + "`"):
            continue
        cells = line.split("|")
        assert len(cells) == 5, line
        cells[2] = " **supported** "
        lines[index] = "|".join(cells)
        count += 1
    assert count == 1, (victim, count)
    mutated = "\n".join(lines) + "\n"

    drifted = _parse_capability_states(mutated)
    # The KEY SET is identical — which is exactly why the predecessor guard could
    # not see this defect.
    assert set(drifted) == set(real)
    assert drifted[victim] == "supported"
    assert drifted != real


def test_the_capability_table_guard_fails_closed_on_a_restructured_table():
    """A renamed authority must FAIL the guard, not quietly disarm it.

    Renaming the `State` column is the disarming move: a lenient parser matches
    zero rows and compares an empty set, which passes trivially. This asserts the
    parser raises BEFORE row extraction.
    """
    doc = (
        Path(__file__).resolve().parents[1]
        / "docs"
        / "architecture"
        / "PROCESS_IR_V1.md"
    ).read_text()

    renamed = doc.replace(
        _CAPABILITY_TABLE_HEADER, "| Capability | Status | Owner |", 1
    )
    assert renamed != doc

    with pytest.raises(AssertionError) as caught:
        _parse_capability_states(renamed)
    assert _CAPABILITY_TABLE_HEADER in str(caught.value)


def test_every_semantic_rule_names_the_authority_it_states_a_fact_about():
    """#146 F8. All seven hardcoded `runtime.process_ir_models`.

    That module contains the word "intersection" zero times, yet the
    convergence rule cited it — and swapping every rule to an unrelated source
    failed nothing but the byte snapshot, because the attribution lived at one
    construction site and no test compared it to anything.
    """
    from boomi_mcp.authoring.process_ir_projection import (
        _SEMANTIC_RULE_SOURCES,
        _SEMANTIC_RULES,
    )

    from boomi_mcp.authoring.process_ir_projection import _EFFECT_FAMILY_RULES

    # The effect rules are declared in `_SEMANTIC_RULES` for their PROSE but
    # take their source from the resolver's authority table, so they are
    # deliberately absent from this hand-kept map.
    declared = {rule[0] for rule in _SEMANTIC_RULES} - set(
        _EFFECT_FAMILY_RULES.values())
    assert set(_SEMANTIC_RULE_SOURCES) == declared, sorted(
        set(_SEMANTIC_RULE_SOURCES) ^ declared
    )

    # Scoped to the HAND-WRITTEN rules. #154 added generated effect-authority
    # entries which are also `semantic_rule` type but carry their own source,
    # derived from the resolver's authority table rather than from this map —
    # requiring them here would put the generated rows back into a hand-kept
    # list, which is the drift this whole entry family was moved away from.
    from boomi_mcp.authoring.process_ir_projection import _EFFECT_FAMILY_RULES

    served = {
        entry.contract_entry_id: tuple(sorted(s.source_id for s in entry.sources))
        for entry in entries()
        if entry.entry_type == "semantic_rule"
        and entry.contract_entry_id.startswith("semantic_rule.")
        and entry.contract_entry_id not in set(_EFFECT_FAMILY_RULES.values())
    }
    assert served == {
        rule_id: tuple(sorted(source_ids))
        for rule_id, source_ids in _SEMANTIC_RULE_SOURCES.items()
    }

    # The generated rules are covered in BOTH directions: one served entry per
    # row of the resolver's table, under the rule id that already served that
    # family — no parallel `effect_authority.*` namespace beside them.
    from boomi_mcp.authoring.process_ir_effects import effect_authority_rows

    assert not [
        entry for entry in entries()
        if entry.contract_entry_id.startswith("effect_authority.")
    ]
    generated = {
        _EFFECT_FAMILY_RULES[family] for family, _authority in effect_authority_rows()
    }
    served_ids = {entry.contract_entry_id for entry in entries()}
    assert generated <= served_ids, sorted(generated - served_ids)
    assert generated, "no generated authority rows — the check would be vacuous"
    # every family reaches exactly one rule, and no rule serves two families
    assert len(generated) == len(list(effect_authority_rows()))

    # ...and they are not all the same source again, which is the state this
    # finding describes.
    assert len({ids for ids in served.values()}) >= 4, served


# ---------------------------------------------------------------------------
# #154 (QA-154-r1-03/-04): placement prose is DERIVED, never hand-written
# ---------------------------------------------------------------------------


def _node_entries_by_id():
    from boomi_mcp.authoring.process_ir_projection import (
        build_process_ir_authoring_entries,
        reset_process_ir_authoring_cache,
    )

    reset_process_ir_authoring_cache()
    return {
        entry.contract_entry_id: entry
        for entry in build_process_ir_authoring_entries()
        if entry.contract_entry_id.startswith("node.")
    }


def test_every_node_entry_carries_a_derived_placement_fact():
    entries = _node_entries_by_id()
    assert entries, "no node entries — the test would be vacuous"
    for entry_id, entry in entries.items():
        placement = [f for f in entry.ordering_facts if f.startswith("Control-body placement:")]
        assert len(placement) == 1, (entry_id, entry.ordering_facts)


def test_the_derived_placement_fact_agrees_with_the_matrix():
    """BOTH directions, per node, against the enforcement table itself."""
    from boomi_mcp.compiler.process_ir import body_capabilities as bodycaps

    expected = {}
    for context, slot, kinds in bodycaps.body_placement_rows():
        for kind in kinds:
            expected.setdefault(kind, set()).add("{0} {1}".format(context, slot))

    for entry_id, entry in _node_entries_by_id().items():
        kind = entry_id[len("node."):]
        fact = next(f for f in entry.ordering_facts if f.startswith("Control-body placement:"))
        want = expected.get(kind, set())
        if not want:
            assert "none" in fact, (kind, fact)
            continue
        for slot in want:
            assert slot in fact, (kind, slot, fact)
        # ...and it claims nothing it does not hold
        for other in {s for slots in expected.values() for s in slots} - want:
            assert other not in fact, (kind, other, fact)


def test_the_two_sentences_this_slice_falsified_are_gone():
    """#154 widened the grammar and these stated the OLD rules.

    One of them contradicted its own machine-readable ``placements`` list in the
    same served object, and the contract rebase then froze both — so the suite was
    pinning served text against a golden asserting the pre-widening grammar.
    """
    blob = " ".join(
        " ".join(entry.ordering_facts) for entry in _node_entries_by_id().values()
    )
    assert "Admitted in the root sequence only" not in blob
    assert "A trailing cache_put belongs in a Branch path terminal" not in blob
    # the committed golden must not carry them either
    committed = (_CONTRACT_FIXTURES / "process_ir_authoring_v1.contract.json").read_text()
    assert "Admitted in the root sequence only" not in committed
    assert "A trailing cache_put belongs in a Branch path terminal" not in committed


def test_the_trailing_cache_put_sentence_derives_from_the_model_table():
    from boomi_mcp.models.process_ir import TRAILING_CACHE_PUT_TERMINALS

    fact = next(
        f for f in _node_entries_by_id()["node.cache_put"].ordering_facts
        if "tolerated only where" in f
    )
    tolerating = {c for c, t in TRAILING_CACHE_PUT_TERMINALS.items() if t}
    assert tolerating, "no context tolerates it — the assertion would be vacuous"
    for context in tolerating:
        for terminal in TRAILING_CACHE_PUT_TERMINALS[context]:
            assert terminal in fact, (context, terminal, fact)
    # a context that tolerates NOTHING must not be advertised as tolerating
    for context in set(TRAILING_CACHE_PUT_TERMINALS) - tolerating:
        assert context not in fact, (context, fact)


def test_an_unreviewed_placement_sentence_fails_the_build():
    """NON-VACUITY WITNESS for the review trip.

    Plants a sentence naming a placement slot on a node that has none allowlisted,
    PROVES the plant is present, then proves the projection refuses to build.
    """
    import pytest as _pytest

    from boomi_mcp.authoring import process_ir_projection as pr

    kind = "message"
    original = pr._NODE_FACTS[kind]
    planted = dict(original)
    planted[pr._ORDERING] = tuple(original.get(pr._ORDERING, ())) + (
        "Admitted in a Try/Catch body only.",
    )
    assert any("try/catch body" in f.lower() for f in planted[pr._ORDERING])

    patched = dict(pr._NODE_FACTS)
    patched[kind] = planted
    from types import MappingProxyType

    saved = pr._NODE_FACTS
    pr._NODE_FACTS = MappingProxyType(patched) if isinstance(saved, MappingProxyType) else patched
    try:
        pr.reset_process_ir_authoring_cache()
        with _pytest.raises(ValueError, match="UNREVIEWED ordering fact"):
            pr.build_process_ir_authoring_entries()
    finally:
        pr._NODE_FACTS = saved
        pr.reset_process_ir_authoring_cache()


def test_every_allowlisted_sentence_is_still_present_somewhere():
    """An allowlist entry for a sentence nobody serves is dead weight that hides
    the next real one."""
    from boomi_mcp.authoring import process_ir_projection as pr

    served = {
        f for facts in (v.get(pr._ORDERING, ()) for v in pr._NODE_FACTS.values()) for f in facts
    }
    for kind, sentences in pr._REVIEWED_PLACEMENT_PROSE.items():
        for sentence in sentences:
            assert sentence in served, (kind, sentence)


def test_the_cache_put_summary_does_not_contradict_its_ordering_facts():
    """#154 Codex P2: the entry said BOTH that every step-position cache_put
    needs an immediate read AND that a trailing one is allowed in two bodies.

    A served entry that contradicts itself is worse than either half alone — a
    caller cannot tell which sentence to believe, and the machine-readable
    ordering fact is the one that matches enforcement.
    """
    entry = _node_entries_by_id()["node.cache_put"]
    # the summary must scope its unconditional claim to MID-LIST
    assert "MID-LIST" in entry.summary
    # ...and must not restate the trailing rule it does not own
    assert "Branch path terminal" not in entry.summary
    trailing = [f for f in entry.ordering_facts if "LAST step" in f]
    assert len(trailing) == 1, entry.ordering_facts
    assert "MID-LIST" in trailing[0]
