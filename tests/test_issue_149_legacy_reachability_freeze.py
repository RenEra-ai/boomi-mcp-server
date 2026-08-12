"""Issue #149 (M12.12) — the freeze suite for the pre-deletion legacy
reachability inventory, allowlist baseline and served-surface retraction matrix.

This is the *instrument* #160 depends on, so its job is to fail when reachability
GROWS — a new legacy caller, a new legacy `process_kind` producer, a new
Component-XML write route, or a drifted served string — between now and the
deletion. It passes with every legacy path still present; zero-reachability
ENFORCEMENT belongs to #160.

Re-baselining after an INTENTIONAL change (the same doctrine as
`tests/test_issue_135_compatibility_freeze.py`): run

    PYTHONPATH=src .venv/bin/python tests/_m12_12_legacy_inventory.py \\
        --write tests/fixtures/m12_12/legacy_reachability_inventory.json

then update the matching section-11 rows in
`docs/architecture/M12_COMPATIBILITY_INVENTORY.md` in the SAME change. Both the
pin and the ledger move together or the two-way test fails.
"""

import sys
from pathlib import Path

import pytest

_TESTS = str(Path(__file__).resolve().parent)
if _TESTS not in sys.path:
    sys.path.insert(0, _TESTS)

import _m12_12_legacy_inventory as inv  # noqa: E402


@pytest.fixture(scope="module")
def baseline():
    return inv.load_baseline()


@pytest.fixture(scope="module")
def derived():
    return inv.build_inventory()


@pytest.fixture(scope="module")
def census_only():
    """The AST/producer census without the served collection — the cheap
    derivation the mutation overlays re-run repeatedly."""
    return inv.build_inventory(include_served=False)


# ======================================================================
# Baseline identity
# ======================================================================

def test_baseline_identity_and_schema_are_frozen(baseline):
    assert baseline["schema_version"] == inv.SCHEMA_VERSION
    meta = baseline["baseline"]
    assert meta["sha"] == "9711a9c0cb6c88dda41ada94d88694915b659f36"
    assert len(meta["sha"]) == 40
    assert meta["issue"] == "#149"
    assert meta["scanner_version"] == inv.SCANNER_VERSION

    row_ids = [r["row_id"] for r in baseline["census"]]
    assert len(row_ids) == len(set(row_ids)), "census row_ids must be unique"
    ledger_ids = [r["ledger_id"] for r in baseline["ledger_rows"]]
    assert len(ledger_ids) == len(set(ledger_ids)), "ledger_ids must be unique"
    route_ids = [r["route_id"] for r in baseline["component_xml_write_routes"]]
    assert len(route_ids) == len(set(route_ids)), "route_ids must be unique"

    for row in baseline["census"]:
        assert row["census"] in inv.CENSUS_KINDS
    for row in baseline["ledger_rows"]:
        assert row["owning_issue"] in inv.OWNING_ISSUES
        assert row["disposition"], "every ledger row needs a disposition"
    for route in baseline["component_xml_write_routes"]:
        assert route["classification"] in inv.ROUTE_CLASSIFICATIONS
        assert route["owning_issue"] in inv.OWNING_ISSUES
        assert route["post_retraction_assertion"]


#: A cell that says nothing. "unknown" is the criterion's own word; the rest are
#: the usual placeholders that mean the same thing.
_EMPTY_CELL_VALUES = {"", "unknown", "unspecified", "tbd", "todo", "n/a", "-", "?"}


def test_no_owner_or_disposition_cell_is_left_unfilled(baseline):
    """The acceptance criteria forbid a path marked "unknown".

    The check is on a cell's VALUE, not on a substring: a post-retraction
    assertion that legitimately reads "mutation-tested with an unknown/future
    declared type" is describing the guard's test matrix, not an unfilled cell.
    Substring matching also false-positives on served text, where real MCP tool
    descriptions use the word in their own prose. Closed-vocabulary cells (owner,
    classification) are separately pinned to their enumerations in
    `test_baseline_identity_and_schema_are_frozen`; this node covers the
    free-text cells, which no enumeration can police.
    """
    cells = []
    for row in baseline["ledger_rows"]:
        cells.append(("ledger %s disposition" % row["ledger_id"], row["disposition"]))
    for route in baseline["component_xml_write_routes"]:
        cells.append(("route %s summary" % route["route_id"], route["summary"]))
        cells.append(("route %s assertion" % route["route_id"],
                      route["post_retraction_assertion"]))
    for surface in baseline["served_surface_retraction_matrix"]:
        for field in ("surface_class", "producer", "legacy_guidance",
                      "post_retraction_assertion"):
            cells.append(("surface %s %s" % (surface["surface_id"], field),
                          surface[field]))

    offenders = [label for label, value in cells
                 if str(value).strip().lower().rstrip(".") in _EMPTY_CELL_VALUES]
    assert offenders == [], "unfilled inventory cells: %s" % offenders


# ======================================================================
# Non-vacuity — guard the guard
# ======================================================================

def test_the_scan_universe_is_complete_and_non_vacuous(derived):
    scanned = {r["path"] for r in derived["census"]}
    contract = derived["scan_contract"]

    sources = set(inv.python_sources())
    assert "server.py" in sources
    for required in (
        "src/boomi_mcp/categories/integration_builder.py",
        "src/boomi_mcp/categories/components/builders/process_flow_builder.py",
        "src/boomi_mcp/categories/components/builders/process_emitters/legacy.py",
        "src/boomi_mcp/models/_process_ir_compat.py",
        "src/boomi_mcp/compiler/process_ir/semantic_validation/legacy_bridge.py",
    ):
        assert required in sources, "%s left the scan universe" % required
    assert contract["python_source_count"] == len(sources)

    examples = set(inv.example_documents())
    assert examples, "the examples/ producer census must not be empty"
    assert contract["example_document_count"] == len(examples)

    # Every derived vocabulary family is populated: an empty family is a gate
    # that watches nothing and passes on everything.
    inv.assert_vocabulary_non_vacuous(
        {k: tuple(v) for k, v in contract["vocabulary"].items()})

    # And every census family the inventory claims to cover actually found rows.
    present = {r["census"] for r in derived["census"]}
    for required in ("registry_lookup", "renderer_call", "legacy_emitter",
                     "legacy_semantic_validation", "component_xml_write",
                     "raw_api_invoker", "process_kind_producer",
                     "process_kind_consumer", "example_producer",
                     "authoring_boundary"):
        assert required in present, "census family %r is empty" % required

    # The four registry-resolution sites issue #149 names are all present.
    resolvers = {
        r["symbol"] for r in derived["census"]
        if r["census"] == "registry_lookup"
        and r["path"] == "src/boomi_mcp/categories/integration_builder.py"
    }
    assert {"_resolve_preservation_policy", "build_structured_update_xml",
            "_execute_component", "_process_component_preflight"} <= resolvers


def test_the_vocabulary_is_derived_from_the_live_runtime(derived):
    """The watched names must come from the runtime authority, so a builder a
    later endgame issue registers is watched the day it registers."""
    from boomi_mcp.categories.components.builders.process_flow_builder import (
        PROCESS_FLOW_BUILDERS,
    )
    vocab = derived["scan_contract"]["vocabulary"]
    registered = {cls.__name__ for cls in PROCESS_FLOW_BUILDERS.values()}
    assert registered <= set(vocab["builder_classes"])
    assert len(vocab["legacy_emitters"]) >= 20
    assert "validate_legacy_process_config" in vocab["legacy_semantic_validation"]
    assert "_create_component_raw" in vocab["component_xml_write_sinks"]
    assert "_update_component_xml" in vocab["component_xml_write_sinks"]
    # A read helper must NOT join the write census.
    assert "parse_component_xml" not in vocab["component_xml_write_sinks"]


# ======================================================================
# The freeze itself
# ======================================================================

def test_legacy_callers_and_process_kind_producers_match_the_baseline(derived, baseline):
    diff = inv.compare(derived, baseline)
    assert diff.empty(), (
        "the legacy-reachability census drifted from the frozen #149 baseline.\n\n%s\n\n"
        "If this change is INTENTIONAL, rebaseline with\n"
        "  PYTHONPATH=src .venv/bin/python tests/_m12_12_legacy_inventory.py "
        "--write %s\n"
        "and update the matching section-11 rows in %s in the same change."
        % (diff.report(), inv.FIXTURE_RELPATH, inv.INVENTORY_DOC))


def test_the_derivation_is_deterministic():
    first = inv.build_inventory()
    second = inv.build_inventory()
    assert inv.dumps(first) == inv.dumps(second)


# ======================================================================
# Mutation checks — the gate must FAIL on a new legacy caller
# ======================================================================

def _overlay(name: str, source: str):
    sources = dict(inv.python_sources())
    sources["src/boomi_mcp/categories/%s.py" % name] = source
    return sources


def _added(census_only, name: str, source: str):
    current = inv.build_inventory(sources=_overlay(name, source), include_served=False)
    return inv.compare(current, census_only)


def test_a_synthetic_legacy_caller_breaks_the_freeze(census_only):
    diff = _added(census_only, "_m12_12_synthetic_offender", (
        "from .components.builders import get_process_flow_builder as gpfb\n"
        "def render(config):\n"
        "    kind = config['process_kind']\n"
        "    return gpfb(kind).build(config, name='x')\n"
    ))
    assert not diff.empty(), "a synthetic legacy caller did not break the freeze"
    kinds = {row.split(" | ")[0] for row in diff.added}
    assert {"registry_lookup", "renderer_call", "process_kind_consumer"} <= kinds, (
        "the synthetic caller must be reported as a registry lookup, a renderer "
        "call AND a process_kind access; got %s" % sorted(kinds))
    assert diff.removed == []


def test_a_renderer_invoked_straight_off_a_registry_subscript_is_reported(census_only):
    """`PROCESS_FLOW_BUILDERS[kind].build(...)` binds no intermediate variable.

    An earlier draft of the scanner missed exactly this shape — the attribute
    chain roots in a Subscript, so the dotted-name resolver returned None and the
    renderer call fell through every branch. A fail-open in the gate, caught only
    because the mutation overlay used it.
    """
    diff = _added(census_only, "_m12_12_synthetic_subscript", (
        "from .components.builders import PROCESS_FLOW_BUILDERS as R\n"
        "def render(config):\n"
        "    return R[config['process_kind']].build(config)\n"
    ))
    kinds = {row.split(" | ")[0] for row in diff.added}
    assert {"registry_lookup", "renderer_call"} <= kinds, sorted(kinds)


def test_a_new_legacy_emitter_or_write_sink_caller_breaks_the_freeze(census_only):
    emitter = _added(census_only, "_m12_12_synthetic_emitter", (
        "from .components.builders.process_emitters.legacy import _emit_message\n"
        "def f(**kw):\n"
        "    return _emit_message(**kw)\n"
    ))
    assert "legacy_emitter" in {row.split(" | ")[0] for row in emitter.added}

    writer = _added(census_only, "_m12_12_synthetic_writer", (
        "from .components._shared import _create_component_raw\n"
        "def f(client, xml):\n"
        "    return _create_component_raw(client, xml)\n"
    ))
    assert "component_xml_write" in {row.split(" | ")[0] for row in writer.added}


def test_an_unresolvable_dynamic_sink_access_is_reported_not_ignored(census_only):
    """Fail-closed residue: a watched name reached through `getattr` cannot be
    statically resolved, so it is RECORDED as unclassified rather than dropped."""
    diff = _added(census_only, "_m12_12_synthetic_dynamic", (
        "from .components import builders as mod\n"
        "def f(kind):\n"
        "    return getattr(mod, 'get_process_flow_builder')(kind)\n"
    ))
    assert "unclassified_dynamic" in {row.split(" | ")[0] for row in diff.added}


def test_an_unrelated_addition_does_not_break_the_freeze(census_only):
    """Negative control: the gate must report reachability, not merely 'a file
    changed'. Without this, every assertion above would pass on a scanner that
    diffed file names."""
    diff = _added(census_only, "_m12_12_synthetic_harmless",
                  "def harmless(x):\n    return x + 1\n")
    assert diff.added == [], diff.added
    assert diff.removed == []
    assert diff.count_changes == []
    # The scan universe legitimately grew by one file, and nothing else moved.
    assert diff.scalar_changes == [
        "scan_contract.python_source_count: %d -> %d"
        % (census_only["scan_contract"]["python_source_count"],
           census_only["scan_contract"]["python_source_count"] + 1)]


def test_a_second_call_in_an_existing_function_is_reported_as_a_count_change(census_only):
    """Symbolic keys deliberately ignore line numbers, so the COUNT is what
    catches a duplicated call inside a function that already has one."""
    sources = dict(inv.python_sources())
    path = "src/boomi_mcp/categories/_m12_12_synthetic_counted.py"
    sources[path] = ("from .components.builders import get_process_flow_builder\n"
                     "def render(kind):\n"
                     "    return get_process_flow_builder(kind)\n")
    once = inv.build_inventory(sources=sources, include_served=False)

    sources[path] = ("from .components.builders import get_process_flow_builder\n"
                     "def render(kind):\n"
                     "    get_process_flow_builder(kind)\n"
                     "    return get_process_flow_builder(kind)\n")
    twice = inv.build_inventory(sources=sources, include_served=False)

    diff = inv.compare(twice, once)
    assert diff.added == []
    assert diff.count_changes, "a duplicated call site must be reported"
    assert "1 -> 2" in diff.count_changes[0]


def test_an_inserted_blank_line_does_not_break_the_freeze(census_only):
    """The complement of the count test: the freeze must not be so brittle that
    unrelated edits break it. Line numbers ride along as evidence only."""
    sources = dict(inv.python_sources())
    target = "src/boomi_mcp/categories/integration_builder.py"
    sources[target] = "\n\n" + sources[target]
    shifted = inv.build_inventory(sources=sources, include_served=False)
    assert inv.compare(shifted, census_only).empty()


# ======================================================================
# Component-XML write routes
# ======================================================================

def test_every_component_xml_sink_is_classified_and_no_route_is_stale(derived, baseline):
    reconciliation = derived["route_reconciliation"]
    assert reconciliation["unclassified"] == [], (
        "a Component-XML write sink no route claims — #160 would have to "
        "rediscover it: %s" % reconciliation["unclassified"])
    assert reconciliation["stale_claims"] == [], (
        "a route cites a call site the scanner cannot find — the checklist #160 "
        "executes is out of date: %s" % reconciliation["stale_claims"])
    # Locations that legitimately host several routes are pinned, so a NEW
    # sharing is a diff rather than a silent reclassification.
    assert reconciliation["shared_locations"] == \
        baseline["route_reconciliation"]["shared_locations"]

    classifications = {r["classification"]
                       for r in derived["component_xml_write_routes"]}
    assert classifications <= set(inv.ROUTE_CLASSIFICATIONS)
    for required in ("raw_process_capable", "platform_sourced_rematerialization",
                     "legacy_structured_process", "preserve", "dormant"):
        assert required in classifications, "no route classified %r" % required


def test_the_dormant_shared_writer_has_no_production_callers(derived):
    """`_update_component_xml` is inventoried precisely BECAUSE it is dormant, so
    a future caller cannot revive an unguarded raw write route.

    Dormancy is PRODUCTION-scoped: the two deliberate callers in
    `tests/test_component_raw_transport.py` are evidence the transport works.
    """
    callers = inv.dormant_writer_callers(derived["census"])
    assert callers == [], (
        "the dormant raw-XML writer gained a production caller: %s. It must sit "
        "behind #160's two-sided guard before any caller uses it." % callers)

    dormant = [r for r in derived["component_xml_write_routes"]
               if r["classification"] == "dormant"]
    assert len(dormant) == 1
    assert "_update_component_xml" in dormant[0]["locations"][0]


# ======================================================================
# Served artifacts
# ======================================================================

def test_served_artifacts_match_the_committed_values(derived, baseline):
    current = {a["artifact_id"]: a for a in derived["served_artifacts"]}
    frozen = {a["artifact_id"]: a for a in baseline["served_artifacts"]}
    assert sorted(current) == sorted(frozen)
    changed = [aid for aid in sorted(current)
               if current[aid]["sha256"] != frozen[aid]["sha256"]]
    assert changed == [], (
        "served surface(s) drifted: %s. A name-based grep would have missed this "
        "— obsolete guidance survives under names that are never deleted." % changed)
    for aid, artifact in current.items():
        if not artifact["value_omitted"]:
            assert artifact["value"] == frozen[aid]["value"]


def test_large_served_artifacts_keep_identity_and_a_legacy_excerpt(baseline):
    """The size rule is applied consistently, and omission never costs drift
    detection: `sha256` always covers the whole canonical value."""
    for artifact in baseline["served_artifacts"]:
        assert artifact["sha256"]
        if artifact["value_omitted"]:
            assert artifact["canonical_length"] > inv._INLINE_VALUE_LIMIT
            assert "value" not in artifact
            assert artifact["legacy_excerpt"], (
                "%s was omitted but carries no legacy excerpt — it would not "
                "have been collected at all" % artifact["artifact_id"])
        else:
            assert artifact["canonical_length"] <= inv._INLINE_VALUE_LIMIT
            assert "value" in artifact


def test_the_served_collection_cannot_touch_boomi_transport(monkeypatch):
    """Every served producer must return without reaching the platform.

    Patched through the SAME bare-`boomi_mcp` import the collector uses — the
    `src.`-prefixed spelling is a second module object and would patch nothing.
    """
    from boomi.services.component import ComponentService
    from boomi_mcp.categories.components import _shared

    def bomb(*args, **kwargs):
        raise AssertionError("the #149 served collection reached the Boomi transport")

    for name in ("send_request", "send_request_raw", "create_component",
                 "create_component_raw", "update_component", "update_component_raw",
                 "get_component", "get_component_raw", "bulk_component"):
        monkeypatch.setattr(ComponentService, name, bomb, raising=False)
    monkeypatch.setattr(_shared, "_create_component_raw", bomb, raising=False)
    monkeypatch.setattr(_shared, "_update_component_xml", bomb, raising=False)

    artifacts = inv.collect_served_artifacts()
    assert artifacts, "the served collection produced nothing under the transport bomb"
    assert len(artifacts) == len(inv.load_baseline()["served_artifacts"])


def test_every_served_artifact_belongs_to_exactly_one_matrix_class(derived):
    matrix = {r["surface_id"] for r in derived["served_surface_retraction_matrix"]}
    assert matrix == set(inv.SURFACE_CLASSES)

    owned = {}
    for artifact in derived["served_artifacts"]:
        owned.setdefault(artifact["surface_class"], []).append(artifact["artifact_id"])
        assert artifact["surface_class"] in matrix, (
            "served artifact %s has no retraction-matrix row — #160 could not "
            "sweep it" % artifact["artifact_id"])

    for surface_id in inv.SURFACE_CLASSES:
        assert owned.get(surface_id), (
            "retraction-matrix row %s owns no frozen artifact, so nothing pins "
            "its text" % surface_id)


def test_the_retraction_matrix_is_executable_from_the_matrix_alone(derived):
    """Acceptance criterion: #160 can execute the sweep from the matrix alone."""
    for row in derived["served_surface_retraction_matrix"]:
        assert row["anchors"], "%s names no HEAD source anchor" % row["surface_id"]
        for anchor in row["anchors"]:
            path = anchor.split(":")[0].split(" ")[0]
            assert (inv.repo_root() / path).is_file(), (
                "retraction-matrix anchor %r does not resolve at HEAD" % anchor)
        assert row["producer"]
        assert row["legacy_guidance"]
        assert row["post_retraction_assertion"]

    # The acceptance criteria name the schema-description class explicitly.
    pydantic = [r for r in derived["served_surface_retraction_matrix"]
                if r["surface_id"] == "SS-PYDANTIC"]
    assert pydantic, "the schema-description surface class is missing"
    assert any("integration_models.py" in a for a in pydantic[0]["anchors"])


# ======================================================================
# The human ledger and the machine record agree
# ======================================================================

def test_the_ledger_and_the_json_are_two_way_complete(baseline):
    documented = inv.parse_section_11_ids()

    expected_ledger = {r["ledger_id"] for r in baseline["ledger_rows"]}
    actual_ledger = set(documented["ledger_ids"])
    assert len(documented["ledger_ids"]) == len(actual_ledger), "duplicate ledger IDs"
    assert actual_ledger == expected_ledger, (
        "section 11 and the frozen baseline disagree.\nmissing from the doc: %s\n"
        "not in the baseline: %s"
        % (sorted(expected_ledger - actual_ledger)[:10],
           sorted(actual_ledger - expected_ledger)[:10]))

    expected_routes = {r["route_id"] for r in baseline["component_xml_write_routes"]}
    assert set(documented["route_ids"]) == expected_routes

    expected_surfaces = {r["surface_id"]
                         for r in baseline["served_surface_retraction_matrix"]}
    assert set(documented["surface_ids"]) == expected_surfaces


def test_the_ledger_sections_partition_every_census_kind():
    """A census family frozen in the JSON but tabulated nowhere would be
    invisible to #160 — and `test_the_ledger_and_the_json_are_two_way_complete`
    would fail obscurely rather than saying which family escaped."""
    sections = inv.ledger_section_kinds()
    covered = [kind for kinds in sections.values() for kind in kinds]
    assert sorted(covered) == sorted(set(covered)), \
        "a census kind appears in two ledger sections: %s" % covered
    assert set(covered) == set(inv.CENSUS_KINDS), (
        "ledger sections do not partition the census kinds.\nnot tabulated: %s\n"
        "tabulated but not a census kind: %s"
        % (sorted(set(inv.CENSUS_KINDS) - set(covered)),
           sorted(set(covered) - set(inv.CENSUS_KINDS))))


def test_the_markdown_tables_are_regenerable_from_the_json(baseline):
    """Nobody can hand-edit the ledger into disagreement with the machine record:
    every §11 table is emitted from the fixture and must appear verbatim."""
    body = inv.section_11_text()
    for subsection, table in sorted(inv.emit_section_11_markdown(baseline).items()):
        assert table in body, (
            "the §%s table in %s is not the one generated from %s. Regenerate it "
            "with `tests/_m12_12_legacy_inventory.py --emit-markdown` and paste it "
            "verbatim." % (subsection, inv.INVENTORY_DOC, inv.FIXTURE_RELPATH))
