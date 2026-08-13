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


def _transport_bomb():
    """Replace every Boomi TRANSPORT entry point with a raising sentinel.

    Scoped to the SDK's `ComponentService`, deliberately. The repo-side helpers
    `_shared._create_component_raw` / `_update_component_xml` are NOT patched:
    every path through them reaches `ComponentService.create_component` /
    `update_component`, which are bombed here, so patching them adds no coverage
    — and it actively breaks the derivation. `legacy_sink_vocabulary()` reflects
    `_shared` and keeps only members whose `__module__` is `_shared`; a sentinel
    defined in this test module fails that test, silently dropping both writers
    from the watched vocabulary and taking three real call sites out of the
    census with them.
    """
    from boomi.services.component import ComponentService

    def bomb(*args, **kwargs):
        raise AssertionError("the #149 derivation reached the Boomi transport")

    # DERIVED from the same vocabulary the census watches, plus the read verbs.
    # A hand-listed sentinel drifts from the watched set the moment the set
    # grows: `stream_request` joined the vocabulary and the sentinel did not,
    # leaving a transport verb the "transport-free" test could not have caught.
    watched = set(inv.legacy_sink_vocabulary()["component_xml_write_sinks"])
    reads = {"get_component", "get_component_raw", "query_component_metadata"}
    patches = [
        (ComponentService, name, getattr(ComponentService, name))
        for name in sorted(watched | reads)
        if hasattr(ComponentService, name)
    ]
    assert any(n == "stream_request" for _, n, _ in patches) or \
        not hasattr(ComponentService, "stream_request"), \
        "stream_request exists but is not sentinelled"
    for owner, name, _ in patches:
        setattr(owner, name, bomb)
    return patches


def _disarm(patches):
    for owner, name, original in patches:
        setattr(owner, name, original)


@pytest.fixture(scope="module", autouse=True)
def transport_guard():
    """Arm the transport sentinel for the WHOLE module, not one test node.

    The `derived` fixture and the determinism test each run a full served
    collection. With the guard installed only inside
    `test_the_served_collection_cannot_touch_boomi_transport`, those collections
    ran unprotected and a producer that regressed into transport would have made
    a real call before any test noticed. Module-scoped and autouse, so every
    derivation in this file is covered — the read-only claim is enforced
    continuously rather than sampled once.
    """
    patches = _transport_bomb()
    try:
        yield
    finally:
        _disarm(patches)


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
        # A row at a location several routes claim inherits every claiming
        # route's owner, joined with `/` — e.g. `#153/#160`.
        for issue in str(row["owning_issue"]).split("/"):
            assert issue in inv.OWNING_ISSUES, (row["ledger_id"], issue)
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


def test_a_renderer_reached_through_a_module_qualified_registry_is_reported(census_only):
    """`builders.PROCESS_FLOW_BUILDERS['sync_pipeline'].build(config)`.

    The subscript roots in an `ast.Attribute` rather than an `ast.Name`, and with
    a constant registry key there is no `process_kind` access either — so an
    earlier draft produced NO census row at all for a complete legacy renderer
    path. Both spellings of the registry must resolve.
    """
    diff = _added(census_only, "_m12_12_synthetic_qualified", (
        "from .components import builders\n"
        "def render(config):\n"
        "    return builders.PROCESS_FLOW_BUILDERS['sync_pipeline'].build(config)\n"
    ))
    kinds = {row.split(" | ")[0] for row in diff.added}
    assert {"registry_lookup", "renderer_call"} <= kinds, sorted(kinds)


@pytest.mark.parametrize("body", [
    # a qualified `.get` lookup
    "from .components import builders\n"
    "def render(c):\n"
    "    return builders.PROCESS_FLOW_BUILDERS.get(c['process_kind']).build(c)\n",
    # a qualified subscript bound to a variable first
    "from .components import builders\n"
    "def render(c):\n"
    "    cls = builders.PROCESS_FLOW_BUILDERS[c['process_kind']]\n"
    "    return cls.build(c)\n",
    # a qualified READ-ONLY reference — no call at all
    "from .components import builders\n"
    "def hint():\n"
    "    return sorted(builders.PROCESS_FLOW_BUILDERS)\n",
])
def test_every_module_qualified_registry_spelling_is_reported(census_only, body):
    """Recognition routes through one predicate, so no spelling escapes.

    Three separate shapes had leaked past shape-specific checks — a qualified
    `.get`, a qualified subscript bound to a variable, and a bare qualified read.
    The read is the one that mattered most: it produced NO row of any kind, so a
    file consulting the legacy registry was indistinguishable from a harmless
    one. Production carries no qualified reference today; this is the gate
    closing before the case arrives, not a repair of a live undercount.
    """
    diff = _added(census_only, "_m12_12_synthetic_qualified_variants", body)
    assert "registry_lookup" in {row.split(" | ")[0] for row in diff.added}, diff.report()


def test_a_builder_method_reached_through_getattr_is_reported(census_only):
    """`getattr(SyncPipelineBuilder, 'build')(config)` and its dynamic sibling.

    Both invoke the legacy renderer without a registry lookup or a `process_kind`
    read. The constant form escaped because the watched set excluded the builder
    METHODS; the dynamic form escaped because the branch was a literal no-op.
    """
    constant = _added(census_only, "_m12_12_synthetic_getattr_const", (
        "from .components.builders import SyncPipelineBuilder as B\n"
        "def render(config):\n"
        "    return getattr(B, 'build')(config)\n"
    ))
    assert "unclassified_dynamic" in {r.split(" | ")[0] for r in constant.added}

    dynamic = _added(census_only, "_m12_12_synthetic_getattr_dyn", (
        "from .components.builders import SyncPipelineBuilder as B\n"
        "def render(config, name):\n"
        "    return getattr(B, name)(config)\n"
    ))
    assert "unclassified_dynamic" in {r.split(" | ")[0] for r in dynamic.added}


def test_a_generic_method_name_on_an_unrelated_target_is_not_reported(census_only):
    """`build` is a generic name and says nothing on its own.

    Precision matters as much as recall for a FROZEN census: reporting
    `getattr(plugin, "build")` in unrelated code would fail the gate for an edit
    with no legacy reachability at all, and a gate that cries wolf gets
    re-baselined without reading. Distinctive names (`get_process_flow_builder`,
    `_create_component_raw`) still match on the name alone.
    """
    unrelated = _added(census_only, "_m12_12_synthetic_generic_name", (
        "def f(plugin):\n"
        "    return getattr(plugin, 'build')()\n"
    ))
    assert unrelated.added == [], unrelated.added

    distinctive = _added(census_only, "_m12_12_synthetic_distinctive_name", (
        "def f(mod):\n"
        "    return getattr(mod, 'get_process_flow_builder')('sync_pipeline')\n"
    ))
    assert "unclassified_dynamic" in {r.split(" | ")[0] for r in distinctive.added}


@pytest.mark.parametrize("label,body", [
    ("aliased wrapper around a legacy-bearing function",
     "from .integration_builder import build_structured_update_xml as bsux\n"
     "def wrap(comp, xml):\n"
     "    return bsux(comp, xml)\n"),
    ("wrapper around the public build entry point",
     "from .integration_builder import build_integration_action\n"
     "def wrap(client, profile, action, config):\n"
     "    return build_integration_action(client, profile, action, config)\n"),
    ("a wrapper AROUND the wrapper (second hop)",
     "from .integration_builder import build_structured_update_xml\n"
     "def inner(comp, xml):\n"
     "    return build_structured_update_xml(comp, xml)\n"
     "def outer(comp, xml):\n"
     "    return inner(comp, xml)\n"),
])
def test_a_wrapper_around_a_legacy_path_is_reported(census_only, label, body):
    """The leaf census alone is fail-open.

    A function that merely CALLS `build_structured_update_xml` reaches the legacy
    renderer but names no watched sink itself, so it produced no row and the
    freeze stayed green — a new entry point onto the legacy path, invisible. The
    census is now closed to a fixed point over legacy-bearing module-level
    functions, which is why the second-hop case is parametrised here too: a
    one-hop rule would let `outer` hide behind `inner`.
    """
    diff = _added(census_only, "_m12_12_synthetic_wrapper", body)
    assert "legacy_transitive_call" in {r.split(" | ")[0] for r in diff.added}, \
        "%s escaped the freeze: %s" % (label, diff.report())


def test_a_wrapper_importing_through_a_barrel_reexport_is_reported(census_only):
    """A package `__init__` re-export makes the import site differ from the
    defining site.

    `get_process_flow_builder` is DEFINED in `process_flow_builder.py` and
    imported by every caller from `builders/__init__.py`. Keying the closure on
    the import site lost the edge outright — measured as two production callers
    (`_resolve_preservation_policy`, `build_structured_update_xml`) silently
    dropping it. Callees are canonicalized through a re-export index.
    """
    diff = _added(census_only, "_m12_12_synthetic_barrel", (
        "from .components.builders import get_process_flow_builder\n"
        "def wrap(kind):\n"
        "    return get_process_flow_builder(kind)\n"
    ))
    kinds = {r.split(" | ")[0] for r in diff.added}
    assert "legacy_transitive_call" in kinds, diff.report()


def test_the_barrel_reexport_edges_exist_at_head(baseline):
    """Guard the guard: the two production sites the regression removed."""
    edges = {
        (r["path"], r["symbol"]) for r in baseline["census"]
        if r["census"] == "legacy_transitive_call"
        and "get_process_flow_builder" in r["form"]
    }
    for symbol in ("_resolve_preservation_policy", "build_structured_update_xml"):
        assert ("src/boomi_mcp/categories/integration_builder.py", symbol) in edges, symbol


def test_every_repo_root_module_and_script_is_scanned():
    """`server.py` was the only root module scanned, so a legacy caller in
    `server_http.py` — a production entry point — was invisible, and
    `python_source_count` could not move for an edit to it."""
    scanned = set(inv.python_sources())
    for required in ("server.py", "server_http.py"):
        assert required in scanned, required

    # Scoped to what GIT considers part of the repository. A filesystem walk
    # swept in gitignored working-copy files, and the baseline that pinned one
    # failed on a clean checkout.
    #
    # `None` means git is unavailable — a source export with no `.git`, which is
    # exactly where a clean-checkout reproduction runs. The fallback to the plain
    # walk is CORRECT there (an ignored file is not in the export either), so
    # requiring git here would fail the very environment the fix exists to
    # support.
    visible = inv._repository_files()
    universe = visible if visible is not None else scanned

    # EVERY visible `.py` under EVERY declared root, `src/boomi_mcp/**` included.
    # Covering only the repo root and `scripts/` meant a file dropped from
    # `src/boomi_mcp/**` left this node green — and the `python_source_count`
    # arm is blind to a file that never enters the scan at all, which is exactly
    # how a C-quoted path went missing.
    for root_glob in ("*.py",):
        expected = {p.name for p in inv.repo_root().glob(root_glob)} & universe
        assert expected <= scanned, sorted(expected - scanned)
    for root in ("src/boomi_mcp", "scripts"):
        base = inv.repo_root() / root
        if not base.is_dir():
            continue
        expected = {p.relative_to(inv.repo_root()).as_posix()
                    for p in base.rglob("*.py")} & universe
        assert expected, "no repository-visible module under %s was scanned" % root
        assert expected <= scanned, sorted(expected - scanned)

    roots = {p.name for p in inv.repo_root().glob("*.py")} & universe
    assert roots <= scanned, sorted(roots - scanned)
    scripts = {p.relative_to(inv.repo_root()).as_posix()
               for p in (inv.repo_root() / "scripts").rglob("*.py")} & universe
    assert scripts, "no repository-visible script was scanned"
    assert scripts <= scanned, sorted(scripts - scanned)
    if visible is not None:
        assert not (scanned - visible), \
            "the scan includes files git does not: %s" % sorted(scanned - visible)[:5]


@pytest.mark.parametrize("body", [
    # module-level client, plain assignment
    "import httpx\n"
    "_C = httpx.Client()\n"
    "def push(xml):\n"
    "    return _C.post('https://api.boomi.com/Component', content=xml)\n",
    # context-manager binding — how this repo actually opens clients
    "import httpx\n"
    "def push(xml):\n"
    "    with httpx.Client() as client:\n"
    "        return client.post('https://api.boomi.com/Component', content=xml)\n",
    # stdlib
    "from urllib import request\n"
    "def push(xml):\n"
    "    return request.urlopen('https://api.boomi.com/Component', data=xml)\n",
])
def test_a_hand_rolled_http_client_is_reported(census_only, body):
    """The sink vocabulary derives from the Boomi SDK, so a POST to /Component
    through `httpx`/`requests`/`urllib` bypasses it entirely — and this repo
    already drives `httpx` against remote hosts in four places, so it is an
    in-repo idiom rather than a hypothetical."""
    diff = _added(census_only, "_m12_12_synthetic_http", body)
    assert "http_client_call" in {r.split(" | ")[0] for r in diff.added}, diff.report()


def test_an_edge_from_a_script_into_a_root_module_is_reported(census_only):
    """`import server` in `scripts/` must resolve.

    The absolute-import branch accepted only `boomi_mcp.*`, so a script importing
    a root module got no module path and its calls produced no edge — while the
    root modules had just been added to the scan universe. The real case is
    `scripts/provision_qa_noop_fixture.py` calling `server.manage_component(...)`.
    """
    diff = _added(census_only, "_m12_12_synthetic_script_edge",
                  "import server\n"
                  "def go(*a):\n"
                  "    return server.analyze_component(*a)\n")
    assert "legacy_transitive_call" in {r.split(" | ")[0] for r in diff.added}, diff.report()


def test_a_root_module_import_resolves_from_a_script():
    """Guard the guard for the case the review named — at the RESOLVER.

    The concrete instance the review cited, `scripts/provision_qa_noop_fixture.py`
    calling `server.manage_component(...)`, is no longer in the census: that file
    is gitignored (`.gitignore:140`), and the scan now takes its universe from
    git rather than from the filesystem, because the previous baseline pinned
    that working-copy-only file and therefore FAILED on a clean checkout. The
    mechanism it demonstrated is still pinned — here at the resolver, and by
    `test_an_edge_from_a_script_into_a_root_module_is_reported` end-to-end.
    """
    known = frozenset(inv.python_sources()) | {"scripts/probe.py"}
    assert inv._resolve_module_path("server", 0, "scripts/probe.py", known) == "server.py"
    assert inv._resolve_module_path("server_http", 0, "scripts/probe.py", known) \
        == "server_http.py"
    # A genuinely third-party module is still not ours.
    assert inv._resolve_module_path("requests", 0, "scripts/probe.py", known) is None


def test_a_tool_registered_inside_a_module_level_conditional_can_bear(baseline):
    """`server.py` registers most MCP tools inside `if invoke_api:`.

    Collecting module-level functions from `tree.body` alone left every one of
    them non-bearing, so no caller of them could ever produce an edge.
    """
    import ast as _ast
    tree = _ast.parse(inv.python_sources()["server.py"])
    top_level = {n.name for n in tree.body
                 if isinstance(n, (_ast.FunctionDef, _ast.AsyncFunctionDef))}
    namespace = inv._module_level_functions(tree)
    assert namespace > top_level, "no server.py tool is registered conditionally?"
    assert "manage_component" in namespace


def test_re_pointing_an_http_call_at_the_component_api_breaks_the_freeze(census_only):
    """The census key carries the request TARGET.

    Keying on the verb alone let an existing `external_transport` route be
    re-pointed at `/Component` with no census row, no count change and no
    reconciliation change — the stale route claim stayed green while the call
    became a Component-XML write.
    """
    before = _overlay("_m12_12_synthetic_repoint",
                      "import httpx\n"
                      "def call(x):\n"
                      "    return httpx.post('https://example.test/ping', content=x)\n")
    after = _overlay("_m12_12_synthetic_repoint",
                     "import httpx\n"
                     "def call(x):\n"
                     "    return httpx.post('https://api.boomi.com/Component', content=x)\n")
    diff = inv.compare(inv.build_inventory(sources=after, include_served=False),
                       inv.build_inventory(sources=before, include_served=False))
    assert not diff.empty(), "re-pointing an HTTP call at /Component did not move the census"
    assert any("COMPONENT-API" in row for row in diff.added), diff.report()


@pytest.mark.parametrize("body", [
    "from httpx import post\n"
    "def push(x):\n"
    "    return post('https://api.boomi.com/Component', content=x)\n",
    "from urllib.request import urlopen\n"
    "def push(x):\n"
    "    return urlopen('https://api.boomi.com/Component', data=x)\n",
])
def test_a_directly_imported_http_write_function_is_reported(census_only, body):
    """`from httpx import post; post(...)` — a bare Name callee was rejected
    outright even though its import origin was already recorded."""
    diff = _added(census_only, "_m12_12_synthetic_direct_http", body)
    assert "http_client_call" in {r.split(" | ")[0] for r in diff.added}, diff.report()


def test_a_wrapper_over_a_hand_rolled_http_sink_is_reported(census_only):
    """HTTP sinks must bear transitively like SDK-backed writers do."""
    diff = _added(census_only, "_m12_12_synthetic_http_wrapper", (
        "import httpx\n"
        "def sink(x):\n"
        "    return httpx.post('https://api.boomi.com/Component', content=x)\n"
        "def public(x):\n"
        "    return sink(x)\n"
    ))
    kinds = {r.split(" | ")[0] for r in diff.added}
    assert {"http_client_call", "legacy_transitive_call"} <= kinds, diff.report()


def test_a_function_local_constant_cannot_shadow_a_module_selector(census_only):
    """One scanner-wide constant map let a function-local `KEY = 'other'`
    overwrite the module constant and erase a producer row elsewhere."""
    diff = _added(census_only, "_m12_12_synthetic_shadow", (
        "KEY = 'process_kind'\n"
        "def helper():\n"
        "    KEY = 'other'\n"
        "    return KEY\n"
        "def prepare(config):\n"
        "    config[KEY] = 'sync_pipeline'\n"
        "    return config\n"
    ))
    assert "process_kind_producer" in {r.split(" | ")[0] for r in diff.added}, diff.added


def test_a_function_local_import_is_not_treated_as_a_reexport():
    """The re-export index must be built from module-namespace imports only.

    `ast.walk` also reaches imports nested in functions, which bind a LOCAL
    name; indexing one rewrites `(module, f)` to an unrelated function and drops
    the real edge for every caller of the module-level `f`.
    """
    import ast as _ast
    tree = _ast.parse(
        "def f(x):\n"
        "    return x\n"
        "def helper():\n"
        "    from .other import f\n"
        "    return f\n"
    )
    index = inv.reexport_index({"src/boomi_mcp/categories/m.py": tree,
                                "src/boomi_mcp/categories/other.py": _ast.parse("def f(x):\n    return x\n")})
    assert ("src/boomi_mcp/categories/m.py", "f") not in index, index


def test_the_trading_partner_standard_templates_are_frozen(derived):
    """`_TP_OVERVIEW` advertises the standards; the `operation=create` payload
    does not. Seeding the axis from the operation payload alone left edifact,
    hl7 and the rest outside the digest entirely."""
    by_selector = {a["selector"]: a for a in derived["served_artifacts"]}
    walked = by_selector["walked_surface_digest"]["value"]
    for standard in ("x12", "edifact", "hl7"):
        key = "resource_type=trading_partner|operation=create|standard=%s" % standard
        assert key in walked, "standard template %r is not digested" % standard


def test_the_mcp_digest_covers_every_served_field(derived):
    """FastMCP serves `outputSchema`, `title`, `annotations` and `meta` beside
    `description`/`inputSchema`; this repo already has a non-null output schema
    on `plan_integration_design`."""
    tools = inv._served_tools()
    surface = inv._mcp_tool_surface(tools["plan_integration_design"])
    assert set(surface) >= {"description", "inputSchema"}, sorted(surface)
    assert "outputSchema" in surface, sorted(surface)
    # And the digest is computed over that surface, not a two-field subset.
    digest = {a["selector"]: a for a in derived["served_artifacts"]}[
        "registered_surface_digest"]["value"]
    assert digest["plan_integration_design"] == \
        inv._sha256(inv.canonical_json(surface))


def test_a_selector_hoisted_into_a_constant_is_still_a_producer(census_only):
    """`KEY = "process_kind"; cfg[KEY] = ...` writes a process_kind.

    Accepting only `ast.Constant` at the subscript let hoisting the selector
    into a module constant erase the producer row.
    """
    diff = _added(census_only, "_m12_12_synthetic_const_key", (
        "KEY = 'process_kind'\n"
        "def prepare(config):\n"
        "    config[KEY] = 'sync_pipeline'\n"
        "    return config\n"
    ))
    assert "process_kind_producer" in {r.split(" | ")[0] for r in diff.added}, diff.added


def test_a_setattr_producer_is_reported(census_only):
    diff = _added(census_only, "_m12_12_synthetic_setattr", (
        "def prepare(spec):\n"
        "    setattr(spec, 'process_kind', 'sync_pipeline')\n"
        "    return spec\n"
    ))
    assert "process_kind_producer" in {r.split(" | ")[0] for r in diff.added}, diff.added


def test_the_served_digests_cover_every_surface_not_just_token_matches(derived):
    """Detection must not depend on the token filter.

    `_LEGACY_TOKENS` is a fixed substring list, and served text can acquire
    legacy guidance in words it does not contain — measured: adding "pass the
    complete component XML document in config['xml']" to a tool description
    produced zero artifact churn. The filter now decides only which surfaces
    carry a full frozen VALUE; every registered tool and every walked template
    surface is pinned by digest, so any change to any of them fails the freeze.
    """
    by_selector = {a["selector"]: a for a in derived["served_artifacts"]}

    registered = by_selector["registered_surface_digest"]["value"]
    assert set(registered) == set(inv._served_tools()), \
        "the registered-surface digest is not exhaustive"
    assert len(registered) > 40

    walked = by_selector["walked_surface_digest"]["value"]
    from boomi_mcp.categories import meta_tools
    assert set(walked) == set(inv._schema_template_surfaces(meta_tools)), \
        "the walked-surface digest is not exhaustive"
    assert len(walked) > 200

    # And the digests are identities, not placeholders.
    assert all(len(v) == 64 for v in walked.values())


def test_a_module_qualified_wrapper_is_reported(census_only):
    """`import mod; mod.build_structured_update_xml(...)`.

    The closure once admitted only `ast.Name` callees, so a module-qualified
    wrapper reached the legacy renderer with no census row at all.
    """
    diff = _added(census_only, "_m12_12_synthetic_qualified_wrapper", (
        "from . import integration_builder as ib\n"
        "def wrap(comp, xml):\n"
        "    return ib.build_structured_update_xml(comp, xml)\n"
    ))
    assert "legacy_transitive_call" in {r.split(" | ")[0] for r in diff.added}, diff.report()


def test_an_unrelated_function_sharing_a_bearing_name_is_not_reported(census_only):
    """Transitive identity is `(path, symbol)`, not a bare name.

    `_build_main_process` is defined in THREE archetype modules. A bare-name
    closure links whichever one is legacy-bearing to callers of the other two —
    edges that are true only by coincidence, and false in general.
    """
    diff = _added(census_only, "_m12_12_synthetic_name_collision", (
        "def _build_main_process(spec):\n"
        "    return spec\n"
        "def caller(spec):\n"
        "    return _build_main_process(spec)\n"
    ))
    assert diff.added == [], diff.added


def test_a_one_argument_setdefault_is_still_a_producer(census_only):
    """`d.setdefault(k)` inserts `k` with `None` — it writes, so it produces."""
    diff = _added(census_only, "_m12_12_synthetic_setdefault_1arg", (
        "def prepare(config):\n"
        "    config.setdefault('process_kind')\n"
        "    return config\n"
    ))
    assert "process_kind_producer" in {r.split(" | ")[0] for r in diff.added}, diff.added


def test_the_legacy_process_protocol_templates_are_frozen(derived):
    """`get_schema_template(resource_type='process', operation='create')` echoes
    `process_protocols: ['database_to_api_sync','wrapper_subprocess','sync_pipeline']`
    — the legacy protocol list the issue puts in scope.

    The axis walk once followed only `valid_protocols`, so it descended no
    protocol axis at all and froze none of these templates.
    """
    frozen = {a["selector"] for a in derived["served_artifacts"]
              if a["surface_class"] == "SS-SCHEMA-TEMPLATES"}
    for protocol in ("database_to_api_sync", "wrapper_subprocess", "sync_pipeline"):
        selector = "resource_type=process|operation=create|protocol=%s" % protocol
        assert selector in frozen, "legacy protocol template %r is not frozen" % protocol
    assert sum(1 for s in frozen if "|protocol=" in s) >= 8


def test_no_ledger_row_contradicts_its_write_route(baseline):
    """§11 must give #160 ONE instruction per site.

    A generic census disposition once told the sweep to "guard behind the shared
    process-content classifier" at `_get_channel_raw_json` — a deliberate
    lossless GET whose route says leave it unchanged — while telling its caller
    to "delete or re-home". Ledger dispositions for write sites now come from
    the route table, and transitive rows say plainly that they are edges.
    """
    routed = {loc: r for r in baseline["component_xml_write_routes"]
              for loc in r["locations"]}
    for row in baseline["ledger_rows"]:
        location = "%s::%s" % (row["path"], row["symbol"].split(".")[0])
        if row["census"] in ("component_xml_write", "raw_api_invoker") \
                and location in routed:
            assert routed[location]["route_id"] in row["disposition"] \
                or any(r["route_id"] in row["disposition"]
                       for r in baseline["component_xml_write_routes"]
                       if location in r["locations"]), (
                "%s: ledger disposition does not come from its route" % row["ledger_id"])
        if row["census"] == "legacy_transitive_call":
            assert "edge, not a site" in row["disposition"], row["ledger_id"]


def test_a_setdefault_producer_is_reported(census_only):
    """`config.setdefault("process_kind", ...)` WRITES a process_kind.

    Treating every `.setdefault` as a read put a default-injecting producer in
    the consumer column, where the producer census would never see it.
    """
    diff = _added(census_only, "_m12_12_synthetic_setdefault", (
        "def prepare(config):\n"
        "    config.setdefault('process_kind', 'sync_pipeline')\n"
        "    return config\n"
    ))
    assert "process_kind_producer" in {r.split(" | ")[0] for r in diff.added}


def test_a_hand_rolled_transport_post_is_reported_as_a_write_sink(census_only):
    """A direct `send_request` bypasses every typed create/update verb."""
    diff = _added(census_only, "_m12_12_synthetic_transport", (
        "def post(client, xml):\n"
        "    return client.component.send_request(xml)\n"
    ))
    assert "component_xml_write" in {r.split(" | ")[0] for r in diff.added}


def test_a_legacy_semantic_validation_caller_is_reported(census_only):
    """`_process_ir_semantic_error` is named by the design plan and is private,
    so a public-only filter over the bridge module missed it."""
    diff = _added(census_only, "_m12_12_synthetic_semantic", (
        "from .integration_builder import _process_ir_semantic_error\n"
        "def check(kind, config):\n"
        "    return _process_ir_semantic_error(kind, config)\n"
    ))
    assert "legacy_semantic_validation" in {r.split(" | ")[0] for r in diff.added}


def test_a_bulk_component_caller_is_reported_as_a_write_sink(census_only):
    """`bulk_component` is not a read.

    This inventory's own `sdk_evidence` records that `ComponentBulkRequestType`
    admits CREATE, UPDATE and DELETE, so a production caller could mutate through
    the bulk route. Excluding the bulk verbs on a create_/update_ name prefix
    would have let that happen with no `component_xml_write` row and no entry in
    route reconciliation.
    """
    diff = _added(census_only, "_m12_12_synthetic_bulk", (
        "def f(client, envelope):\n"
        "    return client.component.bulk_component(envelope)\n"
    ))
    assert "component_xml_write" in {r.split(" | ")[0] for r in diff.added}


def test_a_new_tool_that_advertises_a_legacy_path_is_collected():
    """Served-contract growth must be visible.

    The MCP snapshot is derived from the full registry, not a fixed name list, so
    a tool that STARTS advertising `process_kind` or raw process XML becomes a new
    artifact and fails the freeze. Asserting the derivation covers every
    registered legacy-bearing tool is the checkable form of that claim.
    """
    tools = inv._served_tools()
    steering = {
        name for name, tool in tools.items()
        if inv._mentions_legacy(tool.description or "")
        or inv._mentions_legacy(tool.parameters or {})
    }
    collected = {
        a["selector"].rsplit(".", 1)[0]
        for a in inv.load_baseline()["served_artifacts"]
        if a["surface_class"] == "SS-MCP-DESCRIPTIONS"
        # Whole-surface digests are not per-tool artifacts.
        and a["selector"] not in ("registered_surface_digest",
                                  "non_tool_surface_digest")
    }
    assert steering == collected, (
        "registered tools carrying legacy guidance are not the ones frozen.\n"
        "advertising but not frozen: %s\nfrozen but no longer advertising: %s"
        % (sorted(steering - collected), sorted(collected - steering)))
    assert set(inv._LEGACY_STEERING_TOOL_FLOOR) <= steering


def test_the_comparator_reads_every_frozen_section(baseline):
    """A section the inventory declares frozen but the comparator never reads is
    not frozen. Perturb each one and require `compare()` to report it."""
    import copy

    perturbations = {
        "sdk_evidence":
            lambda d: d["sdk_evidence"]["call_shapes"]["update_component"]
            .__setitem__("http_method", "PUT"),
        "ledger_rows":
            lambda d: d["ledger_rows"][0].__setitem__("owning_issue", "#151"),
        "component_xml_write_routes":
            lambda d: d["component_xml_write_routes"][0]
            .__setitem__("classification", "preserve"),
        "served_surface_retraction_matrix":
            lambda d: d["served_surface_retraction_matrix"][0]
            .__setitem__("owning_issue", "#151"),
        "route_reconciliation":
            lambda d: d["route_reconciliation"]["unclassified"].append("x::y"),
    }
    for section, perturb in perturbations.items():
        mutated = copy.deepcopy(baseline)
        perturb(mutated)
        diff = inv.compare(mutated, baseline)
        assert not diff.empty(), (
            "compare() does not read %s — that section is documented as frozen "
            "but nothing would fail if it changed" % section)
        assert any(section in line for line in diff.scalar_changes), diff.report()


#: The escape shapes five consecutive review rounds produced, one per new spelling.
#: They are parametrized together deliberately: the point of the residue invariant is
#: that it covers the CLASS, so they must all be closed by one mechanism rather than by
#: eight more branches.
_ESCAPE_SHAPES = [
    ("typed attribute assignment",
     "def f(spec):\n    spec.process_kind = 'sync_pipeline'\n    return spec\n"),
    ("selector hoisted into a constant",
     "SEL = 'process_kind'\ndef f(c):\n    c[SEL] = 'sync_pipeline'\n    return c\n"),
    ("HTTP call through an injected client",
     "def f(client, x):\n"
     "    return client.post('https://api.boomi.com/Component', content=x)\n"),
    ("HTTP client held on self",
     "import httpx\n"
     "class C:\n"
     "    def __init__(self):\n"
     "        self._c = httpx.Client()\n"
     "    def go(self, x):\n"
     "        return self._c.post('https://api.boomi.com/Component', content=x)\n"),
    ("aliased direct HTTP import",
     "from httpx import post as _p\n"
     "def go(x):\n    return _p('https://api.boomi.com/Component', content=x)\n"),
    ("registry behind a dispatch dict",
     "from .categories.components.builders import get_process_flow_builder\n"
     "def f(k, c):\n"
     "    return {'f': get_process_flow_builder}['f'](k).build(c, name='x')\n"),
    ("symbol reached through globals()",
     "def f(k):\n    return globals()['get_process_flow_builder'](k)\n"),
    ("concatenated selector literal",
     "def f(c):\n    c['process' + '_kind'] = 'sync_pipeline'\n    return c\n"),
]


def _appended(census_only, body):
    """Append to an existing zero-row module.

    Adding a FILE moves `python_source_count`, which makes any diff non-empty
    regardless of the body — so an escape probe that adds a file measures nothing.
    Appending keeps the count fixed, so a reported row is attributable to the body.
    """
    sources = dict(inv.python_sources())
    target = "src/boomi_mcp/__init__.py"
    sources[target] = sources[target] + "\n\n" + body
    current = inv.build_inventory(sources=sources, include_served=False)
    return inv.compare(current, census_only)


@pytest.mark.parametrize("label,body", _ESCAPE_SHAPES,
                         ids=[label for label, _ in _ESCAPE_SHAPES])
def test_every_known_escape_shape_is_at_least_residue(census_only, label, body):
    """No new spelling may vanish.

    Each of these reached a legacy path, a `process_kind` producer or the Component
    API while naming nothing the shape-matching census recognized, so each produced
    ZERO rows and the freeze stayed green. They are closed as a class by the
    total-accounting invariant: classified, or `unclassified_reference` residue.
    """
    diff = _appended(census_only, body)
    kinds = {row.split(" | ")[0] for row in diff.added}
    assert kinds, "%s produced no census row at all — it escaped the freeze" % label
    assert "unclassified_reference" in kinds or kinds - {"unclassified_reference"}, kinds


def test_an_unrelated_append_still_produces_nothing(census_only):
    """Negative control for the residue invariant: it must report MENTIONS of the
    watched vocabulary, not merely that a file changed."""
    diff = _appended(census_only, "def harmless(x):\n    return x + 1\n")
    assert diff.empty(), diff.report()


def test_a_watched_mention_nested_in_a_classified_call_is_not_swallowed(census_only):
    """Consumption marks the SPINE, never the subtree.

    Walking the subtree swallowed any watched mention nested inside a classified
    node, which made the accounting invariant FALSE and was exploitable on real
    frozen sites: wrapping a receiver preserves the emitted row's form AND count,
    so `_pick(ns, _create_component_raw)._emit_x(...)` smuggled a Component-XML
    write sink past the gate with zero census movement.
    """
    sources = dict(inv.python_sources())
    target = ("src/boomi_mcp/categories/components/builders/process_emitters/"
              "legacy.py")
    mutated = sources[target].replace(
        '_emit_documentproperty_assignment("ddp"',
        '_pick(_ns, _create_component_raw)._emit_documentproperty_assignment("ddp"', 1)
    assert mutated != sources[target], "the real call site moved — re-anchor this test"
    sources[target] = mutated

    diff = inv.compare(inv.build_inventory(sources=sources, include_served=False),
                       census_only)
    assert not diff.empty(), "a receiver-wrapped sink was swallowed by consumption"
    assert "unclassified_reference" in {r.split(" | ")[0] for r in diff.added}, diff.report()


def test_the_component_endpoint_marker_is_a_path_segment_not_a_substring():
    """Precision: a bare substring test made 7 of 9 endpoint rows false positives,
    so an unrelated prose edit tripped the freeze."""
    for real in ("https://api.boomi.com/Component", "/Component/{componentId}",
                 "Component/bulk", "POST /Component"):
        assert inv._COMPONENT_ENDPOINT_RE.search(real), real
    for prose in ("/intent/integration_spec/components/", "name/component_name",
                  "references/components/map_component.md", "</Component>",
                  "ProcessIR/topology/component is validated"):
        assert not inv._COMPONENT_ENDPOINT_RE.search(prose), prose

    # The BARE collection path — POST to it is the create route. A `?` or `#`
    # ends the path exactly as end-of-string does, so anchoring on `$` alone let
    # `Component?foo=bar` through.
    def _matches(value):
        return bool(inv._COMPONENT_ENDPOINT_RE.search(value)
                    or inv._COMPONENT_COLLECTION_RE.match(value))

    for collection in ("Component", "Component?foo=bar", "Component#frag"):
        assert _matches(collection), collection
    for not_a_path in ("Componentry", "Component is a thing", "component",
                       "COMPONENT"):
        assert not _matches(not_a_path), not_a_path


@pytest.mark.parametrize("literal", [
    "{}Component", "%sComponent", "%sComponent/%s", "{base}Component",
    "${b}Component", "%(base)sComponent", "%sComponent?x=1", "{}Component/{}",
    "%(base)1sComponent/%(id)s", "%(n)-10.3fComponent",
])
def test_an_interpolated_prefix_does_not_hide_the_component_endpoint(literal):
    """The predicate is normalised, not enumerated.

    The residue pass accounts for every literal, but the test applied to each was
    itself an anchor enumeration — a hand-listed answer to "what may sit next to
    the marker". Three rounds each patched one adjacency (`/Component`,
    `Component/`, `Component?`) and a placeholder glued directly in front
    defeated all of them; `"%sComponent/%s"` is the UPDATE route for a specific
    component. A placeholder means "some unknown prefix", which is what
    string-start and `/` already mean, so it is stripped before the anchors run.
    """
    assert inv._mentions_component_endpoint(literal), literal


@pytest.mark.parametrize("literal", [
    "Componentry", "component", "Component is a thing", "some {x} text",
    "100% done", "a {placeholder} with no marker",
])
def test_placeholder_normalisation_costs_no_precision(literal):
    assert not inv._mentions_component_endpoint(literal), literal


def test_the_endpoint_predicate_has_no_marginal_false_positives():
    """Measured over the whole scan universe rather than asserted.

    Stripping placeholders widens what the anchors see, so the cost has to be
    counted: over ~59,700 string/bytes literals it matches exactly the one known
    prose row, the same as before normalisation.
    """
    import ast as _ast

    def _raw_hit(value):
        return bool(inv._COMPONENT_ENDPOINT_RE.search(value)
                    or inv._COMPONENT_COLLECTION_RE.match(value))

    before, after = [], []
    for path, text in inv.python_sources().items():
        for node in _ast.walk(_ast.parse(text, filename=path)):
            if isinstance(node, _ast.Constant) and isinstance(node.value, (str, bytes)):
                value = (node.value if isinstance(node.value, str)
                         else node.value.decode("utf-8", "ignore"))
                if _raw_hit(value):
                    before.append((path, value[:80]))
                if inv._mentions_component_endpoint(value):
                    after.append((path, value[:80]))

    # A slack bound (`<= 2`) would let ONE new false positive through while the
    # test still passed, and this is the precision guard the documented
    # re-baselining flow leans on. Compare the sets instead: normalisation may
    # only widen recall over literals the anchors already miss for a REASON
    # (a placeholder), never manufacture a new prose match.
    assert set(after) == set(before), (
        "placeholder normalisation introduced %d marginal match(es): %s"
        % (len(set(after) - set(before)), sorted(set(after) - set(before))[:5]))


def test_the_non_tool_mcp_surfaces_are_frozen(derived):
    """`list_tools()` is not the whole served MCP surface. Resources, prompts and
    resource templates are all empty today and nothing asserted they stay that
    way, so registering a legacy-steering resource was invisible growth."""
    surface = {a["selector"]: a for a in derived["served_artifacts"]}[
        "non_tool_surface_digest"]["value"]
    assert set(surface) == {"resources", "prompts", "resource_templates"}, surface
    for label, value in surface.items():
        assert isinstance(value, dict), (label, value)


def test_the_unscanned_asset_universe_is_frozen_and_machine_independent(baseline):
    """Non-Python assets are read by no census; their COUNT is frozen so their
    arrival is a diff.

    Machine-independence has TWO axes and both are enforced here: dotfiles
    (`.DS_Store`) are excluded, and — like the source scan and the example
    census — the universe is what git considers part of the repository, so
    gitignored build output (`*.so`, `*.log`, `*_local_secrets.json` are all
    live patterns) cannot move the count either. All three universes go through
    the same helper, per the structural-fix rule.
    """
    assets = inv.unscanned_assets()
    assert baseline["scan_contract"]["unscanned_asset_count"] == len(assets)
    assert not any(part.startswith(".") for path in assets for part in path.split("/"))

    visible = inv._repository_files()
    if visible is not None:
        assert set(assets) <= visible, sorted(set(assets) - visible)
        assert set(inv.example_documents()) <= visible
        assert set(inv.python_sources()) <= visible


def test_a_c_quotable_path_is_not_silently_dropped():
    """`git ls-files` C-quotes non-ASCII paths under the default
    `core.quotePath=true`, so a plain-newline split never matched them and a
    TRACKED source file vanished from the scan while the freeze stayed green.

    `-z` is what makes the listing faithful; this pins that it is used, since the
    failure mode is silence rather than an error.
    """
    import inspect as _inspect

    source = _inspect.getsource(inv._repository_files)
    assert '"-z"' in source, "git ls-files must use -z or C-quoted paths are dropped"
    assert "split(b" in source, "the -z listing must be split on NUL"

    visible = inv._repository_files()
    if visible is not None:
        # Nothing in the listing is a C-quoted artefact.
        assert not any(path.startswith('"') and path.endswith('"') for path in visible), \
            [p for p in visible if p.startswith('"')][:3]


def test_every_watched_mention_is_classified_or_residue():
    """The conservation invariant, stated directly.

    For every scanned module: each syntactic occurrence of a watched symbol, a
    producer selector, or a Component-endpoint literal is either consumed by a
    positively-classified census row or emitted as residue. This is what replaced
    enumerating recognized shapes — the property is derived from the source, so a
    spelling nobody anticipated lands in residue instead of nowhere.
    """
    import ast as _ast

    vocab = inv.legacy_sink_vocabulary()
    watched = (set(vocab["registry_names"]) | set(vocab["builder_classes"])
               | set(vocab["legacy_emitters"])
               | set(vocab["legacy_semantic_validation"])
               | set(vocab["component_xml_write_sinks"])
               | set(vocab["raw_api_invokers"]))
    selectors = set(vocab["producer_selectors"])

    sources = inv.python_sources()

    # PER-OCCURRENCE, not per module. A module-level check is satisfied by any
    # single row, so a newly missed shape added to a file that already emits
    # rows — `integration_builder.py`, say — left the assertion green while the
    # occurrence was accounted for by nothing.
    unaccounted = []
    for path, text in sources.items():
        tree = _ast.parse(text, filename=path)
        scanner = inv._Scanner(
            path, vocab, local_defs=inv._module_level_functions(tree),
            known_paths=frozenset(sources))
        scanner.visit(tree)
        residue = inv._ResidueScanner(path, vocab, scanner._consumed,
                                      aliases=scanner._aliases)
        residue.visit(tree)
        accounted = set(scanner._consumed) | set(residue._reported)

        for node in _ast.walk(tree):
            mention = None
            if isinstance(node, _ast.Name):
                resolved = scanner._aliases.get(node.id, node.id)
                # `selectors` too: the guard's Name branch omitted them while its
                # Attribute branch included them, so it could not see the very
                # hole it was meant to detect.
                if resolved in watched or resolved in selectors:
                    mention = node.id
            elif isinstance(node, _ast.arg):
                if node.arg in selectors:
                    mention = node.arg
            elif isinstance(node, _ast.Attribute):
                if node.attr in watched or node.attr in selectors:
                    mention = node.attr
            elif isinstance(node, _ast.Constant) and isinstance(node.value, str):
                if node.value in (watched | selectors):
                    mention = node.value
            if mention is not None and id(node) not in accounted \
                    and id(node) not in residue._docstrings:
                unaccounted.append("%s:%s %s" % (path, getattr(node, "lineno", "?"),
                                                 mention))

    assert unaccounted == [], (
        "%d watched occurrence(s) are neither consumed by a classified row nor "
        "emitted as residue — the accounting invariant is broken:\n  %s"
        % (len(unaccounted), "\n  ".join(unaccounted[:20])))


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
    # Values are always stored (see `test_every_served_artifact_stores_its_exact_value`),
    # so this compares every artifact, not a subset.
    for aid, artifact in current.items():
        assert artifact["value"] == frozen[aid]["value"]


def test_every_served_artifact_stores_its_exact_value(baseline):
    """The plan chose exact values over hashes so a failure is REVIEWABLE.

    An earlier draft omitted the value above 8192 canonical characters and kept
    only the hash plus a token excerpt — drift was still detected, but a
    reviewer facing a changed hash on an 80 KB schema had to re-extract the
    value to see what moved, which is the exact drawback the plan rejected
    hash-only snapshots for. Large artifacts now keep the value AND gain the
    excerpt as a convenience.
    """
    for artifact in baseline["served_artifacts"]:
        assert artifact["sha256"]
        assert artifact["value_omitted"] is False
        assert "value" in artifact, artifact["artifact_id"]
        assert inv._sha256(inv.canonical_json(artifact["value"])) == artifact["sha256"], (
            "%s: the stored value does not hash to the stored identity"
            % artifact["artifact_id"])
        if artifact["canonical_length"] > inv._INLINE_VALUE_LIMIT:
            assert "legacy_excerpt" in artifact


def test_the_served_collection_cannot_touch_boomi_transport(baseline):
    """Every served producer must return without reaching the platform.

    The sentinel is already armed for the whole module (`transport_guard`);
    this node states the property explicitly and compares BY VALUE.

    Comparing only the artifact COUNT would have been vacuous: the diagnostic
    probes used to swallow every exception into a `{"_probe_error": ...}`
    artifact, so a probe that hit the sentinel produced an artifact of the same
    shape and the count matched. The probes no longer catch anything, and this
    node now checks hashes, so a sentinel hit fails the derivation outright.
    """
    artifacts = inv.collect_served_artifacts()
    assert artifacts, "the served collection produced nothing under the transport bomb"

    frozen = {a["artifact_id"]: a["sha256"] for a in baseline["served_artifacts"]}
    current = {a["artifact_id"]: a["sha256"] for a in artifacts}
    assert current == frozen, (
        "the served collection differs under the transport bomb — a producer "
        "reached, or tried to reach, the platform")

    blob = inv.canonical_json(artifacts)
    assert "_probe_error" not in blob, (
        "a served artifact recorded its own probe failure instead of the served "
        "text it is supposed to pin")


def test_the_transport_sentinel_is_actually_armed():
    """Guard the guard: prove the bomb fires, so the node above is not passing
    because the patch silently missed its target."""
    from boomi.services.component import ComponentService

    with pytest.raises(AssertionError, match="reached the Boomi transport"):
        ComponentService.create_component(object(), "<Component/>")


def test_the_sdk_evidence_survives_the_transport_sentinel(derived, baseline):
    """The SDK shapes must be read from the class's SOURCE FILE, not its live
    attributes.

    This module patches `ComponentService.create_component` and friends for its
    whole lifetime. An earlier draft resolved call shapes by inspecting those
    attributes, so under the sentinel every verb resolved to the bomb's body and
    the evidence silently became `resolved: false` — evidence that depends on
    whether a test has patched the class is not evidence.
    """
    shapes = derived["sdk_evidence"]["call_shapes"]
    for verb in ("create_component", "update_component", "bulk_component"):
        assert shapes[verb]["resolved"], "%s shape unresolved under the sentinel" % verb
    assert shapes == baseline["sdk_evidence"]["call_shapes"]
    # The two facts the issue's raw-API analysis rests on.
    assert shapes["update_component"]["http_method"] == "POST"
    assert shapes["update_component"]["url_template"] == "/Component/{componentId}"
    assert shapes["bulk_component"]["url_template"] == "/Component/bulk"
    assert shapes["bulk_component"]["accept_header"] == "application/xml"


def test_section_11_7_cites_every_test_in_this_module():
    """The evidence table must not drift from the suite.

    `tests/test_m12_migration_matrix_evidence.py::test_every_cited_regression_test_resolves`
    already proves each CITED node exists; this is the other direction — a node
    added here without a §11.7 row would leave the ledger understating its own
    evidence.
    """
    import inspect as _inspect

    defined = {
        name for name, obj in globals().items()
        if name.startswith("test_") and _inspect.isfunction(obj)
        and obj.__module__ == __name__
    }
    body = inv.section_11_text()
    missing = sorted(n for n in defined if n not in body)
    assert missing == [], (
        "these test nodes are not cited in §11.7 of %s: %s" % (inv.INVENTORY_DOC, missing))


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


def test_the_templates_issue_149_names_are_frozen(derived):
    """The acceptance criteria name specific served templates by file:line.

    The axis walk once followed `available_actions` on the process overview —
    `['list','get']`, the read-only MCP actions, not the template operations
    `['create','list']` — so it could never reach `operation='create'` and froze
    none of these. The operation axis is now derived from the REFUSAL envelope's
    `valid_operations`, which is the authoritative list.
    """
    frozen = {a["selector"] for a in derived["served_artifacts"]
              if a["surface_class"] == "SS-SCHEMA-TEMPLATES"}
    for selector in inv._ALWAYS_FROZEN_TEMPLATES:
        assert selector in frozen, (
            "issue #149 names the served template %r (raw_xml_escape_hatch / "
            "_COMPONENT_CREATE / _COMPONENT_CLONE) and it is not frozen" % selector)

    # The two that carry the escape-hatch text must actually contain it, or the
    # artifact is pinning the wrong payload.
    by_selector = {a["selector"]: a for a in derived["served_artifacts"]}
    for selector in ("resource_type=process|operation=create",
                     "resource_type=component|operation=create"):
        blob = inv.canonical_json(by_selector[selector]["value"])
        assert "process_kind" in blob or "config.xml" in blob, selector


def test_the_schema_template_walk_descends_its_axes(derived):
    """Guard the guard: a walk that stops at the overviews freezes nothing that
    matters, and does so silently."""
    selectors = {a["selector"] for a in derived["served_artifacts"]
                 if a["surface_class"] == "SS-SCHEMA-TEMPLATES"}
    assert any("|operation=" in s for s in selectors)
    assert any("|component_type=" in s for s in selectors)
    assert len(selectors) > 20, len(selectors)


def test_the_retraction_matrix_carries_artifact_ids_not_counts(baseline):
    """#160 executes the sweep from the matrix alone, so the matrix must say
    WHICH strings to retract, not how many."""
    table = inv.emit_matrix_table(baseline)
    assert "Frozen artifact IDs" in table
    ids = {a["artifact_id"] for a in baseline["served_artifacts"]}
    for surface_id in inv.SURFACE_CLASSES:
        owned = sorted(a["artifact_id"] for a in baseline["served_artifacts"]
                       if a["surface_class"] == surface_id)
        assert owned, surface_id
        # Every owned ID appears in the row (Markdown-escaped, since selectors
        # contain the `|` that delimits table cells).
        for artifact_id in owned:
            assert inv._cell(artifact_id) in table, (surface_id, artifact_id)
    assert ids  # non-vacuity


def test_every_ledger_row_carries_a_real_line(baseline):
    """`file:line`, per the acceptance criteria — not `file:0`.

    Producer and boundary rows once hard-coded `evidence_line: 0`, which the
    ledger rendered as `-`.
    """
    zero = [r["ledger_id"] for r in baseline["ledger_rows"] if not r["evidence_line"]]
    assert zero == [], "ledger rows with no line: %s" % zero


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
