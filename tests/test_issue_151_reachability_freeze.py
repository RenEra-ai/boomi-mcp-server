"""#160's machine-checked deletion gate (issue #151, M12.14).

The invariant: **no surviving emitter capability is reachable only through an
authoring route scheduled for deletion.** #160 removes the public
``database_to_api_sync`` dispatch entry together with its ``flow_sequence``
sub-dialect; ten emitter-registry keys are reachable through that sub-dialect,
so a deletion taken without this gate could strand them.

Everything here is DERIVED from the runtime authority and then MEASURED:

* the capability set is the emitter registry itself (``_REGISTRATIONS``),
* the direct canonical route is computed by lowering the closed ``ProcessNodeV1``
  union's committed specimens through the real compiler and reading the emitter
  kind each plan node actually carries,
* the legacy routes are computed by running the registered adapters in
  ``legacy_adapters.registry._MIGRATED`` over committed configs and lowering the
  results the way production does,
* the deletion scope is computed from ``ProcessFlowBuilder.PROCESS_KIND`` plus
  every migrated sub-dialect of it.

Nothing in that chain is a hand-written answer key. That matters: the obvious
shortcut — seeding each registration's route from its ``supported_capability``
field — looks derived but is a tautology, because that field is the same literal
``process_ir_v1`` on all 18 rows, so ``routes <= deletion_routes`` could never be
true and the gate would freeze nothing. Issue #149 shipped exactly that shape of
mistake (a walk down echo keys that did not exist, which therefore passed
everything), so this module carries both fail-closed floors and an explicit
non-vacuity witness that asserts the check FAILS on a constructed stranded case.

Honest statement of what the gate proves today: the direct route reaches all 18
keys, so the stranded set is empty with margin. Its value is as a REGRESSION
gate for #158/#159/#160 — the moment a union kind, a lowering arm, or a specimen
stops producing a key, that key falls back to legacy-only and this fails.

Stated bounds (not defects): only one committed ``wrapper_subprocess`` config
exists, so that route's key set is a lower bound; and the per-arm terminal unions
(``BranchLegV1.terminal`` and friends) are inline ``Annotated[Union[...]]`` on
model fields rather than module-level aliases, so they are covered indirectly —
the corpus walk asserts every observed kind is a ``ProcessNodeV1`` member, so a
terminal-only kind missing from the root union fails the first test.
"""

import copy
import json
import pathlib
import sys

import pytest

_HERE = pathlib.Path(__file__).resolve().parent
if str(_HERE) not in sys.path:  # the corpus module is a sibling, not a package
    sys.path.insert(0, str(_HERE))

from boomi_mcp.categories.components.builders.process_flow_builder import (  # noqa: E402
    PROCESS_FLOW_BUILDERS,
    ProcessFlowBuilder,
    _FLOW_SEQUENCE_ALLOWED_KINDS,
    _flow_sequence_enabled,
    get_process_flow_builder,
)
from boomi_mcp.compiler.process_ir import emitter_registry as ER  # noqa: E402
from boomi_mcp.compiler.process_ir import lowering  # noqa: E402
from boomi_mcp.compiler.process_ir.contracts import (  # noqa: E402
    ComponentSymbolV1,
    SymbolTableV1,
)
from boomi_mcp.compiler.process_ir.legacy_adapters import emission as EM  # noqa: E402
from boomi_mcp.compiler.process_ir.legacy_adapters import registry as LAR  # noqa: E402
from boomi_mcp.compiler.process_ir.legacy_adapters.flow_sequence import (  # noqa: E402
    adapt_flow_sequence,
)
from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1  # noqa: E402
from boomi_mcp.compiler.process_ir.semantic_validation.validation_policy import (  # noqa: E402
    lookup_policy,
    registered_adapters,
)
from boomi_mcp.models._process_ir_compat import _KIND_ALIASES  # noqa: E402
from boomi_mcp.models.process_ir import (  # noqa: E402
    BranchLegStepV1,
    DecisionFalseArmStepV1,
    DecisionTrueArmStepV1,
    LinearNodeV1,
    ProcessNodeV1,
    TryCatchBodyStepV1,
    _kinds_of,
    parse_process_ir_v1,
)

import _wave_gate_golden_corpus as corpus  # noqa: E402

# The BARE spelling is used throughout, deliberately: the corpus module warns that
# the bare and ``src.``-prefixed spellings resolve to DIFFERENT module objects, and
# a route map derived from one while the registry came from the other would compare
# two unrelated runtimes. Only the corpus's plain-dict config factories are used
# here, never its ``src.``-spelled builder wrappers.

_FIXTURES = _HERE / "fixtures" / "process_ir"

#: The direct canonical authoring route. The label is the runtime capability
#: constant, not a typed string, so it cannot drift from the registry.
DIRECT_ROUTE = ER.CAPABILITY_PROCESS_IR_V1

#: ``sequence`` is the container discriminator of a control body, not a member of
#: ``ProcessNodeV1``. It is named here so the corpus walk can assert that it is the
#: ONLY non-node ``kind`` present — an unexpected extra kind fails instead of being
#: silently tolerated.
_CONTAINER_KINDS = frozenset({"sequence"})


# ---------------------------------------------------------------------------
# Derivation helpers (not test nodes)
# ---------------------------------------------------------------------------

def _union_kinds():
    return frozenset(_kinds_of(ProcessNodeV1))


def _deletion_routes():
    """The routes #160 removes: the scheduled process kind and its sub-dialects.

    Derived from two runtime constants rather than hand-listed, so a sub-dialect
    added later is picked up automatically. Deliberately NOT "every public builder
    key": ``sync_pipeline`` and ``wrapper_subprocess`` survive the deletion, and
    marking them scheduled would both misinform #160 and over-tighten this gate.
    """
    kind = ProcessFlowBuilder.PROCESS_KIND
    return frozenset({kind}) | frozenset(
        d for d in LAR.migrated_dialects() if d.split("/")[0] == kind
    )


def _policy_key(dialect):
    """The validation-policy name for an adapter dialect.

    The two registries use different vocabularies — the adapter registry keys on
    the full ``database_to_api_sync/flow_sequence`` while the policy registry (and
    the production call site in ``process_flow_builder``) keys on the leaf
    ``flow_sequence``. The leaf mapping is asserted in
    :func:`test_no_emitter_key_is_reachable_only_through_a_deletion_scheduled_route`
    so this stays a checked invariant rather than a second hand-model.
    """
    return dialect.rsplit("/", 1)[-1]


def _shared_block():
    raw = json.loads((_FIXTURES / "flow_sequence_compat_cases.json").read_text())
    return raw["shared"], raw["cases"]


def _resolve(value, shared):
    if isinstance(value, str) and value.startswith("@"):
        return copy.deepcopy(shared[value[1:]])
    if isinstance(value, dict):
        return {k: _resolve(v, shared) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve(v, shared) for v in value]
    return value


def _sentinel_symbols(cfg, bindings):
    """A symbol per authored ref, typed only enough for lowering.

    Modelled on ``tests/test_process_emitter_parity.py``'s builder: lowering
    resolves ref -> component_id and ignores the type, so the ref token doubles as
    the id.
    """
    refs = set()
    for node in cfg.nodes:
        s = node.semantic
        for field in ("connection_ref", "operation_ref", "map_ref", "cache_ref",
                      "process_ref"):
            if getattr(s, field, None):
                refs.add(getattr(s, field))
        for step in getattr(s, "steps", ()):
            if getattr(step, "profile_ref", None):
                refs.add(step.profile_ref)
        for src in getattr(s, "source_values", ()):
            if getattr(src, "profile_ref", None):
                refs.add(src.profile_ref)
    symbols = []
    for ref in sorted(refs):
        b = bindings.get(ref)
        symbols.append(
            ComponentSymbolV1(
                ref=ref,
                component_id=ref,
                component_type="sentinel",
                connector_type=b["connector_type"] if b else None,
                action_type=b["action_type"] if b else None,
            )
        )
    return SymbolTableV1(symbols=tuple(symbols))


def _plan_keys(plan, where):
    assert plan.nodes, "%s: lowered to an EMPTY plan — a specimen that emits " \
                       "nothing would silently shrink the derived route map" % where
    return {node.emitter_input.emitter_kind for node in plan.nodes}


def _direct_corpus():
    """Every committed direct-ProcessIR specimen, globbed so a newly committed
    document joins the derivation automatically."""
    docs = []
    root = json.loads((_FIXTURES / "process_ir_v1.json").read_text())
    for name in sorted(root):
        docs.append(("process_ir_v1.json::%s" % name, root[name], "root"))
    for sub, tag in (("rich_control", "rich"), ("error_handling", "error")):
        for path in sorted((_FIXTURES / sub).glob("*.json")):
            docs.append(("%s/%s" % (sub, path.name),
                         json.loads(path.read_text()), tag))
    return docs


def _direct_route_keys():
    """Emitter keys the DIRECT canonical ProcessIR route actually reaches.

    Measured by lowering each committed specimen — never read off a registration
    field. This is what makes the stranded check falsifiable.
    """
    shared, _cases = _shared_block()
    bindings = shared["bindings"]
    keys = set()
    for where, doc, tag in _direct_corpus():
        if tag == "root":
            ir = parse_process_ir_v1(doc)
            cfg = lowering.lower_process_ir_to_cfg(ir)
            plan = lowering.lower_cfg_to_emission_plan(
                cfg, _sentinel_symbols(cfg, bindings))
        elif tag == "rich":
            (_cfg, plan), _tbl = corpus.rich_compile_doc(doc)
        else:
            _cfg, plan = corpus.error_compile(doc)
        keys |= _plan_keys(plan, where)
    return keys


def _legacy_specimens():
    """{dialect: [(label, config), ...]} over committed configs only."""
    shared, cases = _shared_block()
    specimens = {d: [] for d in LAR.migrated_dialects()}
    for name in sorted(cases):
        cfg = _resolve(cases[name], shared)["config"]
        dialect = (LAR.WRAPPER_SUBPROCESS_DIALECT
                   if cfg.get("process_kind") == "wrapper_subprocess"
                   else LAR.FLOW_SEQUENCE_DIALECT)
        specimens[dialect].append(("flow_sequence_compat_cases::%s" % name, cfg))
    sync = json.loads(
        (_FIXTURES / "sync_pipeline_emitter_parity_cases.json").read_text())["cases"]
    for name in sorted(sync):
        specimens[LAR.SYNC_PIPELINE_DIALECT].append(
            ("sync_parity::%s" % name, corpus.sync_parity_case(name)["config"]))
    return specimens


def _legacy_route_keys():
    """{dialect: {emitter_key, ...}} by running the REGISTERED adapters."""
    specimens = _legacy_specimens()
    assert set(specimens) == set(LAR.migrated_dialects()), (
        "every migrated dialect needs specimens; a newly migrated dialect must "
        "supply them before this gate can speak for it"
    )
    out = {}
    for dialect, cases in sorted(specimens.items()):
        assert cases, "%s: no committed specimen" % dialect
        reached = set()
        for label, cfg in cases:
            result = LAR._MIGRATED[dialect](cfg)
            symbols = EM._symbol_table(result, lambda ref: ref)
            _cfg, plan = compile_process_ir_v1(
                result.process_ir, symbols,
                validation_policy=lookup_policy(_policy_key(dialect)))
            reached |= _plan_keys(plan, "%s/%s" % (dialect, label))
        out[dialect] = reached
    return out


def _authored_flow_sequence_kinds(config):
    """Every ``kind`` authored anywhere in a legacy config's ``flow_sequence``."""
    found = set()

    def walk(node):
        if isinstance(node, dict):
            if isinstance(node.get("kind"), str):
                found.add(node["kind"])
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(config.get("flow_sequence") or [])
    return found


def _ir_kinds_by_source_path(process_ir):
    """``{source_path: ir_kind}`` for every node in an adapted IR body.

    The path spelling is the compiler's own (`/body/steps/1`, `/body/terminal`,
    …) because it is read back off ``EmissionPlanNodeV1.source_path`` — the two
    are the same identity, so attribution needs no second model of the shape.
    """
    document = json.loads(process_ir.model_dump_json())
    paths = {}

    def walk(node, path):
        if isinstance(node, dict):
            if isinstance(node.get("kind"), str):
                paths[path] = node["kind"]
            for key, value in node.items():
                walk(value, path + "/" + key)
        elif isinstance(node, list):
            for index, value in enumerate(node):
                walk(value, path + "/" + str(index))

    walk(document.get("body"), "/body")
    return paths


#: IR node kinds the SURROUNDING config contributes rather than the authored
#: `flow_sequence` list — the source/target endpoints, the compiler's terminal
#: Stop, the body container, and a config-level `return_documents`. None of them
#: is a member of `_FLOW_SEQUENCE_ALLOWED_KINDS`, which is asserted rather than
#: assumed, so excluding them from the step census is derived, not chosen.
_CONFIG_LEVEL_IR_KINDS = frozenset(
    {"sequence", "source", "target", "stop", "return_documents"}
)


def _expected_ir_kind_census(config):
    """The IR step kinds a legacy config's `flow_sequence` MUST produce, as a count.

    Per KIND, not a total. `_KIND_ALIASES` is the adapter's own legacy->IR rename
    table (`models/_process_ir_compat.py`), so reading it here is derivation from
    the runtime authority rather than a second hand-model of the rename — the
    renames are real (`dataprocess` -> `data_process`, `doccacheload` ->
    `cache_put`, `doccacheretrieve` -> `document_cache_retrieve`, `doccacheremove`
    -> `cache_remove`) and a hand-copied table here would be exactly the duplicate
    authority this slice exists to remove.

    Order cannot be used for this: the adapter flattens nested branch legs and
    decision arms elsewhere in the body, so zipping authored order against IR path
    order produces a bogus correspondence (measured: it pairs `message` with
    `branch`). A per-kind multiset is order-free and still per-item.
    """
    census = {}
    for kind in _authored_flow_sequence_kind_list(config):
        mapped = _KIND_ALIASES.get(kind, kind)
        census[mapped] = census.get(mapped, 0) + 1
    return census


def _authored_flow_sequence_kind_list(config):
    """Every authored `flow_sequence` step kind, in document order, nested included."""
    found = []

    def walk(node):
        if isinstance(node, dict):
            if isinstance(node.get("kind"), str):
                found.append(node["kind"])
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(config.get("flow_sequence") or [])
    return found


def _flow_sequence_kind_attribution():
    """Per-STEP attribution for the `flow_sequence` route.

    Returns ``(authored_kinds, ir_kind -> {emitter_key}, unattributed_ir_kinds,
    census)`` where ``census`` is ``[(case, authored_step_count, ir_step_count)]``.

    Aggregating emitter keys across a corpus is not enough to satisfy the issue's
    "a kind added later cannot silently lack a route" criterion: two kinds can
    lower onto the same key, so one can stop lowering while the other keeps the
    aggregate whole.

    The census closes a SECOND hole that per-path attribution alone does not.
    `unattributed` can only report an IR node that reached no plan node — so a
    step the ADAPTER drops outright produces no IR node, appears in neither map,
    and is invisible. Measured: simulating an adapter that drops every `set_ddp`
    leaves all four of the kind assertions passing, because `set_dpp` keeps
    supplying the shared `setproperties_step` key. Counting authored steps against
    the IR nodes they became links each legacy step to its adapted output, which
    is what the review required.
    """
    shared, cases = _shared_block()
    authored = set()
    routes = {}
    unattributed = {}
    census = []
    for name in sorted(cases):
        config = _resolve(cases[name], shared)["config"]
        if config.get("process_kind") == "wrapper_subprocess":
            continue
        authored |= _authored_flow_sequence_kinds(config)
        result = LAR._MIGRATED[LAR.FLOW_SEQUENCE_DIALECT](config)
        _cfg, plan = compile_process_ir_v1(
            result.process_ir,
            EM._symbol_table(result, lambda ref: ref),
            validation_policy=lookup_policy(_policy_key(LAR.FLOW_SEQUENCE_DIALECT)),
        )
        by_path = _ir_kinds_by_source_path(result.process_ir)
        emitted = {}
        for node in plan.nodes:
            path = getattr(node, "source_path", None)
            if path is not None:
                emitted.setdefault(path, set()).add(node.emitter_input.emitter_kind)
        for path, kind in by_path.items():
            if path in emitted:
                routes.setdefault(kind, set()).update(emitted[path])
            else:
                unattributed.setdefault(kind, set()).add("%s::%s" % (name, path))
        actual = {}
        for kind in by_path.values():
            if kind not in _CONFIG_LEVEL_IR_KINDS:
                actual[kind] = actual.get(kind, 0) + 1
        census.append((name, _expected_ir_kind_census(config), actual))
    return authored, routes, unattributed, census


def _route_map():
    """{emitter_key: {route, ...}} over every registry key."""
    direct = _direct_route_keys()
    legacy = _legacy_route_keys()
    routes = {}
    for key in ER.registry_keys():
        r = set()
        if key in direct:
            r.add(DIRECT_ROUTE)
        for dialect, reached in legacy.items():
            if key in reached:
                r.add(dialect)
        routes[key] = r
    return routes, direct, legacy


def _stranded(route_map, deletion_routes):
    """Capabilities whose EVERY route is scheduled for deletion.

    The one helper both the gate and its non-vacuity witness call, so the witness
    exercises the same code path the gate trusts.
    """
    return {
        key: routes
        for key, routes in route_map.items()
        if routes and routes <= set(deletion_routes)
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_the_direct_process_ir_corpus_covers_the_closed_step_kind_union():
    """Every ``ProcessNodeV1`` member has a committed specimen.

    This is the precondition that makes the route map meaningful: a union member
    with no specimen would silently contribute no direct route, and the stranded
    check would then blame the wrong thing.
    """
    union = _union_kinds()
    # Fail closed if the derivation itself collapses.
    assert len(union) >= 20, "ProcessNodeV1 kind derivation collapsed: %s" % sorted(union)
    assert "sequence" not in union, (
        "'sequence' is a container discriminator; if it became a node kind the "
        "container carve-out below would start hiding a real member"
    )
    for alias in (LinearNodeV1, BranchLegStepV1, DecisionTrueArmStepV1,
                  DecisionFalseArmStepV1, TryCatchBodyStepV1):
        assert set(_kinds_of(alias)) <= union, (
            "%s carries a kind outside ProcessNodeV1" % alias)

    observed = set()

    def walk(node):
        if isinstance(node, dict):
            if isinstance(node.get("kind"), str):
                observed.add(node["kind"])
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    for _where, doc, _tag in _direct_corpus():
        walk(doc)

    missing = union - observed
    assert not missing, (
        "ProcessNodeV1 members with no committed direct-ProcessIR specimen: %s — "
        "add one before relying on this gate" % sorted(missing)
    )
    extra = observed - union - _CONTAINER_KINDS
    assert not extra, (
        "corpus carries kinds that are neither ProcessNodeV1 members nor known "
        "containers: %s" % sorted(extra)
    )


def test_no_emitter_key_is_reachable_only_through_a_deletion_scheduled_route():
    """#160's gate: nothing surviving is stranded behind a deleted route."""
    # The capability set is the registry, cross-checked against the closed
    # discriminator so a key that exists in only one of them fails here.
    assert set(ER._REGISTRY) == ER.discriminator_keys() == set(ER.registry_keys())
    assert len(ER.registry_keys()) >= 18, "emitter registry collapsed"

    # The two registries speak different vocabularies (full dialect vs leaf); pin
    # the mapping instead of hand-modelling it.
    assert set(registered_adapters()) == {
        _policy_key(d) for d in LAR.migrated_dialects()
    }, (
        "validation-policy names no longer match the migrated dialects' leaf "
        "names; _policy_key() would silently pick the strict path"
    )

    routes, direct, legacy = _route_map()
    deletion_routes = _deletion_routes()
    assert len(deletion_routes) >= 2, (
        "deletion scope collapsed: %s" % sorted(deletion_routes))

    # MEASURED, not seeded from a constant field: this equality is what makes the
    # stranded comparison below falsifiable.
    assert direct == set(ER.registry_keys()), (
        "direct ProcessIR route no longer reaches every emitter key; missing %s"
        % sorted(set(ER.registry_keys()) - direct)
    )

    # The flow_sequence authoring vocabulary and what it lowers onto.
    fs = legacy[LAR.FLOW_SEQUENCE_DIALECT]
    assert len(_FLOW_SEQUENCE_ALLOWED_KINDS) >= 14, (
        "flow_sequence kind union collapsed: %s" % sorted(_FLOW_SEQUENCE_ALLOWED_KINDS))
    assert len(fs) >= 11, (
        "flow_sequence lowered onto fewer emitter keys than the 11 it is known to "
        "reach: %s" % sorted(fs))

    # PER-KIND, not aggregate. The floor above says only how many kinds the runtime
    # allows and how many keys the corpus reaches in total; neither notices a kind
    # that is allowed but has no specimen, nor one that stops lowering while a
    # sibling keeps the aggregate whole (two kinds share `setproperties_step`, and
    # `cache_get`/`document_cache_retrieve` share `doccacheretrieve`).
    authored, per_kind, unattributed, census = _flow_sequence_kind_attribution()

    # The config-level exclusion used by the census must not quietly swallow an
    # authored kind — derived, so a kind promoted into the flow_sequence
    # vocabulary later cannot be excluded by an unexamined constant.
    assert not (_CONFIG_LEVEL_IR_KINDS & set(_FLOW_SEQUENCE_ALLOWED_KINDS)), (
        "a config-level IR kind is also an authored flow_sequence kind: %s"
        % sorted(_CONFIG_LEVEL_IR_KINDS & set(_FLOW_SEQUENCE_ALLOWED_KINDS))
    )

    # Every authored step must become an IR node. Per-path attribution alone
    # cannot see a step the adapter DROPS (no IR node exists to be unattributed),
    # and a sibling sharing its emitter key hides the loss from the key sets.
    assert census, "flow_sequence census is empty — the walk found no specimens"
    diverged = [
        (name, expected, actual) for name, expected, actual in census
        if expected != actual
    ]
    assert not diverged, (
        "authored flow_sequence steps did not survive adaptation intact "
        "(case, expected-by-kind, actual-by-kind): %s" % diverged
    )
    # The alias table must actually cover the renames it is being trusted for; a
    # silently emptied table would make every expectation the identity and hide a
    # rename regression.
    assert set(_KIND_ALIASES) >= {
        "dataprocess", "doccacheload", "doccacheretrieve", "doccacheremove"
    }, "the adapter's legacy->IR alias table lost entries: %s" % sorted(_KIND_ALIASES)

    # EXACT equality, both directions: an allowed kind with no committed specimen
    # fails here rather than passing silently, which is the criterion "a kind added
    # later cannot silently lack a route".
    assert authored == set(_FLOW_SEQUENCE_ALLOWED_KINDS), (
        "the committed flow_sequence specimens and the builder's allowed-kind "
        "union disagree — allowed but unauthored: %s; authored but not allowed: %s"
        % (sorted(set(_FLOW_SEQUENCE_ALLOWED_KINDS) - authored),
           sorted(authored - set(_FLOW_SEQUENCE_ALLOWED_KINDS)))
    )

    # Every authored step must reach an emitter of its own. `sequence` is the only
    # legitimate exception: it is the body container, not a step, and emits nothing.
    assert set(unattributed) <= _CONTAINER_KINDS, (
        "authored flow_sequence steps produced no emitter node: %s"
        % {k: sorted(v) for k, v in sorted(unattributed.items())
           if k not in _CONTAINER_KINDS}
    )
    assert len(per_kind) >= 14, (
        "per-kind attribution collapsed: only %d IR kinds attributed" % len(per_kind))
    routeless_kinds = sorted(k for k, v in per_kind.items() if not v)
    assert not routeless_kinds, (
        "IR kinds reached from flow_sequence with no emitter key: %s" % routeless_kinds)
    others = set().union(*(v for k, v in legacy.items()
                           if k != LAR.FLOW_SEQUENCE_DIALECT))
    exclusive = fs - others
    assert len(exclusive) >= 10, (
        "flow_sequence-exclusive key set shrank below the 10 this issue exists to "
        "protect: %s" % sorted(exclusive))

    # No key may be routeless: "unknown" must never read as safe.
    routeless = sorted(k for k, r in routes.items() if not r)
    assert not routeless, "emitter keys with no derived authoring route: %s" % routeless

    stranded = _stranded(routes, deletion_routes)
    assert not stranded, (
        "capabilities reachable ONLY through a route scheduled for deletion in "
        "#160 (%s): %s" % (
            sorted(deletion_routes),
            {k: sorted(v) for k, v in sorted(stranded.items())},
        )
    )

    # A readable #160 precondition: every legacy dialect is a subset of direct.
    for dialect, reached in sorted(legacy.items()):
        assert reached <= direct, (
            "%s reaches emitter keys the direct route does not: %s"
            % (dialect, sorted(reached - direct)))

    # The ordinary database_to_api_sync arm is deliberately NOT plan-bearing; it is
    # reserved rather than migrated, so no emission plan exists for it. Asserted,
    # not assumed — if it is ever migrated, _legacy_route_keys()'s completeness
    # check fails until specimens exist.
    assert LAR.adapter_for(ProcessFlowBuilder.PROCESS_KIND) is None
    assert ProcessFlowBuilder.PROCESS_KIND in LAR.RESERVED_DIALECTS


def test_the_freeze_fires_when_a_key_loses_its_direct_process_ir_route():
    """Non-vacuity witness: a constructed stranded case the gate must REJECT.

    Without this, a route map that silently degenerated would keep passing — the
    #149 failure. Two independent shapes are exercised: a real key losing its real
    direct route, and an injected sentinel, so the helper is shown not to
    special-case known keys.
    """
    routes, _direct, legacy = _route_map()
    deletion_routes = _deletion_routes()
    assert not _stranded(routes, deletion_routes), "precondition: tree is clean"

    victim = "doccacheretrieve"
    assert victim in routes and DIRECT_ROUTE in routes[victim]
    assert LAR.FLOW_SEQUENCE_DIALECT in routes[victim], (
        "the witness needs a key the deleted sub-dialect genuinely reaches")

    mutant = {k: set(v) for k, v in routes.items()}
    mutant[victim].discard(DIRECT_ROUTE)
    caught = _stranded(mutant, deletion_routes)
    assert caught == {victim: {LAR.FLOW_SEQUENCE_DIALECT}}, (
        "the freeze did not fire on a key left with only a deleted route: %s"
        % {k: sorted(v) for k, v in caught.items()})

    sentinel = {k: set(v) for k, v in routes.items()}
    sentinel["__sentinel_deleted_only__"] = {LAR.FLOW_SEQUENCE_DIALECT}
    assert "__sentinel_deleted_only__" in _stranded(sentinel, deletion_routes)

    # The mutations were local: production state is untouched.
    again, _d, _l = _route_map()
    assert again == routes
    assert DIRECT_ROUTE in again[victim]


def test_flow_sequence_remains_a_legacy_oracle_without_a_public_kind():
    """Scope item 2: the adapter survives as a parity oracle and gains nothing.

    Byte-level "no new capability" is enforced separately by the diff guard over
    ``flow_sequence.py`` / ``legacy_adapters/registry.py`` / ``process_flow_builder.py``
    recorded in the slice's validation commands; this pins the reachable surface.
    """
    assert LAR._MIGRATED[LAR.FLOW_SEQUENCE_DIALECT] is adapt_flow_sequence

    # No new public process_kind, and no new spelling.
    assert set(PROCESS_FLOW_BUILDERS) == {
        "database_to_api_sync", "sync_pipeline", "wrapper_subprocess"}
    assert LAR.FLOW_SEQUENCE_DIALECT not in PROCESS_FLOW_BUILDERS
    assert get_process_flow_builder(LAR.FLOW_SEQUENCE_DIALECT) is None
    assert ProcessFlowBuilder.PROCESS_KIND == "database_to_api_sync"

    # Selection stays key-presence, with the empty list still enabling.
    assert _flow_sequence_enabled({}) is False
    assert _flow_sequence_enabled({"flow_sequence": None}) is False
    assert _flow_sequence_enabled({"flow_sequence": []}) is True

    # No new adapter registry entry.
    assert set(LAR.migrated_dialects()) == {
        LAR.WRAPPER_SUBPROCESS_DIALECT,
        LAR.FLOW_SEQUENCE_DIALECT,
        LAR.SYNC_PIPELINE_DIALECT,
    }
    assert LAR.RESERVED_DIALECTS == frozenset({"database_to_api_sync"})

    # The sub-dialect is a sub-dialect of the scheduled kind — the structural fact
    # _deletion_routes() derives its scope from.
    assert LAR.FLOW_SEQUENCE_DIALECT.split("/")[0] == ProcessFlowBuilder.PROCESS_KIND
