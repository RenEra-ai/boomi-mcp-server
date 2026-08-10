"""#146 amendment: the CLEAN-ROOM gate.

Every other test in this repo may import whatever it likes. This one may not,
and that restriction IS the test: it stands in for an LLM caller who has only
the registered MCP tools and their responses — no compiler import, no capability
registry, no repository file it cannot fetch.

Two things are proven here that nothing else can prove:

1. **Sufficiency.** A representative process can be designed and compiled using
   only public responses. Every design decision the fixture makes carries a
   machine-readable citation of the contract entry that authorized it, and the
   harness resolves every citation against the contract the server actually
   serves. A dangling citation fails CI.

2. **Repairability.** A plausible INVALID design is submitted, the structured
   diagnostic comes back, the repair is derived from the cited entries alone,
   and the repaired document compiles. That round trip is what "a caller can
   author and correct ProcessIR without hidden repository knowledge" means
   operationally.

**What CI proves and what it does not.** CI proves citation completeness and
drift — that every fact the fixtures rely on is served and resolvable. It cannot
prove a real LLM finds the contract sufficient; that evidence comes from the
live QA stage. Saying so here is the honest boundary, and it is the reason the
harness asserts what it can mechanically check rather than claiming more.
"""

import ast
import asyncio
import json
import os
import sys
from pathlib import Path

import pytest

_root = str(Path(__file__).resolve().parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)
_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

os.environ["BOOMI_LOCAL"] = "true"

# The ONLY production import this module may make. Everything the tests below
# learn about ProcessIR comes back through a registered tool's response.
import server  # noqa: E402

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "authoring_contract" / "clean_room"

#: Modules a clean-room test may never import. The guard below parses THIS
#: file's own source and enforces it, so the restriction cannot be lost to a
#: careless edit — a test that quietly imported the compiler would still pass
#: every assertion while proving nothing about the public surface.
_FORBIDDEN_IMPORT_PREFIXES = (
    "boomi_mcp.compiler",
    "boomi_mcp.models.process_ir",
    "boomi_mcp.models.cache_property_models",
    "boomi_mcp.authoring",
    "boomi_mcp.kb",
    "boomi_mcp.recipes",
    "boomi_mcp.categories",
)

#: Repository artifacts a served response may not cite, because no MCP tool can
#: fetch them.
_UNSERVED_ARTIFACTS = (".codex/", "docs/architecture", "PROCESS_IR_V1_CAPABILITIES")

#: The ONLY top-level modules a clean-room test may import: the stdlib pieces it
#: needs, pytest, and `server` — the public entry point a caller has.
_ALLOWED_TOP_LEVEL_IMPORTS = frozenset(
    {"ast", "asyncio", "json", "os", "re", "sys", "pathlib", "pytest", "server"}
)


def _resolves(document, pointer):
    """Does an RFC 6901-ish authored path address something in ``document``?"""
    node = document
    for token in [t for t in pointer.split("/") if t]:
        if isinstance(node, list):
            if not token.isdigit() or int(token) >= len(node):
                return False
            node = node[int(token)]
        elif isinstance(node, dict):
            if token not in node:
                return False
            node = node[token]
        else:
            return False
    return True


# ---------------------------------------------------------------------------
# The public surface, reached only through registered tools
# ---------------------------------------------------------------------------


def schema_template(**kwargs):
    return server.get_schema_template(**kwargs)


def _run(coro):
    """Drive one async tool call on a throwaway loop.

    ``asyncio.run`` clears the thread's event loop on exit, which poisons legacy
    modules that still call ``get_event_loop()`` — the same reason the other
    server-importing suites in this repo use a private loop.
    """
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _unwrap(result):
    return (
        result.structured_content
        if hasattr(result, "structured_content")
        else result
    )


def capabilities():
    return _unwrap(_run(server.list_capabilities()))


def design_brief(**kwargs):
    return _unwrap(server.plan_integration_design(**kwargs))


@pytest.fixture(autouse=True)
def _offline_credentials(monkeypatch):
    """Patch the CREDENTIAL boundary, and nothing else.

    ``build_integration`` resolves a profile and constructs an SDK client before
    it dispatches, even for the two read-only actions that never touch the
    network. Requiring a real credential profile would make this suite pass or
    fail on the developer's machine rather than on the contract.

    The concession is deliberately TRANSPORT-only: no authoring fact is stubbed,
    no response is synthesized, and every rule the fixtures rely on still comes
    back from a real served payload. A stub that reached any further would make
    the clean-room claim worthless.
    """
    monkeypatch.setattr(
        server,
        "get_secret",
        lambda subject, profile: {
            "account_id": "clean-room",
            "username": "clean-room",
            "password": "clean-room",
        },
    )


def build(action, config):
    """A read-only build action, driven through the registered wrapper."""
    # ``config`` crosses the MCP boundary as a JSON STRING, which is how a real
    # caller sends it. Serializing here keeps the harness on the same path a
    # client takes rather than on a dict shortcut only Python callers have.
    return _unwrap(
        server.build_integration(
            profile="_clean_room", action=action, config=json.dumps(config)
        )
    )


def resolve_entry(entry_id):
    payload = schema_template(
        schema_name="process_ir_authoring", authoring_entry_id=entry_id
    )
    assert payload["_success"] is True, payload
    return payload["contract_page"]["entries"]


# ---------------------------------------------------------------------------
# The guard on the harness itself
# ---------------------------------------------------------------------------


def test_this_module_imports_nothing_it_is_not_allowed_to():
    """The restriction is the test, so it is enforced mechanically.

    Parsed from this file's own AST rather than trusted: a clean-room suite that
    can be made non-clean by adding one import line is a suite whose result
    means nothing.
    """
    tree = ast.parse(Path(__file__).read_text())
    imported = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)
    # An ALLOWLIST, not a denylist. A denylist only forbids the packages someone
    # thought of: a future `boomi_mcp.something_new` would import cleanly and
    # the suite would still call itself clean-room.
    offenders = [
        name
        for name in imported
        if name.split(".")[0] not in _ALLOWED_TOP_LEVEL_IMPORTS
    ]
    assert offenders == [], offenders
    # The denylist is kept as a second, redundant check — it names the packages
    # that would most obviously defeat the point.
    denied = [
        name
        for name in imported
        if any(name.startswith(prefix) for prefix in _FORBIDDEN_IMPORT_PREFIXES)
    ]
    assert denied == [], denied


def test_the_citation_harness_actually_fails_on_a_dangling_id():
    """The sentinel.

    A harness that cannot fail proves nothing. This asserts the resolver
    distinguishes a real entry from an invented one — which is exactly what
    every fixture assertion below depends on.
    """
    assert resolve_entry("node.branch"), "a real id must resolve"
    assert resolve_entry("node.this_was_renamed") == [], "an invented id must not"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _fixture_document(fixture):
    """The request a fixture's decisions address.

    A repair fixture carries `invalid_request`; a straight authoring fixture
    carries `request`. The citation harness needs whichever one exists.
    """
    for key in ("invalid_request", "request"):
        if key in fixture:
            return fixture[key]
    raise AssertionError(f"fixture {fixture.get('fixture_id')} carries no request")


def _fixture_files():
    return sorted(FIXTURES.glob("*.json"))


def test_fixtures_exist():
    assert _fixture_files(), f"no clean-room fixtures under {FIXTURES}"


@pytest.mark.parametrize(
    "fixture_path", _fixture_files(), ids=lambda path: path.stem
)
def test_every_design_decision_cites_a_resolvable_contract_entry(fixture_path):
    """The citation-resolution harness.

    Each fixture records, per design decision, the authored path it constrains
    and the contract entry ids that authorized it. Three failure modes are
    caught: a citation that resolves to nothing (the contract moved), a
    duplicate citation (the fixture is confused about which rule applies), and a
    decision with no citation at all (a fact the author knew from somewhere
    other than the public surface).
    """
    fixture = json.loads(fixture_path.read_text())
    decisions = fixture["design_decisions"]
    assert decisions, fixture_path.name

    seen_ids = set()
    for decision in decisions:
        assert decision["decision_id"], fixture_path.name
        authored_path = decision["authored_path"]
        assert authored_path, decision["decision_id"]
        # ...and it must RESOLVE inside the fixture's own request. Checking only
        # for non-emptiness let a citation point at a path the document does
        # not contain, which is a dangling reference of a different kind.
        assert _resolves(_fixture_document(fixture), authored_path), (
            decision["decision_id"],
            authored_path,
        )
        cited = decision["contract_entry_ids"]
        assert cited, f"{decision['decision_id']} cites nothing"
        assert len(cited) == len(set(cited)), decision["decision_id"]
        for entry_id in cited:
            entries = resolve_entry(entry_id)
            assert entries, f"{decision['decision_id']} -> dangling {entry_id}"
            seen_ids.add(entry_id)

    assert seen_ids


@pytest.mark.parametrize(
    "fixture_path", _fixture_files(), ids=lambda path: path.stem
)
def test_no_cited_entry_sends_the_caller_to_an_unfetchable_artifact(fixture_path):
    fixture = json.loads(fixture_path.read_text())
    for decision in fixture["design_decisions"]:
        for entry_id in decision["contract_entry_ids"]:
            blob = json.dumps(resolve_entry(entry_id))
            for artifact in _UNSERVED_ARTIFACTS:
                assert artifact not in blob, (entry_id, artifact)


# ---------------------------------------------------------------------------
# The positive flow: discover -> grammar -> rules -> plan -> compile
# ---------------------------------------------------------------------------


def test_the_public_workflow_reaches_a_read_only_compile():
    """Every step a caller with no repository access can actually take."""
    discovery = capabilities()
    assert discovery["_success"] is True
    contract_index = discovery["authoring_contract"]["process_ir_authoring"]
    assert contract_index["entry_count"] > 0

    grammar = schema_template(schema_name="ProcessIRV1")
    assert grammar["_success"] is True
    assert grammar["json_schema"]["$defs"]

    rules = schema_template(schema_name="process_ir_authoring")
    assert rules["_success"] is True
    assert rules["contract_page"]["facets"]["node_kinds"]

    brief = design_brief(authoring_mode="process_ir")
    assert brief["mode"] == "process_ir_pre_selection"
    assert brief["missing_inputs"] == []

    # ...and it must actually REACH the compile the name promises. Stopping at
    # advisory planning left the last two steps — the ones that prove the public
    # surface is sufficient to produce an artifact — unexercised.
    request = _load("branch_leg_order.json")["repaired_request"]

    planned = build("plan", {"authoring_request": request})
    assert planned["authoring_result"]["validation_report"]["is_valid"] is True
    assert planned["mutation_performed"] is False

    compiled = build("compile", {"authoring_request": request})
    assert compiled["_success"] is True
    assert compiled["mutation_performed"] is False
    assert compiled["authoring_result"]["revision_binding"]["compile_hash"]

    # Every phase reports the SAME revisions a caller discovered in step 1.
    served = discovery["authoring_contract"]
    for phase in (planned, compiled):
        binding = phase["authoring_result"]["revision_binding"]
        for field in ("schema_revision", "capability_revision", "compiler_revision"):
            assert binding[field] == served[field], (field, phase["action"])


def test_the_grammar_alone_describes_every_node_it_serves():
    """No undescribed node, checked from the caller's side.

    The model suite asserts the same thing against the generator; this asserts
    it against the SERVED payload, which is the only thing a caller sees.
    """
    schema = schema_template(schema_name="ProcessIRV1")["json_schema"]
    undescribed = sorted(
        name for name, body in schema["$defs"].items() if not body.get("description")
    )
    assert undescribed == []
    blob = json.dumps(schema)
    for artifact in _UNSERVED_ARTIFACTS:
        assert artifact not in blob, artifact


def test_the_behavioural_facts_a_caller_needs_are_all_reachable():
    """The specific facts the amendment says were missing.

    Each was previously either unserved or served only inside a compiler module.
    Reached here through the public selector, with no import.
    """
    branch = resolve_entry("node.branch")[0]
    facts = " ".join(branch["ordering_facts"]).lower()
    assert "authored order" in facts
    # The bound is OURS, and the contract has to say so — a caller told
    # otherwise goes looking for a Boomi setting that does not exist.
    assert "bound of this contract" in facts
    assert "not a boomi platform limit" in facts
    assert "nested branch is not a legal branch-path terminal" in facts

    flow = resolve_entry("node.flow_control")[0]
    assert "documents-per-batch" in flow["summary"].lower() or "batch" in flow["summary"].lower()

    send = resolve_entry("connector_action.database.send")[0]
    send_facts = " ".join(send["ordering_facts"])
    assert "unverified" in send_facts
    assert "May NOT sit inside a retried region" in send_facts

    placement = resolve_entry("placement.branch_path.terminal")[0]
    assert "branch" not in placement["node_kinds"], (
        "a nested Branch must not be advertised as a legal Branch-path terminal"
    )

    dpp = resolve_entry("state_visibility.dpp")[0]
    dpp_facts = " ".join(dpp["ordering_facts"])
    assert "sibling paths: yes" in dpp_facts
    ddp = resolve_entry("state_visibility.ddp")[0]
    assert "sibling paths: no" in " ".join(ddp["ordering_facts"])


def test_the_apply_refusal_is_discoverable_before_authoring_anything():
    """A caller must learn the boundary BEFORE spending effort on the design."""
    brief = design_brief(authoring_mode="process_ir")
    refusals = [
        gap
        for gap in brief["process_ir_capability_gaps"]
        if gap["capability_id"] == "authoring.typed_apply.process_materialization"
    ]
    assert refusals and refusals[0]["state"] == "unsupported"
    assert "apply" not in [step["action"] for step in brief["typed_next_steps"]]


# ---------------------------------------------------------------------------
# The negative flow: invalid design -> diagnostic -> repair -> compile
# ---------------------------------------------------------------------------


def _load(name):
    return json.loads((FIXTURES / name).read_text())


def test_an_invalid_design_returns_a_repairable_structured_diagnostic():
    """A SEMANTIC failure, not a grammar one — which is the harder case.

    The invalid document parses cleanly: the JSON Schema admits both leg
    orderings, so nothing structural is wrong with it. Only the behavioural
    contract knows that Branch paths run in the authored order and that a
    process property written in a later path is not visible to an earlier one.
    That is precisely the class of fact this amendment exists to serve.
    """
    fixture = _load("branch_leg_order.json")
    result = build("plan", {"authoring_request": fixture["invalid_request"]})

    report = result["authoring_result"]["validation_report"]
    assert report["is_valid"] is False
    assert fixture["expected_code"] in report["codes"]

    errors = result["authoring_result"]["errors"]
    assert errors
    matching = [e for e in errors if fixture["expected_code"] in e["cause_codes"]]
    assert matching, errors

    for diagnostic in matching:
        # The AUTHORED node, not the document root.
        assert diagnostic["path"].startswith("/body/steps/1/legs/"), diagnostic["path"]
        assert diagnostic["node_identity"]
        # The validator's OWN remediation, not a genericized restatement.
        assert diagnostic["remediation"]
        assert "re-plan" not in diagnostic["remediation"].lower()
        # Structural evidence a caller can act on.
        assert diagnostic["evidence"]
        # ...and every citation resolves through the public selector.
        assert diagnostic["authoring_contract_entry_ids"]
        for entry_id in diagnostic["authoring_contract_entry_ids"]:
            assert resolve_entry(entry_id), entry_id

    blob = json.dumps(result)
    for artifact in _UNSERVED_ARTIFACTS:
        assert artifact not in blob, artifact


def test_the_repair_derived_from_the_cited_rules_reaches_a_clean_compile():
    """The round trip, closed all the way to compile.

    The repair is the one the served remediation prescribes — swap the paths so
    the write precedes the read — and it must clear BOTH gates. Clearing plan
    but not compile would mean the contract described a fix that only half
    works.
    """
    fixture = _load("branch_leg_order.json")

    # DERIVE the repair from the served remediation, rather than trusting the
    # fixture's prewritten `repaired_request`. The test claimed the repair came
    # from the cited rules; compiling a document someone already fixed proves
    # only that the fixture is valid.
    rejected = build("plan", {"authoring_request": fixture["invalid_request"]})
    diagnostics = [
        row
        for row in rejected["authoring_result"]["errors"]
        if fixture["expected_code"] in row["cause_codes"]
    ]
    assert diagnostics
    remediation = " ".join(row["remediation"] for row in diagnostics).lower()
    # The served instruction is "move the write into an earlier leg" — i.e. swap
    # the paths. Applying THAT, mechanically, to the invalid document.
    assert "earlier leg" in remediation or "order" in remediation

    derived = json.loads(json.dumps(fixture["invalid_request"]))
    legs = derived["intent"]["process_ir"]["body"]["steps"][1]["legs"]
    legs.reverse()
    assert derived == fixture["repaired_request"], (
        "the repair derived from the served remediation must be the fixture's "
        "repaired document — if they differ, one of them is wrong"
    )

    planned = build("plan", {"authoring_request": derived})
    report = planned["authoring_result"]["validation_report"]
    assert report["is_valid"] is True, report
    assert fixture["expected_code"] not in report["codes"]

    compiled = build("compile", {"authoring_request": derived})
    assert compiled["_success"] is True, compiled
    assert compiled["mutation_performed"] is False
    binding = compiled["authoring_result"]["revision_binding"]
    assert binding["compile_hash"], "a successful compile must produce a hash"
    assert compiled["authoring_result"]["artifact_fingerprints"]
    # A compile produces NO build: nothing was materialized.
    assert not compiled.get("build_id")


def test_the_invalid_design_is_refused_at_compile_too():
    """Plan and compile agree. A design that plan rejects must not compile."""
    fixture = _load("branch_leg_order.json")
    compiled = build("compile", {"authoring_request": fixture["invalid_request"]})
    assert compiled["_success"] is False
    assert compiled["mutation_performed"] is False


def test_the_diagnostic_never_echoes_an_authored_value():
    """Value-free, checked with a sentinel the fixture plants in the payload."""
    fixture = _load("branch_leg_order.json")
    result = build("plan", {"authoring_request": fixture["invalid_request"]})
    assert fixture["sentinel_value"] not in json.dumps(result)


def test_every_authoring_fixture_plans_and_compiles_through_the_public_surface():
    """A fixture that is never submitted proves only that JSON parses."""
    compiled = 0
    for fixture_path in _fixture_files():
        fixture = json.loads(fixture_path.read_text())
        if "request" not in fixture:
            continue  # a repair fixture; covered by the negative-flow tests
        result = build("plan", {"authoring_request": fixture["request"]})
        report = result["authoring_result"]["validation_report"]
        assert report["is_valid"] is True, (fixture_path.name, report["codes"])

        result = build("compile", {"authoring_request": fixture["request"]})
        assert result["_success"] is True, fixture_path.name
        assert result["mutation_performed"] is False
        assert result["authoring_result"]["revision_binding"]["compile_hash"]
        compiled += 1
    assert compiled >= 1


#: Construct families the clean-room corpus is INTENDED to cover, and the state
#: of each. Recorded rather than implied: the §6 review found the corpus was one
#: flow while the module docstring implied a representative set, and a coverage
#: gap nobody wrote down is a gap nobody closes.
_CLEAN_ROOM_COVERAGE = {
    "connector": True,
    "map": True,
    "decision": True,
    "terminal": True,
    "references": True,
    "branch": True,
    "state_lineage": True,
    # Reachable only after #146 F6: a try/catch body admits `connector_call`
    # and not `source`/`target`, so while every `connector_call` failed
    # reference resolution, NO try/catch document was compilable at all.
    "try_catch_retry": True,
    # Not yet exercised end-to-end. Each needs a compiling document of its own,
    # and each is named here so the gap is visible in CI output rather than
    # discovered by the next reviewer.
    "flow_control_batching": False,
    "split_combine": False,
    "process_call": False,
    "return_documents": False,
    "cache": False,
}


def test_the_clean_room_coverage_census_matches_the_fixtures():
    """The census must describe the corpus that actually exists."""
    covered = set()
    for fixture_path in _fixture_files():
        fixture = json.loads(fixture_path.read_text())
        covered.update(fixture.get("covers") or ())
        if fixture.get("fixture_id") == "branch_leg_order":
            covered.update({"branch", "state_lineage"})

    claimed = {name for name, done in _CLEAN_ROOM_COVERAGE.items() if done}
    assert claimed == covered, (
        "the coverage census disagrees with the fixtures — update "
        "_CLEAN_ROOM_COVERAGE when a family is added or removed",
        claimed ^ covered,
    )
    # The uncovered families are RECORDED, not silently absent — every census
    # name is accounted for on exactly one side. The earlier form asserted the
    # uncovered set was non-empty, which made closing the last gap FAIL the
    # test: it required the corpus to stay incomplete.
    uncovered = {name for name, done in _CLEAN_ROOM_COVERAGE.items() if not done}
    assert claimed | uncovered == set(_CLEAN_ROOM_COVERAGE)
    assert claimed & uncovered == set()


def test_a_first_class_connector_call_compiles_through_the_public_surface():
    """#146 F6. `connector_call` could not be authored at all.

    `build_symbol_table` never read an operation component's
    `config["connection_key"]`, so every operation symbol carried
    `connection_ref=None`, and `connector_resolution` — which resolves the
    connection off exactly that field — rejected every first-class
    `connector_call`. Because Try/Catch bodies admit `connector_call` and not
    `source`/`target`, no Try/Catch document was compilable either.

    The two shipped fixtures stayed green throughout because `source`/`target`
    carry their own `connection_ref` in the IR, so nothing exercised the symbol
    path. That is why this test authors the node kind the contract publishes as
    first-class rather than reusing a fixture.
    """
    request = json.loads(
        json.dumps(_load("decision_route_connector_map.json")["request"])
    )
    request["intent"]["process_ir"]["body"] = {
        "kind": "sequence",
        "steps": [
            {"kind": "connector_call", "operation_ref": "$ref:src_op"},
            {"kind": "stop"},
        ],
    }

    planned = build("plan", {"authoring_request": request})
    assert planned["_success"] is True, planned.get("error")

    compiled = build("compile", {"authoring_request": request})
    assert compiled["_success"] is True, (
        compiled.get("error"),
        [d.get("code") for d in (compiled.get("authoring_diagnostics") or [])],
    )
    assert not (compiled.get("authoring_diagnostics") or [])
