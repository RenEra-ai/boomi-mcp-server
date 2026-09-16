"""#184: which references name ONE component, on the route a caller reaches.

QA round r3 (QA-184-s1-r3-01, report `agents/reports/2026-09-13-issue-184-stage2-r3.md`)
measured that two component-plan specs binding one existing document cache validated
as two caches through `build_integration`: the plan-time symbol table gives every key
its own placeholder id, and the canonical cache spelling grouped by that id. The request
shapes below are that round's probes, authored from the served request and component
templates. Expected codes and pointers are the ones the report states, never this
implementation's output.

The binding authority is the builder's own (`reused_keys_for_components`, the one
`_will_reuse_at_apply` predicate), read from declared bindings only.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
for _p in (str(_ROOT), str(_ROOT / "src"), str(_HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.authoring import process_ir_effects  # noqa: E402
from boomi_mcp.authoring.workflow import plan_authoring_request_v1  # noqa: E402
from boomi_mcp.categories.integration_builder import (  # noqa: E402
    ComponentWriteConflictError,
    declared_bindings_for_components,
)
from boomi_mcp.compiler.process_ir.contracts import component_identity  # noqa: E402
from boomi_mcp.compiler.process_ir.semantic_validation.context import (  # noqa: E402
    canonical_cache_refs,
)
from boomi_mcp.compiler.process_ir.semantic_validation.pipeline import (  # noqa: E402
    validate_process_ir,
)
from boomi_mcp.models.authoring_workflow import AuthoringRequestV1  # noqa: E402
from boomi_mcp.models.integration_models import IntegrationComponentSpec  # noqa: E402
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402
from boomi_mcp.recipes.materialization import build_symbol_table  # noqa: E402

_MISMATCH = "PROCESS_IR_SEMANTIC_PROFILE_MISMATCH"
_WRITER_MISSING = "PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING"
#: The existing document cache the QA round named (created and executed green in round e0).
_CACHE_ID = "0370d8d8-2c63-42d7-ae11-9aa5bbf64262"


# ---------------------------------------------------------------------------
# The QA round's alias probes, through the typed plan entry
# ---------------------------------------------------------------------------


def _profile(key, field):
    return {"key": key, "type": "profile.json", "name": key, "action": "create", "config": {
        "component_type": "profile.json", "profile_type": "json.generated", "component_name": key,
        "root": {"name": "Root", "kind": "object", "children": [
            {"name": field, "kind": "simple", "data_type": "character", "required": False}]}}}


def _get(key, client):
    return {"key": key, "type": "connector-action", "name": key, "action": "create",
            "depends_on": ["conn", "p_client"], "config": {
                "component_type": "connector-action", "connector_type": "rest",
                "operation_mode": "execute", "component_name": key, "connection_ref_key": "conn",
                "method": "GET", "path": "/admin/cdscm/api/v1/clients/" + client,
                "return_application_errors": True, "track_response": True,
                "response_profile_id": "$ref:p_client", "response_profile_type": "json"}}


def _map(key, source, target, source_field, target_field):
    return {"key": key, "type": "transform.map", "name": key, "action": "create",
            "depends_on": [source, target], "config": {
                "component_type": "transform.map", "map_type": "direct", "component_name": key,
                "source_profile_id": "$ref:" + source, "source_profile_type": "profile.json",
                "target_profile_id": "$ref:" + target, "target_profile_type": "profile.json",
                "field_mappings": [{"source_path": "Root/" + source_field,
                                    "target_path": "Root/" + target_field}]}}


def _cache(key, profile, field, binding):
    spec = {"key": key, "type": "documentcache", "name": key, "action": "create",
            "depends_on": [profile], "config": {
                "component_type": "documentcache", "component_name": key,
                "profile_type": "profile.json", "profile_id": "$ref:" + profile,
                "enforce_single_lucene": True,
                "indexes": [{"index_id": 1, "index_name": "by " + field, "keys": [
                    {"id": 1, "element_key": "3", "name": field + " (Root/Object/" + field + ")"}]}]}}
    if binding == "create_by_id":
        spec["component_id"] = _CACHE_ID
    elif binding == "create_by_padded_id":
        spec["component_id"] = " " + _CACHE_ID + " "
    elif binding == "reference_only":
        spec["config"].update(reference_only=True, component_id=_CACHE_ID)
    return spec


def _step(kind, **fields):
    return dict({"kind": kind}, **fields)


_B2 = _step("set_ddp", name="DDP_S3F1", source_values=[
    {"value_type": "static", "value": "x"},
    {"value_type": "profile", "element_id": "3", "element_name": "b2 (Root/Object/b2)",
     "profile_ref": "$ref:p_b", "profile_type": "profile.json"}])
_READ_A = {"steps": [_step("cache_get", cache_ref="$ref:c_a"), _step("map_ref", map_ref="$ref:m2"), _B2],
           "terminal": _step("stop")}
#: F1a: P1 staged through `c_a`, client documents through `c_alias`, read back through `c_a`.
_MIXED = {"version": "1", "body": {"kind": "sequence", "steps": [_step("branch", legs=[
    {"steps": [_step("connector_call", operation_ref="$ref:op_get1"), _step("map_ref", map_ref="$ref:m1")],
     "terminal": _step("cache_put", cache_ref="$ref:c_a")},
    {"steps": [_step("connector_call", operation_ref="$ref:op_get2")],
     "terminal": _step("cache_put", cache_ref="$ref:c_alias")},
    _READ_A])]}}
#: F1b: P1 staged through `c_alias` alone, read back through `c_a`.
_THROUGH_ALIAS = {"version": "1", "body": {"kind": "sequence", "steps": [_step("branch", legs=[
    {"steps": [_step("connector_call", operation_ref="$ref:op_get1"), _step("map_ref", map_ref="$ref:m1")],
     "terminal": _step("cache_put", cache_ref="$ref:c_alias")},
    _READ_A])]}}


def _plan_errors(ir, binding, alias_profile, alias_field, policy="reuse"):
    components = [
        {"key": "conn", "type": "connector-settings", "name": "Sandbox CDS Mock REST (no-auth)",
         "component_id": "2fe488e4-3169-4529-9515-d854570c8ffc", "action": "create",
         "config": {"connector_type": "rest", "component_name": "Sandbox CDS Mock REST (no-auth)",
                    "base_url": "http://host.docker.internal:8081", "auth": "NONE"}},
        _profile("p_client", "key"),
        _get("op_get1", "1bdb1503-2807-4771-b1b7-8689be8f8e0a"),
        _get("op_get2", "97ef6619-0f72-41bf-9b29-dba25de5f9da"),
        _profile("p_a", "a1"),
        _profile("p_b", "b2"),
        _map("m1", "p_client", "p_a", "key", "a1"),
        _map("m2", "p_a", "p_b", "a1", "b2"),
        _cache("c_a", "p_a", "a1", binding),
        _cache("c_alias", alias_profile, alias_field, binding),
    ]
    request = AuthoringRequestV1.model_validate({"contract_version": "2", "intent": {
        "intent_kind": "process_ir", "integration_name": "identity",
        "units": [{"envelope": {"component_key": "root", "name": "root", "action": "create",
                                "depends_on": [spec["key"] for spec in components]},
                   "process_ir": ir}],
        "components": components, "conflict_policy": policy}})
    result = plan_authoring_request_v1(request, profile="qa_profile", account_id="qa_account")[0]
    return sorted((code, error.path) for error in result.errors for code in error.cause_codes)


@pytest.mark.parametrize("binding", ["create_by_id", "reference_only"])
def test_two_specs_binding_one_cache_are_one_cache_on_the_typed_plan_route(binding):
    """F1a and F1b, both spellings: a client write through the alias makes the other
    spelling's P1 read unproved, and a P1 write through the alias alone establishes it."""
    mixed = _plan_errors(_MIXED, binding, "p_client", "key")
    assert (_MISMATCH, "/body/steps/0/legs/2/steps/1/map_ref") in mixed, mixed
    assert _plan_errors(_THROUGH_ALIAS, binding, "p_a", "a1") == []


def test_two_new_caches_stay_two_caches_on_the_typed_plan_route():
    """Controls F1ad and F1bd: nothing binds the specs to one component."""
    assert _plan_errors(_MIXED, "new", "p_client", "key") == []
    codes = {code for code, _path in _plan_errors(_THROUGH_ALIAS, "new", "p_a", "a1")}
    assert _WRITER_MISSING in codes, codes


def test_a_create_the_clone_policy_copies_is_its_own_cache():
    """Under `clone` a create that names an id is written as a new component, so the
    binding the reuse policy would share does not exist."""
    codes = {code for code, _path in _plan_errors(_THROUGH_ALIAS, "create_by_id", "p_a", "a1", policy="clone")}
    assert _WRITER_MISSING in codes, codes


# ---------------------------------------------------------------------------
# The binding authority and every identity consumer
# ---------------------------------------------------------------------------


def _spec(key, component_type, action="create", component_id=None, **config):
    return IntegrationComponentSpec(
        key=key, type=component_type, action=action, name=key, component_id=component_id, config=config)


def test_declared_bindings_follow_the_apply_reuse_predicate():
    components = [
        _spec("created", "documentcache"),
        _spec("create_by_id", "documentcache", component_id="C-1"),
        _spec("reference", "documentcache", component_id="C-1", reference_only=True),
        _spec("reference_by_config", "documentcache", reference_only=True, component_id="C-3"),
        _spec("updated", "documentcache", action="update", component_id="C-2"),
    ]
    assert declared_bindings_for_components(components, "reuse") == {
        "create_by_id": "C-1", "reference": "C-1", "reference_by_config": "C-3", "updated": "C-2"}
    # `clone` and `fail` write a create's config, so only the references and the update bind.
    for policy in ("clone", "fail"):
        assert declared_bindings_for_components(components, policy) == {
            "reference": "C-1", "reference_by_config": "C-3", "updated": "C-2"}, policy
    symbols = {symbol.ref: symbol for symbol in build_symbol_table(components).symbols}
    assert component_identity(symbols["$ref:create_by_id"]) == component_identity(symbols["$ref:reference"]) == "C-1"
    assert component_identity(symbols["$ref:created"]) == symbols["$ref:created"].component_id


def test_a_reused_create_contributes_no_profile_facts():
    """SELF-184-01: apply keeps the existing component, so the create's config is discarded."""
    components = [
        _spec("p1", "profile.json"),
        _spec("reused", "documentcache", component_id="C-1", profile_id="$ref:p1"),
        _spec("written", "documentcache", action="update", component_id="C-2", profile_id="$ref:p1"),
    ]

    def fact(policy, ref):
        table = build_symbol_table(components, conflict_policy=policy)
        return next(symbol for symbol in table.symbols if symbol.ref == ref).cache_profile_ref

    assert fact("reuse", "$ref:reused") is None
    assert fact("clone", "$ref:reused") == "$ref:p1"   # CONTROL: clone writes this config
    assert fact("reuse", "$ref:written") == "$ref:p1"  # an update writes its config


def test_a_profile_is_compared_by_the_component_its_references_bind():
    """The sibling identity rule: two profile references bound to one component are one
    profile to a map's input check."""
    components = [
        _spec("p_x", "profile.json", reference_only=True, component_id="PROFILE-1"),
        _spec("p_y", "profile.json", reference_only=True, component_id="PROFILE-1"),
        _spec("p_z", "profile.json", reference_only=True, component_id="PROFILE-2"),
        _spec("m_x", "transform.map", source_profile_id="$ref:p_x", target_profile_id="$ref:p_x"),
        _spec("m_y", "transform.map", source_profile_id="$ref:p_y", target_profile_id="$ref:p_y"),
        _spec("m_z", "transform.map", source_profile_id="$ref:p_z", target_profile_id="$ref:p_z"),
    ]
    symbols = build_symbol_table(components)

    def errors(second):
        ir = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
            {"kind": "passthrough"}, {"kind": "map_ref", "map_ref": "$ref:m_x"},
            {"kind": "map_ref", "map_ref": second}, {"kind": "stop"}]}})
        return [(item.code, item.path) for item in validate_process_ir(ir, symbols).errors]

    assert errors("$ref:m_y") == []
    assert (_MISMATCH, "/body/steps/2/map_ref") in errors("$ref:m_z")


def test_the_effect_resolver_binds_and_seeds_by_component_identity():
    components = [
        _spec("p_x", "profile.json", reference_only=True, component_id="PROFILE-1"),
        _spec("p_y", "profile.json", reference_only=True, component_id="PROFILE-1"),
        _spec("p_z", "profile.json", reference_only=True, component_id="PROFILE-2"),
        _spec("c_a", "documentcache", reference_only=True, component_id="CACHE-1"),
        _spec("c_b", "documentcache", reference_only=True, component_id="CACHE-1"),
        _spec("c_new", "documentcache"),
    ]
    symbols = build_symbol_table(components)
    assert process_ir_effects._aliases(symbols, "$ref:c_a") == frozenset({"$ref:c_a", "$ref:c_b"})
    assert process_ir_effects._aliases(symbols, "$ref:c_new") == frozenset({"$ref:c_new"})
    assert process_ir_effects._caller_cache_seeds(
        [("$ref:c_a", "$ref:p_x"), ("$ref:c_a", "$ref:p_y")], symbols) == (("$ref:c_a", "$ref:p_x"),)
    # CONTROL: two profiles that are two components seed nothing.
    assert process_ir_effects._caller_cache_seeds(
        [("$ref:c_a", "$ref:p_x"), ("$ref:c_a", "$ref:p_z")], symbols) == ()


def test_every_production_symbol_table_names_its_conflict_policy():
    """Sibling sweep, derived from the source: which declared bindings apply keeps is a
    fact of the request's policy, so no production construction may fall back to one."""
    source = _ROOT / "src" / "boomi_mcp"
    sites = {"build_symbol_table": [], "_build_canonical_symbols": []}
    for path in sorted(source.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if not isinstance(node, ast.Call):
                continue
            name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
            if name in sites:
                sites[name].append((path.relative_to(source).as_posix(), node.lineno,
                                    {keyword.arg for keyword in node.keywords}))
    # Non-vacuity: the workflow, the recipe engine, the raw route's builder and the
    # revision oracle build tables; the raw route's builder has three callers.
    assert len(sites["build_symbol_table"]) >= 4, sites
    assert len(sites["_build_canonical_symbols"]) == 3, sites
    missing = [site for calls in sites.values() for site in calls if "conflict_policy" not in site[2]]
    assert missing == [], missing


# ---------------------------------------------------------------------------
# Pre-commit adversarial verification of correction batch 3
# ---------------------------------------------------------------------------


def test_a_padded_declared_id_binds_like_its_stripped_spelling():
    """SELF-184-02: the create and update paths hand the declared id back as authored,
    and the symbol field refuses surrounding whitespace, so the plan route raised."""
    components = [
        _spec("c_create", "documentcache", component_id=" C-1 "),
        _spec("c_ref", "documentcache", component_id="C-1", reference_only=True),
        _spec("c_update", "documentcache", action="update", component_id="C-2 "),
    ]
    symbols = {symbol.ref: symbol for symbol in build_symbol_table(components).symbols}
    assert component_identity(symbols["$ref:c_create"]) == component_identity(symbols["$ref:c_ref"]) == "C-1"
    assert symbols["$ref:c_update"].bound_component_id == "C-2"
    # The typed plan route reports; it does not raise.
    assert _plan_errors(_THROUGH_ALIAS, "create_by_padded_id", "p_a", "a1") == []


def test_a_declared_id_spelled_like_a_plan_placeholder_binds_nothing():
    """SELF-184-04: `id-<key>` is this plan's own placeholder spelling, not an account
    component, so it must not merge the reference with the key it stands for."""
    components = [
        _spec("p2", "profile.json"),
        _spec("foo", "documentcache", profile_id="$ref:p2"),
        _spec("bar", "documentcache", component_id="id-foo", reference_only=True),
    ]
    table = build_symbol_table(components)
    symbols = {symbol.ref: symbol for symbol in table.symbols}
    assert symbols["$ref:bar"].bound_component_id is None
    assert canonical_cache_refs(table) == {"$ref:bar": "$ref:bar", "$ref:foo": "$ref:foo"}
    # CONTROL: a real declared id still binds.
    real = build_symbol_table(components[:2] + [_spec("bar", "documentcache", component_id="C-9", reference_only=True)])
    assert {symbol.ref: symbol.bound_component_id for symbol in real.symbols}["$ref:bar"] == "C-9"


# ---------------------------------------------------------------------------
# Stage-2 review round r3 (`cdx-review.0m4OoB`), correction batch 4
# ---------------------------------------------------------------------------

from test_process_ir_effect_declarations import (  # noqa: E402
    _accepted,
    _components,
    _effect,
    _join,
    _map_component,
    _symbols as _effect_symbols,
    _valid_map_config,
)

from boomi_mcp.compiler.process_ir.contracts import SymbolTableV1  # noqa: E402
from boomi_mcp.compiler.process_ir.semantic_validation.contracts import (  # noqa: E402
    DEFAULT_VALIDATION_CAPABILITIES,
)
from boomi_mcp.models.authoring_workflow import (  # noqa: E402
    ProcessIREffectDeclarationsV1,
    ProcessIRMapEffectDeclarationV1,
)


def _declared_map_verdict(alias_key=None, extra=(), policy="reuse"):
    """A map the request UPDATES, an optional `reference_only` alias of the same component, and a
    declaration of the map's effect: `(finding reasons, inert declarations, validation errors)`."""
    written = _map_component(
        [_accepted("dynamic_process_property_set", parameters={"property_name": "OUT"})],
        action="update", component_id="M-1")
    aliases = () if alias_key is None else (IntegrationComponentSpec(
        key=alias_key, type="transform.map", name=alias_key,
        config={"reference_only": True, "component_id": "M-1"}),)
    components = _components(written, *aliases, *extra)
    built = build_symbol_table(components, conflict_policy=policy)
    symbols = SymbolTableV1(symbols=tuple(built.symbols) + tuple(
        symbol for symbol in _effect_symbols().symbols if symbol.ref in ("$ref:CONN", "$ref:GETOP")))
    root = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "source", "connection_ref": "$ref:CONN", "operation_ref": "$ref:GETOP"},
        {"kind": "map_ref", "map_ref": "$ref:MAP"},
        {"kind": "set_dpp", "name": "Y", "source_values": [{"value_type": "dpp", "property_name": "OUT"}]},
        {"kind": "return_documents"},
    ]}})
    declarations = ProcessIREffectDeclarationsV1(map_effects=(ProcessIRMapEffectDeclarationV1(
        map_ref="$ref:MAP", effect=_effect(writes=[("dpp", "OUT")], replay_safe=True)),))
    resolution = process_ir_effects.resolve_process_ir_effect_declarations(
        [("p", root)], declarations, symbols, components, conflict_policy=policy)
    context = resolution.capabilities_by_root.get("p") or DEFAULT_VALIDATION_CAPABILITIES
    report = validate_process_ir(root, symbols, capabilities=context)
    return (
        tuple(finding.reason for finding in resolution.findings),
        resolution.inert,
        [(item.code, item.path) for item in report.errors],
    )


@pytest.mark.parametrize("policy", ["reuse", "fail"])
@pytest.mark.parametrize("alias_key", ["A_REF", "Z_REF"])
def test_a_map_declaration_derives_from_the_config_apply_writes(alias_key, policy):
    """CDX-184-r3-01, under correction batch 9: the map the request updates derives its declared effect
    from the config apply writes, so the declaration binds and the later read passes. An alias naming the
    same component beside it is refused as a written component named twice, whichever spelling sorts first."""
    assert _declared_map_verdict(policy=policy) == ((), (), [])
    with pytest.raises(ComponentWriteConflictError) as refused:
        _declared_map_verdict(alias_key, policy=policy)
    assert refused.value.conflicts == {"M-1": tuple(sorted((alias_key, "MAP")))}


@pytest.mark.parametrize("property_name, over", [("OTHER", {}), ("OUT", {"name": "another component name"})])
def test_two_writes_of_one_map_refuse_the_request(property_name, over):
    """CDX-184-r4-02 and CDX-184-r5-01: two updates of one map leave which configuration executes
    undecided, whether their effects differ or only their metadata does. The request is refused
    before any fact or effect is derived, so nothing is judged against either configuration."""
    other = _map_component(
        [_accepted("dynamic_process_property_set", parameters={"property_name": property_name})],
        key="MAP_TWO", action="update", component_id="M-1", **over)
    with pytest.raises(ComponentWriteConflictError) as refused:
        _declared_map_verdict(extra=(other,))
    assert (refused.value.code, refused.value.keys) == (
        "INTEGRATION_COMPONENT_WRITE_CONFLICT", ("MAP", "MAP_TWO"))


def test_a_written_component_is_named_by_its_writer_only():
    """Stage-2 review round r8 and QA round r9 (correction batch 9): a component the request writes is named
    by that one spec. A `reference_only` spec or a reused create naming it beside the writer is refused,
    whichever spelling sorts first. Specs naming a component nothing in the request writes are unaffected,
    and each describes itself."""
    written = _spec("b_map", "transform.map", action="update", component_id="MAP-1",
                    source_profile_id="$ref:p1", target_profile_id="$ref:p2")
    base = [_spec("p1", "profile.json"), _spec("p2", "profile.json"),
            _spec("m12", "transform.map", source_profile_id="$ref:p1", target_profile_id="$ref:p2")]
    for other in (_spec("a_map", "transform.map", component_id="MAP-1", reference_only=True),
                  _spec("a_map", "transform.map", component_id="MAP-1")):
        with pytest.raises(ComponentWriteConflictError) as refused:
            build_symbol_table(base + [other, written])
        assert refused.value.conflicts == {"MAP-1": ("a_map", "b_map")}
    symbols = build_symbol_table(base + [
        written,
        _spec("r_one", "transform.map", component_id="MAP-2", reference_only=True),
        _spec("r_two", "transform.map", component_id="MAP-2", source_profile_id="$ref:p1", target_profile_id="$ref:p1"),
    ])
    facts = {symbol.ref: (symbol.input_profile_ref, symbol.output_profile_ref) for symbol in symbols.symbols}
    assert facts["$ref:b_map"] == ("$ref:p1", "$ref:p2")
    assert facts["$ref:r_one"] == facts["$ref:r_two"] == (None, None)  # nothing in the request writes MAP-2
    ir = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "passthrough"}, {"kind": "map_ref", "map_ref": "$ref:m12"},
        {"kind": "map_ref", "map_ref": "$ref:b_map"}, {"kind": "stop"}]}})
    errors = [(item.code, item.path) for item in validate_process_ir(ir, symbols).errors]
    assert (_MISMATCH, "/body/steps/2/map_ref") in errors, errors


# ---------------------------------------------------------------------------
# Stage-2 review round r4 (`cdx-review.JxcQ6s`), correction batch 5
# ---------------------------------------------------------------------------


_CONFLICT = "INTEGRATION_COMPONENT_WRITE_CONFLICT"


@pytest.mark.parametrize("second_id", ["C-1", " C-1 "])
def test_two_updates_of_one_cache_are_refused_on_every_route(second_id):
    """CDX-184-r4-01 and CDX-184-r5-01: two updates of one existing cache leave which declaration
    the cache keeps undecided, so the request is refused, with the same code, on the typed plan,
    in the recipe engine and in the raw route's pre-write pass. A padded id is the same id."""
    import types

    from boomi_mcp.authoring.connector_resolution_snapshot import build_connector_resolution_snapshot
    from boomi_mcp.categories import integration_builder
    from boomi_mcp.recipes import engine
    from boomi_mcp.recipes.errors import RecipeError

    components = [
        _spec("p1", "profile.json"),
        _spec("p2", "profile.json"),
        _spec("u1", "documentcache", action="update", component_id="C-1", profile_id="$ref:p1"),
        _spec("u2", "documentcache", action="update", component_id=second_id, profile_id="$ref:p2"),
    ]
    with pytest.raises(ComponentWriteConflictError) as refused:
        build_symbol_table(components)
    assert (refused.value.code, refused.value.conflicts) == (_CONFLICT, {"C-1": ("u1", "u2")})

    # The raw route: its one symbol builder raises, and its pre-write refusal serves the code.
    with pytest.raises(ComponentWriteConflictError) as raw:
        integration_builder._build_canonical_symbols(
            spec=types.SimpleNamespace(components=components, processes=()),
            resolution=build_connector_resolution_snapshot(components, declared={}),
            conflict_policy="reuse",
            existing_ids={},
        )
    envelope = integration_builder._pre_write_refusal(raw.value, failed_step="root")
    assert envelope["error_code"] == _CONFLICT
    assert "the one spec that writes it" in envelope["hint"]

    # The recipe engine: one recipe diagnostic per writing spec.
    with pytest.raises(RecipeError) as recipe:
        engine._compile_processes(composed=type("C", (), {"process_roots": ()})(),
                                  components=components, connector_metadata={}, resolver=None)
    assert [(item.target, item.cause_codes) for item in recipe.value.diagnostics] == [
        ("u1", (_CONFLICT,)), ("u2", (_CONFLICT,))]

    # The typed plan, from the served request shape: reported, one error per writing spec.
    typed = [_profile("p_a", "a1"), _cache("u1", "p_a", "a1", "create_by_id"),
             _cache("u2", "p_a", "a1", "create_by_id")]
    for item in typed[1:]:
        item["action"] = "update"
    typed[2]["component_id"] = _CACHE_ID if second_id == "C-1" else " " + _CACHE_ID + " "
    request = AuthoringRequestV1.model_validate({"contract_version": "2", "intent": {
        "intent_kind": "process_ir", "integration_name": "identity",
        "units": [{"envelope": {"component_key": "root", "name": "root", "action": "create",
                                "depends_on": [spec["key"] for spec in typed]},
                   "process_ir": {"version": "1", "body": {"kind": "sequence", "steps": [
                       {"kind": "passthrough"}, {"kind": "stop"}]}}}],
        "components": typed, "conflict_policy": "reuse"}})
    result = plan_authoring_request_v1(request, profile="qa_profile", account_id="qa_account")[0]
    assert sorted((error.code, error.subject_id) for error in result.errors) == [
        (_CONFLICT, "u1"), (_CONFLICT, "u2")]


_PROFILE_CONFIG = {"component_type": "profile.json", "profile_type": "json.generated", "root": {
    "name": "Root", "kind": "object", "children": [{"name": "a", "kind": "simple", "data_type": "character"}]}}
#: Every branch apply takes for a spec naming an existing id: the create-by-id reuse, clone and
#: refusal; the planner's `reference_only` create; an update, with `reference_only` in every
#: spelling; the connector update apply binds because it authors only metadata, and one it writes.
_APPLY_KINDS = {
    "create_by_id": ("profile.json", "create", _PROFILE_CONFIG),
    "create_reference_only": ("profile.json", "create", {"reference_only": True}),
    "update": ("profile.json", "update", _PROFILE_CONFIG),
    "update_reference_only": ("profile.json", "update", dict(_PROFILE_CONFIG, reference_only=True)),
    "update_reference_only_false_string": ("profile.json", "update", dict(_PROFILE_CONFIG, reference_only="false")),
    "update_reference_only_one": ("profile.json", "update", dict(_PROFILE_CONFIG, reference_only=1)),
    "connector_metadata_update": ("connector-action", "update", {"connector_type": "rest"}),
    "connector_renaming_update": ("connector-action", "update", {"connector_type": "rest", "component_name": "renamed"}),
}


def _apply_component(key, kind):
    component_type, action, config = _APPLY_KINDS[kind]
    return {"key": key, "type": component_type, "name": key, "action": action, "component_id": "X-1",
            "config": dict(config)}


_OUTCOMES = {}


def _apply_outcome(kind, policy):
    """What apply DOES with one spec of this kind naming the existing id `X-1`: `refused`, or the
    status it records (`reused`, `created`, `updated`). Offline: only execution is replaced."""
    import copy
    from unittest import mock

    from boomi_mcp.categories import integration_builder

    if (kind, policy) in _OUTCOMES:
        return _OUTCOMES[kind, policy]
    config = {"dry_run": False, "conflict_policy": policy,
              "integration_spec": {"name": "outcome", "components": [_apply_component("a", kind)]}}

    def execute(*args, **kwargs):
        target = kwargs.get("target_id")
        return {"_success": True, "component_id": target or "NEW", "status": "updated" if target else "created"}

    with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []), \
            mock.patch.object(integration_builder, "_execute_component", side_effect=execute):
        result = integration_builder._apply_plan(mock.MagicMock(), "outcome_profile", copy.deepcopy(config))
    row = (result.get("results") or {}).get("a") or {}
    outcome = row.get("status") if result.get("_success") else "refused"
    if outcome == "updated" and kind == "create_by_id":
        outcome = "created"  # the clone branch hands the executor the id it copies
    _OUTCOMES[kind, policy] = outcome
    return outcome


@pytest.mark.parametrize("policy", ["reuse", "clone", "fail"])
@pytest.mark.parametrize("kind", sorted(_APPLY_KINDS))
def test_the_write_predicates_agree_with_what_apply_does(kind, policy):
    """Pre-commit verification of correction batch 6: the predicates are checked against apply's
    own run, not against a statement of it. A `reference_only` update is WRITTEN."""
    from boomi_mcp.categories.integration_builder import (
        apply_writes_component_config,
        component_writes_existing,
    )

    outcome = _apply_outcome(kind, policy)
    assert outcome in ("refused", "reused", "created", "updated"), outcome
    spec = IntegrationComponentSpec(**_apply_component("a", kind))
    if outcome != "refused":
        assert apply_writes_component_config(spec, policy) is (outcome in ("created", "updated")), outcome
    assert component_writes_existing(spec) is (outcome == "updated"), outcome


def test_the_apply_outcomes_cover_every_branch():
    """Non-vacuity: the kinds above reach every outcome apply records for an existing id."""
    seen = {_apply_outcome(kind, policy) for kind in _APPLY_KINDS for policy in ("reuse", "clone", "fail")}
    assert seen == {"refused", "reused", "created", "updated"}, seen
    assert _apply_outcome("update_reference_only", "reuse") == "updated"
    assert _apply_outcome("connector_metadata_update", "reuse") == "reused"


@pytest.mark.parametrize("policy", ["reuse", "clone", "fail"])
@pytest.mark.parametrize("first", sorted(_APPLY_KINDS))
@pytest.mark.parametrize("second", sorted(_APPLY_KINDS))
def test_a_write_conflict_is_a_written_component_named_twice(first, second, policy):
    """The coverage claim, derived from apply (correction batch 9): a request is refused exactly when apply
    binds or writes the one existing component from both specs and writes it from at least one."""
    outcomes = (_apply_outcome(first, policy), _apply_outcome(second, policy))
    expected = all(outcome in ("updated", "reused") for outcome in outcomes) and "updated" in outcomes
    components = [IntegrationComponentSpec(**_apply_component("a", first)),
                  IntegrationComponentSpec(**_apply_component("b", second))]
    try:
        build_symbol_table(components, conflict_policy=policy)
        refused = False
    except ComponentWriteConflictError:
        refused = True
    assert refused is expected, outcomes


def test_a_reference_only_update_is_described_by_its_own_config():
    """Pre-commit verification of correction batch 6: a `reference_only` update is written, so it
    keeps its own facts, its map effect is derivable, and a second update of the same id is a
    write conflict whatever spelling the flag takes."""
    from boomi_mcp.authoring.process_ir_effects import _may_be_substituted

    alone = build_symbol_table([
        _spec("p2", "profile.json"),
        _spec("c", "documentcache", action="update", component_id="C-1", profile_id="$ref:p2", reference_only=True),
    ])
    assert {symbol.ref: symbol.cache_profile_ref for symbol in alone.symbols}["$ref:c"] == "$ref:p2"
    written_map = _spec("m", "transform.map", action="update", component_id="M-1", reference_only=True)
    assert _may_be_substituted(written_map, "reuse") is False
    assert _may_be_substituted(_spec("r", "transform.map", component_id="M-1", reference_only=True), "clone") is True
    for flag in (True, "false", 1):
        with pytest.raises(ComponentWriteConflictError):
            build_symbol_table([
                _spec("p1", "profile.json"), _spec("p2", "profile.json"),
                _spec("w", "documentcache", action="update", component_id="C-1", profile_id="$ref:p1"),
                _spec("c", "documentcache", action="update", component_id="C-1", profile_id="$ref:p2",
                      reference_only=flag),
            ])


def test_a_write_conflict_keeps_every_check_that_needs_no_symbol_table():
    """Pre-commit verification of correction batch 6: the typed plan still hands a caller
    everything wrong at once. A literal extension-binding connection id is refused beside the
    write conflict, and the control without the conflict reports the same relocatability error."""
    literal = "35813b90-1f42-4dcb-98f5-82d8f96be61d"
    conn = {"key": "conn", "type": "connector-settings", "name": "conn", "action": "create",
            "config": {"connector_type": "rest", "component_name": "conn",
                       "base_url": "https://orders.example.invalid", "auth": "NONE"}}
    op = {"key": "op", "type": "connector-action", "name": "op", "action": "create", "depends_on": ["conn"],
          "config": {"connector_type": "rest", "operation_mode": "execute", "component_name": "op",
                     "connection_ref_key": "conn", "method": "GET", "path": "/v1/things"}}
    updates = [_profile("p_a", "a1"), _cache("u1", "p_a", "a1", "create_by_id"),
               _cache("u2", "p_a", "a1", "create_by_id")]
    for item in updates[1:]:
        item["action"] = "update"

    def errors(components):
        request = AuthoringRequestV1.model_validate({"contract_version": "2", "intent": {
            "intent_kind": "process_ir", "integration_name": "report_all",
            "units": [{"envelope": {"component_key": "proc", "name": "proc", "action": "create",
                                    "depends_on": ["conn", "op"], "process_extensions": {"connections": [{
                                        "connection_id": literal, "connector_type": "rest",
                                        "fields": [{"id": "url", "label": "x"}]}]}},
                       "process_ir": {"version": "1", "body": {"kind": "sequence", "steps": [
                           {"kind": "source", "connection_ref": "$ref:conn", "operation_ref": "$ref:op"},
                           {"kind": "message", "text": "hello"}, {"kind": "return_documents"}]}}}],
            "components": components, "conflict_policy": "reuse"}})
        result = plan_authoring_request_v1(request, profile="qa_profile", account_id="qa_account")[0]
        return sorted((error.code, error.subject_id, error.path) for error in result.errors)

    relocatable = ("PROCESS_MATERIALIZATION_REFERENCE_NOT_RELOCATABLE", "proc", "/process_extensions/connections/0/connection_id")
    assert relocatable in errors([conn, op])
    with_conflict = errors([conn, op] + updates)
    assert relocatable in with_conflict, with_conflict
    assert (_CONFLICT, "u1", "") in with_conflict and (_CONFLICT, "u2", "") in with_conflict, with_conflict


def test_a_write_conflict_does_not_judge_effect_declarations_without_a_symbol_table():
    """Second pre-commit verification of correction batch 6: with no symbol table, a correct map and
    subprocess declaration cannot be judged, so the plan reports the conflict and no declaration
    mismatch. The control without the conflict admits the same declarations."""
    def request(conflict):
        components = [_profile("p_a", "a1"), _profile("p_b", "b2"), _map("m1", "p_a", "p_b", "a1", "b2")]
        if conflict:
            updates = [_cache("u1", "p_a", "a1", "create_by_id"), _cache("u2", "p_a", "a1", "create_by_id")]
            for item in updates:
                item["action"] = "update"
            components += updates
        root = {"version": "1", "body": {"kind": "sequence", "steps": [
            {"kind": "passthrough"}, {"kind": "branch", "legs": [
                {"steps": [{"kind": "map_ref", "map_ref": "$ref:m1"}], "terminal": {"kind": "stop"}},
                {"steps": [{"kind": "message", "text": "m"}],
                 "terminal": {"kind": "process_call", "process_ref": "$ref:child"}}]}]}}
        child = {"version": "1", "body": {"kind": "sequence", "steps": [
            {"kind": "passthrough"}, {"kind": "message", "text": "m"}, {"kind": "return_documents"}]}}
        return AuthoringRequestV1.model_validate({"contract_version": "2", "intent": {
            "intent_kind": "process_ir", "integration_name": "declared",
            "units": [
                {"envelope": {"component_key": "child", "name": "child", "action": "create"}, "process_ir": child},
                {"envelope": {"component_key": "root", "name": "root", "action": "create",
                              "depends_on": [item["key"] for item in components] + ["child"]}, "process_ir": root},
            ],
            "components": components, "conflict_policy": "reuse"},
            "effect_declarations": {
                "map_effects": [{"map_ref": "$ref:m1", "effect": {"writes": [{"scope": "dpp", "name": "OUT"}],
                                                                  "replay_safe": True}}],
                "subprocess_effects": [{"process_ref": "$ref:child", "effect": {"replay_safe": True}}]}})

    def errors(conflict):
        result = plan_authoring_request_v1(request(conflict), profile="qa_profile", account_id="qa_account")[0]
        return sorted((error.code, error.subject_id, error.path) for error in result.errors)

    assert errors(False) == []
    assert errors(True) == [(_CONFLICT, "u1", ""), (_CONFLICT, "u2", "")]


def test_a_child_summary_keys_its_caches_by_component():
    """Pre-commit verification of correction batch 6 (the child-summary sibling of CDX-184-r5-02):
    a child writing a cache through one reference and reading it back through another requires
    nothing of its caller, so the truthful declaration binds and an over-read is a mismatch."""
    from boomi_mcp.authoring.process_ir_effects import derive_subprocess_effect
    from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1
    from boomi_mcp.models.authoring_workflow import ProcessIRSubprocessEffectDeclarationV1

    def sym(ref, component_id, component_type):
        return ComponentSymbolV1(ref="$ref:" + ref, component_id=component_id, component_type=component_type)

    symbols = SymbolTableV1(symbols=(sym("CACHE", "C-1", "documentcache"), sym("CACHE_ALIAS", "C-1", "documentcache"),
                                     sym("PARENT", "P-1", "process"), sym("CHILD", "P-2", "process")))
    entry = {"kind": "passthrough", "label": "Receive"}
    message = {"kind": "message", "text": "m"}
    child = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [entry, {"kind": "branch", "legs": [
        {"steps": [message], "terminal": {"kind": "cache_put", "cache_ref": "$ref:CACHE"}},
        {"steps": [{"kind": "cache_get", "cache_ref": "$ref:CACHE_ALIAS"}, message], "terminal": {"kind": "stop"}},
    ]}]}})
    parent = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        entry, {"kind": "process_call", "process_ref": "$ref:CHILD"}]}})
    assert derive_subprocess_effect(child, symbols=symbols).effect[0] == ()
    # CONTROL: against no table the aliases are two caches, which is what the resolver used to ask.
    assert derive_subprocess_effect(child).effect[0] == (("cache", "$ref:CACHE_ALIAS"),)

    def findings(declared):
        roots = [("PARENT", parent), ("CHILD", child)]
        resolution = process_ir_effects.resolve_process_ir_effect_declarations(
            roots, ProcessIREffectDeclarationsV1(subprocess_effects=(
                ProcessIRSubprocessEffectDeclarationV1(process_ref="$ref:CHILD", effect=declared),)),
            symbols, [], child_roots={"$ref:" + key: ir for key, ir in roots})
        return [finding.reason for finding in resolution.findings]

    assert findings(_effect(writes=[("cache", "$ref:CACHE")])) == []
    assert findings(_effect(writes=[("cache", "$ref:CACHE_ALIAS")])) == []
    assert findings(_effect(reads=[("cache", "$ref:CACHE")], writes=[("cache", "$ref:CACHE")])) == ["content-mismatch"]


def test_the_write_conflict_reads_the_bind_predicate_apply_runs():
    """The non-body connector update apply binds instead of writing (QA-157-r2-01) is decided by ONE
    predicate, and both apply's bind step and the write predicate call it. A metadata-only update alone
    binds and is admitted; beside a write of the same operation it is a second spec naming a written
    component, and so is a second write (correction batch 9)."""
    import ast

    from boomi_mcp.categories import integration_builder

    tree = ast.parse(Path(integration_builder.__file__).read_text(encoding="utf-8"))
    callers = set()
    rules = []
    for function in (node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)):
        for node in ast.walk(function):
            if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "_binds_as_metadata_only_connector_update":
                callers.add(function.name)
            if isinstance(node, ast.BoolOp):
                called = {getattr(item.func, "id", None) for item in ast.walk(node) if isinstance(item, ast.Call)}
                if {"_is_metadata_only_update", "smart_merge_would_change"} <= called:
                    rules.append(function.name)
    # Apply's bind step and the one "apply writes this config" predicate every plan-time
    # check asks (`component_writes_existing` through it).
    assert {"_apply_plan", "apply_writes_component_config"} <= callers, callers
    assert set(rules) == {"_binds_as_metadata_only_connector_update"}, rules

    def op(key, **config):
        return _spec(key, "connector-action", action="update", component_id="OP-1", connector_type="rest", **config)

    build_symbol_table([op("alias"), _spec("p1", "profile.json")])
    for second in (op("alias"), op("other", response_profile_id="$ref:p1")):
        with pytest.raises(ComponentWriteConflictError):
            build_symbol_table([op("op", response_profile_id="$ref:p1"), second, _spec("p1", "profile.json")])


def _joined_map_findings(join_ref, declared_ref, alias_id="C-7"):
    """A map the request updates, joining one existing cache through `join_ref`, with a
    declaration naming the cache through `declared_ref`: the resolver's finding reasons."""
    indexes = [{"index_id": 1, "keys": [{"id": 1, "name": "a (Root/a)"}]}]

    def cache(key, component_id):
        return IntegrationComponentSpec(key=key, type="documentcache", action="create", name=key,
                                        component_id=component_id, config={
                                            "component_name": key, "reference_only": True,
                                            "component_id": component_id, "indexes": indexes})

    config = _valid_map_config("function", document_cache_joins=[_join(document_cache_id=join_ref)],
                               function_mappings=[_accepted("dynamic_process_property_set")])
    written = IntegrationComponentSpec(key="MAP", type="transform.map", action="update", component_id="M-7",
                                       name="MAP", depends_on=["SP", "TP", join_ref[len("$ref:"):]], config=config)
    components = _components(written, cache("CACHE", "C-7"), cache("CACHE_ALIAS", alias_id))
    built = build_symbol_table(components)
    symbols = SymbolTableV1(symbols=tuple(built.symbols) + tuple(
        symbol for symbol in _effect_symbols().symbols if symbol.ref in ("$ref:CONN", "$ref:GETOP")))
    root = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "source", "connection_ref": "$ref:CONN", "operation_ref": "$ref:GETOP"},
        {"kind": "map_ref", "map_ref": "$ref:MAP"}, {"kind": "return_documents"}]}})
    declarations = ProcessIREffectDeclarationsV1(map_effects=(ProcessIRMapEffectDeclarationV1(
        map_ref="$ref:MAP", effect=_effect(reads=[("cache", declared_ref)], writes=[("dpp", "OUT")],
                                           replay_safe=True)),))
    resolution = process_ir_effects.resolve_process_ir_effect_declarations(
        [("p", root)], declarations, symbols, components)
    return tuple(finding.reason for finding in resolution.findings), resolution.inert


def test_a_declared_cache_read_is_compared_by_component_not_spelling():
    """CDX-184-r5-02 and its single-writer sibling: a map joining a cache through one reference
    and a declaration naming it through another describe one read, so the declaration binds.
    The control names a different cache and stays a mismatch."""
    assert _joined_map_findings("$ref:CACHE_ALIAS", "$ref:CACHE") == ((), ())
    assert _joined_map_findings("$ref:CACHE", "$ref:CACHE_ALIAS") == ((), ())
    assert _joined_map_findings("$ref:CACHE_ALIAS", "$ref:CACHE", alias_id="C-8") == (("content-mismatch",), ())


def test_every_declared_effect_is_compared_in_canonical_cache_spelling():
    """The sibling sweep for CDX-184-r5-02, read from the resolver's source: every declared effect
    (map, script and subprocess) reaches its comparison only through `_canonical_effect`."""
    import ast

    tree = ast.parse(Path(process_ir_effects.__file__).read_text(encoding="utf-8"))
    parents = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[child] = node
    declared = [node for node in ast.walk(tree)
                if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "_declared"]
    # Non-vacuity: the three declaration kinds each compare one.
    assert len(declared) == 3, len(declared)
    for call in declared:
        parent = parents[call]
        assert isinstance(parent, ast.Call) and getattr(parent.func, "id", None) == "_canonical_effect", call.lineno


def test_a_metadata_only_alias_beside_the_operation_update_is_refused():
    """CDX-184-r4-03 and QA-184-s1-r7-02, under correction batch 9: an update alias naming only the connector
    type binds the operation the structured update writes, so the pair is a written component named twice and
    is refused. The structured update alone admits a GET feeding a map of its profile; the alias alone states
    no action, so a call through it is still refused."""
    from boomi_mcp.compiler.process_ir import connector_capabilities

    rest = connector_capabilities.REST_FAMILY
    base = [_spec("p1", "profile.json"), _spec("p2", "profile.json"),
            _spec("conn", "connector-settings", connector_type="rest"),
            _spec("m12", "transform.map", source_profile_id="$ref:p1", target_profile_id="$ref:p2")]
    op = _spec("op", "connector-action", action="update", component_id="OP-1", connector_type="rest",
               connection_ref_key="conn", response_profile_id="$ref:p1")
    alias = _spec("op_alias", "connector-action", action="update", component_id="OP-1", connector_type="rest")
    metadata = {"conn": (rest, None), "op": (rest, "GET")}
    with pytest.raises(ComponentWriteConflictError) as refused:
        build_symbol_table(base + [op, alias], connector_metadata=metadata)
    assert refused.value.conflicts == {"OP-1": ("op", "op_alias")}

    def errors(components, operation_ref):
        ir = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
            {"kind": "connector_call", "operation_ref": operation_ref},
            {"kind": "map_ref", "map_ref": "$ref:m12"}, {"kind": "stop"}]}})
        table = build_symbol_table(components, connector_metadata=metadata)
        return [(item.code, item.path) for item in validate_process_ir(ir, table).errors]

    assert errors(base + [op], "$ref:op") == []
    assert errors(base + [alias], "$ref:op_alias") != []


def test_the_served_profile_mismatch_text_names_every_reporting_site():
    """QA-184-s1-r5-01: the served message, remediation and taxonomy summary for the profile-
    mismatch code name every kind of site that reports it. The sites are read from the two
    reporting modules' own source, so a new site fails here until the served text names it."""
    import re

    from boomi_mcp.compiler.process_ir.diagnostics import compiler_diagnostic_specs
    from boomi_mcp.errors import ERROR_TAXONOMY

    code = "PROCESS_IR_SEMANTIC_PROFILE_MISMATCH"
    compiler = _ROOT / "src" / "boomi_mcp" / "compiler" / "process_ir"
    sites = set()
    for module in (compiler / "semantic_validation" / "lineage.py", compiler / "connector_resolution.py"):
        lines = module.read_text(encoding="utf-8").splitlines()
        for index, line in enumerate(lines):
            if code in line or "mismatch(" in line or "_profile_failure(" in line:
                window = "\n".join(lines[max(0, index - 8): index + 8])
                sites.update(
                    (module.name, tail)
                    for tail in re.findall(r'"(?:\{0\})?(/[a-z_]+(?:/\{0\}/[a-z_]+)?)"', window))
    # Keyed by module (ARCH-184-r1-02): lineage's cache-fed declared-input call reports at
    # `/operation_ref`, a tail connector resolution already reported at, so a set of tails alone
    # could not see that a new rule had started raising the code.
    keywords = {
        ("lineage.py", "/map_ref"): "map",
        ("lineage.py", "/cache_ref"): "cache write",
        ("lineage.py", "/process_ref"): "call",
        ("lineage.py", "/source_values/{0}/profile_ref"): "profile source",
        ("lineage.py", "/operation_ref"): "declared input",
        ("connector_resolution.py", "/map_ref"): "map",
        ("connector_resolution.py", "/operation_ref"): "operation",
    }
    # Both directions: every reporting site is one this pin knows, and every word it requires
    # still stands for a site in the source.
    assert sites == set(keywords), sorted(sites)
    served = next(row for row in compiler_diagnostic_specs() if row["code"] == code)
    # Each served text on its own (QA-184-s1-r6-01): joined, a site named only by the
    # remediation passed for a message that omitted it.
    texts = {"message": served["message"], "remediation": served["remediation"],
             "summary": ERROR_TAXONOMY[code].summary}
    missing = sorted((name, site) for name, text in texts.items()
                     for site, word in keywords.items() if word not in text.lower())
    assert missing == [], missing
    assert "named only by reference" in served["remediation"]


def _rest_operation(key, method, response=None, request=None):
    """A REST operation spec authored like `_get`'s, declaring only the profiles it is given."""
    config = {"component_type": "connector-action", "connector_type": "rest",
              "operation_mode": "execute", "component_name": key, "connection_ref_key": "conn",
              "method": method, "path": "/admin/cdscm/api/v1/clients/1bdb1503-2807-4771-b1b7-8689be8f8e0a",
              "return_application_errors": True, "track_response": True}
    depends_on = ["conn"]
    if response is not None:
        config.update(response_profile_id="$ref:" + response, response_profile_type="json")
        depends_on.append(response)
    if request is not None:
        config.update(request_profile_id="$ref:" + request, request_profile_type="json")
        depends_on.append(request)
    return {"key": key, "type": "connector-action", "name": key, "action": "create",
            "depends_on": depends_on, "config": config}


def test_the_typed_route_judges_cache_content_at_every_typed_consumer():
    """ARCH-184-r1-01 and ARCH-184-r1-02, on the route a caller reaches.

    One leg stages documents in `c_p1`, which declares p1, and the next reads them back. A PATCH
    declaring input p2, after a map that wrote p1, is refused at its own `/operation_ref`. A profile
    source naming p1, after an undeclared GET's write, is refused at `/source_values/0/profile_ref`:
    the cache's declaration is not its content. Control: the same source after a GET that declares
    p1 compiles and is bound to a revision."""
    from unittest import mock

    from boomi_mcp.authoring.workflow import compile_authoring_request_v1
    from boomi_mcp.categories import integration_builder

    components = [
        {"key": "conn", "type": "connector-settings", "name": "Sandbox CDS Mock REST (no-auth)",
         "action": "create",
         "config": {"connector_type": "rest", "component_name": "Sandbox CDS Mock REST (no-auth)",
                    "base_url": "http://host.docker.internal:8081", "auth": "NONE"}},
        _profile("p1", "key"),
        _profile("p2", "other"),
        _rest_operation("op_get", "GET", response="p1"),
        _rest_operation("op_undeclared", "GET"),
        _rest_operation("op_patch", "PATCH", request="p2"),
        _map("m11", "p1", "p1", "key", "key"),
        _cache("c_p1", "p1", "key", "new"),
    ]
    get = _step("connector_call", operation_ref="$ref:op_get")
    source = _step("set_ddp", name="X", source_values=[
        {"value_type": "profile", "element_id": "3", "element_name": "key (Root/Object/key)",
         "profile_ref": "$ref:p1", "profile_type": "profile.json"}])

    def staged(stage_steps, consumer):
        legs = [{"steps": list(stage_steps), "terminal": _step("cache_put", cache_ref="$ref:c_p1")},
                {"steps": [_step("cache_get", cache_ref="$ref:c_p1"), consumer], "terminal": _step("stop")}]
        return AuthoringRequestV1.model_validate({"contract_version": "2", "intent": {
            "intent_kind": "process_ir", "integration_name": "cache_content",
            "units": [{"envelope": {"component_key": "root", "name": "root", "action": "create",
                                    "depends_on": [spec["key"] for spec in components]},
                       "process_ir": {"version": "1", "body": {"kind": "sequence", "steps": [
                           _step("branch", legs=legs)]}}}],
            "components": components, "conflict_policy": "reuse"}})

    def compiled(request):
        with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []):
            return compile_authoring_request_v1(request, boomi_client=mock.MagicMock(), profile="qa",
                                                account_id="qa_account")[0]

    def blocked_at(diagnostics, pointer):
        return any(item.code == "AUTHORING_COMPILE_BLOCKED" and item.path == pointer
                   and _MISMATCH in item.cause_codes for item in diagnostics)

    refused = (
        (staged([get, _step("map_ref", map_ref="$ref:m11")], _step("connector_call", operation_ref="$ref:op_patch")),
         "/body/steps/0/legs/1/steps/1/operation_ref"),
        (staged([_step("connector_call", operation_ref="$ref:op_undeclared")], source),
         "/body/steps/0/legs/1/steps/1/source_values/0/profile_ref"),
    )
    for request, pointer in refused:
        planned = plan_authoring_request_v1(request, profile="qa_profile", account_id="qa_account")[0]
        assert blocked_at(planned.errors, pointer), planned.errors
        with pytest.raises(Exception) as blocked:
            compiled(request)
        assert blocked_at(getattr(blocked.value, "diagnostics", ()), pointer), blocked.value
    assert compiled(staged([get], source)).revision_binding is not None


# ---------------------------------------------------------------------------
# QA round r7 (`agents/reports/2026-09-13-issue-184-stage2-r7.md`), correction batch 7
# ---------------------------------------------------------------------------


def test_a_component_id_has_one_canonical_spelling():
    """QA-184-s1-r7-01: the platform returns a component GUID in lowercase and reads it in any case,
    so a GUID is compared and sent lowercase; whitespace is not part of an id (SELF-184-02), and a
    blank value names nothing. Any other value is kept as written."""
    from boomi_mcp.categories.integration_builder import canonical_component_id

    upper = "675C81CF-41C8-4CDA-B650-537966A67A6E"
    assert canonical_component_id(" " + upper + " ") == upper.lower()
    assert canonical_component_id(upper.lower()) == upper.lower()
    assert canonical_component_id(" C-1 ") == "C-1"
    assert canonical_component_id("Not-A-Guid") == "Not-A-Guid"
    assert canonical_component_id("   ") is None
    assert canonical_component_id(None) is None
    assert canonical_component_id(5) is None


_GUID = "675c81cf-41c8-4cda-b650-537966a67a6e"


@pytest.mark.parametrize("family", ["map", "cache", "operation"])
def test_ids_differing_only_in_guid_case_name_one_component(family):
    """QA-184-s1-r7-01: two updates naming one existing component through two spellings of its GUID
    are two writes of ONE component, in every family the refusal covers; different GUIDs are not."""
    def update(key, component_id):
        if family == "map":
            return _spec(key, "transform.map", action="update", component_id=component_id,
                         source_profile_id="$ref:p1", target_profile_id="$ref:p1")
        if family == "cache":
            return _spec(key, "documentcache", action="update", component_id=component_id, profile_id="$ref:p1")
        return _spec(key, "connector-action", action="update", component_id=component_id, connector_type="rest",
                     operation_mode="execute", method="GET", response_profile_id="$ref:p1")

    with pytest.raises(ComponentWriteConflictError) as refused:
        build_symbol_table([_spec("p1", "profile.json"), update("a", _GUID), update("b", " " + _GUID.upper() + " ")])
    assert refused.value.conflicts == {_GUID: ("a", "b")}
    # CONTROL: a different GUID is a different component.
    build_symbol_table([_spec("p1", "profile.json"), update("a", _GUID),
                        update("b", "0370D8D8-2C63-42D7-AE11-9AA5BBF64262")])


def test_guid_case_spellings_are_refused_on_the_typed_plan_and_the_raw_route():
    """QA-184-s1-r7-01, on the routes a caller reaches: the typed plan reports the write conflict, and
    the raw route's pre-write refusal serves it, so nothing is written."""
    import types

    from boomi_mcp.authoring.connector_resolution_snapshot import build_connector_resolution_snapshot
    from boomi_mcp.categories import integration_builder

    typed = [_profile("p_a", "a1"), _cache("u1", "p_a", "a1", "create_by_id"), _cache("u2", "p_a", "a1", "create_by_id")]
    for item in typed[1:]:
        item["action"] = "update"
    typed[2]["component_id"] = _CACHE_ID.upper()
    request = AuthoringRequestV1.model_validate({"contract_version": "2", "intent": {
        "intent_kind": "process_ir", "integration_name": "guid_case",
        "units": [{"envelope": {"component_key": "root", "name": "root", "action": "create",
                                "depends_on": [spec["key"] for spec in typed]},
                   "process_ir": {"version": "1", "body": {"kind": "sequence", "steps": [
                       {"kind": "passthrough"}, {"kind": "stop"}]}}}],
        "components": typed, "conflict_policy": "reuse"}})
    result = plan_authoring_request_v1(request, profile="qa_profile", account_id="qa_account")[0]
    assert sorted((error.code, error.subject_id) for error in result.errors) == [(_CONFLICT, "u1"), (_CONFLICT, "u2")]

    components = [_spec("p1", "profile.json"),
                  _spec("u1", "documentcache", action="update", component_id=_GUID, profile_id="$ref:p1"),
                  _spec("u2", "documentcache", action="update", component_id=_GUID.upper(), profile_id="$ref:p1")]
    with pytest.raises(ComponentWriteConflictError) as raw:
        integration_builder._build_canonical_symbols(
            spec=types.SimpleNamespace(components=components, processes=()),
            resolution=build_connector_resolution_snapshot(components, declared={}),
            conflict_policy="reuse",
            existing_ids={},
        )
    assert integration_builder._pre_write_refusal(raw.value, failed_step="root")["error_code"] == _CONFLICT


def test_apply_sends_an_update_to_the_canonical_component_id():
    """QA-184-s1-r7-01: the platform refuses an update whose URL id differs in case from the
    component's own, so apply sends the update to the canonical id (measured by running apply
    offline; only execution is replaced)."""
    import copy
    from unittest import mock

    from boomi_mcp.categories import integration_builder

    config = {"dry_run": False, "conflict_policy": "reuse", "integration_spec": {"name": "canonical", "components": [
        {"key": "a", "type": "profile.json", "name": "a", "action": "update",
         "component_id": " " + _GUID.upper() + " ", "config": dict(_PROFILE_CONFIG)}]}}
    targets = []

    def execute(*args, **kwargs):
        targets.append(kwargs.get("target_id"))
        return {"_success": True, "component_id": kwargs.get("target_id"), "status": "updated"}

    with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []), \
            mock.patch.object(integration_builder, "_execute_component", side_effect=execute):
        result = integration_builder._apply_plan(mock.MagicMock(), "canonical_profile", copy.deepcopy(config))
    assert result.get("_success"), result
    assert targets == [_GUID]
    assert result["results"]["a"]["component_id"] == _GUID


def test_every_declared_component_id_reaches_the_builder_through_the_canonical_authority():
    """QA-184-s1-r7-01, the sibling sweep read from the builder's source: every place a spec's or a
    process envelope's declared id is bound, compared or sent goes through `canonical_component_id`.
    The one other use echoes what the author wrote into the served plan step."""
    import ast

    from boomi_mcp.categories import integration_builder

    tree = ast.parse(Path(integration_builder.__file__).read_text(encoding="utf-8"))
    parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
    uses = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Attribute) and node.attr == "component_id"
                and isinstance(node.value, ast.Name) and node.value.id in ("comp", "envelope")):
            continue
        parent = parents[node]
        if isinstance(parent, ast.Call) and getattr(parent.func, "id", None) == "_first_nonblank_str":
            parent = parents[parent]
        if isinstance(parent, ast.Call) and getattr(parent.func, "id", None) == "canonical_component_id":
            uses.append("canonical")
        elif isinstance(parent, ast.Dict):
            uses.append("echo")
        else:
            uses.append("raw line {0}".format(node.lineno))
    # Non-vacuity: the planner binding (both branches), the plan's update-target check, the apply
    # target, and a canonical root's plan and execute targets.
    assert sorted(uses) == ["canonical"] * 6 + ["echo"], uses


def test_every_symbol_is_described_by_its_own_spec():
    """Correction batch 9, as an invariant over the symbol model: every field of each symbol equals the same
    field with that spec alone in the table, so no spec's configuration describes another. Three references
    naming one operation nothing in the request writes keep their own facts, including the connection each
    names and the path-binding requirement the snapshot reads for each. The fields are read from the model,
    so a fact added later is covered."""
    from types import SimpleNamespace

    from boomi_mcp.authoring.workflow import _connector_metadata_from_components
    from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1

    operation_id = "66ff8c9e-9c83-48d7-8ec8-921783ced17a"
    base = [_spec("p1", "profile.json"), _spec("conn_a", "connector-settings", connector_type="rest"),
            _spec("conn_b", "connector-settings", connector_type="rest")]
    references = [
        _spec("op_get", "connector-action", component_id=operation_id, reference_only=True, connector_type="rest",
              operation_mode="execute", method="GET", connection_ref_key="conn_a", response_profile_id="$ref:p1"),
        _spec("op_alias", "connector-action", action="update", component_id=operation_id.upper(), connector_type="rest"),
        _spec("op_other", "connector-action", component_id=operation_id, reference_only=True, connector_type="rest",
              operation_mode="execute", method="POST", connection_ref_key="conn_b"),
    ]
    routes = {"op_get": "dynamic", "op_other": "static"}

    def lookup(key):
        if key not in routes:
            return None
        return SimpleNamespace(component_key=key, route_state=routes[key], family="rest",
                               listener_input_type=None, listener_request_profile=None)

    snapshot = SimpleNamespace(lookup=lookup)
    fields = sorted(set(ComponentSymbolV1.model_fields) - {"ref", "component_id"})

    def table(components):
        return {symbol.ref: symbol for symbol in build_symbol_table(
            components, connector_metadata=_connector_metadata_from_components(components),
            connector_resolution_snapshot=snapshot).symbols}

    combined = table(base + references)
    for reference in references:
        alone = table(base + [reference])["$ref:" + reference.key]
        assert {name: getattr(combined["$ref:" + reference.key], name) for name in fields} == {
            name: getattr(alone, name) for name in fields}, reference.key
    keys = ("op_get", "op_alias", "op_other")
    # Non-vacuity: the three name one component and still differ in their own facts.
    assert len({combined["$ref:" + key].bound_component_id for key in keys}) == 1
    assert [combined["$ref:" + key].action_type for key in keys] == ["GET", None, "POST"]
    assert [combined["$ref:" + key].connection_ref for key in ("op_get", "op_other")] == ["$ref:conn_a", "$ref:conn_b"]
    assert [combined["$ref:" + key].requires_path_binding for key in ("op_get", "op_other")] == [True, False]


def test_a_reference_names_its_own_connection():
    """Pre-commit verification of correction batch 7, under correction batch 9: the operation->connection edge
    is a fact of the STEP. The builder emits identical operation XML whichever connection is named, so each
    reference keeps the connection it names, and an operation the request writes is named by its writer alone."""
    from boomi_mcp.authoring.workflow import _connector_metadata_from_components
    from boomi_mcp.categories.components.builders.connector_builder import RestClientOperationBuilder

    config = {"component_type": "connector-action", "connector_type": "rest", "operation_mode": "execute",
              "component_name": "op", "method": "GET", "path": "/v1/things",
              "response_profile_id": "profile-guid-1", "response_profile_type": "json"}
    # The authority: what apply writes into the operation does not depend on the connection.
    assert (RestClientOperationBuilder().build(**dict(config, connection_ref_key="conn_a"))
            == RestClientOperationBuilder().build(**dict(config, connection_ref_key="conn_b")))

    operation_id = "66ff8c9e-9c83-48d7-8ec8-921783ced17a"
    base = [_spec("conn_a", "connector-settings", connector_type="rest"),
            _spec("conn_b", "connector-settings", connector_type="rest")]
    references = [
        _spec("op_a", "connector-action", component_id=operation_id, reference_only=True, connector_type="rest",
              operation_mode="execute", method="GET", connection_ref_key="conn_a"),
        _spec("op_b", "connector-action", component_id=operation_id.upper(), reference_only=True, connector_type="rest",
              operation_mode="execute", method="GET", connection_ref_key="conn_b"),
    ]
    symbols = {symbol.ref: symbol for symbol in build_symbol_table(
        base + references, connector_metadata=_connector_metadata_from_components(base + references)).symbols}
    assert [symbols["$ref:op_a"].connection_ref, symbols["$ref:op_b"].connection_ref] == ["$ref:conn_a", "$ref:conn_b"]
    writer = _spec("op", "connector-action", action="update", component_id=operation_id, connector_type="rest",
                   operation_mode="execute", method="GET", connection_ref_key="conn_a")
    with pytest.raises(ComponentWriteConflictError):
        build_symbol_table(base + references + [writer])


def test_a_listener_named_beside_its_writer_is_refused():
    """Stage-2 review round r8 (the cleared listener profile), under correction batch 9: a WSS listener update
    that omits its request profile clears it at apply, and no other spec may name that listener beside it,
    whichever case its GUID is spelled in."""
    listener_id = "66ff8c9e-9c83-48d7-8ec8-921783ced17a"
    writer = _spec("op", "connector-action", action="update", component_id=listener_id, connector_type="wss",
                   operation_mode="listen", input_type="singlejson")
    reference = _spec("op_ref", "connector-action", component_id=listener_id.upper(), reference_only=True,
                      connector_type="wss")
    with pytest.raises(ComponentWriteConflictError) as refused:
        build_symbol_table([writer, reference])
    assert refused.value.conflicts == {listener_id: ("op", "op_ref")}


def test_a_reference_only_spec_spelling_a_guid_in_upper_case_binds_the_component():
    """Pre-commit verification of correction batch 7, under correction batch 9: the planner binding's
    reference_only branch reads the canonical id. A reference_only update or create spelling the GUID in upper
    case, at the top level or in its config, beside an update of it names the written component twice. Two
    reference_only spellings of a component nothing in the request writes bind it once and are admitted."""
    upper = _GUID.upper()
    writer = _spec("w", "documentcache", action="update", component_id=_GUID, profile_id="$ref:p1")
    for reference in (
        _spec("r", "documentcache", action="update", component_id=upper, reference_only=True),
        _spec("r", "documentcache", component_id=upper, reference_only=True),
        IntegrationComponentSpec(key="r", type="documentcache", name="r",
                                 config={"reference_only": True, "component_id": upper}),
    ):
        with pytest.raises(ComponentWriteConflictError) as refused:
            build_symbol_table([_spec("p1", "profile.json"), writer, reference])
        assert refused.value.conflicts == {_GUID: ("r", "w")}
    symbols = {symbol.ref: symbol for symbol in build_symbol_table([
        _spec("r", "documentcache", component_id=upper, reference_only=True),
        IntegrationComponentSpec(key="s", type="documentcache", name="s",
                                 config={"reference_only": True, "component_id": _GUID}),
    ]).symbols}
    assert symbols["$ref:r"].bound_component_id == symbols["$ref:s"].bound_component_id == _GUID


def test_a_blank_update_id_names_nothing_at_plan_and_at_apply():
    """Pre-commit verification of correction batch 7: a blank top-level id names nothing, so an update
    naming no resolvable target plans `error_missing_target` and apply executes nothing (measured by
    running the plan and apply offline)."""
    import copy
    from unittest import mock

    from boomi_mcp.categories import integration_builder

    config = {"dry_run": False, "conflict_policy": "reuse", "integration_spec": {"name": "blank", "components": [
        {"key": "a", "type": "profile.json", "name": "a", "action": "update", "component_id": "   ",
         "config": dict(_PROFILE_CONFIG)}]}}
    calls = []
    with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []), \
            mock.patch.object(integration_builder, "_execute_component",
                              side_effect=lambda *a, **k: calls.append(k) or {"_success": True}):
        plan = integration_builder._build_plan(mock.MagicMock(), copy.deepcopy(config))
        result = integration_builder._apply_plan(mock.MagicMock(), "blank_profile", copy.deepcopy(config))
    assert [(step.get("key"), step.get("planned_action")) for step in plan.get("steps") or []] == [("a", "error_missing_target")]
    assert result.get("_success") is False and calls == []


def test_a_canonical_root_update_reads_its_target_by_the_canonical_id():
    """Pre-commit verification of correction batch 7: a canonical process root updated by an upper-case
    GUID reads and writes its live target by the canonical id, through the public dispatcher with only
    the network boundary faked (the #153 end-to-end harness). The live read is made to fail, so nothing
    is written."""
    from unittest.mock import MagicMock, patch

    import test_issue_153_canonical_apply_e2e as e2e
    from _m12_11_support import appliable_process_unit
    from boomi_mcp.categories.integration_builder import build_integration_action

    unit = appliable_process_unit(component_id=_GUID.upper())
    unit = unit.model_copy(update={"envelope": unit.envelope.model_copy(update={"action": "update"})})
    with patch(e2e._PAGINATE) as paginate:
        paginate.return_value = []
        payload = e2e._bound_payload(e2e.process_ir_request(units=(unit,)))
    reads = []

    def get_xml(_client, component_id, *_args, **_kwargs):
        reads.append(component_id)
        if isinstance(component_id, str) and component_id.strip().lower() == _GUID:
            raise RuntimeError("the live component cannot be read")
        return {"type": "connector-settings", "xml": e2e._LIVE_COMPONENT_XML}

    with patch(e2e._PAGINATE) as paginate, patch(e2e._EXECUTE) as execute, patch(e2e._GET_XML) as read:
        paginate.return_value = []
        execute.side_effect = lambda *a, **k: {"_success": True, "component_id": "cid-1"}
        read.side_effect = get_xml
        build_integration_action(MagicMock(), e2e._PROFILE, "apply", config={"authoring_request": payload, "dry_run": False})
    target_reads = [value for value in reads if isinstance(value, str) and value.strip().lower() == _GUID]
    assert target_reads and set(target_reads) == {_GUID}, reads


def test_a_connection_binding_spelling_one_guid_two_ways_names_one_component():
    """Pre-commit verification of correction batch 7 (the governance sibling): a connection binding's
    top-level and config ids are compared in canonical spelling, so two spellings of one GUID are one
    component. Two different ids are still a binding conflict. Asked of the governance check itself,
    which the typed plan runs before anything else about the binding."""
    from boomi_mcp.authoring.governance import _refuse_contradictory_identity
    from boomi_mcp.errors import GOVERNANCE_CONNECTION_BINDING_CONFLICT

    def binding(top_level, in_config):
        config = {"connector_type": "rest", "base_url": "https://x.invalid", "component_id": in_config}
        return IntegrationComponentSpec(key="conn", type="connector-settings", name="conn",
                                        component_id=top_level, config=config), config

    component, config = binding(_GUID, " " + _GUID.upper() + " ")
    _refuse_contradictory_identity(component, config, "conn")
    component, config = binding(_GUID, "0370D8D8-2C63-42D7-AE11-9AA5BBF64262")
    with pytest.raises(Exception) as refused:
        _refuse_contradictory_identity(component, config, "conn")
    assert getattr(refused.value, "code", None) == GOVERNANCE_CONNECTION_BINDING_CONFLICT


# ---------------------------------------------------------------------------
# Stage-2 review round r7 (`cdx-review.7pic9D`), correction batch 8
# ---------------------------------------------------------------------------


def test_the_round_r8_shapes_are_refused_before_any_fact_is_read():
    """Stage-2 review rounds r7 and r8 and QA round r9 (correction batch 9). Each shape named one existing
    component through a writer and a reference, and every rule for which spec describes it was wrong for one
    of them. The request is now refused before any fact is read:
    - a rename-only operation update beside a reference_only spec of the same operation;
    - a raw-XML operation update beside a reference declaring GET;
    - a listener update that clears its request profile beside a reference of the old listener.
    Alone, the reference keeps its own declared action, and a call through it is not refused as an
    unsupported action."""
    from boomi_mcp.authoring.workflow import _connector_metadata_from_components

    operation_id = "66ff8c9e-9c83-48d7-8ec8-921783ced17a"
    conn = _spec("conn", "connector-settings", connector_type="rest")
    reference = _spec("op_ref", "connector-action", component_id=operation_id, reference_only=True,
                      connector_type="rest", operation_mode="execute", method="GET", connection_ref_key="conn")
    writers = (
        _spec("op", "connector-action", action="update", component_id=operation_id, connector_type="rest",
              component_name="renamed"),
        _spec("op", "connector-action", action="update", component_id=operation_id, connector_type="rest",
              xml="<submitted/>"),
        _spec("op", "connector-action", action="update", component_id=operation_id, connector_type="wss",
              operation_mode="listen", input_type="singlejson"),
    )
    for writer in writers:
        with pytest.raises(ComponentWriteConflictError) as refused:
            build_symbol_table([conn, writer, reference])
        assert refused.value.conflicts == {operation_id: ("op", "op_ref")}
    alone = build_symbol_table([conn, reference], connector_metadata=_connector_metadata_from_components([conn, reference]))
    call = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "connector_call", "operation_ref": "$ref:op_ref"}, {"kind": "stop"}]}})
    codes = [item.code for item in validate_process_ir(call, alone).errors]
    assert "PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED" not in codes, codes


# ---------------------------------------------------------------------------
# Correction batch 10: a spec bound by NAME (QA-184-s1-r10-01)
# ---------------------------------------------------------------------------

_STORED = "stored cache"


def _named_candidates(name_to_id):
    """`_resolve_existing_components` answering by exact name, as the account does, recording each read."""
    calls = []

    def resolve(_client, comp):
        calls.append(comp.key)
        component_id = name_to_id.get(comp.name)
        return [{"component_id": component_id, "name": comp.name}] if component_id else []

    return resolve, calls


def _by_name_components():
    return [
        _spec("p1", "profile.json"),
        _spec("writer", "documentcache", action="update", component_id=_CACHE_ID, profile_id="$ref:p1"),
        IntegrationComponentSpec(key="by_name", type="documentcache", action="create", name=_STORED,
                                 config={"profile_id": "$ref:p1"}),
        IntegrationComponentSpec(key="reference_by_name", type="documentcache", action="create", name=_STORED,
                                 config={"reference_only": True}),
    ]


def _by_name_request_components():
    typed = [_profile("p_a", "a1"), _cache("u1", "p_a", "a1", "create_by_id"), _cache("u2", "p_a", "a1", "new")]
    typed[1]["action"] = "update"
    typed[2]["name"] = typed[2]["config"]["component_name"] = _STORED
    return typed


def test_a_name_bound_spec_beside_its_writer_is_refused_on_the_typed_plan():
    """QA-184-s1-r10-01 on the route a caller reaches: with the account, a cache named by name beside the update of
    that cache reports the write conflict for both specs; a name the account does not hold binds nothing."""
    from unittest import mock

    from boomi_mcp.categories import integration_builder

    typed = _by_name_request_components()
    request = AuthoringRequestV1.model_validate({"contract_version": "2", "intent": {
        "intent_kind": "process_ir", "integration_name": "by_name",
        "units": [{"envelope": {"component_key": "root", "name": "root", "action": "create",
                                "depends_on": [spec["key"] for spec in typed]},
                   "process_ir": {"version": "1", "body": {"kind": "sequence", "steps": [
                       {"kind": "passthrough"}, {"kind": "stop"}]}}}],
        "components": typed, "conflict_policy": "reuse"}})

    def conflicts(name_to_id):
        resolve, _calls = _named_candidates(name_to_id)
        with mock.patch.object(integration_builder, "_resolve_existing_components", resolve), \
                mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []):
            result = plan_authoring_request_v1(
                request, boomi_client=mock.MagicMock(), profile="qa_profile", account_id="qa_account")[0]
        return sorted((error.code, error.subject_id) for error in result.errors if error.code == _CONFLICT)

    assert conflicts({_STORED: _CACHE_ID}) == [(_CONFLICT, "u1"), (_CONFLICT, "u2")]
    assert conflicts({}) == []


def test_apply_binds_a_name_bound_spec_to_the_written_component_before_any_write():
    """QA-184-s1-r10-01 at apply: `_build_plan` resolves every spec with the account, and the canonical symbol table
    built from that plan, as `_apply_plan` builds it before its first write, refuses the conflict with its code."""
    import types
    from unittest import mock

    from boomi_mcp.authoring.connector_resolution_snapshot import build_connector_resolution_snapshot
    from boomi_mcp.categories import integration_builder

    raw = _by_name_request_components()
    config = {"conflict_policy": "reuse", "integration_spec": {"name": "by_name", "components": raw}}
    rows = [{"component_id": _CACHE_ID, "name": _STORED, "type": "documentcache"}]
    with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: [dict(row) for row in rows]):
        planned = integration_builder._build_plan(mock.MagicMock(), config)
    assert planned.get("_success"), planned
    existing_ids = {step["key"]: step["existing_component_id"] for step in planned["steps"]}
    assert existing_ids["u2"] == _CACHE_ID, existing_ids
    components = [IntegrationComponentSpec(**spec) for spec in raw]
    spec = types.SimpleNamespace(components=components, processes=())
    with pytest.raises(ComponentWriteConflictError) as refused:
        integration_builder._build_canonical_symbols(
            spec=spec, resolution=build_connector_resolution_snapshot(components, declared={}),
            conflict_policy="reuse", existing_ids=existing_ids)
    assert refused.value.conflicts == {_CACHE_ID: ("u1", "u2")}
    assert integration_builder._pre_write_refusal(refused.value, failed_step="root")["error_code"] == _CONFLICT
    # CONTROL: the same symbol table from apply's plan when the name matches nothing.
    with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []):
        unmatched = integration_builder._build_plan(mock.MagicMock(), config)
    integration_builder._build_canonical_symbols(
        spec=spec, resolution=build_connector_resolution_snapshot(components, declared={}),
        conflict_policy="reuse", existing_ids={step["key"]: step["existing_component_id"] for step in unmatched["steps"]})


def test_a_binding_the_account_answers_decides_identity_conflicts_and_the_written_configuration():
    """QA-184-s1-r10-01 and the pre-commit verification of correction batch 10. The route's binding map, as
    apply's component plan answers it, decides three things. The first is which component a spec bound by
    name is, which makes a write conflict beside the writer. The second is that a create reused by name
    contributes nothing from its discarded configuration. The third is that an update bound only by name
    writes that component. Without the map, only declared ids bind."""
    from boomi_mcp.categories import integration_builder

    components = _by_name_components()
    account = {"by_name": _CACHE_ID.upper(), "reference_by_name": _CACHE_ID}
    with pytest.raises(ComponentWriteConflictError) as refused:
        build_symbol_table(components, existing_ids=account)
    assert refused.value.conflicts == {_CACHE_ID: ("by_name", "reference_by_name", "writer")}
    with pytest.raises(ComponentWriteConflictError) as cloned:
        build_symbol_table(components, conflict_policy="clone", existing_ids=account)
    assert cloned.value.conflicts == {_CACHE_ID: ("reference_by_name", "writer")}
    build_symbol_table(components)  # the request alone cannot see a name match

    writerless = [component for component in components if component.key != "writer"]
    symbols = {symbol.ref: symbol for symbol in build_symbol_table(writerless, existing_ids=account).symbols}
    assert (symbols["$ref:by_name"].bound_component_id, symbols["$ref:by_name"].cache_profile_ref) == (_CACHE_ID, None)
    assert symbols["$ref:reference_by_name"].bound_component_id == _CACHE_ID
    by_name = next(component for component in components if component.key == "by_name")
    assert integration_builder.apply_writes_component_config(by_name, "reuse") is True
    assert integration_builder.apply_writes_component_config(by_name, "reuse", existing_ids=account) is False
    assert integration_builder.apply_writes_component_config(by_name, "clone", existing_ids=account) is True
    unmatched = build_symbol_table(writerless, existing_ids={"by_name": None, "reference_by_name": None}).symbols
    assert {symbol.ref: symbol.cache_profile_ref for symbol in unmatched}["$ref:by_name"] == "$ref:p1"

    update_by_name = IntegrationComponentSpec(key="update_by_name", type="documentcache", action="update",
                                              name=_STORED, config={"profile_id": "$ref:p1"})
    assert integration_builder.component_writes_existing(update_by_name) is False
    assert integration_builder.component_writes_existing(update_by_name, existing_ids={"update_by_name": _CACHE_ID})
    reference = _spec("reference", "documentcache", component_id=_CACHE_ID, reference_only=True)
    with pytest.raises(ComponentWriteConflictError) as by_name_writer:
        build_symbol_table([_spec("p1", "profile.json"), update_by_name, reference],
                           existing_ids={"update_by_name": _CACHE_ID})
    assert by_name_writer.value.conflicts == {_CACHE_ID: ("reference", "update_by_name")}


_CONN = {"key": "conn", "type": "connector-settings", "name": "probe conn", "action": "create",
         "config": {"connector_type": "rest", "component_name": "probe conn",
                    "base_url": "http://host.docker.internal:8081", "auth": "NONE"}}


def _named(spec, name=_STORED):
    spec["name"] = name
    spec["config"]["component_name"] = name.strip()
    return spec


def _route_request(components, ir=None):
    ir = ir or {"version": "1", "body": {"kind": "sequence", "steps": [{"kind": "passthrough"}, {"kind": "stop"}]}}
    return AuthoringRequestV1.model_validate({"contract_version": "2", "intent": {
        "intent_kind": "process_ir", "integration_name": "routes",
        "units": [{"envelope": {"component_key": "root", "name": "root", "action": "create",
                                "depends_on": [spec["key"] for spec in components]},
                   "process_ir": ir}],
        "components": components, "conflict_policy": "reuse"}})


def _writer_beside(second):
    writer = _cache("u1", "p_a", "a1", "create_by_id")
    writer["action"] = "update"
    return [_profile("p_a", "a1"), writer, second]


def _route_cases():
    padded = _named(_cache("u2", "p_a", "a1", "new"), " " + _STORED + " ")
    typecase = _named(_cache("u2", "p_a", "a1", "new"))
    typecase["type"] = "DocumentCache"
    wrapper = {"key": "u2", "type": "component", "name": _STORED, "action": "create",
               "config": {"type": "documentcache", "reference_only": True}}
    writer_by_name = _named(_cache("u1", "p_a", "a1", "new"))
    writer_by_name["action"] = "update"
    through_alias = [_CONN, _profile("p_client", "key"), _get("op_get1", "1bdb1503-2807-4771-b1b7-8689be8f8e0a"),
                     _profile("p_a", "a1"), _profile("p_b", "b2"), _map("m1", "p_client", "p_a", "key", "a1"),
                     _map("m2", "p_a", "p_b", "a1", "b2"),
                     _named(_cache("c_a", "p_a", "a1", "new")), _named(_cache("c_alias", "p_a", "a1", "new"))]
    mixed = [_CONN, _profile("p_client", "key"), _get("op_get1", "1bdb1503-2807-4771-b1b7-8689be8f8e0a"),
             _get("op_get2", "97ef6619-0f72-41bf-9b29-dba25de5f9da"), _profile("p_a", "a1"), _profile("p_b", "b2"),
             _map("m1", "p_client", "p_a", "key", "a1"), _map("m2", "p_a", "p_b", "a1", "b2"),
             _named(_cache("c_a", "p_a", "a1", "new")), _cache("c_alias", "p_client", "key", "reference_only")]
    return {
        "exact_name": (_route_request(_writer_beside(_named(_cache("u2", "p_a", "a1", "new")))), _CONFLICT),
        "padded_name": (_route_request(_writer_beside(padded)), _CONFLICT),
        "type_case": (_route_request(_writer_beside(typecase)), _CONFLICT),
        "wrapper": (_route_request(_writer_beside(wrapper)), _CONFLICT),
        "writer_by_name": (_route_request([_profile("p_a", "a1"), writer_by_name,
                                           _cache("c_ref", "p_a", "a1", "reference_only")]), _CONFLICT),
        "name_matches_nothing": (_route_request(_writer_beside(_named(_cache("u2", "p_a", "a1", "new"), "another"))),
                                 None),
        "writerless_through_alias": (_route_request(through_alias, _THROUGH_ALIAS), None),
        "writerless_mixed": (_route_request(mixed, _MIXED), _MISMATCH),
    }


@pytest.mark.parametrize("case", sorted(_route_cases()))
def test_plan_compile_and_apply_judge_one_binding(case):
    """Pre-commit verification of correction batch 10: the typed plan, compile, the raw apply and the typed
    apply read the one component plan apply builds with the account, so they agree on every spelling of a
    name binding the verification confirmed. The spellings are a padded name, a type in another case, a
    wrapper spec, an update bound by name, and requests with no writer at all. A refusal happens before any
    component is written. An admitted request passes the pre-write pass and reaches its writes. Offline: only
    the account's metadata listing and component execution are replaced."""
    import copy
    from unittest import mock

    from boomi_mcp.authoring.workflow import _normalize_intent, compile_authoring_request_v1
    from boomi_mcp.categories import integration_builder

    request, refusal = _route_cases()[case]
    rows = [{"component_id": _CACHE_ID, "name": _STORED, "type": "documentcache", "folder_name": "f"}]
    writes = []

    def execute(*args, **kwargs):
        comp = kwargs.get("comp") or (args[1] if len(args) > 1 else None)
        writes.append(getattr(comp, "key", None))
        target = kwargs.get("target_id")
        return {"_success": True, "component_id": target or "NEW-" + str(len(writes)),
                "status": "updated" if target else "created"}

    def account():
        return (mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: [dict(r) for r in rows]),
                mock.patch.object(integration_builder, "_execute_component", side_effect=execute))

    def applied(config):
        del writes[:]
        listing, executor = account()
        with listing, executor:
            try:
                result = integration_builder._apply_plan(mock.MagicMock(), "qa", copy.deepcopy(config))
            except Exception:  # noqa: BLE001 - past the pre-write pass the offline harness cannot create a process
                result = {"_success": False, "error_code": None}
        return ("refused", result.get("error_code")) if not writes else ("wrote",)

    listing, executor = account()
    with listing, executor:
        planned = plan_authoring_request_v1(request, boomi_client=mock.MagicMock(), profile="qa",
                                            account_id="qa_account")[0]
    plan_codes = {code for error in planned.errors for code in (error.code, *error.cause_codes)}
    raw = applied({"dry_run": False, "conflict_policy": "reuse",
                   "integration_spec": _normalize_intent(request).integration_spec.model_dump(mode="json")})
    if refusal is None:
        assert not planned.errors, plan_codes
        assert raw == ("wrote",), raw
        client = mock.MagicMock()
        listing, executor = account()
        with listing, executor:
            compiled = compile_authoring_request_v1(request, boomi_client=client, profile="qa",
                                                    account_id=integration_builder._client_account_id(client))[0]
        payload = request.model_dump(mode="json")
        payload["expected_capability_revision"] = compiled.revision_binding.capability_revision
        payload["expected_compile_hash"] = compiled.revision_binding.compile_hash
        assert applied({"dry_run": False, "authoring_request": payload}) == ("wrote",)
    else:
        assert refusal in plan_codes, plan_codes
        assert raw == ("refused", refusal), raw
        listing, executor = account()
        with listing, executor, pytest.raises(Exception) as blocked:
            compile_authoring_request_v1(request, boomi_client=mock.MagicMock(), profile="qa",
                                         account_id="qa_account")
        assert refusal in {code for diagnostic in getattr(blocked.value, "diagnostics", ())
                           for code in (diagnostic.code, *diagnostic.cause_codes)}, blocked.value


def test_every_identity_reading_takes_the_bindings_its_route_resolved():
    """Correction batch 10, the sibling sweep read from the source.

    - Every call of an identity reading passes the route's bindings (`existing_ids`): the binding itself, the
      reuse set, the declared bindings, whether an update writes an existing component, the write-conflict
      check, whether apply writes a spec's configuration, and the symbol table.
    - The exceptions are three modules. The recipe engine and the revision oracle touch no account by contract,
      and effect derivation holds no plan: for a create under `reuse` it already answers "may be substituted",
      whatever the binding.
    - The declared-only planner binding is read for its id by `_bound_existing_id` alone.
    - `apply_writes_component_config` reads only its `reference_only` flag.
    - Both account-holding routes build the map from their component plan."""
    import ast

    from boomi_mcp.categories import integration_builder

    src = Path(integration_builder.__file__).resolve().parents[1]
    readings = {"_bound_existing_id", "reused_keys_for_components", "declared_bindings_for_components",
                "component_writes_existing", "component_write_conflicts", "apply_writes_component_config",
                "build_symbol_table"}
    exempt = {"recipes/engine.py", "authoring/contract.py", "authoring/process_ir_effects.py"}

    def calls(path):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            called = node.func.id if isinstance(node.func, ast.Name) else getattr(node.func, "attr", None)
            owner = node
            while owner in parents and not isinstance(owner, (ast.FunctionDef, ast.AsyncFunctionDef)):
                owner = parents[owner]
            yield called, (owner.name if isinstance(owner, ast.FunctionDef) else "<module>"), node, parents.get(node)

    unbound, declared, planned, seen = [], set(), set(), 0
    for path in sorted(src.rglob("*.py")):
        rel = path.relative_to(src).as_posix()
        for called, owner, call, parent in calls(path):
            if called in readings:
                seen += 1
                passes = any(keyword.arg == "existing_ids" for keyword in call.keywords) or (
                    called == "_bound_existing_id" and len(call.args) >= 2)
                if not passes and rel not in exempt:
                    unbound.append("{0}:{1}:{2}".format(rel, owner, called))
            if called == "resolve_planner_binding" and any(k.arg == "declared_only" for k in call.keywords):
                declared.add((owner, parent.attr if isinstance(parent, ast.Attribute) else None))
            if called == "planned_existing_ids":
                planned.add(owner)
    assert unbound == [], unbound
    assert declared == {("_bound_existing_id", "existing_id"), ("apply_writes_component_config", "reference_only")}, declared
    assert planned >= {"_apply_plan", "plan_authoring_request_v1"}, planned
    assert seen > 20, seen


def test_an_unbuildable_component_plan_is_served_as_unjudged():
    """QA-184-s1-r11-01, QA-184-s1-r12-01 and the owner decision of 2026-09-14.

    With an account in hand, the component plan could not be built because the name listing failed. The owner's
    decision is that plan and compile keep working while the account is unreadable. So nothing blocks, and the
    plan serves one advisory naming every verdict that plan decides and nobody judged. That covers a single create
    under `fail`, whose collision only the account decides, as well as a writer beside a name.
    - The binding compiled then is compiled again at apply against the readable account, which refuses it with no
      write, because the account decides the name conflict the degraded plan could not see.
    - Without a client the plan says only that the lint did not run.
    - A working account serves neither advisory.
    Offline: only the name listing fails, and a lookup by id answers empty."""
    import copy
    from unittest import mock

    from boomi_mcp.authoring.workflow import compile_authoring_request_v1
    from boomi_mcp.categories import integration_builder

    def request(components, policy="reuse"):
        return AuthoringRequestV1.model_validate({"contract_version": "2", "intent": {
            "intent_kind": "process_ir", "integration_name": "unjudged",
            "units": [{"envelope": {"component_key": "root", "name": "root", "action": "create",
                                    "depends_on": [spec["key"] for spec in components]},
                       "process_ir": {"version": "1", "body": {"kind": "sequence", "steps": [
                           {"kind": "passthrough"}, {"kind": "stop"}]}}}],
            "components": components, "conflict_policy": policy}})

    writer = _cache("u1", "p_a", "a1", "create_by_id")
    writer["action"] = "update"
    beside_writer = request([_profile("p_a", "a1"), writer, _named(_cache("u2", "p_a", "a1", "new"))])
    single_under_fail = request([_profile("p_a", "a1"), _named(_cache("c_a", "p_a", "a1", "new"))], policy="fail")

    def unreadable(_client, _comp):
        raise ConnectionError("the component metadata listing failed")

    def component_plan(diagnostics):
        return [d for d in diagnostics if d.code == "AUTHORING_COMPILE_BLOCKED" and d.subject_kind == "component_plan"]

    def listing(resolver):
        return (mock.patch.object(integration_builder, "_resolve_existing_components", resolver),
                mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []))

    def planned(req, client=True, resolver=unreadable):
        names, by_id = listing(resolver)
        with names, by_id:
            return plan_authoring_request_v1(req, boomi_client=mock.MagicMock() if client else None,
                                             profile="qa", account_id="qa_account")[0]

    for req in (beside_writer, single_under_fail):
        served = planned(req)
        assert not component_plan(served.errors), served.errors
        (advisory,) = component_plan(served.warnings)
        assert advisory.severity == "advisory"
        for verdict in ("could not be built from the account", "which existing component each spec names",
                        "write conflicts", "ambiguous or colliding names", "profile facts",
                        "apply refuses a binding the account decides differently"):
            assert verdict in advisory.message, (verdict, advisory.message)

    # Without a client the lint simply did not run; a working account serves no component-plan advisory at all.
    (offline,) = component_plan(planned(beside_writer, client=False).warnings)
    assert "did not run" in offline.message and "could not be built" not in offline.message
    # (Its component-plan lint ran, so its own warnings may be served; what must be absent is the unjudged advisory.)
    assert not [d for d in component_plan(planned(beside_writer, resolver=lambda _client, _comp: []).warnings)
                if d.severity == "advisory"]

    # The guard the advisory names: the binding compiled while the listing failed is refused at apply once the
    # account answers, before any write.
    names, by_id = listing(unreadable)
    with names, by_id:
        compiled = compile_authoring_request_v1(beside_writer, boomi_client=mock.MagicMock(), profile="qa",
                                                account_id="qa_account")[0]
    payload = beside_writer.model_dump(mode="json")
    payload["expected_capability_revision"] = compiled.revision_binding.capability_revision
    payload["expected_compile_hash"] = compiled.revision_binding.compile_hash
    rows = [{"component_id": _CACHE_ID, "name": _STORED, "type": "documentcache", "folder_name": "f"}]
    writes = []
    with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: [dict(r) for r in rows]), \
            mock.patch.object(integration_builder, "_execute_component",
                              side_effect=lambda *a, **k: writes.append(k.get("target_id")) or {"_success": True}):
        applied = integration_builder._apply_plan(
            mock.MagicMock(), "qa", copy.deepcopy({"dry_run": False, "authoring_request": payload}))
    assert applied.get("_success") is False, applied
    assert writes == [], writes


def test_apply_refuses_a_binding_it_could_not_confirm_against_the_account():
    """QA-184-s1-r13-01. A typed apply confirms its binding by compiling again. By the owner decision of 2026-09-14
    that compile degrades when the component plan cannot be built from the account. So when the name listing failed
    only inside apply's own recompile, the recompile reproduced a hash compiled while the account was unreadable,
    and the write loop then bound from the account that answered again.
    - Now the recompile's unbuilt component plan refuses the apply before any binding is compared, with nothing
      written.
    - CONTROL: the same binding with a listing that never fails inside the recompile, and a healthy compile applied
      to a healthy account, both reach their writes.
    Offline: only the name listing, the metadata listing and component execution are replaced."""
    import copy
    import json
    from unittest import mock

    from boomi_mcp.authoring.workflow import compile_authoring_request_v1
    from boomi_mcp.categories import integration_builder

    request = _route_request([_profile("p_a", "a1"), _named(_cache("c_a", "p_a", "a1", "new"))])
    rows = [{"component_id": _CACHE_ID, "name": _STORED, "type": "documentcache", "folder_name": "f"}]
    real = integration_builder._resolve_existing_components
    client = mock.MagicMock()
    account_id = integration_builder._client_account_id(client)

    def in_recompile():
        frame = sys._getframe()
        while frame is not None:
            if frame.f_code.co_name == "preflight_typed_apply_v1":
                return True
            frame = frame.f_back
        return False

    def unreadable(_client, _comp):
        raise ConnectionError("the component metadata listing failed")

    def split(listed_client, comp):
        if in_recompile():
            raise ConnectionError("the component metadata listing failed during apply's recompile")
        return real(listed_client, comp)

    def listing(resolver):
        return (mock.patch.object(integration_builder, "_resolve_existing_components", resolver),
                mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: [dict(r) for r in rows]))

    def compiled_with(resolver):
        names, metadata = listing(resolver)
        with names, metadata:
            return compile_authoring_request_v1(request, boomi_client=client, profile="qa", account_id=account_id)[0]

    def applied_with(binding, resolver):
        payload = request.model_dump(mode="json")
        payload["expected_capability_revision"] = binding.revision_binding.capability_revision
        payload["expected_compile_hash"] = binding.revision_binding.compile_hash
        writes = []

        def execute(*args, **kwargs):
            comp = kwargs.get("comp") or (args[1] if len(args) > 1 else None)
            writes.append(getattr(comp, "key", None))
            target = kwargs.get("target_id")
            return {"_success": True, "component_id": target or "NEW-" + str(len(writes)),
                    "status": "updated" if target else "created"}

        names, metadata = listing(resolver)
        with names, metadata, mock.patch.object(integration_builder, "_execute_component", side_effect=execute):
            try:
                result = integration_builder._apply_plan(
                    mock.MagicMock(), "qa", copy.deepcopy({"dry_run": False, "authoring_request": payload}))
            except Exception:  # noqa: BLE001 - past the pre-write pass the offline harness cannot create a process
                result = {"_success": False, "error_code": None}
        return result, writes

    degraded = compiled_with(unreadable)
    healthy = compiled_with(real)
    assert degraded.revision_binding.compile_hash != healthy.revision_binding.compile_hash, (
        "the unreadable account must compile a different binding, or the split window proves nothing")

    refused, writes = applied_with(degraded, split)
    assert writes == [], writes
    assert refused.get("error_code") == "AUTHORING_APPLY_VALIDATION_REQUIRED", refused
    assert "could not build the component plan from the account" in json.dumps(refused), refused

    # CONTROL: the binding and account the refusal needs are real. A healthy compile applied to the healthy account
    # writes, so the refusal above is the unbuilt component plan and not the harness.
    assert applied_with(healthy, real)[1], "a healthy apply of a healthy binding must reach its writes"


def test_every_identity_decision_of_the_plan_route_moves_the_compiler_revision():
    """CDX-184-r11-01, the structural half. The revision oracle and its perturbations were hand-listed, and each
    batch that added an identity reading to the typed plan's route left it uncovered: the served compiler revision
    stayed identical while plan and compile acceptance changed (SELF-184-28, then CDX-184-r11-01).

    The set is now derived from the source: every function the validation route imports from the builder, in
    `build_symbol_table`, `_validate_processes` and `plan_authoring_request_v1`. Each must be reached by the
    revision oracle, called directly or through `build_symbol_table`, and perturbed in
    `test_the_revision_moves_with_component_identity_and_forwarding_behaviour`, which asserts that every
    perturbation moves the revision. The exceptions are the error class and `_client_account_id`, which names the
    account a resolution describes and decides no verdict."""
    import ast

    from boomi_mcp.categories import integration_builder

    src = Path(integration_builder.__file__).resolve().parents[1]
    route = {"authoring/workflow.py": {"_validate_processes", "plan_authoring_request_v1"},
             "recipes/materialization.py": {"build_symbol_table"}}
    exempt = {"ComponentWriteConflictError", "_client_account_id"}

    def functions(path):
        return {node.name: node for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
                if isinstance(node, ast.FunctionDef)}

    def builder_imports(node):
        return {alias.name for inner in ast.walk(node) if isinstance(inner, ast.ImportFrom)
                and (inner.module or "").endswith("integration_builder") for alias in inner.names}

    decisions = set()
    for path, names in route.items():
        defined = functions(src / path)
        for name in names:
            decisions |= builder_imports(defined[name])
    decisions -= exempt

    oracle = functions(src / "authoring" / "contract.py")["_component_identity_behaviour_oracle"]
    called = {inner.func.id for inner in ast.walk(oracle)
              if isinstance(inner, ast.Call) and isinstance(inner.func, ast.Name)}
    reached = builder_imports(oracle) & called
    if "build_symbol_table" in called:
        reached |= builder_imports(functions(src / "recipes" / "materialization.py")["build_symbol_table"])
    # Closed over the builder's own calls: a reading reached only through another builder function (the reuse
    # set, through the declared bindings) is still exercised by the oracle.
    builder = functions(src / "categories" / "integration_builder.py")
    frontier = set(reached)
    while frontier:
        name = frontier.pop()
        for inner in ast.walk(builder[name]) if name in builder else ():
            if isinstance(inner, ast.Call) and isinstance(inner.func, ast.Name) and inner.func.id in builder \
                    and inner.func.id not in reached:
                reached.add(inner.func.id)
                frontier.add(inner.func.id)
    assert decisions - reached == set(), sorted(decisions - reached)

    revision_test = functions(src.parents[1] / "tests" / "test_issue_184_child_entries.py")[
        "test_the_revision_moves_with_component_identity_and_forwarding_behaviour"]
    perturbed = {
        element.elts[1].value
        for element in ast.walk(revision_test)
        if isinstance(element, ast.Tuple) and len(element.elts) == 3
        and isinstance(element.elts[0], ast.Name) and element.elts[0].id == "integration_builder"
        and isinstance(element.elts[1], ast.Constant)
    }
    assert decisions - perturbed == set(), sorted(decisions - perturbed)
    # Non-vacuity: the derivation sees the reading batch 10 added, and the perturbation parse sees its rows.
    assert {"planned_existing_ids", "reused_keys_for_components"} <= decisions, decisions
    assert len(perturbed) >= 8, perturbed
