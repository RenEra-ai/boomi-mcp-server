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
