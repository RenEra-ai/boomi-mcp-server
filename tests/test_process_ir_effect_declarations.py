"""Issue #154 (M12.16) items 7-8 — the effect-declaration trust boundary.

The property under test throughout: a caller declaration supplies IDENTITY and
never CONTENT. Every acceptance case here shows the server deriving the effect
itself; every adversarial case shows a declaration that cannot establish state.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))
_SRC = str(_HERE.parent / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from boomi_mcp.authoring.process_ir_effects import (  # noqa: E402
    derive_map_effect,
    derive_subprocess_effect,
    resolve_process_ir_effect_declarations,
)
from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1, SymbolTableV1  # noqa: E402
from boomi_mcp.models.authoring_workflow import (  # noqa: E402
    ProcessIREffectDeclarationsV1,
    ProcessIRExternalWriterDeclarationV1,
    ProcessIRMapEffectDeclarationV1,
    ProcessIRScriptEffectDeclarationV1,
    ProcessIRStateEffectDeclarationV1,
    ProcessIRStateReferenceV1,
)
from boomi_mcp.models.integration_models import IntegrationComponentSpec  # noqa: E402
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402
from boomi_mcp.authoring.vetted_scripts import (  # noqa: E402
    VettedScriptContractV1,
    script_digest,
    vetted_script_registry_for_tests,
)

_SCRIPT = "def out = 1\n"


def _symbols():
    return SymbolTableV1(symbols=(
        ComponentSymbolV1(ref="$ref:MAP", component_id="m-1", component_type="transform.map"),
        ComponentSymbolV1(ref="$ref:CACHE", component_id="c-1", component_type="documentcache"),
        ComponentSymbolV1(ref="$ref:CHILD", component_id="p-1", component_type="process"),
        ComponentSymbolV1(ref="$ref:CONN", component_id="cn-1",
                          component_type="connector-settings", connector_type="rest"),
        ComponentSymbolV1(ref="$ref:GETOP", component_id="op-1",
                          component_type="connector-action", connector_type="rest",
                          action_type="GET", connection_ref="$ref:CONN"),
    ))


#: A map config the BUILDER accepts. Effect derivation asks the builder whether
#: it would build this config at all, so a fixture the builder refuses is opaque
#: for a reason that has nothing to do with the effect under test — which is
#: exactly how a test starts passing for the wrong reason.
#: A profile component the plan can INDEX. `validate_transform_map` resolves a
#: `$ref` profile through the generated-profile builder, so a stub with a bare
#: field list is not indexable and every map referencing it is refused for a
#: reason that has nothing to do with effects.
def _profile_component(key):
    return IntegrationComponentSpec(
        key=key, type="profile.json", action="create", name=key,
        config={
            "component_type": "profile.json",
            "profile_type": "json.generated",
            "component_name": key,
            "root": {"name": "root", "kind": "object", "children": [
                {"name": "a", "kind": "simple", "data_type": "character"}]},
        },
    )


def _plan_context():
    """``(depends_on, components_by_key)`` — the plan inputs the gate needs.

    Derivation asks `validate_transform_map`, which is what the PLAN asks, and
    that needs the referenced profile components to resolve a field index. A
    fixture without them is refused with a profile-index error, so a test built
    on one proves nothing about effects.
    """
    profiles = {key: _profile_component(key) for key in ("SP", "TP")}
    return ["SP", "TP"], profiles


def _derive(config, **kwargs):
    """`derive_map_effect` with the plan context supplied.

    Derivation asks the plan's own authority, which needs the referenced profile
    components; a bare call is refused for a profile-index reason and would make
    every assertion below pass for the wrong reason.
    """
    depends_on, components_by_key = _plan_context()
    kwargs.setdefault("depends_on", depends_on)
    kwargs.setdefault("components_by_key", components_by_key)
    return derive_map_effect(config, **kwargs)


def _valid_map_config(map_type="direct", **overrides):
    config = {
        "component_name": "M12.16 map",
        "map_type": map_type,
        "source_profile_id": "$ref:SP",
        "source_profile_type": "profile.json",
        "target_profile_id": "$ref:TP",
        "target_profile_type": "profile.json",
    }
    if map_type in ("direct",):
        config["field_mappings"] = [{"source_path": "root/a", "target_path": "root/a"}]
    config.update(overrides)
    return config


#: A function mapping the builder accepts and whose effect is empty. The
#: earlier fixtures used `function_mappings=[]`, which the builder refuses
#: outright ("must be a non-empty list") — so with derivation now gated on
#: builder acceptance those fixtures tested nothing.
def _accepted(function_type, **over):
    """A function mapping the BUILDER accepts for `function_type`.

    Each family has its own arity and parameter rules, and derivation is gated on
    builder acceptance — so a fixture refused for an unrelated shape reason tests
    nothing at all.
    """
    shapes = {
        "dynamic_process_property_set": {"inputs": ["root/a"], "parameters": {"property_name": "OUT"}},
        "dynamic_process_property_get": {"target_path": "root/a", "parameters": {"property_name": "P"}},
        "document_property_set": {"inputs": ["root/a"], "parameters": {"document_property_name": "D"}},
        "document_property_get": {"target_path": "root/a", "parameters": {"document_property_name": "D"}},
        "defined_process_property_get": {
            "target_path": "root/a",
            "parameters": {"process_property_component_id": "$ref:X",
                           "process_property_component_name": "P",
                           "process_property_key": "K"}},
        "defined_process_property_set": {
            "inputs": ["root/a"],
            "parameters": {"process_property_component_id": "$ref:X",
                           "process_property_component_name": "P",
                           "process_property_key": "K"}},
        "sequential_value": {"target_path": "root/a", "parameters": {"key_name": "K"}},
        "uppercase": {"inputs": ["root/a"], "target_path": "root/a", "parameters": {}},
    }
    mapping = {"function_type": function_type}
    mapping.update(shapes.get(function_type, {"inputs": ["root/a"], "target_path": "root/a", "parameters": {}}))
    for key, value in over.items():
        if key == "parameters":
            mapping["parameters"] = {**mapping.get("parameters", {}), **value}
        else:
            mapping[key] = value
    return mapping


def _noop_mapping():
    return _accepted("dynamic_process_property_get",
                     parameters={"property_name": "IGNORED", "default_value": "d"})


def _fn(function_type, parameters=None, **extra):
    """One function mapping the builder accepts for `function_type`."""
    mapping = {"function_type": function_type, "parameters": dict(parameters or {})}
    mapping.update(extra)
    return mapping


def _map_component(function_mappings, **over):
    """The map spec, declaring the profiles the plan must resolve."""
    kwargs = dict(
        key="MAP", type="transform.map", depends_on=["SP", "TP"],
        config=_valid_map_config("function", function_mappings=function_mappings),
    )
    kwargs.update(over)
    return IntegrationComponentSpec(**kwargs)


def _components(*specs):
    """A spec list the resolver can resolve — the map plus its profiles.

    The resolver builds its `components_by_key` from this list, and the plan
    authority needs the referenced profiles to resolve a field index. Omitting
    them makes every map inert for a profile reason.
    """
    return [*specs, *_plan_context()[1].values()]


def _effect(reads=(), writes=(), replay_safe=False):
    return ProcessIRStateEffectDeclarationV1(
        reads=tuple(ProcessIRStateReferenceV1(scope=s, name=n) for s, n in reads),
        writes=tuple(ProcessIRStateReferenceV1(scope=s, name=n) for s, n in writes),
        replay_safe=replay_safe,
    )


def _root_with_map():
    return parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "source", "connection_ref": "$ref:CONN", "operation_ref": "$ref:GETOP"},
        {"kind": "map_ref", "map_ref": "$ref:MAP"},
        {"kind": "return_documents"},
    ]}})


def _root_with_script():
    return parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "source", "connection_ref": "$ref:CONN", "operation_ref": "$ref:GETOP"},
        {"kind": "data_process", "steps": [
            {"operation": "custom_scripting", "language": "groovy2", "script": _SCRIPT}]},
        {"kind": "return_documents"},
    ]}})


def _root_with_external_cache_read():
    return parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "source", "connection_ref": "$ref:CONN", "operation_ref": "$ref:GETOP"},
        {"kind": "cache_get", "cache_ref": "$ref:CACHE", "external_writer": True},
        {"kind": "return_documents"},
    ]}})


# ---------------------------------------------------------------------------
# omitted declarations are the pre-#154 path exactly
# ---------------------------------------------------------------------------


def test_omitted_declarations_yield_no_capabilities_at_all():
    roots = [("p", _root_with_map())]
    resolution = resolve_process_ir_effect_declarations(roots, None, _symbols(), [])
    assert resolution.ok
    assert resolution.capabilities_by_root == {"p": None}


def test_an_empty_envelope_normalises_to_omitted_on_the_request():
    from boomi_mcp.models.authoring_workflow import AuthoringRequestV1
    assert ProcessIREffectDeclarationsV1().is_empty()
    # and the request-level validator turns that into None, so no key can enter
    # the normalised payload and rotate an existing plan hash.
    assert "effect_declarations" in AuthoringRequestV1.model_fields


# ---------------------------------------------------------------------------
# map: content comes from inspecting the component, never from the declaration
# ---------------------------------------------------------------------------


def test_map_effect_is_derived_from_the_function_registry():
    derived = _derive(_valid_map_config("function", function_mappings=[
        _accepted("dynamic_process_property_set", parameters={"property_name": "OUT"}),
        _accepted("uppercase"),
    ]))
    assert derived == ((), (("dpp", "OUT"),), True)


def test_a_map_with_one_unannotated_function_is_wholly_opaque():
    """Partial knowledge is never promoted to a complete effect."""
    derived = _derive(_valid_map_config("function", function_mappings=[
        _accepted("dynamic_process_property_set", parameters={"property_name": "OUT"}),
        _accepted("defined_process_property_get", parameters={"process_property_component_id": "$ref:X", "property_name": "P"}),
    ]))
    assert derived is None


def test_an_unknown_function_family_makes_the_map_opaque():
    assert _derive(_valid_map_config(
        "function", function_mappings=[_accepted("not_a_real_family")])) is None


def test_a_sequential_value_map_is_derivable_but_not_replay_safe():
    reads, writes, replay_safe = _derive(_valid_map_config(
        "function",
        function_mappings=[_accepted("sequential_value", parameters={"key_name": "K"})]))
    assert (reads, writes) == ((), ())
    assert replay_safe is False


def test_a_matching_map_declaration_is_accepted_and_bound_to_its_root():
    roots = [("p", _root_with_map())]
    declarations = ProcessIREffectDeclarationsV1(map_effects=(
        ProcessIRMapEffectDeclarationV1(
            map_ref="$ref:MAP", effect=_effect(writes=[("dpp", "OUT")], replay_safe=True)),
    ))
    components = _components(_map_component([
        _accepted("dynamic_process_property_set", parameters={"property_name": "OUT"})]))
    resolution = resolve_process_ir_effect_declarations(
        roots, declarations, _symbols(), components, conflict_policy="fail")
    assert resolution.ok, resolution.findings
    capabilities = resolution.capabilities_by_root["p"]
    assert capabilities.map_effect("$ref:MAP").writes == (("dpp", "OUT"),)


def test_a_forged_map_declaration_is_rejected_rather_than_believed():
    """The caller claims a write the component does not make."""
    roots = [("p", _root_with_map())]
    declarations = ProcessIREffectDeclarationsV1(map_effects=(
        ProcessIRMapEffectDeclarationV1(
            map_ref="$ref:MAP", effect=_effect(writes=[("dpp", "SOMETHING_ELSE")], replay_safe=True)),
    ))
    components = _components(_map_component([
        _accepted("dynamic_process_property_set", parameters={"property_name": "OUT"})]))
    resolution = resolve_process_ir_effect_declarations(
        roots, declarations, _symbols(), components, conflict_policy="fail")
    assert not resolution.ok
    assert resolution.findings[0].reason == "content-mismatch"
    assert resolution.capabilities_by_root["p"] is None


def test_a_map_declaration_naming_an_unmentioned_ref_is_unbound():
    roots = [("p", _root_with_script())]  # no map anywhere
    declarations = ProcessIREffectDeclarationsV1(map_effects=(
        ProcessIRMapEffectDeclarationV1(map_ref="$ref:MAP", effect=_effect()),))
    resolution = resolve_process_ir_effect_declarations(
        roots, declarations, _symbols(), [_map_component([])])
    assert [f.reason for f in resolution.findings] == ["unbound"]


def test_a_map_declaration_naming_a_non_map_component_is_rejected():
    roots = [("p", _root_with_map())]
    symbols = SymbolTableV1(symbols=(
        ComponentSymbolV1(ref="$ref:MAP", component_id="x", component_type="documentcache"),))
    declarations = ProcessIREffectDeclarationsV1(map_effects=(
        ProcessIRMapEffectDeclarationV1(map_ref="$ref:MAP", effect=_effect()),))
    resolution = resolve_process_ir_effect_declarations(roots, declarations, symbols, [])
    assert [f.reason for f in resolution.findings] == ["unresolved-or-wrong-type"]


# ---------------------------------------------------------------------------
# script: the registry is the only content authority
# ---------------------------------------------------------------------------


def test_a_registry_backed_script_is_accepted():
    registry = vetted_script_registry_for_tests(VettedScriptContractV1(
        "groovy2", _SCRIPT, writes=(("dpp", "OUT"),), replay_safe=True,
        rationale="test fixture"))
    declarations = ProcessIREffectDeclarationsV1(script_effects=(
        ProcessIRScriptEffectDeclarationV1(
            language="groovy2", source_sha256="sha256:" + script_digest(_SCRIPT),
            effect=_effect(writes=[("dpp", "OUT")], replay_safe=True)),))
    resolution = resolve_process_ir_effect_declarations(
        [("p", _root_with_script())], declarations, _symbols(), [], script_registry=registry)
    assert resolution.ok, resolution.findings
    capabilities = resolution.capabilities_by_root["p"]
    assert capabilities.script_effect("groovy2", _SCRIPT).writes == (("dpp", "OUT"),)


def test_an_exact_but_UNREGISTERED_script_is_inert_not_trusted():
    """The adversarial case the acceptance criteria name explicitly.

    The digest matches, so the server knows exactly which script this is — and
    still has no authority for what it does. Nothing is established and nothing
    is rejected.
    """
    declarations = ProcessIREffectDeclarationsV1(script_effects=(
        ProcessIRScriptEffectDeclarationV1(
            language="groovy2", source_sha256="sha256:" + script_digest(_SCRIPT),
            effect=_effect(writes=[("dpp", "OUT")], replay_safe=True)),))
    resolution = resolve_process_ir_effect_declarations(
        [("p", _root_with_script())], declarations, _symbols(), [],
        script_registry=vetted_script_registry_for_tests())
    assert resolution.ok, resolution.findings
    assert resolution.inert == ("/effect_declarations/script_effects/0",)
    assert resolution.capabilities_by_root["p"].script_effects == ()
    assert resolution.capabilities_by_root["p"].script_effect("groovy2", _SCRIPT) is None


def test_a_mismatched_digest_cannot_bind():
    other = "def out = 2\n"
    assert script_digest(other) != script_digest(_SCRIPT)
    declarations = ProcessIREffectDeclarationsV1(script_effects=(
        ProcessIRScriptEffectDeclarationV1(
            language="groovy2", source_sha256="sha256:" + script_digest(other),
            effect=_effect()),))
    resolution = resolve_process_ir_effect_declarations(
        [("p", _root_with_script())], declarations, _symbols(), [])
    assert [f.reason for f in resolution.findings] == ["unbound-or-digest-mismatch"]


def test_the_registry_key_is_recomputed_not_taken_from_the_caller():
    """One byte of source changes the key, so an old declaration cannot bind."""
    registry = vetted_script_registry_for_tests(
        VettedScriptContractV1("groovy2", _SCRIPT, writes=(("dpp", "OUT"),), replay_safe=True))
    mutated = _SCRIPT + " \n"
    assert script_digest(mutated) != script_digest(_SCRIPT)
    from boomi_mcp.authoring.vetted_scripts import lookup_vetted_script
    assert lookup_vetted_script("groovy2", _SCRIPT, registry) is not None
    assert lookup_vetted_script("groovy2", mutated, registry) is None


# ---------------------------------------------------------------------------
# external writer: an assertion that can never establish state
# ---------------------------------------------------------------------------


def test_an_external_writer_contract_carries_no_effect_payload_at_all():
    assert set(ProcessIRExternalWriterDeclarationV1.model_fields) == {"cache_ref"}


def test_the_external_writer_flag_governs_the_EFFECT_not_the_IDENTITY():
    """The four-case truth table, as the design plan states it.

    Identity is "a cache_get in this root names this cache". The authored flag
    decides whether the missing-writer error may downgrade — not whether the
    declaration is about a real thing.

    The first implementation gated IDENTITY on the flag, so an unflagged but
    matching declaration was rejected as unbound. That is a different answer from
    the design's: such a declaration is VALID and simply establishes nothing.
    Turning "this proves nothing" into "your payload is invalid" is the same
    overreach as trusting an unverified claim, pointed the other way — and the
    delta-scoped gate could not see it, because it confirmed the implementation
    against itself rather than against the plan.
    """
    declarations = ProcessIREffectDeclarationsV1(external_writers=(
        ProcessIRExternalWriterDeclarationV1(cache_ref="$ref:CACHE"),))

    # (1) flag authored + declaration -> the contract binds
    flagged = resolve_process_ir_effect_declarations(
        [("p", _root_with_external_cache_read())], declarations, _symbols(), [])
    assert flagged.ok, flagged.findings
    assert flagged.capabilities_by_root["p"].external_writers[0].cache_ref == "$ref:CACHE"
    assert flagged.inert == ()

    # (2) cache_get present but UNFLAGGED -> valid, and INERT rather than rejected
    unflagged = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "source", "connection_ref": "$ref:CONN", "operation_ref": "$ref:GETOP"},
        {"kind": "cache_get", "cache_ref": "$ref:CACHE"},
        {"kind": "return_documents"}]}})
    inert = resolve_process_ir_effect_declarations(
        [("p", unflagged)], declarations, _symbols(), [])
    assert inert.ok, inert.findings
    assert inert.inert == ("/effect_declarations/external_writers/0",)
    assert inert.capabilities_by_root["p"].external_writers == ()

    # (3) flag authored, NO declaration -> nothing established
    none_supplied = resolve_process_ir_effect_declarations(
        [("p", _root_with_external_cache_read())], None, _symbols(), [])
    assert none_supplied.capabilities_by_root["p"] is None

    # (4) no cache_get names this cache at all -> genuinely unbound
    no_read = resolve_process_ir_effect_declarations(
        [("p", _root_with_map())], declarations, _symbols(), [])
    assert [f.reason for f in no_read.findings] == ["unbound"]


def test_an_external_writer_never_adds_a_cache_write_to_state():
    declarations = ProcessIREffectDeclarationsV1(external_writers=(
        ProcessIRExternalWriterDeclarationV1(cache_ref="$ref:CACHE"),))
    resolution = resolve_process_ir_effect_declarations(
        [("p", _root_with_external_cache_read())], declarations, _symbols(), [])
    capabilities = resolution.capabilities_by_root["p"]
    assert capabilities.map_effects == ()
    assert capabilities.script_effects == ()
    assert capabilities.subprocess_summaries == ()
    # it registers ONLY as the assumption flag
    assert capabilities.writes_cache_externally("$ref:CACHE") is True


# ---------------------------------------------------------------------------
# per-root partitioning and the no-forwarding boundary
# ---------------------------------------------------------------------------


def test_capabilities_are_partitioned_per_root():
    """A declaration binds only the roots that actually mention it."""
    roots = [("with_map", _root_with_map()), ("without_map", _root_with_external_cache_read())]
    declarations = ProcessIREffectDeclarationsV1(map_effects=(
        ProcessIRMapEffectDeclarationV1(
            map_ref="$ref:MAP", effect=_effect(writes=[("dpp", "OUT")], replay_safe=True)),))
    components = _components(_map_component([
        _accepted("dynamic_process_property_set", parameters={"property_name": "OUT"})]))
    resolution = resolve_process_ir_effect_declarations(
        roots, declarations, _symbols(), components, conflict_policy="fail")
    assert resolution.ok, resolution.findings
    assert len(resolution.capabilities_by_root["with_map"].map_effects) == 1
    assert resolution.capabilities_by_root["without_map"].map_effects == ()


def test_the_public_declaration_object_never_becomes_the_compiler_context():
    """Type-level proof that nothing is forwarded verbatim.

    This is why the public models do NOT share the compiler's class names: if
    they did, an isinstance check here would pass for either object and the
    assertion would be vacuous.
    """
    from boomi_mcp.compiler.process_ir.semantic_validation.contracts import (
        MapEffectContractV1 as InternalMap,
        ProcessIRValidationCapabilitiesV1,
    )
    roots = [("p", _root_with_map())]
    declarations = ProcessIREffectDeclarationsV1(map_effects=(
        ProcessIRMapEffectDeclarationV1(
            map_ref="$ref:MAP", effect=_effect(writes=[("dpp", "OUT")], replay_safe=True)),))
    components = _components(_map_component([
        _accepted("dynamic_process_property_set", parameters={"property_name": "OUT"})]))
    resolution = resolve_process_ir_effect_declarations(
        roots, declarations, _symbols(), components, conflict_policy="fail")
    capabilities = resolution.capabilities_by_root["p"]
    assert isinstance(capabilities, ProcessIRValidationCapabilitiesV1)
    assert InternalMap is not ProcessIRMapEffectDeclarationV1
    for row in capabilities.map_effects:
        assert isinstance(row, InternalMap)
        assert not isinstance(row, ProcessIRMapEffectDeclarationV1)
        assert row is not declarations.map_effects[0]


def test_one_bad_declaration_withholds_the_whole_context():
    """No partially trusted context: its contents would depend on which
    declaration happened to fail."""
    roots = [("p", _root_with_map())]
    declarations = ProcessIREffectDeclarationsV1(
        map_effects=(ProcessIRMapEffectDeclarationV1(
            map_ref="$ref:MAP", effect=_effect(writes=[("dpp", "OUT")], replay_safe=True)),),
        external_writers=(ProcessIRExternalWriterDeclarationV1(cache_ref="$ref:CACHE"),),
    )
    components = _components(_map_component([
        _accepted("dynamic_process_property_set", parameters={"property_name": "OUT"})]))
    resolution = resolve_process_ir_effect_declarations(
        roots, declarations, _symbols(), components, conflict_policy="fail")
    assert not resolution.ok
    assert resolution.capabilities_by_root["p"] is None


# ---------------------------------------------------------------------------
# findings carry no authored values
# ---------------------------------------------------------------------------


def test_findings_are_value_free():
    """A planted canary must not survive into any finding."""
    canary = "CANARY-sk_live_154"
    roots = [("p", _root_with_map())]
    declarations = ProcessIREffectDeclarationsV1(map_effects=(
        ProcessIRMapEffectDeclarationV1(
            map_ref="$ref:MAP", effect=_effect(writes=[("dpp", canary)])),))
    components = _components(_map_component([
        _accepted("dynamic_process_property_set", parameters={"property_name": canary})]))
    # force a mismatch so a finding is produced at all
    declarations = ProcessIREffectDeclarationsV1(map_effects=(
        ProcessIRMapEffectDeclarationV1(
            map_ref="$ref:MAP", effect=_effect(writes=[("dpp", "OTHER")])),))
    resolution = resolve_process_ir_effect_declarations(
        roots, declarations, _symbols(), components, conflict_policy="fail")
    assert resolution.findings
    blob = " ".join(f.code + f.path + f.reason for f in resolution.findings)
    assert canary not in blob, blob


# ---------------------------------------------------------------------------
# subprocess derivation
# ---------------------------------------------------------------------------


def test_subprocess_must_writes_exclude_a_branch_local_write():
    """A write inside one Decision arm is not a write on every normal exit."""
    child = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "source", "connection_ref": "$ref:CONN", "operation_ref": "$ref:GETOP"},
        {"kind": "set_dpp", "name": "SPINE",
         "source_values": [{"value_type": "static", "value": "v"}]},
        {"kind": "decision", "label": "d", "comparison": "equals",
         "left": {"value_type": "static", "static_value": "a"},
         "right": {"value_type": "static", "static_value": "b"},
         "true_arm": {"steps": [
             {"kind": "set_dpp", "name": "ARM_ONLY",
              "source_values": [{"value_type": "static", "value": "v"}]}],
             "terminal": {"kind": "stop"}},
         "false_arm": {"steps": [], "terminal": {"kind": "stop"}}},
    ]}})
    reads, must_writes, _replay = derive_subprocess_effect(child)
    names = {name for _scope, name in must_writes}
    assert "SPINE" in names
    assert "ARM_ONLY" not in names, must_writes


def test_a_reference_only_child_is_inert():
    """No authored child root to inspect, so no content authority."""
    root = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "process_call", "process_ref": "$ref:CHILD"}]}})
    from boomi_mcp.models.authoring_workflow import ProcessIRSubprocessEffectDeclarationV1
    declarations = ProcessIREffectDeclarationsV1(subprocess_effects=(
        ProcessIRSubprocessEffectDeclarationV1(process_ref="$ref:CHILD", effect=_effect()),))
    resolution = resolve_process_ir_effect_declarations(
        [("p", root)], declarations, _symbols(), [])
    assert resolution.ok, resolution.findings
    assert resolution.inert == ("/effect_declarations/subprocess_effects/0",)
    assert resolution.capabilities_by_root["p"].subprocess_summaries == ()


# ---------------------------------------------------------------------------
# item 10 — the boundary is machine-visible, not just implemented
# ---------------------------------------------------------------------------


def test_the_served_contract_states_where_each_effect_content_comes_from():
    from boomi_mcp.authoring.process_ir_projection import (
        build_process_ir_authoring_entries,
        reset_process_ir_authoring_cache,
    )

    reset_process_ir_authoring_cache()
    served = {
        entry.contract_entry_id: entry
        for entry in build_process_ir_authoring_entries()
        if entry.contract_entry_id.startswith("semantic_rule.effect.")
    }
    assert set(served) == {
        "semantic_rule.effect.declaration_boundary",
        "semantic_rule.effect.map_inspection",
        "semantic_rule.effect.script_registry",
        "semantic_rule.effect.subprocess_inspection",
        "semantic_rule.effect.external_writer",
    }
    # the two facts a caller most needs, stated in served text rather than only
    # in behaviour
    assert "inert" in served["semantic_rule.effect.script_registry"].summary
    assert "never establishes" in served["semantic_rule.effect.external_writer"].summary
    # item 9's recorded decision is served, not merely commented
    assert "cache_get is the" in served["semantic_rule.effect.external_writer"].summary


def test_every_declaration_family_has_exactly_one_served_authority_row():
    """A family with no row would be a trust boundary nobody published."""
    from boomi_mcp.authoring.process_ir_effects import effect_authority_rows

    rows = dict(effect_authority_rows())
    for field in ProcessIREffectDeclarationsV1.model_fields:
        assert field in rows, field
    assert rows["external_writers"].startswith("caller-assertion")
    assert rows["map_effects"].startswith("server-inspection")
    assert rows["script_effects"].startswith("server-registry")


def test_the_public_schema_exposes_the_declarations_and_hides_the_internal_context():
    from boomi_mcp.models.authoring_workflow import AuthoringRequestV1

    schema = AuthoringRequestV1.model_json_schema()
    blob = str(schema)
    assert "effect_declarations" in schema["properties"]
    assert "ProcessIREffectDeclarationsV1" in blob
    # the compiler's own trusted-context model is NOT public API
    assert "ProcessIRValidationCapabilitiesV1" not in blob


def test_the_effect_authorities_are_in_the_capability_revision():
    """Re-annotating a function family must move the served revision.

    Otherwise a caller could hold a binding across a change in which strict
    findings their declaration is able to silence.
    """
    from boomi_mcp.authoring.contract import _effect_authority_payload

    payload = _effect_authority_payload()
    assert len(payload["map_function_effects"]) == 20
    names = {row[0] for row in payload["map_function_effects"]}
    assert "dynamic_process_property_set" in names
    # the production vetted registry really is empty at this HEAD
    assert payload["vetted_scripts"] == []


def test_an_unannotated_function_family_would_fail_closed():
    """The non-vacuity witness for the OPAQUE default.

    The probe must be an EXISTING family with its annotation stripped, not an
    invented name: derivation is gated on the builder accepting the config, and a
    family the registry does not know is also one the builder refuses — so an
    invented probe would go opaque for a reason that has nothing to do with the
    annotation, and the witness would pass vacuously.
    """
    import dataclasses
    from types import MappingProxyType

    from boomi_mcp.categories.components.builders import map_function_registry as reg

    real = reg.FUNCTION_FAMILIES["uppercase"]
    assert real.effect_kind == "pure"
    stripped = dataclasses.replace(real, effect_kind=None)
    assert stripped.effect_kind is None  # the mutation took effect

    mapping = _accepted("uppercase")
    # CONTROL: annotated, this family derives.
    assert _derive(
        _valid_map_config("function", function_mappings=[mapping])
    ) == ((), (), True)

    patched = dict(reg.FUNCTION_FAMILIES)
    patched["uppercase"] = stripped
    original = reg.FUNCTION_FAMILIES
    reg.FUNCTION_FAMILIES = MappingProxyType(patched)
    try:
        # ...and unannotated it is OPAQUE rather than silently pure.
        assert _derive(
            _valid_map_config("function", function_mappings=[mapping])
        ) is None
    finally:
        reg.FUNCTION_FAMILIES = original


# ---------------------------------------------------------------------------
# QA-154-r1-05: the canonical payload must not gain a key for an absent field
# ---------------------------------------------------------------------------


def test_effect_declarations_stay_out_of_the_canonical_payload_when_absent():
    """An `effect_declarations: null` key would rotate every existing hash.

    Nothing pinned this before (QA-154-r1-05): `_normalized_payload` was
    referenced by zero test files, so the protection was load-bearing and
    unwitnessed.
    """
    import sys as _sys
    from pathlib import Path as _Path

    _support = str(_Path(__file__).resolve().parent)
    if _support not in _sys.path:
        _sys.path.insert(0, _support)
    from _m12_11_support import process_ir_request

    from boomi_mcp.authoring.workflow import _normalize_intent, _normalized_payload

    request = process_ir_request()
    assert request.effect_declarations is None
    payload = _normalized_payload(_normalize_intent(request), request)
    assert "effect_declarations" not in payload, sorted(payload)


def test_a_supplied_declaration_DOES_enter_the_canonical_payload():
    """The control: the key is omitted because the field is absent, not because
    the payload never carries it."""
    import sys as _sys
    from pathlib import Path as _Path

    _support = str(_Path(__file__).resolve().parent)
    if _support not in _sys.path:
        _sys.path.insert(0, _support)
    from _m12_11_support import process_ir_request

    from boomi_mcp.authoring.workflow import _normalize_intent, _normalized_payload

    declarations = ProcessIREffectDeclarationsV1(external_writers=(
        ProcessIRExternalWriterDeclarationV1(cache_ref="$ref:CACHE"),))
    request = process_ir_request(effect_declarations=declarations)
    assert request.effect_declarations is not None
    payload = _normalized_payload(_normalize_intent(request), request)
    assert "effect_declarations" in payload


def test_an_empty_envelope_leaves_the_payload_untouched():
    """Empty normalises to None on the request, so the payload cannot gain a key."""
    import sys as _sys
    from pathlib import Path as _Path

    _support = str(_Path(__file__).resolve().parent)
    if _support not in _sys.path:
        _sys.path.insert(0, _support)
    from _m12_11_support import process_ir_request

    from boomi_mcp.authoring.workflow import _normalize_intent, _normalized_payload

    request = process_ir_request(effect_declarations=ProcessIREffectDeclarationsV1())
    assert request.effect_declarations is None
    payload = _normalized_payload(_normalize_intent(request), request)
    assert "effect_declarations" not in payload


# ---------------------------------------------------------------------------
# QA-154-r1-02: an exception's own registered codes, not its class name
# ---------------------------------------------------------------------------


def test_a_compile_error_serves_its_registered_codes_not_its_class_name():
    """`ProcessIRCompileError` carries `diagnostics` whose `code` IS the answer.

    Serving `"ProcessIRCompileError"` discarded it — the same pair QA-153-r16-01
    closed for pydantic (an exception CLASS treated as provenance), recurring on
    a different exception type.
    """
    from boomi_mcp.authoring.workflow import _cause_codes_for
    from boomi_mcp.compiler.process_ir.diagnostics import (
        CompilerDiagnostic,
        ProcessIRCompileError,
    )

    exc = ProcessIRCompileError([
        CompilerDiagnostic(
            code="PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING",
            phase="semantic_lowering", path="/body/steps/1",
            node_identity="", message="m", remediation="r", internal_node_id="n1",
        ),
    ])
    assert _cause_codes_for(exc) == ("PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING",)
    assert "ProcessIRCompileError" not in _cause_codes_for(exc)


def test_repeated_codes_are_reported_once_in_order():
    from boomi_mcp.authoring.workflow import _cause_codes_for
    from boomi_mcp.compiler.process_ir.diagnostics import (
        CompilerDiagnostic,
        ProcessIRCompileError,
    )

    def diag(code, path):
        return CompilerDiagnostic(
            code=code, phase="semantic_lowering", path=path, node_identity="",
            message="m", remediation="r", internal_node_id="n",
        )

    exc = ProcessIRCompileError([
        diag("CODE_A", "/a"), diag("CODE_B", "/b"), diag("CODE_A", "/c"),
    ])
    assert _cause_codes_for(exc) == ("CODE_A", "CODE_B")


def test_an_exception_carrying_no_diagnostics_still_falls_back_to_its_class():
    """The fallback is preserved — this widens what is reported, never what is
    refused."""
    from boomi_mcp.authoring.workflow import _cause_codes_for

    class _Bare(Exception):
        pass

    assert _cause_codes_for(_Bare()) == ("_Bare",)


# ---------------------------------------------------------------------------
# Codex P1: effects must describe the artifact that will EXECUTE
# ---------------------------------------------------------------------------


def test_a_reference_only_map_is_opaque_not_pure():
    """The hole this closed.

    A reference-only component carries no map fields at all. Reading that as
    "direct, therefore pure and replay-safe" let an arbitrary live map — never
    inspected, not version-bound — be established as touching no process state,
    which could suppress a real retry-safety or lineage error.
    """
    assert _derive({}) is None
    assert _derive({"component_id": "some-live-map"}) is None
    # ...and the control: a config that DOES say what it is still derives.
    assert _derive(_valid_map_config("direct")) == ((), (), True)


def test_a_create_under_reuse_is_opaque_because_the_plan_may_substitute_it():
    roots = [("p", _root_with_map())]
    declarations = ProcessIREffectDeclarationsV1(map_effects=(
        ProcessIRMapEffectDeclarationV1(
            map_ref="$ref:MAP", effect=_effect(writes=[("dpp", "OUT")], replay_safe=True)),))
    components = _components(_map_component([
        _accepted("dynamic_process_property_set", parameters={"property_name": "OUT"})]))

    reuse = resolve_process_ir_effect_declarations(
        roots, declarations, _symbols(), components, conflict_policy="reuse")
    assert reuse.ok, reuse.findings
    assert reuse.inert == ("/effect_declarations/map_effects/0",)
    assert reuse.capabilities_by_root["p"].map_effects == ()

    # CONTROL: the identical request under a policy that really creates it.
    fail = resolve_process_ir_effect_declarations(
        roots, declarations, _symbols(), components, conflict_policy="fail")
    assert fail.ok and len(fail.capabilities_by_root["p"].map_effects) == 1


def test_an_update_is_not_substitutable_even_under_reuse():
    """An update's config IS applied to the named component."""
    from boomi_mcp.authoring.process_ir_effects import _may_be_substituted

    create = IntegrationComponentSpec(key="MAP", type="transform.map", action="create", config={})
    update = IntegrationComponentSpec(
        key="MAP", type="transform.map", action="update", component_id="live-1", config={})
    assert _may_be_substituted(create, "reuse") is True
    assert _may_be_substituted(create, "fail") is False
    assert _may_be_substituted(update, "reuse") is False


def test_the_function_lookup_is_the_builders_own():
    """A padded, upper-cased family name the builder accepts must not read as
    unknown here — two spellings of one lookup rule is the duplicate-authority
    defect even when the divergence happens to fail closed."""
    from boomi_mcp.categories.components.builders.map_function_registry import (
        get_function_family,
    )

    assert get_function_family("  SEQUENTIAL_VALUE  ") is not None
    derived = _derive(_valid_map_config("function", function_mappings=[
        dict(_accepted("sequential_value"), function_type="  SEQUENTIAL_VALUE  ")]))
    assert derived == ((), (), False), derived


def test_a_defaulted_property_get_records_no_strict_read():
    """A contract read carries no has-default flag and lineage treats every one
    as strict, so recording a defaulted read would fail a flow that runs fine."""
    defaulted = _derive(_valid_map_config("function", function_mappings=[
        _accepted("dynamic_process_property_get",
                  parameters={"property_name": "P", "default_value": "fallback"})]))
    assert defaulted == ((), (), True), defaulted
    # CONTROL: without the default it IS a read, so the omission is about the
    # default rather than about the derivation never recording reads.
    plain = _derive(_valid_map_config("function", function_mappings=[
        _accepted("dynamic_process_property_get", parameters={"property_name": "P"})]))
    assert plain == ((("dpp", "P"),), (), True), plain


# ---------------------------------------------------------------------------
# QA-154-r2-03: pin the CALL SITE, not only the callee
# ---------------------------------------------------------------------------


def _drive_compile_watching_materialization(monkeypatch, drop_argument):
    """Compile a fixture request and record what the call site handed onward.

    The resolver is replaced with one that returns a KNOWN context for every
    root, so the test does not depend on the fixture plan carrying a bindable
    cache — the subject is the THREADING from resolver to call site to callee,
    which is exactly what QA's mutant severed.
    """
    import sys as _sys
    from pathlib import Path as _Path

    _support = str(_Path(__file__).resolve().parent)
    if _support not in _sys.path:
        _sys.path.insert(0, _support)
    from _m12_11_support import MutationSpy, process_ir_request

    from boomi_mcp.authoring import process_ir_effects as _effects
    from boomi_mcp.authoring import process_materialization as _pm
    from boomi_mcp.authoring import workflow as _workflow
    from boomi_mcp.compiler.process_ir.semantic_validation.contracts import (
        ProcessIRValidationCapabilitiesV1,
    )

    MutationSpy().install(monkeypatch)
    sentinel = ProcessIRValidationCapabilitiesV1()

    def fake_resolve(process_roots, declarations, symbols, components=(), **kwargs):
        return _effects.EffectResolutionV1(
            {key: sentinel for key, _ir in process_roots}, (), ()
        )

    monkeypatch.setattr(_effects, "resolve_process_ir_effect_declarations", fake_resolve)

    seen = []
    real = _pm.build_materialization_plan

    def watcher(**kwargs):
        if drop_argument:
            kwargs.pop("capabilities", None)
        seen.append(kwargs.get("capabilities"))
        return real(**kwargs)

    monkeypatch.setattr(_pm, "build_materialization_plan", watcher)
    try:
        _workflow.compile_authoring_request_v1(
            process_ir_request(), profile="qa_profile", account_id="qa_account"
        )
    except Exception:
        pass
    return seen, sentinel


def test_the_compile_path_hands_the_resolved_context_to_materialization(monkeypatch):
    """Pin the CALL SITE, not only the callee.

    Both earlier tests targeted `build_materialization_plan` itself, so deleting
    `(effect_capabilities or {}).get(component_key)` at its call site in
    `build_artifact_descriptors` left the suite green while the external-writer
    compile failed again — QA measured 4041 tests green under that mutant. A
    callee that accepts a parameter nobody passes is not a fix.
    """
    seen, sentinel = _drive_compile_watching_materialization(monkeypatch, drop_argument=False)
    assert seen, "build_materialization_plan was never called — the pin is vacuous"
    assert all(item is sentinel for item in seen), seen


def test_the_call_site_pin_fails_when_the_argument_is_dropped(monkeypatch):
    """CONTROL: reproduce QA's mutant and prove the pin above discriminates."""
    seen, _sentinel = _drive_compile_watching_materialization(monkeypatch, drop_argument=True)
    assert seen, "the mutant harness never fired — the control is vacuous"
    assert all(item is None for item in seen), seen


# ---------------------------------------------------------------------------
# QA-154-r2-01: reference_only resolves to a reuse INDEPENDENT of the policy
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("policy", ["reuse", "clone", "fail"])
def test_a_reference_only_component_is_substitutable_under_every_policy(policy):
    """`component_materialization_mode` checks `reference_only` BEFORE `action`,
    and the builder resolves it to a reuse independent of `conflict_policy`.

    The first version of `_may_be_substituted` re-derived that rule as "update is
    safe, otherwise ask the policy" and missed this case entirely, so a
    `{reference_only: true, map_type: "direct"}` spec derived a pure,
    replay-safe effect for a component nobody had read.
    """
    from boomi_mcp.authoring.process_ir_effects import _may_be_substituted

    spec = IntegrationComponentSpec(
        key="MAP", type="transform.map", action="create",
        config={"reference_only": True, "map_type": "direct"},
    )
    assert _may_be_substituted(spec, policy) is True
    # ...and the QA reproduction derives nothing rather than a pure effect
    assert _derive(spec.config, substitutable=True) is None


def test_substitutability_uses_the_materialization_authority_not_a_copy():
    """The rule is ASKED of `component_materialization_mode`, and the constants
    are imported rather than re-typed.

    Both halves were defects in turn: the rule was re-derived and missed
    `reference_only`, then the first fix compared against the literal `"reuse"`
    while the constant is `"reuse_reference"` — so it matched nothing and changed
    nothing.
    """
    from boomi_mcp.recipes.materialization import _REUSE, component_materialization_mode

    assert _REUSE != "reuse", "the literal and the constant differ — that was the bug"
    spec = IntegrationComponentSpec(
        key="MAP", type="transform.map", action="create",
        config={"reference_only": True},
    )
    assert component_materialization_mode(spec) == _REUSE


@pytest.mark.parametrize(
    "action,config,policy,expected",
    [
        ("create", {}, "reuse", True),    # may collide and be reused
        ("create", {}, "clone", False),   # writes a suffixed NEW component
        ("create", {}, "fail", False),    # refuses on collision
        ("update", {}, "reuse", False),   # the config IS applied
    ],
)
def test_the_policy_overlay_is_the_part_the_authority_does_not_model(
    action, config, policy, expected
):
    from boomi_mcp.authoring.process_ir_effects import _may_be_substituted

    spec = IntegrationComponentSpec(
        key="MAP", type="transform.map", action=action,
        component_id="live-1" if action == "update" else None, config=config,
    )
    assert _may_be_substituted(spec, policy) is expected


# ---------------------------------------------------------------------------
# Codex round 2 P1: the map-type vocabulary is the builders', not a copy
# ---------------------------------------------------------------------------


def test_every_function_map_alias_the_builder_supports_is_derivable():
    """`map_function` is a supported alias, and the hand-written list omitted it.

    A declaration on such a map went silently INERT, so legitimate writes could
    not satisfy lineage and an impure family inside a retry region lost its
    `replay_safe=False` — a retry-safety ERROR degraded to a non-blocking
    opaque-effect warning.
    """
    from boomi_mcp.categories.components.builders.map_builder import MapFunctionBuilder

    impure = [_accepted("sequential_value")]
    assert MapFunctionBuilder.SUPPORTED_MAP_TYPES, "no aliases — the test would be vacuous"
    for alias in MapFunctionBuilder.SUPPORTED_MAP_TYPES:
        derived = _derive(_valid_map_config(alias, function_mappings=impure))
        assert derived == ((), (), False), (alias, derived)


def test_every_direct_map_type_the_builder_supports_is_pure():
    from boomi_mcp.categories.components.builders.map_builder import DirectMapBuilder

    assert DirectMapBuilder.SUPPORTED_MAP_TYPES
    for alias in DirectMapBuilder.SUPPORTED_MAP_TYPES:
        assert _derive(_valid_map_config(alias)) == ((), (), True), alias


def test_a_script_map_is_opaque_because_its_authority_is_the_registry():
    """`MapScriptBuilder`'s types are deliberately absent from the derivation.

    A script map's effect depends on what its embedded scripts do, and the only
    authority for that is the vetted registry. Inspecting the map config would
    establish nothing, so falling through to opaque is the correct answer rather
    than a gap.
    """
    from boomi_mcp.categories.components.builders.map_builder import MapScriptBuilder

    assert MapScriptBuilder.SUPPORTED_MAP_TYPES
    for alias in MapScriptBuilder.SUPPORTED_MAP_TYPES:
        assert _derive(_valid_map_config(alias)) is None, alias


def test_the_vocabulary_is_read_from_the_builders_not_restated():
    """NON-VACUITY WITNESS.

    Narrows a builder's supported types and proves the derivation follows. A
    hand-written copy would keep deriving the removed alias, which is exactly how
    the original list drifted in both directions at once.
    """
    from boomi_mcp.categories.components.builders import map_builder as mb

    original = mb.MapFunctionBuilder.SUPPORTED_MAP_TYPES
    assert "map_function" in original
    try:
        mb.MapFunctionBuilder.SUPPORTED_MAP_TYPES = ("function",)
        assert "map_function" not in mb.MapFunctionBuilder.SUPPORTED_MAP_TYPES
        assert _derive(_valid_map_config("map_function", function_mappings=[_noop_mapping()])) is None
        # CONTROL: the alias that remains still derives, so the None above is
        # about the narrowing rather than about the derivation breaking.
        assert _derive(_valid_map_config("function", function_mappings=[_noop_mapping()])) == ((), (), True)
    finally:
        mb.MapFunctionBuilder.SUPPORTED_MAP_TYPES = original
    assert _derive(_valid_map_config("map_function", function_mappings=[_noop_mapping()])) == ((), (), True)


def test_a_map_type_no_builder_supports_is_opaque():
    """`profile` was in the hand-written list and is supported by nothing."""
    from boomi_mcp.categories.components.builders.map_builder import (
        DirectMapBuilder,
        MapFunctionBuilder,
        MapScriptBuilder,
    )

    known = (
        set(DirectMapBuilder.SUPPORTED_MAP_TYPES)
        | set(MapFunctionBuilder.SUPPORTED_MAP_TYPES)
        | set(MapScriptBuilder.SUPPORTED_MAP_TYPES)
    )
    assert "profile" not in known
    assert _derive(_valid_map_config("profile")) is None


# ---------------------------------------------------------------------------
# QA-154-r3-01/-02/-04: the direct branch is pinned too, and both vocabularies
# ---------------------------------------------------------------------------


def test_a_direct_map_carrying_function_mappings_is_opaque_not_pure():
    """QA-154-r3-01. Deciding `direct` before looking at the config turned a
    REFUSED declaration into an AGREED one.

    `{map_type: "direct", function_mappings: [sequential_value]}` derived
    `((), (), True)` — pure and replay-safe — where the previous tree derived
    `replay_safe=False`. That is the mirror of the defect that motivated deriving
    the vocabulary, and worse in kind: it agreed silently instead of warning.
    """
    impure = [_accepted("sequential_value")]
    assert _derive(_valid_map_config("direct", function_mappings=impure)) is None
    # CONTROL: a well-formed direct map still derives, so the None above is about
    # the rejected key rather than about `direct` having stopped working.
    assert _derive(_valid_map_config("direct")) == ((), (), True)


def test_every_direct_reject_key_makes_a_direct_map_opaque():
    """SIBLING SWEEP, read off the builder's own table rather than enumerated.

    `function_mappings` is one of six keys a direct map may not carry; fixing only
    the one QA reported would leave the same hole under five other spellings.
    """
    from boomi_mcp.categories.components.builders.map_builder import (
        _DIRECT_ONLY_REJECT_KEYS,
    )

    assert _DIRECT_ONLY_REJECT_KEYS, "no reject keys — the sweep would be vacuous"
    for key in _DIRECT_ONLY_REJECT_KEYS:
        assert _derive(_valid_map_config("direct", **{key: ["anything"]})) is None, key


def test_the_direct_vocabulary_is_read_from_its_builder_too(monkeypatch):
    """QA-154-r3-02. The first witness narrowed only the FUNCTION vocabulary, so
    hand-copying the DIRECT one back survived the affected file and the full
    suite — a hole exactly one mutation wide.
    """
    from boomi_mcp.categories.components.builders import map_builder as mb

    original = mb.DirectMapBuilder.SUPPORTED_MAP_TYPES
    assert "direct" in original
    try:
        mb.DirectMapBuilder.SUPPORTED_MAP_TYPES = ()
        assert "direct" not in mb.DirectMapBuilder.SUPPORTED_MAP_TYPES
        assert _derive(_valid_map_config("direct")) is None
        # CONTROL: the function vocabulary is untouched and still derives, so the
        # None above is about the narrowing rather than a wholesale break.
        assert _derive(_valid_map_config("function", function_mappings=[_noop_mapping()])) == ((), (), True)
    finally:
        mb.DirectMapBuilder.SUPPORTED_MAP_TYPES = original
    assert _derive(_valid_map_config("direct")) == ((), (), True)


@pytest.mark.parametrize("bad", [["direct"], {"a": 1}, {"direct"}, 3, None])
def test_a_non_string_map_type_is_opaque_and_does_not_crash(bad):
    """QA-154-r3-04. An authored `map_type` need not be hashable.

    `["direct"]` raised `TypeError: unhashable type` straight out of the tool
    with `error_code: None` and no machine code at all — a caller learned nothing
    from a value they had supplied.
    """
    assert _derive(_valid_map_config(bad, function_mappings=[_noop_mapping()])) is None


def test_a_non_string_map_type_survives_the_whole_resolver():
    """The crash reached the tool, so the guard is asserted at that level too."""
    spec = IntegrationComponentSpec(
        key="MAP", type="transform.map", action="create",
        config={"map_type": ["direct"], "function_mappings": []},
    )
    declarations = ProcessIREffectDeclarationsV1(map_effects=(
        ProcessIRMapEffectDeclarationV1(map_ref="$ref:MAP", effect=_effect()),))
    resolution = resolve_process_ir_effect_declarations(
        [("p", _root_with_map())], declarations, _symbols(), [spec],
        conflict_policy="fail")
    # INERT, not a crash and not a false agreement.
    assert resolution.ok, resolution.findings
    assert resolution.inert == ("/effect_declarations/map_effects/0",)


def test_a_raw_xml_map_is_opaque_whatever_its_structured_fields_say():
    """Codex round 3 P2. The raw-XML escape hatch bypasses the structured
    builder entirely, so the emitted bytes are `config["xml"]` and the
    structured fields alongside it never run.

    Deriving from them would let a matching declaration describe content the
    server never inspected — the slice's recurring defect in its purest form.
    """
    structured = [_accepted("dynamic_process_property_set", parameters={"property_name": "OUT"})]
    # CONTROL: without the raw XML this config DOES derive, so the None below is
    # about the escape hatch rather than about the mappings being unreadable.
    assert _derive(
        _valid_map_config("map_function", function_mappings=structured)
    ) == ((), (("dpp", "OUT"),), True)
    assert _derive(
        _valid_map_config("map_function", xml="<Map/>", function_mappings=structured)
    ) is None
    # ...and a direct map carrying raw XML is opaque for the same reason.
    assert _derive(_valid_map_config("direct", xml="<Map/>")) is None


def test_the_raw_xml_bypass_matches_the_builder_predicate():
    """The builder's condition is a TRUTHY check on `xml`, not a presence check.

    Asserted behaviourally at both ends so the two cannot drift into disagreeing
    about an empty string.
    """
    from boomi_mcp.categories.integration_builder import _resolve_preservation_policy
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    comp = IntegrationComponentSpec(key="MAP", type="transform.map", config={})
    assert _resolve_preservation_policy(comp, {"xml": "<Map/>"}) is None
    # an EMPTY xml is falsy for the builder, and must be falsy here too
    assert _derive(_valid_map_config("direct", xml="")) == ((), (), True)


# ---------------------------------------------------------------------------
# QA-154-r4-01: all FOUR route-class tables, asked rather than modelled
# ---------------------------------------------------------------------------


def _all_route_class_keys():
    from boomi_mcp.categories.components.builders import map_builder as mb

    return {
        "raw_xml": set(mb._RAW_XML_REJECT_KEYS),
        "direct_only": set(mb._DIRECT_ONLY_REJECT_KEYS),
        "function_builder": set(mb._FUNCTION_BUILDER_REJECT_KEYS),
        "script_builder": set(mb._SCRIPT_BUILDER_REJECT_KEYS),
    }


def test_derivation_requires_builder_ACCEPTANCE_across_every_reject_table():
    """QA-154-r4-01 / r5-02, rewritten.

    The first version of this test gated all 72 cells behind
    `if _route_is_rejected(config)` — a call to the very predicate under test — so
    it passed whatever that predicate said: 68 of 72 assertions executed at HEAD,
    16 under a one-table mutant, 0 under another, green every time.

    The expectation is now computed from the BUILDER, independently: for every key
    of every route-class table, whatever the builder says about the config is what
    derivation must do. No cell is skipped.
    """
    from boomi_mcp.categories.components.builders.map_builder import MAP_BUILDERS

    tables = _all_route_class_keys()
    assert all(tables.values()), tables
    checked = 0
    for table, keys in tables.items():
        for key in sorted(keys):
            for map_type in ("direct", "function", "map_function"):
                base = _valid_map_config(map_type)
                if map_type != "direct":
                    base["function_mappings"] = [_noop_mapping()]
                config = dict(base, **{key: ["v"]})
                builder = MAP_BUILDERS[("transform.map", map_type)]
                accepted = builder.validate_config(dict(config)) is None
                derived = _derive(config)
                assert (derived is not None) is accepted, (table, key, map_type, derived)
                checked += 1
    assert checked == sum(len(k) for k in tables.values()) * 3, checked


def test_a_secret_shaped_key_cannot_mask_a_route_violation():
    """QA-154-r5-01. Every map builder runs a deep secret-shaped-key scan BEFORE
    its route tables, so reading only the FIRST error let a route violation
    accompanied by a secret-shaped key answer with the secret code — and the
    effect was trusted.

    Asking for ACCEPTANCE rather than for a particular error code removes the
    order dependence entirely.
    """
    base = _valid_map_config("direct")
    plain = dict(base, xslt=["v"])
    masked = dict(base, xslt=["v"], token="sekrit")
    nested = dict(base, xslt=["v"],
                  field_mappings=[{"source_path": "x", "target_path": "y", "password": "p"}])
    assert _derive(dict(base)) == ((), (), True)  # control: base derives
    for label, config in (("plain", plain), ("masked", masked), ("nested", nested)):
        assert _derive(config) is None, label


def test_an_incomplete_config_is_opaque_and_that_costs_no_diagnostic():
    """The scoping question, settled by measurement rather than by argument.

    An earlier version deliberately let an INCOMPLETE config still derive, on the
    reasoning that incompleteness says nothing about whether the fields present
    are the ones the route runs, and that going inert would hide a diagnostic.
    QA measured the premise away: an incomplete map config has its step refused at
    plan time and a forced apply executes nothing, so nothing is hidden. With the
    justification gone the simpler property is also the safer one.
    """
    from boomi_mcp.categories.components.builders.map_builder import MAP_BUILDERS

    incomplete = {"map_type": "direct"}
    error = MAP_BUILDERS[("transform.map", "direct")].validate_config(dict(incomplete))
    assert error is not None
    assert _derive(incomplete) is None
    # CONTROL: completing it makes it derive, so the None is about completeness
    # rather than about `direct` having stopped working.
    assert _derive(_valid_map_config("direct")) == ((), (), True)


def test_a_map_type_with_no_builder_route_is_opaque():
    from boomi_mcp.categories.components.builders.map_builder import MAP_BUILDERS

    assert ("transform.map", "profile") not in MAP_BUILDERS
    assert _derive({"map_type": "profile"}) is None


# ---------------------------------------------------------------------------
# QA-154-r4-02: witness the shapes where the two predicates could DIVERGE
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,bypassed",
    [
        ("", False),        # falsy for both -- the builder RUNS
        ("<Map/>", True),
        ("   ", True),      # truthy whitespace: a strip-based predicate would differ
        (1, True),          # non-string truthy: a presence/str predicate would differ
        (0, False),         # falsy non-string
        ([], False),        # falsy container
        (["<Map/>"], True), # truthy container
    ],
)
def test_the_raw_xml_predicate_matches_the_builder_on_divergent_shapes(value, bypassed):
    """QA-154-r4-02. The first witness pinned only `""` — the ONE shape where a
    strip-based and a truthiness-based predicate agree.

    A mutant using `.strip()`, or `in config` instead of truthiness, survived
    the whole suite. These are the shapes that tell them apart.
    """
    from boomi_mcp.categories.components.builders.map_builder import MAP_BUILDERS
    from boomi_mcp.categories.integration_builder import _resolve_preservation_policy
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    comp = IntegrationComponentSpec(key="MAP", type="transform.map", config={})
    builder_bypasses = _resolve_preservation_policy(comp, {"xml": value}) is None
    assert builder_bypasses is bypassed, (value, builder_bypasses)

    derived = _derive(_valid_map_config("direct", xml=value))
    if bypassed:
        assert derived is None, (value, derived)
    else:
        assert derived == ((), (), True), (value, derived)
    assert MAP_BUILDERS  # the routing table is real, not an empty stand-in


def test_both_raw_xml_bypass_sites_use_the_same_predicate():
    """QA-154-r5-03. The shape witness exercises `_resolve_preservation_policy`,
    which is a PROXY: the authoritative bypass is inside `_execute_component`,
    and moving that one survived the whole suite.

    `_execute_component` needs a live client and a resolved plan, so it cannot be
    driven here. What CAN be checked is bounded and stated as such: both sites
    test `xml` for TRUTHINESS rather than presence or emptiness, and neither
    normalises it. A change to either lands in this diff.

    The authoritative evidence that the predicate matches the platform is the
    live measurement recorded in the round-4 QA report — `xml=""` runs the
    builder, `xml="   "` and `xml=1` bypass it — which the shape table above
    encodes. This test guards the two in-repo sites from drifting apart; it does
    not re-derive that measurement.
    """
    import re
    from pathlib import Path as _P

    source = (_P(__file__).resolve().parent.parent
              / "src" / "boomi_mcp" / "categories" / "integration_builder.py").read_text()
    conditions = re.findall(r'(?:not )?(?:raw_config|payload)\.get\("xml"\)', source)
    assert len(conditions) >= 2, conditions
    # neither site strips, lowercases, or compares to a literal
    assert not re.search(r'\.get\("xml"\)\s*(?:\.strip\(\)|!=\s*""|==\s*""|is not None)', source)
    assert '"xml" in raw_config' not in source and '"xml" in payload' not in source


@pytest.mark.parametrize(
    "present,expected",
    [
        ("__absent__", True),   # nothing to disagree about -- every predicate injects
        ("", False),            # present-but-EMPTY: setdefault does NOT inject
        (None, False),          # present-but-None: same
        (0, False),             # present-but-falsy non-string
        ([], False),            # present-but-falsy container
        ("   ", False),         # truthy whitespace: neither predicate injects, so
                                # they AGREE -- and the plan then refuses it,
                                # because it strips before requiring. The axis's
                                # own control: measured, not assumed.
    ],
)
def test_the_name_injection_matches_the_plans_setdefault_form(present, expected):
    """QA-154-r6-01 / r6-03.

    `integration_builder` has nine `component_name` injection sites and EIGHT use
    `setdefault` — including the transform.map plan gate. The one falsy-form site
    is the apply-time drift re-validation, and copying that outlier made a
    present-but-falsy `component_name` derive a TRUSTED effect for a config the
    plan refuses: measured 5 misalignments before, 8 after.

    The first guard's ONLY case deleted `component_name` entirely — the one shape
    where every candidate predicate agrees — so swapping the predicate for the
    authority's own `setdefault` survived the whole reachable universe. These are
    the shapes that tell them apart.
    """
    from boomi_mcp.categories.components.builders.transform_map_validation import (
        validate_transform_map,
    )

    config = _valid_map_config("direct")
    if present == "__absent__":
        del config["component_name"]
    else:
        config["component_name"] = present

    depends_on, by_key = _plan_context()
    # What the PLAN would say, using its own injection form.
    effective = dict(config)
    if not effective.get("component_name"):
        effective.setdefault("component_name", "M12.16 spec name")
    plan_accepts = validate_transform_map(dict(effective), depends_on, by_key) is None
    assert plan_accepts is expected, (present, plan_accepts)

    derived = _derive(config, name="M12.16 spec name")
    assert (derived is not None) is expected, (present, derived)


def test_the_effective_config_uses_setdefault_not_a_falsy_check():
    """The two forms differ on exactly one shape, and that shape is asserted."""
    from boomi_mcp.authoring.process_ir_effects import _effective_map_config

    # absent -> injected by both forms
    assert _effective_map_config({}, "N")["component_name"] == "N"
    # present-but-empty -> setdefault leaves it; a falsy check would overwrite it
    assert _effective_map_config({"component_name": ""}, "N")["component_name"] == ""
    # present-and-truthy -> untouched by both
    assert _effective_map_config({"component_name": "X"}, "N")["component_name"] == "X"


def test_the_resolver_passes_the_specs_name_through():
    """End to end: a spec naming itself at the top level must not go inert."""
    config = _valid_map_config("function", function_mappings=[
        _accepted("dynamic_process_property_set")])
    del config["component_name"]
    spec = IntegrationComponentSpec(
        key="MAP", type="transform.map", name="M12.16 map", action="create",
        depends_on=["SP", "TP"], config=config)
    declarations = ProcessIREffectDeclarationsV1(map_effects=(
        ProcessIRMapEffectDeclarationV1(
            map_ref="$ref:MAP", effect=_effect(writes=[("dpp", "OUT")], replay_safe=True)),))
    resolution = resolve_process_ir_effect_declarations(
        [("p", _root_with_map())], declarations, _symbols(), _components(spec), conflict_policy="fail")
    assert resolution.ok, resolution.findings
    assert resolution.inert == (), resolution.inert
    assert len(resolution.capabilities_by_root["p"].map_effects) == 1

    # CONTROL: an UNNAMED spec has nothing to inject, so it is genuinely inert —
    # the acceptance above is about the name arriving, not about the gate being off.
    unnamed = IntegrationComponentSpec(
        key="MAP", type="transform.map", action="create",
        depends_on=["SP", "TP"], config=config)
    inert = resolve_process_ir_effect_declarations(
        [("p", _root_with_map())], declarations, _symbols(), _components(unnamed), conflict_policy="fail")
    assert inert.inert == ("/effect_declarations/map_effects/0",)


def test_a_literal_profile_map_is_not_refused_for_an_index_this_call_cannot_supply():
    """Codex round 6 P1.

    `validate_transform_map` resolves a LITERAL existing-profile UUID only from
    `literal_indexes`, which the plan supplies from caller-provided or
    live-discovered indexes and this resolver has none of. Treating that as a
    refusal made every literal-profile map inert even when the plan builds it.

    Inertness is NOT uniformly the safe direction, which is why this is not left
    to fail closed: for a claimed WRITE inert establishes nothing and is
    conservative, but for REPLAY SAFETY a derived `replay_safe=False` produces a
    retry-safety ERROR while an opaque map produces only a non-blocking warning —
    so treating an unavailable index as a refusal can LOSE an error.
    """
    from boomi_mcp.categories.components.builders.transform_map_validation import (
        validate_transform_map,
    )

    depends_on, by_key = _plan_context()
    literal = dict(_valid_map_config("direct"),
                   source_profile_id="aaaaaaaa-1111-1111-1111-111111111111",
                   target_profile_id="bbbbbbbb-2222-2222-2222-222222222222")
    # The premise: asking from here really does return the index error.
    error = validate_transform_map(dict(literal), depends_on, by_key)
    assert getattr(error, "error_code", None) == "MAP_PROFILE_INDEX_UNAVAILABLE"
    # ...and derivation declines to treat that as the plan's verdict.
    assert _derive(literal) == ((), (), True)


def test_only_the_undecidable_profile_index_branch_is_deferred():
    """QA-154-r7-01 / r7-02. The profile-index refusal has THREE sources and only
    one is a question this call cannot ask.

    The `$ref` branch reports the key it could not resolve and is fully decidable
    from the `components_by_key` this call already supplies; deferring it let a
    map naming a CONNECTION as its profile derive a trusted, pure, replay-safe
    effect. The literal existing-profile branch reports no key because there is
    nothing here to resolve it against.

    Pins the SCOPE, not just the direction: a broader predicate (defer on the code
    alone) and a narrower one (never defer) both fail here.
    """
    from boomi_mcp.categories.components.builders.transform_map_validation import (
        validate_transform_map,
    )

    depends_on, by_key = _plan_context()
    base = _valid_map_config("direct")
    deps = depends_on + ["NOPE", "MAP"]

    literal = dict(base, source_profile_id="aaaaaaaa-1111-1111-1111-111111111111",
                   target_profile_id="bbbbbbbb-2222-2222-2222-222222222222")
    missing_ref = dict(base, source_profile_id="$ref:NOPE")

    # Both refuse with the SAME code — the code alone cannot separate them.
    for config in (literal, missing_ref):
        error = validate_transform_map(dict(config), deps, by_key)
        assert getattr(error, "error_code", None) == "MAP_PROFILE_INDEX_UNAVAILABLE"
    # ...and only the `$ref` one carries the key that makes it decidable.
    assert "ref_key" in (validate_transform_map(dict(missing_ref), deps, by_key).details or {})
    assert "ref_key" not in (validate_transform_map(dict(literal), deps, by_key).details or {})

    # DEFERRED: nothing here could answer it.
    assert _derive(literal, depends_on=deps) == ((), (), True)
    # NOT deferred: this call had everything it needed to answer.
    assert _derive(missing_ref, depends_on=deps) is None
    # ...including a `$ref` that resolves to something that is not a profile.
    assert _derive(dict(base, source_profile_id="$ref:MAP"), depends_on=deps) is None
    # CONTROL: the ordinary valid map still derives.
    assert _derive(base) == ((), (), True)


def test_declining_on_the_index_does_not_soften_any_other_refusal():
    """CONTROL: only the profile-index question is deferred.

    Every other refusal still makes the map opaque, so the exception is scoped to
    one question rather than being a general loosening.
    """
    depends_on, _by_key = _plan_context()
    impure = [_accepted("sequential_value")]
    literal_base = dict(_valid_map_config("function", function_mappings=impure),
                        source_profile_id="aaaaaaaa-1111-1111-1111-111111111111",
                        target_profile_id="bbbbbbbb-2222-2222-2222-222222222222")
    # a literal-profile impure map derives, and KEEPS replay_safe=False
    assert _derive(literal_base) == ((), (), False)
    # ...but a route violation on the same map is still opaque
    assert _derive(dict(literal_base, xslt=["v"])) is None
    # ...and so is a missing name. Asserted WITH a spec name supplied, because
    # `_derive` alone passes none — so the earlier version of this leg passed for
    # a reason unrelated to the refusal it claimed to check (QA-154-r7-03).
    nameless = {k: v for k, v in literal_base.items() if k != "component_name"}
    assert _derive(nameless, name="M12.16 spec name") is not None  # the name IS injected
    assert _derive(dict(nameless, component_name=""), name="M12.16 spec name") is None


@pytest.mark.parametrize(
    "expected_code,override",
    [
        # r6-02 made these two visible; deferring either would undo that.
        ("MAP_FIELD_NOT_FOUND",
         {"field_mappings": [{"source_path": "nope", "target_path": "root/a"}]}),
        ("MAP_PROFILE_REF_REQUIRED",
         {"source_profile_id": "$ref:SP", "depends_on_override": []}),
        # the code the deferred residue in #179 is built on
        ("MAP_DOCUMENT_CACHE_JOINS_INVALID", {"document_cache_joins": "not-a-list"}),
    ],
)
def test_other_profile_related_refusals_are_not_deferred(expected_code, override):
    """QA-154-r8-02. The docstring said "these two codes" and the case list had one.

    Waving `MAP_PROFILE_REF_REQUIRED` survived the whole 559-node reachable
    universe, which left half of the round-7 disposition undischarged. The
    document-cache-joins code is included because it is the one the deferred
    residue in #179 is built on — if that ever became deferred here, the residue
    would silently widen.
    """
    from boomi_mcp.categories.components.builders.transform_map_validation import (
        validate_transform_map,
    )

    depends_on, by_key = _plan_context()
    override = dict(override)
    if "depends_on_override" in override:
        depends_on = override.pop("depends_on_override")
    config = dict(_valid_map_config("direct"), **override)

    error = validate_transform_map(dict(config), depends_on, by_key)
    assert getattr(error, "error_code", None) == expected_code, error
    assert _derive(config, depends_on=depends_on) is None


def test_the_deferred_branch_is_the_one_carrying_only_a_side_detail():
    """QA-154-r8-03, recorded rather than silently relied on.

    The profile-index refusal has THREE branches. The literal index TYPE-mismatch
    branch also carries no resolved-key detail, so the current predicate defers it
    too — and that is harmless ONLY because it is unreachable from this caller,
    which supplies no indexes. Proven here with a positive control rather than
    asserted: the identical config WITH indexes reaches the type-mismatch branch,
    WITHOUT them it reaches the literal branch.

    This is why #179's acceptance criterion is worded against the exact detail set
    rather than against the absence of a key: threading indexes makes the
    type-mismatch branch reachable, and a predicate keyed on absence alone would
    keep deferring it.
    """
    from boomi_mcp.categories.components.builders.transform_map_validation import (
        validate_transform_map,
    )

    depends_on, by_key = _plan_context()
    literal = dict(_valid_map_config("direct"),
                   source_profile_id="aaaaaaaa-1111-1111-1111-111111111111",
                   target_profile_id="bbbbbbbb-2222-2222-2222-222222222222")

    without = validate_transform_map(dict(literal), depends_on, by_key)
    assert set((without.details or {})) == {"side"}, without.details

    # POSITIVE CONTROL, stated as an EXACT requirement rather than as "different".
    # The first version accepted `None` and accepted any unrelated error whose
    # detail keys merely differed, so a regression that stopped reaching the
    # type-mismatch refusal at all would have left it green.
    with_index = validate_transform_map(
        dict(literal), depends_on, by_key,
        literal_indexes={"aaaaaaaa-1111-1111-1111-111111111111": {
            "profile_component_type": "profile.xml", "field_index_by_path": {"root/a": {}}}},
    )
    assert with_index is not None, "the type-mismatch branch was not reached at all"
    assert with_index.error_code == "MAP_PROFILE_INDEX_UNAVAILABLE", with_index.error_code
    assert set((with_index.details or {})) == {"side", "index_type", "declared_type"}, (
        with_index.details
    )


# ---------------------------------------------------------------------------
# §6 architect review: plan-conformance corrections
# ---------------------------------------------------------------------------


def _cache_plan_context():
    """Plan context including an indexable document cache, for join fixtures."""
    depends_on, by_key = _plan_context()
    by_key = dict(by_key)
    by_key["CACHE"] = IntegrationComponentSpec(
        key="CACHE", type="documentcache", action="create", name="CACHE",
        config={"component_name": "CACHE",
                "indexes": [{"index_id": 1, "keys": [{"id": 1, "name": "a (Root/a)"}]}]},
    )
    return depends_on + ["CACHE"], by_key


def _join(**over):
    join = {"document_cache_id": "$ref:CACHE", "cache_index": 1, "join_id": 1,
            "src_parent_key": "1",
            "key_values": [{"cache_key_id": 1, "cache_key_name": "a (Root/a)",
                            "src_link_key": "2"}]}
    join.update(over)
    return join


@pytest.mark.parametrize("map_type", ["direct", "function"])
def test_a_map_document_cache_join_is_a_cache_READ(map_type):
    """§6 P1. `document_cache_joins` were never inspected, so a map carrying one
    reported reading nothing — on BOTH routes.

    The repository already owns this fact: `cache_property_lineage` records one
    cache read per join. Deriving map effects without asking the joins made this
    module a second, incomplete model of a fact that had an owner — the defect
    class this issue exists to remove, reproduced inside the fix for it.
    """
    depends_on, by_key = _cache_plan_context()
    overrides = {"document_cache_joins": [_join()]}
    if map_type == "function":
        overrides["function_mappings"] = [_accepted("dynamic_process_property_set")]
    config = _valid_map_config(map_type, **overrides)
    reads, writes, _replay = derive_map_effect(
        config, depends_on=depends_on, components_by_key=by_key)
    assert ("cache", "$ref:CACHE") in reads, (map_type, reads)
    if map_type == "function":
        assert ("dpp", "OUT") in writes, writes
    # CONTROL: the same map without the join records no cache read.
    plain = _valid_map_config(map_type, **{k: v for k, v in overrides.items()
                                           if k != "document_cache_joins"})
    assert derive_map_effect(
        plain, depends_on=depends_on, components_by_key=by_key)[0] == ()


def test_an_externally_written_join_records_no_read():
    """Mirrors the existing authority, which marks such a join externally
    satisfied. A contract read carries no such flag and lineage treats every one
    as strict, so recording it would turn a valid flow into a false
    missing-writer error — the same reasoning as a defaulted property get."""
    depends_on, by_key = _cache_plan_context()
    config = _valid_map_config("direct", document_cache_joins=[_join(external_writer=True)])
    assert derive_map_effect(
        config, depends_on=depends_on, components_by_key=by_key) == ((), (), True)


def test_a_subprocess_read_the_child_satisfies_is_not_required_of_the_caller():
    """§6 P1(a). Reporting it made a valid payload invalid purely by declaring
    the child's truthful summary, so the only way to stay green was to omit the
    declaration — which costs exactly what the channel exists for."""
    from boomi_mcp.authoring.process_ir_effects import derive_subprocess_effect

    values = [{"value_type": "static", "value": "v"}]
    read_p = [{"value_type": "dpp", "property_name": "P"}]
    src = {"kind": "source", "connection_ref": "$ref:CONN", "operation_ref": "$ref:GETOP"}

    satisfied = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        src, {"kind": "set_dpp", "name": "P", "source_values": values},
        {"kind": "set_ddp", "name": "D", "source_values": read_p},
        {"kind": "return_documents"}]}})
    reads, writes, _ = derive_subprocess_effect(satisfied)
    assert ("dpp", "P") not in reads, reads
    assert ("dpp", "P") in writes

    # CONTROL: the same read with NO preceding write IS required of the caller.
    unsatisfied = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        src, {"kind": "set_ddp", "name": "D", "source_values": read_p},
        {"kind": "return_documents"}]}})
    assert ("dpp", "P") in derive_subprocess_effect(unsatisfied)[0]


def test_a_subprocess_that_writes_a_cache_is_not_replay_safe():
    """§6 P1(c). Re-running the child would write the cache twice."""
    from boomi_mcp.authoring.process_ir_effects import derive_subprocess_effect

    child = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "source", "connection_ref": "$ref:CONN", "operation_ref": "$ref:GETOP"},
        {"kind": "cache_put", "cache_ref": "$ref:DC"},
        {"kind": "cache_get", "cache_ref": "$ref:DC"},
        {"kind": "return_documents"}]}})
    _reads, writes, replay_safe = derive_subprocess_effect(child)
    assert ("cache", "$ref:DC") in writes
    assert replay_safe is False


def test_a_child_with_an_uninspectable_step_is_INERT_not_exact_empty():
    """§6 P1(d). An exact-empty summary for an uninspectable child was an unsound
    ACCEPTANCE — it asserted "this child touches nothing" about contents nobody
    read, and a declaration matching that fabrication was then trusted."""
    from boomi_mcp.authoring.process_ir_effects import derive_subprocess_effect

    # A `map` is uninspectable in the only sense that matters here: its STATE
    # effect is knowable only through a typed contract, which a child summary
    # does not carry. (`connector` is deliberately NOT this case — it does I/O
    # but attributes no state key, so it stays inspectable and is handled on
    # the replay axis instead.)
    opaque = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "source", "connection_ref": "$ref:CONN", "operation_ref": "$ref:GETOP"},
        {"kind": "map_ref", "map_ref": "$ref:MAP"},
        {"kind": "return_documents"}]}})
    assert derive_subprocess_effect(opaque) is None
    # CONTROL: an inspectable child still derives, so INERT is about the opaque
    # step rather than about derivation having stopped working.
    plain = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "source", "connection_ref": "$ref:CONN", "operation_ref": "$ref:GETOP"},
        {"kind": "set_dpp", "name": "P",
         "source_values": [{"value_type": "static", "value": "v"}]},
        {"kind": "return_documents"}]}})
    # replay_safe is False: the child contains a connector, whose repetition is
    # observable outside the process.
    assert derive_subprocess_effect(plain) == ((), (("dpp", "P"),), False)


def test_declaration_families_hash_the_same_in_any_authored_order():
    """§6 §6. These tuples enter the payload the semantic hash covers, so two
    requests declaring the SAME effects in a different order produced different
    plan and compile hashes and forced a re-plan that established nothing.

    Stage-2 r9 P2 moved WHERE that is enforced. Canonicalising by reordering the
    parsed request bought hash stability with pointer fidelity: the resolver
    enumerates these same tuples to build `/effect_declarations/<family>/<index>`
    diagnostics, so the caller was sent to whichever item the sort put at that
    position. Both properties hold now, at their own seams.
    """
    a = ProcessIREffectDeclarationsV1(external_writers=(
        ProcessIRExternalWriterDeclarationV1(cache_ref="$ref:B"),
        ProcessIRExternalWriterDeclarationV1(cache_ref="$ref:A")))
    b = ProcessIREffectDeclarationsV1(external_writers=(
        ProcessIRExternalWriterDeclarationV1(cache_ref="$ref:A"),
        ProcessIRExternalWriterDeclarationV1(cache_ref="$ref:B")))
    # the HASH INPUT is order-independent ...
    assert a.canonical_payload() == b.canonical_payload()
    # ... while the request still reads back as the caller wrote it.
    assert [w.cache_ref for w in a.external_writers] == ["$ref:B", "$ref:A"]
    assert [w.cache_ref for w in b.external_writers] == ["$ref:A", "$ref:B"]


def test_a_finding_points_at_the_index_the_caller_actually_authored():
    """The r9 P2 regression, at the seam that produces the pointer.

    `$ref:UNBOUND` names nothing in the root, so the SECOND authored writer is
    the invalid one. Sorting the request first moved it to index 0 and the
    caller was sent to edit the declaration that was fine.
    """
    root = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "source", "connection_ref": "$ref:CONN", "operation_ref": "$ref:GETOP"},
        {"kind": "cache_get", "cache_ref": "$ref:ZCACHE"},
        {"kind": "return_documents"}]}})
    declarations = ProcessIREffectDeclarationsV1(external_writers=(
        ProcessIRExternalWriterDeclarationV1(cache_ref="$ref:ZCACHE"),
        ProcessIRExternalWriterDeclarationV1(cache_ref="$ref:UNBOUND")))
    resolution = resolve_process_ir_effect_declarations(
        [("p", root)], declarations, _symbols(), [])
    unbound = [f for f in resolution.findings if f.reason == "unbound"]
    assert len(unbound) == 1, resolution.findings
    assert unbound[0].path == "/effect_declarations/external_writers/1", unbound[0].path


def test_the_public_script_language_matches_the_compilers_own():
    """§6 §6. Accepting any non-empty string let a declaration name a language the
    authored step cannot carry, surfacing only as a failed digest lookup later."""
    import typing

    from boomi_mcp.models.authoring_workflow import (
        ProcessIRScriptLanguageV1, _script_languages,
    )

    assert set(typing.get_args(ProcessIRScriptLanguageV1)) == set(_script_languages())
    with pytest.raises(Exception):
        ProcessIRScriptEffectDeclarationV1(
            language="python", source_sha256="sha256:" + "a" * 64,
            effect=ProcessIRStateEffectDeclarationV1())


def test_a_declared_map_effect_reaches_compile_and_emit_on_the_legacy_topology():
    """§6 §7. The audit record claimed NO map_ref compiles through the public
    path, and every checked-in test drove resolvers directly or substituted a
    fake — so the claim was never contradicted by coverage.

    It is false. The bracketed `connector_call` form is blocked by call-pair
    profile checking, but the LEGACY `source -> map_ref -> return_documents` form
    deliberately bypasses that check, and compiles. This drives a real declared
    map effect through resolve -> compile -> emit on that topology.
    """
    import sys as _sys
    from pathlib import Path as _P

    _here = str(_P(__file__).resolve().parent)
    if _here not in _sys.path:
        _sys.path.insert(0, _here)
    from _wave_gate_golden_corpus import error_symbols

    from boomi_mcp.categories.components.process_graph_verifier import verify_process_graph
    from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1
    from boomi_mcp.compiler.process_ir.emitter_registry import emit_process
    from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1

    symbols = error_symbols(
        ComponentSymbolV1(ref="$ref:MAP", component_id="m-1", component_type="transform.map"))
    root = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "source", "connection_ref": "$ref:CONN", "operation_ref": "$ref:GETOP"},
        {"kind": "map_ref", "map_ref": "$ref:MAP"},
        {"kind": "return_documents"}]}})

    depends_on, by_key = _plan_context()
    map_spec = IntegrationComponentSpec(
        key="MAP", type="transform.map", action="create", name="M12.16 map",
        depends_on=depends_on,
        config=_valid_map_config("function", function_mappings=[
            _accepted("dynamic_process_property_set")]),
    )
    declarations = ProcessIREffectDeclarationsV1(map_effects=(
        ProcessIRMapEffectDeclarationV1(
            map_ref="$ref:MAP",
            effect=_effect(writes=[("dpp", "OUT")], replay_safe=True)),))

    resolution = resolve_process_ir_effect_declarations(
        [("p", root)], declarations, symbols, _components(map_spec), conflict_policy="fail")
    assert resolution.ok, resolution.findings
    assert resolution.inert == (), "the declaration went inert — it establishes nothing"
    capabilities = resolution.capabilities_by_root["p"]
    assert capabilities.map_effect("$ref:MAP").writes == (("dpp", "OUT"),)

    # ...and the SAME context reaches a real compile, which then emits.
    _cfg, plan = compile_process_ir_v1(root, symbols, capabilities=capabilities)
    xml = emit_process(plan, symbols).process_xml
    assert "<bns:" in xml or "shapetype" in xml
    assert not verify_process_graph(xml).get("errors")

    # CONTROL: without the declaration the same root still compiles, so the
    # assertion above is about the effect arriving rather than about the compile.
    _cfg2, plan2 = compile_process_ir_v1(root, symbols)
    assert emit_process(plan2, symbols).process_xml == xml


# ---------------------------------------------------------------------------
# Stage-2 r9 P1 — the summary is read off the lineage walk, not a second scan
# ---------------------------------------------------------------------------


def _control_only_child(arm_steps):
    """A connector-free root: exactly one Decision and nothing else (#141)."""
    return parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "decision", "label": "d", "comparison": "equals",
         "left": {"value_type": "static", "static_value": "a"},
         "right": {"value_type": "static", "static_value": "b"},
         "true_arm": {"steps": arm_steps, "terminal": {"kind": "stop"}},
         "false_arm": {"steps": [], "terminal": {"kind": "stop"}}}]}})


def test_a_read_inside_a_control_body_is_still_required_of_the_caller():
    """The r9 P1 regression: a root-spine-only scan reported requiring nothing.

    A child that reads `dpp:K` inside a Decision arm genuinely depends on its
    caller establishing K. The scan this replaces matched only `/body/steps/N`,
    so the read vanished — and BOTH directions were then wrong: a caller
    declaring the truthful read was rejected as a content mismatch, while a
    caller declaring nothing matched the fabricated empty summary and was
    trusted. The dependency disappeared from the strict classifiers either way.
    """
    from boomi_mcp.authoring.process_ir_effects import derive_subprocess_effect

    child = _control_only_child([
        {"kind": "set_dpp", "name": "OUT",
         "source_values": [{"value_type": "dpp", "property_name": "K"}]}])
    reads, _writes, _replay = derive_subprocess_effect(child)
    assert reads == (("dpp", "K"),), reads


def test_a_read_the_child_satisfies_itself_is_not_required_of_the_caller():
    """CONTROL for the test above, in the opposite direction.

    Without this, a derivation that simply reported EVERY read would pass the
    regression test while making every self-contained child's declaration
    impossible to write truthfully.
    """
    from boomi_mcp.authoring.process_ir_effects import derive_subprocess_effect

    child = _control_only_child([
        {"kind": "set_dpp", "name": "K",
         "source_values": [{"value_type": "static", "value": "v"}]},
        {"kind": "set_dpp", "name": "OUT",
         "source_values": [{"value_type": "dpp", "property_name": "K"}]}])
    reads, _writes, _replay = derive_subprocess_effect(child)
    assert reads == (), reads


def test_a_write_on_one_decision_arm_is_never_a_guarantee():
    """The two axes approximate in OPPOSITE directions.

    Reads are a MAY set and writes are a MUST set. A derivation that used one
    lattice for both would have to be wrong on one of them.
    """
    from boomi_mcp.authoring.process_ir_effects import derive_subprocess_effect

    child = _control_only_child([
        {"kind": "set_dpp", "name": "ARM_ONLY",
         "source_values": [{"value_type": "static", "value": "v"}]}])
    _reads, writes, _replay = derive_subprocess_effect(child)
    assert writes == (), writes


def test_a_connector_anywhere_makes_the_child_replay_unsafe():
    """Replay safety is a MAY property: I/O on any path is observable."""
    from boomi_mcp.authoring.process_ir_effects import derive_subprocess_effect

    child = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "source", "connection_ref": "$ref:CONN", "operation_ref": "$ref:GETOP"},
        {"kind": "set_dpp", "name": "P",
         "source_values": [{"value_type": "static", "value": "v"}]},
        {"kind": "return_documents"}]}})
    assert derive_subprocess_effect(child)[2] is False
    # CONTROL: the same child without the connector IS replay-safe, so the flag
    # is about the connector rather than pinned False for everything.
    assert derive_subprocess_effect(_control_only_child([
        {"kind": "set_dpp", "name": "P",
         "source_values": [{"value_type": "static", "value": "v"}]}]))[2] is True


def test_the_inspectable_and_opaque_child_kinds_partition_the_vocabulary():
    """The allowlist is pinned BIDIRECTIONALLY to the compiler's own union.

    The denylist this replaces named `connector_call` and silently missed
    `connector` — the kind a `source`/`target` step actually lowers to — so the
    most common child shape was summarised rather than refused. A hand-written
    set on either side can drift the same way; this fails instead, on both a
    kind added to the compiler and a kind dropped from it.
    """
    import typing

    from boomi_mcp.authoring.process_ir_effects import INSPECTABLE_CHILD_KINDS
    from boomi_mcp.compiler.process_ir import contracts as C

    vocabulary = set()
    for member in typing.get_args(typing.get_args(C.CfgSemanticV1)[0]):
        annotation = member.model_fields["semantic_kind"].annotation
        vocabulary.update(typing.get_args(annotation))
    assert vocabulary, "no semantic kinds discovered — the probe itself is vacuous"

    # The three kinds whose STATE is knowable only through a typed contract,
    # named by the lineage authority's own `_opaque_reason`.
    opaque = {"map", "data_process", "process_call"}
    assert INSPECTABLE_CHILD_KINDS | opaque == vocabulary, (
        "unclassified kinds: {0}".format(
            vocabulary ^ (INSPECTABLE_CHILD_KINDS | opaque)))
    assert not (INSPECTABLE_CHILD_KINDS & opaque)


def test_every_effect_authority_row_names_its_own_authority():
    """Stage-2 r9 P2. All five rows were served as `runtime.process_ir_models`.

    That module carries the declaration SHAPE and none of these facts: map
    effects come from the map-function registry, script effects from the vetted
    registry, subprocess effects from inspecting the child. A served `source_id`
    is a claim about provenance, and a wrong one sends a caller to the wrong
    place to verify it — the same defect `_SEMANTIC_RULE_SOURCES` was
    introduced to fix, recurring in the generated rows.

    Totality is the load-bearing half: without it a family added to
    `effect_authority_rows()` would inherit whatever the lookup defaulted to.
    """
    from boomi_mcp.authoring.process_ir_effects import effect_authority_rows
    from boomi_mcp.authoring.process_ir_projection import (
        SOURCE_MODELS,
        _EFFECT_AUTHORITY_SOURCES,
        _effect_authority_entries,
    )

    authorities = {authority for _family, authority in effect_authority_rows()}
    assert set(_EFFECT_AUTHORITY_SOURCES) == authorities, sorted(
        set(_EFFECT_AUTHORITY_SOURCES) ^ authorities)

    entries = _effect_authority_entries()
    assert len(entries) == len(authorities)
    served = {s.source_id for entry in entries for s in entry.sources}
    # The rows state facts about several DIFFERENT modules, so one shared source
    # for all of them is exactly the wrong answer.
    assert len(served) > 1, served
    assert SOURCE_MODELS not in served, served


def test_the_control_body_union_is_composed_from_the_linear_members():
    """QA-154-r9-07. Nothing pinned the composition: respelling the control
    union back to a duplicated literal Union passed the whole corpus.

    This catches DIVERGENCE, which is the risk that actually materialises — a
    second copy edited on one side only. It does NOT catch a faithful
    re-spelling, and cannot: a duplicate that still lists exactly these members
    in this order IS this union, and no runtime probe can tell them apart. The
    next edit to either side is what this test is waiting for.
    """
    import typing

    from boomi_mcp.models import process_ir as M

    linear = typing.get_args(typing.get_args(M.LinearNodeV1)[0])
    control = typing.get_args(typing.get_args(M.ControlBodyStepV1)[0])
    assert linear == M._LINEAR_MEMBERS, linear
    assert control == M._LINEAR_MEMBERS + (M.ConnectorCallNodeV1,), control


def test_a_persisted_property_makes_a_child_replay_unsafe():
    """QA-154-r9 mutation residue: deleting the persist check was invisible.

    A persisted process property outlives the execution, so re-running the
    child does not start from the same state.
    """
    from boomi_mcp.authoring.process_ir_effects import derive_subprocess_effect

    def child(persist):
        step = {"kind": "set_dpp", "name": "P",
                "source_values": [{"value_type": "static", "value": "v"}]}
        if persist:
            step["persist"] = True
        return _control_only_child([step])

    assert derive_subprocess_effect(child(True))[2] is False
    # CONTROL: the same child without the flag stays replay-safe, so the check
    # is about persistence rather than pinned False for every set_property.
    assert derive_subprocess_effect(child(False))[2] is True


def test_every_effect_authority_family_has_served_wording():
    """QA-154-r9 mutation residue: the prose map's fail-closed branch was never
    exercised, and the correspondence was pinned in NEITHER direction.

    A `raises` probe cannot pin it: the construction site now looks up TWO
    hand-written maps by the same generated authority token, so an unknown
    token raises from whichever is consulted first and the assertion passes
    whichever branch is broken. (Measured: making the prose lookup fail OPEN
    left a `pytest.raises(KeyError)` test green, because the sources lookup
    raised instead.) Totality, asserted per map, is what actually distinguishes
    them.
    """
    from boomi_mcp.authoring.process_ir_effects import effect_authority_rows
    from boomi_mcp.authoring.process_ir_projection import (
        _EFFECT_AUTHORITY_PROSE,
        _effect_authority_entries,
    )

    authorities = {authority for _family, authority in effect_authority_rows()}
    assert set(_EFFECT_AUTHORITY_PROSE) == authorities, sorted(
        set(_EFFECT_AUTHORITY_PROSE) ^ authorities)
    # ... and every row actually carries its wording through to the served entry.
    for entry in _effect_authority_entries():
        assert entry.summary.strip(), entry.contract_entry_id
