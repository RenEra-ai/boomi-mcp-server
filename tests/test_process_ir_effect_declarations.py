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
def _valid_map_config(map_type="direct", **overrides):
    config = {
        "component_name": "M12.16 map",
        "map_type": map_type,
        "source_profile_id": "aaaaaaaa-1111-1111-1111-111111111111",
        "source_profile_type": "profile.json",
        "target_profile_id": "bbbbbbbb-2222-2222-2222-222222222222",
        "target_profile_type": "profile.json",
    }
    if map_type in ("direct",):
        config["field_mappings"] = [{"source_path": "a", "target_path": "b"}]
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
        "dynamic_process_property_set": {"inputs": ["a"], "parameters": {"property_name": "OUT"}},
        "dynamic_process_property_get": {"target_path": "t", "parameters": {"property_name": "P"}},
        "document_property_set": {"inputs": ["a"], "parameters": {"document_property_name": "D"}},
        "document_property_get": {"target_path": "t", "parameters": {"document_property_name": "D"}},
        "defined_process_property_get": {
            "target_path": "t",
            "parameters": {"process_property_component_id": "$ref:X",
                           "process_property_component_name": "P",
                           "process_property_key": "K"}},
        "defined_process_property_set": {
            "inputs": ["a"],
            "parameters": {"process_property_component_id": "$ref:X",
                           "process_property_component_name": "P",
                           "process_property_key": "K"}},
        "sequential_value": {"target_path": "t", "parameters": {"key_name": "K"}},
        "uppercase": {"inputs": ["a"], "target_path": "t", "parameters": {}},
    }
    mapping = {"function_type": function_type}
    mapping.update(shapes.get(function_type, {"inputs": ["a"], "target_path": "t", "parameters": {}}))
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


def _map_component(function_mappings):
    return IntegrationComponentSpec(
        key="MAP", type="transform.map",
        config=_valid_map_config("function", function_mappings=function_mappings),
    )


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
    derived = derive_map_effect(_valid_map_config("function", function_mappings=[
        _accepted("dynamic_process_property_set", parameters={"property_name": "OUT"}),
        _accepted("uppercase"),
    ]))
    assert derived == ((), (("dpp", "OUT"),), True)


def test_a_map_with_one_unannotated_function_is_wholly_opaque():
    """Partial knowledge is never promoted to a complete effect."""
    derived = derive_map_effect(_valid_map_config("function", function_mappings=[
        _accepted("dynamic_process_property_set", parameters={"property_name": "OUT"}),
        _accepted("defined_process_property_get", parameters={"process_property_component_id": "$ref:X", "property_name": "P"}),
    ]))
    assert derived is None


def test_an_unknown_function_family_makes_the_map_opaque():
    assert derive_map_effect(_valid_map_config(
        "function", function_mappings=[_accepted("not_a_real_family")])) is None


def test_a_sequential_value_map_is_derivable_but_not_replay_safe():
    reads, writes, replay_safe = derive_map_effect(_valid_map_config(
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
    components = [_map_component([
        _accepted("dynamic_process_property_set", parameters={"property_name": "OUT"})])]
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
    components = [_map_component([
        _accepted("dynamic_process_property_set", parameters={"property_name": "OUT"})])]
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


def test_external_writer_binds_only_when_the_node_authored_the_flag():
    declarations = ProcessIREffectDeclarationsV1(external_writers=(
        ProcessIRExternalWriterDeclarationV1(cache_ref="$ref:CACHE"),))
    ok = resolve_process_ir_effect_declarations(
        [("p", _root_with_external_cache_read())], declarations, _symbols(), [])
    assert ok.ok, ok.findings
    assert ok.capabilities_by_root["p"].external_writers[0].cache_ref == "$ref:CACHE"

    # the same declaration against a read that did NOT author the flag
    unflagged = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "source", "connection_ref": "$ref:CONN", "operation_ref": "$ref:GETOP"},
        {"kind": "cache_get", "cache_ref": "$ref:CACHE"},
        {"kind": "return_documents"}]}})
    bad = resolve_process_ir_effect_declarations(
        [("p", unflagged)], declarations, _symbols(), [])
    assert [f.reason for f in bad.findings] == ["unbound"]


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
    components = [_map_component([
        _accepted("dynamic_process_property_set", parameters={"property_name": "OUT"})])]
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
    components = [_map_component([
        _accepted("dynamic_process_property_set", parameters={"property_name": "OUT"})])]
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
    components = [_map_component([
        _accepted("dynamic_process_property_set", parameters={"property_name": "OUT"})])]
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
    components = [_map_component([
        _accepted("dynamic_process_property_set", parameters={"property_name": canary})])]
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
    assert derive_map_effect(
        _valid_map_config("function", function_mappings=[mapping])
    ) == ((), (), True)

    patched = dict(reg.FUNCTION_FAMILIES)
    patched["uppercase"] = stripped
    original = reg.FUNCTION_FAMILIES
    reg.FUNCTION_FAMILIES = MappingProxyType(patched)
    try:
        # ...and unannotated it is OPAQUE rather than silently pure.
        assert derive_map_effect(
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
    assert derive_map_effect({}) is None
    assert derive_map_effect({"component_id": "some-live-map"}) is None
    # ...and the control: a config that DOES say what it is still derives.
    assert derive_map_effect(_valid_map_config("direct")) == ((), (), True)


def test_a_create_under_reuse_is_opaque_because_the_plan_may_substitute_it():
    roots = [("p", _root_with_map())]
    declarations = ProcessIREffectDeclarationsV1(map_effects=(
        ProcessIRMapEffectDeclarationV1(
            map_ref="$ref:MAP", effect=_effect(writes=[("dpp", "OUT")], replay_safe=True)),))
    components = [_map_component([
        _accepted("dynamic_process_property_set", parameters={"property_name": "OUT"})])]

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
    derived = derive_map_effect(_valid_map_config("function", function_mappings=[
        dict(_accepted("sequential_value"), function_type="  SEQUENTIAL_VALUE  ")]))
    assert derived == ((), (), False), derived


def test_a_defaulted_property_get_records_no_strict_read():
    """A contract read carries no has-default flag and lineage treats every one
    as strict, so recording a defaulted read would fail a flow that runs fine."""
    defaulted = derive_map_effect(_valid_map_config("function", function_mappings=[
        _accepted("dynamic_process_property_get",
                  parameters={"property_name": "P", "default_value": "fallback"})]))
    assert defaulted == ((), (), True), defaulted
    # CONTROL: without the default it IS a read, so the omission is about the
    # default rather than about the derivation never recording reads.
    plain = derive_map_effect(_valid_map_config("function", function_mappings=[
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
    assert derive_map_effect(spec.config, substitutable=True) is None


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
        derived = derive_map_effect(_valid_map_config(alias, function_mappings=impure))
        assert derived == ((), (), False), (alias, derived)


def test_every_direct_map_type_the_builder_supports_is_pure():
    from boomi_mcp.categories.components.builders.map_builder import DirectMapBuilder

    assert DirectMapBuilder.SUPPORTED_MAP_TYPES
    for alias in DirectMapBuilder.SUPPORTED_MAP_TYPES:
        assert derive_map_effect(_valid_map_config(alias)) == ((), (), True), alias


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
        assert derive_map_effect(_valid_map_config(alias)) is None, alias


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
        assert derive_map_effect(_valid_map_config("map_function", function_mappings=[_noop_mapping()])) is None
        # CONTROL: the alias that remains still derives, so the None above is
        # about the narrowing rather than about the derivation breaking.
        assert derive_map_effect(_valid_map_config("function", function_mappings=[_noop_mapping()])) == ((), (), True)
    finally:
        mb.MapFunctionBuilder.SUPPORTED_MAP_TYPES = original
    assert derive_map_effect(_valid_map_config("map_function", function_mappings=[_noop_mapping()])) == ((), (), True)


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
    assert derive_map_effect(_valid_map_config("profile")) is None


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
    assert derive_map_effect(_valid_map_config("direct", function_mappings=impure)) is None
    # CONTROL: a well-formed direct map still derives, so the None above is about
    # the rejected key rather than about `direct` having stopped working.
    assert derive_map_effect(_valid_map_config("direct")) == ((), (), True)


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
        assert derive_map_effect(_valid_map_config("direct", **{key: ["anything"]})) is None, key


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
        assert derive_map_effect(_valid_map_config("direct")) is None
        # CONTROL: the function vocabulary is untouched and still derives, so the
        # None above is about the narrowing rather than a wholesale break.
        assert derive_map_effect(_valid_map_config("function", function_mappings=[_noop_mapping()])) == ((), (), True)
    finally:
        mb.DirectMapBuilder.SUPPORTED_MAP_TYPES = original
    assert derive_map_effect(_valid_map_config("direct")) == ((), (), True)


@pytest.mark.parametrize("bad", [["direct"], {"a": 1}, {"direct"}, 3, None])
def test_a_non_string_map_type_is_opaque_and_does_not_crash(bad):
    """QA-154-r3-04. An authored `map_type` need not be hashable.

    `["direct"]` raised `TypeError: unhashable type` straight out of the tool
    with `error_code: None` and no machine code at all — a caller learned nothing
    from a value they had supplied.
    """
    assert derive_map_effect(_valid_map_config(bad, function_mappings=[_noop_mapping()])) is None


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
    assert derive_map_effect(
        _valid_map_config("map_function", function_mappings=structured)
    ) == ((), (("dpp", "OUT"),), True)
    assert derive_map_effect(
        _valid_map_config("map_function", xml="<Map/>", function_mappings=structured)
    ) is None
    # ...and a direct map carrying raw XML is opaque for the same reason.
    assert derive_map_effect(_valid_map_config("direct", xml="<Map/>")) is None


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
    assert derive_map_effect(_valid_map_config("direct", xml="")) == ((), (), True)


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
                derived = derive_map_effect(config)
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
    assert derive_map_effect(dict(base)) == ((), (), True)  # control: base derives
    for label, config in (("plain", plain), ("masked", masked), ("nested", nested)):
        assert derive_map_effect(config) is None, label


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
    assert derive_map_effect(incomplete) is None
    # CONTROL: completing it makes it derive, so the None is about completeness
    # rather than about `direct` having stopped working.
    assert derive_map_effect(_valid_map_config("direct")) == ((), (), True)


def test_a_map_type_with_no_builder_route_is_opaque():
    from boomi_mcp.categories.components.builders.map_builder import MAP_BUILDERS

    assert ("transform.map", "profile") not in MAP_BUILDERS
    assert derive_map_effect({"map_type": "profile"}) is None


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

    derived = derive_map_effect(_valid_map_config("direct", xml=value))
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
