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


def _map_component(function_mappings):
    return IntegrationComponentSpec(
        key="MAP", type="transform.map",
        config={"map_type": "function", "function_mappings": function_mappings},
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
    derived = derive_map_effect({"map_type": "function", "function_mappings": [
        {"function_type": "dynamic_process_property_set", "parameters": {"property_name": "OUT"}},
        {"function_type": "uppercase", "parameters": {}},
    ]})
    assert derived == ((), (("dpp", "OUT"),), True)


def test_a_map_with_one_unannotated_function_is_wholly_opaque():
    """Partial knowledge is never promoted to a complete effect."""
    derived = derive_map_effect({"map_type": "function", "function_mappings": [
        {"function_type": "dynamic_process_property_set", "parameters": {"property_name": "OUT"}},
        {"function_type": "defined_process_property_get",
         "parameters": {"process_property_component_id": "$ref:X"}},
    ]})
    assert derived is None


def test_an_unknown_function_family_makes_the_map_opaque():
    assert derive_map_effect({"map_type": "function", "function_mappings": [
        {"function_type": "not_a_real_family", "parameters": {}}]}) is None


def test_a_sequential_value_map_is_derivable_but_not_replay_safe():
    reads, writes, replay_safe = derive_map_effect({
        "map_type": "function",
        "function_mappings": [{"function_type": "sequential_value", "parameters": {}}]})
    assert (reads, writes) == ((), ())
    assert replay_safe is False


def test_a_matching_map_declaration_is_accepted_and_bound_to_its_root():
    roots = [("p", _root_with_map())]
    declarations = ProcessIREffectDeclarationsV1(map_effects=(
        ProcessIRMapEffectDeclarationV1(
            map_ref="$ref:MAP", effect=_effect(writes=[("dpp", "OUT")], replay_safe=True)),
    ))
    components = [_map_component([
        {"function_type": "dynamic_process_property_set", "parameters": {"property_name": "OUT"}}])]
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
        {"function_type": "dynamic_process_property_set", "parameters": {"property_name": "OUT"}}])]
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
        {"function_type": "dynamic_process_property_set", "parameters": {"property_name": "OUT"}}])]
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
        {"function_type": "dynamic_process_property_set", "parameters": {"property_name": "OUT"}}])]
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
        {"function_type": "dynamic_process_property_set", "parameters": {"property_name": "OUT"}}])]
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
        {"function_type": "dynamic_process_property_set",
         "parameters": {"property_name": canary}}])]
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
    """The non-vacuity witness for the opaque default.

    Constructs a family with no effect annotation, PROVES it is unannotated, and
    shows a map containing it derives nothing rather than deriving 'pure'.
    """
    from boomi_mcp.categories.components.builders import map_function_registry as reg

    real = reg.FUNCTION_FAMILIES["uppercase"]
    assert real.effect_kind == "pure"
    import dataclasses

    unannotated = dataclasses.replace(real, name="unannotated_probe", effect_kind=None)
    assert unannotated.effect_kind is None  # the mutation took effect

    annotated = dataclasses.replace(real, name="annotated_probe", effect_kind="pure")

    patched = dict(reg.FUNCTION_FAMILIES)
    patched["unannotated_probe"] = unannotated
    patched["annotated_probe"] = annotated
    from types import MappingProxyType

    original = reg.FUNCTION_FAMILIES
    reg.FUNCTION_FAMILIES = MappingProxyType(patched)
    try:
        # CONTROL. `derive_map_effect` returns None both for an unannotated
        # family and for one it cannot find at all, so the opaque assertion below
        # would pass just as happily if the patch had never taken effect. This
        # control distinguishes the two: an ANNOTATED probe injected the same way
        # must derive a real effect, which is only possible if the injection is
        # visible to the function under test.
        assert derive_map_effect({"map_type": "function", "function_mappings": [
            {"function_type": "annotated_probe", "parameters": {}}]}) == ((), (), True)
        # ...and the unannotated one is opaque, not silently pure.
        assert derive_map_effect({"map_type": "function", "function_mappings": [
            {"function_type": "unannotated_probe", "parameters": {}}]}) is None
    finally:
        reg.FUNCTION_FAMILIES = original


# ---------------------------------------------------------------------------
# QA-154-r1-01 regression: EVERY compile the authoring path runs gets the context
# ---------------------------------------------------------------------------


def test_the_materialization_compile_accepts_the_resolved_context():
    """The site that was strict.

    `build_materialization_plan` owns the THIRD compile of a root. While it took
    no `capabilities` parameter, a declaration whose whole purpose is to turn a
    blocking finding into a warning validated clean and then failed here — so
    using the feature as documented left a caller strictly worse off than
    omitting it.
    """
    import inspect

    from boomi_mcp.authoring.process_materialization import build_materialization_plan

    assert "capabilities" in inspect.signature(build_materialization_plan).parameters


def test_the_materialization_compile_actually_receives_the_context(monkeypatch):
    """BEHAVIOURAL, not structural.

    Builds a real materialization plan and spies on the core compile every entry
    funnels through. Before the fix this site called the strict two-argument form
    and the spy would record ``None``.
    """
    import copy

    from boomi_mcp.authoring import process_materialization as pm
    from boomi_mcp.authoring.contract import get_authoring_revisions
    from boomi_mcp.compiler.process_ir import pipeline as _pipeline
    from boomi_mcp.compiler.process_ir.emitter_registry import emitter_revision
    from boomi_mcp.compiler.process_ir.semantic_validation.contracts import (
        ProcessIRValidationCapabilitiesV1,
    )
    from boomi_mcp.models.process_component import ProcessComponentEnvelopeV1

    import test_process_materialization_plan as mp

    seen = []
    real = _pipeline._compile_parsed_process_ir_v1

    def spy(*args, **kwargs):
        seen.append(kwargs.get("capabilities"))
        return real(*args, **kwargs)

    monkeypatch.setattr(_pipeline, "_compile_parsed_process_ir_v1", spy)

    # A DISTINCT empty instance. It binds nothing, so the compiler's
    # unbound-contract check has nothing to reject — while still being a specific
    # object the spy can identify, which is what makes the assertion below about
    # THREADING rather than about equality with the default.
    context = ProcessIRValidationCapabilitiesV1()
    assert context is not None
    pm.build_materialization_plan(
        envelope=ProcessComponentEnvelopeV1(component_key="proc", name="P", action="create"),
        process_ir=parse_process_ir_v1(copy.deepcopy(mp.VALID_IR_DOC)),
        symbols=mp._symbols(None),
        conflict_policy="reuse",
        compiler_revision=get_authoring_revisions()["compiler_revision"],
        emitter_revision=emitter_revision(),
        materializer_revision="sha256:" + "a" * 64,
        capabilities=context,
    )
    assert seen, "the spy never fired — the test would be vacuous"
    assert all(item is context for item in seen), seen

    # CONTROL: omitting the keyword must still reach the compile with None, so
    # the assertion above is discriminating rather than always-true.
    seen.clear()
    pm.build_materialization_plan(
        envelope=ProcessComponentEnvelopeV1(component_key="proc", name="P", action="create"),
        process_ir=parse_process_ir_v1(copy.deepcopy(mp.VALID_IR_DOC)),
        symbols=mp._symbols(None),
        conflict_policy="reuse",
        compiler_revision=get_authoring_revisions()["compiler_revision"],
        emitter_revision=emitter_revision(),
        materializer_revision="sha256:" + "a" * 64,
    )
    assert seen and all(item is None for item in seen), seen


def test_the_recipe_intent_forwards_declarations_to_the_engine():
    """A recipe-kind intent compiles INSIDE normalization, before validation.

    So if the declarations do not reach `run_recipes` at that point, that intent
    kind can never use the channel however well the later sites are wired.
    """
    import inspect

    from boomi_mcp.authoring import workflow as _workflow

    source = inspect.getsource(_workflow._normalize_recipe_intent)
    assert "effect_declarations=effect_declarations" in source, source
    # ...and the caller supplies it from the request rather than defaulting.
    normalize = inspect.getsource(_workflow._normalize_intent)
    assert "_normalize_recipe_intent(intent, request.effect_declarations)" in normalize


# ---------------------------------------------------------------------------
# QA-154-r1-07: the REAL opaque rows, covered directly
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "family", ["defined_process_property_get", "defined_process_property_set"]
)
def test_defined_process_property_families_are_opaque_on_purpose(family):
    """A defined Process Property is NOT the `dpp` store lineage tracks.

    Annotating these as `dpp` would let a map declaration establish a `dpp` write
    that never happens — a false state claim reaching the lineage analysis. The
    previous coverage passed coincidentally: it drove the family with a
    parameters dict lacking the key an annotated version would read, so
    `derive_map_effect` returned None for the wrong reason and the assertion held
    either way (QA-154-r1-07).
    """
    from boomi_mcp.categories.components.builders.map_function_registry import (
        FUNCTION_FAMILIES,
    )

    assert FUNCTION_FAMILIES[family].effect_kind is None
    assert FUNCTION_FAMILIES[family].effect_scope is None


@pytest.mark.parametrize(
    "family,parameter",
    [
        ("defined_process_property_get", "property_name"),
        ("defined_process_property_set", "property_name"),
    ],
)
def test_annotating_a_defined_process_property_family_would_be_detected(family, parameter):
    """The mutation witness the coincidental test lacked.

    Annotates the REAL row as a `dpp` accessor and supplies the parameter such an
    annotation would read — so `derive_map_effect` cannot fall through for a
    missing-parameter reason. Under the mutant the map becomes DERIVABLE and
    claims a `dpp` effect; that difference is what the assertion detects.
    """
    import dataclasses
    from types import MappingProxyType

    from boomi_mcp.categories.components.builders import map_function_registry as reg

    mapping = {"function_type": family, "parameters": {parameter: "SOME_PROPERTY"}}
    # Baseline: opaque, and NOT because the parameter is missing.
    assert derive_map_effect({"map_type": "function", "function_mappings": [mapping]}) is None

    kind = "property_get" if family.endswith("_get") else "property_set"
    mutant = dataclasses.replace(
        reg.FUNCTION_FAMILIES[family],
        effect_kind=kind, effect_scope="dpp", effect_parameter=parameter,
    )
    assert mutant.effect_kind == kind  # the mutation took effect

    patched = dict(reg.FUNCTION_FAMILIES)
    patched[family] = mutant
    original = reg.FUNCTION_FAMILIES
    reg.FUNCTION_FAMILIES = MappingProxyType(patched)
    try:
        derived = derive_map_effect({"map_type": "function", "function_mappings": [mapping]})
        # Under the mutant the map DOES derive, and claims dpp state.
        assert derived is not None, "mutant had no effect — this witness is vacuous"
        reads, writes, _replay = derived
        assert ("dpp", "SOME_PROPERTY") in (reads + writes)
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
    assert derive_map_effect({"map_type": "direct"}) == ((), (), True)


def test_a_create_under_reuse_is_opaque_because_the_plan_may_substitute_it():
    roots = [("p", _root_with_map())]
    declarations = ProcessIREffectDeclarationsV1(map_effects=(
        ProcessIRMapEffectDeclarationV1(
            map_ref="$ref:MAP", effect=_effect(writes=[("dpp", "OUT")], replay_safe=True)),))
    components = [_map_component([
        {"function_type": "dynamic_process_property_set", "parameters": {"property_name": "OUT"}}])]

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
    derived = derive_map_effect({"map_type": "function", "function_mappings": [
        {"function_type": "  SEQUENTIAL_VALUE  ", "parameters": {}}]})
    assert derived == ((), (), False), derived


def test_a_defaulted_property_get_records_no_strict_read():
    """A contract read carries no has-default flag and lineage treats every one
    as strict, so recording a defaulted read would fail a flow that runs fine."""
    defaulted = derive_map_effect({"map_type": "function", "function_mappings": [
        {"function_type": "dynamic_process_property_get",
         "parameters": {"property_name": "P", "default_value": "fallback"}}]})
    assert defaulted == ((), (), True), defaulted
    # CONTROL: without the default it IS a read, so the omission is about the
    # default rather than about the derivation never recording reads.
    plain = derive_map_effect({"map_type": "function", "function_mappings": [
        {"function_type": "dynamic_process_property_get",
         "parameters": {"property_name": "P"}}]})
    assert plain == ((("dpp", "P"),), (), True), plain
