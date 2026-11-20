"""The component-material channel and per-invocation provenance (issue #159 S1).

A secrets/security-class change: ``RecipeInvocationRequestV1.component_material``
carries connection settings, SQL, URLs and paths into a TRUSTED server-side
preparer. The canaries below plant a unique value in every material field and
prove, at the public authoring boundary, that none of them reaches:

* the recipe executor's input, any contribution, the run provenance or the run
  diagnostics;
* any served result field OTHER than the component-plan preview's component
  ``config`` (the materialization echo every intent already serves for its
  components — measured and pinned below as the one allowlisted location);
* any error, diagnostic, repr, log record or warning — on the success path and
  on every failure path.
"""

from __future__ import annotations

import copy
import json
import logging
import sys
import warnings
from pathlib import Path

import pytest

_TESTS = Path(__file__).resolve().parent
_SRC = _TESTS.parent / "src"
for _path in (str(_SRC), str(_TESTS)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from _fanout_v2_support import RECIPE_ID, V2, flagship_v2, recipe_request  # noqa: E402

from boomi_mcp.authoring.workflow import (  # noqa: E402
    AuthoringWorkflowError,
    compile_authoring_request_v1,
    plan_authoring_request_v1,
)
from boomi_mcp.errors import (  # noqa: E402
    AUTHORING_COMPILE_BLOCKED,
    AUTHORING_PLAN_STALE,
    COMPOSITION_UNSUPPORTED_TOPOLOGY,
    GOVERNANCE_RETIRED_SPELLING,
    GOVERNANCE_SOURCE_CONFLICT,
    RECIPE_CONSTRAINT_FAILED,
    RECIPE_INPUT_INVALID,
)
from boomi_mcp.models.authoring_workflow import (  # noqa: E402
    AuthoringRequestV1,
    RecipeInvocationRequestV1,
)
from boomi_mcp.recipes import engine as engine_module  # noqa: E402
from boomi_mcp.recipes.builtins import catalog as catalog_module  # noqa: E402
from boomi_mcp.recipes.builtins.fanout import (  # noqa: E402
    ComposeDbRestFanoutInputV2,
    FanoutComponentMaterialV2,
    emit_db_rest_fanout_v2,
    prepare_db_rest_fanout_v2,
)
from boomi_mcp.recipes.contracts import RecipeRegistrationV1  # noqa: E402
from boomi_mcp.recipes.engine import RecipeRequestV1, run_recipes  # noqa: E402
from boomi_mcp.recipes.errors import RecipeError  # noqa: E402
from boomi_mcp.recipes.materialization import MaterializationCatalog  # noqa: E402
from boomi_mcp.recipes.registry import RecipeRegistry  # noqa: E402


# ---------------------------------------------------------------------------
# Canary material
# ---------------------------------------------------------------------------

#: One unique, greppable value per material field that carries a value.
CANARIES = {
    "host": "canary-host-5c1e.invalid",
    "database": "CanaryDb5c1e",
    "username": "canary_user_5c1e",
    "credential_ref": "credential://canary-cred-5c1e",
    "sql": "SELECT canary_sql_5c1e FROM t",
    "base_url_0": "https://canary-url-a-5c1e.invalid",
    "base_url_1": "https://canary-url-b-5c1e.invalid",
    "path_0": "/canary-path-a-5c1e",
    "path_1": "/canary-path-b-5c1e",
    "query_literal": "canary-qv-5c1e",
}


def canary_v2():
    raw, material = flagship_v2()
    connection = material["source"]["connection"]
    connection.update(
        host=CANARIES["host"],
        database=CANARIES["database"],
        username=CANARIES["username"],
        credential_ref=CANARIES["credential_ref"],
    )
    material["source"]["read"]["sql"] = CANARIES["sql"]
    for index in (0, 1):
        target = material["targets"][index]
        target["connection"]["base_url"] = CANARIES[f"base_url_{index}"]
        target["operation"]["path"] = CANARIES[f"path_{index}"]
    material["targets"][0]["operation"]["query_parameters"] = [
        {"name": "q", "value_source": "literal", "literal_value": CANARIES["query_literal"]}
    ]
    return raw, material


def _leaks(text: str):
    return sorted(name for name, value in CANARIES.items() if value in text)


def _without_preview_configs(dump):
    """The served dump with the ONE allowlisted location removed."""
    stripped = copy.deepcopy(dump)
    for component in stripped["integration_spec_preview"]["components"]:
        component["config"] = "<allowlisted materialization echo>"
    return stripped


# ---------------------------------------------------------------------------
# Validation per the RESOLVED registration
# ---------------------------------------------------------------------------


def _blocked(request):
    with pytest.raises(AuthoringWorkflowError) as caught:
        plan_authoring_request_v1(request, profile="p")
    assert caught.value.code == AUTHORING_COMPILE_BLOCKED
    return caught.value.diagnostics


def test_material_is_refused_by_a_registration_that_declares_none():
    raw, material = flagship_v2()
    (diagnostic,) = _blocked(recipe_request({"version": "1"}, material, recipe_version=None))
    assert diagnostic.path == "/intent/invocations/0/component_material"
    assert RECIPE_INPUT_INVALID in diagnostic.cause_codes
    assert diagnostic.subject_id == "component_material_not_declared"


def test_material_is_required_by_a_registration_that_declares_it():
    raw, _material = flagship_v2()
    (diagnostic,) = _blocked(recipe_request(raw, None))
    assert diagnostic.path == "/intent/invocations/0/component_material"
    assert diagnostic.subject_id == "component_material_required"


@pytest.mark.parametrize(
    "edit, pointer, extra_cause",
    [
        pytest.param(
            lambda m: m["source"]["connection"].update(smuggled="x"),
            # Truncated at the last DECLARED segment: the unknown key's name is
            # caller content and is never served.
            "/intent/invocations/0/component_material/source/connection/create",
            None,
            id="unknown_key",
        ),
        pytest.param(
            lambda m: m["source"]["connection"].update(jdbc_options={"a": "b"}),
            "/intent/invocations/0/component_material/source/connection/jdbc_options",
            GOVERNANCE_RETIRED_SPELLING,
            id="retired_spelling",
        ),
        pytest.param(
            lambda m: m["source"]["connection"].update(
                {"canary-key-5c1e": {"jdbc_options": {"a": "b"}}}
            ),
            "/intent/invocations/0/component_material/source/connection",
            GOVERNANCE_RETIRED_SPELLING,
            id="retired_spelling_under_a_caller_key",
        ),
        pytest.param(
            lambda m: m["targets"][0]["connection"].update(mode="oauth"),
            "/intent/invocations/0/component_material/targets/0/connection",
            None,
            id="unknown_binding_mode",
        ),
    ],
)
def test_the_material_model_is_closed_and_strict(edit, pointer, extra_cause):
    raw, material = flagship_v2()
    edit(material)
    (diagnostic,) = _blocked(recipe_request(raw, material))
    assert diagnostic.path == pointer
    assert "canary-key-5c1e" not in diagnostic.model_dump_json()
    assert RECIPE_INPUT_INVALID in diagnostic.cause_codes
    if extra_cause is not None:
        assert extra_cause in diagnostic.cause_codes


def test_a_forbidden_raw_input_key_is_pointed_into_the_invocations_raw_input():
    raw, material = flagship_v2()
    raw["source"]["host"] = "x"
    (diagnostic,) = _blocked(recipe_request(raw, material))
    assert diagnostic.path == "/intent/invocations/0/raw_input/source/host"
    assert RECIPE_INPUT_INVALID in diagnostic.cause_codes


def test_a_composition_refusal_reaches_the_caller_with_cause_and_pointer():
    raw, material = flagship_v2()
    material["targets"][1]["operation"]["query_parameters"] = [
        {"name": "since", "value_source": "watermark"}
    ]
    (diagnostic,) = _blocked(recipe_request(raw, material))
    assert diagnostic.path == (
        "/intent/invocations/0/component_material/targets/1/operation/query_parameters/0"
    )
    assert set(diagnostic.cause_codes) == {COMPOSITION_UNSUPPORTED_TOPOLOGY, RECIPE_CONSTRAINT_FAILED}


def test_an_envelope_naming_a_recipe_emitted_unit_is_a_source_conflict():
    raw, material = flagship_v2()
    request = recipe_request(
        raw, material, process_envelopes=[{"component_key": "main_process", "name": "Other"}]
    )
    with pytest.raises(AuthoringWorkflowError) as caught:
        plan_authoring_request_v1(request, profile="p")
    assert caught.value.code == GOVERNANCE_SOURCE_CONFLICT
    assert caught.value.diagnostics[0].path == "/intent/process_envelopes"


def _registration(**overrides):
    base = next(
        reg
        for reg in catalog_module.PRODUCTION_REGISTRATIONS
        if reg.recipe_id == RECIPE_ID and reg.recipe_version == V2
    )
    fields = {name: getattr(base, name) for name in base.__dataclass_fields__}
    fields.update(overrides)
    return RecipeRegistrationV1(**fields)


def test_the_production_material_model_passes_the_input_closedness_gates():
    from boomi_mcp.recipes import registry as registry_module

    registry_module._check_input_schema_closed(RECIPE_ID, FanoutComponentMaterialV2)
    registry_module._check_input_model_forbids_extras(RECIPE_ID, FanoutComponentMaterialV2)


# ---------------------------------------------------------------------------
# Provenance (C7)
# ---------------------------------------------------------------------------


def spy_executor(inp):
    """A module-level executor that records what it was handed."""
    SEEN_INPUTS.append(inp.model_dump_json())
    return emit_db_rest_fanout_v2(inp)


SEEN_INPUTS: list = []


def _registry(*registrations):
    return RecipeRegistry(tuple(registrations))


def _hashes(monkeypatch, registry, version):
    monkeypatch.setattr(engine_module, "production_registry", lambda: registry)
    raw, material = flagship_v2()
    result, _ = plan_authoring_request_v1(
        recipe_request(raw, material, recipe_version=version), profile="p"
    )
    return result


def test_recipe_provenance_is_served_and_registry_derived():
    raw, material = flagship_v2()
    result, _ = compile_authoring_request_v1(recipe_request(raw, material), profile="p")
    (record,) = result.recipe_provenance
    from boomi_mcp.recipes import production_registry

    descriptor = production_registry().resolve(RECIPE_ID, V2)
    assert record.recipe_id == RECIPE_ID
    assert record.recipe_version == V2
    assert record.invocation_id == "fanout"
    assert record.implementation_sha256 == descriptor.provenance.implementation_sha256
    assert record.source_revision == descriptor.provenance.source_revision


def test_a_default_and_an_explicit_pin_of_one_descriptor_bind_identically(monkeypatch):
    registry = _registry(_registration(is_default=True))
    pinned = _hashes(monkeypatch, registry, V2)
    default = _hashes(monkeypatch, registry, None)
    assert pinned.recipe_provenance == default.recipe_provenance
    assert pinned.revision_binding.semantic_hash == default.revision_binding.semantic_hash
    assert pinned.revision_binding.plan_hash == default.revision_binding.plan_hash


def test_two_versions_composing_identical_ir_bind_to_different_hashes(monkeypatch):
    registry = _registry(_registration(), _registration(recipe_version="2.0.1"))
    first = _hashes(monkeypatch, registry, V2)
    second = _hashes(monkeypatch, registry, "2.0.1")
    assert first.integration_spec_preview == second.integration_spec_preview
    assert first.revision_binding.semantic_hash != second.revision_binding.semantic_hash


def test_an_implementation_change_at_one_version_moves_the_hash(monkeypatch):
    original = _hashes(monkeypatch, _registry(_registration()), V2)
    changed = _hashes(monkeypatch, _registry(_registration(executor=spy_executor)), V2)
    assert original.integration_spec_preview == changed.integration_spec_preview
    assert (
        original.recipe_provenance[0].implementation_sha256
        != changed.recipe_provenance[0].implementation_sha256
    )
    assert original.revision_binding.semantic_hash != changed.revision_binding.semantic_hash


def test_a_binding_from_other_recipe_code_is_stale_at_compile(monkeypatch):
    raw, material = flagship_v2()
    monkeypatch.setattr(engine_module, "production_registry", lambda: _registry(_registration()))
    bound, _ = compile_authoring_request_v1(recipe_request(raw, material), profile="p")

    monkeypatch.setattr(
        engine_module,
        "production_registry",
        lambda: _registry(_registration(executor=spy_executor)),
    )
    request = recipe_request(raw, material)
    stale = AuthoringRequestV1.model_validate(
        {
            **request.model_dump(mode="json"),
            "expected_compile_hash": bound.revision_binding.compile_hash,
        }
    )
    with pytest.raises(AuthoringWorkflowError) as caught:
        compile_authoring_request_v1(stale, profile="p")
    assert caught.value.code == AUTHORING_PLAN_STALE


def test_direct_intent_payloads_carry_no_recipe_provenance_key():
    from boomi_mcp.authoring.workflow import _NormalizedIntent, _normalized_payload
    from boomi_mcp.models.integration_models import IntegrationSpecV1

    request = AuthoringRequestV1.model_validate(
        {
            "intent": {
                "intent_kind": "integration_spec",
                "integration_spec": {"name": "x", "components": []},
            }
        }
    )
    normalized = _NormalizedIntent(
        integration_spec=IntegrationSpecV1(name="x"), gaps=(), connector_metadata={}
    )
    assert "recipe_provenance" not in _normalized_payload(normalized, request)


# ---------------------------------------------------------------------------
# Secret canaries
# ---------------------------------------------------------------------------


def test_canaries_never_reach_the_executor_contributions_provenance_or_diagnostics(monkeypatch):
    raw, material = canary_v2()
    prepared = prepare_db_rest_fanout_v2(
        ComposeDbRestFanoutInputV2.model_validate(raw),
        FanoutComponentMaterialV2.model_validate(material),
        base_catalog={},
    )
    SEEN_INPUTS.clear()
    registry = _registry(_registration(executor=spy_executor))
    from boomi_mcp.authoring.workflow import _connector_metadata_from_components

    result = run_recipes(
        [RecipeRequestV1(RECIPE_ID, "fanout", raw, V2)],
        catalog=MaterializationCatalog({}),
        registry=registry,
        connector_metadata=_connector_metadata_from_components(prepared.supporting_components),
        supporting_components=prepared.supporting_components,
    )
    assert SEEN_INPUTS and all(_leaks(seen) == [] for seen in SEEN_INPUTS)
    composed = result.composed
    attributed = (
        list(composed.process_unit_contributions)
        + list(composed.constraints)
        + list(composed.recorded_intents)
    )
    for item in attributed:
        assert _leaks(item.contribution.model_dump_json()) == []
    assert _leaks(json.dumps([r.model_dump(mode="json") for r in result.invocation_provenance])) == []
    assert _leaks(json.dumps(result.provenance)) == []
    assert _leaks(json.dumps([d.model_dump(mode="json") for d in result.diagnostics])) == []
    # Non-vacuity: the canaries DID reach the supporting specs.
    assert _leaks(json.dumps([c.model_dump(mode="json") for c in prepared.supporting_components])) == sorted(
        CANARIES
    )


def test_canaries_appear_on_served_results_only_in_the_preview_component_configs(caplog):
    raw, material = canary_v2()
    caplog.set_level(logging.DEBUG)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        plan, _ = plan_authoring_request_v1(recipe_request(raw, material), profile="p")
        compiled, _ = compile_authoring_request_v1(recipe_request(raw, material), profile="p")

    for result in (plan, compiled):
        dump = result.model_dump(mode="json")
        assert _leaks(json.dumps(_without_preview_configs(dump))) == []
        # The allowlisted location is measured, not assumed: it carries exactly
        # the materialized connection/profile/operation values.
        assert _leaks(json.dumps(dump)) == sorted(CANARIES)
    assert _leaks("\n".join(record.getMessage() for record in caplog.records)) == []
    assert _leaks("\n".join(str(w.message) for w in caught)) == []


def _fail_with(edit):
    raw, material = canary_v2()
    edit(raw, material)
    return raw, material


_FAILURES = {
    "authority_value_rule": lambda r, m: m["targets"][1]["connection"].update(
        base_url="not a url " + CANARIES["base_url_1"]
    ),
    "unknown_key_carrying_a_canary": lambda r, m: m["source"]["connection"].update(
        smuggled=CANARIES["host"]
    ),
    "auth_consistency": lambda r, m: m["source"]["connection"].pop("username"),
    "contract_mismatch": lambda r, m: m["transform"]["operations"][0].update(
        source_field=CANARIES["sql"]
    ),
    "later_target_watermark": lambda r, m: m["targets"][1]["operation"].update(
        query_parameters=[{"name": CANARIES["query_literal"], "value_source": "watermark"}]
    ),
    # The DB read-profile BUILDER refuses a blank statement (MISSING_DB_QUERY);
    # every other canary is still in the material.
    "builder_refusal": lambda r, m: m["source"]["read"].update(sql="   "),
    "material_on_an_undeclared_registration": lambda r, m: r.clear() or r.update(version="1"),
}


@pytest.mark.parametrize("case", sorted(_FAILURES))
def test_canaries_never_reach_an_error_on_any_failure_path(case, caplog):
    raw, material = _fail_with(_FAILURES[case])
    version = None if case == "material_on_an_undeclared_registration" else V2
    caplog.set_level(logging.DEBUG)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(AuthoringWorkflowError) as refused:
            compile_authoring_request_v1(
                recipe_request(raw, material, recipe_version=version), profile="p"
            )
    error = refused.value
    rendered = "\n".join(
        [
            str(error),
            repr(error),
            json.dumps([d.model_dump(mode="json") for d in error.diagnostics]),
            "\n".join(record.getMessage() for record in caplog.records),
            "\n".join(str(w.message) for w in caught),
        ]
    )
    assert _leaks(rendered) == []
    assert error.__cause__ is None and error.__suppress_context__


def test_material_reprs_are_redacted():
    raw, material = canary_v2()
    invocation = RecipeInvocationRequestV1.model_validate(
        {"recipe_id": RECIPE_ID, "invocation_id": "x", "raw_input": raw, "component_material": material}
    )
    request = recipe_request(raw, material)
    validated = FanoutComponentMaterialV2.model_validate(material)
    prepared = prepare_db_rest_fanout_v2(
        ComposeDbRestFanoutInputV2.model_validate(raw), validated, base_catalog={}
    )
    for rendered in (repr(invocation), repr(request), str(request), repr(validated), repr(prepared)):
        assert _leaks(rendered) == []


def test_the_recipe_input_model_cannot_carry_material():
    """Structural: no v2 input field can hold a material value."""
    raw, material = canary_v2()
    poisoned = dict(raw, host=CANARIES["host"])
    with pytest.raises(Exception):
        ComposeDbRestFanoutInputV2.model_validate(poisoned)
    assert _leaks(ComposeDbRestFanoutInputV2.model_validate(raw).model_dump_json()) == []


# ---------------------------------------------------------------------------
# The MCP tool boundary, with an account in hand
# ---------------------------------------------------------------------------


def _strip_tool_configs(envelope):
    stripped = copy.deepcopy(envelope)
    for components in (
        (stripped.get("integration_spec") or {}).get("components") or (),
        ((stripped.get("authoring_result") or {}).get("integration_spec_preview") or {}).get(
            "components"
        )
        or (),
    ):
        for component in components:
            component["config"] = "<allowlisted materialization echo>"
    return stripped


def test_the_tool_boundary_with_a_client_serves_canaries_only_in_component_configs(caplog):
    """``build_integration(action='plan')`` WITH an account: the legacy
    component-plan lint runs and its warnings are served — outside the
    allowlist, so they must carry no material value."""
    from unittest import mock

    from boomi_mcp.categories import integration_builder

    raw, material = canary_v2()
    payload = recipe_request(raw, material).model_dump(mode="json")
    caplog.set_level(logging.DEBUG)
    with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []):
        envelope = integration_builder.build_integration_action(
            mock.MagicMock(), "p", "plan", {"authoring_request": payload}
        )
    assert envelope["_success"] is True, envelope.get("error")
    assert _leaks(json.dumps(_strip_tool_configs(envelope))) == []
    assert _leaks(json.dumps(envelope)) == sorted(CANARIES)
    assert _leaks("\n".join(record.getMessage() for record in caplog.records)) == []


@pytest.mark.parametrize(
    "edit",
    [
        pytest.param(lambda inv: inv.update(bogus=CANARIES["host"]), id="extra_sibling_field"),
        pytest.param(lambda inv: inv.pop("invocation_id"), id="missing_required_sibling"),
        pytest.param(lambda inv: inv.update(recipe_version=7), id="wrong_typed_sibling"),
    ],
)
def test_a_request_parse_failure_at_the_tool_boundary_echoes_no_material(edit):
    from unittest import mock

    from boomi_mcp.categories.integration_builder import build_integration_action

    raw, material = canary_v2()
    payload = recipe_request(raw, material).model_dump(mode="json")
    edit(payload["intent"]["invocations"][0])
    envelope = build_integration_action(
        mock.MagicMock(), "p", "plan", {"authoring_request": payload}
    )
    assert envelope["_success"] is False
    assert _leaks(json.dumps(envelope)) == []
