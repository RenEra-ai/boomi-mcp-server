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
            "/intent/invocations/0/component_material/source/connection",
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


def _resolves(request_payload, pointer):
    """True when the JSON pointer addresses a real location of the request."""
    node = request_payload
    for part in pointer.split("/")[1:]:
        if isinstance(node, list):
            node = node[int(part)]
        else:
            node = node[part]
    return True


@pytest.mark.parametrize(
    "edit, pointer",
    [
        pytest.param(
            lambda m: m["source"]["connection"].update(port="not-a-port"),
            "/intent/invocations/0/component_material/source/connection/port",
            id="create_variant_field",
        ),
        pytest.param(
            lambda m: m["source"]["connection"].update(smuggled="x"),
            "/intent/invocations/0/component_material/source/connection",
            id="create_variant_unknown_key",
        ),
        pytest.param(
            lambda m: m["targets"][0]["connection"].update(base_url=7),
            "/intent/invocations/0/component_material/targets/0/connection/base_url",
            id="rest_create_variant_field",
        ),
        pytest.param(
            lambda m: m["targets"][0]["operation"].update(path=7),
            "/intent/invocations/0/component_material/targets/0/operation/path",
            id="no_tag_control",
        ),
    ],
)
def test_a_material_pointer_addresses_a_real_request_location(edit, pointer):
    raw, material = flagship_v2()
    edit(material)
    request = recipe_request(raw, material)
    (diagnostic,) = _blocked(request)
    assert diagnostic.path == pointer
    assert _resolves(request.model_dump(mode="json"), diagnostic.path)
    assert "/create" not in diagnostic.path and "/reuse" not in diagnostic.path


def test_a_reuse_variant_error_drops_the_tag_and_keeps_declared_fields():
    raw, material = flagship_v2()
    reuse = next(
        (t for t in material["targets"] if t["connection"].get("mode") == "reuse"), None
    )
    if reuse is None:
        reuse = material["targets"][0]
        reuse["connection"] = {"mode": "reuse", "component_id": "x", "component_name": "n"}
    reuse["connection"]["component_id"] = 7
    index = material["targets"].index(reuse)
    request = recipe_request(raw, material)
    (diagnostic,) = _blocked(request)
    assert diagnostic.path == (
        f"/intent/invocations/0/component_material/targets/{index}/connection/component_id"
    )
    assert _resolves(request.model_dump(mode="json"), diagnostic.path)


def test_a_forbidden_raw_input_key_is_pointed_into_the_invocations_raw_input():
    """The forbidden key is the caller's own key, so since CDX-r1-03 the pointer
    stops at its nearest DECLARED parent (it used to end ON the key)."""
    raw, material = flagship_v2()
    raw["source"]["host"] = "x"
    (diagnostic,) = _blocked(recipe_request(raw, material))
    assert diagnostic.path == "/intent/invocations/0/raw_input/source"
    assert RECIPE_INPUT_INVALID in diagnostic.cause_codes


# ---------------------------------------------------------------------------
# CDX-r1-03: a scan pointer never serves a caller-authored key name
# ---------------------------------------------------------------------------

_KEY_CANARY = "hunter2c4f1"


@pytest.mark.parametrize(
    "edit, owner",
    [
        pytest.param(
            lambda raw: raw["source"].update({"password_" + _KEY_CANARY: "x"}),
            "source",
            id="secret_shaped_key",
        ),
        pytest.param(
            lambda raw: raw["source"].update({"wrap_" + _KEY_CANARY: {"api_key": "v"}}),
            "source",
            id="forbidden_key_under_an_undeclared_caller_key",
        ),
        pytest.param(
            lambda raw: raw["targets"][1].update({"token_" + _KEY_CANARY: "x"}),
            "targets/1",
            id="inside_a_declared_array_item",
        ),
    ],
)
def test_a_secret_shaped_raw_input_key_is_pointed_at_its_declared_parent(edit, owner, caplog):
    raw, material = flagship_v2()
    edit(raw)
    caplog.set_level(logging.DEBUG)
    request = recipe_request(raw, material)
    with pytest.raises(AuthoringWorkflowError) as caught:
        plan_authoring_request_v1(request, profile="p")
    (diagnostic,) = caught.value.diagnostics
    assert diagnostic.path == f"/intent/invocations/0/raw_input/{owner}"
    assert _resolves(request.model_dump(mode="json"), diagnostic.path)
    assert RECIPE_INPUT_INVALID in diagnostic.cause_codes
    served = json.dumps([d.model_dump(mode="json") for d in caught.value.diagnostics])
    assert _KEY_CANARY not in served
    assert _KEY_CANARY not in str(caught.value) and _KEY_CANARY not in repr(caught.value)
    assert _KEY_CANARY not in "\n".join(record.getMessage() for record in caplog.records)


def test_the_engine_envelope_the_legacy_route_serves_never_carries_the_key():
    """``run_recipes`` -> ``recipe_error_envelope`` is what the legacy compose
    route serves; the walk lives in the engine, so it covers that route too."""
    from boomi_mcp.recipes.errors import recipe_error_envelope

    raw, _material = flagship_v2()
    raw["source"]["password_" + _KEY_CANARY] = "x"
    with pytest.raises(RecipeError) as caught:
        run_recipes(
            [RecipeRequestV1(RECIPE_ID, "fanout", raw, V2)],
            catalog=MaterializationCatalog({}),
        )
    envelope = json.dumps(recipe_error_envelope(caught.value))
    assert [d.path for d in caught.value.diagnostics] == ["/source"]
    assert _KEY_CANARY not in envelope and _KEY_CANARY not in str(caught.value)
    # Non-vacuity: the scan DID find the planted key (the walk is what hid it).
    from boomi_mcp.models.recipe_contributions import scan_forbidden_recipe_shape

    assert _KEY_CANARY in str(scan_forbidden_recipe_shape(raw)[0])


def test_a_retired_spelling_under_a_digit_only_caller_key_does_not_serve_the_key():
    """The retired-spelling branch of the material pointer walks the raw path
    with the shared walker: a digit-only DICT key is caller content, not an
    array index (the CDX-r1-03 sweep found the name-blind digit rule)."""
    raw, material = flagship_v2()
    material["source"]["connection"]["904417"] = {"jdbc_options": {"a": "b"}}
    (diagnostic,) = _blocked(recipe_request(raw, material))
    assert diagnostic.path == "/intent/invocations/0/component_material/source/connection"
    assert "904417" not in diagnostic.model_dump_json()
    assert GOVERNANCE_RETIRED_SPELLING in diagnostic.cause_codes


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


def _typed_apply(payload):
    """The typed apply through the TOOL boundary, network faked, writes spied."""
    from unittest.mock import MagicMock

    from boomi_mcp.categories import integration_builder

    with pytest.MonkeyPatch.context() as patcher:
        patcher.setattr(integration_builder, "paginate_metadata", lambda *a, **k: [])
        return integration_builder.build_integration_action(
            MagicMock(),
            "p",
            "apply",
            config={"authoring_request": payload, "dry_run": False},
        )


def test_a_binding_from_other_recipe_code_is_stale_at_typed_apply(monkeypatch):
    """C7: the binding from one recipe implementation cannot APPLY under another.

    Compiled under one registered implementation, then applied — through the
    typed apply route, with the binding's capability revision and compile hash —
    after the same id/version was re-registered with a different executor. The
    IR is identical; only the recipe provenance in the semantic payload differs.
    The apply must refuse with AUTHORING_PLAN_STALE on ``compile_hash``, before
    any write door is touched. The control applies the same binding under the
    SAME code and gets past the gate, so the refusal is not vacuous.
    """
    from _m12_11_support import MutationSpy

    spy = MutationSpy().install(monkeypatch)
    raw, material = flagship_v2()
    original = _registry(_registration())
    monkeypatch.setattr(engine_module, "production_registry", lambda: original)
    bound, _ = compile_authoring_request_v1(recipe_request(raw, material), profile="p")
    payload = recipe_request(raw, material).model_dump(mode="json")
    payload["expected_capability_revision"] = bound.revision_binding.capability_revision
    payload["expected_compile_hash"] = bound.revision_binding.compile_hash

    from boomi_mcp.authoring.workflow import preflight_typed_apply_v1

    # control: the same code reproduces the binding and passes the gate
    bundle = preflight_typed_apply_v1(dict(payload), profile="p")
    assert bundle.compile_result.revision_binding.compile_hash == (
        bound.revision_binding.compile_hash
    )

    changed = _registry(_registration(executor=spy_executor))
    monkeypatch.setattr(engine_module, "production_registry", lambda: changed)
    result = _typed_apply(payload)

    assert result["_success"] is False
    assert result["error_code"] == AUTHORING_PLAN_STALE
    assert result["mutation_performed"] is False
    with pytest.raises(AuthoringWorkflowError) as caught:
        preflight_typed_apply_v1(dict(payload), profile="p")
    assert caught.value.code == AUTHORING_PLAN_STALE
    assert [d.subject_id for d in caught.value.diagnostics] == ["compile_hash"]
    assert spy.calls == []


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


#: Caller content the legacy flows/endpoint rows SERVED: a transform hint and a
#: reuse selector. Each has an exact served home and must appear nowhere else.
HINT_CANARY = "canary-hint-7d2a"
SELECTOR_CANARY = "canary-selector-7d2a"


def test_hint_and_reuse_selector_canaries_appear_only_where_the_rows_serve_them(monkeypatch):
    """D9 for the #157 projection (#159 S1): the projection facts carry one piece
    of material — the documentation hint — and the endpoint rows serve a reuse
    selector. Pin both homes, and assert absence everywhere else, including the
    facts' and the normalized intent's reprs and the executor's input."""
    raw, material = flagship_v2()
    material["transform"]["operations"][0]["documentation_hint"] = HINT_CANARY
    raw["targets"][1]["binding_mode"] = "reuse"
    material["targets"][1]["connection"] = {"mode": "reuse", "component_id": SELECTOR_CANARY}
    SEEN_INPUTS.clear()
    spying = _registry(_registration(executor=spy_executor))
    monkeypatch.setattr(engine_module, "production_registry", lambda: spying)
    result, internals = plan_authoring_request_v1(recipe_request(raw, material), profile="p")
    dump = result.model_dump(mode="json")
    preview = dump["integration_spec_preview"]

    # the homes, measured (non-vacuity)
    (transform,) = [row for row in preview["flows"] if row["key"] == "transform"]
    assert [op.get("documentation_hint") for op in transform["operations"]][0] == HINT_CANARY
    assert [e.get("component_id") for e in preview["endpoints"]].count(SELECTOR_CANARY) == 1

    stripped = copy.deepcopy(dump)
    for op in [r for r in stripped["integration_spec_preview"]["flows"] if r["key"] == "transform"][0][
        "operations"
    ]:
        op.pop("documentation_hint", None)
    for endpoint in stripped["integration_spec_preview"]["endpoints"]:
        endpoint.pop("component_id", None)
    for component in stripped["integration_spec_preview"]["components"]:
        component["config"] = "<allowlisted materialization echo>"
        component["component_id"] = None
    # the pre-existing served home of every reuse id: the resolved references
    assert SELECTOR_CANARY in [r["component_id"] for r in stripped["resolved_references"]]
    for reference in stripped["resolved_references"]:
        reference["component_id"] = None
    text = json.dumps(stripped)
    assert HINT_CANARY not in text and SELECTOR_CANARY not in text

    facts = internals.normalized.recipe_projection_facts
    assert facts and HINT_CANARY not in repr(facts)
    assert HINT_CANARY not in repr(internals.normalized)
    assert SEEN_INPUTS
    assert all(HINT_CANARY not in seen and SELECTOR_CANARY not in seen for seen in SEEN_INPUTS)
    assert HINT_CANARY not in json.dumps(
        [r.model_dump(mode="json") for r in result.recipe_provenance]
        + [d.model_dump(mode="json") for d in result.recipe_diagnostics]
    )


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


# ---------------------------------------------------------------------------
# QA-159-S1-r2-01: a raw_input refusal names the invocation and the declared field
# ---------------------------------------------------------------------------


def _second_invocation(raw, material):
    return {
        "recipe_id": RECIPE_ID,
        "recipe_version": V2,
        "invocation_id": "second",
        "raw_input": copy.deepcopy(raw),
        "component_material": copy.deepcopy(material),
    }


def _input_refusal(raw_bad, *, at_index):
    raw, material = flagship_v2()
    if at_index == 0:
        request = recipe_request(raw_bad, material)
    else:
        request = recipe_request(
            raw, material, extra_invocations=(_second_invocation(raw_bad, material),)
        )
    diagnostics = _blocked(request)
    assert diagnostics, "the refusal carried no diagnostic"
    assert RECIPE_INPUT_INVALID in diagnostics[0].cause_codes
    return diagnostics[0]


@pytest.mark.parametrize("at_index", [0, 1])
def test_a_raw_input_refusal_points_at_the_declared_field_of_its_invocation(at_index):
    raw, _material = flagship_v2()
    bad = copy.deepcopy(raw)
    bad["source"]["binding_mode"] = "not-a-mode"
    diagnostic = _input_refusal(bad, at_index=at_index)
    assert diagnostic.path == f"/intent/invocations/{at_index}/raw_input/source/binding_mode"


def test_an_unknown_raw_input_key_is_placed_without_serving_its_name():
    raw, _material = flagship_v2()
    bad = copy.deepcopy(raw)
    bad["source"]["canary_key_5c1e"] = "canary-value-5c1e"
    diagnostic = _input_refusal(bad, at_index=1)
    assert diagnostic.path == "/intent/invocations/1/raw_input/source"
    served = json.dumps(diagnostic.model_dump(mode="json"))
    assert "canary_key_5c1e" not in served and "canary-value-5c1e" not in served


def test_a_cross_field_raw_input_refusal_names_the_object_that_failed():
    raw, _material = flagship_v2()
    bad = copy.deepcopy(raw)
    assert bad["source"]["binding_mode"] == "create"
    bad["source"].pop("auth_mode")
    diagnostic = _input_refusal(bad, at_index=0)
    assert diagnostic.path.startswith("/intent/invocations/0/raw_input/source")


def test_a_v1_raw_input_refusal_names_its_invocations_input():
    """The engine's own invocation loop (a registration without material) places
    the refusal too: under the invocation's ``raw_input``, the key name unserved."""
    request = recipe_request({"canary_key_5c1e": 1}, None, recipe_version=None)
    (diagnostic,) = _blocked(request)
    assert RECIPE_INPUT_INVALID in diagnostic.cause_codes
    # The first refusal is the missing required field: a declared location.
    assert diagnostic.path == "/intent/invocations/0/raw_input/version"
    assert "canary_key_5c1e" not in json.dumps(diagnostic.model_dump(mode="json"))


# ---------------------------------------------------------------------------
# CDX-r1-01/02: the credential boundary — ONE check, every secret-capable field
# ---------------------------------------------------------------------------

#: The enumerated secret-capable set (the structural fix's sibling sweep). A
#: field joins it only with a rule that can REFUSE plaintext there.
SECRET_CAPABLE_FIELDS = {
    ("FanoutDbCreateMaterialV2", "credential_ref"),
    ("FanoutDbCreateMaterialV2", "host"),
    ("FanoutDbCreateMaterialV2", "database"),
    ("FanoutRestCreateMaterialV2", "base_url"),
    ("FanoutRestOperationMaterialV2", "path"),
    ("FanoutQueryParameterV2", "literal_value"),
}


def _reachable_material_fields():
    """Every ``(model, field)`` reachable from the material model — DERIVED."""
    import typing

    from pydantic import BaseModel

    found, seen = set(), set()

    def models_in(annotation):
        stack, out = [annotation], []
        while stack:
            node = stack.pop()
            if isinstance(node, type) and issubclass(node, BaseModel):
                out.append(node)
                continue
            origin = typing.get_origin(node)
            if origin is typing.Annotated:
                stack.append(typing.get_args(node)[0])
            elif origin is not typing.Literal:
                stack.extend(typing.get_args(node))
        return out

    def walk(model):
        if model in seen:
            return
        seen.add(model)
        for name, field in model.model_fields.items():
            found.add((model, name))
            for nested in models_in(field.annotation):
                walk(nested)

    walk(FanoutComponentMaterialV2)
    return found


def test_the_credential_boundary_classifies_exactly_the_reachable_material_fields():
    from boomi_mcp.recipes.builtins import fanout

    table = fanout._material_field_classes_v2()
    assert set(table) == _reachable_material_fields()
    secret_capable = {(model.__name__, name) for (model, name), rule in table.items() if rule}
    assert secret_capable == SECRET_CAPABLE_FIELDS
    assert {rule for rule in table.values() if rule} <= set(fanout._SECRET_RULES)


def test_the_credential_boundary_fails_closed_on_an_unclassified_field(monkeypatch):
    """Non-vacuity of the fail-closed walk: drop ONE entry and a perfectly valid
    flagship is refused at exactly that field."""
    from boomi_mcp.recipes.builtins import fanout

    table = dict(fanout._material_field_classes_v2())
    table.pop((fanout.FanoutDbReadMaterialV2, "sql"))
    monkeypatch.setattr(fanout, "_material_field_classes_v2", lambda: table)
    raw, material = flagship_v2()
    (diagnostic,) = _blocked(recipe_request(raw, material))
    assert diagnostic.path == "/intent/invocations/0/component_material/source/read/sql"
    assert fanout._PLAINTEXT_SECRET_CAUSE in diagnostic.cause_codes


_SECRET_CANARY = "plaintext-5ecret-9b3d"


def _plant(field):
    def edit(material):
        connection = material["source"]["connection"]
        if field == "credential_ref":
            connection["credential_ref"] = _SECRET_CANARY
        elif field == "host":
            connection["host"] = "db.example.invalid;password=" + _SECRET_CANARY
        elif field == "database":
            connection["database"] = "AppDB;password=" + _SECRET_CANARY
        elif field == "base_url":
            material["targets"][1]["connection"]["base_url"] = (
                "https://svc:" + _SECRET_CANARY + "@billing.example.invalid"
            )
        elif field == "path":
            material["targets"][1]["operation"]["path"] = "/v1/bills?api_key=" + _SECRET_CANARY
        elif field == "literal_value":
            material["targets"][1]["operation"]["query_parameters"] = [
                {"name": "api_key", "value_source": "literal", "literal_value": _SECRET_CANARY}
            ]

    return edit


_PLANT_POINTERS = {
    "credential_ref": "/source/connection/credential_ref",
    "host": "/source/connection/host",
    "database": "/source/connection/database",
    "base_url": "/targets/1/connection/base_url",
    "path": "/targets/1/operation/path",
    "literal_value": "/targets/1/operation/query_parameters/0/literal_value",
}


def test_the_canary_witness_covers_every_secret_capable_field():
    assert set(_PLANT_POINTERS) == {name for _model, name in SECRET_CAPABLE_FIELDS}


@pytest.mark.parametrize("field", sorted(_PLANT_POINTERS))
def test_a_plaintext_canary_in_a_secret_capable_field_is_refused_value_free(field, caplog):
    from boomi_mcp.recipes.builtins import fanout

    raw, material = flagship_v2()
    _plant(field)(material)
    caplog.set_level(logging.DEBUG)
    with warnings.catch_warnings(record=True) as caught_warnings:
        warnings.simplefilter("always")
        with pytest.raises(AuthoringWorkflowError) as caught:
            plan_authoring_request_v1(recipe_request(raw, material), profile="p")
    (diagnostic,) = caught.value.diagnostics
    assert diagnostic.path == "/intent/invocations/0/component_material" + _PLANT_POINTERS[field]
    assert fanout._PLAINTEXT_SECRET_CAUSE in diagnostic.cause_codes
    served = json.dumps([d.model_dump(mode="json") for d in caught.value.diagnostics])
    assert _SECRET_CANARY not in served
    assert _SECRET_CANARY not in str(caught.value) and _SECRET_CANARY not in repr(caught.value)
    assert _SECRET_CANARY not in "\n".join(record.getMessage() for record in caplog.records)
    assert _SECRET_CANARY not in "\n".join(str(w.message) for w in caught_warnings)


def test_the_opaque_reference_forms_still_pass_the_boundary():
    """The control: the flagship (``credential://`` reference, plain host, base
    URL without userinfo, a literal under a non-secret name) plans cleanly."""
    raw, material = flagship_v2()
    material["targets"][1]["operation"]["query_parameters"] = [
        {"name": "mode", "value_source": "literal", "literal_value": "full"}
    ]
    plan, _ = plan_authoring_request_v1(recipe_request(raw, material), profile="p")
    assert plan.validation_report.is_valid


def test_the_neutral_rest_authority_is_unchanged_and_still_admits_userinfo():
    """CDX-r1-01 is fixed in the v2 boundary, NOT in ``RestCreateSettings``: the
    legacy route (and its frozen goldens) keep the authority they had."""
    from boomi_mcp.patterns.archetype_parameters import RestCreateSettings

    settings = RestCreateSettings(base_url="https://u:p@h.example.invalid", auth_mode="none")
    assert settings.base_url == "https://u:p@h.example.invalid"


# ---------------------------------------------------------------------------
# CDX-r2-02: REST query secrecy is the REST BUILDER's own classification
# ---------------------------------------------------------------------------

_PAGINATION_NAMES = ("pageToken", "nextPageToken", "page_token")
_BUILDER_SECRET_NAMES = ("api_key", "token", "Authorization")


def _builder_refuses(name, value="v"):
    from boomi_mcp.categories.components.builders.connector_builder import (
        RestClientOperationBuilder,
    )

    error = RestClientOperationBuilder._validate_dict_param({name: value}, "query_parameters")
    return getattr(error, "error_code", None) == "REST_SECRET_VALUE_FORBIDDEN"


@pytest.mark.parametrize("name", _PAGINATION_NAMES)
def test_a_pagination_query_name_plans_through_the_boundary(name):
    assert not _builder_refuses(name)  # the authority admits it
    raw, material = flagship_v2()
    material["targets"][1]["operation"]["query_parameters"] = [
        {"name": name, "value_source": "literal", "literal_value": "abc"}
    ]
    material["targets"][0]["operation"]["path"] += "?" + name + "=first"
    plan, _ = plan_authoring_request_v1(recipe_request(raw, material), profile="p")
    assert plan.validation_report.is_valid


@pytest.mark.parametrize("name", _BUILDER_SECRET_NAMES)
def test_a_query_name_the_rest_builder_refuses_is_still_refused(name):
    from boomi_mcp.recipes.builtins import fanout

    assert _builder_refuses(name)  # non-vacuity: the authority refuses it
    raw, material = flagship_v2()
    material["targets"][1]["operation"]["query_parameters"] = [
        {"name": name, "value_source": "literal", "literal_value": _SECRET_CANARY}
    ]
    (diagnostic,) = _blocked(recipe_request(raw, material))
    assert diagnostic.path == (
        "/intent/invocations/0/component_material/targets/1/operation/query_parameters/0/literal_value"
    )
    assert fanout._PLAINTEXT_SECRET_CAUSE in diagnostic.cause_codes
    assert _SECRET_CANARY not in diagnostic.model_dump_json()


def test_a_secret_shaped_value_under_a_plain_name_is_refused_by_the_value_rule():
    """The builder's VALUE patterns travel too: a bearer credential under an
    innocuous name is still a secret."""
    raw, material = flagship_v2()
    material["targets"][1]["operation"]["query_parameters"] = [
        {"name": "q", "value_source": "literal", "literal_value": "Bearer " + _SECRET_CANARY}
    ]
    (diagnostic,) = _blocked(recipe_request(raw, material))
    assert diagnostic.path.endswith("/targets/1/operation/query_parameters/0/literal_value")
    assert _SECRET_CANARY not in diagnostic.model_dump_json()


# ---------------------------------------------------------------------------
# CDX-r2-04: pointer segments are TYPED end to end
# ---------------------------------------------------------------------------

_DIGIT_KEY = "904417"


@pytest.mark.parametrize("request_path", [False, True], ids=["pydantic_loc", "raw_path"])
def test_the_walker_takes_an_array_step_only_for_an_int_segment(request_path):
    from boomi_mcp.recipes.errors import declared_loc_segments

    walk = lambda loc: declared_loc_segments(  # noqa: E731
        FanoutComponentMaterialV2, loc, request_path=request_path
    )
    # (a) an object key where the array is declared: stop at the declared parent
    assert walk(("targets", _DIGIT_KEY, "part_key")) == ("targets",)
    # a genuine index is still served
    assert walk(("targets", 1, "part_key")) == ("targets", "1", "part_key")


def test_the_forbidden_shape_scan_stops_before_a_digit_object_key_where_an_array_is_declared():
    raw, material = flagship_v2()
    raw["targets"] = {_DIGIT_KEY: {"host": "x"}}
    (diagnostic,) = _blocked(recipe_request(raw, material))
    assert diagnostic.path == "/intent/invocations/0/raw_input/targets"
    assert _DIGIT_KEY not in diagnostic.model_dump_json()


def test_the_forbidden_shape_scan_still_serves_a_genuine_array_index():
    raw, material = flagship_v2()
    raw["targets"][1]["host"] = "x"
    (diagnostic,) = _blocked(recipe_request(raw, material))
    assert diagnostic.path == "/intent/invocations/0/raw_input/targets/1"


def test_a_retired_spelling_under_a_digit_object_key_where_an_array_is_declared():
    raw, material = flagship_v2()
    material["targets"] = {_DIGIT_KEY: {"jdbc_options": {"a": "b"}}}
    (diagnostic,) = _blocked(recipe_request(raw, material))
    assert diagnostic.path == "/intent/invocations/0/component_material/targets"
    assert _DIGIT_KEY not in diagnostic.model_dump_json()
    assert GOVERNANCE_RETIRED_SPELLING in diagnostic.cause_codes


def test_a_retired_spelling_at_a_genuine_array_index_is_still_pointed_at():
    raw, material = flagship_v2()
    material["targets"][1]["connection"]["jdbc_options"] = {"a": "b"}
    request = recipe_request(raw, material)
    (diagnostic,) = _blocked(request)
    assert diagnostic.path == (
        "/intent/invocations/0/component_material/targets/1/connection/jdbc_options"
    )
    assert GOVERNANCE_RETIRED_SPELLING in diagnostic.cause_codes


def test_the_typed_retired_walk_renders_exactly_the_string_walk():
    from boomi_mcp.models.governance_intent import (
        retired_spelling_locations,
        retired_spellings_in,
    )

    value = {"a": [{"jdbc_options": 1}, {_DIGIT_KEY: {"cron": 2}}]}
    typed = retired_spelling_locations(value)
    assert [path for path, _ in typed] == [
        ("a", 0, "jdbc_options"),
        ("a", 1, _DIGIT_KEY, "cron"),
    ]
    assert retired_spellings_in(value) == (
        ("/a/0/jdbc_options", "jdbc_options"),
        ("/a/1/" + _DIGIT_KEY + "/cron", "cron"),
    )


_KEY_PROBE = "canaryKEY77"


@pytest.mark.parametrize(
    "case",
    ["envelope_caller_key", "envelope_digit_key", "material_caller_key", "material_digit_array"],
)
def test_no_retired_spelling_consumer_serves_a_caller_key_at_the_tool_boundary(case):
    """CDX-r2-04 sweep: ``reject_retired_spellings`` has THREE callers (the
    material model, the recipe per-root envelope, the process-component
    envelope). The material refusal's pointer is walked typed; the two envelope
    refusals are served by the typed-request boundary as their model-level
    ``loc`` and ``type`` only (never the ctx path or the message). Measured at
    the served ``build_integration`` envelope, which validates before the
    workflow."""
    from unittest import mock

    from boomi_mcp.categories import integration_builder

    raw, material = flagship_v2()
    intent = {
        "intent_kind": "recipe",
        "integration_name": "retired probe",
        "invocations": [
            {
                "recipe_id": RECIPE_ID,
                "recipe_version": V2,
                "invocation_id": "fanout",
                "raw_input": raw,
                "component_material": material,
            }
        ],
    }
    if case == "envelope_caller_key":
        intent["process_envelopes"] = [{"component_key": "main_process", _KEY_PROBE: {"cron": "x"}}]
    elif case == "envelope_digit_key":
        intent["process_envelopes"] = [{"component_key": "main_process", _DIGIT_KEY: {"cron": "x"}}]
    elif case == "material_caller_key":
        material["source"]["connection"][_KEY_PROBE] = {"jdbc_options": {"a": "b"}}
    else:
        material["targets"] = {_DIGIT_KEY: {"jdbc_options": {"a": "b"}}}
    with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []):
        envelope = integration_builder.build_integration_action(
            mock.MagicMock(), "p", "plan", {"authoring_request": {"contract_version": "2", "intent": intent}}
        )
    assert envelope["_success"] is False
    served = json.dumps(envelope, default=str)
    assert "GOVERNANCE_RETIRED_SPELLING" in served  # the refusal really fired
    assert _KEY_PROBE not in served and _DIGIT_KEY not in served


# ---------------------------------------------------------------------------
# CDX-159-S1-r3-01: a credential NAME the REST builder admits is still refused
# ---------------------------------------------------------------------------

#: Names the builder's whole-name list ADMITS but that denote a credential.
_CREDENTIAL_NAMES_THE_BUILDER_ADMITS = (
    "refresh_token", "access_token", "id_token", "sessionToken", "fooToken",
    "client_secret", "db_password", "passphrase", "private_key", "accessKey", "sig", "key",
)


@pytest.mark.parametrize("name", _CREDENTIAL_NAMES_THE_BUILDER_ADMITS)
def test_a_credential_name_the_builder_admits_is_refused_as_a_literal(name):
    from boomi_mcp.recipes.builtins import fanout

    raw, material = flagship_v2()
    material["targets"][1]["operation"]["query_parameters"] = [
        {"name": name, "value_source": "literal", "literal_value": _SECRET_CANARY}
    ]
    (diagnostic,) = _blocked(recipe_request(raw, material))
    assert diagnostic.path == (
        "/intent/invocations/0/component_material/targets/1/operation/query_parameters/0/literal_value"
    )
    assert fanout._PLAINTEXT_SECRET_CAUSE in diagnostic.cause_codes
    assert _SECRET_CANARY not in diagnostic.model_dump_json()


def test_the_witness_names_are_ones_the_builder_admits():
    """Non-vacuity: at least the token-shaped witnesses are exactly the builder's gap."""
    admitted = [n for n in _CREDENTIAL_NAMES_THE_BUILDER_ADMITS if not _builder_refuses(n)]
    assert {"refresh_token", "fooToken"} <= set(admitted)


@pytest.mark.parametrize("name", ["refresh_token", "client_secret"])
def test_a_credential_name_in_a_url_query_string_is_refused(name):
    raw, material = flagship_v2()
    material["targets"][0]["operation"]["path"] += "?" + name + "=" + _SECRET_CANARY
    (diagnostic,) = _blocked(recipe_request(raw, material))
    assert diagnostic.path == "/intent/invocations/0/component_material/targets/0/operation/path"
    assert _SECRET_CANARY not in diagnostic.model_dump_json()


@pytest.mark.parametrize(
    "name, credential",
    [
        ("pageToken", False), ("nextPageToken", False), ("page_token", False),
        ("continuationToken", False), ("cursor", False), ("keyword", False), ("limit", False),
        ("token", True), ("refresh_token", True), ("X-API-Key", True), ("apiKey", True),
        ("Authorization", True), ("password", True),
    ],
)
def test_the_query_name_rule(name, credential):
    from boomi_mcp.recipes.builtins.fanout import _query_name_is_credential

    assert _query_name_is_credential(name) is credential
