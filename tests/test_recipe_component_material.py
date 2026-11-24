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
        {"name": "filter", "value_source": "literal", "literal_value": CANARIES["query_literal"]}
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


def _materialless_request(raw_input, material):
    """One invocation of a registration that declares NO component material.

    #159 S3 made fan-out 2.0.0 (a material registration) the fan-out default, so
    the material-less vehicle is the surviving Stop-only stub at its default.
    """
    request = recipe_request(raw_input, material, recipe_version=None).model_dump(
        mode="json", exclude_none=True
    )
    request["intent"]["invocations"][0]["recipe_id"] = catalog_module.RECIPE_STUB_MINIMAL
    return AuthoringRequestV1.model_validate(request)


def test_material_is_refused_by_a_registration_that_declares_none():
    raw, material = flagship_v2()
    (diagnostic,) = _blocked(
        _materialless_request({"version": "1", "process_key": "main_process"}, material)
    )
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
    # #159 S3 (AR1-05): the per-root envelope field is retired, so a second
    # governance source cannot be spelled: the typed request refuses the field.
    with pytest.raises(Exception) as caught:
        recipe_request(
            raw, material, process_envelopes=[{"component_key": "main_process", "name": "Other"}]
        )
    assert "process_envelopes" in str(caught.value) and "extra_forbidden" in str(caught.value)


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
    # #159 S3: 2.0.0 is the production default, so the sibling is a non-default.
    registry = _registry(
        _registration(), _registration(recipe_version="2.0.1", is_default=False)
    )
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
    request = _materialless_request({"canary_key_5c1e": 1}, None)
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
    ("FanoutQueryParameterV2", "name"),
    ("FanoutQueryParameterV2", "literal_value"),
}

#: The D12 cause for a literal query name outside the published vocabulary.
QUERY_NAME_CAUSE = "FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED"


def _reachable_material_fields(*roots):
    """Every ``(model, field)`` reachable from ``roots`` (default: the fan-out
    material model) — DERIVED."""
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

    for root in roots or (FanoutComponentMaterialV2,):
        walk(root)
    return found


def test_the_credential_boundary_classifies_exactly_the_reachable_material_fields():
    from boomi_mcp.recipes.builtins import fanout

    # #159 S2: the walker's table is the shared table (every shared block,
    # including blocks fan-out does not reach, e.g. DB write) unioned with the
    # fan-out root table. Every field fan-out reaches is classified, and the
    # fan-out root rows name only fields it reaches.
    table = fanout._material_field_classes_v2()
    reachable = _reachable_material_fields()
    assert reachable <= set(table)
    assert set(fanout._fanout_root_field_classes_v2()) <= reachable
    secret_capable = {
        (model.__name__, name)
        for (model, name), rule in table.items()
        if rule and (model, name) in reachable
    }
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


# ---------------------------------------------------------------------------
# #159 S2: ONE credential authority for every preset (component_material)
# ---------------------------------------------------------------------------


def _material_registrations():
    registrations = [
        registration
        for registration in catalog_module.PRODUCTION_REGISTRATIONS
        if registration.component_material_model is not None
    ]
    assert registrations  # non-vacuous: at least fan-out 2.0.0 declares material
    return registrations


def test_the_shared_table_classifies_exactly_the_shared_block_fields():
    """The shared table's keys are DERIVED from the shared blocks' models: a
    field added to a block without a row, or a row left for a removed field,
    fails here — whichever preset reaches the block."""
    from boomi_mcp.recipes import component_material

    common = component_material._common_material_field_classes()
    assert set(common) == _reachable_material_fields(*component_material.SHARED_MATERIAL_BLOCKS)
    assert {rule for rule in common.values() if rule} <= set(component_material._SECRET_RULES)


def test_every_material_root_is_registered_and_classified_from_its_models():
    """Every production registration that declares a material model has it
    registered with the shared walker, and the walker's table covers EVERY
    field reachable from that root; the root's own rows name only fields the
    root reaches (no stale or borrowed rows)."""
    from boomi_mcp.recipes import component_material

    roots = {registration.component_material_model for registration in _material_registrations()}
    assert roots <= set(component_material.registered_material_roots())
    for root in component_material.registered_material_roots():
        reachable = _reachable_material_fields(root)
        assert reachable <= set(component_material.material_field_classes(root)), root
        own = set(component_material._MATERIAL_ROOTS[root]())
        assert own <= reachable, root


def test_a_root_table_may_not_reclassify_a_shared_field(monkeypatch):
    """Non-vacuity of the one-authority rule: a root table carrying a row for
    a SHARED model's field is refused, so a preset cannot loosen a shared rule."""
    from boomi_mcp.recipes import component_material

    class _BorrowingRoot:
        pass

    monkeypatch.setitem(
        component_material._MATERIAL_ROOTS,
        _BorrowingRoot,
        lambda: {(component_material.FanoutDbCreateMaterialV2, "credential_ref"): None},
    )
    with pytest.raises(ValueError, match="reclassifies a shared material field"):
        component_material.material_field_classes(_BorrowingRoot)
    with pytest.raises(ValueError, match="not a registered material root"):
        component_material.material_field_classes(type("_Unregistered", (), {}))


#: One plaintext value per SECRET RULE that the rule refuses wherever it applies.
_RULE_BREACHES = {
    "credential_reference": "plaintext-5ecret-7a1c",
    "jdbc_host": "db.example.invalid;password=plaintext-5ecret-7a1c",
    "jdbc_property": "AppDB;password=plaintext-5ecret-7a1c",
    "url": "https://svc:plaintext-5ecret-7a1c@billing.example.invalid/v1",
    "literal_query_name": "plaintext-5ecret-7a1c",
    # Escaped space: the value GRAMMAR admits it, the BUILDER refuses it.
    "named_literal": "Bearer%20plaintext-5ecret-7a1c",
}

#: Per material ROOT: ``() -> (raw input, material)`` whose material carries an
#: instance of every model with a secret-classified field. A material
#: registration without a seed fails the canary (no silent skip).
_MATRIX_SEEDS = _TESTS / "fixtures" / "recipe_matrix_seeds"


def _matrix_seed(name):
    """``(raw_input, material)`` of one committed matrix seed (its provenance is
    recorded in the seed file itself)."""
    request = json.loads((_MATRIX_SEEDS / name).read_text(encoding="utf-8"))["request"]
    return copy.deepcopy(request["raw_input"]), copy.deepcopy(request["component_material"])


_CANARY_DB_CREATE = {
    "mode": "create",
    "driver": "microsoft_jdbc",
    "host": "db.example.invalid",
    "database": "AppDB",
    "username": "svc_sync",
    "credential_ref": "credential://secrets/db/svc_sync",
}
_CANARY_LITERAL_QUERY = [{"name": "limit", "value_source": "literal", "literal_value": "50"}]
#: A one-leaf JSON root ``Root/id`` (the L2 lane's canary profile).
_CANARY_PROFILE = {
    "format": "json",
    "root": {"name": "Root", "kind": "object", "children": [
        {"name": "id", "kind": "simple", "data_type": "character"}
    ]},
}


def canary_db_to_api_v1():
    """DB->API 1.0.0: the matrix seed (both sides created) plus a LITERAL query
    parameter, so the REST target's name and value are reached too."""
    raw, material = _matrix_seed("boomi.archetype.database_to_api_sync@1.0.0.json")
    material["source"]["read"]["sql"] = "SELECT source_field_a FROM orders"
    material["target"]["operation"]["query_parameters"] = copy.deepcopy(_CANARY_LITERAL_QUERY)
    return raw, material


def canary_api_to_database_sync_v1():
    """API->DB 1.0.0: the matrix seed with a LITERAL fetch query parameter and a
    CREATED database target (the L3 lane's canary)."""
    raw, material = _matrix_seed("boomi.archetype.api_to_database_sync@1.0.0.json")
    raw["target"] = {"binding_mode": "create", "auth_mode": "username_password"}
    material["source"]["request"]["query_parameters"] = copy.deepcopy(_CANARY_LITERAL_QUERY)
    material["target"]["connection"] = dict(_CANARY_DB_CREATE)
    material["target"]["write_profile"]["table_name"] = "customers"
    return raw, material


def canary_listener_to_db():
    """Listener->DB 1.0.0 with a CREATED database target (the L2 lane's canary)."""
    return (
        {"version": "1", "naming": {"component_prefix": "CANARY"},
         "target": {"binding_mode": "create", "auth_mode": "username_password"}},
        {"version": "1",
         "listener": {"object_name": "canaryIntake", "request_profile": copy.deepcopy(_CANARY_PROFILE)},
         "transform": {"operations": [
             {"operation_type": "direct", "source_field": "Root/id", "target_path": "Fields/id"}]},
         "target": {"connection": {"mode": "create", "driver": "microsoft_jdbc", "host": "db.example.invalid",
                                   "database": "AppDB", "username": "svc",
                                   "credential_ref": "credential://secrets/db/svc"},
                    "write_profile": {"statement_type": "dynamicinsert", "table_name": "orders",
                                      "fields": [{"name": "id", "data_type": "character", "mandatory": True}]}}},
    )


def canary_listener_to_rest():
    """Listener->REST 1.0.0 with a CREATED target and a LITERAL query parameter
    (the L2 lane's canary)."""
    return (
        {"version": "1", "naming": {"component_prefix": "CANARY"}, "target": {"binding_mode": "create"}},
        {"version": "1",
         "listener": {"object_name": "canaryRelay", "request_profile": copy.deepcopy(_CANARY_PROFILE)},
         "transform": {"operations": [
             {"operation_type": "direct", "source_field": "Root/id", "target_path": "Root/id"}]},
         "target": {"connection": {"mode": "create", "base_url": "https://api.example.invalid"},
                    "operation": {"path": "/v1/items", "query_parameters": [
                        {"name": "limit", "value_source": "literal", "literal_value": "10"}]},
                    "payload_profile": copy.deepcopy(_CANARY_PROFILE)}},
    )


def _canary_seeds():
    from boomi_mcp.recipes.builtins.database_to_api_sync import DatabaseToApiSyncMaterialV1
    from boomi_mcp.recipes.builtins.http_listener_to_db import HttpListenerToDbMaterialV1
    from boomi_mcp.recipes.builtins.http_listener_to_rest import HttpListenerToRestMaterialV1
    from boomi_mcp.recipes.builtins.sync import ApiToDatabaseSyncMaterialV1

    return {
        FanoutComponentMaterialV2: canary_v2,
        DatabaseToApiSyncMaterialV1: canary_db_to_api_v1,
        ApiToDatabaseSyncMaterialV1: canary_api_to_database_sync_v1,
        HttpListenerToDbMaterialV1: canary_listener_to_db,
        HttpListenerToRestMaterialV1: canary_listener_to_rest,
    }


_CANARY_SEEDS = _canary_seeds()


def _field_occurrences(node, pointer=()):
    """``(model, field) -> [pointer tuple]`` for every field of a validated material."""
    from pydantic import BaseModel

    found = {}
    if isinstance(node, BaseModel):
        for name in type(node).model_fields:
            here = pointer + (name,)
            found.setdefault((type(node), name), []).append(here)
            for key, pointers in _field_occurrences(getattr(node, name), here).items():
                found.setdefault(key, []).extend(pointers)
    elif isinstance(node, (tuple, list)):
        for index, item in enumerate(node):
            for key, pointers in _field_occurrences(item, pointer + (index,)).items():
                found.setdefault(key, []).extend(pointers)
    return found


def _assembly_spy(monkeypatch):
    """Record every ``IntegrationComponentSpec`` constructed or validated."""
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    built = []
    real_init = IntegrationComponentSpec.__init__
    real_validate = IntegrationComponentSpec.model_validate.__func__
    real_construct = IntegrationComponentSpec.model_construct.__func__

    def init(self, *args, **kwargs):
        built.append("init")
        real_init(self, *args, **kwargs)

    def validate(cls, *args, **kwargs):
        built.append("model_validate")
        return real_validate(cls, *args, **kwargs)

    def construct(cls, *args, **kwargs):
        built.append("model_construct")
        return real_construct(cls, *args, **kwargs)

    monkeypatch.setattr(IntegrationComponentSpec, "__init__", init)
    monkeypatch.setattr(IntegrationComponentSpec, "model_validate", classmethod(validate))
    monkeypatch.setattr(IntegrationComponentSpec, "model_construct", classmethod(construct))
    return built


def test_every_material_registration_refuses_each_secret_field_before_assembly(monkeypatch):
    """Registry-derived canary (#159 S2): for EVERY production registration
    that declares a material model, a plaintext secret planted in EACH field
    the classification tables mark secret-capable is refused by the shared
    walker, at that field, before the preparer assembles any component."""
    from boomi_mcp.recipes import component_material

    assert set(_RULE_BREACHES) == set(component_material._SECRET_RULES)
    built = _assembly_spy(monkeypatch)
    for registration in _material_registrations():
        root = registration.component_material_model
        assert root in _CANARY_SEEDS, root  # a new material root needs a seed here
        # The walker's table is the shared table UNIONED with the root's own: a
        # shared block the root never reaches (e.g. the DB write block, for
        # fan-out) cannot be planted, so only the fields REACHABLE from this
        # root are owed.
        reachable = _reachable_material_fields(root)
        secret = {
            key: rule
            for key, rule in component_material.material_field_classes(root).items()
            if rule and key in reachable
        }
        assert secret  # non-vacuous: the tables mark at least one field
        raw, seed = _CANARY_SEEDS[root]()
        validated_input = registration.input_model.model_validate(raw)
        occurrences = _field_occurrences(root.model_validate(seed))
        # Control: the clean seed DOES assemble components (the spy sees them).
        del built[:]
        registration.component_preparer(validated_input, root.model_validate(seed), base_catalog={})
        assert built
        planted = set()
        for (model, name), rule in sorted(secret.items(), key=lambda item: (item[0][0].__name__, item[0][1])):
            assert occurrences.get((model, name)), (model.__name__, name)  # the seed reaches it
            pointer = occurrences[(model, name)][0]
            material = copy.deepcopy(seed)
            node = material
            for step in pointer[:-1]:
                node = node[step]
            node[pointer[-1]] = _RULE_BREACHES[rule]
            del built[:]
            with pytest.raises(RecipeError) as caught:
                registration.component_preparer(
                    validated_input, root.model_validate(material), base_catalog={}
                )
            (diagnostic,) = caught.value.diagnostics
            assert diagnostic.path == "/component_material/" + "/".join(map(str, pointer))
            assert set(diagnostic.cause_codes) & {
                component_material._PLAINTEXT_SECRET_CAUSE,
                component_material._QUERY_NAME_CAUSE,
            }
            assert "plaintext-5ecret-7a1c" not in repr(caught.value)
            assert built == [], (model.__name__, name)
            planted.add((model, name))
        assert planted == set(secret)


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
            material["targets"][1]["operation"]["path"] = (
                "/v1/bills?filter=Bearer%20" + _SECRET_CANARY
            )
        elif field == "name":
            material["targets"][1]["operation"]["query_parameters"] = [
                {"name": _SECRET_CANARY, "value_source": "literal", "literal_value": "x"}
            ]
        elif field == "literal_value":
            material["targets"][1]["operation"]["query_parameters"] = [
                # Escaped space: the value GRAMMAR admits it, the BUILDER refuses it.
                {"name": "filter", "value_source": "literal", "literal_value": "Bearer%20" + _SECRET_CANARY}
            ]

    return edit


_PLANT_POINTERS = {
    "credential_ref": "/source/connection/credential_ref",
    "host": "/source/connection/host",
    "database": "/source/connection/database",
    "base_url": "/targets/1/connection/base_url",
    "path": "/targets/1/operation/path",
    "name": "/targets/1/operation/query_parameters/0/name",
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
    expected = QUERY_NAME_CAUSE if field == "name" else fanout._PLAINTEXT_SECRET_CAUSE
    assert expected in diagnostic.cause_codes
    served = json.dumps([d.model_dump(mode="json") for d in caught.value.diagnostics])
    assert _SECRET_CANARY not in served
    assert _SECRET_CANARY not in str(caught.value) and _SECRET_CANARY not in repr(caught.value)
    assert _SECRET_CANARY not in "\n".join(record.getMessage() for record in caplog.records)
    assert _SECRET_CANARY not in "\n".join(str(w.message) for w in caught_warnings)


def test_the_served_contract_names_exactly_the_material_registrations():
    """#159 S2: the contract's component_material and derived_flows texts name
    the preset recipe ids that carry the material channel (and so supply
    projection facts). Pinned to the CATALOG, both directions, so a
    registration added or retired without its served sentence fails here."""
    import re

    from boomi_mcp.authoring.contract import authoring_workflow_contract

    material_ids = {
        registration.recipe_id for registration in _material_registrations()
    }
    terminology = authoring_workflow_contract()["terminology"]
    for name in ("component_material", "derived_flows"):
        named = set(re.findall(r"boomi\.(?:archetype|compose)\.[a-z0-9_]+", terminology[name]))
        assert named == material_ids, (name, named ^ material_ids)


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
    """D12 (architect decision A) REVERSED this witness: a pagination cursor
    the REST builder admits is outside fan-out 2.0.0's published literal
    vocabulary, so it is refused on both arms — intentionally and diagnosably —
    while a published name in the same positions still plans. (Node id kept so
    the wave manifest stays a legal successor.)"""
    assert not _builder_refuses(name)  # the builder alone would admit it
    raw, material = flagship_v2()
    material["targets"][1]["operation"]["query_parameters"] = [
        {"name": name, "value_source": "literal", "literal_value": "abc"}
    ]
    (diagnostic,) = _blocked(recipe_request(raw, material))
    assert diagnostic.path.endswith("/targets/1/operation/query_parameters/0/name")
    assert QUERY_NAME_CAUSE in diagnostic.cause_codes
    assert name not in diagnostic.model_dump_json()
    raw, material = flagship_v2()
    material["targets"][0]["operation"]["path"] += "?" + name + "=first"
    (diagnostic,) = _blocked(recipe_request(raw, material))
    assert diagnostic.path.endswith("/targets/0/operation/path")
    assert QUERY_NAME_CAUSE in diagnostic.cause_codes
    # Control: a published name in both positions plans.
    raw, material = flagship_v2()
    material["targets"][1]["operation"]["query_parameters"] = [
        {"name": "offset", "value_source": "literal", "literal_value": "abc"}
    ]
    material["targets"][0]["operation"]["path"] += "?offset=first"
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
    # D12: the NAME is outside the published vocabulary, refused before its value.
    assert diagnostic.path == (
        "/intent/invocations/0/component_material/targets/1/operation/query_parameters/0/name"
    )
    assert QUERY_NAME_CAUSE in diagnostic.cause_codes
    assert fanout._PLAINTEXT_SECRET_CAUSE not in diagnostic.cause_codes
    assert _SECRET_CANARY not in diagnostic.model_dump_json()


def test_a_secret_shaped_value_under_a_plain_name_is_refused_by_the_value_rule():
    """The builder's VALUE patterns travel too: a bearer credential under an
    innocuous name is still a secret."""
    raw, material = flagship_v2()
    material["targets"][1]["operation"]["query_parameters"] = [
        {"name": "filter", "value_source": "literal", "literal_value": "Bearer%20" + _SECRET_CANARY}
    ]
    (diagnostic,) = _blocked(recipe_request(raw, material))
    assert diagnostic.path.endswith("/targets/1/operation/query_parameters/0/literal_value")
    assert "PLAINTEXT_SECRET_REJECTED" in diagnostic.cause_codes
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
    """CDX-r2-04 sweep: ``reject_retired_spellings`` has TWO callers (the
    material model and the process-component envelope; the recipe per-root
    envelope retired in #159 S3). The material refusal's pointer is walked typed; the envelope
    refusal is served by the typed-request boundary as their model-level
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
    if case in ("envelope_caller_key", "envelope_digit_key"):
        # #159 S3 (AR1-05): the recipe per-root envelope is retired; the unit's
        # own authored envelope is the typed envelope that serves the refusal.
        probe = _KEY_PROBE if case == "envelope_caller_key" else _DIGIT_KEY
        intent = {
            "intent_kind": "process_ir",
            "integration_name": "retired probe",
            "units": [{
                "envelope": {"component_key": "main_process", "action": "create", "name": "n",
                             probe: {"cron": "x"}},
                "process_ir": {"version": "1", "body": {"kind": "sequence", "steps": [
                    {"kind": "passthrough"}, {"kind": "stop"}]}},
            }],
        }
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
    # D12: refused by the closed vocabulary, not by a name classifier.
    assert diagnostic.path == (
        "/intent/invocations/0/component_material/targets/1/operation/query_parameters/0/name"
    )
    assert QUERY_NAME_CAUSE in diagnostic.cause_codes
    assert fanout._PLAINTEXT_SECRET_CAUSE not in diagnostic.cause_codes
    served = diagnostic.model_dump_json()
    assert _SECRET_CANARY not in served and name not in served


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
    assert QUERY_NAME_CAUSE in diagnostic.cause_codes
    served = diagnostic.model_dump_json()
    assert _SECRET_CANARY not in served and name not in served




# ---------------------------------------------------------------------------
# D12 (#159 S1, architect decision A): the word-rule classifier is DELETED. The
# rows below were its frozen pin; their node ids are kept (the wave manifest
# must stay a legal successor), and each now asserts the CLOSED vocabulary:
# every name outside LITERAL_QUERY_NAMES_V2 is refused, whatever it denotes.
# ---------------------------------------------------------------------------

_FROZEN_CREDENTIAL_NAMES = {
    # QA r5-01 / CDX r3-01: names the REST builder's whole-name list admits
    "refresh_token": "QA r5-01", "auth_token": "QA r5-01", "access_token": "QA r5-01",
    "id_token": "QA r5-01", "sessionToken": "QA r5-01", "private_key": "QA r5-01",
    "passphrase": "QA r5-01", "api_secret": "QA r5-01", "X-Client-Secret": "QA r5-01",
    "X-Client_Secret": "QA r4", "fooToken": "deny-by-default token", "token": "bare token",
    # QA r6-01: provider credential names the word rule must catch
    "subscription-key": "QA r6-01 Azure APIM", "auth": "QA r6-01 Firebase",
    "auth_key": "QA r6-01", "authKey": "QA r6-01", "app_key": "QA r6-01",
    "license_key": "QA r6-01", "master_key": "QA r6-01", "shared_key": "QA r6-01",
    "session_id": "QA r6-01", "sessionid": "QA r6-01", "sid": "QA r6-01",
    "jwt": "QA r6-01", "pass": "QA r6-01", "hmac": "QA r6-01", "code": "QA r6-01 OAuth code",
    # OAuth / AWS / Azure / Google conventions (QA r6 hunt)
    "client_secret": "OAuth2", "oauth_token": "OAuth1", "oauth_signature": "OAuth1",
    "X-Amz-Signature": "AWS SigV4", "X-Amz-Security-Token": "AWS STS", "Signature": "AWS SigV2",
    "X-Amz-Credential": "AWS SigV4 (fail-closed: rides with X-Amz-Signature)",
    "sig": "Azure SAS", "key": "Google API key", "api_key": "API key", "apiKey": "API key",
    "X-API-Key": "API key", "Authorization": "HTTP auth", "password": "password",
    "passcode": "passcode", "secret_key": "secret key", "db_password": "B4 report",
    "accessKey": "access key",
    # advisor B6 checklist: position-sensitive names that must stay refused
    "secret_value": "advisor", "secretKey": "advisor", "secret_access_key": "advisor",
    "token_value": "advisor", "passwordConfirm": "advisor", "password2": "advisor",
    "client_secret_basic": "advisor",
    # lowercase compounds
    "accesstoken": "compound", "clientsecret": "compound", "authtoken": "compound",
    "apitoken": "compound",
}

_FROZEN_ORDINARY_NAMES = {
    # CDX r2-02 / r4-01 / QA r6-02: pagination cursors
    "pageToken": "CDX r2-02", "nextPageToken": "CDX r2-02", "page_token": "CDX r2-02",
    "pagination_token": "CDX r4-01 X API", "continuationToken": "pagination",
    "NextContinuationToken": "QA r6 S3", "nextToken": "QA r6", "syncToken": "QA r6",
    "skipToken": "QA r6-02 Graph", "resumptionToken": "QA r6-02 OAI-PMH",
    "sinceToken": "QA r6-02", "$skiptoken": "QA r6 OData", "$deltatoken": "QA r6 OData",
    # QA r6-02 / CDX r4-03: descriptors of a credential, not the credential
    "max_tokens": "QA r6-02", "token_type": "QA r6-02 OAuth",
    "signature_method": "QA r6-02", "oauth_signature_method": "QA r6-02 OAuth1",
    "secret_type": "CDX r4-03 GitHub", "key_id": "QA r6", "AWSAccessKeyId": "QA r6-02 key id",
    # QA r6: ordinary names
    "cursor": "QA r6", "limit": "QA r6", "keyword": "QA r6", "mode": "QA r6", "q": "QA r6",
    "sort_key": "QA r6", "partitionKey": "QA r6", "idempotency_key": "QA r6", "monkey": "QA r6",
    "X-Amz-Algorithm": "QA r6 AWS", "X-Amz-Date": "QA r6 AWS", "X-Amz-SignedHeaders": "QA r6 AWS",
    "client_id": "QA r6 OAuth", "state": "QA r6 OAuth", "nonce": "QA r6 OAuth",
    "redirect_uri": "QA r6 OAuth", "code_challenge": "QA r6 PKCE",
    "sv": "QA r6 Azure SAS", "se": "QA r6 Azure SAS", "sp": "QA r6 Azure SAS",
    "sr": "QA r6 Azure SAS", "country_code": "ordinary", "hockey": "ordinary", "passport": "ordinary",
}


def _literal_name_cause(name, value="x"):
    """The boundary's verdict on ONE structured literal ``name=value``."""
    from boomi_mcp.recipes.builtins import fanout

    owner = fanout.FanoutQueryParameterV2(name=name, value_source="literal", literal_value=value)
    return fanout._breaches("literal_query_name", owner.name, owner)


@pytest.mark.parametrize("name", sorted(_FROZEN_CREDENTIAL_NAMES))
def test_the_frozen_credential_names_are_refused(name):
    from boomi_mcp.recipes.builtins.fanout import LITERAL_QUERY_NAMES_V2

    assert name not in LITERAL_QUERY_NAMES_V2
    assert _literal_name_cause(name) == QUERY_NAME_CAUSE, _FROZEN_CREDENTIAL_NAMES[name]


@pytest.mark.parametrize("name", sorted(_FROZEN_ORDINARY_NAMES))
def test_the_frozen_ordinary_names_are_admitted(name):
    """Under D12 only the PUBLISHED names are admitted; an ordinary name the
    retired word rule admitted (a cursor, a key id) is now refused."""
    from boomi_mcp.recipes.builtins.fanout import LITERAL_QUERY_NAMES_V2

    expected = None if name in LITERAL_QUERY_NAMES_V2 else QUERY_NAME_CAUSE
    assert _literal_name_cause(name) == expected, _FROZEN_ORDINARY_NAMES[name]


@pytest.mark.parametrize("arm", ["literal", "path"])
def test_a_flag_value_under_a_credential_word_is_admitted_on_both_arms(arm):
    """D12 REVERSED this witness: ``hide_secret``-style flags are outside the
    published literal vocabulary, so they are refused on both arms whatever
    the value — a flag value is no exception."""
    for value in ("true", "abc123opaque"):
        raw, material = flagship_v2()
        if arm == "literal":
            material["targets"][1]["operation"]["query_parameters"] = [
                {"name": "hide_secret", "value_source": "literal", "literal_value": value}
            ]
        else:
            material["targets"][1]["operation"]["path"] += "?hide_secret=" + value
        (diagnostic,) = _blocked(recipe_request(raw, material))
        assert QUERY_NAME_CAUSE in diagnostic.cause_codes
        assert "hide_secret" not in diagnostic.model_dump_json()


def test_the_name_tokenizer_is_linear():
    """CDX r4-02, kept under D12: the tokenizer is gone; reading a long inline
    query (a long name, many pairs) must still not stall planning."""
    import time

    from boomi_mcp.recipes.builtins.fanout import _judge_request_target

    started = time.perf_counter()
    assert _judge_request_target("/v1/x?" + "A" * 200_000 + "1=v").cause == QUERY_NAME_CAUSE
    assert _judge_request_target("/v1/x?limit=" + "a-" * 100_000).cause is None
    assert _judge_request_target(
        "/v1/x?" + "&".join("limit%d=1" % i for i in range(20_000))
    ).cause == QUERY_NAME_CAUSE
    assert time.perf_counter() - started < 2.0


def test_the_served_text_states_the_query_name_rule():
    """D12: the architect's guarantee is served on the material schema AND the
    authoring contract, with the published names RENDERED from the constant."""
    from boomi_mcp.authoring.contract import authoring_workflow_contract
    from boomi_mcp.recipes.builtins import fanout

    names = ", ".join(fanout.LITERAL_QUERY_NAMES_V2)
    guarantee = fanout.LITERAL_QUERY_GUARANTEE_V2
    assert "the published names (" + names + ")" in guarantee
    # #159 S2: shared served text, preset-neutral (the cause CODE keeps its name).
    assert guarantee.startswith("Component material requires opaque `credential://` references")
    assert "fan-out" not in guarantee.lower()
    for field in (
        fanout.FanoutQueryParameterV2.model_fields["literal_value"],
        fanout.FanoutRestOperationMaterialV2.model_fields["path"],
        fanout.FanoutRestCreateMaterialV2.model_fields["base_url"],
    ):
        assert guarantee in field.description
        assert QUERY_NAME_CAUSE in field.description
    served = json.dumps(authoring_workflow_contract())
    assert json.dumps(guarantee)[1:-1] in served
    assert QUERY_NAME_CAUSE in served


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
    """D12: admitted exactly when PUBLISHED (``credential`` is the retired word
    rule's verdict, kept only so the node id is stable)."""
    from boomi_mcp.recipes.builtins.fanout import LITERAL_QUERY_NAMES_V2

    expected = None if name in LITERAL_QUERY_NAMES_V2 else QUERY_NAME_CAUSE
    assert _literal_name_cause(name) == expected


# ---------------------------------------------------------------------------
# D12 (#159 S1, architect decision A): a CLOSED literal query vocabulary
# ---------------------------------------------------------------------------

_ALLOWED = ("filter", "limit", "mode", "offset")
_UNKNOWN_NAME = "zz_unknown_77c1"
_VALUE_CANARY = "qv-canary-77c1"


def test_the_published_names_are_exactly_the_decision():
    from boomi_mcp.errors import FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED
    from boomi_mcp.recipes.builtins.fanout import LITERAL_QUERY_NAMES_V2

    assert LITERAL_QUERY_NAMES_V2 == _ALLOWED
    assert FANOUT_LITERAL_QUERY_NAME_UNSUPPORTED == QUERY_NAME_CAUSE


@pytest.mark.parametrize("arm", ["literal", "path", "base_url"])
@pytest.mark.parametrize("name", _ALLOWED)
def test_a_published_name_plans_on_every_arm(name, arm):
    raw, material = flagship_v2()
    target = material["targets"][1]
    if arm == "literal":
        target["operation"]["query_parameters"] = [
            {"name": "  " + name + " ", "value_source": "literal", "literal_value": "v1"}
        ]
    elif arm == "path":
        target["operation"]["path"] += "?" + name + "=v1"
    else:
        target["connection"]["base_url"] += "/?" + name + "=v1"
    plan, internals = plan_authoring_request_v1(recipe_request(raw, material), profile="p")
    assert plan.validation_report.is_valid


def _selected_rest_base(base_url):
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    return IntegrationComponentSpec(
        key="shared_rest",
        type="connector-settings",
        name="Shared REST",
        config={"connector_type": "rest", "base_url": base_url, "auth": "NONE"},
    ).model_dump(mode="json")


def _bind_selected_base(raw, material, base_url):
    raw["targets"][1]["connection_key"] = "shared_rest"
    material["targets"][1].pop("connection")
    return recipe_request(raw, material, base_components=[_selected_rest_base(base_url)])


def _refused(request, path, cause, *, absent=()):
    (diagnostic,) = _blocked(request)
    assert diagnostic.path == path
    assert cause in diagnostic.cause_codes
    served = diagnostic.model_dump_json()
    for text in absent:
        assert text not in served
    return diagnostic


_M = "/intent/invocations/0/component_material/targets/1"


@pytest.mark.parametrize("arm", ["literal", "path", "base_url", "selected_base_url"])
def test_an_unknown_literal_name_is_refused_on_every_arm(arm):
    raw, material = flagship_v2()
    target = material["targets"][1]
    pair = _UNKNOWN_NAME + "=" + _VALUE_CANARY
    if arm == "literal":
        target["operation"]["query_parameters"] = [
            {"name": _UNKNOWN_NAME, "value_source": "literal", "literal_value": _VALUE_CANARY}
        ]
        request, path = recipe_request(raw, material), _M + "/operation/query_parameters/0/name"
    elif arm == "path":
        target["operation"]["path"] += "?" + pair
        request, path = recipe_request(raw, material), _M + "/operation/path"
    elif arm == "base_url":
        target["connection"]["base_url"] += "/?" + pair
        request, path = recipe_request(raw, material), _M + "/connection/base_url"
    else:
        request = _bind_selected_base(raw, material, "https://billing.example.invalid/?" + pair)
        path = "/intent/invocations/0/raw_input/targets/1/connection_key"
    _refused(request, path, QUERY_NAME_CAUSE, absent=(_UNKNOWN_NAME, _VALUE_CANARY))


@pytest.mark.parametrize("name", ["refresh_token", "pw", "JSESSIONID", "id_token_hint", "api_key"])
def test_a_known_credential_name_is_refused_by_the_closed_vocabulary(name):
    raw, material = flagship_v2()
    material["targets"][1]["operation"]["query_parameters"] = [
        {"name": name, "value_source": "literal", "literal_value": _VALUE_CANARY}
    ]
    _refused(
        recipe_request(raw, material),
        _M + "/operation/query_parameters/0/name",
        QUERY_NAME_CAUSE,
        absent=(name, _VALUE_CANARY),
    )


def test_a_percent_encoded_published_name_is_admitted():
    raw, material = flagship_v2()
    material["targets"][1]["operation"]["path"] += "?%6C%69%6D%69%74=5"
    plan, _ = plan_authoring_request_v1(recipe_request(raw, material), profile="p")
    assert plan.validation_report.is_valid


@pytest.mark.parametrize(
    "query",
    [
        pytest.param("%70%61%67%65=1", id="encoded_unknown_name"),
        pytest.param("%6C%69%6D%69%74%00=1", id="encoded_name_decoding_to_something_else"),
        pytest.param("%FF=1", id="undecodable_name"),
    ],
)
def test_an_encoded_inline_name_outside_the_vocabulary_is_refused(query):
    raw, material = flagship_v2()
    material["targets"][1]["operation"]["path"] += "?" + query
    _refused(recipe_request(raw, material), _M + "/operation/path", QUERY_NAME_CAUSE)


@pytest.mark.parametrize(
    "query",
    [
        pytest.param("limit", id="pair_without_equals"),
        pytest.param("limit=1&limit=2", id="repeated_inline_name"),
        pytest.param("limit=1&&mode=2", id="empty_pair"),
        pytest.param("limit=1&", id="trailing_separator"),
        pytest.param("limit=1#frag", id="fragment"),
        pytest.param("limit=1?mode=2", id="second_question_mark"),
    ],
)
def test_ambiguous_inline_query_syntax_is_refused(query):
    raw, material = flagship_v2()
    material["targets"][1]["operation"]["path"] += "?" + query
    _refused(recipe_request(raw, material), _M + "/operation/path", QUERY_NAME_CAUSE)


@pytest.mark.parametrize(
    "where, pointer",
    [
        pytest.param("path_and_literal", _M + "/operation/query_parameters/0/name", id="path_and_literal"),
        pytest.param("base_url_and_path", _M + "/operation/path", id="base_url_and_path"),
        pytest.param(
            "selected_base_and_path", _M + "/operation/path", id="selected_base_url_and_path"
        ),
    ],
)
def test_a_name_repeated_across_query_sources_is_refused(where, pointer):
    raw, material = flagship_v2()
    target = material["targets"][1]
    if where == "path_and_literal":
        target["operation"]["path"] += "?limit=1"
        target["operation"]["query_parameters"] = [
            {"name": "limit", "value_source": "literal", "literal_value": "2"}
        ]
        request = recipe_request(raw, material)
    elif where == "base_url_and_path":
        target["connection"]["base_url"] += "/?limit=1"
        target["operation"]["path"] += "?limit=2"
        request = recipe_request(raw, material)
    else:
        target["operation"]["path"] += "?limit=2"
        request = _bind_selected_base(raw, material, "https://billing.example.invalid/?limit=1")
    _refused(request, pointer, QUERY_NAME_CAUSE)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("Bearer " + _VALUE_CANARY, id="bearer"),
        pytest.param("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2lnbmF0dXJlLXZhbHVl", id="jwt"),
    ],
)
@pytest.mark.parametrize("arm", ["literal", "path", "selected_base_url"])
def test_a_builder_rejected_value_under_a_published_name_is_refused(value, arm):
    from urllib.parse import quote

    raw, material = flagship_v2()
    target = material["targets"][1]
    if arm == "literal":
        # Written escaped, as the value grammar requires (a raw space is a
        # grammar refusal); the BUILDER then refuses the decoded credential.
        target["operation"]["query_parameters"] = [
            {"name": "filter", "value_source": "literal", "literal_value": quote(value)}
        ]
        request, path = recipe_request(raw, material), _M + "/operation/query_parameters/0/literal_value"
    elif arm == "path":
        target["operation"]["path"] += "?filter=" + quote(value)
        request, path = recipe_request(raw, material), _M + "/operation/path"
    else:
        request = _bind_selected_base(
            raw, material, "https://billing.example.invalid/?filter=" + quote(value)
        )
        path = "/intent/invocations/0/raw_input/targets/1/connection_key"
    _refused(request, path, "PLAINTEXT_SECRET_REJECTED", absent=(value, quote(value)))


def test_selected_base_userinfo_is_refused():
    raw, material = flagship_v2()
    request = _bind_selected_base(raw, material, "https://svc:" + _VALUE_CANARY + "@b.example.invalid")
    _refused(
        request,
        "/intent/invocations/0/raw_input/targets/1/connection_key",
        "PLAINTEXT_SECRET_REJECTED",
        absent=(_VALUE_CANARY,),
    )


def test_a_selected_base_with_a_clean_base_url_still_binds():
    raw, material = flagship_v2()
    request = _bind_selected_base(raw, material, "https://billing.example.invalid/?limit=10")
    plan, _ = plan_authoring_request_v1(request, profile="p")
    assert plan.validation_report.is_valid


def test_a_watermark_sourced_name_is_not_judged_by_the_vocabulary():
    raw, material = flagship_v2()
    raw["watermark"] = {"field": "source_a", "kind": "timestamp", "query_parameter_refs": ["updatedSince"]}
    material["targets"][0]["operation"]["query_parameters"] = [
        {"name": "updatedSince", "value_source": "watermark"}
    ]
    plan, _ = plan_authoring_request_v1(recipe_request(raw, material), profile="p")
    assert plan.validation_report.is_valid


@pytest.mark.parametrize("arm", ["literal", "path", "selected_base_url"])
def test_a_query_refusal_happens_before_any_component_is_assembled(arm, monkeypatch):
    from boomi_mcp.recipes.builtins import fanout

    calls = []
    for name in ("_emit_db_components_v2", "_emit_transform_components_v2", "_emit_target_components_v2"):
        real = getattr(fanout, name)

        def spy(*args, _real=real, _name=name, **kwargs):
            calls.append(_name)
            return _real(*args, **kwargs)

        monkeypatch.setattr(fanout, name, spy)
    raw, material = flagship_v2()
    if arm == "literal":
        material["targets"][1]["operation"]["query_parameters"] = [
            {"name": _UNKNOWN_NAME, "value_source": "literal", "literal_value": "x"}
        ]
        request = recipe_request(raw, material)
    elif arm == "path":
        material["targets"][1]["operation"]["path"] += "?" + _UNKNOWN_NAME + "=x"
        request = recipe_request(raw, material)
    else:
        request = _bind_selected_base(raw, material, "https://b.example.invalid/?" + _UNKNOWN_NAME + "=x")
    _blocked(request)
    assert calls == []
    # Control: the spies DO see a clean plan's assembly.
    plan_authoring_request_v1(recipe_request(*flagship_v2()), profile="p")
    assert calls


# ---------------------------------------------------------------------------
# B9: the request-target GRAMMAR, judged on what is EMITTED
# (CDX r6-01..04, QA r8-01/02)
# ---------------------------------------------------------------------------

_JWT = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2lnbmF0dXJlLXZhbHVl"


@pytest.mark.parametrize(
    "suffix",
    [
        pytest.param("?filter=Bearer+" + _JWT, id="r6_01_form_plus_bearer"),
        pytest.param("?limit=1;page=2", id="r6_02_semicolon_smuggle"),
        pytest.param("?limit=%ZZ", id="r6_03_malformed_escape"),
        pytest.param("?limit=1%", id="r6_03_trailing_percent"),
        pytest.param("?limit=1#", id="r6_04_bare_trailing_hash"),
        pytest.param("?lim\tit=1", id="r6_04_tab"),
        # Internal: a TRAILING CR is stripped by the path authority before
        # emission (``RestSendRequest`` trims), so it is never persisted.
        pytest.param("?limit=1\r&mode=2", id="r6_04_cr"),
        pytest.param("?lim\nit=1", id="r6_04_lf"),
        pytest.param("?limit=1;K=S", id="r8_02_inline_semicolon_pair"),
    ],
)
def test_an_inline_query_outside_the_grammar_is_refused(suffix):
    raw, material = flagship_v2()
    material["targets"][1]["operation"]["path"] += suffix
    _refused(
        recipe_request(raw, material), _M + "/operation/path", QUERY_NAME_CAUSE, absent=(_JWT,)
    )


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("1&K=S", id="r8_02_ampersand_pair"),
        pytest.param("1;K=S", id="r8_02_semicolon_pair"),
        pytest.param("Bearer+" + _JWT, id="r6_01_form_plus_bearer"),
        pytest.param("1%ZZ", id="r6_03_malformed_escape"),
        pytest.param("1%", id="r6_03_trailing_percent"),
        pytest.param("1#x", id="hash"),
        pytest.param("1?x", id="question_mark"),
        pytest.param("a\tb", id="tab"),
    ],
)
def test_a_structured_literal_value_outside_the_grammar_is_refused(value):
    raw, material = flagship_v2()
    material["targets"][1]["operation"]["query_parameters"] = [
        {"name": "filter", "value_source": "literal", "literal_value": value}
    ]
    _refused(
        recipe_request(raw, material),
        _M + "/operation/query_parameters/0/literal_value",
        QUERY_NAME_CAUSE,
        absent=(_JWT,),
    )


@pytest.mark.parametrize(
    "base_url, path, pointer_suffix",
    [
        # r7-01: the STORED base_url is judged on its own first; a bare trailing
        # '?' is an empty query, so the base (not the path) is refused.
        pytest.param(
            "https://billing.example.invalid/v1/orders?", "K=S", "/connection/base_url",
            id="base_ends_with_question_mark",
        ),
        pytest.param(
            "https://billing.example.invalid/?limit=1", "&K=S", "/operation/path",
            id="base_query_continued_by_path",
        ),
        pytest.param(
            "https://billing.example.invalid/?K=S", "/v1/billing", "/connection/base_url",
            id="offending_pair_in_the_base",
        ),
    ],
)
def test_a_pair_split_across_base_url_and_path_is_refused(base_url, path, pointer_suffix):
    """QA r8-01: base_url and path are judged JOINED, as the request is sent."""
    raw, material = flagship_v2()
    material["targets"][1]["connection"]["base_url"] = base_url
    material["targets"][1]["operation"]["path"] = path
    _refused(recipe_request(raw, material), _M + pointer_suffix, QUERY_NAME_CAUSE)


@pytest.mark.parametrize(
    "base_url, path, pointer",
    [
        pytest.param(
            "https://billing.example.invalid/v1/orders?", "K=S",
            "/intent/invocations/0/raw_input/targets/1/connection_key",
            id="selected_base_ends_with_question_mark",
        ),
        pytest.param(
            "https://billing.example.invalid/?limit=1", "&K=S",
            _M + "/operation/path", id="selected_base_query_continued_by_path",
        ),
        pytest.param(
            "https://billing.example.invalid/?K=S", "/v1/billing",
            "/intent/invocations/0/raw_input/targets/1/connection_key",
            id="offending_pair_in_the_selected_base",
        ),
    ],
)
def test_a_pair_split_across_a_selected_base_and_path_is_refused(base_url, path, pointer):
    raw, material = flagship_v2()
    material["targets"][1]["operation"]["path"] = path
    _refused(_bind_selected_base(raw, material, base_url), pointer, QUERY_NAME_CAUSE)


def _value_alphabet():
    from boomi_mcp.recipes.builtins import fanout

    return set(fanout._URI_UNRESERVED) | set(fanout._QUERY_VALUE_EXTRA)


def test_every_ascii_character_outside_the_value_alphabet_is_refused_in_a_value():
    """NON-VACUITY of the value grammar: iterate the whole ASCII range. Every
    character outside the alphabet is refused in an inline value AND a
    structured literal; every character inside it is admitted by the grammar
    (the control that proves the sweep can pass)."""
    from boomi_mcp.recipes.builtins import fanout

    alphabet = _value_alphabet()
    refused_inline, refused_structured, admitted = [], [], []
    for code in range(128):
        char = chr(code)
        if char == "%":
            continue  # only as an escape; covered by the malformed-escape witnesses
        value = "a" + char + "b"
        inline = fanout._judge_request_target("/v1/x?limit=" + value).cause
        owner = fanout.FanoutQueryParameterV2(name="limit", value_source="literal", literal_value=value)
        structured = fanout._breaches("named_literal", value, owner)
        if char in alphabet:
            assert inline is None and structured is None, repr(char)
            admitted.append(char)
        else:
            assert inline == QUERY_NAME_CAUSE, repr(char)
            assert structured == QUERY_NAME_CAUSE, repr(char)
            refused_inline.append(char)
            refused_structured.append(char)
    assert {"+", ";", "&", "?", "#", " ", "\t", "\r", "\n", "\x00"} <= set(refused_inline)
    assert len(admitted) == len(alphabet)


def test_a_trailing_whitespace_the_authority_strips_is_not_what_is_judged():
    """What is judged is what is EMITTED: the path authority trims ends, so a
    trailing CR/LF never reaches the component; an internal one does, and is
    refused."""
    raw, material = flagship_v2()
    material["targets"][1]["operation"]["path"] += "?limit=1\r\n"
    plan, internals = plan_authoring_request_v1(recipe_request(raw, material), profile="p")
    assert plan.validation_report.is_valid
    operation = next(
        c for c in internals.normalized.integration_spec.components
        if c.key == "target_billing_rest_operation"
    )
    assert operation.config["path"].endswith("?limit=1")


def test_an_escaped_separator_is_data_and_is_still_builder_checked():
    from boomi_mcp.recipes.builtins import fanout

    assert fanout._judge_request_target("/x?filter=a%26b%3Bc%2Bd%23e").cause is None
    assert fanout._judge_request_target("/x?filter=Bearer%20" + _JWT).cause == (
        "PLAINTEXT_SECRET_REJECTED"
    )


def test_the_parity_corpus_meets_the_grammar():
    """The frozen flagship and the six #157 composition inputs: every joined
    request target and every literal value is inside the grammar."""
    import glob

    from _fanout_v2_support import oracle, v2_from_compose_request
    from boomi_mcp.recipes.builtins import fanout

    requests = [oracle()["compose_request"]]
    cases = sorted(
        glob.glob(
            str(_TESTS / "fixtures" / "governance" / "issue_157" / "cases" / "composition" / "*")
        )
    )
    assert len(cases) == 6
    for case in cases:
        requests.append(json.loads((Path(case) / "input.legacy.json").read_text())["input"])
    judged = 0
    for request in requests:
        _raw, material = v2_from_compose_request(request)
        for target in material["targets"]:
            base = (target.get("connection") or {}).get("base_url", "")
            for joined, _start in fanout._joined_targets(base, target["operation"]["path"]):
                assert fanout._judge_request_target(joined).cause is None, joined
                judged += 1
            for parameter in target["operation"]["query_parameters"]:
                if parameter.get("value_source") == "literal":
                    assert fanout._query_value_violation(parameter["literal_value"]) is None
    assert judged >= 7


def test_the_served_text_states_the_grammar():
    from boomi_mcp.authoring.contract import authoring_workflow_contract
    from boomi_mcp.recipes.builtins import fanout

    sentence = fanout.QUERY_GRAMMAR_SENTENCE_V2
    assert " ".join(fanout._QUERY_VALUE_EXTRA) in sentence
    # QA-159-S1-r9-01 (live: the platform sent `?limit=5&filter=a%2526b`): the
    # sentence states BOTH sending behaviours.
    assert "an inline query value (in base_url or path) is sent as written" in sentence
    assert "the server decodes its escapes" in sentence
    assert "a structured literal value is percent-encoded by the platform" in sentence
    assert "a %XX in a structured value is sent as those literal characters" in sentence
    # The text must not invite a value the grammar refuses (raw space, &, ;, +).
    assert "unescaped" not in sentence
    assert "still limited to the value alphabet" in sentence
    for field in (
        fanout.FanoutQueryParameterV2.model_fields["literal_value"],
        fanout.FanoutRestOperationMaterialV2.model_fields["path"],
        fanout.FanoutRestCreateMaterialV2.model_fields["base_url"],
    ):
        assert sentence in field.description
    assert json.dumps(sentence)[1:-1] in json.dumps(authoring_workflow_contract())


# ---------------------------------------------------------------------------
# B10: Codex r7 — standalone fields, leading authority, strict UTF-8 escapes,
# watermark literal values, escape pointers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("arm", ["material_base_url", "selected_base_url"])
def test_a_secret_value_in_a_stored_base_url_query_is_refused_standalone(arm):
    """r7-01: ``?filter=<JWT>`` on the base is persisted as-is; joined with the
    path the value would read ``<JWT>/v1/billing`` and escape the builder's
    whole-value pattern, so the stored field is judged on its own."""
    from boomi_mcp.recipes.builtins import fanout

    base = "https://billing.example.invalid/?filter=" + _JWT
    path = "/v1/bill.ing"  # a '.' after the JWT breaks the builder's whole-value pattern
    # Non-vacuity: the JOINED targets alone would pass.
    for joined, _start in fanout._joined_targets(base, path):
        assert fanout._judge_request_target(joined).cause is None
    raw, material = flagship_v2()
    material["targets"][1]["operation"]["path"] = path
    if arm == "material_base_url":
        material["targets"][1]["connection"]["base_url"] = base
        request, pointer = recipe_request(raw, material), _M + "/connection/base_url"
    else:
        request = _bind_selected_base(raw, material, base)
        pointer = "/intent/invocations/0/raw_input/targets/1/connection_key"
    _refused(request, pointer, "PLAINTEXT_SECRET_REJECTED", absent=(_JWT,))


def test_a_clean_base_url_query_still_plans_with_its_path():
    raw, material = flagship_v2()
    material["targets"][1]["connection"]["base_url"] = "https://billing.example.invalid/?filter=active"
    plan, _ = plan_authoring_request_v1(recipe_request(raw, material), profile="p")
    assert plan.validation_report.is_valid


@pytest.mark.parametrize("binding", ["reuse", "create"])
def test_a_scheme_like_text_inside_a_path_is_path_data(binding):
    """r7-02: ``://`` later in a path opens no authority, so its ``@`` is data."""
    raw, material = flagship_v2()
    if binding == "reuse":
        raw["targets"][1]["binding_mode"] = "reuse"
        material["targets"][1]["connection"] = {"mode": "reuse", "component_id": "rest-id"}
    material["targets"][1]["operation"]["path"] = "/v1/tags://alice@example.org"
    plan, _ = plan_authoring_request_v1(recipe_request(raw, material), profile="p")
    assert plan.validation_report.is_valid


@pytest.mark.parametrize(
    "path",
    [
        pytest.param("//alice:" + _VALUE_CANARY + "@evil.example.invalid/x", id="scheme_relative"),
        pytest.param("https://alice:" + _VALUE_CANARY + "@evil.example.invalid/x", id="absolute"),
    ],
)
def test_userinfo_in_a_leading_authority_of_a_reused_path_is_refused(path):
    raw, material = flagship_v2()
    raw["targets"][1]["binding_mode"] = "reuse"
    material["targets"][1]["connection"] = {"mode": "reuse", "component_id": "rest-id"}
    material["targets"][1]["operation"]["path"] = path
    _refused(
        recipe_request(raw, material), _M + "/operation/path", "PLAINTEXT_SECRET_REJECTED",
        absent=(_VALUE_CANARY,),
    )


@pytest.mark.parametrize(
    "arm, escape",
    [
        ("inline", "%FF"), ("inline", "%C3"), ("structured", "%FF"), ("structured", "%E2%82"),
    ],
)
def test_an_escape_that_is_not_strict_utf8_is_refused(arm, escape):
    """r7-03: escapes must decode as strict UTF-8 — and the served sentence
    says so, rendered from the constants the decoder uses."""
    from boomi_mcp.recipes.builtins import fanout

    raw, material = flagship_v2()
    if arm == "inline":
        material["targets"][1]["operation"]["path"] += "?limit=a" + escape
        pointer = _M + "/operation/path"
    else:
        material["targets"][1]["operation"]["query_parameters"] = [
            {"name": "limit", "value_source": "literal", "literal_value": "a" + escape}
        ]
        pointer = _M + "/operation/query_parameters/0/literal_value"
    _refused(recipe_request(raw, material), pointer, QUERY_NAME_CAUSE)
    rule = "decode as {0} {1}".format(fanout._ESCAPE_ERRORS, fanout._ESCAPE_ENCODING.upper())
    assert rule in fanout.QUERY_GRAMMAR_SENTENCE_V2


def test_a_valid_multibyte_escape_is_admitted():
    raw, material = flagship_v2()
    material["targets"][1]["operation"]["path"] += "?filter=caf%C3%A9"
    plan, _ = plan_authoring_request_v1(recipe_request(raw, material), profile="p")
    assert plan.validation_report.is_valid


def test_a_watermark_parameters_unused_literal_value_is_not_judged_by_the_grammar():
    """r7-04: a watermark-sourced literal_value is never emitted, so the value
    grammar does not apply (``last month`` has a space); the builder's secret
    check still does, as it did before B9."""
    raw, material = flagship_v2()
    raw["watermark"] = {"field": "source_a", "kind": "timestamp", "query_parameter_refs": ["since"]}
    material["targets"][0]["operation"]["query_parameters"] = [
        {"name": "since", "value_source": "watermark", "literal_value": "last month"}
    ]
    plan, _ = plan_authoring_request_v1(recipe_request(raw, material), profile="p")
    assert plan.validation_report.is_valid
    material["targets"][0]["operation"]["query_parameters"] = [
        {"name": "since", "value_source": "watermark", "literal_value": "Bearer " + _VALUE_CANARY}
    ]
    _refused(
        recipe_request(raw, material),
        "/intent/invocations/0/component_material/targets/0/operation/query_parameters/0/literal_value",
        "PLAINTEXT_SECRET_REJECTED",
        absent=(_VALUE_CANARY,),
    )


@pytest.mark.parametrize(
    "base_url, path, pointer_suffix",
    [
        pytest.param(
            "https://billing.example.invalid/?filter=a", "%FF", "/operation/path",
            id="bad_escape_in_the_path_portion",
        ),
        pytest.param(
            "https://billing.example.invalid/?filter=a%FF", "/v1/billing", "/connection/base_url",
            id="bad_escape_in_the_base_portion",
        ),
    ],
)
def test_an_undecodable_escape_is_pointed_at_the_field_that_holds_it(base_url, path, pointer_suffix):
    """r7-05: the offset of the bad escape itself decides the pointer."""
    raw, material = flagship_v2()
    material["targets"][1]["connection"]["base_url"] = base_url
    material["targets"][1]["operation"]["path"] = path
    _refused(recipe_request(raw, material), _M + pointer_suffix, QUERY_NAME_CAUSE)


def test_the_bad_escape_offset_is_the_escape_itself():
    from boomi_mcp.recipes.builtins import fanout

    text = "https://h.example.invalid/?filter=a" + "%FF"
    verdict = fanout._judge_request_target(text)
    assert verdict.offset == len(text) - 3
    assert fanout._first_undecodable("ab%C3%A9%FFx") == 8
    assert fanout._first_undecodable("x%C3%A9") is None


# ---------------------------------------------------------------------------
# #159 S2: the shared DB-write material block (listener -> DB, API -> DB)
# ---------------------------------------------------------------------------


def _db_write_block(**overrides):
    from boomi_mcp.recipes import component_material

    block = {
        "connection": {
            "mode": "create",
            "driver": "microsoft_jdbc",
            "host": "db.example.invalid",
            "database": "AppDb",
            "username": "svc_writer",
            "credential_ref": "credential://db/writer",
        },
        "write_profile": {
            "statement_type": "dynamicinsert",
            "table_name": "orders",
            "fields": [{"name": "order_id", "mandatory": True}, {"name": "total", "data_type": "number"}],
        },
        "operation": {"commit_option": "commitrows", "batch_count": 50},
    }
    block.update(overrides)
    return component_material.DbWriteMaterialV1.model_validate(block)


def _db_write_context():
    from boomi_mcp.patterns.base import PrimitiveBuildContext

    return PrimitiveBuildContext(
        integration_name="Orders", component_prefix="Orders", folder_path="Recipes/Orders"
    )


def test_the_db_write_field_types_are_pinned_to_the_primitive():
    """The block's field-type vocabulary is a PINNED copy of the db_write
    primitive's (it cannot be imported at module scope): equal both ways."""
    import typing

    from boomi_mcp.patterns.primitives.db_write import DbWriteCondition, DbWriteField
    from boomi_mcp.recipes import component_material

    pinned = set(typing.get_args(component_material._DbWriteFieldTypeV1))
    assert pinned == set(typing.get_args(DbWriteField.model_fields["data_type"].annotation))
    assert pinned == set(typing.get_args(DbWriteCondition.model_fields["data_type"].annotation))


def test_the_db_write_block_emits_what_the_db_write_primitive_emits():
    """Parity with the existing authority: the block's preparation hands the
    ``db_write`` primitive the same parameters the legacy API -> DB archetype
    builds from the equivalent ``DbTarget`` (create connection, write profile,
    Send tuning), so the component group is byte-identical."""
    from boomi_mcp.patterns.primitives.db_write import DbWriteParameters, DbWritePrimitive
    from boomi_mcp.recipes import component_material

    authorized = component_material.authorize_db_write_material(
        _db_write_block(), pointer="/component_material/target", auth_mode="username_password"
    )
    assert set(authorized.field_index) == {"Fields/order_id", "Fields/total"}
    emitted = component_material.emit_db_write_components(
        authorized, context=_db_write_context(), key_prefix="target", pointer="/component_material/target"
    )
    legacy = DbWritePrimitive.emit_components(
        _db_write_context(),
        DbWriteParameters.model_validate(
            {
                "key_prefix": "target",
                "connection": {
                    "mode": "create",
                    "driver_id": "microsoft_jdbc",
                    "auth_mode": "username_password",
                    "host": "db.example.invalid",
                    "port": 1433,
                    "dbname": "AppDb",
                    "username": "svc_writer",
                    "credential_ref": "credential://db/writer",
                },
                "write_profile": {
                    "statement_type": "dynamicinsert",
                    "table_name": "orders",
                    "fields": [
                        {"name": "order_id", "mandatory": True},
                        {"name": "total", "data_type": "number"},
                    ],
                },
                "operation": {"commit_option": "commitrows", "batch_count": 50},
            }
        ),
    )
    assert [spec.model_dump() for spec in emitted] == [spec.model_dump() for spec in legacy]
    assert [spec.type for spec in emitted] == ["connector-settings", "profile.db", "connector-action"]


def test_a_base_bound_db_write_block_emits_only_its_profile_and_operation():
    from boomi_mcp.recipes import component_material

    authorized = component_material.authorize_db_write_material(
        _db_write_block(connection=None), pointer="/component_material/target", auth_mode=None
    )
    emitted = component_material.emit_db_write_components(
        authorized,
        context=_db_write_context(),
        key_prefix="target",
        pointer="/component_material/target",
        base_connection_key="shared_db",
    )
    assert [spec.type for spec in emitted] == ["profile.db", "connector-action"]
    assert emitted[1].config["connection_ref_key"] == "shared_db"
    assert emitted[1].depends_on == ["shared_db", emitted[0].key]


@pytest.mark.parametrize(
    "profile, pointer",
    [
        ({"statement_type": "upsert", "table_name": "orders", "fields": [{"name": "a"}]}, "/write_profile"),
        ({"statement_type": "dynamicinsert", "fields": [{"name": "a"}]}, "/write_profile"),
    ],
    ids=["unsupported_statement_type", "missing_table_name"],
)
def test_the_db_write_builder_rules_refuse_before_any_component(profile, pointer, monkeypatch):
    from boomi_mcp.recipes import component_material

    built = _assembly_spy(monkeypatch)
    with pytest.raises(RecipeError) as caught:
        component_material.authorize_db_write_material(
            _db_write_block(write_profile=profile), pointer="/component_material/target", auth_mode="username_password"
        )
    (diagnostic,) = caught.value.diagnostics
    assert diagnostic.path == "/component_material/target" + pointer
    assert built == []


def test_the_credential_walker_covers_the_db_write_block():
    """A plaintext credential in the block's connection is refused by the
    shared walker at that field — the block's rows are in the shared table."""
    from boomi_mcp.recipes import component_material

    block = _db_write_block(
        connection={
            "mode": "create",
            "driver": "microsoft_jdbc",
            "host": "db.example.invalid",
            "database": "AppDb",
            "username": "svc_writer",
            "credential_ref": "hunter2-plaintext",
        }
    )
    with pytest.raises(RecipeError) as caught:
        component_material.check_credential_boundary(
            block, component_material._common_material_field_classes(), pointer="/component_material/target"
        )
    (diagnostic,) = caught.value.diagnostics
    assert diagnostic.path == "/component_material/target/connection/credential_ref"
    assert component_material._PLAINTEXT_SECRET_CAUSE in diagnostic.cause_codes


# ---------------------------------------------------------------------------
# #159 S2: the single-request check is preset-agnostic (send OR fetch)
# ---------------------------------------------------------------------------


def test_the_single_request_check_judges_a_fetch_request_and_returns_its_recheck_entry():
    """``check_rest_request`` judges ONE request from the values the builder
    receives, whatever its verb: a reused-connection GET path is judged alone
    and returned with its binding pointer for the workflow's stored-URL
    recheck; a created connection's base URL is judged JOINED with the path."""
    from boomi_mcp.recipes import component_material

    entry = component_material.check_rest_request(
        connection_key="source_rest_connection",
        path="/v1/orders?limit=50",
        path_pointer="/component_material/source/operation/path",
        created_base_url=None,
        connection_pointer="/component_material/source/connection",
        selected_key=None,
        selected_key_pointer="/raw_input/source/connection_key",
        binding_mode="reuse",
        base_catalog={},
    )
    assert entry == (
        "source_rest_connection",
        "/v1/orders?limit=50",
        "/component_material/source/operation/path",
        "/component_material/source/connection",
    )
    with pytest.raises(RecipeError) as caught:
        component_material.check_rest_request(
            connection_key="source_rest_connection",
            path="/v1/orders",
            path_pointer="/component_material/source/operation/path",
            created_base_url="https://api.example.invalid/?token=abc",
            connection_pointer="/component_material/source/connection",
            selected_key=None,
            selected_key_pointer="/raw_input/source/connection_key",
            binding_mode="create",
            base_catalog={},
        )
    (diagnostic,) = caught.value.diagnostics
    assert diagnostic.path == "/component_material/source/connection/base_url"
    assert QUERY_NAME_CAUSE in diagnostic.cause_codes


def test_the_prepared_result_is_the_shared_type_with_the_s1_alias():
    """#159 S2: ``PreparedFanoutV2`` is the shared ``PreparedRecipeComponentsV1``
    (an alias, same fields), and the fan-out preparer returns it."""
    import dataclasses

    from boomi_mcp.recipes import component_material
    from boomi_mcp.recipes.builtins import fanout

    assert fanout.PreparedFanoutV2 is component_material.PreparedRecipeComponentsV1
    assert [field.name for field in dataclasses.fields(component_material.PreparedRecipeComponentsV1)] == [
        "supporting_components",
        "selected_base_keys",
        "create_bound_base_keys",
        "rest_request_targets",
        "deferred_query_parameter_names",
        "projection_facts",
    ]
    raw, material = flagship_v2()
    prepared = prepare_db_rest_fanout_v2(
        ComposeDbRestFanoutInputV2.model_validate(raw),
        FanoutComponentMaterialV2.model_validate(material),
        base_catalog={},
    )
    assert isinstance(prepared, component_material.PreparedRecipeComponentsV1)
    assert repr(prepared).startswith("PreparedRecipeComponentsV1(supporting_keys=")


def test_a_map_binds_to_the_db_write_profile_through_the_shared_assemblies():
    """The listener/API -> DB chain: the shared transform assembly binds its
    map TARGET to the shared DB-write block's write profile (``target_binding``,
    the binding the legacy API -> DB archetype builds), using the write
    profile's field index from the block's authorization."""
    from boomi_mcp.patterns.primitives._helpers import ROLE_DB_WRITE_PROFILE, primitive_component_key
    from boomi_mcp.recipes import component_material

    authorized = component_material.authorize_db_write_material(
        _db_write_block(), pointer="/component_material/target", auth_mode="username_password"
    )
    written = component_material.emit_db_write_components(
        authorized, context=_db_write_context(), key_prefix="target", pointer="/component_material/target"
    )
    write_profile_key = primitive_component_key("target", ROLE_DB_WRITE_PROFILE)
    assert write_profile_key in [spec.key for spec in written]
    operations = component_material.authorize_transform_operations(
        (
            component_material.FanoutDirectOperationV2.model_validate(
                {"operation_type": "direct", "source_field": " id ", "target_path": "Fields/order_id"}
            ),
        ),
        base_catalog={},
        pointer="/component_material/transform/operations",
    )
    assert operations[0].source_field == "id"  # normalized by the legacy authority
    mapped = component_material.emit_transform_components(
        operations,
        context=_db_write_context(),
        key_prefix="transform",
        pointer="/component_material/transform",
        source={
            "source_profile_id": "$ref:source_response_profile",
            "source_profile_type": "profile.json",
            "source_field_index": {"id": {"data_type": "character", "mappable": True}},
        },
        target_binding={
            "target_profile_id": f"$ref:{write_profile_key}",
            "target_profile_type": "profile.db",
            "target_field_index": dict(authorized.field_index),
        },
    )
    assert [spec.type for spec in mapped] == ["transform.map"]
    assert write_profile_key in (mapped[0].depends_on or []) or (
        f"$ref:{write_profile_key}" in json.dumps(mapped[0].config)
    )
