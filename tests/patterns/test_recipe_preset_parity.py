"""Parity for the typed-contribution migration (issue #145 M12.10).

Four oracles, strongest first. Each answers a different question, and none of
them subsumes another:

* **L1 — full IntegrationSpec dump** against fixtures captured at the BASELINE
  commit (``060dabad64e028d83d192e5820d8f37df64d54d3``) in a git worktree, before
  any of this issue's code existed. That provenance is the point: a fixture
  captured from the migrated tree would be a snapshot of the code it is supposed
  to check.
* **L2 — ComponentPlan order**, from ``_build_plan``. L1 cannot see this:
  ``_topological_order`` is a SORTED topological order, so declaration order
  alone does not determine materialization order.
* **L3 — process XML differential** between the recipe arm and the legacy adapter
  arm under ONE shared resolver. Both arms end in the same ``emit_process``, so
  any difference can only come from the IR — which is exactly what the migration
  changes. A committed golden follows as a uniform-drift detector; the golden
  alone would be satisfied by both arms drifting together.
* **L4 — the exemption asymmetry**, stated as a falsifiable claim rather than an
  assertion: the recipe's shared cache ref compiles with ``validation_policy=None``
  while the legacy occurrence-aliased arm on the SAME composition still fails
  strictly. If the second half ever stops failing, this test fails — so the claim
  cannot rot into a tautology.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from boomi_mcp.categories.integration_authoring import (
    build_from_archetype_action,
    compose_archetypes_action,
)
from boomi_mcp.categories.integration_builder import _build_plan
from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
from boomi_mcp.compiler.process_ir.legacy_adapters.emission import emit_legacy_result
from boomi_mcp.compiler.process_ir.legacy_adapters.flow_sequence import (
    adapt_flow_sequence,
)
from boomi_mcp.compiler.process_ir.legacy_adapters.sync_pipeline import (
    adapt_sync_pipeline_config,
)
from boomi_mcp.models.integration_models import IntegrationSpecV1
from boomi_mcp.patterns.archetypes.api_to_api_sync import ApiToApiSyncArchetype
from boomi_mcp.patterns.archetypes.api_to_database_sync import (
    ApiToDatabaseSyncArchetype,
)
from boomi_mcp.patterns.recipe_bridge import run_fanout_recipe, run_sync_preset_recipe
from boomi_mcp.recipes import placeholder_component_id
from boomi_mcp.recipes.builtins.catalog import (
    RECIPE_API_TO_API_SYNC,
    RECIPE_API_TO_DATABASE_SYNC,
    RECIPE_DB_REST_FANOUT,
)

from test_archetype_composition import _cache_links, _options, _parts

_PAGINATE = "boomi_mcp.categories.integration_builder.paginate_metadata"
_FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "recipe_parity"
_GOLDEN = Path(__file__).resolve().parent.parent / "fixtures" / "golden_xml"

#: The commit the fixtures were captured at. Recorded so a reader can reproduce
#: the capture rather than take the file's provenance on trust.
BASELINE_SHA = "060dabad64e028d83d192e5820d8f37df64d54d3"


def _fixture(name):
    return json.loads((_FIXTURES / name).read_text())


def _plan_steps(spec):
    with patch(_PAGINATE, return_value=[]):
        result = _build_plan(
            MagicMock(), {"integration_spec": spec, "conflict_policy": "reuse"}
        )
    return {
        "_success": result.get("_success", True),
        "steps": [
            {
                "planned_action": step.get("planned_action") or step.get("action"),
                "component_key": step.get("component_key") or step.get("key"),
            }
            for step in (result.get("steps") or [])
        ],
    }


def _extend_parts(n):
    """The shared composition fixture widened to n REST targets."""
    base = _parts()
    targets = [p for p in base if p["kind"] == "rest_target"]
    out = [p for p in base if p["kind"] != "rest_target"]
    for i in range(n):
        template = json.loads(json.dumps(targets[i % len(targets)]))
        template["key"] = f"t{i}"
        template["label"] = f"T{i}"
        template["parameters"]["binding"]["settings"]["base_url"] = (
            f"https://t{i}.example.invalid"
        )
        template["parameters"]["send_request"]["path"] = f"/v1/t{i}"
        out.append(template)
    return out


def _max_cache_links(n):
    return [{"from_part": "db", "to_part": "shape"}] + [
        {"from_part": "shape", "to_part": f"t{i}", "handoff": {"mode": "document_cache"}}
        for i in range(n)
    ]


_COMPOSE_CASES = {
    "compose_stream": (lambda: _parts(), None),
    "compose_mixed_cache": (lambda: _parts(), lambda: _cache_links("billing")),
    "compose_all_cache": (lambda: _parts(), lambda: _cache_links("orders", "billing")),
    "compose_max_stream": (lambda: _extend_parts(25), None),
    "compose_max_cache": (lambda: _extend_parts(24), lambda: _max_cache_links(24)),
}

_PRESET_CASES = {
    "api_to_api_sync_0": ("api_to_api_sync", 0),
    "api_to_api_sync_1": ("api_to_api_sync", 1),
    "api_to_database_sync_0": ("api_to_database_sync", 0),
}

_EXAMPLES = {
    "api_to_api_sync": ApiToApiSyncArchetype,
    "api_to_database_sync": ApiToDatabaseSyncArchetype,
}


def _compose(name):
    parts_fn, links_fn = _COMPOSE_CASES[name]
    options = dict(_options())
    if links_fn is not None:
        options["links"] = links_fn()
    result = compose_archetypes_action(parts=parts_fn(), options=options)
    assert result["_success"] is True, result.get("error")
    return result


def _build_preset(name):
    archetype, index = _PRESET_CASES[name]
    example = _EXAMPLES[archetype].examples[index]
    result = build_from_archetype_action(archetype, example.parameters)
    assert result["_success"] is True, result.get("error")
    return result


# ---------------------------------------------------------------------------
# Guard the guards
# ---------------------------------------------------------------------------


def test_every_parity_fixture_is_claimed_by_a_case():
    """An orphan fixture is a case somebody deleted without noticing."""
    on_disk = {p.stem for p in _FIXTURES.glob("*.json")}
    claimed = set()
    for name in list(_COMPOSE_CASES) + list(_PRESET_CASES):
        claimed.add(name)
        claimed.add(f"{name}_component_plan")
    assert on_disk == claimed, on_disk ^ claimed


# ---------------------------------------------------------------------------
# L1 — full spec dump
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(_COMPOSE_CASES))
def test_l1_compose_spec_is_byte_identical_to_baseline(name):
    assert _compose(name)["integration_spec"] == _fixture(f"{name}.json")


@pytest.mark.parametrize("name", sorted(_PRESET_CASES))
def test_l1_preset_spec_is_byte_identical_to_baseline(name):
    assert _build_preset(name)["integration_spec"] == _fixture(f"{name}.json")


# ---------------------------------------------------------------------------
# L2 — ComponentPlan order
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(_COMPOSE_CASES))
def test_l2_compose_component_plan_order_is_unchanged(name):
    spec = _compose(name)["integration_spec"]
    assert _plan_steps(spec) == _fixture(f"{name}_component_plan.json")


@pytest.mark.parametrize("name", sorted(_PRESET_CASES))
def test_l2_preset_component_plan_order_is_unchanged(name):
    spec = _build_preset(name)["integration_spec"]
    assert _plan_steps(spec) == _fixture(f"{name}_component_plan.json")


# ---------------------------------------------------------------------------
# L3 — process XML differential (the primary recipe oracle)
# ---------------------------------------------------------------------------


def _spec_and_process(payload):
    spec = IntegrationSpecV1.model_validate(payload)
    return spec, spec.components[-1]


@pytest.mark.parametrize("name", sorted(_COMPOSE_CASES))
def test_l3_compose_recipe_arm_matches_the_legacy_arm_byte_for_byte(name):
    spec, process = _spec_and_process(_compose(name)["integration_spec"])

    legacy = emit_legacy_result(
        adapt_flow_sequence(process.config),
        resolver=placeholder_component_id,
        dialect="flow_sequence",
    )
    result = run_fanout_recipe(
        recipe_id=RECIPE_DB_REST_FANOUT,
        components=spec.components,
        process=process,
    )
    artifact = result.artifact_for(process.key)
    assert artifact is not None
    assert artifact.process_xml == legacy.process_xml
    assert artifact.verifier.errors == ()


@pytest.mark.parametrize("name", sorted(_PRESET_CASES))
def test_l3_preset_recipe_arm_matches_the_legacy_arm_byte_for_byte(name):
    archetype = _PRESET_CASES[name][0]
    recipe_id = (
        RECIPE_API_TO_API_SYNC
        if archetype == "api_to_api_sync"
        else RECIPE_API_TO_DATABASE_SYNC
    )
    spec, process = _spec_and_process(_build_preset(name)["integration_spec"])

    legacy = emit_legacy_result(
        adapt_sync_pipeline_config(process.config),
        resolver=placeholder_component_id,
        dialect="sync_pipeline",
    )
    result = run_sync_preset_recipe(
        recipe_id=recipe_id, components=spec.components, process=process
    )
    artifact = result.artifact_for(process.key)
    assert artifact is not None
    assert artifact.process_xml == legacy.process_xml
    assert artifact.verifier.errors == ()


_GOLDEN_CASES = {
    "compose_stream": "composed_db_to_api_fanout.xml",
    "compose_all_cache": "composed_db_to_api_cache_fanout.xml",
    "api_to_api_sync_0": "api_to_api_sync_fetch_map_send.xml",
}


@pytest.mark.parametrize("name", sorted(_GOLDEN_CASES))
def test_l3_recipe_arm_matches_its_committed_golden(name):
    """A uniform-drift detector, NOT the oracle.

    The differential above is what proves the recipe arm equals the legacy arm.
    This catches the case where BOTH arms change together — which the
    differential, by construction, cannot see.
    """
    golden = (_GOLDEN / _GOLDEN_CASES[name]).read_bytes().decode("utf-8")
    if name in _COMPOSE_CASES:
        spec, process = _spec_and_process(_compose(name)["integration_spec"])
        result = run_fanout_recipe(
            recipe_id=RECIPE_DB_REST_FANOUT,
            components=spec.components,
            process=process,
        )
    else:
        spec, process = _spec_and_process(_build_preset(name)["integration_spec"])
        result = run_sync_preset_recipe(
            recipe_id=RECIPE_API_TO_API_SYNC,
            components=spec.components,
            process=process,
        )
    assert result.artifact_for(process.key).process_xml == golden


# ---------------------------------------------------------------------------
# L4 — the exemption asymmetry, made falsifiable
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["compose_mixed_cache", "compose_all_cache"])
def test_l4_legacy_cache_arm_passes_strictly_once_cache_identity_is_canonical(name, monkeypatch):
    """The claim's OTHER half, re-measured under #184's canonical cache identity.

    The legacy arm spells its one handoff cache through per-occurrence refs, one for
    the staging put and one for each consuming get, and every one of them resolves
    to the same documentcache component. Keyed by spelling, the put did not
    establish the gets, so this arm needed its exemption. #184 keys every cache
    fact by the resolved component (Stage-2 review finding CDX-184-r1-01, ledger
    C23), so the legacy arm now passes ``validation_policy=None`` like the recipe
    arm. The difference was the SPELLING, and disabling the canonical spelling
    restores the refusal exactly.
    """
    from boomi_mcp.compiler.process_ir.semantic_validation import context, lineage, pipeline

    _spec, process = _spec_and_process(_compose(name)["integration_spec"])
    adapted = adapt_flow_sequence(process.config)

    emit_legacy_result(adapted, resolver=placeholder_component_id, dialect=None)

    for module in (context, pipeline, lineage):
        monkeypatch.setattr(module, "canonical_cache_refs", lambda symbols: {})
    with pytest.raises(Exception) as excinfo:
        emit_legacy_result(adapted, resolver=placeholder_component_id, dialect=None)
    cause = excinfo.value.__cause__
    assert isinstance(cause, ProcessIRCompileError)
    assert {d.code for d in cause.diagnostics} == {
        "PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING"
    }

    # ... and the dialect's registered policy still accepts it, so the exemption
    # stays harmless where a spelling-keyed validator would need it.
    emit_legacy_result(
        adapted, resolver=placeholder_component_id, dialect="flow_sequence"
    )


@pytest.mark.parametrize("name", ["compose_mixed_cache", "compose_all_cache"])
def test_l4_recipe_cache_arm_passes_strictly_with_one_shared_reference(name):
    spec, process = _spec_and_process(_compose(name)["integration_spec"])
    result = run_fanout_recipe(
        recipe_id=RECIPE_DB_REST_FANOUT,
        components=spec.components,
        process=process,
    )
    # It compiled under validation_policy=None (the engine hard-codes it), so
    # reaching here at all is the assertion. Now show WHY: one shared reference
    # across the staging put and every consuming get.
    root = dict(result.composed.process_roots)[process.key]
    branch = root.body.steps[-1]
    refs = set()
    for leg in branch.legs:
        if leg.terminal.kind == "cache_put":
            refs.add(leg.terminal.cache_ref)
        for step in leg.steps:
            if step.kind == "cache_get":
                refs.add(step.cache_ref)
    assert len(refs) == 1, refs
    assert refs == {"$ref:handoff_document_cache"}


def test_l4_recipe_stream_arm_uses_no_cache_reference_at_all():
    """The negative control: no cache handoff means no cache node anywhere."""
    spec, process = _spec_and_process(_compose("compose_stream")["integration_spec"])
    result = run_fanout_recipe(
        recipe_id=RECIPE_DB_REST_FANOUT,
        components=spec.components,
        process=process,
    )
    root = dict(result.composed.process_roots)[process.key]
    branch = root.body.steps[-1]
    for leg in branch.legs:
        assert leg.terminal.kind == "target"
        assert leg.steps == []


# ---------------------------------------------------------------------------
# Composition invariants the issue names explicitly
# ---------------------------------------------------------------------------


def test_cache_staging_leg_precedes_the_first_consumer():
    spec, process = _spec_and_process(
        _compose("compose_mixed_cache")["integration_spec"]
    )
    result = run_fanout_recipe(
        recipe_id=RECIPE_DB_REST_FANOUT,
        components=spec.components,
        process=process,
    )
    root = dict(result.composed.process_roots)[process.key]
    legs = root.body.steps[-1].legs
    staging = [i for i, leg in enumerate(legs) if leg.terminal.kind == "cache_put"]
    consumers = [
        i
        for i, leg in enumerate(legs)
        if any(step.kind == "cache_get" for step in leg.steps)
    ]
    assert len(staging) == 1
    assert staging[0] < min(consumers)


def test_max_cache_composition_is_bounded_at_twenty_four_targets():
    """25 legs total: one staging put plus 24 consumers."""
    spec, process = _spec_and_process(_compose("compose_max_cache")["integration_spec"])
    result = run_fanout_recipe(
        recipe_id=RECIPE_DB_REST_FANOUT,
        components=spec.components,
        process=process,
    )
    root = dict(result.composed.process_roots)[process.key]
    legs = root.body.steps[-1].legs
    assert len(legs) == 25
    assert sum(1 for leg in legs if leg.terminal.kind == "cache_put") == 1


def test_max_stream_composition_carries_all_twenty_five_targets():
    spec, process = _spec_and_process(
        _compose("compose_max_stream")["integration_spec"]
    )
    result = run_fanout_recipe(
        recipe_id=RECIPE_DB_REST_FANOUT,
        components=spec.components,
        process=process,
    )
    root = dict(result.composed.process_roots)[process.key]
    legs = root.body.steps[-1].legs
    assert len(legs) == 25
    assert all(leg.terminal.kind == "target" for leg in legs)


# ---------------------------------------------------------------------------
# The response surface stays additive
# ---------------------------------------------------------------------------


def test_compose_response_carries_recipe_provenance():
    result = _compose("compose_stream")
    provenance = result["recipe_provenance"]
    assert provenance["adapter"]["entry_kind"] == "compatibility_adapter"
    assert provenance["adapter"]["adapter_target"]["recipe_id"] == RECIPE_DB_REST_FANOUT
    assert len(provenance["registry_revision"]) == 64


def test_preset_response_carries_recipe_provenance():
    result = _build_preset("api_to_api_sync_0")
    provenance = result["recipe_provenance"]
    assert provenance["adapter"]["adapter_target"]["recipe_id"] == RECIPE_API_TO_API_SYNC


def test_unmigrated_archetype_reports_no_recipe_provenance():
    """Honest absence beats a plausible-looking reference to unused code."""
    from boomi_mcp.patterns.archetypes.http_listener_to_db import (
        HttpListenerToDbArchetype,
    )

    example = HttpListenerToDbArchetype.examples[0]
    result = build_from_archetype_action("http_listener_to_db", example.parameters)
    assert result["_success"] is True
    assert "recipe_provenance" not in result


# ---------------------------------------------------------------------------
# Codex-review regression (issue #145): the pin must reach execution
# ---------------------------------------------------------------------------


def test_the_pin_names_the_executable_recipe_not_the_adapter():
    """``recipe_version`` pins what RUNS, not the compatibility wrapper.

    Validating the pin against the ADAPTER id made the real executable version
    (``0.1.0``) a hard ``RECIPE_VERSION_UNAVAILABLE`` while the adapter's
    unrelated ``1.0.0`` was accepted and then ignored — a pin the tool took and
    the execution did not keep (issue #145, Codex review).
    """
    from boomi_mcp.categories.integration_authoring import (
        get_integration_archetype_action,
    )
    from boomi_mcp.recipes import production_registry

    registry = production_registry()
    adapter = registry.resolve("boomi.adapter.api_to_api_sync")
    executable = registry.resolve(adapter.adapter_target.recipe_id)
    assert adapter.recipe_version != executable.recipe_version, (
        "the two versions must differ, or this test proves nothing"
    )

    ok = get_integration_archetype_action(
        "api_to_api_sync", recipe_version=executable.recipe_version
    )
    assert ok["_success"] is True
    assert (
        ok["recipe_executable_descriptor"]["recipe_version"]
        == executable.recipe_version
    )

    rejected = get_integration_archetype_action(
        "api_to_api_sync", recipe_version=adapter.recipe_version
    )
    assert rejected["_success"] is False
    assert rejected["error_code"] == "RECIPE_VERSION_UNAVAILABLE"


@pytest.mark.parametrize("name", sorted(_PRESET_CASES))
def test_a_pinned_build_reports_the_version_that_actually_ran(name):
    from boomi_mcp.recipes import production_registry

    archetype = _PRESET_CASES[name][0]
    adapter_id = f"boomi.adapter.{archetype}"
    registry = production_registry()
    executable = registry.resolve(
        registry.resolve(adapter_id).adapter_target.recipe_id
    )

    index = _PRESET_CASES[name][1]
    example = _EXAMPLES[archetype].examples[index]
    result = build_from_archetype_action(
        archetype, example.parameters, recipe_version=executable.recipe_version
    )
    assert result["_success"] is True, result.get("error")
    ran = result["recipe_provenance"]["executable"]
    assert ran["recipe_id"] == executable.recipe_id
    assert ran["recipe_version"] == executable.recipe_version


def test_a_pin_that_cannot_be_honoured_returns_no_spec():
    """Validated BEFORE emission — answering with a spec would answer a
    different question than the caller asked."""
    example = ApiToApiSyncArchetype.examples[0]
    result = build_from_archetype_action(
        "api_to_api_sync", example.parameters, recipe_version="9.9.9"
    )
    assert result["_success"] is False
    assert result["error_code"] == "RECIPE_VERSION_UNAVAILABLE"
    assert "integration_spec" not in result


@pytest.mark.parametrize("name", sorted(_PRESET_CASES))
def test_the_pin_is_carried_all_the_way_into_run_recipes(monkeypatch, name):
    """Structural: the version must appear on the RecipeRequestV1 the engine sees.

    The response field alone could be reporting an intention; this observes the
    request object the engine actually resolves.

    Parametrized over EVERY migrated preset. Covering only ``api_to_api_sync``
    left ``api_to_database_sync``'s threading unguarded — dropping the pin from
    its ``emit_spec`` kept the whole suite green while
    ``recipe_provenance.executable`` reported a version that did not run
    (issue #145, live QA).
    """
    import boomi_mcp.patterns.recipe_bridge as bridge
    from boomi_mcp.recipes import production_registry

    archetype, index = _PRESET_CASES[name]
    registry = production_registry()
    executable = registry.resolve(
        registry.resolve(f"boomi.adapter.{archetype}").adapter_target.recipe_id
    )

    seen = []
    real = bridge.run_recipes

    def spy(requests, **kwargs):
        seen.extend((r.recipe_id, r.recipe_version) for r in requests)
        return real(requests, **kwargs)

    monkeypatch.setattr(bridge, "run_recipes", spy)
    build_from_archetype_action(
        archetype,
        _EXAMPLES[archetype].examples[index].parameters,
        recipe_version=executable.recipe_version,
    )
    assert seen == [(executable.recipe_id, executable.recipe_version)]


@pytest.mark.parametrize("name", sorted(_PRESET_CASES))
def test_an_unpinned_build_reaches_the_engine_with_the_declared_version(
    monkeypatch, name
):
    """An absent pin still names a version — the adapter's declared target.

    This used to assert ``[None]``, i.e. "let the engine pick the default". That
    was the defect: the response reported the DECLARED version while the engine
    ran the DEFAULT, so the two agreed only by coincidence (issue #145, Codex
    review). The right control is that an unpinned call is explicit, not silent.
    """
    import boomi_mcp.patterns.recipe_bridge as bridge
    from boomi_mcp.recipes import production_registry

    archetype, index = _PRESET_CASES[name]
    declared = (
        production_registry()
        .resolve(f"boomi.adapter.{archetype}")
        .adapter_target.recipe_version
    )

    seen = []
    real = bridge.run_recipes

    def spy(requests, **kwargs):
        seen.extend(r.recipe_version for r in requests)
        return real(requests, **kwargs)

    monkeypatch.setattr(bridge, "run_recipes", spy)
    build_from_archetype_action(
        archetype, _EXAMPLES[archetype].examples[index].parameters
    )
    assert seen == [declared]


def test_an_unmigrated_archetype_still_rejects_a_pin():
    from boomi_mcp.patterns.archetypes.http_listener_to_db import (
        HttpListenerToDbArchetype,
    )

    result = build_from_archetype_action(
        "http_listener_to_db",
        HttpListenerToDbArchetype.examples[0].parameters,
        recipe_version="1.0.0",
    )
    assert result["_success"] is False
    assert "integration_spec" not in result


def test_every_archetype_accepts_the_uniform_emit_spec_contract():
    """``recipe_version`` is on the ABC, so all six implementations take it.

    Four ignore it — they have no recipe to pin — but the keyword is part of one
    contract rather than something only some subclasses happen to accept.
    """
    import inspect

    from boomi_mcp.patterns import PatternKind, PatternRegistry

    registry = PatternRegistry.from_package("boomi_mcp.patterns")
    archetypes = registry.list_patterns(kind=PatternKind.ARCHETYPE)
    assert len(archetypes) >= 6
    for cls in archetypes:
        parameters = inspect.signature(cls.emit_spec).parameters
        assert "recipe_version" in parameters, cls.metadata.name
        assert parameters["recipe_version"].kind is inspect.Parameter.KEYWORD_ONLY
        assert parameters["recipe_version"].default is None


def test_an_unpinned_resolve_uses_the_ADAPTERS_declared_version():
    """Not the target's own default — those can differ.

    Reverting ``recipe_version or adapter.adapter_target.recipe_version`` left
    the whole suite green, because every test drove construction and none
    observed the return value (issue #145, live QA). This builds a registry
    where the declared version is NOT the default, which is the only arrangement
    that can tell the two apart.
    """
    from boomi_mcp.categories.integration_authoring import _resolve_pinned_target
    from boomi_mcp.recipes.registry import build_test_registry
    from boomi_mcp.recipes.builtins.sync import SyncRecipeInputV1, emit_api_to_api_sync
    from boomi_mcp.recipes.contracts import (
        RecipeConflictPolicyV1,
        RecipeReferenceV1,
        RecipeRegistrationV1,
    )

    def target(version, default):
        return RecipeRegistrationV1(
            recipe_id="t.target",
            recipe_version=version,
            entry_kind="executable_recipe",
            is_default=default,
            input_model=SyncRecipeInputV1,
            executor=emit_api_to_api_sync,
            output_types=(
                "component_contribution",
                "constraint_requirement",
                "process_ir_patch",
            ),
            conflict_policy=RecipeConflictPolicyV1(),
        )

    registry = build_test_registry(
        (
            RecipeRegistrationV1(
                recipe_id="t.adapter",
                recipe_version="1.0.0",
                entry_kind="compatibility_adapter",
                is_default=True,
                # DECLARES 0.1.0 ...
                adapter_target=RecipeReferenceV1(
                    recipe_id="t.target", recipe_version="0.1.0"
                ),
            ),
            target("0.1.0", False),
            # ... while 0.2.0 is the DEFAULT.
            target("0.2.0", True),
        )
    )
    assert registry.resolve("t.target").recipe_version == "0.2.0"

    unpinned = _resolve_pinned_target("t.adapter", None, registry)
    assert unpinned["recipe_version"] == "0.1.0", "must follow the DECLARED version"

    pinned = _resolve_pinned_target("t.adapter", "0.2.0", registry)
    assert pinned["recipe_version"] == "0.2.0", "an explicit pin still wins"


# ---------------------------------------------------------------------------
# Codex re-review regressions (issue #145)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(_PRESET_CASES))
def test_an_unpinned_run_and_its_provenance_agree(monkeypatch, name):
    """One resolution, so the report cannot describe a version that did not run.

    Resolving separately let them disagree: an unpinned call passed ``None`` to
    the engine (which runs the DEFAULT) while the response derived
    ``recipe_provenance.executable`` from the adapter's DECLARED target
    (issue #145, Codex review).
    """
    import boomi_mcp.patterns.recipe_bridge as bridge

    archetype, index = _PRESET_CASES[name]
    seen = []
    real = bridge.run_recipes

    def spy(requests, **kwargs):
        seen.extend(r.recipe_version for r in requests)
        return real(requests, **kwargs)

    monkeypatch.setattr(bridge, "run_recipes", spy)
    result = build_from_archetype_action(
        archetype, _EXAMPLES[archetype].examples[index].parameters
    )
    assert result["_success"] is True
    provenance = result["recipe_provenance"]
    reported = provenance["executable"]["recipe_version"]
    assert seen == [reported], (seen, reported)
    assert None not in seen, "an unpinned run must still name its version"
    # The same field is pinned on ``compose_archetypes`` and was not here — the
    # one-of-two-identical-surfaces shape this issue keeps hitting (issue #145).
    from boomi_mcp.recipes import production_registry

    assert provenance["registry_revision"] == production_registry().registry_revision


def test_compose_runs_and_reports_the_same_version(monkeypatch):
    """``compose_archetypes`` exposes no pin, so there is ONE right answer."""
    import boomi_mcp.patterns.recipe_bridge as bridge

    seen = []
    real = bridge.run_recipes

    def spy(requests, **kwargs):
        seen.extend(r.recipe_version for r in requests)
        return real(requests, **kwargs)

    monkeypatch.setattr(bridge, "run_recipes", spy)
    result = _compose("compose_stream")
    reported = result["recipe_provenance"]["executable"]["recipe_version"]
    assert seen == [reported], (seen, reported)
    assert None not in seen


@pytest.mark.parametrize("pin", ["", " ", "  "])
def test_an_explicit_empty_pin_is_rejected_by_both_tools_alike(pin):
    """An explicit ``recipe_version=""`` is a PIN, not an absence.

    A truthiness fallback treated it as absent, so ``get_integration_archetype``
    returned the declared target while ``build_from_archetype`` failed later at
    the engine — the same input accepted by one tool and rejected by the other
    (issue #145, Codex review).
    """
    from boomi_mcp.categories.integration_authoring import (
        get_integration_archetype_action,
    )

    described = get_integration_archetype_action("api_to_api_sync", recipe_version=pin)
    assert described["_success"] is False
    assert described["error_code"] == "RECIPE_VERSION_UNAVAILABLE"

    built = build_from_archetype_action(
        "api_to_api_sync",
        ApiToApiSyncArchetype.examples[0].parameters,
        recipe_version=pin,
    )
    assert built["_success"] is False
    assert built["error_code"] == "RECIPE_VERSION_UNAVAILABLE"
    assert "integration_spec" not in built


def test_every_archetype_emit_spec_annotation_resolves():
    """``get_type_hints`` must not ``NameError`` on the widened ABC.

    Two modules used ``Optional`` in the new annotation without importing it —
    invisible under ``from __future__ import annotations`` until something
    resolves the hints (issue #145, Codex review).
    """
    import typing

    from boomi_mcp.patterns import PatternKind, PatternRegistry

    from boomi_mcp.patterns.base import ArchetypePattern

    archetypes = PatternRegistry.from_package("boomi_mcp.patterns").list_patterns(
        kind=PatternKind.ARCHETYPE
    )
    assert len(archetypes) >= 6
    for cls in archetypes:
        hints = typing.get_type_hints(cls.emit_spec)
        assert "recipe_version" in hints, cls.metadata.name

    # The ABC ITSELF. Every concrete archetype overrides ``emit_spec``, so
    # resolving the overrides never reaches ``base.py`` — breaking the ABC's own
    # annotation left the suite green while this docstring named it (issue #145,
    # live QA).
    base_hints = typing.get_type_hints(ArchetypePattern.emit_spec)
    assert "recipe_version" in base_hints


# ---------------------------------------------------------------------------
# #159 S2 — the registration-derived dry-run matrix
# ---------------------------------------------------------------------------
#
# GENERATED from every executable registration in ``PRODUCTION_REGISTRATIONS``,
# keyed by ``(recipe_id, recipe_version)``. Each row runs the recipe plan, the
# compile, dependency late binding and the canonical materialization the apply
# turn runs, with deterministic synthetic ids and the write boundary patched so
# any attempted live write fails the row. One seed FILE per registration
# (``tests/fixtures/recipe_matrix_seeds/<recipe_id>@<recipe_version>.json``), so
# a lane adds its own row without touching a shared file. Every seed records the
# causally independent oracle its values are read from.

_MATRIX_SEEDS = Path(__file__).resolve().parent.parent / "fixtures" / "recipe_matrix_seeds"
_MATRIX_ENTRY_KIND = "executable_recipe"
_SYNTHETIC_ID_PREFIX = "matrix-"

#: Every write the account could receive from a build, as the module-level names
#: the apply path calls. A row patches each to RECORD and RAISE.
_WRITE_BOUNDARY = (
    "boomi_mcp.categories.integration_builder.create_component",
    "boomi_mcp.categories.integration_builder.update_component",
    "boomi_mcp.categories.integration_builder.create_connector",
    "boomi_mcp.categories.integration_builder.update_connector",
    "boomi_mcp.categories.integration_builder.create_trading_partner",
    "boomi_mcp.categories.integration_builder.update_trading_partner",
    "boomi_mcp.categories.integration_builder._execute_component",
    "boomi_mcp.categories.components.manage_component.create_component",
    "boomi_mcp.categories.components.manage_component.update_component",
    "boomi_mcp.categories.components.manage_component.delete_component",
)


class MatrixSeedError(Exception):
    """The seed set and the enumerated executable set disagree."""


def matrix_seed_name(recipe_id, recipe_version):
    return f"{recipe_id}@{recipe_version}.json"


def matrix_pairing(registrations, seed_dir):
    """``((recipe_id, recipe_version), seed path)`` per executable registration.

    A pure function of the registrations and the directory, so the production
    call below and the witness's synthetic tuple run the same comparison. Both
    directions: a registration with no seed and a seed naming no registration
    are each a ``MatrixSeedError``, as is a file whose content names another key.
    """
    enumerated = sorted(
        {
            (reg.recipe_id, reg.recipe_version)
            for reg in registrations
            if reg.entry_kind == _MATRIX_ENTRY_KIND
        }
    )
    wanted = {matrix_seed_name(*key): key for key in enumerated}
    on_disk = {path.name: path for path in Path(seed_dir).glob("*.json")}
    missing = sorted(set(wanted) - set(on_disk))
    orphans = sorted(set(on_disk) - set(wanted))
    if missing or orphans:
        raise MatrixSeedError(
            f"recipe matrix seeds disagree with the executable registrations: "
            f"missing={missing} orphan={orphans}"
        )
    for name, key in wanted.items():
        seed = json.loads(on_disk[name].read_text())
        if (seed.get("recipe_id"), seed.get("recipe_version")) != key:
            raise MatrixSeedError(f"seed {name} names another registration")
    return tuple((key, on_disk[matrix_seed_name(*key)]) for key in enumerated)


def _matrix_production_pairing():
    from boomi_mcp.recipes.builtins.catalog import PRODUCTION_REGISTRATIONS

    return matrix_pairing(PRODUCTION_REGISTRATIONS, _MATRIX_SEEDS)


#: Computed at COLLECTION: a missing or orphan seed fails the module here.
_MATRIX_ROWS = _matrix_production_pairing()
_MATRIX_IDS = [f"{recipe_id}@{version}" for (recipe_id, version), _ in _MATRIX_ROWS]


def _load_seed(path):
    return json.loads(Path(path).read_text())


def _matrix_request(seed):
    from boomi_mcp.models.authoring_workflow import AuthoringRequestV1

    request = seed["request"]
    invocation = {
        "recipe_id": seed["recipe_id"],
        "recipe_version": seed["recipe_version"],
        "invocation_id": request["invocation_id"],
        "raw_input": request["raw_input"],
    }
    if "component_material" in request:
        invocation["component_material"] = request["component_material"]
    return AuthoringRequestV1.model_validate(
        {
            "intent": {
                "intent_kind": "recipe",
                "integration_name": request["integration_name"],
                "invocations": [invocation],
                "base_components": request.get("base_components", []),
                "process_envelopes": request.get("process_envelopes", []),
            }
        }
    )


class _WriteTripwire:
    """Patches every write-boundary name to record the call and raise."""

    def __init__(self):
        self.attempts = []
        self._patches = []

    def _recorder(self, name):
        def record(*_args, **_kwargs):
            self.attempts.append(name)
            raise AssertionError(f"live write attempted through {name}")

        return record

    def __enter__(self):
        for target in _WRITE_BOUNDARY:
            patcher = patch(target, self._recorder(target))
            patcher.start()
            self._patches.append(patcher)
        return self

    def __exit__(self, *exc):
        for patcher in reversed(self._patches):
            patcher.stop()
        return False


def _compile_matrix_seed(seed):
    from boomi_mcp.authoring.workflow import compile_authoring_request_v1

    return compile_authoring_request_v1(_matrix_request(seed), profile="matrix")


def _materialize_matrix_row(seed, internals):
    """Late-bind every compiled root to synthetic ids and materialize it."""
    from boomi_mcp.categories.components.canonical_process_apply import (
        materialize_canonical_process_xml,
    )

    spec = internals.normalized.integration_spec
    id_registry = {c.key: _SYNTHETIC_ID_PREFIX + c.key for c in spec.components}
    id_registry.update(seed.get("synthetic_ids", {}))
    materialized = {
        key: materialize_canonical_process_xml(
            plan=plan, id_registry=id_registry, symbols=internals.symbols
        )
        for key, plan in sorted(internals.materialization_plans.items())
    }
    return id_registry, materialized


def _bound_ids(xml, id_registry):
    """The registry ids that appear as a WHOLE attribute value in the XML."""
    import xml.etree.ElementTree as ET

    wanted = {value: key for key, value in id_registry.items()}
    found = set()
    for element in ET.fromstring(xml).iter():
        for value in element.attrib.values():
            if value in wanted:
                found.add(wanted[value])
    return sorted(found)


def _run_matrix_row(seed, compiled=None):
    """One full row. ``compiled`` reuses a cached ``(result, internals)``."""
    from boomi_mcp.categories.components.canonical_process_apply import _ref_key

    expected = seed["expected"]
    with _WriteTripwire() as tripwire:
        result, internals = compiled if compiled is not None else _compile_matrix_seed(seed)

        assert result.validation_report.is_valid, [d.code for d in result.errors]
        spec = internals.normalized.integration_spec
        assert sorted(s.component_key for s in result.process_cfg) == expected["process_keys"]
        assert sorted(internals.materialization_plans) == expected["process_keys"]
        assert sorted(u.envelope.component_key for u in spec.processes) == expected["process_keys"]
        assert [c.key for c in spec.components] == expected["supporting_component_keys"]
        for key, plan in internals.materialization_plans.items():
            assert plan.execution_profile == expected["execution_profiles"][key], key
            assert sorted({_ref_key(s.ref) for s in plan.unresolved_symbol_slots}) == (
                expected["references"][key]
            ), key

        id_registry, materialized = _materialize_matrix_row(seed, internals)
        assert sorted(materialized) == expected["process_keys"]
        for key, xml in materialized.items():
            assert _bound_ids(xml, id_registry) == expected["references"][key], key
    assert tripwire.attempts == [], tripwire.attempts
    return materialized


_MATRIX_COMPILED = {}


def _compiled_row(path):
    if path not in _MATRIX_COMPILED:
        with _WriteTripwire() as tripwire:
            _MATRIX_COMPILED[path] = _compile_matrix_seed(_load_seed(path))
        assert tripwire.attempts == [], tripwire.attempts
    return _MATRIX_COMPILED[path]


@pytest.mark.parametrize("path", [p for _, p in _MATRIX_ROWS], ids=_MATRIX_IDS)
def test_dry_run_matrix_row(path):
    _run_matrix_row(_load_seed(path), compiled=_compiled_row(path))


@pytest.mark.parametrize("path", [p for _, p in _MATRIX_ROWS], ids=_MATRIX_IDS)
def test_a_matrix_seed_violating_its_input_model_fails_its_row(path):
    """Non-vacuity: an undeclared key — refused by every closed input model —
    must fail the row before anything is compiled."""
    from boomi_mcp.authoring.workflow import AuthoringWorkflowError

    from boomi_mcp.errors import RECIPE_INPUT_INVALID

    seed = _load_seed(path)
    seed["request"]["raw_input"]["matrix_undeclared_field"] = "x"
    with pytest.raises(AuthoringWorkflowError) as caught:
        _run_matrix_row(seed)
    assert any(
        RECIPE_INPUT_INVALID in diagnostic.cause_codes
        for diagnostic in caught.value.diagnostics
    ), [d.cause_codes for d in caught.value.diagnostics]


def test_a_matrix_row_whose_materialization_attempts_a_write_fails(monkeypatch):
    """Non-vacuity of the zero-write assertion: a materializer that reaches the
    write boundary must fail the row, through the tripwire, not pass it."""
    from boomi_mcp.categories import integration_builder
    from boomi_mcp.categories.components import process_component_materializer

    (_key, path), *_ = _MATRIX_ROWS
    real = process_component_materializer.ProcessComponentMaterializer.materialize

    def writing_materialize(self, *args, **kwargs):
        try:
            integration_builder.create_component(None, "matrix", {"xml": ""})
        except AssertionError:
            pass  # swallowed, as a careless caller would: the RECORD must still fail the row
        return real(self, *args, **kwargs)

    monkeypatch.setattr(
        process_component_materializer.ProcessComponentMaterializer,
        "materialize",
        writing_materialize,
    )
    with pytest.raises(AssertionError, match="create_component"):
        _run_matrix_row(_load_seed(path), compiled=_compiled_row(path))


def test_the_matrix_covers_a_registration_set_in_both_directions(tmp_path):
    """Registry witness: adding an executable registration without a seed fails
    the pairing, and so does a seed naming no registration."""
    import shutil

    from boomi_mcp.recipes.builtins.catalog import PRODUCTION_REGISTRATIONS
    from boomi_mcp.recipes.contracts import RecipeRegistrationV1

    for _key, path in _MATRIX_ROWS:
        shutil.copy(path, tmp_path / path.name)
    assert matrix_pairing(PRODUCTION_REGISTRATIONS, tmp_path) == tuple(
        (key, tmp_path / path.name) for key, path in _MATRIX_ROWS
    )

    template = next(r for r in PRODUCTION_REGISTRATIONS if r.entry_kind == _MATRIX_ENTRY_KIND)
    added = RecipeRegistrationV1(
        **{
            **{f: getattr(template, f) for f in template.__dataclass_fields__},
            "recipe_id": "boomi.archetype.matrix_witness",
            "recipe_version": "9.0.0",
            "is_default": True,
        }
    )
    with pytest.raises(MatrixSeedError, match="boomi.archetype.matrix_witness@9.0.0"):
        matrix_pairing(PRODUCTION_REGISTRATIONS + (added,), tmp_path)

    # Non-executable entries are not rows: a constraint-only addition needs no seed.
    constraint = next(r for r in PRODUCTION_REGISTRATIONS if r.entry_kind == "constraint_only")
    matrix_pairing(PRODUCTION_REGISTRATIONS + (constraint,), tmp_path)

    (tmp_path / "boomi.archetype.unregistered@1.0.0.json").write_text("{}")
    with pytest.raises(MatrixSeedError, match="orphan"):
        matrix_pairing(PRODUCTION_REGISTRATIONS, tmp_path)


def test_every_published_pin_resolves():
    """Registry witness: every (id, version) the snapshot publishes, every
    declared default and every adapter's exact target resolves — the target to an
    executable entry — and every matrix seed pins a published version."""
    from boomi_mcp.recipes import production_registry

    registry = production_registry()
    entries = registry.snapshot()["entries"]
    published = {(entry["recipe_id"], entry["recipe_version"]) for entry in entries}
    assert published
    for entry in entries:
        recipe_id, version = entry["recipe_id"], entry["recipe_version"]
        descriptor = registry.resolve(recipe_id, version)
        assert descriptor.recipe_version == version
        if entry["is_default"]:
            assert registry.resolve(recipe_id).recipe_version == version
        if descriptor.adapter_target is not None:
            target = registry.resolve(
                descriptor.adapter_target.recipe_id,
                descriptor.adapter_target.recipe_version,
            )
            assert target.entry_kind == _MATRIX_ENTRY_KIND
    assert {key for key, _ in _MATRIX_ROWS} <= published
