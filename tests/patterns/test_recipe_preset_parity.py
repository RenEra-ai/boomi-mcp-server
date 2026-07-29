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
def test_l4_legacy_cache_arm_still_requires_its_exemption(name):
    """The claim's OTHER half.

    The recipe arm passing strictly is only interesting if the legacy arm does
    not. This asserts the legacy occurrence-aliased arm still fails
    ``validation_policy=None`` with the lineage code, so "the shared cache ref is
    what removes the exemption" stays a measured difference rather than a story.
    """
    _spec, process = _spec_and_process(_compose(name)["integration_spec"])
    adapted = adapt_flow_sequence(process.config)

    with pytest.raises(Exception) as excinfo:
        emit_legacy_result(adapted, resolver=placeholder_component_id, dialect=None)
    cause = excinfo.value.__cause__
    assert isinstance(cause, ProcessIRCompileError)
    assert {d.code for d in cause.diagnostics} == {
        "PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING"
    }

    # ... and with the dialect's registered policy it passes, so the failure
    # above is the EXEMPTION being withheld and not some unrelated breakage.
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
