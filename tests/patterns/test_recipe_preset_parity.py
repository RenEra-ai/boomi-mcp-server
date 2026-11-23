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
from typing import Literal
from unittest.mock import MagicMock, patch

import pytest
from pydantic import Field

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
from boomi_mcp.recipes import placeholder_component_id
from boomi_mcp.recipes.contracts import RecipeInputBase
from boomi_mcp.recipes.builtins.catalog import (
    RECIPE_API_TO_API_SYNC,
    RECIPE_API_TO_DATABASE_SYNC,
    RECIPE_DB_REST_FANOUT,
)

from test_archetype_composition import _cache_links, _options, _parts

import sys as _sys

_TESTS_ROOT = str(Path(__file__).resolve().parent.parent)
if _TESTS_ROOT not in _sys.path:
    _sys.path.insert(0, _TESTS_ROOT)

from _fanout_v2_support import V2, respell_oracle_credential_refs, v2_from_compose_request  # noqa: E402

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


def _compose_request(name):
    parts_fn, links_fn = _COMPOSE_CASES[name]
    options = dict(_options())
    if links_fn is not None:
        options["links"] = links_fn()
    return {"parts": parts_fn(), "options": options}


def _compose(name):
    """The RETIRED compose tool's response (#159 S3). Imported here so only the
    tests of the retired surface fail once the action is deleted."""
    from boomi_mcp.categories.integration_authoring import compose_archetypes_action

    request = _compose_request(name)
    result = compose_archetypes_action(parts=request["parts"], options=request["options"])
    assert result["_success"] is True, result.get("error")
    return result


def _build_preset(name):
    """The RETIRED archetype build tool's response (#159 S3), imported lazily
    for the same reason as :func:`_compose`."""
    from boomi_mcp.categories.integration_authoring import build_from_archetype_action

    archetype, index = _PRESET_CASES[name]
    example = _EXAMPLES[archetype].examples[index]
    result = _build_from_archetype(archetype, example.parameters)
    assert result["_success"] is True, result.get("error")
    return result


def _build_from_archetype(*args, **kwargs):
    from boomi_mcp.categories.integration_authoring import build_from_archetype_action

    return build_from_archetype_action(*args, **kwargs)


# #159 S3: the compose ARMS of L1-L4. Fan-out 1.0.0 and the compose tool are
# retired, so the RECIPE arm is the surviving fan-out 2.0.0 — run on the SAME
# composition, re-spelled into its split by ``v2_from_compose_request`` — and
# the LEGACY arm is the composition FROZEN at the baseline commit (the L1
# fixture), lowered through the isolated legacy adapter. Neither side is
# produced by the retired code path.


def _legacy_compose_spec(name):
    return _fixture(f"{name}.json")


def _v2_run(name):
    """``(prepared, run_result)`` of fan-out 2.0.0 on the named composition."""
    from boomi_mcp.authoring.workflow import _connector_metadata_from_components
    from boomi_mcp.recipes import MaterializationCatalog, RecipeRequestV1, run_recipes
    from boomi_mcp.recipes.builtins.fanout import (
        ComposeDbRestFanoutInputV2,
        FanoutComponentMaterialV2,
        prepare_db_rest_fanout_v2,
    )

    raw, material = v2_from_compose_request(_compose_request(name))
    prepared = prepare_db_rest_fanout_v2(
        ComposeDbRestFanoutInputV2.model_validate(raw),
        FanoutComponentMaterialV2.model_validate(material),
        base_catalog={},
    )
    result = run_recipes(
        [RecipeRequestV1(RECIPE_DB_REST_FANOUT, "parity", raw, V2)],
        catalog=MaterializationCatalog({}),
        connector_metadata=_connector_metadata_from_components(
            prepared.supporting_components
        ),
        supporting_components=prepared.supporting_components,
    )
    return prepared, result


def _v2_root(name):
    _prepared, result = _v2_run(name)
    return dict(result.composed.process_roots)["main_process"]


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
    """#159 S3: fan-out 2.0.0's supporting components against the baseline dump,
    every field of every component (the one documented credential re-spelling
    applied to the expected side); its process is a unit, whose envelope matches
    the baseline process entry here and whose XML L3 compares."""
    prepared, result = _v2_run(name)
    baseline = _fixture(f"{name}.json")
    expected = [
        c for c in respell_oracle_credential_refs(baseline["components"]) if c["type"] != "process"
    ]
    assert [c.model_dump(mode="json") for c in prepared.supporting_components] == expected
    (unit,) = result.process_units
    process = baseline["components"][-1]
    assert unit.envelope.component_key == process["key"]
    assert unit.envelope.name == process["name"]
    assert set(unit.envelope.depends_on) == set(process["depends_on"])


# ---------------------------------------------------------------------------
# L2 — ComponentPlan order
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(_COMPOSE_CASES))
def test_l2_compose_component_plan_order_is_unchanged(name):
    """#159 S3: the planner's order for the typed spec fan-out 2.0.0 produces —
    the baseline envelope, the supporting components and the process unit —
    against the baseline ComponentPlan."""
    prepared, result = _v2_run(name)
    spec = dict(_fixture(f"{name}.json"))
    spec["components"] = [c.model_dump(mode="json") for c in prepared.supporting_components]
    spec["processes"] = [unit.model_dump(mode="json") for unit in result.process_units]
    assert _plan_steps(spec) == _fixture(f"{name}_component_plan.json")


# ---------------------------------------------------------------------------
# L3 — process XML differential (the primary recipe oracle)
# ---------------------------------------------------------------------------


def _spec_and_process(payload):
    spec = IntegrationSpecV1.model_validate(payload)
    return spec, spec.components[-1]


@pytest.mark.parametrize("name", sorted(_COMPOSE_CASES))
def test_l3_compose_recipe_arm_matches_the_legacy_arm_byte_for_byte(name):
    _spec, process = _spec_and_process(_legacy_compose_spec(name))

    legacy = emit_legacy_result(
        adapt_flow_sequence(process.config),
        resolver=placeholder_component_id,
        dialect="flow_sequence",
    )
    _prepared, result = _v2_run(name)
    artifact = result.artifact_for(process.key)
    assert artifact is not None
    assert artifact.process_xml == legacy.process_xml
    assert artifact.verifier.errors == ()


#: #159 S3: the API->API preset arm (``api_to_api_sync_0``) retired with its
#: executable; its golden stays byte-pinned through the direct canonical unit
#: (``recipe:api_to_api_sync_0`` in the wave-gate golden manifest).
_GOLDEN_CASES = {
    "compose_stream": "composed_db_to_api_fanout.xml",
    "compose_all_cache": "composed_db_to_api_cache_fanout.xml",
}


@pytest.mark.parametrize("name", sorted(_GOLDEN_CASES))
def test_l3_recipe_arm_matches_its_committed_golden(name):
    """A uniform-drift detector, NOT the oracle.

    The differential above is what proves the recipe arm equals the legacy arm.
    This catches the case where BOTH arms change together — which the
    differential, by construction, cannot see.
    """
    golden = (_GOLDEN / _GOLDEN_CASES[name]).read_bytes().decode("utf-8")
    # #159 S3: the surviving fan-out 2.0.0 on the same composition.
    assert name in _COMPOSE_CASES
    _prepared, result = _v2_run(name)
    assert result.artifact_for("main_process").process_xml == golden


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

    # #159 S3: the legacy arm's composition is the frozen baseline dump.
    _spec, process = _spec_and_process(_legacy_compose_spec(name))
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
    # #159 S3: the recipe arm is fan-out 2.0.0. It compiled under
    # validation_policy=None (the engine hard-codes it), so reaching here at all
    # is the assertion. Now show WHY: one shared reference across the staging
    # put and every consuming get.
    root = _v2_root(name)
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
    root = _v2_root("compose_stream")
    branch = root.body.steps[-1]
    for leg in branch.legs:
        assert leg.terminal.kind == "target"
        assert leg.steps == []


# ---------------------------------------------------------------------------
# Composition invariants the issue names explicitly
# ---------------------------------------------------------------------------


def test_cache_staging_leg_precedes_the_first_consumer():
    root = _v2_root("compose_mixed_cache")
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
    root = _v2_root("compose_max_cache")
    legs = root.body.steps[-1].legs
    assert len(legs) == 25
    assert sum(1 for leg in legs if leg.terminal.kind == "cache_put") == 1


def test_max_stream_composition_carries_all_twenty_five_targets():
    root = _v2_root("compose_max_stream")
    legs = root.body.steps[-1].legs
    assert len(legs) == 25
    assert all(leg.terminal.kind == "target" for leg in legs)


# ---------------------------------------------------------------------------
# The response surface stays additive
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Codex-review regression (issue #145): the pin must reach execution
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Codex re-review regressions (issue #145)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("pin", ["", " ", "  "])
def test_an_explicit_empty_pin_is_rejected_by_both_tools_alike(pin):
    """An explicit ``recipe_version=""`` is a PIN, not an absence.

    A truthiness fallback treated it as absent, so ``get_integration_archetype``
    returned the declared target while ``build_from_archetype`` failed later at
    the engine — the same input accepted by one tool and rejected by the other
    (issue #145, Codex review).
    """
    # #159 S3: the two tools are now the recipe-only get and the typed
    # build_integration (the archetype build tool is retired). The SAME blank
    # pin on the surviving fan-out id refuses on both, and neither answers with
    # a spec; the get names the pin as unavailable.
    from unittest import mock

    from boomi_mcp.categories import integration_builder
    from boomi_mcp.categories.integration_authoring import (
        get_integration_archetype_action,
    )
    from _fanout_v2_support import flagship_v2, recipe_request

    described = get_integration_archetype_action(RECIPE_DB_REST_FANOUT, recipe_version=pin)
    assert described["_success"] is False
    assert described["error_code"] == "RECIPE_VERSION_UNAVAILABLE"

    request = recipe_request(*flagship_v2()).model_dump(mode="json", exclude_none=True)
    request["intent"]["invocations"][0]["recipe_version"] = pin
    with mock.patch.object(integration_builder, "paginate_metadata", lambda *a, **k: []):
        built = integration_builder.build_integration_action(
            mock.MagicMock(), "p", "plan", {"authoring_request": request}
        )
    assert built["_success"] is False
    assert "integration_spec" not in built and "authoring_result" not in built


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
    executables = {
        (reg.recipe_id, reg.recipe_version): reg
        for reg in registrations
        if reg.entry_kind == _MATRIX_ENTRY_KIND
    }
    enumerated = sorted(executables)
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
        if seed.get("seed_format") != "1":
            raise MatrixSeedError(f"seed {name} is not seed_format 1")
        provenance = seed.get("provenance") or {}
        if not all(
            isinstance(provenance.get(field), str) and provenance[field]
            for field in ("source", "source_sha256", "derivation")
        ):
            raise MatrixSeedError(f"seed {name} does not record its provenance")
        has_material = "component_material" in (seed.get("request") or {})
        if has_material != (executables[key].component_material_model is not None):
            raise MatrixSeedError(
                f"seed {name} must carry component_material exactly when its "
                "registration declares a material model"
            )
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


class _MatrixStubInputV1(RecipeInputBase):
    """The zero-supporting-component witness's input."""

    version: Literal["1"]
    process_key: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    process_name: str = Field(min_length=1)


def _matrix_stub_executor(inp):
    """One process unit, passthrough entry then Stop; no supporting component.

    A bare ``[stop]`` sequence is not a legal ProcessIRV1 root at this tip (a
    connector-flow sequence must start with its source), so the witness uses the
    smallest legal component-free root.
    """
    from boomi_mcp.models.process_component import (
        ProcessAuthoringUnitV1,
        ProcessComponentEnvelopeV1,
    )
    from boomi_mcp.models.process_ir import ProcessIRV1
    from boomi_mcp.models.recipe_contributions import (
        ConstraintRequirementV1,
        ProcessUnitContributionV1,
        RequireProcessV1,
    )

    root = ProcessIRV1.model_validate(
        {"version": "1", "body": {"kind": "sequence", "steps": [{"kind": "passthrough"}, {"kind": "stop"}]}}
    )
    envelope = ProcessComponentEnvelopeV1(
        component_key=inp.process_key, name=inp.process_name, action="create"
    )
    return (
        ProcessUnitContributionV1(
            contribution_kind="process_unit",
            version="1",
            contribution_id="unit.process",
            unit=ProcessAuthoringUnitV1(envelope=envelope, process_ir=root),
        ),
        ConstraintRequirementV1(
            contribution_kind="constraint_requirement",
            version="1",
            requirement_id="req.process",
            requirement=RequireProcessV1(kind="process", process_key=inp.process_key),
        ),
    )


def test_a_zero_supporting_component_registration_completes_a_matrix_row(
    tmp_path, monkeypatch
):
    """The matrix MECHANISM on a component-free recipe, before the real stub lands.

    A test registration carrying descriptor guidance (an example) is installed
    at the engine's registry seam, paired with a temp seed through the same
    ``matrix_pairing`` and run through the same ``_run_matrix_row``: empty
    supporting components, an empty id registry, an empty reference set, zero
    writes. The seed's expected values describe this witness recipe only.
    """
    from boomi_mcp.recipes import engine
    from boomi_mcp.recipes.contracts import (
        RecipeConflictPolicyV1,
        RecipeExampleV1,
        RecipeRegistrationV1,
    )
    from boomi_mcp.recipes.registry import build_test_registry

    raw = {"version": "1", "process_key": "stub_process", "process_name": "Matrix Stub"}
    stub = RecipeRegistrationV1(
        recipe_id="boomi.archetype.matrix_stub_witness",
        recipe_version="1.0.0",
        entry_kind="executable_recipe",
        is_default=True,
        input_model=_MatrixStubInputV1,
        executor=_matrix_stub_executor,
        output_types=("constraint_requirement", "process_unit"),
        conflict_policy=RecipeConflictPolicyV1(),
        limitations=("Emits a passthrough root that only stops.",),
        examples=(
            RecipeExampleV1(
                example_id="stub.basic",
                summary="A passthrough process that only stops.",
                is_template=False,
                raw_input=raw,
            ),
        ),
    )
    registry = build_test_registry((stub,))
    monkeypatch.setattr(engine, "production_registry", lambda: registry)

    seed = {
        "seed_format": "1",
        "recipe_id": stub.recipe_id,
        "recipe_version": stub.recipe_version,
        "provenance": {
            "source": "this test",
            "source_sha256": "-",
            "derivation": "a mechanism witness; no production recipe",
        },
        "request": {
            "integration_name": "Matrix stub witness",
            "invocation_id": "matrix",
            "raw_input": raw,
        },
        "expected": {
            "process_keys": ["stub_process"],
            # The execution-profile rule: a passthrough entry derives "passthrough".
            "execution_profiles": {"stub_process": "passthrough"},
            "supporting_component_keys": [],
            "references": {"stub_process": []},
        },
    }
    (tmp_path / matrix_seed_name(stub.recipe_id, stub.recipe_version)).write_text(
        json.dumps(seed)
    )
    ((_key, path),) = matrix_pairing((stub,), tmp_path)
    materialized = _run_matrix_row(_load_seed(path))
    assert list(materialized) == ["stub_process"]


def test_matrix_pairing_refuses_a_seed_without_provenance_or_with_stray_material(tmp_path):
    import shutil

    from boomi_mcp.recipes.builtins.catalog import PRODUCTION_REGISTRATIONS

    for _key, path in _MATRIX_ROWS:
        shutil.copy(path, tmp_path / path.name)
    (_key, first), *_ = _MATRIX_ROWS
    seed = _load_seed(first)
    target = tmp_path / first.name

    unsourced = dict(seed, provenance={"source": "x"})
    target.write_text(json.dumps(unsourced))
    with pytest.raises(MatrixSeedError, match="provenance"):
        matrix_pairing(PRODUCTION_REGISTRATIONS, tmp_path)

    flipped = json.loads(json.dumps(seed))
    if "component_material" in flipped["request"]:
        del flipped["request"]["component_material"]
    else:
        flipped["request"]["component_material"] = {}
    target.write_text(json.dumps(flipped))
    with pytest.raises(MatrixSeedError, match="component_material"):
        matrix_pairing(PRODUCTION_REGISTRATIONS, tmp_path)
