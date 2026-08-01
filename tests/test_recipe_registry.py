"""Recipe registry, versioning, provenance and skew (issue #145 M12.10).

The registry is the thing that makes "which code is executable here" provable, so
these tests care about three properties above all: entry kinds are distinguished
mechanically, discovery is independent of registration order, and provenance is
derived from code rather than accepted from anyone.
"""

import json
import os
import random
import re
import sys
import tempfile
from pathlib import Path

import pytest

_project_root = Path(__file__).resolve().parent.parent
_src = str(_project_root / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.build_info import (
    BUILD_REVISION_PATH,
    image_build_revision,
    is_source_digest,
    source_digest,
    source_revision,
)
from boomi_mcp.errors import (
    RECIPE_CAPABILITY_GATED,
    RECIPE_NOT_FOUND,
    RECIPE_VERSION_UNAVAILABLE,
)
from boomi_mcp.recipes import RecipeError, build_test_registry, production_registry
from boomi_mcp.recipes import registry as registry_module
from boomi_mcp.recipes.builtins import catalog
from boomi_mcp.recipes.builtins.catalog import (
    PRODUCTION_REGISTRATIONS,
    RECIPE_ADVISORY_INTEGRATION_DESIGN,
    RECIPE_API_TO_API_SYNC,
    RECIPE_CONSTRAINT_INBOUND_VALIDATE,
    RECIPE_DB_REST_FANOUT,
)
from boomi_mcp.recipes.contracts import (
    ExpectedRecipeEntryV1,
    ExpectedRecipeRegistryV1,
    RecipeCapabilityRequirementV1,
    RecipeConflictPolicyV1,
    RecipeReferenceV1,
    RecipeRegistrationV1,
    parse_semver,
)
from boomi_mcp.recipes.builtins.sync import SyncRecipeInputV1, emit_api_to_api_sync


def _inherited_env():
    """The caller's environment, copied. Separate from :func:`_clean_env`, which
    strips ``PYTHONPATH`` — the tracer driver NEEDS it set."""
    return dict(os.environ)


def _clean_env():
    """The child's environment with ``PYTHONPATH`` REMOVED.

    A bare ``subprocess.run`` inherits it, and this repo is normally driven with
    ``PYTHONPATH=src`` — so the child could resolve ``boomi_mcp.*`` through the
    shell and the namespace isolation these probes rely on came from the caller,
    not from the test. An absolute import added to an owned module passed
    (issue #145, live QA).
    """
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    return env


def _reg(**kwargs):
    base = dict(
        recipe_id="test.recipe",
        recipe_version="1.0.0",
        entry_kind="executable_recipe",
        is_default=True,
        input_model=SyncRecipeInputV1,
        executor=emit_api_to_api_sync,
        output_types=(
            "component_contribution",
            "constraint_requirement",
            "process_ir_patch",
        ),
        conflict_policy=RecipeConflictPolicyV1(),
    )
    base.update(kwargs)
    return RecipeRegistrationV1(**base)


# ---------------------------------------------------------------------------
# SemVer
# ---------------------------------------------------------------------------


def test_semver_orders_numerically_not_lexically():
    """The classic trap: ``1.10.0`` must sort AFTER ``1.9.0``."""
    versions = ["1.9.0", "1.10.0", "1.2.0", "2.0.0", "1.0.0"]
    assert sorted(versions, key=parse_semver) == [
        "1.0.0",
        "1.2.0",
        "1.9.0",
        "1.10.0",
        "2.0.0",
    ]


def test_a_prerelease_sorts_before_its_release():
    assert parse_semver("1.0.0-alpha") < parse_semver("1.0.0")
    assert parse_semver("1.0.0-alpha.1") < parse_semver("1.0.0-alpha.beta")
    assert parse_semver("1.0.0-2") < parse_semver("1.0.0-rc")


def test_build_metadata_is_ignored_for_precedence():
    assert parse_semver("1.0.0+build.1") == parse_semver("1.0.0+build.2")


@pytest.mark.parametrize("bad", ["1", "1.0", "v1.0.0", "1.0.0.0", "01.0.0", ""])
def test_an_invalid_semver_is_rejected_at_construction(bad):
    with pytest.raises(ValueError):
        build_test_registry((_reg(recipe_version=bad),))


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------


def test_an_exact_version_resolves():
    registry = build_test_registry((_reg(),))
    assert registry.resolve("test.recipe", "1.0.0").recipe_version == "1.0.0"


def test_a_name_only_call_selects_the_code_declared_default():
    registry = build_test_registry(
        (
            _reg(recipe_version="1.0.0", is_default=False),
            _reg(recipe_version="2.0.0", is_default=True),
        )
    )
    assert registry.resolve("test.recipe").recipe_version == "2.0.0"


def test_an_exact_request_never_falls_forward_or_backward():
    """A silent fall-forward defeats the entire point of pinning."""
    registry = build_test_registry(
        (_reg(recipe_version="1.0.0"), _reg(recipe_version="2.0.0", is_default=False))
    )
    with pytest.raises(RecipeError) as exc:
        registry.resolve("test.recipe", "1.5.0")
    diagnostic = exc.value.diagnostics[0]
    assert diagnostic.code == RECIPE_VERSION_UNAVAILABLE
    assert diagnostic.available_versions == ("1.0.0", "2.0.0")


def test_an_unknown_recipe_id_is_not_found():
    registry = build_test_registry((_reg(),))
    with pytest.raises(RecipeError) as exc:
        registry.resolve("nope")
    assert exc.value.diagnostics[0].code == RECIPE_NOT_FOUND


def test_parallel_versions_coexist():
    registry = build_test_registry(
        (_reg(recipe_version="1.0.0"), _reg(recipe_version="2.0.0", is_default=False))
    )
    assert registry.versions_for("test.recipe") == ("1.0.0", "2.0.0")


def test_two_defaults_for_one_id_fail_at_construction():
    with pytest.raises(ValueError, match="default"):
        build_test_registry(
            (_reg(recipe_version="1.0.0"), _reg(recipe_version="2.0.0"))
        )


def test_a_duplicate_registration_fails_at_construction():
    with pytest.raises(ValueError, match="duplicate"):
        build_test_registry((_reg(), _reg(is_default=False)))


# ---------------------------------------------------------------------------
# Entry-kind invariants
# ---------------------------------------------------------------------------


def test_advisory_may_declare_no_executor_input_or_output():
    """The STRUCTURAL reason doctrine can never become executable."""
    for kwargs in (
        {"executor": emit_api_to_api_sync},
        {"input_model": SyncRecipeInputV1},
        {"output_types": ("constraint_requirement",)},
        {"adapter_target": RecipeReferenceV1(recipe_id="x.y", recipe_version="1.0.0")},
        {"conflict_policy": RecipeConflictPolicyV1()},
    ):
        base = dict(
            entry_kind="advisory",
            executor=None,
            input_model=None,
            output_types=(),
            conflict_policy=None,
        )
        base.update(kwargs)
        with pytest.raises(ValueError, match="advisory"):
            build_test_registry((_reg(**base),))


def test_an_advisory_entry_registers_cleanly_with_nothing_attached():
    registry = build_test_registry(
        (
            _reg(
                entry_kind="advisory",
                executor=None,
                input_model=None,
                output_types=(),
                conflict_policy=None,
            ),
        )
    )
    descriptor = registry.resolve("test.recipe")
    assert descriptor.entry_kind == "advisory"
    assert descriptor.output_types == ()
    with pytest.raises(RecipeError):
        registry.executor_for(descriptor)


def test_constraint_only_may_declare_only_constraint_output():
    with pytest.raises(ValueError, match="constraint_only"):
        build_test_registry(
            (
                _reg(
                    entry_kind="constraint_only",
                    output_types=("component_contribution", "constraint_requirement"),
                ),
            )
        )


def test_a_compatibility_adapter_needs_an_exact_target_and_no_executor():
    with pytest.raises(ValueError, match="adapter target"):
        build_test_registry(
            (
                _reg(
                    entry_kind="compatibility_adapter",
                    executor=None,
                    input_model=None,
                    output_types=(),
                    conflict_policy=None,
                    adapter_target=None,
                ),
            )
        )
    with pytest.raises(ValueError, match="compatibility_adapter"):
        build_test_registry(
            (
                _reg(
                    entry_kind="compatibility_adapter",
                    conflict_policy=None,
                    adapter_target=RecipeReferenceV1(
                        recipe_id="x.y", recipe_version="1.0.0"
                    ),
                ),
            )
        )


def test_an_executable_recipe_must_declare_at_least_one_output():
    with pytest.raises(ValueError, match="output type"):
        build_test_registry((_reg(output_types=()),))


def test_an_unknown_output_type_fails_at_construction():
    with pytest.raises(ValueError, match="unknown output type"):
        build_test_registry((_reg(output_types=("nonsense",)),))


def test_output_types_must_be_sorted_and_unique():
    with pytest.raises(ValueError, match="sorted"):
        build_test_registry(
            (_reg(output_types=("process_ir_patch", "component_contribution")),)
        )
    with pytest.raises(ValueError, match="duplicate"):
        build_test_registry(
            (_reg(output_types=("process_ir_patch", "process_ir_patch")),)
        )


def test_an_input_model_must_subclass_recipe_input_base():
    from pydantic import BaseModel

    class Loose(BaseModel):
        pass

    with pytest.raises(ValueError, match="RecipeInputBase"):
        build_test_registry((_reg(input_model=Loose),))


# ---------------------------------------------------------------------------
# Executor shape
# ---------------------------------------------------------------------------


async def _async_executor(inp):  # pragma: no cover - rejected at registration
    return ()


def test_a_coroutine_executor_is_rejected():
    with pytest.raises(ValueError, match="coroutine"):
        build_test_registry((_reg(executor=_async_executor),))


def test_a_lambda_executor_is_rejected():
    with pytest.raises(ValueError):
        build_test_registry((_reg(executor=lambda inp: ()),))


def test_a_partial_executor_is_rejected():
    import functools

    with pytest.raises(ValueError, match="module-level function"):
        build_test_registry(
            (_reg(executor=functools.partial(emit_api_to_api_sync)),)
        )


def test_a_closure_executor_is_rejected():
    """A closure's captured state is invisible to ``inspect.getsource``.

    Its implementation hash would therefore be blind to the very thing that
    changes its behavior — and the skew report would say ``match`` for two
    registries running different code.
    """
    captured = {"n": 1}

    def closing(inp):  # pragma: no cover - rejected at registration
        return captured["n"]

    with pytest.raises(ValueError):
        build_test_registry((_reg(executor=closing),))


def test_a_bound_method_executor_is_rejected():
    class Holder:
        def run(self, inp):  # pragma: no cover - rejected at registration
            return ()

    with pytest.raises(ValueError):
        build_test_registry((_reg(executor=Holder().run),))


# ---------------------------------------------------------------------------
# Deterministic discovery
# ---------------------------------------------------------------------------


def test_registry_revision_is_invariant_under_registration_order():
    """Randomized, because a single reversed pair proves much less."""
    baseline = build_test_registry(PRODUCTION_REGISTRATIONS).registry_revision
    rng = random.Random(20261010)
    for _ in range(8):
        shuffled = list(PRODUCTION_REGISTRATIONS)
        rng.shuffle(shuffled)
        assert build_test_registry(tuple(shuffled)).registry_revision == baseline


def test_descriptors_are_sorted_by_id_then_parsed_semver_then_kind():
    registry = build_test_registry(
        (
            _reg(recipe_id="b.recipe", recipe_version="1.9.0"),
            _reg(recipe_id="a.recipe", recipe_version="1.10.0"),
            _reg(recipe_id="a.recipe", recipe_version="1.9.0", is_default=False),
        )
    )
    assert [
        (d.recipe_id, d.recipe_version) for d in registry.descriptors()
    ] == [("a.recipe", "1.9.0"), ("a.recipe", "1.10.0"), ("b.recipe", "1.9.0")]


def test_a_descriptor_change_moves_the_registry_revision():
    baseline = build_test_registry(PRODUCTION_REGISTRATIONS).registry_revision
    mutated = list(PRODUCTION_REGISTRATIONS) + [
        _reg(recipe_id="extra.recipe", recipe_version="1.0.0")
    ]
    assert build_test_registry(tuple(mutated)).registry_revision != baseline


def test_the_registry_module_exposes_no_runtime_registration_api():
    """"There is no runtime registrar" is asserted, not merely documented."""
    for name in ("register", "add", "install", "unregister", "clear"):
        assert not hasattr(registry_module, name), name
        assert not hasattr(registry_module.RecipeRegistry, name), name
    from boomi_mcp import recipes

    for name in ("register", "register_recipe", "add_recipe"):
        assert not hasattr(recipes, name), name


def test_the_production_registrations_are_an_immutable_tuple():
    assert isinstance(PRODUCTION_REGISTRATIONS, tuple)
    with pytest.raises((AttributeError, TypeError)):
        PRODUCTION_REGISTRATIONS.append(_reg())  # type: ignore[attr-defined]


def test_the_builtins_package_does_no_package_scanning():
    """A scan would make the registry a property of the filesystem."""
    import boomi_mcp.recipes.builtins as builtins_pkg

    source = Path(builtins_pkg.__file__).read_text()
    for token in ("pkgutil", "importlib", "walk_packages", "iter_modules"):
        assert token not in source, token


# ---------------------------------------------------------------------------
# Provenance
# ---------------------------------------------------------------------------


def test_provenance_is_derived_from_the_registered_callable():
    registry = build_test_registry((_reg(),))
    provenance = registry.resolve("test.recipe").provenance
    assert provenance.package_name == "boomi_mcp"
    assert provenance.module == emit_api_to_api_sync.__module__
    assert provenance.symbol == emit_api_to_api_sync.__qualname__
    assert len(provenance.implementation_sha256) == 64
    assert len(provenance.descriptor_sha256) == 64


def test_a_different_executor_yields_a_different_implementation_hash():
    from boomi_mcp.recipes.builtins.fanout import (
        ComposeDbRestFanoutInputV1,
        emit_db_rest_fanout,
    )

    a = build_test_registry((_reg(),)).resolve("test.recipe")
    b = build_test_registry(
        (_reg(executor=emit_db_rest_fanout, input_model=ComposeDbRestFanoutInputV1),)
    ).resolve("test.recipe")
    assert a.provenance.implementation_sha256 != b.provenance.implementation_sha256


def test_the_same_registration_yields_the_same_implementation_hash():
    a = build_test_registry((_reg(),)).resolve("test.recipe")
    b = build_test_registry((_reg(),)).resolve("test.recipe")
    assert a.provenance.implementation_sha256 == b.provenance.implementation_sha256
    assert a.provenance.descriptor_sha256 == b.provenance.descriptor_sha256


def test_the_descriptor_hash_excludes_itself():
    """A self-referential hash is unverifiable — a reader could not reproduce it."""
    descriptor = build_test_registry((_reg(),)).resolve("test.recipe")
    assert descriptor.provenance.descriptor_sha256 not in (
        descriptor.provenance.implementation_sha256,
    )
    # Reproducible from the published body: same registration, same hash.
    assert (
        build_test_registry((_reg(),)).resolve("test.recipe").provenance.descriptor_sha256
        == descriptor.provenance.descriptor_sha256
    )


def test_source_revision_prefers_the_image_file(tmp_path):
    revision = "a" * 40
    path = tmp_path / "BUILD_REVISION"
    path.write_text(revision)
    assert image_build_revision(str(path)) == revision
    assert source_revision(["boomi_mcp.build_info"], path=str(path)) == revision


def test_a_malformed_image_revision_is_no_evidence_at_all(tmp_path):
    """Half-read is not weaker evidence; reporting it would be confidently wrong."""
    for bad in ("", "not-hex", "ZZZ", "a" * 41, "abc"):
        path = tmp_path / "BUILD_REVISION"
        path.write_text(bad)
        assert image_build_revision(str(path)) is None


def test_the_local_fallback_is_a_labelled_source_digest():
    revision = source_revision(["boomi_mcp.build_info"], path="/nonexistent/path")
    assert is_source_digest(revision)
    assert revision.startswith("source-sha256:")


def test_the_source_digest_is_order_independent_and_content_sensitive():
    a = source_digest(["boomi_mcp.build_info", "boomi_mcp.errors"])
    b = source_digest(["boomi_mcp.errors", "boomi_mcp.build_info"])
    assert a == b
    assert a != source_digest(["boomi_mcp.errors"])


def test_the_source_digest_fails_closed_on_an_unreadable_module():
    with pytest.raises(Exception):
        source_digest(["boomi_mcp.definitely_not_a_module"])


# ---------------------------------------------------------------------------
# Capability preflight
# ---------------------------------------------------------------------------


def test_an_absent_capability_subject_fails_at_construction():
    """A typo must not sit dormant and later blame the caller's platform."""
    with pytest.raises(ValueError, match="unknown"):
        build_test_registry(
            (
                _reg(
                    capability_requirements=(
                        RecipeCapabilityRequirementV1(
                            authority="process_emitter",
                            subject="no.such.emitter",
                            required_state="supported",
                        ),
                    )
                ),
            )
        )


def test_a_gated_process_ir_capability_is_refused_at_preflight():
    registry = build_test_registry(
        (
            _reg(
                capability_requirements=(
                    RecipeCapabilityRequirementV1(
                        authority="process_ir", subject="joins", required_state="supported"
                    ),
                )
            ),
        )
    )
    with pytest.raises(RecipeError) as exc:
        registry.preflight_capabilities(registry.resolve("test.recipe"))
    assert exc.value.diagnostics[0].code == RECIPE_CAPABILITY_GATED


def test_a_supported_process_ir_capability_passes_preflight():
    registry = build_test_registry(
        (
            _reg(
                capability_requirements=(
                    RecipeCapabilityRequirementV1(
                        authority="process_ir",
                        subject="rich_branch_decision_bodies",
                        required_state="supported",
                    ),
                )
            ),
        )
    )
    registry.preflight_capabilities(registry.resolve("test.recipe"))


def test_plannable_only_is_satisfied_by_the_stronger_emittable_state():
    registry = build_test_registry(
        (
            _reg(
                capability_requirements=(
                    RecipeCapabilityRequirementV1(
                        authority="system_topology",
                        subject="process",
                        required_state="plannable-only",
                    ),
                )
            ),
        )
    )
    registry.preflight_capabilities(registry.resolve("test.recipe"))


def test_a_gated_topology_subject_never_satisfies_a_requirement():
    registry = build_test_registry(
        (
            _reg(
                capability_requirements=(
                    RecipeCapabilityRequirementV1(
                        authority="system_topology",
                        subject="queue_reference",
                        required_state="plannable-only",
                    ),
                )
            ),
        )
    )
    with pytest.raises(RecipeError):
        registry.preflight_capabilities(registry.resolve("test.recipe"))


# ---------------------------------------------------------------------------
# Skew
# ---------------------------------------------------------------------------


def _expected_from(registry, **overrides):
    entries = tuple(
        ExpectedRecipeEntryV1(
            recipe_id=d.recipe_id,
            recipe_version=d.recipe_version,
            implementation_sha256=d.provenance.implementation_sha256,
        )
        for d in registry.descriptors()
    )
    payload = dict(
        registry_revision=registry.registry_revision,
        source_revision=registry.source_revision_value,
        entries=entries,
    )
    payload.update(overrides)
    return ExpectedRecipeRegistryV1(**payload)


def test_no_expectation_is_reported_as_not_requested():
    assert production_registry().compare(None).status == "not_requested"


def test_an_exact_expectation_matches():
    registry = production_registry()
    assert registry.compare(_expected_from(registry)).status == "match"


def test_a_missing_recipe_is_a_mismatch():
    registry = production_registry()
    expected = _expected_from(
        registry,
        entries=(
            ExpectedRecipeEntryV1(recipe_id="not.registered", recipe_version="1.0.0"),
        ),
    )
    skew = registry.compare(expected)
    assert skew.status == "mismatch"
    assert skew.missing_from_live == ("not.registered",)


def test_equal_versions_with_different_code_are_a_mismatch_not_a_match():
    """The skew this issue actually exists for.

    Two registries can agree on ``api_to_api_sync@0.1.0`` and run different
    bytes. A version-only comparison would call that a match.
    """
    registry = production_registry()
    entries = []
    for d in registry.descriptors():
        sha = d.provenance.implementation_sha256
        if d.recipe_id == RECIPE_API_TO_API_SYNC:
            sha = "0" * 64
        entries.append(
            ExpectedRecipeEntryV1(
                recipe_id=d.recipe_id,
                recipe_version=d.recipe_version,
                implementation_sha256=sha,
            )
        )
    skew = registry.compare(_expected_from(registry, entries=tuple(entries)))
    assert skew.status == "mismatch"
    assert [m.recipe_id for m in skew.implementation_mismatches] == [
        RECIPE_API_TO_API_SYNC
    ]
    assert skew.version_mismatches == ()


def test_a_version_mismatch_is_reported_separately():
    registry = production_registry()
    expected = _expected_from(
        registry,
        entries=(
            ExpectedRecipeEntryV1(
                recipe_id=RECIPE_DB_REST_FANOUT, recipe_version="9.9.9"
            ),
        ),
    )
    skew = registry.compare(expected)
    assert skew.status == "mismatch"
    assert [m.recipe_id for m in skew.version_mismatches] == [RECIPE_DB_REST_FANOUT]


def test_a_registry_revision_difference_is_reported():
    registry = production_registry()
    skew = registry.compare(_expected_from(registry, registry_revision="f" * 64))
    assert skew.status == "mismatch"
    assert skew.registry_revision_mismatch is True


def test_a_source_revision_difference_is_reported():
    registry = production_registry()
    skew = registry.compare(_expected_from(registry, source_revision="deadbeef"))
    assert skew.status == "mismatch"
    assert skew.source_revision_mismatch is True


def test_every_skew_collection_is_sorted():
    registry = production_registry()
    expected = _expected_from(
        registry,
        entries=(
            ExpectedRecipeEntryV1(recipe_id="z.missing", recipe_version="1.0.0"),
            ExpectedRecipeEntryV1(recipe_id="a.missing", recipe_version="1.0.0"),
        ),
    )
    skew = registry.compare(expected)
    assert list(skew.missing_from_live) == sorted(skew.missing_from_live)
    assert list(skew.live_only) == sorted(skew.live_only)


# ---------------------------------------------------------------------------
# Snapshot
# ---------------------------------------------------------------------------


def test_the_snapshot_is_fully_sorted_and_carries_the_revisions():
    snapshot = production_registry().snapshot()
    ids = [entry["recipe_id"] for entry in snapshot["entries"]]
    assert ids == sorted(ids)
    assert len(snapshot["registry_revision"]) == 64
    assert snapshot["source_revision"]
    assert list(snapshot["capability_revisions"]) == sorted(
        snapshot["capability_revisions"]
    )


def test_the_snapshot_names_every_entry_kind_the_registry_distinguishes():
    kinds = {entry["entry_kind"] for entry in production_registry().snapshot()["entries"]}
    assert kinds == {
        "executable_recipe",
        "constraint_only",
        "advisory",
        "compatibility_adapter",
    }


def test_the_public_descriptor_payload_redacts_capability_subjects():
    """Capability subjects name DARK compiler-authority keys.

    ``tests/test_process_ir_compiler_surface.py`` forbids those on any
    LLM-visible surface, so the public view reports how many were checked
    without naming them. They are still enforced — see the preflight tests above.
    """
    descriptor = production_registry().resolve(RECIPE_DB_REST_FANOUT)
    assert descriptor.capability_requirements  # really has some
    payload = descriptor.public_payload()
    assert payload["capability_requirements"]["count"] == len(
        descriptor.capability_requirements
    )
    import json as _json

    blob = _json.dumps(payload)
    for dark in ("connectoraction_source", "connectoraction_target", "doccacheload"):
        assert dark not in blob, dark


# ---------------------------------------------------------------------------
# Doctrine is not executable
# ---------------------------------------------------------------------------


def test_doctrine_prose_never_resolves_to_a_recipe(monkeypatch):
    """Prose that LOOKS like a recipe id executes nothing.

    Spies on every registered executor, so "no execution" is observed rather
    than inferred from an absent exception.
    """
    calls = []
    for name in ("emit_api_to_api_sync", "emit_api_to_database_sync"):
        original = getattr(catalog, name, None)
        if original is None:
            continue

        def spy(inp, _name=name):  # pragma: no cover - must never run
            calls.append(_name)
            return ()

        monkeypatch.setattr(catalog, name, spy, raising=False)

    registry = production_registry()
    for prose in (
        "Use the api_to_api_sync archetype for REST-to-REST replication.",
        "recipe: db_rest_fanout",
        "design_doctrine:connector_retry_design",
        "api_to_api_sync",  # the ARCHETYPE name — not a recipe id
        "Consider boomi.archetype.api_to_api_sync when the source is REST.",
    ):
        with pytest.raises(RecipeError):
            registry.resolve(prose)
    assert calls == []


def test_naming_a_real_recipe_id_resolves_but_still_executes_nothing(monkeypatch):
    """Resolution is LOOKUP, not execution — the two are separate steps.

    A caller may name a registered recipe; that returns a descriptor and runs no
    code. Execution needs the engine, a validated input, and a materialization
    catalog. Pinned so "resolve" can never quietly become "run".
    """
    calls = []
    monkeypatch.setattr(
        catalog,
        "emit_api_to_api_sync",
        lambda inp: calls.append("ran") or (),  # pragma: no cover
        raising=False,
    )
    descriptor = production_registry().resolve(RECIPE_API_TO_API_SYNC)
    assert descriptor.entry_kind == "executable_recipe"
    assert calls == []


def test_the_advisory_entry_has_no_executor_to_call():
    registry = production_registry()
    descriptor = registry.resolve(RECIPE_ADVISORY_INTEGRATION_DESIGN)
    assert descriptor.entry_kind == "advisory"
    assert descriptor.output_types == ()
    assert descriptor.input_schema_id is None
    with pytest.raises(RecipeError):
        registry.executor_for(descriptor)
    with pytest.raises(RecipeError):
        registry.input_model_for(descriptor)


def test_the_constraint_only_entry_emits_only_requirements():
    descriptor = production_registry().resolve(RECIPE_CONSTRAINT_INBOUND_VALIDATE)
    assert descriptor.entry_kind == "constraint_only"
    assert descriptor.output_types == ("constraint_requirement",)


# ---------------------------------------------------------------------------
# Live-QA regression (issue #145): the digest must track its CALLEES
# ---------------------------------------------------------------------------


def test_the_implementation_digest_covers_the_defining_module_not_only_the_symbol():
    """A behaviour-changing edit to a SHARED HELPER must move the hash.

    Live QA found this: both sync recipes delegate their whole body to
    ``_contributions``, so hashing only the registered function left every hash
    unmoved while ``build_from_archetype`` went from a working spec to a hard
    failure. A caller comparing implementation hashes would have been told
    ``match`` about a registry whose output had changed.

    Simulated by perturbing what ``inspect.getsource`` returns for the MODULE
    while the registered function's own source is untouched — which is exactly
    the shape of the real defect.
    """
    import inspect as inspect_module

    import boomi_mcp.recipes.registry as registry_mod

    baseline = build_test_registry((_reg(),)).resolve("test.recipe")

    real_getsource = inspect_module.getsource
    module_of_executor = inspect_module.getmodule(emit_api_to_api_sync)

    def perturbed(obj):
        text = real_getsource(obj)
        if obj is module_of_executor:
            return text + "\n# a shared helper changed\n"
        return text

    original = registry_mod.inspect.getsource
    registry_mod.inspect.getsource = perturbed
    try:
        mutated = build_test_registry((_reg(),)).resolve("test.recipe")
    finally:
        registry_mod.inspect.getsource = original

    assert (
        mutated.provenance.implementation_sha256
        != baseline.provenance.implementation_sha256
    )


def test_two_recipes_sharing_a_module_stay_distinguishable():
    """Widening the digest must not collapse per-symbol attribution."""
    from boomi_mcp.recipes.builtins.sync import emit_api_to_database_sync

    a = build_test_registry(
        (_reg(recipe_id="r.a", executor=emit_api_to_api_sync),)
    ).resolve("r.a")
    b = build_test_registry(
        (_reg(recipe_id="r.b", executor=emit_api_to_database_sync),)
    ).resolve("r.b")
    assert a.provenance.implementation_sha256 != b.provenance.implementation_sha256


def test_a_recipe_in_another_module_is_unaffected_by_a_shared_helper_change():
    """The digest is per-MODULE, not per-package.

    Hashing the whole import closure would move every recipe's hash on any change
    anywhere — a hash of the package, not of a recipe. The fan-out recipe lives
    in its own module and must not move when the sync module does.
    """
    from boomi_mcp.recipes.builtins.fanout import (
        ComposeDbRestFanoutInputV1,
        emit_db_rest_fanout,
    )
    import inspect as inspect_module

    import boomi_mcp.recipes.registry as registry_mod

    def fanout_reg():
        return _reg(
            recipe_id="r.fanout",
            executor=emit_db_rest_fanout,
            input_model=ComposeDbRestFanoutInputV1,
        )

    baseline = build_test_registry((fanout_reg(),)).resolve("r.fanout")

    real_getsource = inspect_module.getsource
    sync_module = inspect_module.getmodule(emit_api_to_api_sync)

    def perturbed(obj):
        text = real_getsource(obj)
        if obj is sync_module:
            return text + "\n# unrelated module changed\n"
        return text

    original = registry_mod.inspect.getsource
    registry_mod.inspect.getsource = perturbed
    try:
        after = build_test_registry((fanout_reg(),)).resolve("r.fanout")
    finally:
        registry_mod.inspect.getsource = original

    assert (
        after.provenance.implementation_sha256
        == baseline.provenance.implementation_sha256
    )


# ---------------------------------------------------------------------------
# Live-QA regression (issue #145): a partial comparison is reported as such
# ---------------------------------------------------------------------------


def _entries(registry, *, with_hashes):
    return tuple(
        ExpectedRecipeEntryV1(
            recipe_id=d.recipe_id,
            recipe_version=d.recipe_version,
            implementation_sha256=(
                d.provenance.implementation_sha256 if with_hashes else None
            ),
        )
        for d in registry.descriptors()
    )


def test_a_version_only_expectation_is_unknown_not_match():
    """Equal versions are not evidence of equal code — so do not say ``match``.

    A full entries list with no implementation hashes and no revisions is a
    VERSION comparison. Reporting it as ``match`` is the silent "looks fine"
    that ``RecipeRegistrySkewV1`` exists to forbid.
    """
    registry = production_registry()
    skew = registry.compare(
        ExpectedRecipeRegistryV1(entries=_entries(registry, with_hashes=False))
    )
    assert skew.status == "unknown"
    assert "partial_comparison" in skew.reason
    assert "not evidence of equal code" in skew.reason


def test_an_entries_expectation_with_full_hashes_is_a_real_match():
    registry = production_registry()
    skew = registry.compare(
        ExpectedRecipeRegistryV1(entries=_entries(registry, with_hashes=True))
    )
    assert skew.status == "match"
    assert skew.reason is None


def test_a_revision_alone_is_enough_for_a_real_match():
    registry = production_registry()
    skew = registry.compare(
        ExpectedRecipeRegistryV1(
            registry_revision=registry.registry_revision,
            entries=_entries(registry, with_hashes=False),
        )
    )
    assert skew.status == "match"


def test_a_source_revision_alone_is_enough_for_a_real_match():
    registry = production_registry()
    skew = registry.compare(
        ExpectedRecipeRegistryV1(
            source_revision=registry.source_revision_value,
            entries=_entries(registry, with_hashes=False),
        )
    )
    assert skew.status == "match"


def test_an_empty_entries_list_is_a_mismatch_not_unknown():
    """An empty entries list ASSERTS "this registry has no recipes".

    That is a claim, not an absent one — so the live registry's eight entries are
    a real difference, reported with ``live_only``. ``unknown`` is for a
    comparison that could not establish parity, not for one whose answer is no.
    """
    skew = production_registry().compare(ExpectedRecipeRegistryV1())
    assert skew.status == "mismatch"
    assert len(skew.live_only) == 8
    assert skew.missing_from_live == ()


def test_a_partial_comparison_that_finds_a_difference_is_still_a_mismatch():
    """A finding is a finding, however partial the comparison was."""
    registry = production_registry()
    entries = list(_entries(registry, with_hashes=False))
    entries[0] = ExpectedRecipeEntryV1(
        recipe_id=entries[0].recipe_id, recipe_version="9.9.9"
    )
    skew = registry.compare(ExpectedRecipeRegistryV1(entries=tuple(entries)))
    assert skew.status == "mismatch"
    assert skew.version_mismatches


# ---------------------------------------------------------------------------
# Live-QA regression (issue #145): the two hashes have DIFFERENT scopes
# ---------------------------------------------------------------------------
#
# ``implementation_sha256`` answers "WHICH recipe changed" and covers the entry's
# own defining module. ``source_revision`` answers "did anything in the layer
# change" and covers the whole execution path. Round 2 of live QA found the
# second claim documented before it was true: the digest listed only the executor
# modules, so an edit to ``engine.py`` or ``recipe_bridge.py`` moved nothing while
# both migrated presets went from a working spec to a hard failure.


def test_the_layer_module_list_covers_the_whole_recipe_package():
    """Pinned in BOTH directions against the package's real contents.

    A static list keeps the digest a property of the code rather than of the
    filesystem — but a static list that silently stops covering a new module is
    worse than a scan, so this is the guard that makes it safe.
    """
    from boomi_mcp.recipes.registry import RECIPE_LAYER_MODULES

    import boomi_mcp.recipes as recipes_pkg

    package_dir = Path(recipes_pkg.__file__).parent
    on_disk = set()
    for path in package_dir.rglob("*.py"):
        if path.name == "__init__.py":
            continue  # re-export shims carry no logic
        rel = path.relative_to(package_dir).with_suffix("")
        on_disk.add("boomi_mcp.recipes." + ".".join(rel.parts))

    listed = {m for m in RECIPE_LAYER_MODULES if m.startswith("boomi_mcp.recipes.")}
    assert listed == on_disk, listed ^ on_disk

    # Everything OUTSIDE the package must be claimed by clause 2 (a contract
    # module) or clause 3 (an engine invoker). Derived, not a literal set: a
    # hard-coded list would pass even if the RULE covered none of them, which is
    # exactly the gap live QA found (issue #145).
    outside = set(RECIPE_LAYER_MODULES) - listed
    contract_modules = {"boomi_mcp.build_info", "boomi_mcp.models.recipe_contributions"}
    unclaimed = outside - contract_modules - _engine_invoking_modules()
    assert unclaimed == set(), unclaimed
    # ...and neither clause is carrying a module the other already claims.
    assert contract_modules & _engine_invoking_modules() == set()


def test_the_contract_clause_covers_exactly_the_two_modules_it_names():
    """Clause 2 is a NAMED pair, not an open category.

    If it were open, "a contract module" would become the escape hatch the other
    two clauses exist to avoid. These two are outside ``recipes/`` only because
    ``models/`` may not import ``categories/`` and ``build_info`` must stay
    stdlib-only.
    """
    from boomi_mcp.recipes.registry import RECIPE_LAYER_MODULES

    for module in ("boomi_mcp.build_info", "boomi_mcp.models.recipe_contributions"):
        assert module in RECIPE_LAYER_MODULES, module
    # Neither is in the recipes package, and neither invokes the engine — which
    # is precisely why clause 2 has to exist.
    for module in ("boomi_mcp.build_info", "boomi_mcp.models.recipe_contributions"):
        assert not module.startswith("boomi_mcp.recipes."), module
        assert module not in _engine_invoking_modules(), module


def test_the_layer_module_list_is_sorted_and_unique():
    from boomi_mcp.recipes.registry import RECIPE_LAYER_MODULES

    assert list(RECIPE_LAYER_MODULES) == sorted(set(RECIPE_LAYER_MODULES))


@pytest.mark.parametrize(
    "module_name",
    [
        "boomi_mcp.recipes.engine",
        "boomi_mcp.recipes.composer",
        "boomi_mcp.recipes.materialization",
        "boomi_mcp.patterns.recipe_bridge",
        "boomi_mcp.models.recipe_contributions",
    ],
)
def test_source_revision_moves_for_any_module_in_the_execution_path(module_name):
    """The layer-wide backstop, tested where it previously did not reach.

    Each of these modules can change what a recipe run produces, and none of them
    is any single entry's defining module — so ``source_revision`` is the only
    thing that can notice.
    """
    import importlib
    import inspect as inspect_module

    import boomi_mcp.build_info as build_info_mod

    baseline = build_test_registry(PRODUCTION_REGISTRATIONS).source_revision_value

    target = importlib.import_module(module_name)
    real_getsource = inspect_module.getsource

    def perturbed(obj):
        text = real_getsource(obj)
        if obj is target:
            return text + "\n# changed\n"
        return text

    original = build_info_mod.inspect.getsource
    build_info_mod.inspect.getsource = perturbed
    try:
        mutated = build_test_registry(PRODUCTION_REGISTRATIONS).source_revision_value
    finally:
        build_info_mod.inspect.getsource = original

    assert mutated != baseline, module_name


def test_an_executor_less_entry_hashes_its_declaration_source():
    """An adapter/advisory entry IS a declaration, so hash where it is written.

    Live QA found these hashing only ``(id, version, entry_kind)``, which never
    moved for ANY edit — including one to the catalog module they themselves
    declare. Four of eight published entries pinned nothing, in a field the skew
    note tells callers to rely on.
    """
    import importlib
    import inspect as inspect_module

    import boomi_mcp.recipes.registry as registry_mod

    def executor_less_hashes(registry):
        return {
            d.recipe_id: d.provenance.implementation_sha256
            for d in registry.descriptors()
            if d.entry_kind in ("compatibility_adapter", "advisory")
        }

    baseline = executor_less_hashes(build_test_registry(PRODUCTION_REGISTRATIONS))
    assert len(baseline) == 4

    catalog_module = importlib.import_module("boomi_mcp.recipes.builtins.catalog")
    real_getsource = inspect_module.getsource

    def perturbed(obj):
        text = real_getsource(obj)
        if obj is catalog_module:
            return text + "\n# declaration changed\n"
        return text

    original = registry_mod.inspect.getsource
    registry_mod.inspect.getsource = perturbed
    try:
        mutated = executor_less_hashes(build_test_registry(PRODUCTION_REGISTRATIONS))
    finally:
        registry_mod.inspect.getsource = original

    for recipe_id, digest in baseline.items():
        assert mutated[recipe_id] != digest, recipe_id


def test_an_adapters_hash_depends_on_the_target_it_names():
    """Repointing an adapter at a different recipe must move its hash."""
    from boomi_mcp.recipes.contracts import RecipeReferenceV1

    def adapter(target_version):
        return RecipeRegistrationV1(
            recipe_id="test.adapter",
            recipe_version="1.0.0",
            entry_kind="compatibility_adapter",
            is_default=True,
            adapter_target=RecipeReferenceV1(
                recipe_id="test.target", recipe_version=target_version
            ),
        )

    # The targets must be REGISTERED — construction now rejects an adapter that
    # names one that is not, so the registry has to carry both versions.
    def target(version, default):
        return _reg(recipe_id="test.target", recipe_version=version, is_default=default)

    targets = (target("0.1.0", True), target("0.2.0", False))
    a = build_test_registry((adapter("0.1.0"), *targets)).resolve("test.adapter")
    b = build_test_registry((adapter("0.2.0"), *targets)).resolve("test.adapter")
    assert (
        a.provenance.implementation_sha256 != b.provenance.implementation_sha256
    )


def test_every_published_entry_pins_some_code():
    """No published entry may carry a hash that pins nothing.

    Asserted structurally: perturbing the catalog module (which every
    executor-less entry declares) or an executor module must move every entry's
    hash between them — so no entry is left with a constant.
    """
    import importlib
    import inspect as inspect_module

    import boomi_mcp.recipes.registry as registry_mod

    def all_hashes(registry):
        return {
            d.recipe_id: d.provenance.implementation_sha256
            for d in registry.descriptors()
        }

    baseline = all_hashes(build_test_registry(PRODUCTION_REGISTRATIONS))
    moved = set()

    for module_name in (
        "boomi_mcp.recipes.builtins.catalog",
        "boomi_mcp.recipes.builtins.sync",
        "boomi_mcp.recipes.builtins.fanout",
    ):
        target = importlib.import_module(module_name)
        real_getsource = inspect_module.getsource

        def perturbed(obj, _target=target, _real=real_getsource):
            text = _real(obj)
            if obj is _target:
                return text + "\n# changed\n"
            return text

        original = registry_mod.inspect.getsource
        registry_mod.inspect.getsource = perturbed
        try:
            mutated = all_hashes(build_test_registry(PRODUCTION_REGISTRATIONS))
        finally:
            registry_mod.inspect.getsource = original

        moved |= {rid for rid, h in mutated.items() if h != baseline[rid]}

    assert moved == set(baseline), set(baseline) - moved


#: The three entry points that INVOKE the recipe engine. A module calling any of
#: them is in the execution path by definition.
_ENGINE_ENTRY_POINTS = frozenset(
    {"run_recipes", "run_sync_preset_recipe", "run_fanout_recipe"}
)


def _engine_invoking_modules():
    """Every module that calls a recipe-engine entry point DIRECTLY.

    Parsed, not grepped. An earlier version of this pin searched for the string
    ``recipe_bridge``, which pinned reach through exactly one door: a module
    importing the engine directly was invisible to it, and QA demonstrated the
    blind spot.

    DIRECT calls, one level — not the transitive closure. That is the rule
    ``RECIPE_LAYER_MODULES`` encodes, and calling it a "call graph" scan (as an
    earlier docstring did) overstated it: transitively, the reporting layer
    reaches the engine through ``composition.py``, and the digest deliberately
    excludes it. One level is the boundary between "this module runs recipes" and
    "this module called something that does".

    ``__init__.py`` is INCLUDED here, unlike in the package-contents pin below.
    There a re-export shim genuinely carries no logic; here it could carry a call,
    and skipping it left a hole QA found.
    """
    import ast

    import boomi_mcp

    package_dir = Path(boomi_mcp.__file__).parent
    invoking = set()
    for path in sorted(package_dir.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:  # pragma: no cover - defensive
            continue
        called = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Name):
                    called.add(func.id)
                elif isinstance(func, ast.Attribute):
                    called.add(func.attr)
        if called & _ENGINE_ENTRY_POINTS:
            rel = path.relative_to(package_dir).with_suffix("")
            parts = [p for p in rel.parts if p != "__init__"]
            invoking.add(".".join(["boomi_mcp", *parts]))
    return invoking


def test_every_module_that_invokes_the_engine_is_in_the_layer_digest():
    """The membership RULE, decided by an AST scan rather than by judgement.

    Four consecutive rounds of live QA falsified a BROADER sentence than the list
    backed, each time by finding a module the words covered and the digest did
    not. The problem was never the missing module — it was writing a claim no
    test could check. This pin and the rule in ``registry.py`` are now the same
    statement, so a fifth migrated surface fails here until it is listed.
    """
    from boomi_mcp.recipes.registry import RECIPE_LAYER_MODULES

    invoking = _engine_invoking_modules()
    assert invoking, "the call scan found nothing — the pin would be vacuous"
    missing = invoking - set(RECIPE_LAYER_MODULES)
    assert missing == set(), missing


def test_the_engine_invocation_scan_finds_the_surfaces_we_expect():
    """Guard the guard: an AST scan that matched nothing would pass silently."""
    invoking = _engine_invoking_modules()
    assert {
        "boomi_mcp.patterns.recipe_bridge",
        "boomi_mcp.patterns.archetypes.api_to_api_sync",
        "boomi_mcp.patterns.archetypes.api_to_database_sync",
        "boomi_mcp.patterns.composition",
    } <= invoking


def test_the_reporting_layer_is_outside_the_digest_by_the_same_rule():
    """The reporting modules never invoke the engine DIRECTLY.

    They do reach it transitively — ``compose_archetypes_action`` calls
    ``compose_archetypes``, which calls ``run_fanout_recipe`` — so the claim is
    about direct invocation, which is the line the digest draws. An earlier
    version of this docstring said they "never invoke the engine" full stop; QA
    walked the live stack and falsified it.

    Stated as a decision, not an oversight: an edit to a response builder can
    change published bytes without changing any recipe's output, and folding it
    in would move every recipe's revision on any response-shape change.
    """
    from boomi_mcp.recipes.registry import RECIPE_LAYER_MODULES

    invoking = _engine_invoking_modules()
    for reporting in (
        "boomi_mcp.categories.integration_authoring",
        "boomi_mcp.categories.integration_import",
        "boomi_mcp.categories.meta_tools",
    ):
        assert reporting not in invoking, reporting
        assert reporting not in RECIPE_LAYER_MODULES, reporting


def test_the_migrated_surfaces_are_in_the_layer_digest():
    """Named explicitly too, so the derived pin above cannot go vacuous.

    If ``recipe_bridge`` were ever renamed, the caller scan would find nothing
    and silently pass. This states the three modules the digest must carry today.
    """
    from boomi_mcp.recipes.registry import RECIPE_LAYER_MODULES

    assert {
        "boomi_mcp.patterns.archetypes.api_to_api_sync",
        "boomi_mcp.patterns.archetypes.api_to_database_sync",
        "boomi_mcp.patterns.composition",
    } <= set(RECIPE_LAYER_MODULES)


@pytest.mark.parametrize(
    "module_name",
    [
        "boomi_mcp.patterns.archetypes.api_to_api_sync",
        "boomi_mcp.patterns.archetypes.api_to_database_sync",
        "boomi_mcp.patterns.composition",
    ],
)
def test_source_revision_moves_for_a_migrated_surface(module_name):
    """A migrated archetype can silently change the emitted result.

    QA proved it: renaming an emitted component leaves the build succeeding and
    every published field byte-identical. ``source_revision`` is the only thing
    positioned to notice, so it must.
    """
    import importlib
    import inspect as inspect_module

    import boomi_mcp.build_info as build_info_mod

    baseline = build_test_registry(PRODUCTION_REGISTRATIONS).source_revision_value

    target = importlib.import_module(module_name)
    real_getsource = inspect_module.getsource

    def perturbed(obj):
        text = real_getsource(obj)
        if obj is target:
            return text + "\n# changed\n"
        return text

    original = build_info_mod.inspect.getsource
    build_info_mod.inspect.getsource = perturbed
    try:
        mutated = build_test_registry(PRODUCTION_REGISTRATIONS).source_revision_value
    finally:
        build_info_mod.inspect.getsource = original

    assert mutated != baseline, module_name


def test_the_downstream_compiler_modules_are_deliberately_outside_the_digest():
    """The stated BOUND, pinned so it stays a decision rather than a drift.

    A recipe run executes the ProcessIR compiler, the component builders and the
    graph verifier. Editing one of those can change the emitted XML with nothing
    published moving — round 4 of live QA measured it. They are excluded because
    including them would make ``source_revision`` a hash of the package, which is
    the same objection that keeps the per-entry digest scoped to one module.

    This test exists so that boundary is asserted, not merely described: if
    someone later widens the list to "fix" it, they have to change this test and
    confront the trade-off rather than sliding into a package hash.
    """
    from boomi_mcp.recipes.registry import RECIPE_LAYER_MODULES

    listed = set(RECIPE_LAYER_MODULES)
    for downstream in (
        "boomi_mcp.compiler.process_ir.pipeline",
        "boomi_mcp.compiler.process_ir.emitter_registry",
        "boomi_mcp.compiler.process_ir.legacy_adapters.sync_pipeline",
        "boomi_mcp.compiler.process_ir.legacy_adapters.flow_sequence",
        "boomi_mcp.models.process_ir",
        "boomi_mcp.categories.components.process_graph_verifier",
        "boomi_mcp.categories.integration_builder",
    ):
        assert downstream not in listed, downstream


def test_no_listed_module_lies_outside_the_layer_this_issue_owns():
    """Every listed module is one #145 introduced or migrated.

    The converse of the bound above: the digest must not quietly grow into
    someone else's authority either.
    """
    from boomi_mcp.recipes.registry import RECIPE_LAYER_MODULES

    # Named EXACTLY outside the recipes package. A ``patterns.archetypes.``
    # prefix would have admitted the four UNMIGRATED archetypes, which the
    # docstring above says are not in the layer — QA caught that the allowance
    # contradicted its own claim.
    allowed_outside = {
        "boomi_mcp.build_info",
        "boomi_mcp.models.recipe_contributions",
        "boomi_mcp.patterns.archetypes.api_to_api_sync",
        "boomi_mcp.patterns.archetypes.api_to_database_sync",
        "boomi_mcp.patterns.composition",
        "boomi_mcp.patterns.recipe_bridge",
    }
    for module in RECIPE_LAYER_MODULES:
        assert (
            module.startswith("boomi_mcp.recipes.") or module in allowed_outside
        ), module


def test_an_unregistered_recipe_prerequisite_fails_at_construction():
    """A prerequisite naming something unregistered can never be satisfied.

    The same argument that rejects an unregistered adapter target: a
    registration mistake is not a caller's to diagnose, so it is a build defect
    rather than a ``RECIPE_*`` code at run time (issue #145, live QA).
    """
    from boomi_mcp.recipes.contracts import RecipeDependencyV1

    with pytest.raises(ValueError, match="unregistered prerequisite"):
        build_test_registry(
            (
                _reg(
                    prerequisites=(
                        RecipeDependencyV1(
                            kind="recipe",
                            recipe_id="never.registered",
                            recipe_version="1.0.0",
                        ),
                    )
                ),
            )
        )


def test_a_prerequisite_at_the_wrong_version_fails_at_construction():
    from boomi_mcp.recipes.contracts import RecipeDependencyV1

    with pytest.raises(ValueError, match="unregistered prerequisite"):
        build_test_registry(
            (
                _reg(recipe_id="test.base", recipe_version="1.0.0"),
                _reg(
                    recipe_id="test.recipe",
                    prerequisites=(
                        RecipeDependencyV1(
                            kind="recipe",
                            recipe_id="test.base",
                            recipe_version="2.0.0",
                        ),
                    ),
                ),
            )
        )


@pytest.mark.parametrize(
    "edges,label",
    [
        ({"cyc.a": "cyc.b", "cyc.b": "cyc.a"}, "mutual 2-cycle"),
        ({"cyc.a": "cyc.b", "cyc.b": "cyc.c", "cyc.c": "cyc.a"}, "3-cycle"),
    ],
    ids=["two", "three"],
)
def test_a_multi_recipe_prerequisite_cycle_fails_at_construction(edges, label):
    """The rule the code already stated, now implemented for every N.

    ``composer.order_invocations`` raises a BARE ``ValueError`` outside the
    ``RecipeError`` envelope when it cannot make progress. An earlier check
    handled a self-loop only, and live QA measured a mutual 2-cycle, a 3-cycle
    and a same-id cross-version cycle all registering cleanly and then escaping
    at run time (issue #145).
    """
    from boomi_mcp.recipes.contracts import RecipeDependencyV1

    registrations = tuple(
        _reg(
            recipe_id=source,
            prerequisites=(
                RecipeDependencyV1(
                    kind="recipe", recipe_id=target, recipe_version="1.0.0"
                ),
            ),
        )
        for source, target in edges.items()
    )
    with pytest.raises(ValueError, match="prerequisite cycle"):
        build_test_registry(registrations)


def test_a_same_id_cross_version_cycle_fails_at_construction():
    """Two VERSIONS of one recipe depending on each other is still a cycle."""
    from boomi_mcp.recipes.contracts import RecipeDependencyV1

    with pytest.raises(ValueError, match="prerequisite cycle"):
        build_test_registry(
            (
                _reg(
                    recipe_id="cyc.v",
                    recipe_version="1.0.0",
                    prerequisites=(
                        RecipeDependencyV1(
                            kind="recipe", recipe_id="cyc.v", recipe_version="2.0.0"
                        ),
                    ),
                ),
                _reg(
                    recipe_id="cyc.v",
                    recipe_version="2.0.0",
                    is_default=False,
                    prerequisites=(
                        RecipeDependencyV1(
                            kind="recipe", recipe_id="cyc.v", recipe_version="1.0.0"
                        ),
                    ),
                ),
            )
        )


def _depends_on(recipe_id, *targets, **kwargs):
    from boomi_mcp.recipes.contracts import RecipeDependencyV1

    return _reg(
        recipe_id=recipe_id,
        prerequisites=tuple(
            RecipeDependencyV1(kind="recipe", recipe_id=t, recipe_version="1.0.0")
            for t in targets
        ),
        **kwargs,
    )


def test_an_acyclic_prerequisite_chain_still_registers():
    """The non-vacuous control: a legitimate chain must not be rejected."""
    registry = build_test_registry(
        (
            _depends_on("chain.a", "chain.b"),
            _depends_on("chain.b", "chain.c"),
            _reg(recipe_id="chain.c"),
        )
    )
    assert len(registry.descriptors()) == 3


@pytest.mark.parametrize("seed", [0, 1, 2, 3, 4, 5])
def test_a_diamond_is_not_a_cycle_in_any_registration_order(seed):
    """The control that pins the TRI-COLOUR marking, not just "no cycle".

    A linear chain never re-visits a node, so it cannot tell tri-colour from a
    plain ``visited`` set. A DIAMOND does: ``d`` is reached twice, and without
    marking it BLACK on exit the second visit sees it as GREY and reports a cycle
    that is not there — rejecting every legitimate diamond, and escaping as a
    bare ``ValueError`` from ``stack.index`` at that. Live QA measured exactly
    that mutant surviving the whole suite (issue #145).
    """
    registrations = [
        _depends_on("dia.a", "dia.b", "dia.c"),
        _depends_on("dia.b", "dia.d"),
        _depends_on("dia.c", "dia.d"),
        _reg(recipe_id="dia.d"),
    ]
    rng = random.Random(seed)
    rng.shuffle(registrations)
    registry = build_test_registry(tuple(registrations))
    assert len(registry.descriptors()) == 4


def test_two_disjoint_cycles_are_both_rejected_deterministically():
    """Order must not decide WHICH cycle a caller is shown."""
    registrations = [
        _depends_on("cyc.a", "cyc.b"),
        _depends_on("cyc.b", "cyc.a"),
        _depends_on("cyc.y", "cyc.z"),
        _depends_on("cyc.z", "cyc.y"),
    ]
    messages = set()
    for seed in range(6):
        shuffled = list(registrations)
        random.Random(seed).shuffle(shuffled)
        with pytest.raises(ValueError) as exc:
            build_test_registry(tuple(shuffled))
        messages.add(str(exc.value))
    assert len(messages) == 1, messages


def test_the_cycle_message_names_the_actual_path():
    """The stated rule — "sorted so the cycle shown does not depend on dict
    order" — had no check: every test matched only the substring.

    This test pins ONE half, the deterministic entry point: ``list(graph)`` in
    place of ``sorted(graph)`` produces four different messages across these six
    shuffles. It does NOT pin the path slice — every node here is on the cycle,
    so ``stack.index`` returns 0 and removing the slice leaves this string
    unchanged. ``test_a_cycle_reached_from_an_acyclic_root_reports_only_the_cycle``
    is the one that pins the slice, because its approach node sorts first and is
    therefore ON the stack when the back-edge is found.

    An earlier version of this docstring claimed both halves, and claimed that
    removing the slice "collapses to just the closing edge". Both were false —
    removing it EXPANDS the path — and live QA measured it (issue #145).
    """
    registrations = [
        _depends_on("cyc.a", "cyc.b"),
        _depends_on("cyc.b", "cyc.c"),
        _depends_on("cyc.c", "cyc.a"),
    ]
    messages = set()
    for seed in range(6):
        shuffled = list(registrations)
        random.Random(seed).shuffle(shuffled)
        with pytest.raises(ValueError) as exc:
            build_test_registry(tuple(shuffled))
        messages.add(str(exc.value))
    assert messages == {
        "recipe prerequisite cycle: cyc.a@1.0.0 -> cyc.b@1.0.0 -> cyc.c@1.0.0 -> cyc.a@1.0.0"
    }, messages


def test_a_cycle_reached_from_an_acyclic_root_reports_only_the_cycle():
    """The approach node is not part of the cycle and must not be named.

    The NAMES are load-bearing. With an approach node that sorts after the cycle
    (``app.root`` vs ``app.a``), ``sorted(graph)`` enters the DFS at a cycle
    member, the approach node never lands on the stack, and ``stack.index``
    returns 0 — making the slice a no-op and this assertion unfalsifiable. Live
    QA proved that by asserting ``_i == 0`` inside the product and finding the
    whole suite still green (issue #145).

    ``app.aaa_root`` sorts FIRST, so the DFS reaches the cycle through it, the
    stack is three deep when the back-edge is found, and the slice has real work
    to do.
    """
    with pytest.raises(ValueError) as exc:
        build_test_registry(
            (
                _depends_on("app.aaa_root", "app.m"),
                _depends_on("app.m", "app.n"),
                _depends_on("app.n", "app.m"),
            )
        )
    assert "aaa_root" not in str(exc.value), str(exc.value)
    assert str(exc.value) == (
        "recipe prerequisite cycle: app.m@1.0.0 -> app.n@1.0.0 -> app.m@1.0.0"
    )


def test_the_engine_invocation_scan_does_not_skip_package_inits():
    """``__init__.py`` could carry a call; skipping it left a hole QA found.

    Asserted by construction — the scan's own file walk must not filter them —
    rather than by hoping no ``__init__`` ever gains one.
    """
    import inspect as inspect_module

    source = inspect_module.getsource(_engine_invoking_modules)
    assert 'if path.name == "__init__.py"' not in source
    assert '__init__' in source  # the deliberate name-stripping, not a skip


def test_an_unknown_recipe_registry_capability_subject_fails_at_construction():
    """The seventh authority, which used to slip through.

    ``recipe_registry`` subjects cannot be checked per-registration — a recipe may
    require one registered later in the tuple — so they are swept at the END of
    construction. Before that sweep existed, a typo'd subject was accepted and
    then reported as ``RECIPE_CAPABILITY_GATED`` at preflight: exactly the
    "blame the caller's platform for our typo" misdiagnosis construction-time
    enforcement is supposed to prevent (issue #145, live QA).
    """
    with pytest.raises(ValueError, match="unknown recipe_registry subject"):
        build_test_registry(
            (
                _reg(
                    capability_requirements=(
                        RecipeCapabilityRequirementV1(
                            authority="recipe_registry",
                            subject="no.such.recipe",
                            required_state="supported",
                        ),
                    )
                ),
            )
        )


def test_a_recipe_registry_subject_registered_later_still_resolves():
    """The reason the sweep is deferred rather than per-registration."""
    registry = build_test_registry(
        (
            _reg(
                recipe_id="a.first",
                capability_requirements=(
                    RecipeCapabilityRequirementV1(
                        authority="recipe_registry",
                        subject="z.later",
                        required_state="supported",
                    ),
                ),
            ),
            _reg(recipe_id="z.later"),
        )
    )
    registry.preflight_capabilities(registry.resolve("a.first"))


# ---------------------------------------------------------------------------
# Live-QA regression (issue #145): "at least this" was inverted
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "authority,subject",
    [
        ("component_builder", "process"),
        ("process_emitter", "branch"),
        ("process_body", "branch_leg.step"),
    ],
)
@pytest.mark.parametrize("state", ["supported", "emittable", "plannable-only"])
def test_a_registered_membership_subject_satisfies_every_positive_state(
    authority, subject, state
):
    """A membership authority answers "is it there", so presence is the ceiling.

    The check used to be ``present and "supported" in accepted``, which inverted
    the "at least this" rule on five of the seven authorities: a
    ``plannable-only`` requirement — the WEAKEST ask — could never be satisfied,
    because ``_STATE_SATISFIES["plannable-only"]`` deliberately contains no
    ``supported``. Latent while every built-in asks for ``supported``, which is
    why a live-QA mutant read inert before this was found.
    """
    registry = build_test_registry(
        (
            _reg(
                capability_requirements=(
                    RecipeCapabilityRequirementV1(
                        authority=authority, subject=subject, required_state=state
                    ),
                )
            ),
        )
    )
    registry.preflight_capabilities(registry.resolve("test.recipe"))


@pytest.mark.parametrize(
    "authority,absent_subject",
    [
        ("component_builder", "not.a.component.type"),
        ("process_emitter", "not.an.emitter"),
        ("process_body", "not.a.body.slot"),
        ("recipe_registry", "not.a.registered.recipe"),
    ],
)
def test_an_absent_membership_subject_is_rejected_at_construction(
    authority, absent_subject
):
    """The converse guard, made non-vacuous.

    Its first version asked for ``recipe_registry/test.recipe`` — the id the test
    registry had just REGISTERED — so a mutant that ignored presence entirely
    passed the whole suite. QA caught that (issue #145). These subjects are
    genuinely absent, and an absent subject is a build defect: it fails at
    construction, before any preflight.
    """
    with pytest.raises(ValueError, match="unknown"):
        build_test_registry(
            (
                _reg(
                    capability_requirements=(
                        RecipeCapabilityRequirementV1(
                            authority=authority,
                            subject=absent_subject,
                            required_state="supported",
                        ),
                    )
                ),
            )
        )


def test_a_gated_stateful_subject_still_fails_preflight():
    """Widening the membership branch must not have touched the stateful ones."""
    for subject in ("joins", "loops", "keyed_cache"):
        registry = build_test_registry(
            (
                _reg(
                    capability_requirements=(
                        RecipeCapabilityRequirementV1(
                            authority="process_ir",
                            subject=subject,
                            required_state="supported",
                        ),
                    )
                ),
            )
        )
        with pytest.raises(RecipeError):
            registry.preflight_capabilities(registry.resolve("test.recipe"))


@pytest.mark.parametrize(
    "authority,subject,state",
    [
        ("process_ir", "rich_branch_decision_bodies", "emittable"),
        ("process_ir", "rich_branch_decision_bodies", "plannable-only"),
        ("system_topology", "process", "supported"),
    ],
)
def test_an_unsatisfiable_authority_state_pair_fails_at_construction(
    authority, subject, state
):
    """A required_state the authority can NEVER report is a build defect.

    ``process_ir`` only ever reports supported/gated/unsupported, so requiring
    ``emittable`` of it is impossible by construction. Surfacing that at preflight
    as ``RECIPE_CAPABILITY_GATED`` would blame the caller's platform for our own
    impossible declaration (issue #145, live QA).
    """
    with pytest.raises(ValueError, match="can never report it"):
        build_test_registry(
            (
                _reg(
                    capability_requirements=(
                        RecipeCapabilityRequirementV1(
                            authority=authority, subject=subject, required_state=state
                        ),
                    )
                ),
            )
        )


def test_the_runtime_capability_path_gets_the_same_guards_as_construction():
    """A CONTRIBUTED ``RequireCapability`` is checked like a descriptor one.

    The runtime path used to get neither the unknown-subject nor the
    unsatisfiable-state guard, so a typo'd subject reported "not satisfied"
    rather than "you asked for something that does not exist" — and round 8's
    mutant read inert because of it (issue #145, live QA). Both return ``False``
    here rather than raising: a contribution is caller-adjacent input, so an
    unanswerable requirement is an unmet one.
    """
    registry = build_test_registry((_reg(),))

    # A typo'd subject on a membership authority.
    assert not registry.capability_satisfied(
        RecipeCapabilityRequirementV1(
            authority="process_emitter",
            subject="no.such.emitter",
            required_state="supported",
        )
    )
    # A typo'd subject on a stateful authority.
    assert not registry.capability_satisfied(
        RecipeCapabilityRequirementV1(
            authority="process_ir", subject="no.such.feature", required_state="supported"
        )
    )
    # An unregistered recipe id.
    assert not registry.capability_satisfied(
        RecipeCapabilityRequirementV1(
            authority="recipe_registry",
            subject="not.registered",
            required_state="supported",
        )
    )
    # An (authority, state) pair the authority can never report.
    assert not registry.capability_satisfied(
        RecipeCapabilityRequirementV1(
            authority="process_ir",
            subject="rich_branch_decision_bodies",
            required_state="emittable",
        )
    )
    # ...and the legitimate case still passes, so this is not a blanket False.
    assert registry.capability_satisfied(
        RecipeCapabilityRequirementV1(
            authority="process_ir",
            subject="rich_branch_decision_bodies",
            required_state="supported",
        )
    )


def test_a_capability_diagnostic_does_not_publish_the_dark_subject():
    """``public_payload`` redacts capability subjects; the diagnostic must too.

    Publishing them here would have leaked through the failure path exactly what
    the descriptor projection withholds — the emitter registry's own keys.
    """
    registry = build_test_registry(
        (
            _reg(
                capability_requirements=(
                    RecipeCapabilityRequirementV1(
                        authority="process_ir", subject="joins", required_state="supported"
                    ),
                )
            ),
        )
    )
    with pytest.raises(RecipeError) as exc:
        registry.preflight_capabilities(registry.resolve("test.recipe"))
    diagnostic = exc.value.diagnostics[0]
    assert diagnostic.target == "capability:process_ir"
    assert "joins" not in diagnostic.target


def test_an_async_generator_executor_is_rejected():
    """``iscoroutinefunction`` is False for ``async def f(): yield``.

    ``isfunction`` is True, so it slipped through construction and surfaced at
    run time as ``RECIPE_CONTRIBUTION_INVALID`` — a build defect wearing a
    caller-facing code (issue #145, live QA).
    """

    async def async_gen(inp):  # pragma: no cover - rejected at registration
        yield ()

    with pytest.raises(ValueError, match="coroutine"):
        build_test_registry((_reg(executor=async_gen),))


def test_a_plain_generator_executor_is_rejected():
    def gen(inp):  # pragma: no cover - rejected at registration
        yield ()

    with pytest.raises(ValueError, match="generator"):
        build_test_registry((_reg(executor=gen),))


def test_a_self_dependent_prerequisite_is_rejected_at_construction():
    """It used to build cleanly and then raise a BARE ValueError mid-run.

    The composer's cycle guard fired from inside ``run_recipes``, outside the
    ``RecipeError`` envelope, so only the MCP layer's last-line ``except
    Exception`` caught it (issue #145, live QA). It is a build defect.
    """
    from boomi_mcp.recipes.contracts import RecipeDependencyV1

    with pytest.raises(ValueError, match="prerequisite cycle"):
        build_test_registry(
            (
                _reg(
                    prerequisites=(
                        RecipeDependencyV1(
                            kind="recipe",
                            recipe_id="test.recipe",
                            recipe_version="1.0.0",
                        ),
                    )
                ),
            )
        )


def test_running_a_registered_but_non_executable_entry_says_so():
    """An advisory/adapter id IS registered — "not found" sent callers hunting
    for a typo in a name that was correct (issue #145, live QA)."""
    from boomi_mcp.errors import RECIPE_REQUEST_INVALID

    registry = production_registry()
    for recipe_id in (
        RECIPE_ADVISORY_INTEGRATION_DESIGN,
        "boomi.adapter.compose_archetypes",
    ):
        descriptor = registry.resolve(recipe_id)
        with pytest.raises(RecipeError) as exc:
            registry.executor_for(descriptor)
        diagnostic = exc.value.diagnostics[0]
        assert diagnostic.code == RECIPE_REQUEST_INVALID
        assert diagnostic.target.startswith("not_executable:")


def test_registry_revision_is_stable_across_semver_build_metadata():
    """Build metadata is ignored FOR PRECEDENCE, so two versions differing only
    in it compare equal — and their order fell to dict insertion order."""
    def regs(order):
        return tuple(
            _reg(recipe_id="r.build", recipe_version=v, is_default=(i == 0))
            for i, v in enumerate(order)
        )

    a = build_test_registry(regs(["1.0.0+alpha", "1.0.0+beta"]))
    b = build_test_registry(regs(["1.0.0+beta", "1.0.0+alpha"]))
    assert [d.recipe_version for d in a.descriptors()] == [
        d.recipe_version for d in b.descriptors()
    ]


# ---------------------------------------------------------------------------
# Codex-review regressions (issue #145)
# ---------------------------------------------------------------------------


def test_an_extra_live_version_is_reported_as_skew():
    """``live_only`` compares (id, VERSION), not id.

    An id-only subtraction is blind to an extra VERSION of a known recipe: a
    live registry carrying x@1.0.0 AND x@2.0.0 reported ``match`` against a
    fully-hashed expectation naming only x@1.0.0 — the parallel-version support
    this registry advertises (issue #145, Codex review).
    """
    def reg(version, default):
        return _reg(recipe_id="x.multi", recipe_version=version, is_default=default)

    registry = build_test_registry((reg("1.0.0", True), reg("2.0.0", False)))
    first = next(d for d in registry.descriptors() if d.recipe_version == "1.0.0")
    skew = registry.compare(
        ExpectedRecipeRegistryV1(
            entries=(
                ExpectedRecipeEntryV1(
                    recipe_id="x.multi",
                    recipe_version="1.0.0",
                    implementation_sha256=first.provenance.implementation_sha256,
                ),
            )
        )
    )
    assert skew.status == "mismatch"
    assert skew.live_only == ("x.multi@2.0.0",)


def test_an_expectation_naming_every_live_version_still_matches():
    """The converse: the fix must not make a complete expectation mismatch."""
    def reg(version, default):
        return _reg(recipe_id="x.multi", recipe_version=version, is_default=default)

    registry = build_test_registry((reg("1.0.0", True), reg("2.0.0", False)))
    skew = registry.compare(
        ExpectedRecipeRegistryV1(
            entries=tuple(
                ExpectedRecipeEntryV1(
                    recipe_id=d.recipe_id,
                    recipe_version=d.recipe_version,
                    implementation_sha256=d.provenance.implementation_sha256,
                )
                for d in registry.descriptors()
            )
        )
    )
    assert skew.status == "match"
    assert skew.live_only == ()


def test_a_wholly_unknown_live_id_is_reported_by_id_not_by_version():
    """An id the caller never claimed to know about is reported once, not per
    version — otherwise one unknown recipe with five versions drowns the finding."""
    registry = build_test_registry((_reg(recipe_id="a.known"), _reg(recipe_id="z.unknown")))
    known = next(d for d in registry.descriptors() if d.recipe_id == "a.known")
    skew = registry.compare(
        ExpectedRecipeRegistryV1(
            entries=(
                ExpectedRecipeEntryV1(
                    recipe_id="a.known",
                    recipe_version=known.recipe_version,
                    implementation_sha256=known.provenance.implementation_sha256,
                ),
            )
        )
    )
    assert skew.live_only == ("z.unknown",)


def test_the_layer_module_list_follows_the_ACTIVE_import_namespace():
    """``PACKAGE_NAME`` is the DISTRIBUTION name; the import prefix is not.

    Run in a SUBPROCESS that loads the registry as ``src.boomi_mcp.*``, because
    under pytest the active namespace already IS ``boomi_mcp`` — so asserting
    the derivation in-process passes just as well with the prefix hard-coded,
    which is how the first version of this test managed to guard nothing
    (issue #145, live QA).
    """
    import subprocess
    import sys

    # Assert the NEGATIVE first: ``boomi_mcp`` must be unreachable in the child.
    # ``_clean_env`` shuts PYTHONPATH, but the editable-install ``.pth`` is a
    # second channel — it only fails to leak today because it points at a stale
    # path. Depending on which channel happens to be shut is not isolation
    # (issue #145, live QA).
    probe = (
        "import sys; sys.path.insert(0, '.')\n"
        "try:\n"
        "    import boomi_mcp\n"
        "except ImportError:\n"
        "    pass\n"
        "else:\n"
        "    raise SystemExit('LEAKED: boomi_mcp is importable in the probe')\n"
        "from src.boomi_mcp.recipes.registry import RECIPE_LAYER_MODULES\n"
        "print(RECIPE_LAYER_MODULES[0])"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=str(_project_root),
        capture_output=True,
        text=True,
        env=_clean_env(),
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().startswith("src.boomi_mcp."), result.stdout

    # ...and the DISTRIBUTION name in published provenance is unaffected.
    assert production_registry().descriptors()[0].provenance.package_name == "boomi_mcp"


def test_the_recipe_layer_itself_loads_under_the_src_namespace():
    """The property the prefix exists to guarantee, for the modules #145 owns.

    HONEST BOUND: the three MIGRATED SURFACES (``patterns/composition`` and the
    two archetypes) do NOT load under ``src.boomi_mcp`` — five pre-existing
    files, ``connector_builder.py`` among them, use absolute ``boomi_mcp.``
    imports, and that has been true since long before this issue (verified
    against the baseline tree). Fixing those is not #145's to do, so this pins
    what the prefix derivation actually buys: every module the recipe layer owns
    now resolves under whichever namespace loaded it.
    """
    import subprocess
    import sys

    owned = [
        "build_info",
        "models.recipe_contributions",
        "patterns.recipe_bridge",
        "recipes.builtins.catalog",
        "recipes.builtins.fanout",
        "recipes.builtins.sync",
        "recipes.composer",
        "recipes.contracts",
        "recipes.engine",
        "recipes.errors",
        "recipes.materialization",
        "recipes.registry",
    ]
    probe = (
        "import sys, importlib; sys.path.insert(0, '.')\n"
        "try:\n"
        "    import boomi_mcp\n"
        "except ImportError:\n"
        "    pass\n"
        "else:\n"
        "    raise SystemExit('LEAKED: boomi_mcp is importable in the probe')\n"
        f"for m in {owned!r}: importlib.import_module('src.boomi_mcp.' + m)\n"
        "print('ok')"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=str(_project_root),
        capture_output=True,
        text=True,
        env=_clean_env(),
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


def test_every_layer_module_is_importable_under_the_active_namespace():
    """Under the namespace pytest actually runs in, ALL of them load."""
    import importlib

    from boomi_mcp.recipes.registry import RECIPE_LAYER_MODULES

    for module in RECIPE_LAYER_MODULES:
        importlib.import_module(module)


def test_an_adapter_naming_an_unregistered_target_fails_at_construction():
    """An adapter's declared target must name a REGISTERED (id, version).

    Nothing required it before, so an adapter could point at a version that did
    not exist and every surface reading ``adapter_target`` would report a recipe
    nobody could run (issue #145, live QA).
    """
    with pytest.raises(ValueError, match="unregistered adapter target"):
        build_test_registry(
            (
                RecipeRegistrationV1(
                    recipe_id="test.adapter",
                    recipe_version="1.0.0",
                    entry_kind="compatibility_adapter",
                    is_default=True,
                    adapter_target=RecipeReferenceV1(
                        recipe_id="never.registered", recipe_version="1.0.0"
                    ),
                ),
            )
        )


def test_an_adapter_pointing_at_a_non_executable_entry_fails_at_construction():
    """Adapting TO an advisory pointer would produce an unrunnable route."""
    with pytest.raises(ValueError, match="not executable"):
        build_test_registry(
            (
                RecipeRegistrationV1(
                    recipe_id="test.adapter",
                    recipe_version="1.0.0",
                    entry_kind="compatibility_adapter",
                    is_default=True,
                    adapter_target=RecipeReferenceV1(
                        recipe_id="test.advisory", recipe_version="1.0.0"
                    ),
                ),
                RecipeRegistrationV1(
                    recipe_id="test.advisory",
                    recipe_version="1.0.0",
                    entry_kind="advisory",
                    is_default=True,
                ),
            )
        )


def test_every_production_adapter_target_is_registered_and_executable():
    """The production catalog satisfies the rule it now enforces."""
    registry = production_registry()
    by_key = {(d.recipe_id, d.recipe_version): d for d in registry.descriptors()}
    adapters = [d for d in registry.descriptors() if d.adapter_target is not None]
    assert len(adapters) == 3
    for adapter in adapters:
        key = (adapter.adapter_target.recipe_id, adapter.adapter_target.recipe_version)
        assert key in by_key, key
        assert by_key[key].entry_kind == "executable_recipe", key


def test_live_only_is_globally_sorted_in_the_mixed_case():
    """Two sorted runs concatenated is not a sorted collection."""
    registry = build_test_registry(
        (
            _reg(recipe_id="m.known", recipe_version="1.0.0"),
            _reg(recipe_id="m.known", recipe_version="2.0.0", is_default=False),
            _reg(recipe_id="a.unknown"),
            _reg(recipe_id="z.unknown"),
        )
    )
    known = next(
        d for d in registry.descriptors()
        if d.recipe_id == "m.known" and d.recipe_version == "1.0.0"
    )
    skew = registry.compare(
        ExpectedRecipeRegistryV1(
            entries=(
                ExpectedRecipeEntryV1(
                    recipe_id="m.known",
                    recipe_version="1.0.0",
                    implementation_sha256=known.provenance.implementation_sha256,
                ),
            )
        )
    )
    assert skew.status == "mismatch"
    assert list(skew.live_only) == sorted(skew.live_only), skew.live_only
    assert set(skew.live_only) == {"a.unknown", "m.known@2.0.0", "z.unknown"}


def test_a_version_mismatch_is_not_also_reported_as_live_only():
    """One fact, one finding. Reporting it twice makes the collections disagree
    about how many problems there are."""
    registry = build_test_registry((_reg(recipe_id="v.one", recipe_version="1.0.0"),))
    skew = registry.compare(
        ExpectedRecipeRegistryV1(
            entries=(
                ExpectedRecipeEntryV1(recipe_id="v.one", recipe_version="9.9.9"),
            )
        )
    )
    assert skew.status == "mismatch"
    assert [m.recipe_id for m in skew.version_mismatches] == ["v.one"]
    assert skew.live_only == ()


# ---------------------------------------------------------------------------
# Live-QA regression (issue #145): build defects with no test at all
# ---------------------------------------------------------------------------


def test_an_executable_recipe_without_an_input_model_fails_at_construction():
    with pytest.raises(ValueError, match="requires both an executor and an input model"):
        build_test_registry((_reg(input_model=None),))


def test_an_executable_recipe_without_an_executor_fails_at_construction():
    with pytest.raises(ValueError, match="requires both an executor and an input model"):
        build_test_registry((_reg(executor=None),))


def test_an_executable_recipe_naming_an_adapter_target_fails_at_construction():
    """Only a compatibility adapter adapts TO something."""
    with pytest.raises(ValueError, match="must not name an adapter target"):
        build_test_registry(
            (
                _reg(
                    adapter_target=RecipeReferenceV1(
                        recipe_id="x.y", recipe_version="1.0.0"
                    )
                ),
            )
        )


def test_an_executable_recipe_without_a_conflict_policy_fails_at_construction():
    """A recipe that can write a contested slot must say what it will merge."""
    with pytest.raises(ValueError, match="must declare a conflict policy"):
        build_test_registry((_reg(conflict_policy=None),))


def test_a_nested_executor_is_rejected_at_construction():
    """``<locals>`` in the qualname — a function defined inside another.

    Its source is readable but its enclosing scope is not, so the same
    provenance argument that rejects closures applies.
    """

    def outer():
        def inner(inp):  # pragma: no cover - rejected at registration
            return ()

        return inner

    with pytest.raises(ValueError, match="module level"):
        build_test_registry((_reg(executor=outer()),))


def _raise_value_error_sites(module):
    """Every ``raise ValueError`` line in a module, and whether it is pragma'd.

    Matches BOTH spellings: ``raise ValueError(...)`` (an ``ast.Call``) and a bare
    ``raise ValueError`` (an ``ast.Name``). Counting only the call form left the
    bare one silent — and the bare form is precisely what bypasses a guard that
    says it counts them all (issue #145, live QA).
    """
    import ast

    source = Path(module.__file__).read_text()
    lines = source.splitlines()
    sites = {}
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Raise) or node.exc is None:
            continue
        exc = node.exc
        name = (
            getattr(exc.func, "id", None)
            if isinstance(exc, ast.Call)
            else getattr(exc, "id", None)
        )
        if name != "ValueError":
            continue
        pragma = any(
            "pragma: no cover" in lines[i]
            for i in range(max(0, node.lineno - 3), node.lineno)
        )
        sites[node.lineno] = pragma
    return sites


def test_every_reachable_registry_build_defect_is_exercised_by_a_test():
    """A real REACHABILITY assertion, not a count.

    Runs this module's own tests in a subprocess under ``sys.settrace`` and
    requires every non-pragma ``raise ValueError`` in ``registry.py`` to have been
    executed. An earlier version was a pinned count, justified by "asserting
    reachability needs a coverage tool this repo does not carry" — which live QA
    refuted: the stdlib tracer below is that assertion, and it is green
    (issue #145).

    The tracer returns ``None`` for frames outside the target file, so only
    ``registry.py`` is line-traced and the run stays fast.
    """
    import subprocess
    import sys

    if os.environ.get("BOOMI_RECIPE_TRACE_CHILD"):
        pytest.skip("re-entry guard: this test spawns the run it measures")

    from boomi_mcp.recipes import registry as registry_module

    sites = _raise_value_error_sites(registry_module)
    expected = sorted(line for line, pragma in sites.items() if not pragma)
    assert expected, "no raise sites found — the matcher is broken"

    driver = f"""
import sys, pathlib, json
target = str(pathlib.Path({str(Path(registry_module.__file__))!r}).resolve())
hit = set()
def tracer(frame, event, arg):
    if frame.f_code.co_filename != target:
        return None
    if event == "line":
        hit.add(frame.f_lineno)
    return tracer
import pytest
sys.settrace(tracer)
code = pytest.main(["-q", "-p", "no:cacheprovider", {str(Path(__file__))!r}])
sys.settrace(None)
print("TRACE:" + json.dumps(sorted(hit)))
raise SystemExit(0 if code == 0 else 1)
"""
    result = subprocess.run(
        [sys.executable, "-c", driver],
        cwd=str(_project_root),
        capture_output=True,
        text=True,
        env={
            **_inherited_env(),
            "PYTHONPATH": str(_project_root / "src"),
            # Without this the child re-runs THIS test, which spawns another
            # child, forever.
            "BOOMI_RECIPE_TRACE_CHILD": "1",
        },
    )
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-2000:]
    marker = [ln for ln in result.stdout.splitlines() if ln.startswith("TRACE:")]
    assert marker, result.stdout[-2000:]
    hit = set(json.loads(marker[-1][len("TRACE:"):]))

    missed = [line for line in expected if line not in hit]
    assert missed == [], (
        f"registry.py build defects with no test: lines {missed}. "
        "Either add a test or mark the site '# pragma: no cover' with a reason."
    )

    # The ESCAPE HATCH is pinned too. Excluding pragma'd sites and asserting
    # nothing about them turned "edit a pinned tuple" into "type a comment": a
    # new untested raise fails above, and adding a pragma to the same line made
    # it pass silently (issue #145, live QA).
    # The ESCAPE HATCH is pinned by IDENTITY, not cardinality. A count is
    # substitution-blind: removing one pragma'd raise and adding a different
    # untested one in the same commit keeps it at 3 and the suite green (issue
    # #145, live QA). The reason text is what makes two exemptions distinct.
    exempted = _pragma_raise_identities(registry_module)
    assert exempted == (
        (
            "RecipeRegistry._check_entry_kind",
            "the Literal already bounds this",
            'raise ValueError(f"unknown recipe entry kind {kind!r}")',
        ),
        (
            "RecipeRegistry._implementation_identity",
            "environment",
            'raise ValueError( "recipe provenance requires readable source for "'
            ' f"{catalog_module}" ) from exc',
        ),
        (
            "RecipeRegistry._implementation_identity",
            "environment",
            'raise ValueError( f"recipe provenance requires readable source for "'
            ' f"{module}.{symbol}" ) from exc',
        ),
    ), (
        f"registry.py's exempted raise sites changed to {exempted}. "
        "A pragma is an assertion that the site is unreachable — justify it here."
    )


def _pragma_reason(line):
    """The justification text on a ``# pragma: no cover`` line, or ``""``.

    Stops at a following tool directive: ``# pragma: no cover  # type: ignore``
    has no reason, and consuming the directive as one was how a bare exemption
    passed (issue #145, live QA).
    """
    marker = "pragma: no cover"
    if marker not in line:
        return ""
    tail = line.split(marker, 1)[1]
    tail = tail.split("#", 1)[0]
    return tail.strip(" -\u2014\t").strip()


def _enclosing_qualname(tree, lineno):
    """The dotted name of the function/class enclosing a line."""
    import ast

    best = []
    for node in ast.walk(tree):
        if not isinstance(
            node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
        ):
            continue
        end = getattr(node, "end_lineno", node.lineno)
        if node.lineno <= lineno <= end:
            best.append((node.lineno, node.name))
    return ".".join(name for _, name in sorted(best)) or "<module>"


def _pragma_raise_identities(module):
    """``(enclosing qualname, reason)`` per pragma'd ``raise ValueError``.

    IDENTITY, not a count and not a bare reason multiset. A count is blind to
    substitution; so is a reason multiset when two exemptions legitimately share
    a reason string — which two of these three do ("environment"), so deleting
    one and adding a different untested raise with the same reason kept the tuple
    equal and the suite green (issue #145, live QA). The enclosing function is
    what makes two same-reason exemptions distinguishable.
    """
    import ast

    source = Path(module.__file__).read_text()
    tree = ast.parse(source)
    lines = source.splitlines()
    by_line = {
        node.lineno: node
        for node in ast.walk(tree)
        if isinstance(node, ast.Raise) and node.exc is not None
    }
    identities = []
    for line_number, pragma in _raise_value_error_sites(module).items():
        if not pragma:
            continue
        reason = ""
        for i in range(max(0, line_number - 3), line_number):
            reason = _pragma_reason(lines[i]) or reason
        # The raise's OWN source too. Two exemptions can share a function AND a
        # reason — two of these three do — so neither is a discriminator on its
        # own; the message is what makes them distinct.
        node = by_line.get(line_number)
        message = " ".join((ast.get_source_segment(source, node) or "").split())
        identities.append(
            (_enclosing_qualname(tree, line_number), reason, message)
        )
    return tuple(sorted(identities))


def test_every_pragma_in_the_registry_states_a_reason():
    """``# pragma: no cover`` is a claim; a bare one is a claim with no argument.

    One site carried a bare pragma, so the guard's own instruction ("mark it with
    a reason") was unenforced (issue #145, live QA).
    """
    from boomi_mcp.recipes import registry as registry_module

    bare = []
    for number, line in enumerate(
        Path(registry_module.__file__).read_text().splitlines(), start=1
    ):
        if "pragma: no cover" not in line:
            continue
        reason = _pragma_reason(line)
        # At least one real WORD (3+ letters). ``- .`` is not a justification,
        # and a following ``# type: ignore`` is a different directive, not this
        # one's reason (issue #145, live QA). One word is enough — "environment"
        # says what it needs to. 3 rather than 4 so ``- see ADR-001 §5`` passes:
        # the bar is "not punctuation", not "verbose".
        if not re.findall(r"[A-Za-z]{3,}", reason):
            bare.append(number)
    assert bare == [], f"pragma with no usable reason at lines {bare}"


def test_the_raise_site_matcher_sees_the_bare_spelling():
    """Guard the guard: ``raise ValueError`` with no call must be counted.

    The matcher missing it is what made the previous census silent on exactly the
    form that bypasses it.
    """
    import ast
    import types

    module = types.SimpleNamespace()
    with tempfile.NamedTemporaryFile(
        "w", suffix=".py", delete=False, encoding="utf-8"
    ) as handle:
        handle.write("def f(x):\n    if x:\n        raise ValueError\n")
        module.__file__ = handle.name
    try:
        sites = _raise_value_error_sites(module)
        assert sites == {3: False}, sites
    finally:
        os.unlink(module.__file__)


@pytest.mark.parametrize(
    "line,expected",
    [
        ("x = 1  # pragma: no cover - environment", "environment"),
        ("x = 1  # pragma: no cover - see ADR-001 §5", "see ADR-001 §5"),
        ("x = 1  # pragma: no cover", ""),
        ("x = 1  # pragma: no cover  # type: ignore", ""),
        ("x = 1  # pragma: no cover - .", "."),
        ("x = 1", ""),
    ],
)
def test_the_pragma_reason_parser_stops_at_a_following_directive(line, expected):
    """``# pragma: no cover  # type: ignore`` has NO reason.

    Consuming the following tool directive as the justification is how a bare
    exemption passed the lint (issue #145, live QA). The parser had no test of
    its own — only the lint that uses it — so reverting either half of the fix
    left the suite green.
    """
    assert _pragma_reason(line) == expected


@pytest.mark.parametrize(
    "reason,accepted",
    [
        ("environment", True),
        ("see ADR-001 §5", True),
        ("Literal-bounded", True),
        (".", False),
        ("", False),
        ("x", False),
    ],
)
def test_the_reason_bar_is_not_punctuation_rather_than_verbose(reason, accepted):
    """The bar is deliberately loose: one real word is enough.

    A 4-letter minimum rejected ``see ADR-001 §5``, which is a perfectly good
    justification — so the threshold is 3. ``- .`` is still rejected, which is
    the case that matters.
    """
    assert bool(re.findall(r"[A-Za-z]{3,}", reason)) is accepted
