"""Security boundary for the recipe layer (issue #145 M12.10).

The compatibility adapters legitimately accept SQL, hosts, usernames, credential
references, headers and script bodies — those are their existing public inputs.
The whole security design is that none of that crosses into a recipe input, a
contribution, a diagnostic, a log record, a repr, or the registry snapshot.

So the central test here is a SENTINEL test: drive the real presets and the real
composition with recognizable poison in every legacy field, then assert the poison
appears nowhere on the recipe side. That is stronger than enumerating forbidden
keys, because it does not depend on having thought of the right key name.
"""

import json
import logging
import pickle
import sys
from pathlib import Path

import pytest
from pydantic import ConfigDict, model_validator

_project_root = Path(__file__).resolve().parent.parent
_src = str(_project_root / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(_project_root / "tests" / "patterns"))

from boomi_mcp.categories.integration_authoring import (
    build_from_archetype_action,
    compose_archetypes_action,
    list_integration_archetypes_action,
)
from boomi_mcp.categories.meta_tools import (
    get_schema_template_action,
    plan_integration_design_action,
)
from boomi_mcp.errors import RECIPE_CONTRIBUTION_INVALID, RECIPE_INPUT_INVALID
from boomi_mcp.models.integration_models import IntegrationSpecV1
from boomi_mcp.models.recipe_contributions import (
    RecipeContributionValidationError,
    canonical_recipe_contributions_json,
    parse_recipe_contribution,
    scan_forbidden_recipe_shape,
)
from boomi_mcp.patterns.archetypes.api_to_api_sync import ApiToApiSyncArchetype
from boomi_mcp.patterns.recipe_bridge import (
    build_catalog,
    run_fanout_recipe,
    run_sync_preset_recipe,
)
from boomi_mcp.recipes import (
    MaterializationCatalog,
    RecipeError,
    RecipeRequestV1,
    production_registry,
    recipe_error_envelope,
    run_recipes,
)
from boomi_mcp.recipes.registry import build_test_registry
from boomi_mcp.recipes.builtins.catalog import (
    RECIPE_API_TO_API_SYNC,
    RECIPE_DB_REST_FANOUT,
)
from boomi_mcp.recipes.contracts import RecipeConflictPolicyV1, RecipeRegistrationV1
from boomi_mcp.recipes.builtins.sync import SyncRecipeInputV1

from test_archetype_composition import _cache_links, _options, _parts

SENTINELS = (
    "SENTINEL-SQL-SELECT-SECRETS",
    "SENTINEL-HEADER-VALUE",
    "SENTINEL-CREDENTIAL-REF",
    "SENTINEL-HOSTNAME",
    "SENTINEL-USERNAME",
)


# ---------------------------------------------------------------------------
# The sentinel test — the one that does not depend on guessing a key name
# ---------------------------------------------------------------------------


def _poisoned_compose_parts():
    parts = _parts()
    for part in parts:
        if part["kind"] == "db_source":
            settings = part["parameters"]["binding"]["settings"]
            settings["host"] = "SENTINEL-HOSTNAME"
            settings["username"] = "SENTINEL-USERNAME"
            settings["credential_ref"] = "SENTINEL-CREDENTIAL-REF"
            part["parameters"]["read_operation"]["sql"] = "SENTINEL-SQL-SELECT-SECRETS"
    return parts


def _poisoned_preset_parameters():
    parameters = json.loads(json.dumps(ApiToApiSyncArchetype.examples[0].parameters))
    source_binding = parameters["source"]["binding"]
    if source_binding.get("settings") is not None:
        source_binding["settings"]["base_url"] = "https://SENTINEL-HOSTNAME.invalid"
        source_binding["settings"]["default_headers"] = {
            "X-Trace": "SENTINEL-HEADER-VALUE"
        }
    target_binding = parameters["target"]["binding"]
    if target_binding.get("settings") is not None:
        target_binding["settings"]["base_url"] = "https://SENTINEL-HOSTNAME.invalid"
        target_binding["settings"]["default_headers"] = {
            "X-Trace": "SENTINEL-HEADER-VALUE"
        }
    return parameters


def _recipe_side_blob(result):
    """Everything the RECIPE side produced, as one searchable string."""
    parts = [
        canonical_recipe_contributions_json(
            [item.contribution for item in result.composed.component_slots]
        ),
        canonical_recipe_contributions_json(
            [item.contribution for item in result.composed.constraints]
        ),
        json.dumps(
            {
                key: root.model_dump(mode="json")
                for key, root in result.composed.process_roots
            },
            sort_keys=True,
        ),
        json.dumps(dict(result.provenance), sort_keys=True, default=str),
    ]
    for item in result.composed.component_slots:
        parts.append(repr(item.contribution))
    return "\n".join(parts)


def test_compose_sentinels_never_reach_the_recipe_side(caplog):
    caplog.set_level(logging.DEBUG)
    options = dict(_options())
    options["links"] = _cache_links("billing")
    response = compose_archetypes_action(parts=_poisoned_compose_parts(), options=options)
    assert response["_success"] is True

    # The sentinels ARE in the legacy spec — otherwise the test proves nothing.
    spec_blob = json.dumps(response["integration_spec"], sort_keys=True)
    assert "SENTINEL-SQL-SELECT-SECRETS" in spec_blob
    assert "SENTINEL-HOSTNAME" in spec_blob

    spec = IntegrationSpecV1.model_validate(response["integration_spec"])
    result = run_fanout_recipe(
        recipe_id=RECIPE_DB_REST_FANOUT,
        components=spec.components,
        process=spec.components[-1],
    )
    blob = _recipe_side_blob(result)
    for sentinel in SENTINELS:
        assert sentinel not in blob, sentinel
    assert not any(sentinel in caplog.text for sentinel in SENTINELS)


def test_preset_sentinels_never_reach_the_recipe_side(caplog):
    caplog.set_level(logging.DEBUG)
    response = build_from_archetype_action(
        "api_to_api_sync", _poisoned_preset_parameters()
    )
    assert response["_success"] is True

    spec = IntegrationSpecV1.model_validate(response["integration_spec"])
    result = run_sync_preset_recipe(
        recipe_id=RECIPE_API_TO_API_SYNC,
        components=spec.components,
        process=spec.components[-1],
    )
    blob = _recipe_side_blob(result)
    for sentinel in SENTINELS:
        assert sentinel not in blob, sentinel
    assert not any(sentinel in caplog.text for sentinel in SENTINELS)


def test_the_recipe_provenance_on_the_response_carries_no_sentinel():
    response = compose_archetypes_action(
        parts=_poisoned_compose_parts(), options=_options()
    )
    blob = json.dumps(response["recipe_provenance"], sort_keys=True)
    for sentinel in SENTINELS:
        assert sentinel not in blob


def test_the_registry_snapshot_carries_no_authored_material():
    blob = json.dumps(production_registry().snapshot(), sort_keys=True)
    for token in SENTINELS + ("password", "credential", "base_url", "sql"):
        assert token not in blob, token


# ---------------------------------------------------------------------------
# The catalog holds the poison and refuses to travel
# ---------------------------------------------------------------------------


def test_the_materialization_catalog_is_not_serializable():
    """A serializable catalog is one ``model_dump()`` from a diagnostic."""
    spec = IntegrationSpecV1.model_validate(
        compose_archetypes_action(parts=_poisoned_compose_parts(), options=_options())[
            "integration_spec"
        ]
    )
    catalog, _slots = build_catalog(spec.components)
    with pytest.raises(TypeError):
        pickle.dumps(catalog)
    with pytest.raises(TypeError):
        catalog.__getstate__()


def test_the_catalog_repr_leaks_nothing():
    spec = IntegrationSpecV1.model_validate(
        compose_archetypes_action(parts=_poisoned_compose_parts(), options=_options())[
            "integration_spec"
        ]
    )
    catalog, _slots = build_catalog(spec.components)
    rendered = repr(catalog)
    assert rendered.startswith("<MaterializationCatalog slots=")
    for sentinel in SENTINELS:
        assert sentinel not in rendered


def test_the_projected_slot_headers_carry_no_configuration():
    """The slots the recipe receives are headers plus an opaque name."""
    spec = IntegrationSpecV1.model_validate(
        compose_archetypes_action(parts=_poisoned_compose_parts(), options=_options())[
            "integration_spec"
        ]
    )
    _catalog, slots = build_catalog(spec.components)
    blob = json.dumps(slots, sort_keys=True)
    for sentinel in SENTINELS:
        assert sentinel not in blob
    for slot in slots:
        assert set(slot) == {
            "contribution_id",
            "component_key",
            "component_type",
            "materialization_mode",
            "materializer_slot",
        }


# ---------------------------------------------------------------------------
# Input rejection
# ---------------------------------------------------------------------------

_POISONED_INPUT_FIELDS = [
    ("config", {"host": "x"}),
    ("settings", {"password": "p"}),
    ("headers", {"X-Api-Key": "k"}),
    ("sql", "SELECT 1"),
    ("credential_ref", "SECRET"),
    ("base_url", "https://x.invalid"),
    ("username", "svc"),
    ("script", "x = 1"),
    ("language", "groovy2"),
    ("xml", "<Component/>"),
    ("depends_on", ["a"]),
    ("path", "/body/steps/0"),
    ("provenance", {"module": "x"}),
    ("validation_policy", "flow_sequence"),
]


@pytest.mark.parametrize(
    "field,value", _POISONED_INPUT_FIELDS, ids=[f[0] for f in _POISONED_INPUT_FIELDS]
)
def test_a_poisoned_recipe_input_is_rejected_before_the_model(field, value):
    raw = {
        "version": "1",
        "process_key": "main_process",
        "source_connection_ref": "$ref:a",
        "source_operation_ref": "$ref:b",
        "map_ref": "$ref:m",
        "target_connection_ref": "$ref:c",
        "target_operation_ref": "$ref:d",
        "component_slots": [
            {
                "contribution_id": "c.0",
                "component_key": "main_process",
                "component_type": "process",
                "materialization_mode": "create",
                "materializer_slot": "slot.main",
            }
        ],
        field: value,
    }
    with pytest.raises(RecipeError) as exc:
        run_recipes(
            [
                RecipeRequestV1(
                    recipe_id=RECIPE_API_TO_API_SYNC,
                    invocation_id="i1",
                    raw_input=raw,
                )
            ],
            catalog=MaterializationCatalog({}),
        )
    assert exc.value.diagnostics[0].code == RECIPE_INPUT_INVALID


def test_an_input_rejection_diagnostic_carries_no_value():
    raw = {"version": "1", "sql": "SENTINEL-SQL-SELECT-SECRETS"}
    with pytest.raises(RecipeError) as exc:
        run_recipes(
            [
                RecipeRequestV1(
                    recipe_id=RECIPE_API_TO_API_SYNC,
                    invocation_id="i1",
                    raw_input=raw,
                )
            ],
            catalog=MaterializationCatalog({}),
        )
    blob = json.dumps([d.model_dump(mode="json") for d in exc.value.diagnostics])
    assert "SENTINEL-SQL-SELECT-SECRETS" not in blob
    assert "SENTINEL-SQL-SELECT-SECRETS" not in str(exc.value)
    assert "SENTINEL-SQL-SELECT-SECRETS" not in repr(exc.value)


# ---------------------------------------------------------------------------
# Executor misbehaviour
# ---------------------------------------------------------------------------


def _leaky_executor(inp):
    raise ValueError("SENTINEL-SQL-SELECT-SECRETS leaked from an executor")


def _non_tuple_executor(inp):
    return [1, 2, 3]


def _foreign_object_executor(inp):
    return ("not a contribution",)


def _undeclared_output_executor(inp):
    return (
        parse_recipe_contribution(
            {
                "contribution_kind": "constraint_requirement",
                "version": "1",
                "requirement_id": "req.x",
                "requirement": {"kind": "process", "process_key": "p"},
            }
        ),
    )


def _registry_with(executor, output_types=("process_ir_patch",)):
    return build_test_registry(
        (
            RecipeRegistrationV1(
                recipe_id="test.recipe",
                recipe_version="1.0.0",
                entry_kind="executable_recipe",
                is_default=True,
                input_model=SyncRecipeInputV1,
                executor=executor,
                output_types=output_types,
                conflict_policy=RecipeConflictPolicyV1(),
            ),
        )
    )


_VALID_INPUT = {
    "version": "1",
    "process_key": "main_process",
    "source_connection_ref": "$ref:a",
    "source_operation_ref": "$ref:b",
    "map_ref": "$ref:m",
    "target_connection_ref": "$ref:c",
    "target_operation_ref": "$ref:d",
    "component_slots": [
        {
            "contribution_id": "c.0",
            "component_key": "main_process",
            "component_type": "process",
            "materialization_mode": "create",
            "materializer_slot": "slot.main",
        }
    ],
}


def _run(registry):
    return run_recipes(
        [
            RecipeRequestV1(
                recipe_id="test.recipe", invocation_id="i1", raw_input=_VALID_INPUT
            )
        ],
        catalog=MaterializationCatalog({}),
        registry=registry,
    )


# ---------------------------------------------------------------------------
# A validator that returns something other than the model
#
# Both registration gates read the compiled schema, which records that a
# validator RUNS somewhere but never what it RETURNS. ``mode="after"`` receives
# the model and its return value IS the result, so a model carrying no banned
# node at all delivered the caller's undeclared keys to the executor. The engine
# closes it by inspecting the value that came back (issue #145, live QA).
# ---------------------------------------------------------------------------


_SMUGGLED_RAW: dict = {}

#: Module level, not a closure — the registry refuses an executor that closes
#: over state, because a closure is invisible to ``implementation_sha256``.
_EXECUTOR_CALLS: list = []


def _recording_executor(inp):
    _EXECUTOR_CALLS.append(inp)
    return _undeclared_output_executor(inp)


class _BypassInputV1(SyncRecipeInputV1):
    """Frozen, ``extra='forbid'``, and free of every banned node — and it leaks.

    ``before`` stashes the raw mapping and hands validation only the declared
    keys, so validation genuinely succeeds; ``after`` then discards the validated
    model and returns the stash. Both node types are ones the ban ALLOWS.
    """

    @model_validator(mode="before")
    @classmethod
    def _stash(cls, data):
        if isinstance(data, dict):
            _SMUGGLED_RAW.clear()
            _SMUGGLED_RAW.update(data)
            return {k: v for k, v in data.items() if k in cls.model_fields}
        return data

    @model_validator(mode="after")
    def _return_the_stash(self):
        return dict(_SMUGGLED_RAW)


class _SubclassBypassInputV1(SyncRecipeInputV1):
    """Returns a SUBCLASS instance, which ``isinstance(validated, model)`` allows.

    The returned class must descend from the REGISTERED one or ``isinstance``
    rejects it on its own and the test proves nothing — a first version returned
    a sibling and was silently vacuous, which the ``isinstance`` mutant caught.
    ``model_construct`` is what makes it reachable: it skips revalidation, so the
    undeclared keys are never re-checked against ``extra='forbid'``.
    """

    @model_validator(mode="after")
    def _return_a_subclass(self):
        return _OpenSubclassInputV1.model_construct(
            **self.model_dump(), smuggled="SENTINEL-SQL-SELECT-SECRETS"
        )


class _OpenSubclassInputV1(_SubclassBypassInputV1):
    """The ``isinstance`` defeat: a subclass of the registered model that accepts
    undeclared keys. Never registered itself, so no gate ever inspects it."""

    model_config = ConfigDict(extra="allow", frozen=True)


class _HonestAfterInputV1(SyncRecipeInputV1):
    """The firing control: ``mode='after'`` used the way it is meant to be."""

    @model_validator(mode="after")
    def _check_something(self):
        if not self.process_key:
            raise ValueError("process_key is required")
        return self


def _registry_with_input(model, executor=None):
    return build_test_registry(
        (
            RecipeRegistrationV1(
                recipe_id="test.recipe",
                recipe_version="1.0.0",
                entry_kind="executable_recipe",
                is_default=True,
                input_model=model,
                executor=executor or _undeclared_output_executor,
                output_types=("constraint_requirement",),
                conflict_policy=RecipeConflictPolicyV1(),
            ),
        )
    )


#: Each case pairs a bypassing model with the raw input that REACHES its bypass.
#:
#: The pairing is load-bearing and was wrong once. The subclass model has no
#: ``before`` validator, so sending it an input carrying ``smuggled`` is refused
#: by ordinary ``extra='forbid'`` handling long before the guard runs — the test
#: then passed with the guard deleted entirely. It must be given CLEAN input; the
#: model injects the undeclared key itself, from inside the validator.
_BYPASS_CASES = [
    (_BypassInputV1, "poisoned"),
    (_SubclassBypassInputV1, "clean"),
]


@pytest.mark.parametrize(
    "model,raw_kind",
    _BYPASS_CASES,
    ids=["returns_a_raw_dict", "returns_an_open_subclass"],
)
def test_a_validator_that_returns_a_foreign_object_is_rejected(model, raw_kind):
    """The registration gates PASS these models — that is the point of the test.

    Asserts all three consequences at once: the run is refused, the diagnostic is
    value-free, and the executor is never handed the object.
    """
    _SMUGGLED_RAW.clear()
    _EXECUTOR_CALLS.clear()
    registry = _registry_with_input(model, executor=_recording_executor)

    raw = dict(_VALID_INPUT)
    if raw_kind == "poisoned":
        raw["smuggled"] = "SENTINEL-SQL-SELECT-SECRETS"

    with pytest.raises(RecipeError) as exc:
        run_recipes(
            [RecipeRequestV1(recipe_id="test.recipe", invocation_id="i1", raw_input=raw)],
            catalog=MaterializationCatalog({}),
            registry=registry,
        )
    assert exc.value.diagnostics[0].code == RECIPE_INPUT_INVALID
    assert _EXECUTOR_CALLS == []
    blob = json.dumps([d.model_dump(mode="json") for d in exc.value.diagnostics])
    assert "SENTINEL-SQL-SELECT-SECRETS" not in blob
    assert "SENTINEL-SQL-SELECT-SECRETS" not in str(exc.value)


def test_the_subclass_case_actually_reaches_the_guard():
    """Anti-vacuity control for the pairing above.

    Pins that the subclass model's CLEAN input validates successfully and yields
    an object that ``isinstance`` accepts and exact-type rejects — so the case
    genuinely exercises the guard rather than ordinary extras rejection.
    """
    validated = _SubclassBypassInputV1.model_validate(dict(_VALID_INPUT))
    assert isinstance(validated, _SubclassBypassInputV1)
    assert type(validated) is not _SubclassBypassInputV1
    assert getattr(validated, "smuggled", None) == "SENTINEL-SQL-SELECT-SECRETS"


def test_an_honest_after_validator_is_not_refused():
    """Firing control.

    A guard that refused every ``mode='after'`` would pass the three tests above
    while breaking a production input model. ``ComposeDbRestFanoutInputV1`` uses
    ``after`` to enforce a cross-field rule that no field validator can express,
    which is exactly why the fix is a check on the returned VALUE rather than one
    more node type on the ban list.
    """
    registry = _registry_with_input(_HonestAfterInputV1)
    try:
        run_recipes(
            [
                RecipeRequestV1(
                    recipe_id="test.recipe",
                    invocation_id="i1",
                    raw_input=_VALID_INPUT,
                )
            ],
            catalog=MaterializationCatalog({}),
            registry=registry,
        )
    except RecipeError as exc:  # a later phase may object; input must not
        assert exc.diagnostics[0].code != RECIPE_INPUT_INVALID

    # And the guard's own predicate accepts it.
    validated = _HonestAfterInputV1.model_validate(_VALID_INPUT)
    assert type(validated) is _HonestAfterInputV1


def test_the_production_after_validator_returns_self():
    """Pins the premise of the paragraph above: ``after`` is used legitimately,
    so banning the node type is not an available fix."""
    from boomi_mcp.recipes.builtins.fanout import ComposeDbRestFanoutInputV1

    model = ComposeDbRestFanoutInputV1.model_construct(
        version="1",
        process_key="p",
        targets=(),
        component_slots=(),
    )
    assert ComposeDbRestFanoutInputV1._cache_rules(model) is model


def test_an_executor_exception_message_never_reaches_the_caller():
    with pytest.raises(RecipeError) as exc:
        _run(_registry_with(_leaky_executor))
    assert exc.value.diagnostics[0].code == RECIPE_CONTRIBUTION_INVALID
    blob = json.dumps([d.model_dump(mode="json") for d in exc.value.diagnostics])
    assert "SENTINEL-SQL-SELECT-SECRETS" not in blob
    assert "SENTINEL-SQL-SELECT-SECRETS" not in str(exc.value)


def test_a_non_tuple_return_is_rejected():
    with pytest.raises(RecipeError) as exc:
        _run(_registry_with(_non_tuple_executor))
    assert exc.value.diagnostics[0].code == RECIPE_CONTRIBUTION_INVALID


def test_a_foreign_object_return_is_rejected():
    with pytest.raises(RecipeError) as exc:
        _run(_registry_with(_foreign_object_executor))
    assert exc.value.diagnostics[0].code == RECIPE_CONTRIBUTION_INVALID


def test_an_undeclared_output_type_is_rejected():
    """Declaring outputs is a promise the engine keeps the recipe to."""
    with pytest.raises(RecipeError) as exc:
        _run(_registry_with(_undeclared_output_executor, ("process_ir_patch",)))
    diagnostic = exc.value.diagnostics[0]
    assert diagnostic.code == RECIPE_CONTRIBUTION_INVALID
    assert diagnostic.target == "undeclared_output:constraint_requirement"


# ---------------------------------------------------------------------------
# The advisory surface returns references, never executable material
# ---------------------------------------------------------------------------


def test_plan_integration_design_returns_references_only():
    """An advisory surface that handed back executable material would BE the
    "prose becomes executable" failure this issue exists to prevent."""
    response = plan_integration_design_action(archetype="api_to_api_sync")
    recipes = response["recommended_recipes"]
    assert recipes
    for entry in recipes:
        assert set(entry) <= {
            "recipe_id",
            "recipe_version",
            "entry_kind",
            "adapter_target",
        }
    blob = json.dumps(response, sort_keys=True, default=str)
    for token in (
        "contribution_kind",
        "set_process_root",
        "materializer_slot",
        "component_slots",
        "process_ir_patch",
    ):
        assert token not in blob, token


def test_the_doctrine_schema_surface_exposes_no_executable_payload():
    for name in ("design_doctrine", "account_governance"):
        response = get_schema_template_action(schema_name=name)
        blob = json.dumps(response, sort_keys=True, default=str)
        for token in ("materializer_slot", "set_process_root", "contribution_kind"):
            assert token not in blob, (name, token)


def test_the_recipe_schema_surface_exposes_no_credentials_or_config():
    for name in ("recipe_contributions", "recipe_registry"):
        blob = json.dumps(
            get_schema_template_action(schema_name=name), sort_keys=True, default=str
        )
        for sentinel in SENTINELS:
            assert sentinel not in blob


# ---------------------------------------------------------------------------
# Error envelopes
# ---------------------------------------------------------------------------


def test_the_recipe_error_envelope_declares_no_xml_and_no_mutation():
    with pytest.raises(RecipeError) as exc:
        production_registry().resolve("no.such.recipe")
    envelope = recipe_error_envelope(exc.value, registry_revision="abc")
    assert envelope["_success"] is False
    assert envelope["raw_xml_exposed"] is False
    assert envelope["boomi_mutation"] is False
    assert envelope["error"] == "Recipe processing failed"
    for diagnostic in envelope["recipe_diagnostics"]:
        assert diagnostic["message"]
        assert diagnostic["remediation"]


def test_diagnostic_messages_are_static_not_interpolated():
    """Two different failures of one code carry the SAME message text.

    An interpolated message is a leak path; equality here is what proves the
    table lookup is real rather than incidental.
    """
    registry = production_registry()
    messages = set()
    for missing in ("no.such.recipe", "another.missing.recipe"):
        with pytest.raises(RecipeError) as exc:
            registry.resolve(missing)
        messages.add(exc.value.diagnostics[0].message)
        assert missing not in exc.value.diagnostics[0].message
    assert len(messages) == 1


# ---------------------------------------------------------------------------
# Scanner behaviour on nested structures
# ---------------------------------------------------------------------------


def test_the_scanner_reaches_arbitrary_depth():
    payload = {"operations": [{"root": {"body": {"steps": [{"headers": {"a": "b"}}]}}}]}
    found = scan_forbidden_recipe_shape(payload)
    assert found is not None
    assert found[0][-1] == "headers"


def test_the_scanner_reports_a_structural_pointer_not_a_value():
    payload = {"component_slots": [{"config": {"password": "hunter2"}}]}
    path, reason = scan_forbidden_recipe_shape(payload)
    assert path == ("component_slots", 0, "config")
    assert "hunter2" not in reason


def test_the_scanner_accepts_a_legitimate_contribution_unchanged():
    """A guard that rejects valid input is not a guard, it is an outage."""
    spec = IntegrationSpecV1.model_validate(
        compose_archetypes_action(parts=_parts(), options=_options())["integration_spec"]
    )
    result = run_fanout_recipe(
        recipe_id=RECIPE_DB_REST_FANOUT,
        components=spec.components,
        process=spec.components[-1],
    )
    for _key, root in result.composed.process_roots:
        assert scan_forbidden_recipe_shape(root.model_dump(mode="json")) is None


# ---------------------------------------------------------------------------
# Live-QA regression (issue #145): the slot projection is LOSSY
# ---------------------------------------------------------------------------


def test_colliding_component_keys_are_rejected_not_silently_merged():
    """``_slot_for`` lower-cases and folds punctuation, so it is not injective.

    ``"Order Map"`` and ``"order_map"`` both fold to ``slot.order_map``. The
    catalog used to overwrite, leaving two contributed headers pointing at ONE
    component — a build that succeeds having emitted the wrong thing. Raised
    instead: a collision means the projection lost a component, and there is no
    safe way to guess which one the recipe meant.
    """
    from boomi_mcp.models.integration_models import IntegrationComponentSpec
    from boomi_mcp.patterns.recipe_bridge import _slot_for, build_catalog

    assert _slot_for("Order Map") == _slot_for("order_map")

    components = [
        IntegrationComponentSpec(
            key="Order Map", type="transform.map", action="create", name="A", config={}
        ),
        IntegrationComponentSpec(
            key="order_map", type="transform.map", action="create", name="B", config={}
        ),
    ]
    with pytest.raises(RuntimeError, match="collided"):
        build_catalog(components)


def test_distinct_keys_that_do_not_collide_still_build_a_full_catalog():
    """The guard must not reject the ordinary case."""
    from boomi_mcp.patterns.recipe_bridge import build_catalog

    spec = IntegrationSpecV1.model_validate(
        compose_archetypes_action(parts=_parts(), options=_options())["integration_spec"]
    )
    catalog, slots = build_catalog(spec.components)
    assert len(catalog.slots()) == len(spec.components)
    assert len(slots) == len(spec.components)
    assert len({s["materializer_slot"] for s in slots}) == len(spec.components)


def test_a_very_long_component_key_still_yields_a_valid_slot():
    """A long name is not a security finding, and must not be reported as one.

    A 101-character key used to produce a 129-character slot, which failed the
    input model and told the caller their input carried "credentials, SQL, raw
    XML" (issue #145, live QA). The slot now folds to a digest suffix.
    """
    from boomi_mcp.models.recipe_contributions import RecipeSemanticId
    from boomi_mcp.patterns.recipe_bridge import _MAX_SLOT_LENGTH, _slot_for
    from pydantic import TypeAdapter

    adapter = TypeAdapter(RecipeSemanticId)
    for length in (1, 50, 100, 101, 200, 1000):
        slot = _slot_for("k" * length)
        assert len(slot) <= _MAX_SLOT_LENGTH, length
        adapter.validate_python(slot)  # a VALID RecipeSemanticId at every length


def test_long_keys_that_differ_still_produce_different_slots():
    """Folding must not reintroduce the collision ``build_catalog`` rejects."""
    from boomi_mcp.patterns.recipe_bridge import _slot_for

    a = _slot_for("k" * 200 + "-alpha")
    b = _slot_for("k" * 200 + "-beta")
    assert a != b


def test_a_long_part_key_composes_end_to_end():
    """The user-visible half: the whole action must succeed, not just the slot."""
    parts = _parts()
    for part in parts:
        if part["kind"] == "rest_target" and part["key"] == "billing":
            part["key"] = "b" * 101
    response = compose_archetypes_action(parts=parts, options=_options())
    assert response["_success"] is True, response.get("error")


def test_a_non_ascii_component_key_still_yields_a_valid_slot():
    """``str.isalnum()`` is True for 'é' and 'Ω', which are NOT in
    ``RecipeSemanticId``'s class — so "ALWAYS valid" failed for any non-ASCII
    key (issue #145, live QA)."""
    from boomi_mcp.models.recipe_contributions import RecipeSemanticId
    from boomi_mcp.patterns.recipe_bridge import _slot_for
    from pydantic import TypeAdapter

    adapter = TypeAdapter(RecipeSemanticId)
    for key in ("café", "Ωmega", "日本語", "naïve_map", "Ωmega" * 40, "é"):
        adapter.validate_python(_slot_for(key))


def test_the_recipe_secret_list_detects_everything_processir_does():
    """The claim is DETECTION power, not literal superset — pinned as measured.

    ``credential`` substring-matches everything ``credentials`` does, so the
    recipe scan is strictly stronger even though its literal list is not a
    superset. An earlier docstring claimed the superset; QA measured it false.
    """
    from boomi_mcp.models.process_ir import _FORBIDDEN_SECRET_KEY_SUBSTRINGS
    from boomi_mcp.models.recipe_contributions import _FORBIDDEN_KEY_SUBSTRINGS

    for token in _FORBIDDEN_SECRET_KEY_SUBSTRINGS:
        assert any(mine in token for mine in _FORBIDDEN_KEY_SUBSTRINGS), token
